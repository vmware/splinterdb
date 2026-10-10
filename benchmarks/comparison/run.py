#!/usr/bin/env python3
"""Run paired SplinterDB comparisons with ABBA ordering and a hard deadline."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def parse_deadline(value: str) -> float:
    if value.isdigit():
        return float(value)
    now = dt.datetime.now().astimezone()
    hour, minute = map(int, value.split(":"))
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += dt.timedelta(days=1)
    return target.timestamp()


class Runner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = args.out.resolve()
        self.raw = self.out / "results" / "raw"
        self.data = self.out / "data"
        self.bin = self.out / "bin"
        self.manifest = self.out / "results" / "runs.jsonl"
        self.kv_csv = self.out / "results" / "kvbench.csv"
        for path in (self.raw, self.data, self.bin):
            path.mkdir(parents=True, exist_ok=True)
        self.stop_thermal = threading.Event()
        self.step = 0
        self.completed = set()
        if self.manifest.exists():
            for line in self.manifest.read_text().splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("status") == "ok":
                    self.completed.add((record.get("kind"), record.get("version"),
                                        record.get("rep"), record.get("label")))

    def remaining(self) -> float:
        return self.args.deadline - time.time()

    def enough_time(self, estimate_s: float) -> bool:
        return self.remaining() > estimate_s + self.args.report_reserve_minutes * 60

    def record(self, item: dict) -> None:
        item = dict(item)
        item["recorded_epoch"] = time.time()
        with self.manifest.open("a") as stream:
            json.dump(item, stream, sort_keys=True)
            stream.write("\n")

    def thermal_sampler(self) -> None:
        path = self.out / "results" / "thermal.csv"
        new = not path.exists()
        with path.open("a", newline="") as stream:
            writer = csv.writer(stream)
            if new:
                writer.writerow(["epoch", "load1", "mem_available_kib", "swap_free_kib",
                                 "mean_cpu_mhz", "max_temp_c"])
            while not self.stop_thermal.wait(2):
                try:
                    mem = {}
                    for line in Path("/proc/meminfo").read_text().splitlines():
                        key, value, *_ = line.replace(":", "").split()
                        mem[key] = int(value)
                    freqs = []
                    for cpu in self.args.cpus.split(","):
                        for number in self.expand_cpu(cpu):
                            p = Path(f"/sys/devices/system/cpu/cpu{number}/cpufreq/scaling_cur_freq")
                            if p.exists():
                                freqs.append(int(p.read_text()) / 1000)
                    temps = []
                    for p in Path("/sys/class/thermal").glob("thermal_zone*/temp"):
                        try:
                            value = int(p.read_text()) / 1000
                            if 0 < value < 150:
                                temps.append(value)
                        except (OSError, ValueError):
                            pass
                    load1 = float(Path("/proc/loadavg").read_text().split()[0])
                    writer.writerow([time.time(), load1, mem.get("MemAvailable", 0),
                                     mem.get("SwapFree", 0),
                                     sum(freqs) / len(freqs) if freqs else "",
                                     max(temps) if temps else ""])
                    stream.flush()
                except OSError:
                    pass

    @staticmethod
    def expand_cpu(part: str):
        if "-" in part:
            lo, hi = map(int, part.split("-"))
            return range(lo, hi + 1)
        return [int(part)]

    def run(self, name: str, command: list[str], metadata: dict,
            estimate_s: float, allow_failure: bool = False) -> bool:
        if not self.enough_time(estimate_s):
            self.record({**metadata, "name": name, "status": "skipped_deadline",
                         "remaining_s": self.remaining()})
            return False
        self.step += 1
        stem = f"{self.step:04d}-{name.replace(':', '_').replace('/', '_')}"
        stdout_path = self.raw / f"{stem}.out"
        stderr_path = self.raw / f"{stem}.err"
        disk = shutil.disk_usage(self.out)
        if disk.free < self.args.min_free_gib * (1 << 30):
            raise RuntimeError(f"only {disk.free / (1 << 30):.1f} GiB free")
        started = time.time()
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] START {name}; "
              f"{self.remaining()/3600:.2f}h left", flush=True)
        status, rc = "ok", 0
        try:
            with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
                stdout.write(f"command={json.dumps(command)}\n")
                stdout.write(f"disk_free_gib={disk.free / (1 << 30):.3f}\n")
                stdout.flush()
                result = subprocess.run(["taskset", "-c", self.args.cpus, *command],
                                        stdout=stdout, stderr=stderr,
                                        timeout=max(1, self.remaining()
                                                    - self.args.report_reserve_minutes * 60))
                rc = result.returncode
                if rc:
                    status = "failed"
        except subprocess.TimeoutExpired:
            status, rc = "deadline", 124
        ended = time.time()
        self.record({**metadata, "name": name, "status": status, "rc": rc,
                     "started_epoch": started, "ended_epoch": ended,
                     "wall_s": ended - started,
                     "stdout": str(stdout_path.relative_to(self.out)),
                     "stderr": str(stderr_path.relative_to(self.out)),
                     "command": command})
        print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] END   {name} "
              f"rc={rc} wall={ended-started:.1f}s", flush=True)
        if rc and not allow_failure:
            print(f"warning: {name} failed; continuing with the other version", file=sys.stderr)
        return rc == 0

    def insert_run(self, kind: str, version: str, rep: int, label: str,
                   seconds: int, target: int = 0, barrier_ms: int = 0,
                   readers: int = 0, inserters: int = 6, log: bool = True,
                   preload_records: int = 0) -> bool:
        if (kind, version, rep, label) in self.completed:
            print(f"SKIP completed {kind}-{label}-{version}-r{rep}", flush=True)
            return True
        run_dir = self.out / "results" / kind / f"{label}-{version}-r{rep}"
        run_dir.mkdir(parents=True, exist_ok=True)
        image = self.data / f"{kind}-{label}-{version}-r{rep}.db"
        image.unlink(missing_ok=True)
        for file in (run_dir / "series.csv", run_dir / "summary.csv"):
            file.unlink(missing_ok=True)
        if preload_records:
            preload_csv = run_dir / "preload-series.csv"
            preload_summary = run_dir / "preload-summary.csv"
            preload_csv.unlink(missing_ok=True)
            preload_summary.unlink(missing_ok=True)
            preload = [str(self.bin / f"insert_benchmark-{version}"),
                       "--db", str(image), "--csv", str(preload_csv),
                       "--summary", str(preload_summary), "--records",
                       str(preload_records), "--cache-mib", "3072",
                       "--disk-gib", "128", "--inserters", str(inserters),
                       "--readers", "0", "--report-ms", "1000",
                       "--min-free-gib", str(self.args.min_free_gib)]
            if not log:
                preload.append("--no-log")
            setup_meta = {"kind": "setup", "parent_kind": kind,
                          "version": version, "rep": rep, "label": label,
                          "records": preload_records}
            if not self.run(f"setup-{kind}-{label}-{version}-r{rep}", preload,
                            setup_meta, 180, allow_failure=True):
                image.unlink(missing_ok=True)
                return False
        command = [str(self.bin / f"insert_benchmark-{version}"),
                   "--db", str(image), "--csv", str(run_dir / "series.csv"),
                   "--summary", str(run_dir / "summary.csv"),
                   "--records", str(1 << 40), "--cache-mib", "3072",
                   "--disk-gib", "128", "--inserters", str(inserters),
                   "--readers", str(readers), "--seconds", str(seconds),
                   "--report-ms", "100", "--min-free-gib", str(self.args.min_free_gib)]
        if preload_records:
            command += ["--open", "--id-base", str(preload_records)]
        if not log:
            command.append("--no-log")
        if target:
            command += ["--target-insert-ops-s", str(target)]
        if barrier_ms:
            command += ["--barrier-ms", str(barrier_ms)]
        meta = {"kind": kind, "version": version, "rep": rep, "label": label,
                "target_insert_ops_s": target, "barrier_ms": barrier_ms,
                "seconds": seconds, "readers": readers, "inserters": inserters,
                "log": int(log), "series": str((run_dir / "series.csv").relative_to(self.out)),
                "summary": str((run_dir / "summary.csv").relative_to(self.out))}
        ok = self.run(f"{kind}-{label}-{version}-r{rep}", command, meta,
                      seconds + 90, allow_failure=True)
        if ok:
            self.completed.add((kind, version, rep, label))
        image.unlink(missing_ok=True)
        return ok

    def enhanced(self) -> None:
        for rep in range(1, self.args.reps + 1):
            order = ("head", "main") if rep % 2 else ("main", "head")
            for version in order:
                self.insert_run("stall", version, rep, "mixed-8i8q", 120,
                                readers=8, inserters=8)

        targets = (0, 500_000, 1_000_000, 1_500_000, 2_000_000,
                   2_500_000, 3_000_000)
        for rep in range(1, self.args.reps + 1):
            first = ("head", "main") if rep % 2 else ("main", "head")
            for index, target in enumerate(targets):
                order = first if index % 2 == 0 else tuple(reversed(first))
                for version in order:
                    self.insert_run("open-loop", version, rep,
                                    f"target-{target}", 45, target=target,
                                    readers=0, inserters=6,
                                    preload_records=1 << 25)

        # This API is new on the audited branch, so it has no main equivalent.
        for barrier_ms in (1, 10, 100):
            self.insert_run("barrier", "head", 1, f"every-{barrier_ms}ms", 60,
                            barrier_ms=barrier_ms, readers=4, inserters=6)

    def kv_common(self, version: str, image: Path, size: str, order: str,
                  log: int, rep: int, secs: int, warmup: int) -> list[str]:
        if size == "small":
            n, cache, disk = 1 << 23, 4096, 32
        else:
            n, cache, disk = 1 << 27, 1024, 128
        return [str(self.bin / f"kvbench-{version}"), "--db", str(image),
                "--n", str(n), "--cache-mib", str(cache), "--disk-gib", str(disk),
                "--log", str(log), "--threads", "6", "--mt-bg", "1",
                "--normal-bg", "4", "--secs", str(secs), "--warmup", str(warmup),
                "--vtag", version, "--stag", size, "--otag", order,
                "--rep", str(rep), "--csv", str(self.kv_csv),
                "--hang-dir", str(self.raw)]

    def core_cell(self, size: str, order_name: str, log: int, rep: int,
                  suite: str) -> None:
        first = ("head", "main") if rep % 2 else ("main", "head")
        images = {v: self.data / f"core-{size}-{order_name}-log{log}-{v}-r{rep}.db"
                  for v in first}
        for image in images.values():
            image.unlink(missing_ok=True)
        alive = {"head": True, "main": True}

        def one(version: str, label: str, create: bool, phases: list[str],
                secs: int | None = None, warmup: int | None = None,
                estimate: int = 90):
            if not alive[version]:
                return False
            actual_secs = self.args.phase_seconds if secs is None else secs
            actual_warmup = (3 if size == "small" else 5) if warmup is None else warmup
            command = self.kv_common(version, images[version], size, order_name,
                                     log, rep, actual_secs, actual_warmup)
            command += ["--create" if create else "--open", *phases]
            metadata = {"kind": "core", "version": version, "rep": rep,
                        "size": size, "order": order_name, "log": log,
                        "label": label, "phases": phases}
            ok = self.run(f"core-{size}-{order_name}-l{log}-r{rep}-{version}-{label}",
                          command, metadata, estimate, allow_failure=True)
            if not ok:
                alive[version] = False
            return ok

        for version in first:
            one(version, "load", True, [f"load:{order_name}"], estimate=600 if size == "large" else 180)

        phase_list = {
            "full": ["point:uniform", "point:zipf", "point:seq", "point:neg",
                     "point:uniform@1", "range:uniform:1@1", "range:uniform:100@1",
                     "range:uniform:1", "range:uniform:10", "range:uniform:100",
                     "range:uniform:10000", "range:zipf:1", "range:zipf:10",
                     "range:zipf:100", "range:zipf:10000"],
            "reduced": ["point:uniform", "point:neg", "range:uniform:10",
                        "range:uniform:10000"],
        }[suite]
        phase_index = 0
        if size == "large":
            for version in first:
                one(version, "verify", False, ["warm"], estimate=600)
        for phase in phase_list:
            order = first if phase_index % 2 == 0 else tuple(reversed(first))
            phase_index += 1
            for version in order:
                prefix = ["warm"] if size == "small" else []
                one(version, phase, False, [*prefix, phase], estimate=120)

        # Large/random overwrites have multi-minute post-load splitting and
        # compaction transients. Run both distributions long enough to get past
        # that transient. Each sparse image can expand by tens of GiB, so run
        # all overwrite phases for one version before moving to the other and
        # reclaim the completed image immediately. Repetition order still gives
        # ABBA ordering for the two version blocks.
        overwrite_phases = (("ow:uniform", "ow:zipf")
                            if size == "large" and order_name == "random"
                            else ("ow:zipf", "ow:uniform"))
        steady = size == "large" and order_name == "random"
        if steady:
            for version in first:
                for phase in overwrite_phases:
                    phase_index += 1
                    one(version, phase, False, [phase],
                        secs=self.args.steady_seconds,
                        warmup=self.args.steady_warmup,
                        estimate=self.args.steady_seconds
                                 + self.args.steady_warmup + 180)
                images[version].unlink(missing_ok=True)
        else:
            for phase in overwrite_phases:
                order = first if phase_index % 2 == 0 else tuple(reversed(first))
                phase_index += 1
                warmup = 3 if size == "small" else 5
                for version in order:
                    one(version, phase, False, [phase],
                        secs=self.args.phase_seconds, warmup=warmup,
                        estimate=self.args.phase_seconds + warmup + 180)

        for image in images.values():
            image.unlink(missing_ok=True)

    def core(self) -> None:
        # Repeated high-value cells come first so paired estimates exist even if
        # the deadline prevents completion of the broader coverage matrix.
        cells = []
        for rep in range(1, self.args.reps + 1):
            cells += [("small", "random", 1, rep, "full"),
                      ("large", "random", 1, rep, "full"),
                      ("large", "random", 0, rep, "full"),
                      ("small", "random", 0, rep, "full")]
        for rep in range(1, self.args.reps + 1):
            cells += [(size, "seq", log, rep, "reduced")
                      for size in ("small", "large") for log in (0, 1)]
        for cell in cells:
            if not self.enough_time(300):
                break
            self.core_cell(*cell)

    def execute(self) -> None:
        thread = threading.Thread(target=self.thermal_sampler, daemon=True)
        thread.start()
        try:
            if not self.args.skip_enhanced:
                self.enhanced()
            if self.args.core != "none":
                self.core()
        finally:
            self.stop_thermal.set()
            thread.join(timeout=3)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--deadline", type=parse_deadline, required=True,
                        help="local HH:MM or epoch; reserves time for reporting")
    parser.add_argument("--cpus", default="0-11")
    parser.add_argument("--reps", type=int, default=2)
    parser.add_argument("--phase-seconds", type=int, default=20)
    parser.add_argument("--steady-warmup", type=int, default=180)
    parser.add_argument("--steady-seconds", type=int, default=300)
    parser.add_argument("--report-reserve-minutes", type=int, default=30)
    parser.add_argument("--min-free-gib", type=int, default=30)
    parser.add_argument("--core", choices=("priority", "none"), default="priority")
    parser.add_argument("--skip-enhanced", action="store_true")
    args = parser.parse_args()
    if args.reps < 1:
        parser.error("invalid arguments")
    runner = Runner(args)
    runner.execute()


if __name__ == "__main__":
    main()
