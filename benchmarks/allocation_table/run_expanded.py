#!/usr/bin/env python3
"""Run controlled insertion comparisons with exclusive cooperative machine locks."""
import argparse
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import time
import uuid


def dump(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def registrations(coord):
    result = {}
    for path in coord.glob("*.md"):
        lines = path.read_text().splitlines()
        status = next((line.partition(":")[2].strip().lower()
                       for line in lines if line.startswith("Status:")), None)
        if status is not None and any(line.startswith("Branch:") for line in lines):
            result[path.stem] = status
    return result


def active_projects(coord):
    return sorted(name for name, status in registrations(coord).items()
                  if not status.startswith(("complete", "idle", "inactive", "done")))


def reserve_cooperatively(reservation, coordination):
    """Caller has stopped its children; a queued owner must pause for its peer."""
    acknowledged = None
    while True:
        try:
            fcntl.flock(reservation, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            pass
        try:
            request = json.loads((coordination / "benchmark-request.json").read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            request = {}
        request_id = request.get("request_id")
        if request_id and request.get("owner") != "allocation-table" and request_id != acknowledged:
            dump(coordination / "allocation-table-paused.json", dict(
                request_id=request_id, participants_paused=True, children_idle=True,
                owner_waits_for_reservation=True, active_measurements=False))
            acknowledged = request_id
            print("Queued and paused for peer request", request_id, flush=True)
        time.sleep(1)


def snapshot():
    paths = [Path("/proc/meminfo"), Path("/proc/stat"), Path("/proc/diskstats"),
             Path("/proc/pressure/cpu"), Path("/proc/pressure/io"),
             Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")]
    paths += sorted(Path("/sys/class/thermal").glob("thermal_zone*/temp"))
    paths += sorted(Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_cur_freq"))
    return {str(path): path.read_text().strip() for path in paths if path.exists()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--configs", type=Path, required=True)
    parser.add_argument("--diagnostic-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--coordination", type=Path, required=True)
    parser.add_argument("--locks", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, required=True)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--variants", nargs="+", required=True)
    parser.add_argument("--ack-timeout", type=int, default=180)
    parser.add_argument("--expected-minutes", type=int, default=30)
    parser.add_argument("--command-timeout", type=int, default=900)
    args = parser.parse_args()
    if min(args.repetitions, *args.threads) < 1 or max(args.threads) > 8:
        parser.error("positive counts required; fixed foreground placement supports at most8 threads")
    if len(set(args.variants)) != len(args.variants):
        parser.error("duplicate variants")
    coord, locks = args.coordination.resolve(), args.locks.resolve()
    with (locks / "benchmark.lock").open() as reservation:
        print("Waiting for exclusive benchmark reservation", flush=True)
        reserve_cooperatively(reservation, coord)
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=False)
        manifest = json.loads(args.manifest.read_text())
        configs = json.loads(args.configs.read_text())
        for variant in args.variants:
            entry = manifest["variants"][variant]
            if digest(entry["binary"]) != entry["sha256"]:
                raise RuntimeError("changed frozen binary: " + variant)
        diagnostic = None
        if args.diagnostic_manifest:
            diagnostic = json.loads(args.diagnostic_manifest.read_text())
            for item in diagnostic["variants"].values():
                if digest(item["binary"]) != item["sha256"]:
                    raise RuntimeError("changed diagnostic binary")
            dump(output / "diagnostic-manifest.json", diagnostic)
        dump(output / "manifest.json", manifest)
        dump(output / "configs.json", configs)
        dump(output / "options.json", {key: str(value) if isinstance(value, Path) else value
                                      for key, value in vars(args).items()})
        (output / "run_expanded.py").write_bytes(Path(__file__).read_bytes())
        request_id = "allocation-expanded-" + uuid.uuid4().hex
        done = False
        commands = []
        def terminate(signum, frame):
            raise SystemExit(128 + signum)
        signal.signal(signal.SIGTERM, terminate)
        participants = active_projects(coord)
        request = dict(request_id=request_id, owner="allocation-table",
                       purpose="controlled allocator width/ownership insertion comparison",
                       requested_at=time.time(), participants=participants,
                       expected_duration_minutes=args.expected_minutes)
        dump(coord / "benchmark-request.json", request)
        dump(output / "request.json", request)
        dump(output / "registrations.json", registrations(coord))
        dump(coord / "allocation-table-paused.json", dict(
            request_id=request_id, participants_paused=True,
            owner_runs_measurements_only=True, children_idle=True))
        try:
            deadline = time.monotonic() + args.ack_timeout
            while True:
                acknowledgements = {}
                for name in participants:
                    try:
                        ack = json.loads((coord / (name + "-paused.json")).read_text())
                    except (FileNotFoundError, json.JSONDecodeError):
                        continue
                    if ack.get("request_id") == request_id and ack.get("participants_paused"):
                        acknowledgements[name] = ack
                missing = sorted(set(participants) - acknowledgements.keys())
                if not missing:
                    break
                print("Waiting for paused projects:", missing, flush=True)
                if time.monotonic() >= deadline:
                    raise RuntimeError("missing pause acknowledgements; no measurements")
                time.sleep(5)
            dump(output / "acknowledgements.json", acknowledgements)
            with (locks / "machine.lock").open() as machine:
                fcntl.flock(machine, fcntl.LOCK_EX)
                dump(output / "environment.json", dict(
                    uname=list(os.uname()), started_at=time.time(),
                    affinity=sorted(os.sched_getaffinity(0)),
                    lscpu=subprocess.check_output(["lscpu", "--json"], text=True),
                    foreground_cpus="0,2,4,6,1,3,5,7", background_cpus="8,9,10,11",
                    initial_state=snapshot()))
                environment = dict(os.environ, KVBENCH_FG_CPUS="0,2,4,6,1,3,5,7",
                                   KVBENCH_BG_CPUS="8,9,10,11",
                                   KVBENCH_STATS_AFTER_VERIFY="1")
                environment.pop("KVBENCH_STATS_BEFORE_CLOSE", None)
                # Fixed before measurement. Each full rotation balances position;
                # reverse successive cycles to reduce predecessor imbalance.
                base_order = list(args.variants)
                random.Random(20261026).shuffle(base_order)
                schedule = []
                for rep in range(args.repetitions):
                    rotation = rep % len(base_order)
                    cycle_order = base_order if (rep // len(base_order)) % 2 == 0 else base_order[::-1]
                    order = cycle_order[rotation:] + cycle_order[:rotation]
                    for config in configs:
                        for threads in args.threads:
                            schedule.extend(dict(config=config, threads=threads, rep=rep + 1,
                                                 variant=variant) for variant in order)
                if diagnostic:
                    schedule.extend(dict(config="diagnostic", threads=8, rep=1,
                                         variant=variant, diagnostic=True)
                                    for variant in ["compact2", "compact4", "compact8"])
                dump(output / "schedule.json", schedule)
                for index, run in enumerate(schedule):
                    # If a new project registered, it must acknowledge before the
                    # next measurement even though ordinary batches are locked out.
                    new = set(active_projects(coord)) - set(participants)
                    if new:
                        raise RuntimeError("new active projects require a fresh reservation: " + str(new))
                    diagnostic_run = run.get("diagnostic", False)
                    config = (dict(keys=10000000, cache_mib=128, disk_gib=8)
                              if diagnostic_run else configs[run["config"]])
                    selected_manifest = diagnostic if diagnostic_run else manifest
                    results_csv = output / ("diagnostic.csv" if diagnostic_run else "database.csv")
                    name = "{config}-t{threads}-r{rep}-{variant}".format(**run)
                    database = output / (name + ".db")
                    command = [selected_manifest["variants"][run["variant"]]["binary"], "--create",
                               "--db", str(database), "--n", str(config["keys"]),
                               "--cache-mib", str(config["cache_mib"]),
                               "--disk-gib", str(config["disk_gib"]), "--log", "1",
                               "--threads", str(run["threads"]), "--mt-bg", "1",
                               "--normal-bg", "4", "--memtable-mib", "16",
                               "--seed", str(41 + run["rep"]), "--vtag", run["variant"],
                               "--stag", run["config"], "--rep", str(run["rep"]),
                               "--csv", str(results_csv), "load:random", "warm"]
                    environment["KVBENCH_PHASE_EVENTS"] = str(output / (name + "-phases.jsonl"))
                    before = snapshot()
                    started = time.time()
                    print(f"Starting {index + 1}/{len(schedule)} {name}", flush=True)
                    with (output / (name + ".log")).open("w") as log:
                        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                                   env=environment, start_new_session=True)
                        try:
                            returncode = process.wait(timeout=args.command_timeout)
                        except subprocess.TimeoutExpired:
                            returncode = 124
                        finally:
                            if process.poll() is None:
                                os.killpg(process.pid, signal.SIGKILL)
                                process.wait()
                    record = dict(run, name=name, command=command, started_at=started,
                                  finished_at=time.time(), returncode=returncode,
                                  state_before=before, state_after=snapshot())
                    commands.append(record)
                    with (output / "commands.jsonl").open("a") as journal:
                        journal.write(json.dumps(record) + "\n")
                    if returncode:
                        raise RuntimeError("run failed: " + name)
                    with results_csv.open() as data:
                        rows = [r for r in csv.DictReader(data)
                                if r["size"] == run["config"] and r["version"] == run["variant"]
                                and int(r["rep"]) == run["rep"] and int(r["threads"]) == run["threads"]]
                    if len(rows) != 2 or {r["phase"] for r in rows} != {"load:random", "warm"}:
                        raise RuntimeError("missing/duplicate load or verification: " + name)
                    if any(int(r["errors"]) or int(r["violations"]) or int(r["ops"]) != config["keys"]
                           for r in rows):
                        raise RuntimeError("verification failed: " + name)
                    log = (output / (name + ".log")).read_text()
                    if any(marker in log for marker in ["FINDING:", "VIOLATION:", "ERROR:", "FATAL:"]):
                        raise RuntimeError("diagnostic failure: " + name)
                    # This directory was newly created by this invocation; only
                    # its own stopped and successfully verified scratch DB is removed.
                    info = database.stat()
                    database.unlink()
                    with (output / "discarded-databases.jsonl").open("a") as journal:
                        journal.write(json.dumps(dict(path=str(database), verified=True,
                            apparent_bytes=info.st_size, allocated_bytes=info.st_blocks * 512,
                            discarded_at=time.time())) + "\n")
                    load = next(r for r in rows if r["phase"] == "load:random")
                    if diagnostic_run:
                        counts = [json.loads(line.removeprefix("RC_DIAG "))
                                  for line in log.splitlines() if line.startswith("RC_DIAG ")]
                        if len(counts) != 2 or any(item["performance_valid"] for item in counts):
                            raise RuntimeError("missing/invalid diagnostic counters: " + name)
                        with (output / "diagnostic-counts.jsonl").open("a") as journal:
                            for item in counts:
                                journal.write(json.dumps(dict(item, variant=run["variant"])) + "\n")
                        print(f"Finished {index + 1}/{len(schedule)} {name}: counters collected; verified; timing invalid", flush=True)
                    else:
                        print(f"Finished {index + 1}/{len(schedule)} {name}: {load['ops_s']} inserts/s; verified", flush=True)
                done = True
        finally:
            release = dict(request_id=request_id, owner="allocation-table", released_at=time.time(),
                           complete=done, measurements_completed=len(commands))
            history = coord / "allocation-release-history"
            history.mkdir(exist_ok=True)
            dump(history / (request_id + ".json"), release)
            dump(coord / "benchmark-release.json", release)
            dump(output / "release.json", release)
            print("Released benchmark reservation; complete:", done, flush=True)


if __name__ == "__main__":
    main()
