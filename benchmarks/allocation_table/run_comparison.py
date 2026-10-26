#!/usr/bin/env python3
"""Run frozen comparisons only after every active project acknowledges pause.

This process owns both locks during measurement. It performs no source edits or
builds. Missing acknowledgements cancel the reservation; time never substitutes
for permission. Parent must stop all child agents before invoking this runner.
"""
import argparse
import fcntl
import hashlib
import json
import os
import signal
from pathlib import Path
import subprocess
import time
import uuid


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--coordination", type=Path, required=True)
    parser.add_argument("--locks", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--keys", type=int, default=2000000)
    parser.add_argument("--ack-timeout", type=int, default=120)
    parser.add_argument("--expected-minutes", type=int, default=12)
    args = parser.parse_args()
    def terminate(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    if min(args.repetitions, args.keys, args.ack_timeout) < 1:
        parser.error("repetitions, keys and ack timeout must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(args.manifest.read_text())
    for item in [manifest["micro"], *manifest["variants"].values()]:
        if sha(item["binary"]) != item["sha256"]:
            raise RuntimeError("frozen binary changed: " + item["binary"])
    atomic_json(output / "manifest.json", manifest)
    (output / "run_comparison.py").write_bytes(Path(__file__).read_bytes())
    coord, locks = args.coordination.resolve(), args.locks.resolve()
    request_id = "allocation-table-" + uuid.uuid4().hex
    completed = False
    commands = []
    with (locks / "benchmark.lock").open("r") as bench_lock:
        print("Waiting for benchmark reservation", flush=True)
        fcntl.flock(bench_lock, fcntl.LOCK_EX)
        participants = []
        for path in coord.glob("*.md"):
            lines = path.read_text().splitlines()
            status = next((line.partition(":")[2].strip().lower()
                           for line in lines if line.startswith("Status:")), None)
            if status is None or not any(line.startswith("Branch:") for line in lines):
                continue  # auxiliary coordination notes are not registrations
            if not status.startswith(("complete", "idle", "inactive", "done")):
                participants.append(path.stem)
        request = dict(request_id=request_id, owner="allocation-table",
                       purpose="packed refcounts and mini ownership comparison",
                       requested_at=time.time(), participants=sorted(participants),
                       expected_duration_minutes=args.expected_minutes)
        atomic_json(coord / "benchmark-request.json", request)
        atomic_json(coord / "allocation-table-paused.json",
                    dict(request_id=request_id, participants_paused=True,
                         owner_runs_measurements_only=True))
        atomic_json(output / "request.json", request)
        try:
            deadline = time.monotonic() + args.ack_timeout
            while True:
                waiting = []
                for name in participants:
                    path = coord / (name + "-paused.json")
                    try:
                        ack = json.loads(path.read_text())
                    except (FileNotFoundError, json.JSONDecodeError):
                        ack = {}
                    if ack.get("request_id") != request_id or not ack.get("participants_paused"):
                        waiting.append(name)
                if not waiting:
                    break
                print("Waiting for paused acknowledgements:", ", ".join(waiting), flush=True)
                if time.monotonic() >= deadline:
                    raise RuntimeError("cancelled without measurements: missing acknowledgements")
                time.sleep(5)
            atomic_json(output / "acknowledgements.json", {
                name: json.loads((coord / (name + "-paused.json")).read_text())
                for name in participants})
            with (locks / "machine.lock").open("r") as machine_lock:
                fcntl.flock(machine_lock, fcntl.LOCK_EX)
                atomic_json(output / "environment.json", dict(
                    uname=list(os.uname()), cpu_affinity=sorted(os.sched_getaffinity(0)),
                    lscpu=subprocess.check_output(["lscpu", "--json"], text=True),
                    started=time.time(), keys=args.keys, repetitions=args.repetitions))

                def invoke(command, name, environment=None):
                    started = time.time()
                    with (output / (name + ".log")).open("w") as log:
                        proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                                env=environment, start_new_session=True)
                        try:
                            returncode = proc.wait(timeout=180)
                        finally:
                            if proc.poll() is None:
                                os.killpg(proc.pid, signal.SIGKILL)
                                proc.wait()
                    record = dict(name=name, command=command, started=started,
                                  finished=time.time(), returncode=returncode)
                    commands.append(record)
                    with (output / "commands.jsonl").open("a") as journal:
                        journal.write(json.dumps(record) + "\n")
                    if returncode:
                        raise RuntimeError("comparison failed: " + name)
                    return (output / (name + ".log")).read_text()

                micro = manifest["micro"]["binary"]
                with (output / "micro.jsonl").open("w") as results:
                    for rep in range(args.repetitions):
                        for extents in [65536, 1048576, 8388608]:
                            for threads in [1, 4, 16]:
                                for work in ["get", "allocated", "claim", "promote", "high", "hot"]:
                                    order = ["dense", "compact"] if rep % 2 == 0 else ["compact", "dense"]
                                    checksums = []
                                    for backend in order:
                                        ops = max(1, (200000 if work == "hot" else 2000000) // threads)
                                        name = f"micro-{rep}-{extents}-{threads}-{work}-{backend}"
                                        command = [micro, "--backend", backend, "--workload", work,
                                                   "--extents", str(extents), "--threads", str(threads),
                                                   "--ops", str(ops), "--seed", str(42 + rep)]
                                        if work in ("get", "allocated"):
                                            command += ["--high-count", str(extents // 100)]
                                        record = json.loads(invoke(command, name))
                                        record["rep"] = rep
                                        results.write(json.dumps(record) + "\n")
                                        results.flush()
                                        checksums.append(record["checksum"])
                                    if work != "hot" and checksums[0] != checksums[1]:
                                        raise RuntimeError("backend operation streams differ")
                            print("Micro repetition", rep + 1, "extents", extents, "complete", flush=True)
                environment = dict(os.environ, KVBENCH_STATS_BEFORE_CLOSE="1")
                variants = ["original", "dense", "compact"]
                for rep in range(args.repetitions):
                    for threads in [1, 8]:
                        order = variants[rep % 3:] + variants[:rep % 3]
                        for variant in order:
                            name = f"database-{rep}-{threads}-{variant}"
                            command = [manifest["variants"][variant]["binary"], "--create",
                                       "--db", str(output / (name + ".db")), "--n", str(args.keys),
                                       "--cache-mib", "128", "--disk-gib", "8", "--log", "1",
                                       "--threads", str(threads), "--mt-bg", "1", "--normal-bg", "4",
                                       "--memtable-mib", "16", "--secs", "3", "--warmup", "1",
                                       "--seed", str(42 + rep), "--vtag", variant, "--rep", str(rep + 1),
                                       "--csv", str(output / "database.csv"), "load:random", "warm",
                                       "point:uniform", "range:uniform:32", "ow:uniform", "warm"]
                            invoke(command, name, environment)
                            print("Database", name, "complete", flush=True)
                completed = True
        finally:
            release = dict(request_id=request_id, owner="allocation-table",
                           released_at=time.time(), complete=completed,
                           measurements_completed=len(commands))
            atomic_json(coord / "benchmark-release.json", release)
            atomic_json(output / "release.json", release)
            print("Released benchmark reservation; complete:", completed, flush=True)


if __name__ == "__main__":
    main()
