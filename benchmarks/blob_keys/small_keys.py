#!/usr/bin/env python3
"""Coordinated small-key regression measurements of frozen source profiles.

Build/profile metadata is prepared before acquiring the benchmark reservation.
No compilation, source edits, or analysis runs inside the measurement window.
"""
import argparse
import csv
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import uuid


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2) + "\n")
    temp.replace(path)


def registrations(coord):
    result = {}
    for path in coord.glob("*.md"):
        text = path.read_text()
        status = re.search(r"^Status:\s*(.*)$", text, re.M | re.I)
        # Auxiliary notes without Status are not independent projects.
        if status:
            result[path.stem] = {
                "status": status[1],
                "complete": bool(re.match(r"(complete|completed|done|inactive)\b",
                                         status[1], re.I)),
                "text": text,
            }
    return result


def acquire_reservation(reservation, coord):
    """Queued owners are idle and must acknowledge the current owner."""
    while True:
        try:
            fcntl.flock(reservation, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            try:
                request = json.loads((coord / "benchmark-request.json").read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                request = {}
            try:
                release = json.loads((coord / "benchmark-release.json").read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                release = {}
            if (request.get("request_id")
                    and request.get("owner") != "blob-keys"
                    and request["request_id"] != release.get("request_id")):
                write_json(coord / "blob-keys-paused.json", {
                    "request_id": request["request_id"],
                    "participants_paused": True,
                    "participants": ["blob-keys"],
                    "owner_children_paused": True,
                    "paused_at": now(),
                    "note": "Queued benchmark owner idle; all preparation completed.",
                })
            time.sleep(1)


def micro_cases():
    cases = []
    for size in (24, 64, 256, 512):
        for pattern in ("random", "common"):
            for op in ("insert", "overwrite", "hit", "miss", "scan32", "scan4096", "pack"):
                cases.append((size, pattern, op))
    for op in ("insert", "overwrite", "hit", "miss", "scan32", "scan4096", "pack"):
        cases.append((64, "nonlexical", op))
    return cases


def run_micro(args, out, manifest):
    cases = micro_cases()
    if args.cases:
        wanted = set(args.cases.split(","))
        cases = [c for c in cases if f"{c[0]}-{c[1]}-{c[2]}" in wanted]
        if len(cases) != len(wanted):
            raise ValueError("unknown or duplicate case")
    profiles = json.loads(args.profiles.read_text())
    for name in args.variants:
        p = profiles[name]
        if sha(p["binary"]) != p["binary_sha256"]:
            raise RuntimeError("binary changed: " + name)
    manifest["profiles"] = {name: profiles[name] for name in args.variants}
    write_json(out / "manifest.json", manifest)
    for repetition in range(args.repetitions):
        for case_no, (size, pattern, op) in enumerate(cases):
            variants = args.variants[:]
            if (repetition + case_no) % 2:
                variants.reverse()
            for variant in variants:
                name = f"r{repetition}-{size}-{pattern}-{op}-{variant}"
                db = out / (name + ".db")
                keys = 65536 if size <= 64 else 32768
                operation = "lookup" if op in ("hit", "miss") else "scan" if op.startswith("scan") else op
                batches = 4 if operation in ("insert", "overwrite", "pack") else 2
                ops = 500000 if operation == "lookup" else 4096 if op == "scan4096" else 131072
                command = [profiles[variant]["binary"], "--db", str(db),
                           "--operation", operation, "--pattern", pattern,
                           "--keys", str(keys), "--key-bytes", str(size),
                           "--value-bytes", "32", "--cache-mib", "512",
                           "--disk-mib", "2048", "--cpu", str(args.cpu),
                           "--batches", str(batches), "--ops", str(ops),
                           "--seed", str(42 + repetition)]
                if op == "miss":
                    command += ["--miss"]
                if op.startswith("scan"):
                    command += ["--scan-length", op[4:]]
                print(name, flush=True)
                started = now()
                with (out / (name + ".stdout")).open("x") as stdout, \
                     (out / (name + ".stderr")).open("x") as stderr:
                    result = subprocess.run(command, stdout=stdout, stderr=stderr,
                                            timeout=180)
                rows = [json.loads(line) for line in (out / (name + ".stdout")).read_text().splitlines()
                        if line.startswith("{")]
                run = dict(name=name, variant=variant, repetition=repetition,
                           case=f"{size}-{pattern}-{op}", command=command,
                           started=started, finished=now(), returncode=result.returncode,
                           rows=rows)
                manifest["runs"].append(run)
                write_json(out / "manifest.json", manifest)
                if result.returncode or len(rows) != 1 or not rows[0].get("verified") or not rows[0].get("resident"):
                    raise RuntimeError("invalid benchmark run: " + name)
                if rows[0]["active_wall_ns"] < 1000000:
                    raise RuntimeError("measurement too short: " + name)
                # Only our freshly created, successfully verified scratch image.
                db.unlink(missing_ok=True)


def run_public(args, out, manifest):
    profiles = json.loads(args.public_profiles.read_text())
    manifest["public_profiles"] = {v: profiles[v] for v in args.variants}
    for variant in args.variants:
        if sha(profiles[variant]["binary"]) != profiles[variant]["binary_sha256"]:
            raise RuntimeError("public binary changed: " + variant)
    for rep in range(args.repetitions):
        for log in (0, 1):
            for threads in (1, 4):
                variants = args.variants[:]
                if (rep + log + threads) % 2:
                    variants.reverse()
                for variant in variants:
                    name = f"public-r{rep}-log{log}-t{threads}-{variant}"
                    db, csv_path = out / (name + ".db"), out / (name + ".csv")
                    command = ["taskset", "-c", "0-7", profiles[variant]["binary"],
                               "--db", str(db), "--create", "--n", "1000000",
                               "--cache-mib", "512", "--disk-gib", "4",
                               "--log", str(log), "--threads", str(threads),
                               "--mt-bg", "1", "--normal-bg", "4",
                               "--memtable-mib", "24", "--secs", "3",
                               "--warmup", "1", "--csv", str(csv_path),
                               "--hang-dir", str(out), "--vtag", variant,
                               "--stag", "1m-24byte-keys", "--otag", "random",
                               "--rep", str(rep), "--seed", str(42 + rep),
                               "load:random", "warm", "point:uniform", "point:neg",
                               "range:uniform:32", "range:uniform:4096", "ow:uniform"]
                    print(name, flush=True)
                    started = now()
                    with (out / (name + ".stdout")).open("x") as stdout, \
                         (out / (name + ".stderr")).open("x") as stderr:
                        result = subprocess.run(command, stdout=stdout, stderr=stderr,
                                                timeout=300)
                    rows = list(csv.DictReader(csv_path.open())) if csv_path.exists() else []
                    manifest["runs"].append(dict(name=name, variant=variant,
                        repetition=rep, case=f"public-log{log}-t{threads}",
                        command=command, started=started, finished=now(),
                        returncode=result.returncode, rows=rows))
                    write_json(out / "manifest.json", manifest)
                    if result.returncode or len(rows) != 7 or any(
                            int(r["violations"]) or int(r["errors"]) for r in rows):
                        raise RuntimeError("invalid public run: " + name)
                    db.unlink(missing_ok=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profiles", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--coordination", type=Path, required=True)
    p.add_argument("--lock-dir", type=Path, required=True)
    p.add_argument("--variants", nargs="+", default=["baseline", "candidate-v1"])
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--cpu", type=int, default=2)
    p.add_argument("--cases")
    p.add_argument("--public-profiles", type=Path)
    p.add_argument("--public-only", action="store_true")
    p.add_argument("--ack-timeout", type=int, default=1800)
    p.add_argument("--expected-minutes", type=int, default=12)
    a = p.parse_args()
    a.out = a.out.resolve()
    a.profiles = a.profiles.resolve()
    a.out.mkdir(parents=True, exist_ok=False)
    request = dict(request_id="blob-keys-small-" + uuid.uuid4().hex,
                   owner="blob-keys", purpose="small-key B-tree and public API regression comparison",
                   requested_at=now(), expected_duration_minutes=a.expected_minutes,
                   participants=["blob-keys"], owner_children_paused=True)
    manifest = dict(request=request, command_line=os.sys.argv, runs=[],
                    runner_sha256=sha(__file__), started=now(),
                    cpu=Path("/proc/cpuinfo").read_text().split("\n\n")[0],
                    affinity=sorted(os.sched_getaffinity(0)),
                    storage=subprocess.check_output(
                        ["findmnt", "-n", "-o", "FSTYPE,SOURCE", "-T", str(a.out)],
                        text=True).strip())
    write_json(a.out / "manifest.json", manifest)
    shutil_source = a.out / "runner.py"
    shutil_source.write_bytes(Path(__file__).read_bytes())
    with (a.lock_dir / "benchmark.lock").open("r") as reservation:
        print("Waiting for benchmark reservation", flush=True)
        acquire_reservation(reservation, a.coordination)
        request["requested_at"] = now()
        request["participants"] += sorted(k for k, v in registrations(a.coordination).items()
                                           if k != "blob-keys" and not v["complete"])
        write_json(a.coordination / "benchmark-request.json", request)
        write_json(a.out / "request.json", request)
        try:
            deadline = time.monotonic() + a.ack_timeout
            while True:
                peers = registrations(a.coordination)
                missing, acknowledgements = [], {}
                for name, info in peers.items():
                    if name == "blob-keys" or info["complete"]:
                        continue
                    try:
                        ack = json.loads((a.coordination / (name + "-paused.json")).read_text())
                    except (FileNotFoundError, json.JSONDecodeError):
                        ack = {}
                    if ack.get("request_id") == request["request_id"] and ack.get("participants_paused") is True:
                        acknowledgements[name] = ack
                    else:
                        missing.append(name)
                if not missing:
                    manifest["acknowledgements"] = acknowledgements
                    manifest["registrations"] = peers
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Missing explicit acknowledgements: " + ", ".join(missing))
                print("Awaiting paused roots and children: " + ", ".join(missing), flush=True)
                time.sleep(10)
            with (a.lock_dir / "machine.lock").open("r") as machine:
                fcntl.flock(machine, fcntl.LOCK_EX)
                manifest["measurement_start"] = now()
                write_json(a.out / "manifest.json", manifest)
                print("All peers paused; exclusive measurement window starts", flush=True)
                if not a.public_only:
                    run_micro(a, a.out, manifest)
                if a.public_profiles:
                    run_public(a, a.out, manifest)
                manifest["complete"] = True
                manifest["measurement_end"] = now()
                write_json(a.out / "manifest.json", manifest)
        finally:
            release = dict(request_id=request["request_id"], owner="blob-keys",
                           released_at=now(), complete=manifest.get("complete", False),
                           measurements_completed=len(manifest["runs"]))
            write_json(a.coordination / "benchmark-release.json", release)
            write_json(a.out / "release.json", release)
            print("Exclusive benchmark window released", flush=True)


if __name__ == "__main__":
    def interrupt(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    signal.signal(signal.SIGTERM, interrupt)
    main()
