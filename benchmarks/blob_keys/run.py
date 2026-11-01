#!/usr/bin/env python3
"""Run real blob-key/B-tree experiments with cross-project quiescence."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)

def completed(path):
    text = path.read_text()
    status = re.search(r"^Status:\s*(.*)$", text, re.MULTILINE | re.IGNORECASE)
    return bool(status and re.match(r"(completed|complete|done|inactive)\b",
                                   status[1], re.IGNORECASE))

def active_projects(coord):
    return {p.stem for p in coord.glob("*.md")
            if p.stem != "blob-keys" and not completed(p)}

def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--coordination", type=Path, required=True)
    parser.add_argument("--lock-dir", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=900,
                        help="seconds to wait for every explicit pause acknowledgement")
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    args.binary = args.binary.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    coord = args.coordination.resolve()
    request_id = "blob-keys-" + uuid.uuid4().hex
    request = dict(request_id=request_id, owner="blob-keys",
                   purpose="real B-tree inline/blob-key comparison",
                   requested_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    root = Path(__file__).resolve().parents[2]
    fs = subprocess.check_output(
        ["findmnt", "-n", "-o", "FSTYPE,SOURCE", "-T", str(args.out)], text=True).strip()
    if fs.split()[0] in {"tmpfs", "ramfs"}:
        raise RuntimeError("Use disk-backed storage for this experiment")
    manifest = dict(request=request, binary=str(args.binary),
                    binary_sha256=sha256(args.binary), storage=fs,
                    revision=subprocess.check_output(["git", "rev-parse", "HEAD"],
                                                     cwd=root, text=True).strip(),
                    source_sha256={p.name: sha256(p) for p in Path(__file__).parent.iterdir()
                                   if p.suffix in {".c", ".h", ".py"}},
                    cpu=Path("/proc/cpuinfo").read_text().split("\n\n")[0],
                    command_line=os.sys.argv, runs=[])
    write_json(args.out / "manifest.json", manifest)
    benchmark_lock = open(args.lock_dir / "benchmark.lock", "a+")
    print("Waiting for benchmark lock", flush=True)
    fcntl.flock(benchmark_lock, fcntl.LOCK_EX)
    write_json(coord / "benchmark-request.json", request)
    write_json(args.out / "request.json", request)
    try:
        deadline = time.monotonic() + args.timeout
        while True:
            missing, acknowledged = [], {}
            for project in sorted(active_projects(coord)):
                path = coord / (project + "-paused.json")
                try:
                    ack = json.loads(path.read_text())
                except (FileNotFoundError, json.JSONDecodeError):
                    ack = {}
                if ack.get("request_id") == request_id and ack.get("participants_paused") is True:
                    acknowledged[project] = ack
                else:
                    missing.append(project)
            if not missing:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("No quiet-machine acknowledgement: " + ", ".join(missing))
            print("Waiting for paused acknowledgements: " + ", ".join(missing), flush=True)
            time.sleep(10)
        manifest["acknowledgements"] = acknowledged
        manifest["completed_projects"] = [p.stem for p in coord.glob("*.md") if completed(p)]
        write_json(args.out / "manifest.json", manifest)
        with open(args.lock_dir / "machine.lock", "a+") as machine:
            fcntl.flock(machine, fcntl.LOCK_EX)
            print("All active projects paused; exclusive measurements starting", flush=True)
            cases = [
                (128, 32768, 131072),
                (512, 16384, 65536),
                (4096, 8192, 32768),
                (65536, 1024, 4096),
                (1048576, 64, 256),
                (2097152, 16, 64),
            ]
            for repetition in range(args.repetitions):
                for size, records, queries in cases:
                    for pattern in ("early", "late"):
                        variants = [("inline", 0), ("blob", 32)] if size <= 512 else [
                            ("blob", 0), ("blob", 32)]
                        if repetition % 2:
                            variants.reverse()
                        for mode, prefix in variants:
                            name = f"r{repetition}-{size}-{pattern}-{mode}-p{prefix}"
                            command = [str(args.binary), "--db", str(args.out / (name + ".db")),
                                       "--mode", mode, "--records", str(records),
                                       "--queries", str(queries), "--key-bytes", str(size),
                                       "--prefix", str(prefix), "--common-prefix",
                                       str(0 if pattern == "early" else size - 8),
                                       "--cache-mib", "64"]
                            print(name, flush=True)
                            start = time.time()
                            with open(args.out / (name + ".jsonl"), "w") as stdout, \
                                 open(args.out / (name + ".stderr"), "w") as stderr:
                                result = subprocess.run(command, stdout=stdout, stderr=stderr)
                            run = dict(name=name, repetition=repetition, pattern=pattern,
                                       command=command, returncode=result.returncode,
                                       started=start, finished=time.time())
                            manifest["runs"].append(run)
                            write_json(args.out / "manifest.json", manifest)
                            if result.returncode != 0:
                                raise RuntimeError("Benchmark failed: " + name)
                            lines = [json.loads(line) for line in
                                     (args.out / (name + ".jsonl")).read_text().splitlines()
                                     if line.startswith("{")]
                            if not any(row.get("validation") == "passed" for row in lines):
                                raise RuntimeError("Missing successful validation: " + name)
                            # Only delete the scratch image produced by this completed run.
                            (args.out / (name + ".db")).unlink(missing_ok=True)
            manifest["completed"] = True
            write_json(args.out / "manifest.json", manifest)
    finally:
        release = dict(request_id=request_id, owner="blob-keys",
                       released_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                       complete=manifest.get("completed", False))
        write_json(coord / "benchmark-release.json", release)
        write_json(args.out / "release.json", release)
        fcntl.flock(benchmark_lock, fcntl.LOCK_UN)
        benchmark_lock.close()
        print("Benchmark lock released", flush=True)

if __name__ == "__main__":
    main()
