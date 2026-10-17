#!/usr/bin/env python3
"""Build/test the prefix merge; serialize with exclusive campaign benchmarks."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time


ARCHIVE = Path(__file__).resolve().parent
TREE = ARCHIVE.parents[3]
BUILD = TREE / "build-main-6105c49f"
MACHINE_LOCK = Path(
    "/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/machine.lock"
)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def git(*args):
    return subprocess.check_output(["git", *args], cwd=TREE, text=True).strip()


def source_manifest():
    paths = git("ls-files", "Makefile", "src", "include", "tests", "examples")
    return {
        "head": git("rev-parse", "HEAD"),
        "origin_main": git("rev-parse", "origin/main"),
        "source_sha256": {p: sha256(TREE / p) for p in paths.splitlines()},
        "tracked_source_diff": git("diff", "HEAD", "--", "Makefile", "src", "include", "tests", "examples"),
    }


def run(command, cwd, log, timeout, env=None):
    record = {
        "command": [str(arg) for arg in command],
        "cwd": str(cwd),
        "started": time.time(),
        "timeout_seconds": timeout,
    }
    with log.open("w") as output:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            record["returncode"] = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            record.update(returncode=124, timed_out=True)
    record.update(finished=time.time(), log_sha256=sha256(log), log=log.name)
    record["seconds"] = record["finished"] - record["started"]
    return record


def build(mode):
    command = ["make", "-j6", "BUILD_ROOT=" + BUILD.name,
               "BUILD_MODE=" + mode, "all"]
    if mode == "release":
        command.append("benchmarks")
    env = os.environ.copy()
    with tempfile.TemporaryDirectory(prefix="prefix-build-", dir=TREE / ".cache") as tmp:
        env["TMPDIR"] = tmp
        record = run(command, TREE, ARCHIVE / (mode + "-build.log"), 900, env)
        record["TMPDIR"] = tmp
    record["source_manifest"] = source_manifest()
    record["build_config"] = (BUILD / mode / ".build_config").read_text() if (BUILD / mode / ".build_config").exists() else None
    write_json(ARCHIVE / (mode + "-build.json"), record)
    print(mode, "build", record["returncode"], flush=True)
    return record["returncode"]


def test(mode):
    initial_sources = source_manifest()
    write_json(ARCHIVE / (mode + "-source-manifest.json"), initial_sources)
    scratch_root = Path(tempfile.mkdtemp(prefix="prefix-validation-" + mode + "-", dir=TREE / ".cache"))
    jobs = [(name, "unit/" + name + "_test", []) for name in
            ["btree", "superblock", "merge", "splinterdb_quick", "splinter"]]
    jobs.extend([
        ("automatic-log-recovery", "unit/splinterdb_forked_child_test",
         ["automatic_log_barriers_survive_process_exit"]),
        ("functional-log", "driver_test", ["log_test", "--db-location", "log.db",
         "--cache-capacity-mib", "64", "--db-capacity-gib", "4"]),
    ])
    records = []
    for name, binary_name, args in jobs:
        scratch = scratch_root / name
        scratch.mkdir()
        (scratch / BUILD.name).symlink_to(BUILD, target_is_directory=True)
        binary = BUILD / mode / "bin" / binary_name
        command = [str(binary), *args]
        if name == "splinter":
            command.extend(["--db-location", "core.db", "--cache-capacity-mib", "128", "--db-capacity-gib", "64"])
        log = ARCHIVE / (mode + "-" + name + ".log")
        record = run(command, scratch, log, 900)
        content = log.read_text()
        summaries = re.findall(r"RESULTS:.*", content)
        record.update(name=name, binary_sha256=sha256(binary), summaries=summaries,
                      library_sha256=sha256(BUILD / mode / "lib/libsplinterdb.so"))
        if name == "splinter":
            record["blob_cases"] = [line for line in content.splitlines() if "test_blob_" in line]
        if name == "functional-log":
            record["replay_summary"] = [line for line in content.splitlines() if "500000" in line or "500,000" in line]
        if name != "functional-log" and not re.search(r"RESULTS: [1-9][0-9]* tests", content):
            record["missing_test_summary"] = True
            record["returncode"] = record["returncode"] or 125
        records.append(record)
        write_json(ARCHIVE / (mode + "-results.json"), records)
        print(mode, name, record["returncode"], summaries, flush=True)
        if record["returncode"]:
            return record["returncode"]
    final_sources = source_manifest()
    unchanged = initial_sources["source_sha256"] == final_sources["source_sha256"]
    write_json(ARCHIVE / (mode + "-source-check.json"), {"unchanged": unchanged})
    return 0 if unchanged else 126


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["build", "test"])
    parser.add_argument("mode", choices=["debug", "release"])
    args = parser.parse_args()
    (TREE / ".cache").mkdir(exist_ok=True)
    with MACHINE_LOCK.open("rb") as lock:
        print("Waiting for shared campaign machine lock", flush=True)
        fcntl.flock(lock, fcntl.LOCK_SH)
        print("Acquired shared campaign machine lock", flush=True)
        return build(args.mode) if args.action == "build" else test(args.mode)


if __name__ == "__main__":
    raise SystemExit(main())
