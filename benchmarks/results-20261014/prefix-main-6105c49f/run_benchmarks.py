#!/usr/bin/env python3
"""Prefix A/B measurements. The caller must hold the campaign machine lock.

build.json maps main/candidate to tree, commit, library, library_sha256,
binary, binary_sha256 and source_sha256. Paths may be absolute or relative to
the repository root. Databases are deliberately kept out of the archive.
"""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import signal
import subprocess
import time


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATABASES = ROOT / ".cache/prefix-main-6105c49f/databases"
CASES = [f"{regime}-{pattern}" for regime in ("resident", "disk")
         for pattern in ("common", "random", "nonlexical")]
CASES.append("resident-common-disabled")


def sha(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def read_rows(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


def sync(path):
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def file_identity(path):
    stat = path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns,
            "inode": stat.st_ino, "allocated_bytes": stat.st_blocks * 512}


def verify_rows(path, phases, n):
    rows = read_rows(path)
    assert [row["phase"] for row in rows] == phases, (path, rows, phases)
    assert all(int(row["ops"]) > 0 and int(row["errors"]) == 0
               and int(row["violations"]) == 0 for row in rows), path
    assert all(int(row["ops"]) == n for row in rows
               if row["phase"] == "warm" or row["phase"].startswith("load:")), path
    return rows


class Campaign:
    def __init__(self, args):
        self.args = args
        self.builds = json.loads((HERE / "build.json").read_text())
        for profile in ("main", "candidate"):
            build = self.builds[profile]
            for kind in ("binary", "library"):
                path = Path(build[kind])
                if not path.is_absolute():
                    path = ROOT / path
                assert sha(path) == build[kind + "_sha256"], (profile, kind)
                build[kind] = str(path)
        DATABASES.mkdir(parents=True, exist_ok=True)
        filesystem = subprocess.check_output(
            ["findmnt", "-n", "-o", "FSTYPE,SOURCE", "-T", str(DATABASES)],
            text=True).strip()
        assert filesystem.split()[0] not in ("tmpfs", "ramfs"), filesystem
        assert filesystem.split()[1].startswith("/dev/"), filesystem
        self.folder = HERE / args.mode
        self.folder.mkdir(exist_ok=False)
        self.fixture_folder = HERE / "fixtures"
        self.fixture_folder.mkdir(exist_ok=True)
        fixtures_path = HERE / "fixtures.json"
        self.fixtures = json.loads(fixtures_path.read_text()) if fixtures_path.exists() else {}
        self.manifest = {
            "mode": args.mode, "started": time.time(), "complete": False,
            "runner_sha256": sha(__file__), "build_json_sha256": sha(HERE / "build.json"),
            "builds": self.builds, "storage": filesystem,
            "database_directory": str(DATABASES), "io": "O_DIRECT",
            "fixture_copy": "cp --reflink=auto --sparse=always, then fsync",
            "cpu_affinity": args.cpus, "platform": platform.platform(),
            "available_cpu_affinity": sorted(os.sched_getaffinity(0)),
            "loadavg_start": os.getloadavg(), "environment": {
                "PREFIX_PATTERN": "per case", "PREFIX_DISABLE": "unset except candidate disabled case",
                "KVBENCH_STATS_BEFORE_CLOSE": "1 for fixture creation only"},
            "coordination": "Caller holds exclusive campaign benchmark and machine locks",
            "seconds": args.seconds, "warmup_seconds": 1, "seed": 42,
            "loading_threads": 1, "background_workers": 0, "memtable_mib": 4,
            "verification": "Every query verifies key/value or ordered range; overwrite validates up to 4096 measured keys per worker after timing",
            "runs": [],
        }
        self.save()

    def save(self):
        save(self.folder / "manifest.json", self.manifest)

    def invoke(self, folder, label, profile, argv, pattern, disabled=False,
               fixture=False, timeout=600):
        build = self.builds[profile]
        command = ["taskset", "-c", self.args.cpus, build["binary"], *map(str, argv)]
        environment = dict(os.environ)
        environment.pop("PREFIX_DISABLE", None)
        environment.pop("KVBENCH_STATS_BEFORE_CLOSE", None)
        environment["PREFIX_PATTERN"] = pattern
        if disabled:
            environment["PREFIX_DISABLE"] = "1"
        if fixture:
            environment["KVBENCH_STATS_BEFORE_CLOSE"] = "1"
        log = folder / (label + ".log")
        assert not log.exists(), log
        record = {"label": label, "profile": profile, "command": command,
                  "started": time.time(), "log": str(log.relative_to(HERE)),
                  "binary_sha256": build["binary_sha256"],
                  "library_sha256": build["library_sha256"],
                  "source_commit": build["commit"], "pattern": pattern,
                  "compression_disabled": disabled, "verified": False}
        self.manifest["runs"].append(record)
        self.save()
        print(label, flush=True)
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        try:
            with log.open("w") as output:
                process = subprocess.Popen(command, cwd=ROOT, env=environment,
                                           stdout=output, stderr=subprocess.STDOUT,
                                           start_new_session=True)
                try:
                    process.wait(timeout=timeout)
                except BaseException:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            record.update(returncode=process.returncode, finished=time.time(),
                          log_sha256=sha(log),
                          process_cpu_s=after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                          process_read_bytes=512 * (after.ru_inblock - before.ru_inblock),
                          process_write_bytes=512 * (after.ru_oublock - before.ru_oublock))
            assert process.returncode == 0, record
            content = log.read_text()
            assert "FINDING: splinterdb_close" not in content, log
            assert "FATAL:" not in content and "VIOLATION:" not in content, log
            expected = f"prefix-pattern={pattern} key_bytes=64 compression_disabled={'true' if disabled else 'false'}"
            assert expected in content, (log, expected)
            return record
        except BaseException as error:
            record.update(exception=repr(error), finished=time.time())
            raise
        finally:
            self.save()

    def kvargs(self, db, csvpath, profile, regime, wal, index, create=False,
               seconds=None):
        return ["--db", db, "--create" if create else "--open",
                "--n", 200000 if regime == "resident" else 2000000,
                "--cache-mib", 128 if regime == "resident" else 64,
                "--disk-gib", 8, "--log", wal, "--threads", 1 if create else 8,
                "--mt-bg", 0, "--normal-bg", 0, "--memtable-mib", 4,
                "--secs", self.args.seconds if seconds is None else seconds,
                "--warmup", 1, "--csv", csvpath, "--vtag", profile,
                "--stag", regime, "--otag", index, "--rep", index,
                "--seed", 42, "--hang-secs", 60, "--hang-dir", self.folder]

    def fixture(self, profile, regime, pattern, wal, disabled=False):
        key = f"{profile}-{regime}-{pattern}-wal{wal}" + ("-disabled" if disabled else "")
        db = DATABASES / ("fixture-" + key + ".db")
        if key in self.fixtures:
            result = self.fixtures[key]
            assert file_identity(db) == result["identity"], key
            assert result["binary_sha256"] == self.builds[profile]["binary_sha256"], key
            return result
        assert not db.exists(), db
        csvpath = self.fixture_folder / (key + ".csv")
        argv = self.kvargs(db, csvpath, profile, regime, wal, 0, create=True, seconds=1)
        record = self.invoke(self.fixture_folder, "load-" + key, profile,
                             argv + ["load:random@1"], pattern, disabled, fixture=True)
        rows = verify_rows(csvpath, ["load:random@1"], 200000 if regime == "resident" else 2000000)
        record.update(verified=True, errors=0, violations=0, csv=str(csvpath.relative_to(HERE)))
        self.save()
        # Reopened allocator ownership is authoritative; per-page-type counters
        # are not persisted and must not be interpreted after reopening.
        space = self.invoke(self.fixture_folder, "space-" + key, profile,
                            self.kvargs(db, csvpath, profile, regime, wal, 0) + ["space"],
                            pattern, disabled)
        matches = re.findall(r"Currently Allocated:\s+(\d+) extents",
                             (HERE / space["log"]).read_text())
        assert matches, space
        space.update(verified=True, live_extents=int(matches[-1]))
        sync(db)
        result = {"key": key, "profile": profile, "regime": regime,
                  "pattern": pattern, "wal": wal, "disabled": disabled,
                  "path": str(db), "identity": file_identity(db), "sha256": sha(db),
                  "live_extents": int(matches[-1]), "load": rows[0],
                  "binary_sha256": self.builds[profile]["binary_sha256"],
                  "load_log": record["log"], "space_log": space["log"]}
        self.fixtures[key] = result
        save(HERE / "fixtures.json", self.fixtures)
        self.save()
        return result

    def query(self, case, profile, index, phases, wal=0):
        regime, pattern, *tail = case.split("-")
        disabled = bool(tail) and profile == "candidate"
        fixture = self.fixture(profile, regime, pattern, wal, disabled)
        source = Path(fixture["path"])
        before = file_identity(source)
        label = f"{case}-wal{wal}-{index:02d}-{profile}"
        db = DATABASES / (f"{self.args.mode}-{label}.db")
        assert not db.exists(), db
        copy_start = time.time()
        subprocess.run(["cp", "--reflink=auto", "--sparse=always", str(source), str(db)], check=True)
        sync(db)
        copy_seconds = time.time() - copy_start
        csvpath = self.folder / (label + ".csv")
        argv = self.kvargs(db, csvpath, profile, regime, wal, index) + phases
        record = self.invoke(self.folder, label, profile, argv, pattern, disabled)
        rows = verify_rows(csvpath, phases, 200000 if regime == "resident" else 2000000)
        assert file_identity(source) == before, source
        record.update(case=case, wal=wal, index=index, fixture_key=fixture["key"],
                      fixture_sha256=fixture["sha256"], copy_fsynced=True,
                      copy_seconds=copy_seconds, csv=str(csvpath.relative_to(HERE)),
                      csv_sha256=sha(csvpath), allocated_bytes=db.stat().st_blocks * 512,
                      errors=0, violations=0, verified=True,
                      phase_count=len(rows))
        self.save()
        db.unlink()  # Only this verified run's disposable copy is removed.

    def run(self):
        if self.args.mode == "writes":
            cases = ["resident-common", "resident-random"]
            for wal in (0, 1):
                for case in cases:
                    for index, profile in enumerate(("main", "candidate", "candidate", "main")):
                        self.query(case, profile, index, ["warm", "ow:uniform@8"], wal)
        else:
            cases = self.args.case or (CASES if self.args.mode == "reads" else
                                      ["resident-common", "resident-random", "resident-common-disabled"])
            order = ["main", "candidate", "candidate", "main"]
            if self.args.mode == "repeat":
                order = [profile for pair in range(self.args.pairs)
                         for profile in (("main", "candidate") if pair % 2 == 0 else ("candidate", "main"))]
            for case in cases:
                phases = ["warm", "point:uniform@8", "range:uniform:1@8", "range:uniform:10000@8"]
                if self.args.mode == "repeat":
                    phases = ["warm", "range:uniform:10000@8", "range:uniform:10000@1"]
                elif case in ("resident-random", "resident-common-disabled"):
                    phases.append("range:uniform:10000@1")
                for index, profile in enumerate(order):
                    self.query(case, profile, index, phases)
        self.manifest.update(complete=True, finished=time.time(), loadavg_end=os.getloadavg())
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("reads", "writes", "repeat"), required=True)
    parser.add_argument("--case", action="append", choices=CASES,
                        help="Restrict read/repeat cases; default reads cover the full matrix")
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--pairs", type=int, default=3, help="Paired repeats per selected case")
    parser.add_argument("--cpus", default="0-11")
    args = parser.parse_args()
    if args.seconds is None:
        args.seconds = {"reads": 7, "writes": 8, "repeat": 20}[args.mode]
    assert args.seconds > 0 and args.pairs > 0
    Campaign(args).run()


if __name__ == "__main__":
    main()
