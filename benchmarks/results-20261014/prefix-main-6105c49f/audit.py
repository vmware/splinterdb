#!/usr/bin/env python3
"""Check this campaign's source, artifacts, outcomes, and serialization."""

import csv
import hashlib
import json
from pathlib import Path
import re
import subprocess


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    builds = json.loads((HERE / "build.json").read_text())
    sizes = {}
    for profile, build in builds.items():
        for field in ("binary", "library"):
            assert sha(build[field]) == build[field + "_sha256"], (profile, field)
        assert sha(Path(build["binary"]).with_suffix(".c")) == build["source_sha256"]
        for name, expected in build["source_files_sha256"].items():
            assert sha(Path(build["tree"]) / name) == expected, (profile, name)
            committed = subprocess.check_output(
                ["git", "show", build["commit"] + ":" + name], cwd=ROOT)
            assert hashlib.sha256(committed).hexdigest() == expected, (profile, name)
        command = ["gdb", "-nx", "-batch", "-ex", "p sizeof(btree_iterator)",
                   "-ex", "p sizeof(core_range_iterator)",
                   str(Path(build["library"]).with_suffix(".so"))]
        output = subprocess.check_output(command, text=True)
        values = [int(value) for value in re.findall(r"\$\d+ = (\d+)", output)]
        assert len(values) == 2
        sizes[profile] = dict(command=command, btree_iterator=values[0],
                              core_range_iterator=values[1], output=output)
    assert builds["main"]["source_sha256"] == builds["candidate"]["source_sha256"]

    runs = []
    query_count = 0
    phase_count = 0
    for mode in ("reads", "writes", "repeat"):
        manifest = json.loads((HERE / mode / "manifest.json").read_text())
        assert manifest["complete"]
        assert sha(HERE / "run_benchmarks.py") == manifest["runner_sha256"]
        assert sha(HERE / "build.json") == manifest["build_json_sha256"]
        if mode == "repeat":
            assert sha(HERE / "followup.py") == manifest["followup_sha256"]
        for run in manifest["runs"]:
            assert run["returncode"] == 0 and run["verified"], run["label"]
            assert sha(HERE / run["log"]) == run["log_sha256"], run["label"]
            if "csv" in run:
                csvpath = HERE / run["csv"]
                if "csv_sha256" in run:
                    assert sha(csvpath) == run["csv_sha256"], run["label"]
                with csvpath.open() as stream:
                    rows = list(csv.DictReader(stream))
                assert all(int(row["ops"]) > 0 and int(row["errors"]) == 0
                           and int(row["violations"]) == 0 for row in rows)
                phase_count += len(rows)
            query_count += "case" in run
            runs.append(run)
    ordered = sorted(runs, key=lambda run: run["started"])
    assert all(a["finished"] <= b["started"] for a, b in zip(ordered, ordered[1:]))

    tests = {}
    for mode in ("debug", "release"):
        records = json.loads((HERE / "tests" / (mode + "-results.json")).read_text())
        count = 0
        for record in records:
            assert record["returncode"] == 0
            assert sha(HERE / "tests" / record["log"]) == record["log_sha256"]
            assert sha(record["command"][0]) == record["binary_sha256"]
            assert record["finished"] < ordered[0]["started"]
            if record["name"] != "functional-log":
                assert len(record["summaries"]) == 1
                count += int(re.search(r"RESULTS: (\d+) tests", record["summaries"][0])[1])
            else:
                assert "log returned 500000 of 500000 entries" in (
                    HERE / "tests" / record["log"]).read_text()
        assert count == 141
        assert json.loads((HERE / "tests" / (mode + "-source-check.json")).read_text())["unchanged"]
        tests[mode] = dict(unit_cases=count, functional_log_passed=True)

    result = dict(source_and_binary_hashes_match=True, identical_harness=True,
                  benchmark_subprocesses=len(runs), query_invocations=query_count,
                  phase_rows=phase_count, errors=0, violations=0,
                  benchmark_intervals_do_not_overlap=True, tests_before_benchmarks=True,
                  tests=tests, iterator_object_sizes=sizes)
    (HERE / "audit.json").write_text(json.dumps(result, indent=2) + "\n")
    archive = {str(path.relative_to(HERE)): sha(path) for path in sorted(HERE.rglob("*"))
               if path.is_file() and "__pycache__" not in path.parts
               and path.name != "sha256.json"}
    (HERE / "sha256.json").write_text(json.dumps(archive, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
