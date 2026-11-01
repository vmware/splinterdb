#!/usr/bin/env python3
"""Paired small-key timings. Positive time changes mean slower."""
import argparse
import csv
import json
import statistics
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("manifest", type=Path)
p.add_argument("--candidate", required=True)
a = p.parse_args()
m = json.loads(a.manifest.read_text())
groups = {}
for run in m["runs"]:
    if run["case"].startswith("public-"):
        for row in run["rows"]:
            case = run["case"] + "-" + row["phase"]
            groups.setdefault(case, {}).setdefault(run["variant"], {})[
                run["repetition"]] = 1 / float(row["ops_s"])
    else:
        groups.setdefault(run["case"], {}).setdefault(run["variant"], {})[
            run["repetition"]] = run["rows"][0]["active_wall_ns"]
out = a.manifest.with_name("comparison.csv")
with out.open("w") as f:
    w = csv.writer(f)
    w.writerow(["case", "paired_repetitions", "median_time_change_pct",
                "minimum_time_change_pct", "maximum_time_change_pct",
                "median_throughput_change_pct", "baseline_median_time",
                "candidate_median_time"])
    for case, variants in groups.items():
        b, c = variants["baseline"], variants[a.candidate]
        pairs = sorted(b.keys() & c.keys())
        ratios = [c[i] / b[i] for i in pairs]
        med = statistics.median(ratios)
        w.writerow([case, len(pairs), 100*(med-1), 100*(min(ratios)-1),
                    100*(max(ratios)-1), 100*(1/med-1),
                    statistics.median(b.values()), statistics.median(c.values())])
        print(f"{case:38s} time {100*(med-1):+6.1f}%  "
              f"range {100*(min(ratios)-1):+6.1f}..{100*(max(ratios)-1):+6.1f}%")
print(out)
