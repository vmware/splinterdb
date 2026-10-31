#!/usr/bin/env python3
"""Summarize complete run.py output without hiding repetitions or cache I/O."""
import argparse
import csv
import json
from pathlib import Path
from statistics import median

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.results / "manifest.json").read_text())
    if not manifest.get("completed"):
        raise SystemExit("Refusing to summarize an incomplete campaign")
    groups = {}
    for run in manifest["runs"]:
        if run["returncode"]:
            raise SystemExit("Failed run: " + run["name"])
        rows = [json.loads(line) for line in
                (args.results / (run["name"] + ".jsonl")).read_text().splitlines()
                if line.startswith("{")]
        if not any(r.get("validation") == "passed" for r in rows):
            raise SystemExit("Missing validation: " + run["name"])
        for row in rows:
            if "stage" not in row:
                continue
            key = (row["key_bytes"], run["pattern"], row["mode"],
                   row["prefix_bytes"], row["stage"])
            groups.setdefault(key, []).append(row)
    summary = []
    for key, rows in sorted(groups.items()):
        size, pattern, mode, prefix, stage = key
        row = dict(key_bytes=size, pattern=pattern, mode=mode,
                   prefix_bytes=prefix, stage=stage, repetitions=len(rows),
                   records=rows[0]["records"], operations=rows[0]["operations"])
        for metric in ("seconds", "ops_per_sec", "read_bytes", "write_bytes",
                       "allocated_bytes", "comparisons", "prefix_resolved",
                       "blob_fallbacks", "blob_fragments", "blob_bytes", "hash_calls"):
            row["median_" + metric] = median(r[metric] for r in rows)
        row["min_seconds"] = min(r["seconds"] for r in rows)
        row["max_seconds"] = max(r["seconds"] for r in rows)
        summary.append(row)
    with open(args.results / "summary.csv", "w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    selected = ("insert", "pack", "branch_cold_lookup",
                "branch_warm_lookup", "branch_scan")
    lines = [
        "# Blob-key measurements",
        "",
        "Medians across repetitions. Lookup batches contain 50% hits and 50% "
        "misses. Cold-start begins with an empty Splinter cache; repeated is the "
        "next identical pass, whose measured I/O is retained below. Scans decode "
        "and verify every logical key. These are B-tree prototype measurements.",
        "",
    ]
    titles = {"insert": "Insert including key construction",
              "pack": "Pack including clone and logical-key hashing",
              "branch_cold_lookup": "Lookup from a cold cache",
              "branch_warm_lookup": "Repeated lookup",
              "branch_scan": "Scan including full key reconstruction"}
    for stage in selected:
        lines += ["## " + titles[stage], "",
                  "| Key bytes | Difference | Mode/prefix | us/op | Read MiB | Allocated MiB |",
                  "|---:|:---|:---|---:|---:|---:|"]
        for row in summary:
            if row["stage"] != stage:
                continue
            us = row["median_seconds"] * 1e6 / row["operations"]
            lines.append(f'| {row["key_bytes"]} | {row["pattern"]} | '
                         f'{row["mode"]}/{row["prefix_bytes"]} | {us:.3f} | '
                         f'{row["median_read_bytes"] / 2**20:.3f} | '
                         f'{row["median_allocated_bytes"] / 2**20:.3f} |')
        lines.append("")
    (args.results / "summary.md").write_text("\n".join(lines))
    print(args.results / "summary.csv")
    print(args.results / "summary.md")

if __name__ == "__main__":
    main()
