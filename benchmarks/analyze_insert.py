#!/usr/bin/env python3
# Copyright 2026 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Summarize independently sampled throughput and optional internal traces."""

import argparse
import bisect
import collections
import csv
import json
import re
import statistics
from pathlib import Path


def read_samples(path):
    with path.open() as stream:
        return [{k: float(v) for k, v in row.items()} for row in csv.DictReader(stream)]


def read_trace(path, log, symbols):
    metadata = log.read_text()
    start = int(re.search(r"start_monotonic_ns=(\d+)", metadata)[1])
    workers = {
        int(tid): (int(index), kind)
        for index, kind, tid in re.findall(r"worker=(\d+) kind=(\w+) tid=(\d+)", metadata)
    }
    names, addresses = [], []
    base = 0
    if symbols:
        for line in symbols.read_text().splitlines():
            match = re.fullmatch(r"([0-9a-fA-F]+) [tT] (.+)", line)
            if match:
                addresses.append(int(match[1], 16))
                names.append(match[2])
        main = addresses[names.index("main")]
        base = int(re.search(r"main_address=(0x[0-9a-fA-F]+)", metadata)[1], 16) - main
    events = []
    with path.open() as stream:
        for row in csv.DictReader(stream):
            event = dict(row)
            event["start_s"] = (int(row["start_ns"]) - start) / 1e9
            event["duration_s"] = int(row["duration_ns"]) / 1e9
            event["end_s"] = event["start_s"] + event["duration_s"]
            event["worker"] = workers.get(int(row["tid"]), (-1, "background"))
            if row["event"] in ("task_fg", "task_bg"):
                address = int(row["arg1"]) - base
                index = bisect.bisect_right(addresses, address) - 1
                event["function"] = names[index] if index >= 0 else hex(address)
            events.append(event)
    return events


def overlap(event, row):
    return max(0, min(event["end_s"], row["elapsed_s"])
               - max(event["start_s"], row["elapsed_s"] - row["interval_s"]))


def analyze(rows, events):
    columns = [k for k in rows[0] if re.fullmatch(r"i\d+_ops_s", k)]
    if "inserters_running" in rows[0]:
        tail = next((i for i, r in enumerate(rows)
                     if r["inserters_running"] < len(columns)), len(rows))
    else:
        # Compatibility with the initial baseline, before this column existed.
        tail = min(max(i for i, row in enumerate(rows) if row[k] > 0) for k in columns)
    dips = []
    for i, row in enumerate(rows[:tail]):
        if row["elapsed_s"] - row["interval_s"] < 5:
            continue
        neighbors = [r["insert_ops_s"] for j, r in enumerate(rows[max(0, i-5):min(tail, i+6)], max(0, i-5)) if j != i]
        reference = statistics.median(neighbors)
        if row["insert_ops_s"] >= reference / 2:
            continue
        dip = {"start_s": row["elapsed_s"] - row["interval_s"],
               "end_s": row["elapsed_s"], "insert_ops_s": row["insert_ops_s"],
               "query_ops_s": row["query_ops_s"], "neighbor_median_ops_s": reference,
               "drop_pct": 100 * (1 - row["insert_ops_s"] / reference)}
        spans = collections.defaultdict(float)
        for event in events:
            amount = overlap(event, row)
            if amount:
                label = event["event"]
                if event["worker"][1] == "insert":
                    if label == "task_fg":
                        label += ":" + event["function"]
                    spans[label] += amount
                elif label in ("trunk_prepare", "leaf_pivots"):
                    spans[label + ":background"] += amount
        dip["overlapping_thread_seconds"] = dict(sorted(spans.items(), key=lambda x: -x[1]))
        dips.append(dip)
    durations = collections.defaultdict(list)
    heights = collections.Counter()
    for event in events:
        if event["start_s"] < 0 or event["start_s"] >= rows[-1]["elapsed_s"]:
            continue
        name = event["event"]
        if "function" in event:
            name += ":" + event["function"]
        durations[name].append(event["duration_s"])
        if event["event"] == "leaf_pivots":
            heights[event["arg0"]] += 1
    return {
        "samples": len(rows), "inserted": int(rows[-1]["inserted"]),
        "logical_gib": rows[-1]["logical_gib"], "elapsed_s": rows[-1]["elapsed_s"],
        "completion_tail_start_s": rows[tail]["elapsed_s"] - rows[tail]["interval_s"] if tail < len(rows) else None,
        "dip_rule": "Below half the median of up to 5 neighboring samples on each side; excludes first 5 seconds and worker completion tail",
        "dips": dips, "leaf_pivot_heights": dict(heights),
        "trace_summary": {name: {"count": len(ds), "total_thread_s": sum(ds), "max_s": max(ds)}
                          for name, ds in sorted(durations.items())},
    }


def plot(rows, events, report, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    x = [row["elapsed_s"] for row in rows]
    fig, axes = plt.subplots(4 if events else 3, 1, figsize=(13, 10), sharex=True,
                             layout="constrained")
    inserters = sum(bool(re.fullmatch(r"i\d+_ops_s", k)) for k in rows[0])
    readers = sum(bool(re.fullmatch(r"q\d+_ops_s", k)) for k in rows[0])
    fig.suptitle(f"Random inserts + random point queries | {inserters} inserters, {readers} readers")
    axes[0].plot(x, [r["insert_ops_s"] / 1e6 for r in rows], label="Inserts", color="#1565c0")
    axes[0].plot(x, [r["query_ops_s"] / 1e6 for r in rows], label="Point queries", color="#e67700")
    axes[0].set_ylabel("Million operations/s")
    for dip in report["dips"]:
        axes[0].axvspan(dip["start_s"], dip["end_s"], color="#c62828", alpha=.18)
    tail = report["completion_tail_start_s"]
    if tail is not None:
        axes[0].axvspan(tail, x[-1], color="grey", alpha=.2, label="Completion tail")
    axes[0].legend(loc="upper right")
    for k, label in [("insert_max_ms", "Completed insert max"), ("active_max_ms", "Oldest active operation")]:
        axes[1].plot(x, [max(.001, r[k]) for r in rows], label=label)
    axes[1].set_yscale("log")
    axes[1].set_ylabel("Latency / age (ms)")
    axes[1].legend(loc="upper right")
    axes[2].plot(x, [r["cpu_cores"] for r in rows], color="#00897b", label="CPU cores")
    axes[2].set_ylabel("CPU cores")
    io_axis = axes[2].twinx()
    io_axis.plot(x, [r["write_mib_s"] for r in rows], color="#8e24aa", alpha=.6, label="Storage writes")
    io_axis.set_ylabel("Write MiB/s", color="#8e24aa")
    if events:
        for kind, label in [("task_fg", "Foreground tasks"), ("memtable_wait", "Memtable wait (includes tasks)")]:
            selected = [e for e in events if e["event"] == kind and e["worker"][1] == "insert"]
            values = [sum(overlap(e, row) for e in selected) / row["interval_s"] for row in rows]
            axes[3].plot(x, values, label=label)
        axes[3].set_ylabel("Inserter threads occupied")
        axes[3].set_ylim(0, inserters * 1.05)
        axes[3].legend(loc="upper right")
    for ax in axes:
        ax.grid(alpha=.2)
    axes[-1].set_xlabel("Seconds since insertion/query workers started")
    fig.savefig(path, dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--trace", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--symbols", type=Path, help="nm -an output from the exact benchmark binary")
    parser.add_argument("--plot", type=Path, help="PNG/SVG output; requires matplotlib")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    if args.trace and not args.log:
        parser.error("--trace requires --log")
    rows = read_samples(args.csv)
    if not rows:
        parser.error("CSV contains no samples")
    events = read_trace(args.trace, args.log, args.symbols) if args.trace else []
    report = analyze(rows, events)
    output = json.dumps(report, indent=2)
    if args.json:
        args.json.write_text(output + "\n")
    print(output)
    if args.plot:
        plot(rows, events, report, args.plot)


if __name__ == "__main__":
    main()
