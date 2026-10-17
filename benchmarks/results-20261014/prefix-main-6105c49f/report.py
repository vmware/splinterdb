#!/usr/bin/env python3
"""Summarize verified prefix benchmark records; never combine different modes."""

import csv
import json
from pathlib import Path
import statistics


HERE = Path(__file__).resolve().parent


def average(rows, key):
    return statistics.mean(float(row[key]) for row in rows)


def change(before, after):
    return 100 * (after / before - 1) if before else None


def percentage(value):
    return "n/a" if value is None else f"{value:+.2f}%"


def interval(values):
    return f"{min(values):,.1f}–{max(values):,.1f}"


def main():
    summaries = []
    manifests = {}
    for mode in ("reads", "writes", "repeat"):
        manifest_path = HERE / mode / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text())
        manifests[mode] = manifest
        groups = {}
        for record in manifest["runs"]:
            if "case" not in record or not record.get("verified"):
                continue
            with (HERE / record["csv"]).open() as stream:
                for row in csv.DictReader(stream):
                    if row["phase"] == "warm":
                        continue
                    assert int(row["errors"]) == 0 and int(row["violations"]) == 0
                    row["ops_with_drain_s"] = float(row["ops"]) / (float(row["secs"]) + float(row["drain_s"]))
                    key = (record["case"], record["wal"], row["phase"])
                    groups.setdefault(key, {}).setdefault(record["profile"], []).append(row)
        for (case, wal, phase), versions in groups.items():
            if set(versions) != {"main", "candidate"}:
                continue
            baseline, candidate = versions["main"], versions["candidate"]
            summary = {"mode": mode, "case": case, "wal": wal, "phase": phase,
                       "main_samples": len(baseline), "candidate_samples": len(candidate)}
            for metric in ("ops_s", "p99_us", "p999_us", "ops_per_cpu_s", "rd_B_op", "wr_B_op", "ops_with_drain_s"):
                a, b = average(baseline, metric), average(candidate, metric)
                summary.update({f"main_{metric}": a, f"candidate_{metric}": b,
                                f"change_{metric}_pct": change(a, b),
                                f"main_{metric}_min": min(float(row[metric]) for row in baseline),
                                f"main_{metric}_max": max(float(row[metric]) for row in baseline),
                                f"candidate_{metric}_min": min(float(row[metric]) for row in candidate),
                                f"candidate_{metric}_max": max(float(row[metric]) for row in candidate)})
            summary["throughput_ranges_overlap"] = max(summary["main_ops_s_min"], summary["candidate_ops_s_min"]) <= min(summary["main_ops_s_max"], summary["candidate_ops_s_max"])
            summaries.append(summary)
    if not manifests:
        raise SystemExit("No benchmark manifests found")
    (HERE / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
    if summaries:
        with (HERE / "summary.csv").open("w") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(summaries[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(summaries)
    build = json.loads((HERE / "build.json").read_text())
    lines = ["# Prefix benchmark comparison against main 6105c49f", "",
             f"Main source: `{build['main']['commit']}`. Candidate source: `{build['candidate']['commit']}`.", "",
             "This compares the integrated prefix feature with current main; it does not isolate the cost of merging main.", "",
             "CPU affinity 0–11, real disk with O_DIRECT, 64-byte keys, 100-byte values, fixed per-format images, fsynced copies, seed 42, one load thread, no background workers, and a 4 MiB memtable. Resident uses 200,000 keys and 128 MiB cache; disk uses 2 million keys and 64 MiB cache. Read phases have one second of warmup each. Query checks validate values and ordered ranges; overwrite checks up to 4,096 measured keys per worker after timing.", "",
             "ABBA provides two samples per version in the main matrix. Repeats are shown separately. Means and sample ranges are descriptive; overlapping ranges are not confidence intervals. Small differences and tail estimates need cautious interpretation.", ""]
    for mode, manifest in manifests.items():
        verified = [record for record in manifest["runs"] if "case" in record and record.get("verified")]
        lines += [f"## {mode.capitalize()}", "",
                  f"Status: {'complete' if manifest['complete'] else 'INCOMPLETE'}. {len(verified)} verified query invocations; measured phases {manifest['seconds']:g} seconds. All included rows have zero API errors and correctness violations.", ""]
        lines += ["| Case / WAL | Phase | Main ops/s range | Candidate ops/s range | Mean throughput | CPU efficiency | Read B/op | Mean p99 | Ranges overlap |",
                  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |"]
        for item in summaries:
            if item["mode"] != mode:
                continue
            a = [item["main_ops_s_min"], item["main_ops_s_max"]]
            b = [item["candidate_ops_s_min"], item["candidate_ops_s_max"]]
            lines.append(f"| {item['case']} / {item['wal']} | {item['phase']} | {interval(a)} | {interval(b)} | {percentage(item['change_ops_s_pct'])} | {percentage(item['change_ops_per_cpu_s_pct'])} | {percentage(item['change_rd_B_op_pct'])} | {percentage(item['change_p99_us_pct'])} | {'yes' if item['throughput_ranges_overlap'] else 'no'} |")
        lines.append("")
        if any(item["mode"] == mode and item["phase"].startswith("ow:") for item in summaries):
            lines += ["Overwrite rates including close/drain:", "",
                      "| Case / WAL | Main ops/s | Candidate ops/s | Change |",
                      "| --- | ---: | ---: | ---: |"]
            for item in summaries:
                if item["mode"] == mode and item["phase"].startswith("ow:"):
                    lines.append(f"| {item['case']} / {item['wal']} | {item['main_ops_with_drain_s']:,.1f} | {item['candidate_ops_with_drain_s']:,.1f} | {percentage(item['change_ops_with_drain_s_pct'])} |")
            lines += ["", "These bounded overwrites include one second of unmeasured warmup and post-measurement verification before close. The close-inclusive denominator adds measured time and close/drain time; it excludes warmup and verifier time. It is not a sustained-ingest or steady-state write-throughput measurement. WAL=1 exercises current main's automatic durable barriers; this harness does not measure recovery or count barriers.", ""]
    fixture_path = HERE / "fixtures.json"
    if fixture_path.exists():
        fixtures = json.loads(fixture_path.read_text())
        lines += ["## Fixture construction and space", "",
                  "Each fixture is constructed once. Load/drain numbers are setup observations, not repeated write benchmarks. Allocated file bytes include historical written pages; mounted live extents show allocator ownership. Per-page-type counters are not durable and are not used here.", "",
                  "| Fixture | Load + drain seconds | File allocation bytes | Live extents |",
                  "| --- | ---: | ---: | ---: |"]
        for key, fixture in fixtures.items():
            lines.append(f"| {key} | {float(fixture['load']['total_s']):.3f} | {fixture['identity']['allocated_bytes']:,} | {fixture['live_extents']:,} |")
        lines.append("")
    lines += ["Raw per-phase samples and latency percentiles are in the mode CSV files. `summary.csv` retains mean and min/max throughput, CPU efficiency, read/write bytes per operation and p99/p99.9. Manifests retain commands, environment controls, timings, source and binary hashes, fixture hashes, and verification outcomes. Database images and binaries are excluded from the evidence archive.", ""]
    (HERE / "report.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
