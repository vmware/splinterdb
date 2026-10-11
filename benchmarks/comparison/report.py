#!/usr/bin/env python3
"""Generate a paired Markdown/HTML report and standalone SVG graphs."""

from __future__ import annotations

import argparse
import collections
import csv
import html
import json
import math
import statistics
from pathlib import Path


HIGHER_IS_BETTER = {"ops_s", "ops_per_cpu_s", "tuples_s", "achieved_insert_ops_s",
                    "mean_insert_ops_s", "median_interval_ops_s", "min_interval_ops_s"}


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def median(values):
    values = [v for v in values if v is not None and math.isfinite(v)]
    return statistics.median(values) if values else None


def fmt(value):
    if value is None:
        return "-"
    if abs(value) >= 1e6:
        return f"{value / 1e6:.2f}M"
    if abs(value) >= 1e3:
        return f"{value / 1e3:.1f}k"
    if abs(value) >= 100:
        return f"{value:.0f}"
    return f"{value:.2f}"


def read_csv(path: Path):
    if not path.exists():
        return []
    with path.open() as stream:
        return list(csv.DictReader(stream))


def paired_summary(rows, metric):
    by_version = collections.defaultdict(dict)
    for row in rows:
        value = number(row.get(metric))
        if value is not None:
            by_version[row["version"]][row["rep"]] = value
    if "head" not in by_version or "main" not in by_version:
        return None
    pairs = []
    for rep in sorted(set(by_version["head"]) & set(by_version["main"])):
        head, main = by_version["head"][rep], by_version["main"][rep]
        if main == 0:
            continue
        raw = (head - main) / main * 100
        improvement = raw if metric in HIGHER_IS_BETTER else -raw
        pairs.append(improvement)
    if not pairs:
        return None
    h = median(by_version["head"].values())
    m = median(by_version["main"].values())
    raw = (h - m) / m * 100
    improvement = raw if metric in HIGHER_IS_BETTER else -raw
    if len(pairs) >= 2 and improvement <= -10 and all(v <= -5 for v in pairs):
        flag = "major regression"
    elif len(pairs) >= 2 and improvement <= -5 and all(v <= -2 for v in pairs):
        flag = "regression"
    elif len(pairs) >= 2 and improvement >= 10 and all(v >= 5 for v in pairs):
        flag = "major improvement"
    elif len(pairs) == 1 and improvement <= -10:
        flag = "preliminary regression"
    elif len(pairs) == 1 and improvement >= 10:
        flag = "preliminary improvement"
    else:
        flag = ""
    return {"head": h, "main": m, "improvement_pct": improvement,
            "pairs": pairs, "flag": flag, "n": len(pairs)}


def core_results(path: Path):
    rows = read_csv(path)
    groups = collections.defaultdict(list)
    for row in rows:
        key = (row["size"], row["order"], row["log"], row["phase"], row["threads"])
        groups[key].append(row)
    results = []
    for key, values in groups.items():
        phase = key[3]
        # A large-cell `warm` invocation is the standalone cold full-scan
        # verifier. Small cells run warm scans as setup for every read phase,
        # so those repeated setup rows are not comparison results.
        if phase == "warm" and key[0] != "large":
            continue
        shown_phase = "full-scan verify" if phase == "warm" else phase
        metric = "ops_s"
        summary = paired_summary(values, metric)
        if summary:
            diagnostics = {}
            for diagnostic_metric in ("ops_per_cpu_s", "rd_B_op", "wr_B_op"):
                diagnostic = paired_summary(values, diagnostic_metric)
                if diagnostic:
                    diagnostics[diagnostic_metric] = diagnostic
            results.append({"case": f"{key[0]} {key[1]} log{key[2]} {shown_phase} T={key[4]}",
                            "metric": metric, "diagnostics": diagnostics, **summary})
        if phase.startswith(("load", "point", "ow")):
            latency = paired_summary(values, "p99_us")
            if latency:
                results.append({"case": f"{key[0]} {key[1]} log{key[2]} {phase} T={key[4]} p99",
                                "metric": "p99_us", **latency})
        if phase.startswith("range"):
            startup = paired_summary(values, "startup_p99_us")
            if startup:
                results.append({"case": f"{key[0]} {key[1]} log{key[2]} {phase} T={key[4]} startup p99",
                                "metric": "startup_p99_us", **startup})
    integrity = []
    for row in rows:
        if (number(row.get("violations")) or 0) or (number(row.get("errors")) or 0):
            integrity.append(row)
    return rows, sorted(results, key=lambda r: r["improvement_pct"]), integrity


def load_manifest(out: Path):
    path = out / "results" / "runs.jsonl"
    if not path.exists():
        return []
    records = []
    for line in path.read_text().splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return records


def thermal_summary(out: Path, manifest):
    samples = read_csv(out / "results" / "thermal.csv")
    parsed = []
    for row in samples:
        epoch = number(row.get("epoch"))
        mhz = number(row.get("mean_cpu_mhz"))
        temp = number(row.get("max_temp_c"))
        if epoch is not None:
            parsed.append((epoch, mhz, temp))
    if not parsed:
        return None

    run_values = {}
    for record in manifest:
        if record.get("kind") != "core" or record.get("status") != "ok":
            continue
        start = number(record.get("started_epoch"))
        end = number(record.get("ended_epoch"))
        if start is None or end is None:
            continue
        mhz = [sample_mhz for epoch, sample_mhz, _ in parsed
               if start <= epoch <= end and sample_mhz is not None]
        if mhz:
            key = (record.get("size"), record.get("order"), record.get("log"),
                   record.get("rep"), record.get("label"), record.get("version"))
            run_values[key] = median(mhz)

    paired_clock_changes = []
    bases = {key[:-1] for key in run_values}
    for base in bases:
        head = run_values.get((*base, "head"))
        main = run_values.get((*base, "main"))
        if head is not None and main:
            paired_clock_changes.append((head - main) / main * 100)

    all_mhz = [mhz for _, mhz, _ in parsed if mhz is not None]
    all_temp = [temp for _, _, temp in parsed if temp is not None]
    return {
        "samples": len(parsed),
        "median_mhz": median(all_mhz),
        "min_mhz": min(all_mhz) if all_mhz else None,
        "max_mhz": max(all_mhz) if all_mhz else None,
        "max_temp_c": max(all_temp) if all_temp else None,
        "paired_clock_change_pct": median(paired_clock_changes),
        "paired_core_runs": len(paired_clock_changes),
    }


def stall_metrics(out: Path, record):
    rows = read_csv(out / record["series"])
    if not rows:
        return None
    inserter_cols = [key for key in rows[0] if key.startswith("i") and key.endswith("_ops_s")]
    full = [r for r in rows if int(float(r.get("inserters_running", 0))) == record["inserters"]]
    stable = [r for r in full if float(r["elapsed_s"]) > 5]
    if not stable:
        stable = full
    rates = [float(r["insert_ops_s"]) for r in stable]
    reference = median(rates) or 0
    total_s = sum(float(r["interval_s"]) for r in full)
    completed = sum(float(r["insert_ops_s"]) * float(r["interval_s"]) for r in full)
    stall_threshold = reference * .05
    stall_s = sum(float(r["interval_s"]) for r in stable
                  if float(r["insert_ops_s"]) < stall_threshold)
    zero_s = sum(float(r["interval_s"]) for r in stable
                 if inserter_cols and all(float(r[c]) < 1 for c in inserter_cols))
    return {"version": record["version"], "rep": record["rep"],
            "mean_insert_ops_s": completed / total_s if total_s else 0,
            "median_interval_ops_s": reference, "min_interval_ops_s": min(rates),
            "stall_s": stall_s, "all_inserters_zero_s": zero_s,
            "max_active_ms": max(float(r["active_max_ms"]) for r in stable),
            "rows": rows, "record": record}


def svg_frame(width, height, body, title):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}"><rect width="100%" height="100%" fill="white"/>'
            f'<style>text{{font-family:system-ui,sans-serif;fill:#222}} .grid{{stroke:#ddd;stroke-width:1}}'
            f'.axis{{stroke:#333;stroke-width:1}} .head{{stroke:#1565c0;fill:none;stroke-width:2}}'
            f'.main{{stroke:#e67700;fill:none;stroke-width:2}}</style>'
            f'<text x="{width/2}" y="24" text-anchor="middle" font-size="17" font-weight="600">'
            f'{html.escape(title)}</text>{body}</svg>')


def bar_svg(results, path: Path):
    selected = [r for r in results if r["metric"] == "ops_s"]
    selected = sorted(selected, key=lambda r: r["improvement_pct"])[:24]
    if not selected:
        return
    width, row_h, left, right = 1200, 27, 430, 70
    height = 60 + row_h * len(selected)
    limit = max(10, min(100, max(abs(r["improvement_pct"]) for r in selected) * 1.1))
    plot_w, zero = width - left - right, left + (width - left - right) / 2
    pieces = [f'<line class="axis" x1="{zero}" x2="{zero}" y1="42" y2="{height-20}"/>']
    for i, row in enumerate(selected):
        y = 52 + i * row_h
        value = max(-limit, min(limit, row["improvement_pct"]))
        x = zero + value / (2 * limit) * plot_w
        color = "#c62828" if value < 0 else "#2e7d32"
        pieces += [f'<text x="{left-8}" y="{y+5}" text-anchor="end" font-size="11">{html.escape(row["case"])}</text>',
                   f'<rect x="{min(x,zero):.1f}" y="{y-8}" width="{abs(x-zero):.1f}" height="16" fill="{color}"/>',
                   f'<text x="{x + (5 if value >= 0 else -5):.1f}" y="{y+5}" text-anchor="{("start" if value >= 0 else "end")}" font-size="11">{value:+.1f}%</text>']
    pieces.append(f'<text x="{zero}" y="{height-3}" text-anchor="middle" font-size="11">branch vs main; positive is faster</text>')
    path.write_text(svg_frame(width, height, "".join(pieces), "Core throughput comparison"))


def line_svg(series, path: Path, field: str, title: str, ylabel: str, log_y=False):
    valid = [(name, rows) for name, rows in series if rows]
    if not valid:
        return
    width, height, left, top, right, bottom = 1100, 440, 75, 42, 25, 55
    xmax = max(float(r["elapsed_s"]) for _, rows in valid for r in rows)
    values = [max(1e-6, float(r[field])) for _, rows in valid for r in rows]
    transform = math.log10 if log_y else (lambda x: x)
    ymin = min(transform(v) for v in values) if log_y else 0
    ymax = max(transform(v) for v in values)
    if ymax <= ymin:
        ymax = ymin + 1
    pw, ph = width-left-right, height-top-bottom
    pieces = []
    for tick in range(6):
        y = top + ph * tick / 5
        value = ymax - (ymax-ymin) * tick / 5
        shown = 10 ** value if log_y else value
        pieces += [f'<line class="grid" x1="{left}" x2="{width-right}" y1="{y}" y2="{y}"/>',
                   f'<text x="{left-8}" y="{y+4}" text-anchor="end" font-size="11">{shown:.2g}</text>']
    colors = {"head": "#1565c0", "main": "#e67700"}
    for name, rows in valid:
        pts = []
        for row in rows:
            x = left + float(row["elapsed_s"]) / xmax * pw
            value = transform(max(1e-6, float(row[field])))
            y = top + (ymax-value)/(ymax-ymin)*ph
            pts.append(f"{x:.1f},{y:.1f}")
        version = "head" if "head" in name else "main"
        pieces.append(f'<polyline points="{" ".join(pts)}" stroke="{colors[version]}" fill="none" stroke-width="1.5" opacity=".9"/>')
    pieces += [f'<line class="axis" x1="{left}" x2="{width-right}" y1="{top+ph}" y2="{top+ph}"/>',
               f'<text x="{width/2}" y="{height-12}" text-anchor="middle" font-size="12">elapsed seconds</text>',
               f'<text transform="translate(16 {height/2}) rotate(-90)" text-anchor="middle" font-size="12">{html.escape(ylabel)}</text>',
               f'<line x1="{width-180}" x2="{width-150}" y1="55" y2="55" stroke="#1565c0" stroke-width="2"/><text x="{width-145}" y="59" font-size="11">branch</text>',
               f'<line x1="{width-95}" x2="{width-65}" y1="55" y2="55" stroke="#e67700" stroke-width="2"/><text x="{width-60}" y="59" font-size="11">main</text>']
    path.write_text(svg_frame(width, height, "".join(pieces), title))


def open_loop_svg(points, path: Path):
    # The target-0 sample is a closed-loop capacity measurement. Its response
    # time has no scheduled-arrival backlog, so connecting it to the paced
    # points would produce a misleading throughput/latency curve.
    points = [point for point in points if point["target"] > 0]
    if not points:
        return
    grouped = collections.defaultdict(list)
    for point in points:
        grouped[(point["version"], point["target"])].append(point)
    points = [{"version": version, "target": target,
               "achieved": median([p["achieved"] for p in values]),
               "p99": median([p["p99"] for p in values])}
              for (version, target), values in grouped.items()]
    width, height, left, top, right, bottom = 900, 500, 80, 45, 30, 60
    xmax = max(p["achieved"] for p in points) / 1e6 * 1.05
    positive = [p["p99"] for p in points if p["p99"] > 0]
    ymin = math.log10(min(positive)) if positive else -3
    ymax = math.log10(max(positive)) if positive else 0
    if ymax-ymin < 1:
        ymax = ymin+1
    pw, ph = width-left-right, height-top-bottom
    pieces = []
    for version, color in (("head", "#1565c0"), ("main", "#e67700")):
        selected = sorted((p for p in points if p["version"] == version), key=lambda p:p["achieved"])
        pts=[]
        for p in selected:
            x=left+(p["achieved"]/1e6)/xmax*pw
            y=top+(ymax-math.log10(max(p["p99"],1e-6)))/(ymax-ymin)*ph
            pts.append(f"{x:.1f},{y:.1f}")
            pieces.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}"><title>target {p["target"]}: p99 {p["p99"]:.3f} ms</title></circle>')
        pieces.append(f'<polyline points="{" ".join(pts)}" stroke="{color}" fill="none" stroke-width="2"/>')
    for tick in range(6):
        y=top+ph*tick/5; lv=ymax-(ymax-ymin)*tick/5
        pieces += [f'<line class="grid" x1="{left}" x2="{width-right}" y1="{y}" y2="{y}"/>',
                   f'<text x="{left-7}" y="{y+4}" text-anchor="end" font-size="11">{10**lv:.3g}</text>']
    pieces += [f'<line class="axis" x1="{left}" x2="{width-right}" y1="{top+ph}" y2="{top+ph}"/>',
               f'<text x="{width/2}" y="{height-14}" text-anchor="middle">achieved insert throughput (million ops/s)</text>',
               f'<text transform="translate(18 {height/2}) rotate(-90)" text-anchor="middle">p99 response latency including backlog (ms, log scale)</text>']
    path.write_text(svg_frame(width,height,"".join(pieces),"Open-loop throughput vs latency"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    out = args.out.resolve()
    results_dir = out / "results"
    graphs = results_dir / "graphs"
    graphs.mkdir(parents=True, exist_ok=True)
    provenance = json.loads((results_dir / "provenance.json").read_text()) if (results_dir / "provenance.json").exists() else {}
    rows, core, integrity = core_results(results_dir / "kvbench.csv")
    manifest = load_manifest(out)
    thermal = thermal_summary(out, manifest)
    exclusions_path = results_dir / "exclusions.json"
    exclusions = (json.loads(exclusions_path.read_text())
                  if exclusions_path.exists() else {})
    excluded = [r for r in manifest if r.get("name") in exclusions]
    failed = [r for r in manifest
              if r.get("status") == "failed" and r.get("name") not in exclusions]
    skipped = [r for r in manifest if r.get("status") == "skipped_deadline"]

    stalls = [stall_metrics(out, r) for r in manifest
              if r.get("kind") == "stall" and r.get("status") == "ok"]
    stalls = [s for s in stalls if s]
    open_points = []
    for r in manifest:
        if r.get("kind") != "open-loop" or r.get("status") != "ok":
            continue
        summary = read_csv(out / r["summary"])
        if summary:
            row = summary[0]
            open_points.append({"version": r["version"], "rep": r["rep"],
                                "target": r["target_insert_ops_s"],
                                "achieved": float(row["achieved_insert_ops_s"]),
                                "p99": float(row["insert_response_p99_ms"]),
                                "p999": float(row["insert_response_p999_ms"])})

    def open_loop_overloaded(target, points):
        if target == 0:
            return False
        for version in ("head", "main"):
            achieved = [p["achieved"] for p in points
                        if p["target"] == target and p["version"] == version]
            if achieved and median(achieved) < 0.99 * target:
                return True
        return False

    bar_svg(core, graphs / "core-throughput.svg")
    representatives = []
    for version in ("head", "main"):
        match = next((s for s in stalls if s["version"] == version and s["rep"] == 1), None)
        if match:
            representatives.append((version, match["rows"]))
    line_svg(representatives, graphs / "stall-throughput.svg", "insert_ops_s",
             "Mixed-workload insertion throughput", "insert ops/s")
    line_svg(representatives, graphs / "stall-latency.svg", "active_max_ms",
             "Oldest in-flight operation during mixed workload", "milliseconds", True)
    open_loop_svg(open_points, graphs / "open-loop.svg")

    report = ["# SplinterDB branch vs main benchmark report", "",
              f"Generated from `{len(rows)}` core phase rows and `{len(manifest)}` recorded runs.", "",
              "Positive percentages mean the audited branch is better: faster for throughput and lower for latency. "
              "A major regression requires at least two paired repetitions, a median loss of at least 10%, "
              "and a loss of at least 5% in every pair. One-pair findings are marked preliminary.", ""]
    if provenance:
        report += ["## Provenance", "",
                   f"- Audited branch: `{provenance.get('head_commit','?')}`",
                   f"- Main: `{provenance.get('main_commit','?')}`",
                   f"- Compiler: {provenance.get('compiler','?')}",
                   f"- Kernel: {provenance.get('kernel','?')}", ""]
        formatting = provenance.get("post_run_formatting")
        if formatting:
            report.insert(len(report) - 1,
                          f"- Post-run source formatting: {formatting.get('note', '?')}.")
    if thermal:
        clock_bias = thermal["paired_clock_change_pct"]
        report += ["## Run environment", "",
                   f"- Mean pinned-CPU clock across the sampler: median {fmt(thermal['median_mhz'])} MHz "
                   f"(observed range {fmt(thermal['min_mhz'])}-{fmt(thermal['max_mhz'])} MHz).",
                   f"- Maximum reported thermal-zone temperature: {fmt(thermal['max_temp_c'])} C.",
                   f"- Median branch-versus-main clock difference within {thermal['paired_core_runs']} paired core phases: "
                   f"{clock_bias:+.1f}%." if clock_bias is not None else
                   "- Paired CPU-clock comparison is not available.", ""]
    flagged = [r for r in core if "regression" in r["flag"]]
    enhanced_flags = []
    stall_fake = [{"version": s["version"], "rep": str(s["rep"]), **s}
                  for s in stalls]
    for metric in ("mean_insert_ops_s", "stall_s", "all_inserters_zero_s",
                   "max_active_ms"):
        comparison = paired_summary(stall_fake, metric)
        if comparison and "regression" in comparison["flag"]:
            enhanced_flags.append(
                f"Mixed-workload `{metric}`: {comparison['improvement_pct']:+.1f}% "
                f"({comparison['flag']}; pairs "
                f"{' '.join(f'{p:+.1f}%' for p in comparison['pairs'])}).")
    for target in sorted(set(p["target"] for p in open_points)):
        selected = [p for p in open_points if p["target"] == target]
        fake = [{"version": p["version"], "rep": str(p["rep"]),
                 "achieved_insert_ops_s": p["achieved"],
                 "response_p99_ms": p["p99"]} for p in selected]
        for metric in ("achieved_insert_ops_s", "response_p99_ms"):
            comparison = paired_summary(fake, metric)
            overloaded = (metric == "response_p99_ms"
                          and open_loop_overloaded(target, open_points))
            if (comparison and not overloaded
                    and "regression" in comparison["flag"]):
                enhanced_flags.append(
                    f"Open-loop target {target} `{metric}`: "
                    f"{comparison['improvement_pct']:+.1f}% "
                    f"({comparison['flag']}; pairs "
                    f"{' '.join(f'{p:+.1f}%' for p in comparison['pairs'])}).")
    major_core = [r for r in core if r["flag"] == "major regression"]
    major_improvements = sorted(
        (r for r in core if r["flag"] == "major improvement"),
        key=lambda r: r["improvement_pct"], reverse=True)
    major_throughput = [r for r in major_core if r["metric"] == "ops_s"][:3]
    major_latency = [r for r in major_core if r["metric"] != "ops_s"][:3]
    report += ["## Executive summary", "",
               f"- **{len(major_core)} core metrics** met the major-regression rule across "
               f"**{len(set(r['case'] for r in major_core))} cases**."]
    for row in major_throughput:
        report.append(f"- Confirmed throughput regression: `{row['case']}` "
                      f"{row['improvement_pct']:+.1f}%.")
    for row in major_latency:
        report.append(f"- Confirmed latency regression: `{row['case']}` "
                      f"{row['improvement_pct']:+.1f}%.")
    for row in major_improvements[:3]:
        report.append(f"- Confirmed improvement: `{row['case']}` "
                      f"{row['improvement_pct']:+.1f}%.")
    report += [f"- Insertion-specific metrics meeting a regression threshold: **{len(enhanced_flags)}**.",
               f"- Correctness/API-error rows: **{len(integrity)}**; real failed processes: "
               f"**{len(failed)}**; harness exclusions: **{len(excluded)}**.", ""]
    analysis_path = results_dir / "analysis.md"
    if analysis_path.exists():
        report += ["## Analysis", "", analysis_path.read_text().strip(), ""]
    report += ["## Regression highlights", ""]
    if flagged:
        report += ["| case | metric | branch | main | branch vs main | paired changes | classification |",
                   "|---|---:|---:|---:|---:|---:|---|"]
        for r in flagged:
            report.append(f"| {r['case']} | {r['metric']} | {fmt(r['head'])} | {fmt(r['main'])} | "
                          f"{r['improvement_pct']:+.1f}% | {' '.join(f'{p:+.1f}%' for p in r['pairs'])} | {r['flag']} |")
    elif not enhanced_flags:
        report.append("No result met the report's regression threshold.")
    if enhanced_flags:
        report += ["", *[f"- {finding}" for finding in enhanced_flags]]
    diagnostic_rows = [r for r in flagged
                       if r["metric"] == "ops_s" and r.get("diagnostics")]
    if diagnostic_rows:
        report += ["", "Diagnostics for flagged throughput regressions; positive still means the branch is better:", "",
                   "| case | CPU efficiency | read bytes/op | write bytes/op |",
                   "|---|---:|---:|---:|"]
        for row in diagnostic_rows:
            diagnostic = row["diagnostics"]
            def diagnostic_change(name):
                value = diagnostic.get(name)
                return f"{value['improvement_pct']:+.1f}%" if value else "-"
            report.append(f"| {row['case']} | {diagnostic_change('ops_per_cpu_s')} | "
                          f"{diagnostic_change('rd_B_op')} | {diagnostic_change('wr_B_op')} |")
    if (graphs / "core-throughput.svg").exists():
        report += ["", "![Core throughput comparison](graphs/core-throughput.svg)"]

    report += ["", "## Core benchmark results", "",
               "| case | metric | branch | main | branch vs main | pairs | flag |",
               "|---|---:|---:|---:|---:|---:|---|"]
    for r in core:
        report.append(f"| {r['case']} | {r['metric']} | {fmt(r['head'])} | {fmt(r['main'])} | "
                      f"{r['improvement_pct']:+.1f}% | {' '.join(f'{p:+.1f}%' for p in r['pairs'])} | {r['flag']} |")

    report += ["", "## Insertion stalls", ""]
    if stalls:
        report += ["The stall threshold is 5% of each run's median 100 ms insertion rate after the first five seconds. "
                   "The all-zero measure requires every inserter to complete fewer than one operation per second in the sample.", "",
                   "| version | rep | mean inserts/s | median interval | minimum interval | stall seconds | all-zero seconds | oldest active ms |",
                   "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for s in stalls:
            report.append(f"| {s['version']} | {s['rep']} | {fmt(s['mean_insert_ops_s'])} | "
                          f"{fmt(s['median_interval_ops_s'])} | {fmt(s['min_interval_ops_s'])} | "
                          f"{s['stall_s']:.2f} | {s['all_inserters_zero_s']:.2f} | {s['max_active_ms']:.1f} |")
        report += ["", "Paired branch-versus-main stall metrics:", "",
                   "| metric | branch | main | branch vs main | paired changes | classification |",
                   "|---|---:|---:|---:|---:|---|"]
        fake_rows = [{"version": s["version"], "rep": str(s["rep"]), **s}
                     for s in stalls]
        for metric in ("mean_insert_ops_s", "stall_s", "all_inserters_zero_s",
                       "max_active_ms"):
            comparison = paired_summary(fake_rows, metric)
            if comparison:
                report.append(f"| {metric} | {fmt(comparison['head'])} | {fmt(comparison['main'])} | "
                              f"{comparison['improvement_pct']:+.1f}% | "
                              f"{' '.join(f'{p:+.1f}%' for p in comparison['pairs'])} | "
                              f"{comparison['flag']} |")
        report += ["", "![Insertion throughput](graphs/stall-throughput.svg)", "",
                   "![Insertion latency](graphs/stall-latency.svg)"]
    else:
        report.append("No completed insertion-stall runs.")

    report += ["", "## Paced open-loop insertion", "",
               "Each point uses scheduled arrivals. Response latency starts at the scheduled arrival and therefore includes queueing backlog; "
               "service latency starts when the library call begins. The target-0 point is closed-loop capacity and is listed in the table but omitted from the curve because it has no scheduled-arrival backlog. "
               "When either version sustains less than 99% of an offered rate, the queue is considered overloaded: its response latency depends on run duration and is reported as characterization rather than classified as a regression.", ""]
    if open_points:
        report += ["| version | rep | offered ops/s | achieved ops/s | response p99 ms | response p99.9 ms |",
                   "|---|---:|---:|---:|---:|---:|"]
        for p in sorted(open_points, key=lambda x:(x["rep"],x["target"],x["version"])):
            report.append(f"| {p['version']} | {p['rep']} | {p['target']} | {p['achieved']:.0f} | {p['p99']:.3f} | {p['p999']:.3f} |")
        report += ["", "Paired medians by offered rate:", "",
                   "| offered ops/s | metric | branch | main | branch vs main | pairs | flag |",
                   "|---:|---|---:|---:|---:|---:|---|"]
        for target in sorted(set(p["target"] for p in open_points)):
            selected = [p for p in open_points if p["target"] == target]
            fake = [{"version": p["version"], "rep": str(p["rep"]),
                     "achieved_insert_ops_s": p["achieved"], "response_p99_ms": p["p99"]}
                    for p in selected]
            for metric in ("achieved_insert_ops_s", "response_p99_ms"):
                comparison = paired_summary(fake, metric)
                if comparison:
                    flag = comparison["flag"]
                    if (metric == "response_p99_ms"
                            and open_loop_overloaded(target, open_points)):
                        flag = "overload characterization"
                    report.append(f"| {target} | {metric} | {fmt(comparison['head'])} | "
                                  f"{fmt(comparison['main'])} | {comparison['improvement_pct']:+.1f}% | "
                                  f"{' '.join(f'{p:+.1f}%' for p in comparison['pairs'])} | "
                                  f"{flag} |")
        report += ["", "![Open-loop curve](graphs/open-loop.svg)"]
    else:
        report.append("No completed open-loop runs.")

    barriers = [r for r in manifest if r.get("kind") == "barrier" and r.get("status") == "ok"]
    report += ["", "## Durable barriers", ""]
    if barriers:
        report += ["This API is absent from main, so these are audited-branch characterization runs rather than A/B comparisons.", "",
                   "| schedule | mean barrier ops/s | mean barrier latency ms | worst barrier latency ms |",
                   "|---|---:|---:|---:|"]
        for r in barriers:
            series = read_csv(out / r["series"])
            report.append(f"| {r['label']} | {median([number(x['barrier_ops_s']) for x in series]):.2f} | "
                          f"{median([number(x['barrier_avg_ms']) for x in series]):.3f} | "
                          f"{max(number(x['barrier_max_ms']) or 0 for x in series):.3f} |")
    else:
        report.append("No completed barrier runs.")

    report += ["", "## Correctness and run health", "",
               f"- Core rows with correctness violations or API errors: **{len(integrity)}**",
               f"- Failed recorded processes: **{len(failed)}**",
               f"- Harness-excluded processes: **{len(excluded)}**",
               f"- Steps skipped at the deadline: **{len(skipped)}**"]
    for r in failed:
        report.append(f"- Failed: `{r.get('name')}` rc={r.get('rc')} (see `{r.get('stderr')}`)")
    for r in excluded:
        report.append(f"- Harness exclusion: `{r.get('name')}`: {exclusions[r.get('name')]}")
    if integrity:
        for r in integrity[:20]:
            report.append(f"- Integrity: {r['version']} {r['size']} {r['phase']} rep {r['rep']}: "
                          f"violations={r['violations']} errors={r['errors']}")

    report += ["", "## Method and limits", "",
               "- Paired runs use ABBA ordering across repetitions and alternate the first version across phases.",
               "- Each core phase runs in a new process. Small-data reads begin with an untimed/full warm scan; large-data reads remain disk-bound.",
               "- Large random uniform and Zipfian overwrite phases use the configured extended warm-up before measurement so both versions are measured after transient leaf splitting and compaction.",
               "- Overwrite phases perform an untimed deterministic sample check of values written during the measured interval.",
               "- The laptop environment is not isolated. Pairwise deltas and thermal logs are more trustworthy than absolute rates.",
               "- Results completed after the report deadline are absent by construction."]
    report_text = "\n".join(report) + "\n"
    (results_dir / "REPORT.md").write_text(report_text)
    graph_links = "".join(f'<h2>{html.escape(p.stem.replace("-"," ").title())}</h2><img src="graphs/{p.name}" style="max-width:100%">' for p in sorted(graphs.glob("*.svg")))
    (results_dir / "index.html").write_text("<!doctype html><meta charset=utf-8><title>SplinterDB benchmark report</title>"
        "<style>body{font:14px system-ui;max-width:1200px;margin:2em auto;padding:0 1em}pre{white-space:pre-wrap}img{border:1px solid #ddd}</style>"
        f"<h1>SplinterDB branch vs main benchmark report</h1>{graph_links}<h2>Text report</h2><pre>{html.escape(report_text)}</pre>")
    summary = {"core_rows": len(rows), "core_results": core,
               "integrity_failures": len(integrity), "failed_runs": len(failed),
               "excluded_runs": len(excluded),
               "thermal": thermal,
               "stall_runs": [{k:v for k,v in s.items() if k not in ("rows","record")} for s in stalls],
               "open_loop_points": open_points}
    (results_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(results_dir / "REPORT.md")


if __name__ == "__main__":
    main()
