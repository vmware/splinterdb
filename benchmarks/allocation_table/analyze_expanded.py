#!/usr/bin/env python3
"""Summarize fixed-repetition macro runs and paired throughput ratios.

Reads the ordinary kvbench CSV schema. Its size column identifies the workload
configuration (set with --stag). Every measured load must have a successful
reopened warm scan. No measurements are run by this program.

Confidence intervals are Student-t intervals on paired log throughput ratios.
They are per-comparison intervals, not simultaneous/familywise intervals. The
usual independent-block/model assumptions still apply; these intervals cannot
remove systematic measurement bias or justify optional stopping.
"""
import argparse
import csv
from functools import lru_cache
import json
import math
from pathlib import Path
import statistics


def _simpson(function, left, right, tolerance=1e-12):
    """Adaptive integration used only to compute a t critical value."""
    middle = (left + right) / 2
    fl, fm, fr = function(left), function(middle), function(right)
    whole = (right - left) * (fl + 4 * fm + fr) / 6

    def refine(a, b, fa, fc, fb, estimate, epsilon, depth):
        c = (a + b) / 2
        left_middle, right_middle = (a + c) / 2, (c + b) / 2
        fd, fe = function(left_middle), function(right_middle)
        left_estimate = (c - a) * (fa + 4 * fd + fc) / 6
        right_estimate = (b - c) * (fc + 4 * fe + fb) / 6
        difference = left_estimate + right_estimate - estimate
        if depth == 0 or abs(difference) <= 15 * epsilon:
            return left_estimate + right_estimate + difference / 15
        return (refine(a, c, fa, fd, fc, left_estimate, epsilon / 2, depth - 1)
                + refine(c, b, fc, fe, fb, right_estimate, epsilon / 2, depth - 1))

    return refine(left, right, fl, fm, fr, whole, tolerance, 24)


@lru_cache(maxsize=128)
def t_critical(df, confidence=0.95):
    """Two-sided Student-t critical value without external dependencies.

    Substitute t=sqrt(df)*tan(theta) in the t density and integrate the
    resulting bounded cos(theta)**(df-1) density. Bisection solves its quantile.
    """
    if df < 1 or not 0 < confidence < 1:
        raise ValueError("positive degrees of freedom and confidence in (0,1) required")
    coefficient = math.exp(math.lgamma((df + 1) / 2)
                           - math.lgamma(df / 2)) / math.sqrt(math.pi)
    target = confidence / 2
    left, right = 0.0, math.pi / 2
    for _ in range(60):
        middle = (left + right) / 2
        area = coefficient * _simpson(lambda angle: math.cos(angle) ** (df - 1),
                                      0, middle)
        if area < target:
            left = middle
        else:
            right = middle
    return math.sqrt(df) * math.tan((left + right) / 2)


def _summary(values):
    return dict(n=len(values), values=values, median=statistics.median(values),
                minimum=min(values), maximum=max(values), mean=statistics.mean(values))


def _rate(row, metric):
    if metric == "insertion":
        return float(row["ops_s"])
    if metric == "insertion_close":
        return int(row["ops"]) / float(row["total_s"])
    raise ValueError("unknown metric: " + metric)


def _paired_comparison(target_rows, baseline_rows, metric, margin, confidence):
    target = {int(row["rep"]): row for row in target_rows}
    baseline = {int(row["rep"]): row for row in baseline_rows}
    shared = sorted(target.keys() & baseline.keys())
    ratios = [_rate(target[rep], metric) / _rate(baseline[rep], metric)
              for rep in shared]
    logarithms = [math.log(ratio) for ratio in ratios]
    result = dict(metric=metric, n=len(shared), repetitions=shared, ratios=ratios,
                  changes_pct=[100 * (ratio - 1) for ratio in ratios],
                  missing_target_repetitions=sorted(baseline.keys() - target.keys()),
                  missing_baseline_repetitions=sorted(target.keys() - baseline.keys()))
    if not shared:
        result.update(ratio=None, change_pct=None, ci_ratio=None, ci_change_pct=None,
                      interpretation="insufficient_pairs")
        return result
    mean_log = statistics.mean(logarithms)
    ratio = math.exp(mean_log)
    result.update(ratio=ratio, change_pct=100 * (ratio - 1))
    if len(shared) < 2:
        result.update(ci_ratio=None, ci_change_pct=None, interpretation="insufficient_pairs")
        return result
    standard_error = statistics.stdev(logarithms) / math.sqrt(len(shared))
    half_width = t_critical(len(shared) - 1, confidence) * standard_error
    lower, upper = math.exp(mean_log - half_width), math.exp(mean_log + half_width)
    if lower >= 1 - margin and upper <= 1 + margin:
        interpretation = "within_equivalence_band"
    elif lower > 1 + margin:
        interpretation = "faster_beyond_equivalence_band"
    elif upper < 1 - margin:
        interpretation = "slower_beyond_equivalence_band"
    elif lower > 1:
        interpretation = "faster_but_magnitude_uncertain"
    elif upper < 1:
        interpretation = "slower_but_magnitude_uncertain"
    else:
        interpretation = "inconclusive"
    result.update(ci_ratio=[lower, upper],
                  ci_change_pct=[100 * (lower - 1), 100 * (upper - 1)],
                  log_ratio_standard_error=standard_error,
                  interpretation=interpretation)
    return result


def analyze(csv_paths, configs=None, baselines=None, margin=0.03, confidence=0.95):
    rows = []
    for path in csv_paths:
        with Path(path).open(newline="") as stream:
            rows.extend(csv.DictReader(stream))
    if not rows:
        raise ValueError("no CSV records")
    seen = {}
    for row in rows:
        if row["phase"] not in ("load:random", "load:seq", "warm"):
            continue
        key = (row["size"], row["version"], int(row["threads"]), int(row["rep"]), row["phase"])
        if key in seen:
            raise ValueError("duplicate CSV phase key: " + repr(key))
        seen[key] = row
        if int(row["errors"]) or int(row["violations"]):
            raise ValueError("correctness error: " + repr(key))
        if int(row["ops"]) != int(row["n"]):
            raise ValueError("incomplete load/scan: " + repr(key))
    loads = [row for row in seen.values() if row["phase"].startswith("load:")]
    if not loads:
        raise ValueError("no insertion records")
    for row in loads:
        key = (row["size"], row["version"], int(row["threads"]), int(row["rep"]), "warm")
        if key not in seen or int(seen[key]["n"]) != int(row["n"]):
            raise ValueError("load lacks matching reopened scan: " + repr(key))
        rates = [_rate(row, metric) for metric in ("insertion", "insertion_close")]
        if any(not math.isfinite(rate) or rate <= 0 for rate in rates):
            raise ValueError("invalid throughput: " + repr(key))
    groups = {}
    for row in loads:
        key = (row["size"], int(row["threads"]), row["phase"], row["version"])
        groups.setdefault(key, []).append(row)
    output_groups = []
    for (config, threads, phase, variant), records in sorted(groups.items()):
        records.sort(key=lambda row: int(row["rep"]))
        ns = {int(row["n"]) for row in records}
        if len(ns) != 1:
            raise ValueError("configuration mixes dataset sizes")
        output_groups.append(dict(config=config, threads=threads, phase=phase,
                                  variant=variant, keys=next(iter(ns)),
                                  repetitions=[int(row["rep"]) for row in records],
                                  insertion=_summary([_rate(row, "insertion") for row in records]),
                                  insertion_close=_summary([_rate(row, "insertion_close") for row in records]),
                                  drain_s=_summary([float(row["drain_s"]) for row in records]),
                                  cpu_s=_summary([float(row["cpu_s"]) for row in records]),
                                  wamp=_summary([float(row["wamp"]) for row in records]),
                                  p99_us=_summary([float(row["p99_us"]) for row in records]),
                                  p999_us=_summary([float(row["p999_us"]) for row in records])))
    names = sorted({row["version"] for row in loads})
    if baselines is None:
        baselines = [name for name in ("original", "dense") if name in names]
    missing = set(baselines) - set(names)
    if missing:
        raise ValueError("requested baseline not found: " + repr(sorted(missing)))
    comparisons = []
    for (config, threads, phase, variant), records in sorted(groups.items()):
        for baseline in baselines:
            if baseline == variant:
                continue
            baseline_rows = groups.get((config, threads, phase, baseline))
            if baseline_rows is None:
                continue
            if {int(row["n"]) for row in records} != {int(row["n"]) for row in baseline_rows}:
                raise ValueError("comparison mixes dataset sizes")
            for metric in ("insertion", "insertion_close"):
                result = _paired_comparison(records, baseline_rows, metric, margin, confidence)
                comparisons.append(dict(config=config, threads=threads, phase=phase,
                                        variant=variant, baseline=baseline, **result))
    return dict(csv_files=[str(Path(path).resolve()) for path in csv_paths],
                configurations=configs, confidence=confidence, equivalence_margin=margin,
                method="Student-t interval on paired log throughput ratios; geometric mean ratio",
                limitations=["per-comparison intervals; no multiplicity adjustment",
                             "fixed repetition count assumed; optional stopping invalidates nominal coverage",
                             "independent run-block approximation; intervals do not remove systematic bias",
                             "persisted scan checks full keys, value lengths and embedded IDs, not every payload byte"],
                verified_loads=len(loads), verified_insertions=sum(int(row["ops"]) for row in loads),
                individual_runs=loads, groups=output_groups, comparisons=comparisons)


def markdown(result):
    lines = ["# Expanded allocator macro comparison", "",
             f"Verified {result['verified_loads']} loads and reopened scans; "
             f"{result['verified_insertions']:,} inserted records.", "",
             "Rates are thousands of inserts per second. Ranges are sample min–max, not confidence intervals.", "",
             "| Configuration | Threads | Variant | Runs | Median insertion k/s | Range k/s | Median insertion+close k/s |",
             "|---|---:|---|---:|---:|---:|---:|"]
    for group in result["groups"]:
        rate, total = group["insertion"], group["insertion_close"]
        lines.append(f"| {group['config']} | {group['threads']} | {group['variant']} | {rate['n']} | "
                     f"{rate['median'] / 1000:.1f} | {rate['minimum'] / 1000:.1f}–{rate['maximum'] / 1000:.1f} | "
                     f"{total['median'] / 1000:.1f} |")
    lines += ["", "Paired geometric-mean changes, with per-comparison "
              f"{100 * result['confidence']:.0f}% confidence intervals. "
              f"Equivalence requires the entire interval inside ±{100 * result['equivalence_margin']:.1f}%.", "",
              "| Configuration | Threads | Metric | Variant / baseline | Pairs | Change | CI | Interpretation |",
              "|---|---:|---|---|---:|---:|---|---|"]
    for row in result["comparisons"]:
        change = f"{row['change_pct']:+.2f}%" if row["change_pct"] is not None else "n/a"
        interval = (f"[{row['ci_change_pct'][0]:+.2f}%, {row['ci_change_pct'][1]:+.2f}%]"
                    if row["ci_change_pct"] is not None else "n/a")
        lines.append(f"| {row['config']} | {row['threads']} | {row['metric']} | "
                     f"{row['variant']} / {row['baseline']} | {row['n']} | {change} | {interval} | {row['interpretation']} |")
    lines += ["", "Limitations:", ""] + ["- " + note + "." for note in result["limitations"]]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, action="append", required=True)
    parser.add_argument("--configs", type=Path)
    parser.add_argument("--baseline", action="append")
    parser.add_argument("--margin", type=float, default=0.03)
    parser.add_argument("--confidence", type=float, default=0.95)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 0 < args.margin < 1 or not 0 < args.confidence < 1:
        parser.error("margin and confidence must lie in (0,1)")
    configs = json.loads(args.configs.read_text()) if args.configs else None
    result = analyze(args.csv, configs, args.baseline, args.margin, args.confidence)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    (args.output / "summary.md").write_text(markdown(result))
    print(args.output / "summary.md")


if __name__ == "__main__":
    main()
