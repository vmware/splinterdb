# Full small-key B-tree rerun after profiling fixes

Latest scan follow-up: [shared-prefix Scan4096 profiling and retained fix](SHARED_SCAN4096_PROFILE.md)
makes these scans 12.8–13.4% faster than original main across all four key sizes.
The retained fix also removes the intermediate random-scan tradeoff. All 882
runs of the final full matrix verified; custom insertion, overwrite, and hits
retain roughly 2% overhead.

The combined fixes improve packing and lexical mutation/lookup performance, but
**the no-small-key-regression objective is still not met**. The clearest remaining
regression is shared-prefix Scan4096: 64-, 256-, and 512-byte keys take about
**5–7% longer** by paired median. Custom overwrite retains about **2% overhead**.

All **882 runs** passed verification and residency checks, with zero measured
I/O or major faults. This is the complete original 63-case matrix, seven paired
repetitions, on the exact source validated in the latest mutation profile.
No production source changes were made for this rerun.

## Full matrix

Values are medians of seven paired **elapsed-time changes**, current/main.
Positive means slower; negative means faster. These are not throughput changes.
The baseline is the original frozen main binary from SMALL_KEYS_V4.md.

| Key bytes | Distribution / comparator | Insert | Overwrite | Hit | Miss | Scan 32 | Scan 4096 | Pack |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 24 | Random lexical | -29.7% | -26.6% | -26.5% | -26.4% | -11.4% | +2.9% | -12.5% |
| 24 | Shared prefix | -29.4% | -28.4% | -26.4% | -26.3% | -14.0% | +4.3% | -11.7% |
| 64 | Random lexical | -28.2% | -25.0% | -27.4% | -28.0% | -12.5% | +1.4% | -12.0% |
| 64 | Shared prefix | -27.7% | -24.7% | -23.7% | -25.2% | -13.3% | +6.7% | -11.5% |
| 256 | Random lexical | -23.5% | -23.0% | -23.2% | -26.6% | -15.3% | +1.4% | -5.0% |
| 256 | Shared prefix | -22.0% | -20.8% | -19.8% | -20.2% | -12.8% | +5.2% | -10.1% |
| 512 | Random lexical | -17.5% | -20.5% | -19.5% | -24.3% | -17.3% | +1.0% | -16.9% |
| 512 | Shared prefix | -16.9% | -18.7% | -16.1% | -16.4% | -13.2% | +5.2% | -4.9% |
| 64 | Custom comparator | +1.0% | +1.9% | +1.8% | -0.6% | +1.5% | +1.6% | -14.5% |

All nine packing configurations are faster: paired median reductions range from
4.9% to 16.9%, and every packing case has a geometric-mean interval below zero.
Lexical hits are 16.1–27.4% faster by paired median; lexical Scan32 is 11.4–17.3%
faster. These are the combined branch's gains against original main, including
the lexical comparison shortcut added during profiling. They do not isolate the
cost of blob-key support.

The exploratory per-case intervals classify **7 cases as slower,
49 as faster, and 7 as overlapping zero**.
These are not simultaneous confidence guarantees across all 63 comparisons.

## Remaining regressions

Every row below has a positive lower bound in its paired-log 95% interval.
The interval and geometric mean use the same statistic; the median is reported
separately. All seven pairs were slower in every row except 256-random-scan4096,
where six were slower.

| Case | Paired median | Geometric mean | Geometric-mean 95% interval | Slower pairs |
|---|---:|---:|---:|---:|
| 64-common-scan4096 | +6.7% | +6.8% | +6.3% to +7.2% | 7/7 |
| 256-common-scan4096 | +5.2% | +6.7% | +3.1% to +10.4% | 7/7 |
| 512-common-scan4096 | +5.2% | +5.8% | +4.4% to +7.3% | 7/7 |
| 24-random-scan4096 | +2.9% | +3.1% | +2.4% to +3.8% | 7/7 |
| 64-nonlexical-overwrite | +1.9% | +2.2% | +0.7% to +3.7% | 7/7 |
| 64-random-scan4096 | +1.4% | +1.6% | +0.9% to +2.3% | 7/7 |
| 256-random-scan4096 | +1.4% | +1.2% | +0.2% to +2.2% | 6/7 |

**64-byte shared-prefix Scan4096 is the clearest next profiling target**: its
+6.7% median and +6.3% to +7.2% geometric-mean interval are both substantial and
consistent. The 256- and 512-byte shared-prefix cases confirm that the issue
extends beyond one key size. These measurements establish the slowdown; they do
not identify its cause.

Custom insertion is +1.0% by median, +0.8% by geometric mean, with an interval
of -0.8% to +2.3%. Custom hits are +1.8% by median, +2.9% by geometric mean,
with an interval of -0.6% to +6.6%. Neither is a clear regression in this
seven-pair window, but neither demonstrates equivalence or overturns the
earlier evidence of a small insertion cost. Custom overwrite is +1.9% by median,
+2.2% by geometric mean, with an interval of +0.7% to +3.7%.

## Relation to earlier measurements

The following comparison uses the same original-main binary and workload as the
previous full suite, but different measurement windows. It summarizes progress
and does not attribute differences to any single fix.

| Case | Previous full-suite median | Current full-suite median |
|---|---:|---:|
| 64-random-pack | +12.1% | -12.0% |
| 64-nonlexical-insert | +9.9% | +1.0% |
| 64-nonlexical-overwrite | +10.6% | +1.9% |
| 64-random-hit | +6.3% | -27.4% |
| 64-nonlexical-hit | +5.0% | +1.8% |
| 64-common-scan4096 | -0.5% | +6.7% |
| 256-common-scan4096 | +2.8% | +5.2% |
| 512-common-scan4096 | +1.5% | +5.2% |

The most recent focused [mutation profile](MUTATION_PROFILE.md) used main with
the diagnostic **checked-read lexical shortcut** added; this full matrix uses
**unmodified frozen main**, as did the previous full matrix. The focused runs
also used 16 write batches, whereas this matrix uses four. Even custom workloads
can respond to the compiler/layout changes between those baseline binaries.
Therefore, the earlier +0.4% overwrite estimate and this +2.2% estimate are not
a same-baseline, same-workload comparison. The candidate executable is
byte-identical in both campaigns. This rerun does not show that the latest fix
was undone; it does show residual overwrite overhead against original main.

The increased Scan4096 overhead relative to the earlier full suite merits a
same-window comparison of intermediate candidates. This campaign did not run
those intermediate candidates, so it cannot locate when the slowdown arose.
The original large scan losses remain historical context, not evidence that the
current long-scan path is fully fixed.

## Verification and method

- All 441 pairs have equal operation/tuple counts, checksums, verified values,
  and workload parameters. Packed leaf counts, compressed-leaf counts, prefix
  bytes, and allocated bytes match in every pair. The harness does not populate
  mutable-write layout counters; their matching zeros establish no mutable-tree
  geometry claim. Tree height was not instrumented.
- All 882 runs passed, with zero active read/write bytes, synchronous/asynchronous
  cache I/O, and major faults. No successful sample or outlier was discarded.
- Baseline: main d9a61a9abf6feadf017c951d965e2f3224b5eb30, exact binary from the
  previous full matrix. Candidate: robj/blob-keys at bb000cc3 plus the current
  retained working changes, B-tree format 4. All 110 current production
  source/header hashes match the frozen final mutation-profile source.
- The unchanged candidate already passed the full debug suite (295 passed,
  zero failures, five existing skips) and targeted debug/release/ASan B-tree and
  native blob-key tests. Those logs were hash-verified; correctness suites were
  not rerun during this measurement-only task. ASan leak detection was disabled
  in that prior validation, and UBSan was not enabled.
- Both libraries use GCC 14.2.0 release flags -O3 -ffast-math -flto. The identical
  harness additionally uses -march=native. No perf instrumentation was enabled.
  CPU is an Intel Core i7-12800H, pinned to CPU 2. The performance governor and
  2.5 GHz cap were unchanged; frequency was not fixed. Frequency observations
  occur between runs and are not active-operation clock measurements.
- Four key sizes (24/64/256/512 bytes), random and shared-prefix lexical ordering,
  plus 64-byte custom ordering by the last eight bytes. Values are 32 bytes.
  There are 65,536 records for 24/64-byte keys and 32,768 for 256/512-byte keys;
  all keys in this small-key workload remain inline.
- Cache is 512 MiB; configured device size is 2 GiB. Insert, overwrite, and pack
  use four measured batches; reads use two. Lookups perform one million
  operations. Scan32 returns about 8.4 million tuples and Scan4096 about
  33.6 million per run. Setup, warmup, verification, and reclamation are outside
  active timing.
- Seeds 42–48 match within each pair; baseline/candidate order alternates.
  Intervals use Student-t on seven paired log ratios (six degrees of freedom),
  transformed back to percentages. They estimate geometric-mean ratios, not
  medians, and are exploratory per-case intervals without multiplicity correction.
- Both benchmark and machine locks were held exclusively throughout measurement.
  Other projects were explicitly complete; the unchanged async-state registration
  has an archived exact-content completion confirmation superseding its stale
  PREPARING heading. There were no child agents. No builds, edits, tests, or
  result analysis ran during measurement. Request, registrations, confirmations,
  and matching release are retained. Both locks are released.
- Host observations show no cgroup CPU throttling. The scope is resident,
  single-threaded B-tree CPU cost, not disk-bound performance, public API
  throughput, concurrency, compaction policy, or tail latency.

## Evidence

Measured 2026-10-29 22:20:32–22:43:34 PDT (2026-10-30 05:20:32–05:43:34 UTC), 23.0 minutes.

- [All 63 comparisons and intervals](small-key-final-results-20261029/comparison.csv).
- [All 882 runs](small-key-final-results-20261029/runs.csv) and
  [441 paired samples](small-key-final-results-20261029/pairs.csv).
- [Comparison with the earlier full suite](small-key-final-results-20261029/historical-comparison.csv).
- [Source, validation, sample hashes, and coordination evidence](small-key-final-results-20261029/evidence.json).

Raw stdout/stderr, frozen binaries/libraries, harness, runner, metadata, and report
generator are retained under .cache/small-key-final-20261029/. Source archives and
frozen source trees are retained at the paths recorded by the profiles. No
production code was edited, committed, or pushed for this rerun.

To reproduce with a fresh output directory:

~~~sh
python3 .cache/small-key-final-20261029/run.py \
  --profiles .cache/small-key-final-20261029/profiles.json \
  --out .cache/small-key-final-20261029/new-matrix \
  --coordination /home/rob/projects/splinterdb/.cache/projects-20261025/coordination \
  --lock-dir /home/rob/projects/splinterdb/.cache/projects-20261011/coordination \
  --variants baseline candidate --repetitions 7 --expected-minutes 25
~~~

The runner requires fresh matching pause acknowledgements from any active project.
Run report.py --matrix .cache/small-key-final-20261029/new-matrix after completion
to validate and regenerate the report and CSVs.
