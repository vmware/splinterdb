# Small-key B-tree rerun after compact statistics

Latest scan follow-up: [shared-prefix Scan4096 profiling and retained fix](SHARED_SCAN4096_PROFILE.md)
makes these scans 12.8–13.4% faster than original main across all four key sizes.
The retained fix also removes the intermediate random-scan tradeoff. All 882
runs of the final full matrix verified; custom insertion, overwrite, and hits
retain roughly 2% overhead.


The no-small-key-regression objective is **not yet met**. The large scan regressions
have largely cleared, but packing is 2.7–12.1% slower by paired median, and the
64-byte custom comparator retains 9.9% insertion and 10.6% overwrite overhead.
Successful point lookups are 1.2–6.3% slower by paired median.

Follow-up: [packing profiling and fixes](PACK_PROFILE.md) remove the packing
regressions in a same-window comparison of all nine packing cases. The combined
fix is 2.4–17.4% faster than main by paired median; the 256-byte random case remains
inconclusive. The historical full matrix below predates that fix; other operations
have not been remeasured as a full matrix after it.

The later [overwrite profile](OVERWRITE_PROFILE.md) reduces custom-comparator
overwrite overhead from 11.7% to 5.2% in its longer focused comparison. The
remaining regression is confirmed; the fixes do not establish overall parity.

A [later 64-byte random point-hit remeasurement](POINT_HIT_64.md) finds
+6.2% median elapsed time versus main (15 pairs; 95% interval +3.9% to +7.0%).
The lookup regression remains after the packing and overwrite fixes.
Subsequent [point-hit profiling](POINT_HIT_PROFILE.md) adds a lexical pivot
shortcut: the retained branch takes 23.2% less time than unmodified main, but
remains 6.0% slower than main with the same shortcut. Custom controls show no
clear new regression relative to the pre-fix branch.

[Continued profiling](RESIDUAL_PROFILE.md) reduces ordinary key decoding and
comparison overhead: 64-byte random and custom hits now have about 1% overhead
against matched optimized main, with intervals spanning zero. Custom writes
and some larger-key lookups still regress. That follow-up is a targeted subset.

The latest [mutation follow-up](MUTATION_PROFILE.md) removes intermediate
metadata copies: custom overwrite is near main, while insertion retains roughly
2% overhead. Its full write controls show no clear new regression.

All **882 runs** passed verification and residency checks: 63 cases, seven paired
repetitions, main versus the actual blob-capable format-4 candidate. Small keys
remain inline. This campaign measures B-trees only; no public API timings were run.

## Full matrix

Values are the median of seven paired **elapsed-time changes**, candidate/main.
Positive means slower; negative means faster. These are time changes, not
throughput changes. No samples or outliers were discarded.

| Key bytes | Distribution / comparator | Insert | Overwrite | Hit | Miss | Scan 32 | Scan 4096 | Pack |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 24 | Random lexical | -2.5% | +2.9% | +4.3% | +3.5% | +0.7% | -0.4% | +11.1% |
| 24 | Shared prefix | -1.5% | +4.3% | +3.5% | +3.7% | +2.7% | -1.0% | +6.4% |
| 64 | Random lexical | -0.6% | +5.4% | +6.3% | +4.4% | +0.0% | +0.5% | +12.1% |
| 64 | Shared prefix | +0.3% | +4.2% | +2.2% | +1.0% | +0.3% | -0.5% | +8.2% |
| 256 | Random lexical | +3.0% | +3.0% | +5.8% | +3.0% | -5.3% | +2.3% | +5.4% |
| 256 | Shared prefix | +1.2% | +1.6% | +2.7% | +1.0% | -1.9% | +2.8% | +9.5% |
| 512 | Random lexical | +1.9% | -0.9% | +1.6% | +0.5% | -9.1% | +1.1% | +4.4% |
| 512 | Shared prefix | +1.3% | +1.0% | +1.2% | +2.1% | -3.1% | +1.5% | +2.7% |
| 64 | Custom comparator | +9.9% | +10.6% | +5.0% | +3.2% | -0.8% | +0.7% | +7.1% |

## Largest remaining regressions

Each range below is the smallest/largest observed paired change, **not a
confidence interval**. All seven pairs were slower in these cases.

| Case | Median time change | Paired range |
|---|---:|---:|
| 64-random-pack | +12.1% | +6.6% to +16.3% |
| 24-random-pack | +11.1% | +5.8% to +18.5% |
| 64-nonlexical-overwrite | +10.6% | +6.9% to +13.6% |
| 64-nonlexical-insert | +9.9% | +5.3% to +14.3% |
| 256-common-pack | +9.5% | +6.7% to +16.5% |
| 64-common-pack | +8.2% | +6.0% to +13.6% |
| 64-random-hit | +6.3% | +4.0% to +8.5% |

The CSV also includes 95% Student-t intervals computed on the seven paired log
time ratios (six degrees of freedom), then transformed back to percentage changes.
Those intervals estimate the geometric-mean ratio; they are not intervals around
the displayed median. They are per-case exploratory intervals without correction
for the 63 comparisons. Some cases have wide ranges; seven pairs do not establish
performance equivalence.

## Scan comparison and storage layout

The earlier [Scan32 profile](SCAN32_PROFILE.md), against the same main commit,
found +4.3% for 64-byte random lexical keys and +4.2% for the custom comparator.
Those cases now have paired medians of +0.0% and -0.8%, with ranges of -4.3% to
+1.4% and -1.9% to +1.7%. This is consistent with removing the remaining
statistics-width penalty; it is not a same-window ablation of that change alone.

Scan32 medians now range from -9.1% to +2.7%. Scan4096 medians range from -1.0%
to +2.8%, far smaller than the original 25–39% throughput losses. Small residual
scan overhead remains: 256-byte shared-prefix Scan4096 is slower in all seven
pairs (+1.4% to +6.3%). The original [small-key report](SMALL_KEYS.md) used an
older baseline, so its percentages are historical context, not direct ablation data.

Leaf counts, compressed-leaf counts, prefix bytes, and total packed allocation
bytes matched main in **every pair**. This campaign did not instrument tree
height. Index entries use 23 bytes plus the key for these small statistics,
versus 35 in the previous fixed-64-bit candidate. The matched allocation counts
do not explain the remaining CPU regressions; packing and custom-comparator
mutable operations are the clearest next profiling targets.

## Method and limitations

- Baseline: main d9a61a9abf6feadf017c951d965e2f3224b5eb30, using the identical
  frozen baseline binary from the Scan4096/Scan32 investigations.
- Candidate: bb000cc3 plus the frozen working-tree patch implementing contiguous
  32/64-bit counters (B-tree format 4) and attachment accounting. Source, patch,
  static library, harness and executable hashes are preserved.
- CPU: Intel Core i7-12800H, pinned to CPU 2; GCC 14.2.0. Both libraries use
  release -O3 -ffast-math -flto; the identical harness uses those flags plus
  -march=native at compilation/linking. No perf instrumentation was enabled.
- Same original 63-case workload: 24/64/256/512-byte lexical keys, random and
  shared-prefix distributions; 64-byte nonlexical comparator; 32-byte values.
  65,536 records for 24/64-byte keys and 32,768 for 256/512-byte keys.
- 512 MiB cache and 2 GiB configured device. Insert/overwrite/pack use four
  measured batches; reads use two. Lookups execute 1,000,000 operations;
  Scan32 returns about 8.4 million tuples and Scan4096 about 33.6 million.
- Seeds 42–48, matched within each pair; alternating baseline/candidate order.
  Setup, warmup, full verification and reclamation are outside active timing.
- Every run checks values, checksums, ordering where applicable, and reclamation.
  All measured read/write I/O, asynchronous I/O and major faults were zero.
- Both coordination locks were held exclusively throughout the measurement window.
  No other project was active; the owner had no children. Registrations, completion
  confirmations, request and release records are archived. No build, edit or
  result analysis ran during measurement.
- This is a resident CPU-cost comparison. It does not measure storage-bound
  behavior, public API throughput, tail latency, or prolonged production workloads.
  CPU frequency was not fixed. Host load/frequency observations are retained;
  no cgroup CPU throttling was reported. Outlying samples remain in the data.

## Evidence

Measured 2026-10-29T00:15:55.125080+00:00 through 2026-10-29T00:40:18.548267+00:00 UTC
(October 28, 17:15–17:40 America/Los_Angeles). Both locks were released successfully.

- [All 63 comparisons and intervals](small-key-results-20261028-v4/comparison.csv)
- [All 882 per-run results](small-key-results-20261028-v4/runs.csv)
- [All 441 paired samples](small-key-results-20261028-v4/pairs.csv)
- [Identity, validation and coordination evidence](small-key-results-20261028-v4/evidence.json)

Raw frozen sources, libraries, executables, per-run stdout/stderr, runner and
manifest are in .cache/small-key-btree-v4-20261028/ in the topic worktree.

Reproduce from that worktree, with a fresh output directory:

~~~sh
python3 .cache/small-key-btree-v4-20261028/run.py \
  --profiles .cache/small-key-btree-v4-20261028/profiles.json \
  --out .cache/small-key-btree-v4-20261028/new-matrix \
  --coordination /home/rob/projects/splinterdb/.cache/projects-20261025/coordination \
  --lock-dir /home/rob/projects/splinterdb/.cache/projects-20261011/coordination \
  --variants baseline candidate --repetitions 7 --expected-minutes 25
~~~

The runner requires current acknowledgements from any newly active project.
The archived completion confirmation is accepted only while that project record
remains byte-identical; a changed registration requires fresh coordination.
