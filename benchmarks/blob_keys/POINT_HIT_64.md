# 64-byte random point-hit remeasurement

Follow-up: [profiling and the retained lexical pivot optimization](POINT_HIT_PROFILE.md)
updates these timings. The results below describe the pre-optimization branch.

The current blob-key B-tree remains **6.2% slower than main** by median paired
elapsed time. The 95% paired-log-ratio interval is **+3.9% to +7.0%**; 14 of
15 pairs were slower. This is similar to the earlier full matrix's +6.3%,
although the campaigns ran at different times and do not isolate the effect
of the intervening fixes.

| Variant | Median ns/hit |
|---|---:|
| Main | 735.0 |
| Current blob-key candidate | 779.0 |

The paired changes range from -1.4% to +9.3%. Ratios are calculated within each
pair before taking the median, so the headline need not equal the ratio of the
two median times above. The interval uses Student-t on 15 paired log ratios
(14 degrees of freedom); it estimates the geometric-mean ratio, not the median.

## Method and validation

- 15 paired repetitions, seeds 42–56; alternating variant order. No discarded
  runs or outliers, and no profiling instrumentation.
- Same workload as the earlier small-key matrix: 65,536 random 64-byte lexical
  keys, 32-byte values, one million successful point lookups per run in two
  measured batches. The tree is packed before lookup timing. Small keys remain inline.
- Main is frozen commit `d9a61a9abf6feadf017c951d965e2f3224b5eb30`.
  The candidate is the current `robj/blob-keys` working tree, including the
  retained packing and overwrite fixes. All 110 production source hashes match
  the frozen candidate; both executable and static-library hashes were verified.
  Source identities and the identical harness hash are retained in the evidence.
- All 30 runs passed correctness and residency checks. Measured I/O, asynchronous
  I/O, and major faults were zero. Paired checksums, leaf counts, prefix bytes,
  compressed-leaf counts, and packed allocation bytes matched.
- Intel Core i7-12800H, pinned to CPU 2; GCC 14.2.0 release libraries with
  `-O3 -ffast-math -flto`, identical harness with `-march=native`.
  512 MiB cache and 2 GiB configured device. CPU frequency was not fixed.
- Both coordination locks were held exclusively. Other registered projects
  were complete; this owner had no children. No build, edit, or analysis ran
  during measurement. Both locks were released.
- Measured October 28, 22:43:51–22:44:55 America/Los_Angeles
  (2026-10-29 05:43:51–05:44:55 UTC).
- This is a resident B-tree lookup measurement, not public API throughput,
  storage-bound performance, or tail latency. No source changes were made
  for this remeasurement; the remaining lookup overhead has not been profiled here.

## Evidence

- [Comparison and interval](point-hit-results-20261028/comparison.csv)
- [All paired results](point-hit-results-20261028/pairs.csv)
- [All 30 runs](point-hit-results-20261028/runs.csv)
- [Identities, commands, host observations, and coordination](point-hit-results-20261028/evidence.json)
- [Lock release](point-hit-results-20261028/release.json)

Raw output, the runner, and source-identity metadata are in
`.cache/point-hit-20261028/` in this worktree. Frozen executables are reused from
the preceding campaigns; their exact paths and hashes are in the evidence.
