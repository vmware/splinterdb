# Custom-comparator overwrite profile

Latest follow-up: [mutation profiling and fixes](MUTATION_PROFILE.md) remove
intermediate key/message metadata copies. Longer confirmation now finds +0.4%
custom overwrite and +1.7% insertion time versus main; shorter controls still
confirm roughly 2% insertion overhead. The measurements below are historical.

Follow-up: [continued profiling](RESIDUAL_PROFILE.md) improves point lookups,
while custom overwrite still has 4.0% paired geometric-mean overhead in a new
15-pair confirmation. The historical measurements below are retained.

The regression is reduced, but **not eliminated**. In the longer unprofiled
confirmation, the retained candidate is **5.2% slower than main** by paired median,
with a 95% paired-log-ratio interval of **+3.9% to +6.1%**. Before these fixes it
was **11.7% slower** in the same measurement window. The fixes reduce elapsed time
by **6.2%** relative to that pre-fix candidate.

All 15 focused pairs were still slower than main; all 15 were faster than the
pre-fix candidate. The overall no-small-key-regression objective remains unmet.

## Focused confirmation

Each run overwrites 65,536 existing 64-byte keys in a random permutation, with
32-byte replacement values, for 16 measured batches. The application comparator
orders keys by their final eight bytes, so lexical comparison cannot substitute
for the callback. Small keys remain inline.

| Comparison | Median elapsed-time change | 95% interval | Observed paired range |
|---|---:|---:|---:|
| Pre-fix candidate / main | +11.7% | +10.7% to +13.1% | +8.6% to +16.8% |
| Retained candidate / main | +5.2% | +3.9% to +6.1% | +0.4% to +7.4% |
| Retained / pre-fix candidate | -6.2% | -7.1% to -5.3% | -9.0% to -2.1% |

These are 15 paired repetitions, seeds 42–56, with alternating variant order.
Negative means less elapsed time, not a throughput percentage. Median times were
1,048.5 ns/overwrite for main, 1,177.2 ns before the fix, and 1,099.7 ns after it.
Ratios are computed within each repetition before taking medians, so they need
not equal ratios of the separate median times.

The intervals use Student-t on paired log ratios (14 degrees of freedom), then
transform back to percentages. They estimate geometric-mean ratios, not the
reported medians. No samples or outliers were discarded.

## What profiling found and what changed

The original counter runs reproduced approximately 11% overwrite overhead.
Cycle samples concentrated in pivot and leaf searches. The new blob-key search
path validated offsets, entry bounds, flags, and encoded lengths at each binary
search comparison, including mutations of trusted, pinned memtable nodes.

Search implementations now distinguish mutable entries from persisted branch
entries. Mutation searches use the existing trusted entry accessors, while
persisted readers retain the same checked accessors. Both paths retain fallible
blob comparison and propagate blob-read errors. The isolated trusted-entry
variant removed about 486 instructions and 127 branches per custom overwrite,
but improved time by only about 2%; those checks were a contributor rather than
the whole cause.

Every insertion also initialized and cleaned up a key-encoding buffer even when
the incoming key was small. The public B-tree insertion wrapper now normalizes
and validates configuration, rejects externally supplied encoded keys, and calls
a shared insertion body directly for inline keys. Only large incoming keys
initialize the encoding buffer. The shared body receives key/message pointers
to avoid another aggregate argument copy. This changes scratch-buffer setup;
it does not imply that the former small-key path allocated heap storage.

Finally, a mutable leaf has no common-prefix compression. For custom comparators,
its search now goes directly to the callback-driven loop instead of entering
generic compressed-leaf dispatch. Lexical mutation searches keep their lexical
fast path. No user callback signatures, comparison semantics, or on-disk encoding
were changed.

A same-window hardware-counter comparison of the retained code shows:

| Variant | ns/overwrite | Instructions/overwrite | Branches/overwrite | Blocked forwarding/overwrite |
|---|---:|---:|---:|---:|
| Main | 1,074.7 | 2,818.0 | 476.8 | 63.2 |
| Pre-fix candidate | 1,178.0 | 3,658.6 | 649.3 | 74.0 |
| Retained candidate | 1,116.9 | 3,070.6 | 510.6 | 52.8 |

These are medians of three **instrumented** runs and are separate from the
unprofiled results above. The counter set was cycles, instructions, branches,
branch misses, and `ld_blocks.store_forward`, restricted to active user-mode
insertion intervals. All counters ran 100% of their enabled time, without
multiplexing. Five cycle profiles had zero lost samples.

Several experiments were rejected. Explicit callback-member assignments left
the hot pivot callback assembly unchanged. Preparing the query's callback
argument once per search reduced blocked forwarding events but did not improve
time. A smaller cleanup-based lazy-buffer implementation was slower than the
split insertion path. A final experiment bypassing merge-accumulator setup for
definitive overwrites reduced instructions but made custom overwrite slower;
it is **not in the retained implementation**. These results show why lower
instruction or forwarding-event counts alone are insufficient evidence of a fix.
The remaining approximately 5% gap has not been fully attributed or fixed.

## Broader overwrite controls

Before the longer focused confirmation, all nine earlier small-key overwrite
configurations plus custom-comparator insertion were remeasured: seven paired
repetitions, four measured batches per run, and three variants, for 210 runs.
This shorter sweep was noisier. Its near-zero custom-overwrite median did not
establish equivalence; the longer focused confirmation above establishes a
remaining regression. The two campaigns are kept separate.

| Case | Before/main | After/main | After/before | After/main 95% interval |
|---|---:|---:|---:|---|
| 24-random-overwrite | +4.6% | -2.0% | -7.0% | -5.8% to +0.7% |
| 24-common-overwrite | +5.8% | -0.4% | -5.3% | -2.7% to +4.5% |
| 64-random-overwrite | +6.0% | +0.2% | -3.2% | -1.3% to +4.0% |
| 64-common-overwrite | +6.3% | +2.4% | -5.0% | +0.2% to +3.6% |
| 256-random-overwrite | +7.7% | +1.5% | -4.8% | -0.7% to +3.1% |
| 256-common-overwrite | +1.8% | -4.6% | -4.3% | -6.9% to -2.4% |
| 512-random-overwrite | -0.5% | -6.7% | -4.0% | -7.3% to -4.1% |
| 512-common-overwrite | +2.7% | -2.0% | -4.4% | -2.4% to -0.0% |
| 64-nonlexical-insert | +10.2% | +3.0% | -6.2% | +1.4% to +5.1% |
| 64-nonlexical-overwrite | +8.1% | +0.0% | -5.9% | -0.9% to +5.0% |

The matrix intervals use six degrees of freedom and are exploratory per-case
intervals without multiple-comparison adjustment. Custom insertion retains
approximately 3.0% median overhead; the 64-byte shared-prefix overwrite case
retains approximately 2.4%. Point lookups, scans, packing, and public API
performance were not remeasured after these insertion changes.

## Correctness and method

- All **255 unprofiled runs** and **147 counter runs** passed verification and
  residency checks. Each timed workload verifies values, ordering/checksums,
  and allocator reclamation outside the active interval. All reported measured
  I/O, asynchronous I/O, and major faults were zero.
- The full debug suite passed **295 tests, zero failures, five existing skips**.
  The 14 B-tree layout/configuration tests and three native blob-key tests passed
  in release and AddressSanitizer builds. ASan leak detection was disabled.
- The native tests now inject blob-read errors during insertion into both a root
  leaf and a tree with internal pivots, for lexical, custom, and zero-prefix
  configurations. Subsequent inserts and full-tree validation check that errors
  leave the tree usable and release locks. Existing tests also exercise UPDATE
  merging, old-result ownership, callback failure, blob lifetime, malformed
  persisted entries, and rejected formats.
- Main is frozen commit `d9a61a9abf6feadf017c951d965e2f3224b5eb30`. The pre-fix
  candidate is the format-4 working tree at `bb000cc3` plus compact statistics,
  attachment accounting, and the retained [packing fixes](PACK_PROFILE.md).
  All variants use the identical harness; hashes and incremental patches are
  archived. The topic branch is `robj/blob-keys`; changes remain uncommitted.
- Intel Core i7-12800H, CPU 2; GCC 14.2.0. Libraries use release
  `-O3 -ffast-math -flto`; the harness additionally uses `-march=native`.
  512 MiB cache and 2 GiB configured device. CPU frequency was not fixed; the
  recorded governor was performance with a 2.5 GHz cap. No settings were changed.
- Every measurement window held both benchmark and machine locks exclusively.
  No other registered project was active, and this owner had no child agents.
  Completion confirmations, requests, and releases are retained. No builds,
  tests, edits, or result analysis ran concurrently with measurement.
- The broad unprofiled matrix ran October 28, 21:57–22:01 America/Los_Angeles
  (2026-10-29 04:57:18–05:01:18 UTC). The focused confirmation ran October 28,
  22:02–22:04 (05:02:27–05:04:44 UTC). All locks were released.
- These are resident B-tree CPU measurements, not disk-bound, tail-latency,
  full-compaction, public API, or production-throughput measurements.

## Evidence

- [Focused comparisons and intervals](overwrite-results-20261028/focused-comparison.csv),
  [all focused pairs](overwrite-results-20261028/focused-pairs.csv), and
  [all 45 focused runs](overwrite-results-20261028/focused-runs.csv).
- [Broader comparisons](overwrite-results-20261028/comparison.csv),
  [paired samples](overwrite-results-20261028/pairs.csv), and
  [all 210 matrix runs](overwrite-results-20261028/runs.csv).
- [All hardware-counter runs](overwrite-results-20261028/perf-stat.csv),
  [identity, validation and coordination records](overwrite-results-20261028/evidence.json),
  and [retained source/test patch](overwrite-results-20261028/fix.patch).

Raw frozen sources, executables, libraries, perf records, disassembly, stdout,
stderr, test logs, manifests, and build/report scripts are retained in
`.cache/overwrite-profile-20261028/` in the topic worktree. The frozen `candidate`
profile is before this work; `final` is the retained implementation. The
`definitive` profile is a rejected experiment, despite being measured later.

Repeat the focused confirmation with a fresh output directory:

~~~sh
python3 .cache/overwrite-profile-20261028/run.py \
  --out .cache/overwrite-profile-20261028/focused-rerun --mode plain \
  --variants baseline candidate final --cases 64-nonlexical \
  --repetitions 15 --batches 16
~~~

The runner checks frozen binary/library hashes and requires current pause
acknowledgements from any newly active project before acquiring both locks.
