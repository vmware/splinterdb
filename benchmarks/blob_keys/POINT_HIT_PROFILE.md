# 64-byte random point-hit profile

Latest scan follow-up: [shared-prefix Scan4096 profiling and retained fix](SHARED_SCAN4096_PROFILE.md)
makes these scans 12.8–13.4% faster than original main across all four key sizes.
The retained fix also removes the intermediate random-scan tradeoff. All 882
runs of the final full matrix verified; custom insertion, overwrite, and hits
retain roughly 2% overhead.


Follow-up: [continued profiling](RESIDUAL_PROFILE.md) reduces the remaining
64-byte random and custom-hit overhead to about 1% against matched optimized
main; both intervals span zero. Custom-write regressions remain. The results
below describe the preceding implementation and measurement window.

The retained change reduces 64-byte random point-hit time to **566 ns**, versus
**781 ns before this change** and **738 ns on unmodified main**. Across 15 paired
repetitions, that is **23.2% less elapsed time than main** and **27.7% less than the
pre-fix branch**.

This is a general lexical-search optimization, not elimination of the blob-key
cost. Applying the same shortcut to main gives **538 ns/hit**. The retained branch
is still **6.0% slower than that optimized main**, with a 95% interval of
**+4.5% to +6.7%**. The added blob-key overhead remains, and the overall
no-small-key-regression objective is not yet met.

## Final focused comparison

| Comparison | Median elapsed-time change | 95% interval |
|---|---:|---:|
| Pre-fix branch / main | +5.9% | +4.9% to +7.0% |
| Retained branch / main | -23.2% | -24.2% to -22.5% |
| Retained / pre-fix branch | -27.7% | -28.3% to -27.1% |
| Main with lexical shortcut / main | -27.3% | -28.0% to -26.8% |
| Retained / main with lexical shortcut | +6.0% | +4.5% to +6.7% |

Each run performs four million successful lookups in eight measured batches over
65,536 random 64-byte lexical keys, with 32-byte values. The four variants alternate
order across seeds 42–56. All 15 retained/main pairs improved; all 15
retained/optimized-main pairs were slower. No samples were discarded.

Ratios are calculated within each repetition before taking medians. Intervals
use Student-t on paired log ratios (14 degrees of freedom), then transform back
to percentages. They estimate geometric-mean ratios, not intervals around the
displayed median.

## What profiling found

The initial counters reproduced the slowdown at about 775.5 versus 735.1 ns/hit:
the branch executed roughly **372 additional instructions and 65 additional
branches per hit**. Cycle samples concentrated in pivot search: 30.8% of branch
samples and 29.5% of main samples were in that function. Leaf search accounted
for about 10.5% in both. Both profiles had zero lost samples.

Pivot search built by-value callback arguments and invoked the application
comparator even when `key_compare_is_lexical` explicitly allowed unsigned
bytewise lexical comparison. Disassembly and sampled instruction offsets
identified callback-argument construction as a major cost in both versions.
The blob-aware path also adds entry/key validation and blob dispatch around
that existing cost. Samples can skid; the attribution to the callback path is
supported by the controlled optimization and counters, not sample addresses
alone.

A same-window counter comparison of the final code shows:

| Variant | Instructions/hit | Branches/hit | Blocked store-forwarding events/hit |
|---|---:|---:|---:|
| Main | 2,572.0 | 511.3 | 49.6 |
| Pre-fix branch | 2,944.4 | 576.2 | 53.4 |
| Retained branch | 2,617.5 | 507.8 | 10.9 |

A separate matched-optimization counter window found 2,238.4 instructions,
464.7 branches, and 3.0 blocked forwarding events per hit on optimized main,
versus 2,617.5, 507.8, and 10.8 on the retained branch. Thus roughly 379
instructions, 43 branches, and eight blocked forwarding events per hit remain.
Their exact contribution to elapsed time has not been isolated. Checked
entry/key decoding and the remaining blob-aware dispatch/argument construction
are follow-up targets; this experiment does not prove that every remaining
cycle belongs to validation.

These counters are medians of three instrumented repetitions, separate from the
unprofiled timing comparisons. All counters ran for 100% of enabled time without
multiplexing. Events were user-mode cycles, instructions, branches, branch misses,
and `ld_blocks.store_forward`, enabled only around the measured lookup batches.

## Retained implementation and rejected experiments

The retained change is confined to `src/btree.c`. Pivot search dispatches once
between lexical and callback loops. The lexical loop compares two ordinary
inline keys directly with `slice_lex_cmp`, only when the existing lexical flag
is enabled. Blob and sentinel comparisons keep the fallible existing path.
Custom ordering, query-key semantics, and persisted-entry checks are preserved.
There is no on-disk format change.

The search body remains shared source, with explicit lexical/callback and
checked/mutable specializations. Four small wrappers force separate compiled
loops. This matters: the first implementation added the lexical branch inside
the common loop and improved lexical hits, but regressed custom-comparator
insertion, overwrite, and hits by about 10–12% relative to the pre-fix branch.
Simply moving the condition or splitting only lexical/callback functions still
left GCC combining paths; those two intermediate variants were inspected but
not timed. The retained four-way specialization removes the observed custom
regression.

A separate experiment moved blob-key materialization scratch initialization out
of ordinary inline lookups. It made little difference in the counter screen
and was not retained. Its combined version and the rejected common-loop
implementation are preserved as experiments, not current code.

## Final controls

Seven paired repetitions per case, with main, pre-fix, and retained variants:
126 verified runs. Reads use one million operations per run; insertion and
overwrite use four measured batches.

| Case | Retained / main | Retained / pre-fix | Retained / pre-fix 95% interval |
|---|---:|---:|---|
| 64-byte random miss | -26.0% | -27.6% | -29.8% to -26.0% |
| 64-byte shared-prefix hit | -20.2% | -23.9% | -26.1% to -19.7% |
| 512-byte random hit | -17.1% | -18.6% | -19.4% to -18.0% |
| 64-byte custom insertion | +5.0% | -0.1% | -2.7% to +2.0% |
| 64-byte custom overwrite | +5.3% | -2.3% | -4.7% to +3.0% |
| 64-byte custom hit | +6.8% | +0.2% | -1.6% to +3.9% |

The custom controls show no clear new regression; they do not establish
equivalence. Their previous overhead relative to main remains. These exploratory
per-case intervals use six degrees of freedom without multiple-comparison
adjustment. This is a control subset, not a rerun of the full 63-case matrix.
Packing and scans were not remeasured after this change.

## Validation and measurement conditions

- The retained code passed the full debug suite: **295 passed, zero failed,
  five existing skips**. All 14 B-tree layout/configuration tests and three
  native blob-key tests also passed in release and AddressSanitizer builds.
  ASan leak detection was disabled. No new test was added: existing native
  tests cover lexical/custom ordering, zero-prefix blobs, mutable and packed
  lookups, corruption, and propagated blob-read errors.
- Across all experiments, **372 unprofiled runs, 54 counter runs, and two cycle
  profiles** passed correctness and residency checks. Of those, 186 unprofiled
  runs compare the retained code. Measured reads/writes, asynchronous I/O, and
  major faults were zero. Paired checksums, allocation bytes, leaf counts,
  compressed-leaf counts, and prefix bytes matched.
- Baseline main: `d9a61a9abf6feadf017c951d965e2f3224b5eb30`. Pre-fix branch:
  `bb000cc3` plus the existing format-4, attachment-accounting, packing, and
  overwrite changes. The retained code is still uncommitted in
  `robj/blob-keys`. Optimized main is a frozen experimental copy; main itself
  was not edited. Source, library, executable, and identical harness hashes
  are retained, and the current production source matches the measured copy.
- Intel Core i7-12800H, pinned to CPU 2; GCC 14.2.0, release
  `-O3 -ffast-math -flto`, identical harness with `-march=native`.
  512 MiB cache, 2 GiB configured device. CPU frequency was not fixed.
- Every measurement window held both benchmark and machine locks exclusively.
  Other registered projects were complete; this owner had no children.
  No builds, edits, tests, or result analysis ran during measurements.
  All locks are released.
- Final controls ran October 28, 23:14:59–23:18:48 America/Los_Angeles;
  final focused comparison ran 23:19:06–23:22:58
  (2026-10-29 06:14:59–06:18:48 and 06:19:06–06:22:58 UTC).
- Resident B-tree CPU measurements only: no storage-bound, public API,
  production tail-latency, or full-compaction claim.

## Evidence

- [Final focused comparisons](point-hit-profile-results-20261028/plain-final-comparison.csv),
  [pairs](point-hit-profile-results-20261028/plain-final-pairs.csv), and
  [runs](point-hit-profile-results-20261028/plain-final-runs.csv).
- [Final controls](point-hit-profile-results-20261028/controls-final-comparison.csv),
  [pairs](point-hit-profile-results-20261028/controls-final-pairs.csv), and
  [runs](point-hit-profile-results-20261028/controls-final-runs.csv).
- [All hardware counters](point-hit-profile-results-20261028/perf-stat.csv),
  [retained source change](point-hit-profile-results-20261028/fix.patch), and
  [identities, validation, and coordination](point-hit-profile-results-20261028/evidence.json).
- Earlier candidate comparisons, rejected patches, frozen sources and binaries,
  raw stdout/stderr, `perf.data`, disassembly, and analysis scripts are preserved
  under `.cache/point-hit-profile-20261028/` in this worktree.
