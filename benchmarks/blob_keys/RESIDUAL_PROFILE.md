# Remaining small-key regressions: profiling and fixes

Latest scan follow-up: [shared-prefix Scan4096 profiling and retained fix](SHARED_SCAN4096_PROFILE.md)
makes these scans 12.8–13.4% faster than original main across all four key sizes.
The retained fix also removes the intermediate random-scan tradeoff. All 882
runs of the final full matrix verified; custom insertion, overwrite, and hits
retain roughly 2% overhead.


Latest follow-up: [mutation profiling and fixes](MUTATION_PROFILE.md) remove
intermediate key/message metadata copies. Longer confirmation now finds +0.4%
custom overwrite and +1.7% insertion time versus main; shorter controls still
confirm roughly 2% insertion overhead. The measurements below are historical.

The retained fix removes most of the remaining point-lookup overhead. Against
**main with the same lexical pivot shortcut**, 64-byte random hit overhead falls
from **6.0% to 1.0%**, and custom-comparator hit overhead from **5.4% to 1.0%**.
Both final 95% intervals include zero. This supports no clear residual regression
in these two cases; it does not establish equivalence.

The overall no-small-key-regression objective remains unmet. Custom insertion
retains about **5.9%** overhead in the shorter control sweep, custom overwrite
retains **4.0%** in its longer confirmation, and 256-byte random hits retain
**2.5%**. The fix and all experiments stay in the `robj/blob-keys` worktree;
production changes in this round are confined to `src/btree.c`, with a boundary
validation case added to `tests/unit/btree_stress_test.c`.

## Focused uninstrumented comparisons

| Case | Before / matched main | After / matched main | After / main 95% interval | After / before |
|---|---:|---:|---:|---:|
| 64-byte random hit | +6.0% | +1.0% | -0.9% to +2.9% | -4.7% |
| 64-byte custom hit | +5.4% | +1.0% | -0.4% to +2.5% | -4.1% |
| 64-byte custom overwrite | +5.3% | +4.0% | +2.4% to +5.6% | -1.2% |

All percentages in this report's timing tables are **paired geometric-mean
elapsed-time changes**, so the displayed estimate and interval use the same
statistic. Negative means less time, not a throughput percentage. Each interval
uses Student-t on paired log ratios, transformed back to percentages. The CSVs
also retain paired medians, extrema, and every sample. Earlier reports primarily
displayed paired medians; do not compare those estimates as though they were the
same statistic or the same measurement window.

There are 15 repetitions per case, alternating variant order across seeds 42–56.
Each read run makes four million measured lookups in eight batches over 65,536
keys. Each overwrite run makes 16 random-permutation passes over 65,536 existing
keys, for 1,048,576 measured overwrites. Values are 32 bytes. The custom comparator
orders by the last eight key bytes; it must use the application callback.

Separate median absolute times are 512.1 / 551.4 / 522.5 ns for random hits,
787.9 / 824.6 / 791.5 ns for custom hits, and 995.4 / 1049.9 / 1031.1 ns for
custom overwrites (matched main / before / after). Ratios of these separate
medians need not equal the paired estimates above. After/before 95% intervals
are -5.8% to -3.7% for random hits, -5.4% to -2.8% for custom hits, and -2.6% to
+0.2% for overwrite. The overwrite improvement is therefore inconclusive.

## What changed and why

The first cycle profiles put approximately 25–29% of lookup samples in pivot
search and 13–14% in leaf search. The blob-capable implementation decoded keys
and selected general validation paths at each pivot comparison. Disassembly
also showed repeated by-value callback argument construction. Controlled variants
isolated three useful changes:

1. **Validate ordinary pivots directly.** After checking the entry offset and
   fixed header bounds, an entry with zero flags and an inline key length uses
   one bounds check for header, key, and three contiguous 32-bit counters. Blob,
   wide-counter, sentinel, unknown-flag, and oversized-key cases use the existing
   complete validator in a separate function. The common case avoids constructing
   a general internal key and selecting several encoded layouts.
2. **Compare lexical pivots from their stored header.** An ordinary inline pivot
   supplies its length and bytes directly to `slice_lex_cmp`. The existing
   lexical/callback specializations remain separate. This shortcut still requires
   the application's exact unsigned-byte lexical-order contract.
3. **Prepare callback arguments once per search.** For ordinary inline targets,
   pivot search prepares the target `user_key` outside the loop and builds the
   stored argument directly from the entry. Callback leaf search does likewise
   when the leaf has no common prefix. Query-key flags are preserved; stored keys
   carry a false query flag. Blobs, sentinels, and prefix-compressed leaves retain
   their existing comparison and materialization paths.

The entry encoding, format version, callbacks, comparator ordering, and blob
ownership are unchanged. Small keys still remain inline. The ordinary persisted
entry path checks that the counters fit in the page before returning the entry;
it does not merely validate the key bytes.

A same-window three-repetition counter screen shows the reduction in work:

| Workload / variant | Instructions / operation | Branches / operation |
|---|---:|---:|
| Random hit: matched main | 2238.4 | 464.7 |
| Random hit: before | 2617.5 | 507.8 |
| Random hit: after | 2344.5 | 476.1 |
| Custom hit: matched main | 2783.9 | 542.4 |
| Custom hit: before | 3162.6 | 609.1 |
| Custom hit: after | 2849.7 | 560.2 |

These counters are from `stat-direct`; they are instrumented measurements,
separate from the final timing comparisons. The changes reduce about 273
instructions per random hit and 313 per custom hit. Counter reductions alone
were not sufficient to select a fix: several other variants reduced work but
failed to improve time consistently.

## Remaining mutation overhead and rejected experiments

The custom overwrite screen still executes about 2904 instructions per operation
against 2818 on matched main. A separate cache-event screen found no increase in
L1/L2/L3 miss counts after the retained fix relative to main, and only a small
increase in split loads (11.9 versus 11.1 per overwrite). These measurements do
not attribute the remaining 4% elapsed-time gap to a specific memory event.

The final mutation profiles use 64 measured batches each, covering matched main,
the pre-change branch, and the retained code for insertion and overwrite. Pivot
search remains roughly 20–22% of retained samples and leaf incorporation roughly
15%. The insertion body and libc routines also account for substantial samples.
Function boundaries differ between variants, so sample shares are not a direct
measurement of the added cost. These profiles narrow follow-up work to mutation
search, argument passing, and incorporation; they do not establish a single
remaining cause.

Experiments retained only as evidence include simpler general validation,
pointer or forced-inline traversal, a different internal key flag layout,
scoped materialization scratch, global callback-argument zeroing, combined leaf
flag checks, and forced inlining of the insertion body. Changing key flag layout
or globally zeroing callback arguments substantially hurt custom lookups. Forced
inlining of insertion made the overwrite counter screen slower (1092 versus
1073 ns for the retained code in that window). None of those experiments is in
the selected source. Normal configuration validation and blob-capable fallbacks
remain necessary even when the incoming key itself is small.

## Broader uninstrumented controls

Seven paired repetitions per case and three variants: 273 runs. Reads use one
million timed operations; insertion uses four batches, and Scan 32 uses two
batches of 131,072 scan starts. These are exploratory per-case intervals, without
multiple-comparison adjustment. The broader 512-byte intervals are noisy.

| Case | Before / matched main | After / matched main | After / main 95% interval | After / before |
|---|---:|---:|---:|---:|
| 24-random-hit | +3.0% | -0.7% | -4.1% to +2.9% | -3.6% |
| 24-common-hit | +3.7% | -1.2% | -3.6% to +1.3% | -4.7% |
| 64-random-miss | +4.6% | +0.5% | -0.7% to +1.7% | -3.9% |
| 64-random-scan32 | -5.8% | -6.8% | -14.6% to +1.7% | -1.1% |
| 64-common-hit | +6.2% | +1.7% | -0.2% to +3.7% | -4.2% |
| 64-common-miss | +3.2% | -1.9% | -4.2% to +0.4% | -4.9% |
| 256-random-hit | +8.6% | +2.5% | +0.9% to +4.1% | -5.6% |
| 256-common-hit | +5.2% | +1.5% | +0.1% to +2.9% | -3.6% |
| 512-random-hit | +7.1% | +3.1% | -0.4% to +6.6% | -3.8% |
| 512-common-hit | +2.2% | +2.3% | -2.6% to +7.4% | +0.1% |
| 64-nonlexical-insert | +5.6% | +5.9% | +3.1% to +8.7% | +0.3% |
| 64-nonlexical-miss | +4.1% | +1.0% | -0.4% to +2.3% | -3.0% |
| 64-nonlexical-scan32 | -0.2% | +0.3% | -1.0% to +1.6% | +0.5% |

No control has a clear new regression relative to the pre-change branch. This
subset is not a rerun of all 63 cases: packing, long scans, and the full lexical
write matrix were not remeasured in this round. The remaining custom insertion
regression is not materially improved. Both 256-byte hit cases retain positive
intervals versus matched main, though the common-prefix result is marginal.

## Validation and measurement conditions

- All **408 uninstrumented runs, 192 counter runs, and 10 cycle profiles** passed
  verification and residency checks. All measured I/O and major faults were zero.
  Paired uninstrumented runs match operation counts, checksums, packed bytes,
  leaf counts, compressed-leaf counts, and prefix bytes. No successful sample or
  outlier was discarded. All cycle profiles report zero lost samples.
- The complete debug suite passed **295 tests, zero failures, five existing
  skips**. The three native blob-key tests also passed separately in debug.
  Release and AddressSanitizer builds each passed all 14 B-tree layout/config
  tests and all three native tests. Leak detection was disabled; these current
  sanitizer runs used ASan, not UBSan.
- The added malformed-index case enlarges the first key so its narrow counters
  extend past the page. The custom-order 400-to-401-byte case exercises the new
  ordinary-entry path; the lexical 512-to-513-byte case exercises maximum-length
  rejection. Synchronous and asynchronous lookups reject both, and the test
  restores the entry before proceeding. Existing tests cover wide counters,
  invalid flags, rejected formats, mixed/blob keys, prefix compression, failures,
  and ownership.
- Matched main is frozen `d9a61a9abf6feadf017c951d965e2f3224b5eb30` with the
  [previous lexical shortcut](POINT_HIT_PROFILE.md) applied in an isolated
  experiment. The main checkout is untouched. The pre-change branch is that
  report's retained `split` profile. This round's retained profile is
  `entry_direct_leaf`, also named `final`. All 110 production source/header hashes
  match the topic worktree. Branch HEAD is `bb000cc3`; working changes are uncommitted.
- Intel Core i7-12800H, pinned to CPU 2; GCC 14.2.0. Libraries use release
  `-O3 -ffast-math -flto`; the common harness adds `-march=native`. Cache is 512 MiB,
  configured device size 2 GiB. The performance governor had a 2.5 GHz maximum;
  frequency was not fixed, and no frequency settings were changed.
- Counter events were enabled only around timed user-mode operations and ran
  100% of enabled time without multiplexing. Event lists, commands, source and
  binary hashes, per-run outputs, and host observations are retained.
- Every measurement window held both coordination locks exclusively. Other
  registrations were complete, with a recorded exact-content completion
  confirmation for the stale async-state heading. This root had no child agents.
  No builds, tests, edits, or analysis ran during measurement. Each request has a
  matching release; all locks are released.
- Final timing windows ran October 29, 16:16–16:32 America/Los_Angeles
  (23:16–23:32 UTC); mutation profiles finished at 16:34. These are resident
  B-tree CPU measurements, not disk-bound, tail-latency, full-compaction, or
  public API performance results.

## Evidence and reproduction

- [Focused read comparisons](residual-results-20261029/plain-reads-comparison.csv),
  [pairs](residual-results-20261029/plain-reads-pairs.csv), and
  [runs](residual-results-20261029/plain-reads-runs.csv).
- [Overwrite comparisons](residual-results-20261029/plain-overwrite-comparison.csv),
  [pairs](residual-results-20261029/plain-overwrite-pairs.csv), and
  [runs](residual-results-20261029/plain-overwrite-runs.csv).
- [Control comparisons](residual-results-20261029/plain-controls-comparison.csv),
  [pairs](residual-results-20261029/plain-controls-pairs.csv), and
  [runs](residual-results-20261029/plain-controls-runs.csv).
- [Hardware counters](residual-results-20261029/perf-stat.csv),
  [cycle reports](residual-results-20261029/cycle-profile-summary.txt),
  [profile hashes](residual-results-20261029/cycle-profile-index.json),
  [source/test patch](residual-results-20261029/fix.patch), and
  [identity, validation, and coordination evidence](residual-results-20261029/evidence.json).

Frozen sources, binaries, libraries, raw perf data, stdout/stderr, validation
logs, and all build/report scripts are under `.cache/residual-profile-20261029/`
in this worktree; baseline and pre-change binaries remain in the preceding
campaign's `.cache/point-hit-profile-20261028/`. The evidence JSON records exact
paths and hashes. `report.py` regenerates the tables and checks source identity,
run validation, matching paired layouts, counter scheduling, and lost samples.
Individual experimental patches in the results directory are rejected or
intermediate variants; only `fix.patch` describes the cumulative retained change.

After coordinating a fresh exclusive window, repeat the focused read set with:

```sh
python3 .cache/residual-profile-20261029/run.py \
  --out .cache/residual-profile-20261029/new-plain-reads \
  --mode plain --variants baseline candidate final --repetitions 15 \
  --cases 64-random 64-nonlexical --batches 8
```

The runner acquires both locks and requires matching pause acknowledgements from
any active peer. Use a new output directory; the complete control and overwrite
commands are recorded in their raw manifests.
