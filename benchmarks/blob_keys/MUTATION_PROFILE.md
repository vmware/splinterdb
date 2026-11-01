# Custom insertion and overwrite: continued profiling

Latest scan follow-up: [shared-prefix Scan4096 profiling and retained fix](SHARED_SCAN4096_PROFILE.md)
makes these scans 12.8–13.4% faster than original main across all four key sizes.
The retained fix also removes the intermediate random-scan tradeoff. All 882
runs of the final full matrix verified; custom insertion, overwrite, and hits
retain roughly 2% overhead.


Borrowing key and message metadata through the mutation helpers removes most of
the remaining overwrite regression. In the longer paired confirmation,
**overwrite overhead falls from 4.2% to 0.4%**, and **insertion overhead from
3.3% to 1.7%**. These are paired geometric-mean elapsed-time changes against the
same frozen main baseline used in the preceding report.

Both improvements relative to the pre-change branch have 95% intervals below
zero. Both remaining focused intervals versus main include zero. However, the
shorter control sweep still finds **2.4% insertion overhead**, with an interval
of **+1.5% to +3.4%**. The two campaigns are consistent with a small remaining
insertion cost; the overall no-small-key-regression objective is not established.
Overwrite is close to main in both campaigns. No broader control shows a clear
new regression relative to the pre-change branch.

## Focused uninstrumented confirmation

| Workload | Before / main | After / main | After / main 95% interval | After / before | After / before 95% interval |
|---|---:|---:|---:|---:|---:|
| Custom insertion | +3.3% | +1.7% | -0.5% to +3.9% | -1.6% | -2.9% to -0.3% |
| Custom overwrite | +4.2% | +0.4% | -0.3% to +1.1% | -3.6% | -4.5% to -2.8% |

Each case has 15 paired repetitions, seeds 42–56, with alternating variant order.
A run performs 16 measured batches over 65,536 keys: 1,048,576 inserts or
replacement overwrites. Keys are 64 bytes, values 32 bytes; the custom comparator
orders by the last eight bytes. Small keys remain inline in the actual
blob-capable implementation. Each batch builds a fresh mutable tree; overwrite
prefill and verification occur outside timing. The first batch is an unmeasured
warmup of the same path.

Separate median times are **971.7 / 1012.1 / 986.6 ns per insertion**, and
**988.3 / 1028.5 / 992.9 ns per overwrite** (main / before / after).
Paired median improvements are 2.6% for insertion and 3.1% for overwrite.
Paired median after/main overhead is 1.8% for insertion and 0.5% for overwrite.
These medians need not equal ratios of the separate absolute medians.

The table uses geometric means so its estimate and interval use the same
statistic. Intervals use Student-t on paired log ratios (14 degrees of freedom),
then transform back to percentages. The CSVs retain paired medians, extrema,
individual ratios, and every run. Negative means less elapsed time, not a
throughput percentage. No successful sample or outlier was discarded.

## Cause and retained change

The preceding mutation profiles concentrated in pivot search and leaf
incorporation, with additional samples around aggregate copies in the insertion
body. Disassembly showed partial metadata stores followed by wider reloads when
keys were repackaged for calls. Those sample locations motivated the experiments;
sampling skid alone cannot prove their cost.

The existing insertion wrapper already owns by-value key and message objects.
For large keys it also keeps the encoded-key buffer alive through the entire
insertion. The inner body nevertheless copied those metadata objects into more
locals, and copied them again into by-value search/incorporation arguments.

The retained change carries pointers to the wrapper's objects through:

- `btree_insert_encoded`, which now reads its borrowed arguments directly;
- mutable pivot-search dispatch and its lexical/custom wrappers;
- `btree_create_leaf_incorporate_spec`, which takes borrowed key/message inputs
  and reads them directly.

The incorporation specification still stores the metadata it needs. Its inputs
must remain valid for the call and must not alias the output specification; all
production callers use the insertion wrapper's independent objects. The two
private helper-test callers are adapted to pass addresses of their own inputs.
Application comparator and merge callbacks continue to receive their original
arguments, and fallible blob comparisons still propagate errors. No public API,
B-tree format, prefix behavior, or ownership rule changes.

The useful change is the complete path through these helpers. Merely changing a
callee to take pointers while immediately copying their contents into local
aggregates did not give a consistent improvement. Removing those intermediate
copies and retaining pointers across the next call boundaries did.

In the selected GCC build, insertion-body stack allocation falls from 920 to
840 bytes, and leaf-incorporation stack allocation from 424 to 392 bytes. A fresh
cycle profile puts the insertion body's sample share at about 2.7% for insertion
and 2.6% for overwrite, down from about 4.8% for both. Compiler outlining changes
the exact symbol boundaries, so these shares support the diagnosis but are not
an exact cycle attribution.

Final three-repetition hardware-counter measurements show:

| Workload / variant | Instructions / op | Branches / op | Branch misses / op | Blocked forwarding / op |
|---|---:|---:|---:|---:|
| insert / baseline | 2843.1 | 483.2 | 11.1 | 58.7 |
| insert / before | 2958.6 | 501.6 | 11.8 | 55.5 |
| insert / final | 2884.3 | 499.8 | 11.9 | 48.5 |
| overwrite / baseline | 2818.0 | 476.8 | 9.7 | 63.4 |
| overwrite / before | 2903.7 | 493.9 | 10.3 | 56.1 |
| overwrite / final | 2816.7 | 487.9 | 10.3 | 50.7 |

The fix removes about **74 instructions per insertion** and **87 per overwrite**.
Insertion retains about 41 additional instructions and 17 additional branches
versus main; its remaining elapsed-time cost has not been fully attributed.
Counter runs are instrumented and separate from the uninstrumented confirmation.
All events ran for 100% of enabled time without multiplexing, gated to the active
user-mode operation intervals. The final six cycle profiles lost zero samples.

## Experiments rejected

The artifacts retain the full progression, including negative results:

- Pointer parameters alone, forced inlining, a single inlined insertion body,
  separate checked/mutable leaf loops, a smaller cold comparison path, a compact
  pivot-search return value, and callback-argument reuse or a callback helper
  did not consistently improve both workloads.
- Ordinary conditional assignments of search bounds compiled back into the
  original branches. Explicit mask operations did remove branches and cut
  mispredictions, but increased elapsed time. For example, the combined mask
  variant reduced insertion branch misses from 11.8 to 4.6 per operation while
  increasing its screen time from 1011 to 1076 ns. It was rejected.
- A **benchmark-only inline-key control** bypassed stored blob/sentinel dispatch
  in mutable custom searches. It removed instructions and branches but did not
  consistently improve elapsed time. It cannot handle mixed/special keys and is
  deliberately excluded from the working source. No node metadata or format
  change was introduced based on that diagnostic.
- Further member-by-member specification copies did not improve on the retained
  complete borrowing path.

The working source contains only the retained argument-passing change. Lower
instruction counts, fewer branch misses, or a favorable short sample were not
sufficient by themselves to select a variant.

## Broader controls

All nine earlier small-key configurations were measured for both insertion and
overwrite, plus 64-byte random and custom hits: 20 cases, seven repetitions,
three variants, **420 verified runs**. Writes use four measured batches; reads
use one million timed lookups. These are exploratory per-case intervals with six
degrees of freedom and no multiple-comparison adjustment.

The main baseline has the earlier lexical shortcut in **checked reads**. Its
mutable lexical pivot search still calls the application comparator, whereas
both the pre-change and final blob-key branches already use the lexical shortcut
there. Consequently, the large lexical-write advantage over this baseline
**predates this patch**. The after/before columns measure this patch's effect.
This distinction does not affect the custom-comparator target workloads.

| Case | After / before | After / before 95% interval | After / main |
|---|---:|---:|---:|
| 24-random-insert | -2.8% | -12.6% to +8.0% | -29.4% |
| 24-random-overwrite | -0.5% | -6.4% to +5.8% | -26.8% |
| 24-common-insert | -2.0% | -3.9% to +0.0% | -29.3% |
| 24-common-overwrite | +0.6% | -3.2% to +4.5% | -26.6% |
| 64-random-insert | -2.0% | -5.9% to +1.9% | -28.4% |
| 64-random-overwrite | +0.3% | -4.5% to +5.2% | -24.4% |
| 64-random-hit | -1.8% | -5.5% to +2.0% | +0.6% |
| 64-common-insert | -6.0% | -8.0% to -3.9% | -27.3% |
| 64-common-overwrite | -2.6% | -6.6% to +1.6% | -22.6% |
| 256-random-insert | +0.7% | -0.2% to +1.6% | -23.6% |
| 256-random-overwrite | +0.4% | -0.7% to +1.5% | -25.5% |
| 256-common-insert | -5.2% | -9.3% to -1.0% | -22.0% |
| 256-common-overwrite | -1.2% | -5.4% to +3.2% | -20.4% |
| 512-random-insert | -0.1% | -0.7% to +0.6% | -18.2% |
| 512-random-overwrite | -0.1% | -1.2% to +1.1% | -20.1% |
| 512-common-insert | -1.0% | -1.4% to -0.7% | -19.3% |
| 512-common-overwrite | -0.6% | -1.2% to -0.0% | -18.5% |
| 64-nonlexical-insert | -3.5% | -5.9% to -1.0% | +2.4% |
| 64-nonlexical-overwrite | -3.0% | -4.0% to -2.1% | -0.0% |
| 64-nonlexical-hit | -2.0% | -3.4% to -0.5% | +1.5% |

The shorter custom overwrite control is effectively at main's time (-0.02%,
95% interval -0.74% to +0.71%). Custom insertion remains +2.43% (+1.47% to +3.40%).
Custom point hits remain +1.53% (+0.89% to +2.16%) versus main in this window,
while improving relative to the pre-change binary. Random hits have no clear
remaining gap (+0.57%, -2.15% to +3.36%). No read algorithm was changed; these
controls do not attribute timing shifts to a particular compiler-layout effect.
The full 63-case matrix, packing, scans, and public API performance were not
remeasured in this round.

## Validation and reproducibility

- **510 uninstrumented runs, 270 counter runs, and six fresh cycle profiles** all
  passed verification and residency checks, with zero measured I/O and major
  faults. Work counts and checksums match within every uninstrumented pair.
  Packed read layouts also match. Mutable-write layout fields in this harness
  are not populated, so matching zero layout fields does not establish identical
  mutable-tree geometry.
- The complete debug suite passed **295 tests, zero failures, five existing
  skips**. Separate debug, release, and ASan runs each passed all 14 B-tree
  layout/config tests and all three native blob-key tests. Existing native tests
  exercise mixed inline/blob keys, lexical/custom ordering, updates and old
  results, blob lifetimes, callback failures, and mutation blob-read failures.
  ASan leak detection was disabled; these sanitizer runs did not enable UBSan.
- Formatting and whitespace checks pass. Production changes for this round are
  `src/btree.c` and `src/btree_private.h`; `tests/unit/btree_test.c` adapts the two
  helper calls. All 110 production source/header hashes match the frozen final
  profile. Earlier unrelated working changes are preserved; no commit or push.
- `baseline` is frozen main `d9a61a9abf6feadf017c951d965e2f3224b5eb30` with the
  preceding checked-read lexical shortcut. `before` is the retained
  `entry_direct_leaf` implementation from [RESIDUAL_PROFILE.md](RESIDUAL_PROFILE.md).
  `final` is this worktree's validated source; `borrow_through` is the selected
  experiment before comment/style cleanup. Main's checkout is untouched.
- Intel Core i7-12800H, CPU 2; GCC 14.2.0. Library flags are release
  `-O3 -ffast-math -flto`, with `-march=native` additionally used for the unchanged
  harness. Cache is 512 MiB, configured device size 2 GiB. Governor is performance
  with a 2.5 GHz cap; frequency was not fixed or changed.
- All benchmark windows held both benchmark and machine locks exclusively.
  Other project completion records and the exact-content completion confirmation
  for the stale async-state heading were checked. This owner had no child agents.
  No builds, tests, edits, or result analysis ran during measurement. Requests,
  releases, and host observations are retained; all locks are released.
- Focused timings ran October 29, 21:50–21:53 America/Los_Angeles; controls ran
  21:54–22:01; final profiling finished at 22:04 (October 30, 04:50–05:04 UTC).
  These are resident B-tree CPU measurements, not disk-bound, tail-latency,
  concurrent-write, compaction-policy, or end-to-end database results.

## Evidence

- [Insertion comparisons](mutation-results-20261029/plain-insert-comparison.csv),
  [pairs](mutation-results-20261029/plain-insert-pairs.csv), and
  [runs](mutation-results-20261029/plain-insert-runs.csv).
- [Overwrite comparisons](mutation-results-20261029/plain-overwrite-comparison.csv),
  [pairs](mutation-results-20261029/plain-overwrite-pairs.csv), and
  [runs](mutation-results-20261029/plain-overwrite-runs.csv).
- [Controls](mutation-results-20261029/plain-controls-comparison.csv),
  [pairs](mutation-results-20261029/plain-controls-pairs.csv), and
  [runs](mutation-results-20261029/plain-controls-runs.csv).
- [Counters](mutation-results-20261029/perf-stat.csv),
  [cycle reports](mutation-results-20261029/cycle-profile-summary.txt),
  [profile hashes](mutation-results-20261029/cycle-profile-index.json),
  [retained patch](mutation-results-20261029/fix.patch), and
  [source, validation, environment, and coordination evidence](mutation-results-20261029/evidence.json).

Raw logs, frozen sources/binaries/libraries, disassembly, earlier profile sample
attribution, and build/report scripts are retained under
`.cache/mutation-profile-20261029/` in this topic worktree. Pre-change binaries
remain in the preceding campaign directories recorded by the profile metadata.
`report.py` regenerates the CSVs and checks identities, verification, matching
work/results, event scheduling, lost samples, and releases. Files prefixed
`experiment-` are diagnostic or intermediate patches; only `fix.patch` describes
the cumulative retained change.

To repeat a focused workload with a fresh output directory:

```sh
python3 .cache/mutation-profile-20261029/run.py \
  --out .cache/mutation-profile-20261029/new-overwrite \
  --mode plain --variants baseline before final --repetitions 15 \
  --cases 64-nonlexical --operation overwrite --batches 16
```

The runner acquires both locks and requires matching pause acknowledgements from
any active project. Use `--operation insert` for insertion. Full control and
profiling commands are recorded in the raw manifests.
