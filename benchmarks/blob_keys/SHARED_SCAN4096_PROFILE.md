# Shared-prefix Scan4096 profiling and fix

The shared-prefix Scan4096 regression is resolved in the tested workloads.
The retained implementation is **12.8–13.4% faster than original main** by
paired median across 24-, 64-, 256-, and 512-byte shared-prefix keys. A longer
15-pair confirmation finds **18.1% less time than the pre-fix branch** for
64-byte shared-prefix scans.

An initial inlining-only fix introduced about 1.2% overhead on 24-byte random
scans. A second change isolates blob/index result construction from ordinary
leaf results and removes that tradeoff. The retained version is **4.5% faster
than the pre-fix branch** and **2.0% faster than main** for 24-byte random scans
in the longer confirmation.

The retained source received a fresh complete 63-case matrix: **882/882 runs
verified**, seven pairs per case, with zero measured I/O or major faults.
No long-scan case has a clearly positive slowdown interval. The overall
no-small-key-regression objective remains open: custom insertion, overwrite,
and point hits retain roughly 2% overhead.

## Focused confirmation

These are 15 paired repetitions, seeds 42–56, alternating variant order.
Each run has eight measured batches of 4096 starts scanning 4096 tuples:
134,217,728 returned tuples. Negative time changes mean faster execution.

| Case / comparison | Paired median | Geometric mean | Geometric-mean 95% interval |
|---|---:|---:|---:|
| 64-common / before versus baseline | +6.4% | +6.0% | +5.0% to +7.1% |
| 64-common / retained versus baseline | -12.7% | -12.9% | -14.0% to -11.8% |
| 64-common / retained versus before | -18.1% | -17.9% | -18.7% to -17.0% |
| 24-random / before versus baseline | +2.7% | +2.6% | +1.9% to +3.3% |
| 24-random / retained versus baseline | -2.0% | -1.9% | -2.8% to -1.0% |
| 24-random / retained versus before | -4.5% | -4.4% | -5.1% to -3.6% |

Baseline is unmodified frozen main; before is the exact binary from the preceding
full report; retained is the final working source. The experimental profile
named final is the intermediate inlining-only version, not the retained source.
Intervals use Student-t on paired log ratios (14 degrees of freedom), transformed
back to percentages. They estimate geometric-mean ratios, not medians.

## Diagnosis and retained change

The initial same-window comparison reproduced the slowdown: about 19.9 ns/tuple
on original main, 20.1 ns on the earlier format-4 candidate, and 21.1 ns on the
pre-fix branch. The latter two execute essentially the same number of
instructions and branches; the slower binary has fewer blocked store-forwarding
events. This is not the earlier one-stall-per-tuple aggregate-copy problem.

Samples concentrate in iterator advance, current-result construction, and
compressed-key decoding. An alignment-only diagnostic recovered part of the
difference without reducing instructions, showing code-layout sensitivity.
This investigation did not isolate the later regression to one prior change
or identify a precise microarchitectural bottleneck.

The retained change reduces recurring work:

1. Explicitly inline btree_iterator_decode_current(), reconstructing the suffix
   in the positioning path. Allocation, growth, and prefix replacement stay
   in the existing out-of-line prepare_decode() helper.
2. Explicitly inline btree_iterator_check_current() too. Inlining only the
   decoder made GCC move the enlarged entry checker out of next(), introducing
   a call per tuple on uncompressed leaves. Keeping both inline avoids that.
3. Move aggregate construction into btree_iterator_curr_blob_key() and
   btree_iterator_curr_index(). In this GCC build, the ordinary curr() function
   drops from 241 to 79 assembly lines. Local stack allocation falls from
   64 to 8 bytes; one additional saved register makes the total stack-use
   reduction 48 bytes.

Every validation, bounds check, prefix preparation, blob materialization,
callback, and error path remains. Positioning still finishes fallible work
before curr() exposes an entry. Ordinary leaf keys retain the direct member
assignments from the earlier store-forwarding fix. No public API, ownership
rule, persistent format, iterator structure, or compression policy changes.

Only src/btree.c changes in this round. No bounds rewrite or explicit function
alignment is retained.

## Hardware evidence

Medians of three counter runs, counting only active user-mode scans. All events
ran for 100% of enabled time without multiplexing.

| Case / variant | ns/tuple | Cycles/tuple | Instructions/tuple | Branches/tuple |
|---|---:|---:|---:|---:|
| 64-common / baseline | 19.76 | 49.24 | 261.54 | 62.37 |
| 64-common / before | 21.14 | 52.66 | 262.67 | 61.38 |
| 64-common / retained | 17.29 | 43.09 | 247.67 | 56.38 |
| 24-random / baseline | 16.08 | 40.04 | 221.07 | 52.23 |
| 24-random / before | 16.54 | 41.20 | 224.20 | 53.23 |
| 24-random / retained | 15.76 | 39.26 | 223.21 | 50.25 |
| 64-random / baseline | 16.95 | 42.23 | 224.23 | 52.87 |
| 64-random / before | 17.22 | 42.91 | 227.53 | 53.89 |
| 64-random / retained | 16.59 | 41.34 | 226.55 | 50.91 |

For shared-prefix keys, the retained source removes about **15 instructions,
five branches, and 9.6 cycles per tuple** versus the pre-fix branch. For 24-byte
random keys it removes about one instruction and three branches. Store-forwarding
event counts remain very small.

The decoder no longer appears as a separate sampled function because its work
is inlined. All nine cycle profiles lost zero samples. Sample shares change
when functions are inlined or outlined; they are not an exact cycle attribution.

## Full matrix on retained source

Median elapsed-time changes against original main. Positive means slower,
negative means faster. All small keys remain inline.

| Key bytes | Distribution / comparator | Insert | Overwrite | Hit | Miss | Scan32 | Scan4096 | Pack |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 24 | Random lexical | -30.0% | -28.9% | -26.4% | -26.8% | -14.6% | -2.4% | -12.1% |
| 24 | Shared prefix | -29.4% | -28.7% | -26.7% | -27.2% | -20.7% | -12.9% | -11.6% |
| 64 | Random lexical | -29.0% | -25.7% | -26.6% | -26.4% | -15.3% | -2.4% | -14.0% |
| 64 | Shared prefix | -27.0% | -24.5% | -24.2% | -25.4% | -18.8% | -12.8% | -12.8% |
| 256 | Random lexical | -24.2% | -23.2% | -22.6% | -25.1% | -17.2% | +0.2% | -4.7% |
| 256 | Shared prefix | -21.8% | -22.1% | -20.2% | -19.3% | -19.0% | -13.4% | -8.5% |
| 512 | Random lexical | -18.5% | -20.0% | -19.0% | -24.0% | -18.9% | -1.7% | -16.4% |
| 512 | Shared prefix | -18.5% | -18.4% | -16.2% | -17.3% | -18.9% | -13.3% | -4.2% |
| 64 | Custom comparator | +2.4% | +2.1% | +1.8% | +0.1% | -0.8% | -2.0% | -11.6% |

The per-case geometric-mean intervals classify **55 cases as faster,
3 as slower, and 5 as overlapping zero**.
These are exploratory intervals without correction across 63 comparisons.

All shared-prefix Scan4096 cases have intervals below zero. So do 24-, 64-,
and 512-byte random long scans and custom long scans. The 256-byte random case
is +0.2% by median, with a geometric-mean interval of -0.1% to +0.7%: inconclusive,
not proof of equivalence.

The remaining clear regressions are:

| Case | Paired median | Geometric mean | Geometric-mean 95% interval |
|---|---:|---:|---:|
| 64-nonlexical-insert | +2.4% | +2.3% | +1.5% to +3.1% |
| 64-nonlexical-overwrite | +2.1% | +2.0% | +1.2% to +2.9% |
| 64-nonlexical-hit | +1.8% | +1.7% | +0.9% to +2.5% |

All packing medians remain faster than main, but intervals for 256- and 512-byte
shared-prefix packing overlap zero in this window. Outliers were retained.
Large lexical-write and lookup gains include the earlier lexical shortcut;
this baseline does not have the modified-main shortcut used in focused profiles.

Historical long-scan comparison against the same main binary:

| Case | Previous full-suite median | Retained full-suite median | Retained geometric-mean 95% interval |
|---|---:|---:|---:|
| 24-random-scan4096 | +2.9% | -2.4% | -7.8% to -0.2% |
| 24-common-scan4096 | +4.3% | -12.9% | -13.1% to -12.4% |
| 64-random-scan4096 | +1.4% | -2.4% | -2.8% to -2.0% |
| 64-common-scan4096 | +6.7% | -12.8% | -13.2% to -11.7% |
| 256-random-scan4096 | +1.4% | +0.2% | -0.1% to +0.7% |
| 256-common-scan4096 | +5.2% | -13.4% | -15.4% to -12.4% |
| 512-random-scan4096 | +1.0% | -1.7% | -2.6% to -0.2% |
| 512-common-scan4096 | +5.2% | -13.3% | -13.6% to -12.9% |
| 64-nonlexical-scan4096 | +1.6% | -2.0% | -2.2% to -1.8% |

Those full-suite columns come from different windows. The 15-pair focused
comparison above provides direct same-window evidence for the change.

## Rejected and intermediate experiments

- Inlining only the decoder helped compressed scans, but outlining the entry
  checker added overhead to uncompressed scans.
- A separate helper wrapping decode/materialize/error handling increased call
  overhead. Combining that helper with an inline decoder helped less than
  inlining the positioning path.
- Inlining only the entry checker did not help.
- Explicit alignment changed timing with unchanged instruction counts; it was
  diagnostic and is not retained as a production setting.
- The two-function inline version passed a full matrix but introduced a
  confirmed +1.24% geometric-mean cost on 24-byte random scans (95% interval
  +0.88% to +1.59%). The retained implementation improves that case relative
  to the pre-fix branch.
- A compact rewrite of the existing bounds predicates was slower and rejected.

The intermediate full matrix remains in the artifacts. The current full matrix
is full-retained; fix.patch is the cumulative retained change.

## Validation and method

- Retained source: complete debug suite **295 passed, zero failed, five existing
  skips**. Separate debug, release, and ASan runs each passed 14 B-tree
  configuration/layout tests and all three native blob-key tests. Existing
  coverage includes prefix reconstruction, mixed inline/blob keys, custom
  ordering, forward/backward and copied iterators, endpoint errors, and blob-key
  read failures.
- ASan leak detection was disabled; UBSan was not enabled. Early standalone
  native-test invocations printed usage but returned zero; invalid logs are
  retained and not counted as tests. Valid invocations ran all three cases.
  The runner now checks the reported test summary.
- **2,085 measurements verified: 1,941 plain runs, 135 counter runs, and nine
  cycle profiles.** This includes both full matrices, intermediate screens,
  the tradeoff check, and focused confirmations.
- Paired plain runs match work counts, checksums, verified values, and reported
  packed layout counters. Active I/O and major faults are zero. Mutable layout
  counters are unpopulated; matching zeros do not establish identical mutable
  geometry. Tree height was not instrumented.
- Original main: d9a61a9abf6feadf017c951d965e2f3224b5eb30. Before: the exact prior
  full-suite candidate. Retained: robj/blob-keys working source on bb000cc3.
  All 110 production source/header hashes match the frozen retained source;
  only btree.c differs from the start of this round.
- GCC 14.2.0, release -O3 -ffast-math -flto; identical harness additionally uses
  -march=native. Intel Core i7-12800H, CPU 2, performance governor and unchanged
  2.5 GHz cap. Frequency was not fixed; between-run observations are not
  active-operation clock measurements.
- Full matrix: 24/64/256/512-byte random/shared-prefix lexical keys and 64-byte
  custom ordering by the last eight bytes; 32-byte values. 65,536 records for
  24/64-byte keys and 32,768 for larger keys. Cache 512 MiB, device 2 GiB.
  Four measured batches for insert/overwrite/pack, two for reads; one million
  lookups, about 8.4 million Scan32 or 33.6 million Scan4096 tuples per run.
  Seeds 42–48, seven alternating pairs. Setup, warmup, verification, and
  reclamation are outside timing.
- Full-matrix intervals use paired-log Student-t with six degrees of freedom.
  No successful sample or outlier was discarded.
- Every window held both benchmark and machine locks exclusively after checking
  project completion/current acknowledgements. The exact-content completion
  confirmation for the unchanged async-state registration is retained. No child
  agents; no builds, tests, edits, or analysis during measurement. Requests and
  matching releases are preserved; all locks are released.
- Scope: resident single-threaded B-tree CPU performance, not disk-bound,
  public API, large-key/blob throughput, concurrency, compaction-policy, or
  tail-latency measurements. No commit or push.

## Evidence

Retained full matrix: 2026-10-30 00:26:30–00:49:18 PDT (2026-10-30 07:26:30–07:49:18 UTC).

- [Retained full matrix](shared-scan4096-results-20261030/full-retained-comparison.csv),
  [441 pairs](shared-scan4096-results-20261030/full-retained-pairs.csv),
  [882 runs](shared-scan4096-results-20261030/full-retained-runs.csv).
- [Focused comparisons](shared-scan4096-results-20261030/plain-retained-comparison.csv),
  [pairs](shared-scan4096-results-20261030/plain-retained-pairs.csv),
  [runs](shared-scan4096-results-20261030/plain-retained-runs.csv).
- [Counters](shared-scan4096-results-20261030/perf-stat.csv),
  [cycle reports](shared-scan4096-results-20261030/cycle-profile-summary.txt),
  [raw profile hashes](shared-scan4096-results-20261030/cycle-profiles.json).
- [Cumulative patch](shared-scan4096-results-20261030/fix.patch) and
  [source, validation, samples, coordination](shared-scan4096-results-20261030/evidence.json).

Raw binaries/libraries, frozen sources, logs, disassembly, manifests, counter
and record data, and scripts remain under .cache/shared-scan4096-profile-20261029/.
Earlier source/binary locations are recorded in profiles.json.

Repeat the full matrix with a fresh output directory:

~~~sh
python3 .cache/shared-scan4096-profile-20261029/run_full.py \
  --profiles .cache/shared-scan4096-profile-20261029/retained-full-profiles.json \
  --out .cache/shared-scan4096-profile-20261029/new-retained-matrix \
  --coordination /home/rob/projects/splinterdb/.cache/projects-20261025/coordination \
  --lock-dir /home/rob/projects/splinterdb/.cache/projects-20261011/coordination \
  --variants baseline candidate --repetitions 7 --expected-minutes 25
~~~

The runner requires matching pause acknowledgements from active projects.
The focused run.py accepts --variants baseline before retained, --mode
plain/stat/record, --cases 64-common 24-random, --batches 8, and --repetitions 15.
Analyze completed windows with analyze.py, then generate this narrative with
write_report.py; the narrative uses the original full-retained campaign.
