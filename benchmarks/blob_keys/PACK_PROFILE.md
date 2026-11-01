# Packing profile and fix

The packing regression is resolved in this resident small-key sweep. The combined
fix is faster than main in all nine cases by paired median, ranging from 2.4% to
17.4% less elapsed time. Eight per-case intervals exclude zero; the 256-byte random
case remains inconclusive. This does not resolve or remeasure the point-lookup and
custom-comparator write regressions in [the earlier full matrix](SMALL_KEYS_V4.md).

## Cause and implementation

The dominant cost was aggregate argument copying before `btree_pack_append_leaf()`.
With GCC 14.2 and LTO, the caller assembled `key` and `message` fields using small
stack stores, then loaded larger vector-sized aggregates to pass them by value.
The candidate incurred about one additional blocked store-forwarding event per
packed tuple. Cycle samples concentrated around those aggregate loads; ordinary
cycle samples can skid, so the counter change and isolated ablation provide the
stronger evidence than assigning a precise stall to one sampled instruction.

Changing the private helper to take `const key *` and `const message *` removes the
extra aggregate argument copies. The counter falls from about 2.2–2.4 blocked
forwarding events per tuple to about 1.1–1.2. The event count is not zero: some
forwarding stalls are common to both implementations.

A second cost was scanning every entry in a full leaf to discover whether it
contained a blob key before finding a common prefix. As proposed during review,
`btree_pack_req` now keeps `leaf_has_blob_key`. It is initialized at pack setup,
reset on starting a leaf, and set after successfully appending a blob key. Prefix
compression caps its comparison length at P when that flag is set. It does not
need to scan the leaf. This is transient packing state; the on-disk format and
entry flags are unchanged.

The existing append rule still starts a new leaf when a blob key encounters C>P.
Small keys remain inline. Lexical prefix shortcuts still require lexical ordering.
The native test now places inline keys both before and after blob keys and asserts
that later inline leaves regain prefixes longer than P, exercising flag reset.

## Isolated counter experiments

These are medians of three instrumented runs of 24-byte random lexical packing,
64 measured batches per run. Counters cover only active pack calls. Every counter
ran 100% of its enabled time; none was multiplexed. Times here are diagnostic,
not the final unprofiled comparison.

| Variant | ns/tuple | Cycles/tuple | Instructions/tuple | Blocked forwarding/tuple |
|---|---:|---:|---:|---:|
| Main | 37.894 | 94.467 | 274.961 | 1.170 |
| Candidate before fix | 43.827 | 109.202 | 306.218 | 2.245 |
| Argument pointers only | 34.625 | 86.306 | 303.218 | 1.115 |
| Per-leaf flag only | 44.730 | 111.458 | 299.156 | 2.220 |
| Both changes | 33.504 | 83.506 | 296.093 | 1.130 |

Passing arguments by pointer is the dominant improvement. The flag removes work,
but alone did not consistently improve elapsed time. With both changes, the
64-byte random case took 41.729 ns/tuple versus 51.952 before the fix; the 256-byte
shared-prefix case took 60.062 versus 68.447. These ablations do not establish that
the flag improves every workload: the pointer-only 256-byte shared-prefix median
was 58.270 ns/tuple. An earlier conditional-scan ablation is also retained in the
raw data; the implemented flag avoids the scan entirely.

Four cycle profiles covered main and the pre-fix candidate on 24- and 64-byte
random keys, with zero lost samples. Sampling used `cpu_core/cycles/u`, period
1,000,003, and DWARF call chains. One earlier attempt failed before measurement
because its perf ring buffer exceeded the available mapping allowance; retrying
with 64 pages succeeded without changing machine settings. Failed setup is
retained separately and excluded from performance results.

## Unprofiled confirmation

All 189 runs passed: nine cases × seven repetitions × three binaries. Values below
are medians of matched elapsed-time ratios. Negative means less time, not a
throughput percentage. No outliers were discarded. Before/main and after/main
were measured in the same window.

| Key bytes / distribution | Before/main | After/main | After/before | After/main 95% interval |
|---|---:|---:|---:|---|
| 24-random | +12.7% | -13.4% | -21.8% | -22.8% to -7.8% |
| 24-common | +5.4% | -14.8% | -19.2% | -16.8% to -11.4% |
| 64-random | +10.8% | -10.6% | -19.7% | -22.5% to -2.6% |
| 64-common | +7.9% | -12.4% | -19.1% | -14.6% to -8.3% |
| 256-random | +5.4% | -2.4% | -7.5% | -8.0% to +0.4% |
| 256-common | +9.7% | -6.1% | -13.7% | -9.3% to -3.7% |
| 512-random | +3.5% | -17.4% | -20.5% | -19.3% to -16.2% |
| 512-common | +5.3% | -5.7% | -8.9% | -8.2% to -2.4% |
| 64-nonlexical | +8.3% | -9.4% | -16.4% | -13.1% to -4.5% |

The intervals use Student-t on seven paired log ratios and then transform back to
percentages. They estimate geometric-mean ratios, not the displayed median, and
are exploratory per-case intervals without adjustment for multiple comparisons.
The 256-byte random interval includes a small slowdown. Some baseline runs were
noisy; the per-run results and all paired ranges are retained.

Leaf counts, compressed-leaf counts, prefix bytes, and packed allocation bytes
matched across all three variants for every seed. Every run reported zero measured
I/O, zero asynchronous I/O, and zero major faults. Values, checksums, ordering, and
allocator reclamation were verified outside the timed region.

## Method and validation

- Baseline: main `d9a61a9abf6feadf017c951d965e2f3224b5eb30`, the same frozen binary
  used for the full format-4 matrix. Pre-fix candidate: `bb000cc3` plus the frozen
  format-4 compact-statistics and attachment-accounting changes. Fixed candidate:
  that same source with the two packing changes. Source/library/binary hashes
  and isolated patches are preserved.
- Intel Core i7-12800H, pinned to CPU 2; GCC 14.2.0. Libraries use release
  `-O3 -ffast-math -flto`; the identical harness additionally uses `-march=native`.
  The host used intel_pstate's performance governor with a recorded 2.5 GHz cap;
  frequency was not fixed. No machine settings were changed.
- Same nine packing configurations as the earlier matrix: 24/64/256/512-byte
  lexical keys, random and shared-prefix distributions, plus a 64-byte custom
  comparator; 32-byte values. 65,536 input records for 24/64-byte keys and 32,768
  for 256/512-byte keys; 512 MiB cache, 2 GiB device. Final timings use 16 batches
  rather than the earlier matrix's four, with seeds 42–48 and alternating variant
  order. Setup, verification, and reclamation are outside active timing.
- Both benchmark and machine locks were held exclusively for every measurement
  window. No other project was active and the owner had no children. Registration
  confirmations, requests, and releases are archived. No builds, edits, or result
  analysis ran inside measurement windows.
- Final unprofiled window: 2026-10-29 04:22:08–04:24:10 UTC (October 28,
  21:22–21:24 America/Los_Angeles). Both locks were released.
- After the fix: 14 B-tree layout/configuration tests and three expanded native-key
  tests passed in debug, release, and AddressSanitizer builds. All 66 public quick
  tests passed in debug. ASan leak detection was disabled. The complete unit suite
  was not rerun for this local packing change.
- This is a resident packing CPU comparison. It does not establish disk-bound,
  full compaction, public API, tail-latency, or other-operation performance.

## Evidence and reproduction

- [All unprofiled runs](pack-results-20261028/runs.csv),
  [paired samples](pack-results-20261028/pairs.csv), and
  [comparisons and intervals](pack-results-20261028/comparison.csv).
- [All 99 hardware-counter runs](pack-results-20261028/perf-stat.csv).
- [Identities, coordination and test evidence](pack-results-20261028/evidence.json).
- [Final incremental source and test patch](pack-results-20261028/packing.patch).

Raw frozen sources, binaries, stdout/stderr, perf records, disassembly, validation
logs, manifests, and build/report scripts remain in `.cache/pack-profile-20261028/`
in this topic worktree. The `stat-leafbit` campaign contains the final isolated
ablation; `plain-final` contains the unprofiled confirmation.

With the frozen profiles in place, repeat the unprofiled sweep with a fresh output
directory from this worktree:

~~~sh
python3 .cache/pack-profile-20261028/run.py \
  --out .cache/pack-profile-20261028/plain-rerun --mode plain \
  --variants baseline candidate combined --repetitions 7 --batches 16 \
  --cases 24-random 24-common 64-random 64-common 256-random 256-common \
          512-random 512-common 64-nonlexical
~~~

The runner verifies frozen binary/library hashes and requires current pause
acknowledgements from any newly active project before measuring under both locks.
