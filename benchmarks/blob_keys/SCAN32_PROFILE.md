# Scan32 profiling and fixes

The subsequent [full format-4 matrix](SMALL_KEYS_V4.md) remeasures every small-key
B-tree operation after restoring compact contiguous statistics. The profiling
and measurements below describe the preceding fixed-64-bit candidate.

The largest Scan32 regressions came from an extra internal tree level and
avoidable work in iterator setup and tuple validation. The implemented changes
reduce the remaining median Scan32 overhead to 4.3% for 64-byte random lexical
keys and 4.2% for the 64-byte custom comparator. The 256-byte random case is
2.3% faster than main in the final sweep.

The no-small-key-regression objective is not yet established: the two 64-byte
cases retain overhead, and some paired ranges are broad. Small keys stay inline,
and the requested fixed-width uint64 statistics remain in place.

## Tree height explains the size-dependent slowdowns

Index-entry headers grew from 23 bytes on main to 35 bytes with three uint64
statistics. Leaves have the same occupancy, but internal-node fanout falls.
Direct inspection of the packed trees found:

| Case | Main levels, including leaf | Native levels, including leaf |
|---|---:|---:|
| 65,536 64-byte random keys | 3 | 4 |
| 65,536 64-byte custom-comparator keys | 3 | 4 |
| 32,768 256-byte random keys | 4 | 5 |

A controlled threshold comparison uses the **before-fix native candidate** and
main with 62,000 versus 63,000 random 64-byte keys. Both have three levels at
62,000 keys. At 63,000, only the native candidate grows to four levels.

| Records | Main ns/range | Before-fix native ns/range | Paired median overhead |
|---:|---:|---:|---:|
| 62,000 | 1,451.3 | 1,537.7 | +6.0% |
| 63,000 | 1,450.5 | 1,658.7 | +14.0% |

These are three paired repetitions per size. Main stays nearly flat while the
native tree adds about 121 ns/range. This supports the additional level as a
major contributor; matching leaf counts alone did not reveal it. The CSV
retains the paired ranges, including the noisier 62,000-key case.

## Implemented changes

1. **Defer the unbounded upper-endpoint walk.** Initialization used to traverse
   the tree again to find the rightmost page even when the caller requested
   only a short range. For pinned immutable iterators, that lookup now waits
   until a visited page claims to be last in its level. It still reconciles
   that page with the root and propagates lookup failures. A false early
   last-page marker returns an error instead of silently truncating the scan.
   Forward movement, reverse movement and seeks prepare bounds before returning.
   Finite upper bounds and mutable/copied-page semantics retain their paths.
2. **Combine full-entry bounds validation.** Reading a complete leaf tuple had
   started doing both key-only bounds checks and the full key-plus-message
   check. The complete span now bounds both, as on main. The full-key size limit
   rejects infinity encodings; the static assertion documents that constraint.
   Key-only searches retain their checks, and key flags, message flags/types,
   blob descriptors and prefix constraints are still validated.
3. **Keep blob parsing/comparison out of ordinary search code.** Explicit
   noinline attributes prevent link-time optimization from pulling descriptor
   scratch and blob comparison code into each pivot-search function. This had
   a modest benefit in isolation.

There is no persistent-format change or statistics-width rollback in this fix.

## Hardware-counter evidence

The following are per-range medians from two paired runs of the 64-byte random
case, with counters enabled only during the measured Scan32 loop. The final
unprofiled timing sweep below uses five pairs.

| Metric per 32-key range | Main | Before fix | Final |
|---|---:|---:|---:|
| Time, ns | 1,469.8 | 1,670.1 | 1,516.5 |
| User cycles | 3,661.3 | 4,160.8 | 3,778.0 |
| Retired instructions | 10,716.4 | 12,367.1 | 10,967.4 |
| Branches | 2,402.8 | 2,680.7 | 2,448.0 |
| Branch misses | 9.32 | 9.25 | 9.13 |

The final path removes about 1,400 instructions per range. The custom-comparator
case shows the same reduction, from 12,575.8 to 11,176.1 instructions/range.
The full-entry validation change alone removes about 12 instructions per
returned tuple in the long-scan counter experiment.

This was not another large store-forwarding regression: before-fix branch-miss
counts were similar to main, and blocked-forwarding counts were lower.
Cycle samples highlighted pivot search and cache-page acquisition. All accepted
cycle profiles report zero lost samples.

An intermediate endpoint-only change improved Scan32 but worsened long scans.
Its hot iterator instructions matched the old version after relocation, while
cycles increased: code layout was affecting performance. The exact processor
mechanism was not isolated. That version was not retained; the final version
also shortens validation and was remeasured across the complete matrix.
Intermediate binaries, counters and failed trial results remain in the raw
artifacts.

## Final unprofiled Scan32 results

Positive changes mean slower than main. Each value is the median of five
paired time ratios; the range is the smallest and largest paired change,
**not a confidence interval**.

| Key bytes | Distribution / comparator | Time change vs main | Paired range |
|---:|---|---:|---:|
| 24 | Random lexical | -2.9% | -5.0% to +0.5% |
| 24 | Shared prefix | -3.7% | -4.9% to -1.0% |
| 64 | Random lexical | +4.3% | -0.8% to +7.2% |
| 64 | Shared prefix | -3.3% | -4.7% to -1.4% |
| 64 | Custom comparator | +4.2% | +0.3% to +6.6% |
| 256 | Random lexical | -2.3% | -6.0% to +0.0% |
| 256 | Shared prefix | -4.6% | -5.4% to -2.3% |
| 512 | Random lexical | -10.7% | -11.5% to -8.8% |
| 512 | Shared prefix | -6.6% | -7.9% to -5.5% |

The [earlier remeasurement](SCAN32_RESULTS.md) found +14.1%, +12.0% and +9.4%
for 64-byte random, 64-byte custom and 256-byte random, respectively. Those
earlier values came from a different measurement window; the three-way counter
runs above provide a same-window before/final comparison for the 64-byte cases.

## Scan4096 confirmation

Three paired repetitions per case. This screen checks long scans after the
additional hot-path change; it does not prove universal performance neutrality.

| Key bytes | Distribution / comparator | Time change vs main | Paired range |
|---:|---|---:|---:|
| 24 | Random lexical | -2.3% | -2.8% to -2.1% |
| 24 | Shared prefix | -7.8% | -7.9% to -7.6% |
| 64 | Random lexical | -2.8% | -2.8% to -1.9% |
| 64 | Shared prefix | -6.2% | -6.5% to -4.5% |
| 64 | Custom comparator | +0.6% | -3.5% to +1.7% |
| 256 | Random lexical | +2.7% | +2.5% to +3.9% |
| 256 | Shared prefix | -7.4% | -7.4% to -6.9% |
| 512 | Random lexical | +1.8% | +0.1% to +2.4% |
| 512 | Shared prefix | -8.1% | -8.3% to -7.6% |

The 256/512-byte random cases retain small median long-scan overhead. The
intermediate version's roughly 8–12% regression in smaller random keys is gone.

## Reproducibility and validation

Main is frozen at `d9a61a9a`; the before-fix branch is `6689e29a` (production
code identical to `737b6161`). The final raw profile is named `combined`.
The candidate base commit, exact patch, all 110 source/header hashes, binary and
library hashes, and identical harness hash are recorded with the evidence.

Intel Core i7-12800H, pinned to CPU 2; GCC 14.2.0 with matching
`-O3 -ffast-math -flto -march=native` link/build configuration. Values are
32 bytes, cache 512 MiB, configured device 2 GiB. There are 65,536 records for
24/64-byte cases and 32,768 for 256/512-byte cases.

Final Scan32 uses four batches of 131,072 starts: 524,288 ranges and 16,777,216
returned tuples per run. Scan4096 uses two batches of 4,096 starts:
8,192 ranges and 33,554,432 tuples. Main/candidate order alternates, with matching
seeds within each pair. Setup, warmup, full replay verification and reclamation
are outside timing. Pinning and alternating order do not eliminate clock,
thermal or layout sensitivity.

- All 144 final timing runs verified, were cache resident, and had zero
  measured synchronous/asynchronous I/O, process I/O and major faults.
- Leaf counts, compressed-leaf counts, prefix bytes, returned counts and
  checksums match across every final pair.
- Full debug suite on the final source: 300 tests, 295 passed, zero failed,
  five skipped.
- Release native blob-key tests: 3/3; format/layout tests: 14/14.
- The new unbounded-iterator regression fails on the previous code (two root
  acquisitions instead of one) and passes on the final code. It also covers
  pinned/copied pages, both initialization APIs, forward/reverse movement,
  seeks, and false last-page markers.
- Formatting and whitespace checks passed.

The user confirmed no other agents were running; the live agent list contained
only this root. Both benchmark and machine locks were held exclusively for each
measurement window. No compilation, testing or analysis overlapped measurements.
This remains a resident direct-B-tree investigation; other operations and the
public API were not rebenchmarked.

## Evidence

- [Final paired comparisons and absolute times](scan32-profile-results-20261028/comparison.csv)
- [All final timing/validation rows](scan32-profile-results-20261028/timings.csv)
- [Per-run hardware counters](scan32-profile-results-20261028/counters.csv)
- [Height-threshold comparison](scan32-profile-results-20261028/height-threshold.csv)
- [Hashes and validation results](scan32-profile-results-20261028/evidence.json)
- [Raw artifacts](../../.cache/scan32-profile-20261028/): source patch, frozen
  profiles/binaries, layout diagnostics, perf data/reports, commands, manifests,
  stdout/stderr and correctness logs.

`campaign_combined.py` reproduces the final windows and
`summarize_combined.py` regenerates the checked CSVs and metadata. Run them from
the worktree root. The session-specific runner records this campaign's
no-other-agents confirmation; re-establish coordination before any future reuse.
