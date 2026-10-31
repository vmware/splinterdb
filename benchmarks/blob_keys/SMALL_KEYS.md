# Small-key regression benchmarks — native blob-key format

The [complete format-4 rerun](SMALL_KEYS_V4.md) supersedes these historical
B-tree results. It covers all 63 cases after the scan fixes and contiguous
32/64-bit statistics, with seven paired repetitions per case.

The candidate measured on October 26 **did not meet the no-small-key-regression objective**.
The public API loses 7.3–9.5% throughput on successful point lookups and
2.7–9.3% on the measured ranges. Direct B-tree long scans lose 24.9–39.2%.
Small keys remained inline throughout these experiments.

These measurements compare the native B-tree changes, including the requested
uint64 statistics and format version changes, against main at `6e550e1e`.
They are separate from the older forced-blob codec experiments in RESULTS.md.
The working branch is `robj/blob-keys`; native changes and performance changes
were uncommitted at measurement time. Public large-key writes are still outside this implementation.

The subsequent [Scan4096 profiling and fix](SCAN4096_PROFILE.md) rebases onto
newer main and identifies the large scan slowdown. It supersedes the Scan4096
conclusion for that newer candidate. A fresh [Scan32 remeasurement](SCAN32_RESULTS.md)
also shows substantial recovery, with remaining regressions. The historical
data below remain unchanged.

## Public API results

Each cell is the median of three paired candidate/baseline throughput changes.
Negative numbers mean fewer operations per second. Each pair uses the same seed;
run order alternates. These are bounded throughput samples, not tail-latency or
long-term stability claims.

| Operation | Log off, 1 thread | Log off, 4 threads | Log on, 1 thread | Log on, 4 threads |
|---|---:|---:|---:|---:|
| Random load | -1.0% | -7.3% | -3.8% | -4.1% |
| Point hit | -8.3% | -9.5% | -8.0% | -7.3% |
| Point miss | -2.6% | -1.5% | -2.5% | +5.9% |
| Scan 32 | -7.9% | -9.3% | -6.3% | -5.9% |
| Scan 4096 | -5.2% | -7.3% | -5.4% | -2.7% |
| Overwrite | -0.6% | -5.4% | -0.5% | -1.5% |

Load throughput above excludes final close/drain. Including that drain, median
elapsed time increases were 2.7%, 11.9%, 4.2%, and 2.7%, respectively, in the
same column order. Do not substitute operation throughput for completed-load
cost.

Public setup: 1,000,000 keys, 24-byte keys, 100-byte values, 512 MiB cache,
4 GiB configured device, 24 MiB memtable, one memtable worker, four normal
background workers, process affinity CPUs 0–7. Each timed lookup/range/overwrite
phase has one second of warmup and three seconds of measurement. Load has a
fixed key count, closes/drains, and reopens before the read phases. A full
verified scan precedes reads; point results, range order/values, and overwrite
samples are checked. The `warm` phase is retained in the CSV but is a warmup
and verification pass, not an independently stabilized scan benchmark.

## Direct B-tree results

Representative cases below use the same throughput convention. Full results for
all 63 cases, including paired timing ranges, are in
[final-v3.csv](small-key-results-20261026/final-v3.csv).

| Keys / comparator | Insert | Overwrite | Hit | Miss | Scan 32 | Scan 4096 | Pack |
|---|---:|---:|---:|---:|---:|---:|---:|
| 24 random lexical | +2.5% | -3.9% | -3.6% | -4.0% | -19.9% | -39.2% | -8.3% |
| 24 common prefix | +2.2% | -3.3% | -1.6% | -1.1% | -17.0% | -34.5% | -12.4% |
| 64 random lexical | -0.1% | -5.4% | -13.2% | -14.9% | -24.4% | -37.7% | -11.2% |
| 64 common prefix | +1.2% | -5.9% | -0.7% | -1.9% | -15.4% | -34.4% | -6.2% |
| 256 random lexical | -0.7% | -3.1% | -12.8% | -12.2% | -20.0% | -30.2% | -1.3% |
| 512 random lexical | -1.1% | +0.1% | -1.7% | +1.2% | -10.2% | -24.9% | -3.3% |
| 512 common prefix | -0.1% | +0.1% | -2.7% | -1.7% | -14.0% | -33.8% | -3.8% |
| 64 custom comparator | -8.9% | -11.1% | -14.6% | -13.5% | -22.1% | -37.6% | -7.3% |

The matrix covers 24/64/256/512-byte lexical keys with random and shared-prefix
distributions, plus a 64-byte nonlexical comparator. Values are 32 bytes.
There are 65,536 keys at 24/64 bytes and 32,768 keys at 256/512 bytes. Cache size
is 512 MiB; configured device size is 2 GiB; the process is pinned to CPU 2.
Lookups execute one million measured operations. Short/long scans process
approximately 8.4/33.6 million tuples. Insert, overwrite, and pack use four
batches; reads use two. Setup, warmup, full verification and teardown are outside
the active timing interval.

All measured direct B-tree operations were cache resident, with zero measured
I/O according to the harness hooks and process I/O counters. Every run checks
values, checksums, and allocator reclamation. This isolates CPU overhead; it
does not establish cold-cache or storage-bound performance.

Leaf counts and compressed-prefix bytes match baseline. Some packed allocations
increase due to the larger fixed-width index entries. There was no ablation
separating the required uint64 statistics changes from blob dispatch, so the
remaining regressions cannot all be attributed to one feature.

## Iterations and validation

Three complete campaigns produced 1,158 valid process runs:

- `screen-v1b`: 378 runs of the initial native implementation. For example,
  24-byte random long scans took 98.0% more time and packing took 73.0% more time.
- `screen-v2`: 378 runs after preserving the original 24-byte in-memory key
  size, inlining ordinary comparisons, separating descriptor parsing from
  ordinary leaf reads, and reusing the pack key buffer.
- `final-v3`: 378 B-tree runs plus 24 public API runs, after simplifying logical
  key construction in iterator `curr()` and moving index-only materialization
  out of leaf validation. All 402 final runs passed their checks. This last
  change did not eliminate the scan regression; do not describe it as a proven
  scan performance fix.

The combined changes reduced the 24-byte random packing time regression from
73.0% to 9.1%; long-scan time remained 64.5% above baseline. The residual CPU
cost still needs profiling and isolation before performance sign-off.

Correctness validation: the v2 full debug suite passed 246 tests, with four
existing skips and zero failures. After the v3 changes, all three native
blob-key tests, all 14 B-tree layout/configuration tests, and all 62 public API
quick tests passed. The final full 250-test suite was not repeated after v3.
The queued-owner lock protocol was separately checked with an existing owner,
a replacement request ID, and lock release/acquisition.

## Evidence and reproduction

Machine: Intel Core i7-12800H, GCC 14.2.0, release `-O3 -ffast-math -flto
-march=native` on both sides. Both sides use byte-identical benchmark harness
sources. The final campaign ran 2026-10-26 23:58:14 through 2026-10-27 00:13:51 UTC.

Every measured window held both benchmark and machine locks exclusively and
saved fresh acknowledgements that all other active project roots and children
were paused. No compilation, source work, or result analysis ran during these
windows. Other projects' windows were also respected.

Raw artifacts are under
[`.cache/small-key-bench-20261026`](../../.cache/small-key-bench-20261026/):
source snapshots, source/binary/library/harness hashes, compiler commands,
patches, per-run stdout/stderr and CSV, full manifests, lock requests, pause
acknowledgements and release records. The initial `screen-v1` attempt was
aborted by scratch-file cleanup after one run; its data are excluded. The
cleanup was corrected before `screen-v1b`.

Maintained summary evidence:

- [Profile hashes and validation](small-key-results-20261026/evidence.json)
- [Initial candidate](small-key-results-20261026/screen-v1b.csv)
- [First optimization](small-key-results-20261026/screen-v2.csv)
- [Final candidate and public API](small-key-results-20261026/final-v3.csv)

To rerun the frozen final profiles from the topic worktree:

```sh
python3 benchmarks/blob_keys/small_keys.py \
  --profiles .cache/small-key-bench-20261026/profiles.json \
  --public-profiles .cache/small-key-bench-20261026/public-profiles.json \
  --out .cache/small-key-bench-20261026/new-run \
  --coordination /home/rob/projects/splinterdb/.cache/projects-20261025/coordination \
  --lock-dir /home/rob/projects/splinterdb/.cache/projects-20261011/coordination \
  --variants baseline candidate-v3 --repetitions 3 --expected-minutes 20

python3 benchmarks/blob_keys/summarize_small_keys.py \
  .cache/small-key-bench-20261026/new-run/manifest.json --candidate candidate-v3
```

Only invoke the runner after all local builds, tests and child-agent work stop.
The runner now polls an occupied reservation and acknowledges its current owner
while queued; this avoids the handoff stalls encountered in the recorded
campaigns. The manifest snapshots the runner actually used for each campaign.
