# Branch prefix compression: final evaluation

The final implementation has no apparent nonlexical throughput regression
above the requested 2-3% range in the measured resident B-tree workloads.
All nine estimates are within 0.9% of main, and all nominal 95% intervals have
their lower endpoint above -2.1%. This supports merging on performance grounds
under the chosen scope. It is a single-machine exploratory evaluation, not a
universal or simultaneous equivalence guarantee.

The implementation is commit `5fc43c310099ce06d72a7f2351ee7fbd600caa86`.
The reference is main `6105c49f66a24d3006ba0464d9558b52e692e27c`, also the remote
main tip checked on 2026-10-16. The final frozen variant is
`selected-io-alignment`, built from the original candidate `b9cc3ea4` plus the
selected read changes and I/O alignment repair. Applied runtime sources match
that build except for a recorded brace-formatting correction in `btree.c`.
Subsequent merge preparation adds tests, documentation and formatter handling;
it does not change runtime code.

## Nonlexical results

Positive values mean higher throughput. Random insertion uses a separate fixed
18-pair follow-up; the other rows use six pairs. The earlier, less precise
insertion result is retained in the evidence. No samples were discarded or
pooled between campaigns. [CSV](final-results.csv) and [JSON](final-results.json)
include absolute rates and instruction counts.

| Operation | Throughput vs main | Nominal 95% interval | Pairs |
| --- | ---: | ---: | ---: |
| Random insert | -0.26% | [-0.84%, +0.33%] | 18 |
| Sorted insert | +0.74% | [-2.07%, +3.64%] | 6 |
| Overwrite | -0.38% | [-1.82%, +1.08%] | 6 |
| Pack from array | -0.89% | [-1.83%, +0.06%] | 6 |
| Point hit | +0.30% | [-0.88%, +1.50%] | 6 |
| Point miss | -0.31% | [-1.14%, +0.54%] | 6 |
| One-key scan | -0.70% | [-1.85%, +0.46%] | 6 |
| 10,000-key scan | +0.90% | [-0.90%, +2.73%] | 6 |
| Full 200,000-key scan | +0.40% | [-0.73%, +1.54%] | 6 |

The selected read changes improved nonlexical 10,000-key and full scans by
4.25% and 4.93% relative to the original candidate. Compressed-key decoding and
blob-descriptor scratch are moved out of ordinary validation, allowing the
common validator to inline. Leaf search selects callback or lexical comparison
once before binary search, and message-type checking removes a redundant bound
protected by static assertions. Nonlexical keys still reference the page
directly; these paths add no decoded-key allocation.

Residual instructions relative to main are about +1.52% for packing,
+3.47-3.64% for point queries and one-key scans, and +2.34% for long scans.
Insert/overwrite instruction counts are essentially unchanged. The remaining
instruction differences did not produce a resolved throughput regression.

## Lexical tradeoffs and space

Users opting into lexical ordering retain the space/CPU tradeoff. In the
read-optimization confirmation before the alignment repair, common-prefix
packing was 7.53% slower than main and 10,000-key scans were 5.49% slower.
Point hits, misses and one-key scans were respectively 2.00%, 6.20% and 4.26%
faster. These comparisons use six paired repetitions; the complete intervals
are in the archived nonlexical investigation's lexical-control table.

The final aligned candidate's compressed full-scan control was -0.99%
[-2.74%, +0.80%] versus main. Other lexical operations were not retimed after
the alignment repair. The earlier complete on/off/main matrix is retained
separately, so its results are not attributed to the final binary.

Common-prefix packed allocation fell from 35.129 MiB to 25.379 MiB (27.75%),
with 6,261 leaves instead of 8,696. Nonlexical packed allocation and leaf count
were identical to main: 36,835,328 bytes and 8,696 leaves. Compression adds no
new I/O operation; the header grows by two bytes. Different packing boundaries
can still affect individual queries. This resident campaign makes no new
on-disk throughput claim.

## Scope and controls

Measurements use one worker on CPU 2 of an Intel i7-12800H, GCC 14.2 release
builds with `-O3 -flto -march=native`, 200,000 64-byte keys, 100-byte inline
values and a 512 MiB cache. The cache size is not a whole-process memory cap.
Scratch databases use real disk, while timed phases are resident: storage I/O,
asynchronous-I/O counters and major faults remain zero. Hardware counters
report full coverage. Shared locks serialize builds and benchmark workloads.

Writes target a mutable B-tree; point queries and scans use packed trees.
Packing reads from a prepared tuple array, excluding source-iterator tree
work, input generation/sorting, request setup and output reclamation. Scan
timing includes iterator initialization and teardown. Timed consumers
accumulate checksums; checksum comparisons and full correctness oracles run
outside timed phases. This is not a disk, concurrent, or blob-heavy performance
evaluation.

## Correctness and format compatibility

The read changes passed 87 release, 87 debug and 36 fault-injection test
executions. Sanitizers then exposed a preexisting asynchronous-I/O state
alignment defect, reproduced on the original candidate. The repair aligns the
opaque state buffer to `max_align_t` and checks backend alignment statically.
It passed 36 focused ASan/UBSan executions plus the cold custom-comparator
regression in release, debug and fault-injection builds. Original failure logs
and the repair evidence are preserved. LeakSanitizer was disabled; ASan and
UBSan remained enabled, without alignment suppression.

The [persisted-version verification](version-rejection.json) adds 42 passing
executions across release, debug and ASan/UBSan: all 12 B-tree tests plus the
invalid-reference and persisted-old-version tests in each build. Version 1 is
rejected before branch-page interpretation. Clean reopen is lazy; synchronous
and asynchronous point reads and range initialization reject an affected old
branch. Recovery rejects during mount when rebuilding allocation state.
Restoring version 2 permits cold remount and correct tuple retrieval. Existing
NULL-cache tests independently prove early rejection before branch-page
access. No migration or eager whole-tree validation is introduced.

`data_config.key_compare_is_lexical` promises exact unsigned-byte lexical order,
including shorter equal prefixes sorting first, for stored and query keys.
The default comparator opts in; applications installing a different ordering
must clear the flag. Compression is chosen per leaf with a four-byte minimum
common prefix. Uncompressed and mutable keys remain complete.

The repository formatter checks maintained source roots, including the new
benchmark harness, and excludes immutable benchmark snapshots and generated
build trees. The full maintained-source format check passes. Hosted PR CI is
separate from these recorded local checks.

## Evidence and reproduction

[evidence.tar.gz](evidence.tar.gz) contains the complete, unchanged
`benchmarks/results-20261015/btree-prefix` and
`benchmarks/results-20261016/btree-nonlexical` directories: 4,425 files,
including raw samples, compiler commands, source/artifact hashes, analysis
scripts, audits, and historical failures. Its
[manifest](evidence-manifest.json) records every member's SHA256.
The later investigation contains 1,206 samples across 19 campaigns; the earlier
matrix contains 516 comparison samples plus separately recorded pilot/smoke
work. The archives retain their original dated status statements.

[merge-validation.tar.gz](merge-validation.tar.gz) and its
[manifest](merge-validation-manifest.json) preserve both the initial test
fixture failure and the corrected, independently reviewed version test's
complete validation. [version-rejection.json](version-rejection.json) is a
direct copy of the final verification record.

Extract into a fresh directory to avoid touching existing campaign files:

```sh
mkdir -p /tmp/branch-prefix-evidence
tar -xzf benchmarks/branch-prefix-evaluation/evidence.tar.gz -C /tmp/branch-prefix-evidence
tar -xzf benchmarks/branch-prefix-evaluation/merge-validation.tar.gz -C /tmp/branch-prefix-evidence
```

The final analysis is under the extracted
`benchmarks/results-20261016/btree-nonlexical/README.md`; the complete earlier
on/off/main matrix is under `benchmarks/results-20261015/btree-prefix/README.md`.
Each includes measurement commands and provenance. Absolute build/database
paths in the records describe the measurement machine; another machine must
rebuild the frozen commits/patches and supply fresh paths.

The maintained [B-tree harness](../btree_prefix_bench.c) supports insert,
overwrite, lookup, scan and array-fed pack operations. The
[build helper](../build_btree_prefix_bench.py) links it against verified frozen
libraries using the recorded profile metadata. The harness exposes separate
construction and read-lexical switches for controlled comparisons and rejects
lexical flags for the nonlexical comparator.
