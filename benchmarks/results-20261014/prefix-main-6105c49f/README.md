# Prefix integration and performance assessment, 2026-10-14

Merged `origin/main` at `6105c49f66a24d3006ba0464d9558b52e692e27c` into
`robj/branch-prefix` as `97e5c0e38b818ae43cd3abdcf8d4489b7dbe3789`.
The new main change bounds automatic log groups; it merged without conflicts.
A final fetch after measurements still found main at `6105c49f`.
The custom IPv4 comparator example now clears `key_compare_is_lexical`, as its
ordering does not meet that flag's contract. No measured runtime source changed.

## Merge assessment

The branch is suitable for a review PR, but this evidence does not support
merging the default-enabled implementation yet. The strongest performance
blocker is resident nonlexical length-1 scans: throughput fell 5.36% in the
initial ABBA and 5.73% in three longer paired repetitions, with nonoverlapping
sample ranges in both. Compression is disabled for this comparator, so the
workload pays overhead without receiving compression's space benefit.
The repeat's mean p99 increased 7.23%.

Common-prefix resident length-10,000 scans averaged 14.97% slower initially and
12.10% slower in the longer repeat. Both sets have overlapping sample ranges,
so these are recurring concerns, not precise estimates of a universal slowdown.
The initial 20.10% compression-disabled long-scan loss did not reproduce:
the longer repeat was -2.38% at eight readers and +1.89% at one reader, both
with overlapping ranges. Historical random resident scan losses also did not
reproduce in the new initial matrix. Retain these distinctions when reviewing.

Logged random overwrites lost 9.98% including close/drain in the initial screen,
and 6.09% in the repeat. The latter ranges overlap substantially; these short
overwrite runs do not establish steady-state insertion performance or an
isolated regression caused by the new log-group policy.

Shared-prefix disk benefits remain clear in this matrix: points +10.45%,
length-1 scans +10.86%, length-10,000 scans +20.29%, with nonoverlapping
throughput sample ranges. Their read bytes per operation fell 9.06%, 22.75%,
and 29.09%, respectively. File allocation fell 34.73% on disk and 30.16% in
the resident fixture. Mounted live extents fell 24.28% and 25.15%.

There is also unconditional iterator memory overhead. DWARF from the measured
libraries gives `sizeof(btree_iterator)` 336 -> 488 and
`sizeof(core_range_iterator)` 94,592 -> 133,504 bytes: 38 KiB more per public
iterator, including nonlexical and compression-disabled workloads. The core
iterator embeds 256 branch iterators. These are object sizes, not measured RSS.

Source review identifies costs worth profiling before a merge: owned full-key
buffers on the scan path, reconstruction for compressed leaves, and packing
even when a small prefix does not provide space for another tuple. The buffer
layout and allocation changes are candidates for the nonlexical overhead;
key reconstruction cannot explain a comparator with compression disabled.
No causal attribution or performance repair was established in this update.

The subsequent [nonlexical diagnosis](../prefix-nonlexical-diagnosis/README.md)
confirmed the uncompressed key path already skips reconstruction and measured
avoidable iterator setup/state costs. Its lazy-state prototype reduced a later
batch's throughput gap from 1.96% to 0.46%, with compressed-path validation still
outstanding. The original results above remain unchanged.

The lazy-state fix has since been applied and tested, including compressed
pages, allocation failures and leak detection. See the
[fix validation archive](../prefix-lazy-6105c49f/README.md) for its measurements
and updated merge assessment. This archive describes the pre-fix implementation.

## Validation and measurement scope

Debug and release each passed 141 unit cases: B-tree 12, superblock 17, merge
10, public API 57, internal Splinter 44, and automatic-log process-exit recovery
1. Both functional log runs passed and replayed 500,000 records. This covers
prefix roundtrip/reopen, predicate scans, blobs, and the new automatic log
barrier behavior. All build targets passed, including the corrected example.

The benchmark campaign completed 68 query/overwrite invocations: 28 reads,
16 overwrites, and 24 targeted followups, plus 17 fixture load/space pairs.
Every completed invocation succeeded; all phase rows have zero errors and
correctness violations. Reads use seven-second phases, overwrites eight, and
followups fifteen. Initial comparisons use ABBA; followups use three alternating
pairs (A B / B A / A B). The [generated report](report.md) and [summary CSV](summary.csv)
retain all results, including contrary and overlapping evidence.

All database files use O_DIRECT on ext4 `/dev/mapper/snafu-root_crypt`, with CPU
affinity 0-11. Benchmarks hold the existing campaign's exclusive benchmark and
machine locks; tests completed before timing. Each query uses a separate fsynced
copy of a fixed per-format image. Logical keys, seed, cache size, and harness
source match. Default logged runs exercise main's automatic group limit.
These are closed-loop workloads, not offered-rate latency measurements.

Resident configurations use 200,000 64-byte keys, 100-byte values and a 128 MiB
cache; disk configurations use two million keys and a 64 MiB cache. Loading
uses one thread, no background workers and a 4 MiB memtable. The random-key
generator retains a 16-byte-per-logical-key array in both versions. This is an
equal-cache comparison, not an enforced total-memory cap; the iterator object
growth above is additional candidate memory. Fixture loading is measured only
once and is too short for a sustained-write conclusion. CPU frequency and
uncoordinated host activity are not controlled; sample ranges show substantial
resident variability. Sample ranges are not statistical confidence intervals.

## Reproduction and provenance

`build.json` records commits, all runtime source hashes, compiler/link commands,
build flags, library hashes, and identical generated harness hashes. Baseline
runtime was exported from main with `git archive`, then built with
`make -B -j8 BUILD_MODE=release GIT_VERSION=6105c49f libs`. Candidate debug and
release builds use `BUILD_ROOT=build-main-6105c49f`. The existing
`benchmarks/build_prefix_bench.py` builds both harnesses. No database images or
binaries are committed here; the files remain under the worktree's `.cache`
and build directories. Earlier unrelated build directories are preserved.

`tests/validate.py` reproduces build/test commands. `run_benchmarks.py --mode
reads` and `--mode writes` reproduce the first matrix; their caller must hold
the campaign locks. `followup.py` acquires those locks itself. Runs deliberately
refuse to overwrite existing result directories or images: use a fresh archive
and scratch destination for a new campaign. `report.py` regenerates the table
and CSV from recorded samples. `audit.py` verifies hashes, source correspondence,
test results, and serialization and records iterator object sizes.

Codex workspace-requirements errors interrupted orchestration during preparation;
they were not test failures. The completed process logs and manifests are the
evidence for successful execution.
