# Branch leaf prefix experiment

`data_config.key_compare_is_lexical` opts construction into prefix compression.
It promises unsigned-byte lexicographic ordering for stored keys and query
keys, including the rule that a shorter equal prefix sorts first. The default
data configuration opts in. Applications replacing its comparator must clear
the flag unless their replacement preserves this contract.

When a branch leaf first fills, packing extracts the longest common prefix of
its first and last keys, rebuilds entries as suffixes, and continues filling the
page. A later key outside that prefix starts another leaf. Prefix bytes occupy
the page tail, so offset-table growth never moves them. A final partially full
leaf is left uncompressed. Index pivots and mutable memtable keys stay complete.

Point reads compare the prefix once, then binary-search suffixes. A mismatch
can place the query on either side of the leaf. Range iterators reconstruct
full keys into owned buffers, copying the prefix only when changing leaves.
Equal-length successive keys retain the existing buffer, and suffix copies of
at most 16 bytes use bounded inline copies.
Positioning operations perform any allocation before returning; observing the
current tuple stays infallible. Bounds, seeks, asynchronous lookup, diagnostics,
parent pivots and logical key-byte accounting use the full key semantics.

This prototype now uses branch format version `2` within main's superblock
format `8`. Branch readers reject format `1` before interpreting its different
page header. Earlier standalone prefix images used superblock `0x8007` and are
rejected. The lexical opt-in controls construction only; existing compressed
leaves remain readable after changing that setting on reopen.

Debug validation passed all 48 public quick tests. The new regression checks
compression enabled/disabled, actual compressed pages, close/reopen, point hits
and misses, asynchronous lookup, zero-length suffixes, forward/reverse ranges,
repeated seeks, full-key-only lexical callbacks, and prefix mismatches on both
sides. All 15 superblock tests pass, including rejection of the other independent
experiment formats. After the reconstruction optimization, all eight B-tree
tests (including bounded suffix copies) and the prefix roundtrip pass.
Performance remains workload dependent; the completed measurements follow.

`benchmarks/build_prefix_bench.py /path/to/tree` compiles identical generated
kvbench source against baseline and candidate libraries and records source and
binary hashes. The campaign uses the same generator from its coordination
directory. It produces `build/release/bin/prefix_kvbench` with the existing
kvbench phase arguments and additional environment controls:

* `PREFIX_PATTERN=common`: 64-byte keys with 48 fixed leading bytes.
* `PREFIX_PATTERN=random`: uniformly dispersed first words with random tails.
* `PREFIX_PATTERN=nonlexical`: a comparator orders the final eight-byte rank;
  compression stays disabled.
* `PREFIX_DISABLE=1`: disable candidate compression for an overhead control.

Use the same pattern, seed and key count on reopen. The random workload keeps
two eight-byte words per logical key in the harness to generate ordered ranks;
this memory is identical in baseline and candidate. Space mode reports live
allocation statistics and filesystem allocation after close. Report load plus
drain, point throughput, short/long range throughput and space together: saved
leaf bytes can trade off against rebuilding and key reconstruction.

# Measurements

All runs used O_DIRECT files on ext4, CPU affinity 0–11, 64-byte keys,
100-byte values and WAL disabled. Loading used one thread and zero background
workers for reproducible construction within each format. Baseline and candidate
formats require separate images. The random workload's 16-byte/key harness
array is identical for both variants. Resident runs used 200,000 keys and a
128 MiB cache; disk runs used 2 million keys and a 64 MiB cache.

| Common-prefix space | Resident | Disk |
| --- | ---: | ---: |
| Mean allocated file bytes, baseline | 37,408,768 | 833,912,832 |
| Mean allocated file bytes, candidate | 26,128,384 | 545,222,656 |
| File allocation reduction | 30.2% | 34.6% |
| Mounted live extents, baseline → candidate | 342 → 256 | 3,414 → 2,585 |
| Live extent reduction | 25.1% | 24.3% |

Filesystem allocation includes written pages that were subsequently freed;
live extents measure current allocator ownership and include constant empty
memtable allocations. Per-page-type allocation counters are not persisted and
are not used to interpret reopened images. Random and nonlexical inputs showed
no live-extent reduction.

The final common-prefix measurements use copies of fixed per-format images.
Resident order was baseline / previous candidate / final candidate / final
candidate / previous candidate / baseline. Both candidate binaries read the
same candidate image. Disk order was ABBA. Read phases lasted seven seconds.
Positive changes below mean more throughput than baseline.

These full fixed-image matrices used buffered `cp` without an explicit fsync
before O_DIRECT open/warm/timing. Pending copy writeback, including unreachable
historical pages, may confound their results. This is a method limitation, not
proof of the cause of any earlier outlier. The initial seven pattern/control
matrices constructed fresh O_DIRECT databases and did not use this copy path.
The bounded fsynced disk repeat described below checks two final-reader phases;
the original full matrices are preserved here for comparison.

| Final common-prefix reader | Resident | Disk |
| --- | ---: | ---: |
| Point, 1 reader | +0.9% | +11.0% |
| Point, 8 readers | +1.0% | +14.0% |
| Length-1 range, 1 reader | +4.7% | +17.0% |
| Length-1 range, 8 readers | +1.9% | +9.8% |
| Length-10,000 range, 1 reader | +0.8% | +24.2% |
| Length-10,000 range, 8 readers | -2.9% | +19.7% |

The final reconstruction path improved the resident eight-reader long scan by
10.1% relative to the previous candidate on identical images. Disk I/O per query
fell about 9% for points, 23% for length-1 ranges and 29% for long ranges. The
initial disk common-prefix matrix had one severely slow candidate repetition;
the fixed-image repeat above did not reproduce it.

The earlier five-second ABBA controls used the previous reconstruction path.
Random inputs changed resident point throughput by +0.3–0.5%, while long-range
throughput ranged from -5.9% to +1.5%. Disk random-input point throughput was
4.3–5.7% lower and ranges were 1.2–7.6% lower. Nonlexical resident point throughput
was 1.6–3.0% lower, and its eight-reader long range was 10.8% lower. Disabling
compression on common-prefix input left space unchanged and point throughput
within 1.1%, but eight-reader long ranges were 7.1% lower. These controls do not
establish a universally neutral overhead, and they are not final-reader results.

The final nonlexical disk control repeated fixed per-format images in ABBA
order. Point throughput changed by -0.0% / -1.4%, length-1 ranges by -1.4% /
-2.6%, and long ranges by +0.4% / -1.2% (one/eight readers). I/O per operation
remained comparable. This did not reproduce the earlier isolated 8-reader
collapse, but the small negative differences remain part of the measured cost.

A bounded followup fsynced each copied disk fixture before O_DIRECT open and
warmup, then repeated only point and length-10,000 queries with eight readers.
It used the same frozen final binaries, the same per-format images, ABBA order
and seven-second phases. All four runs passed with no errors or violations.

| Fsynced common-prefix disk control | Baseline ops/s, min–max | Candidate ops/s, min–max | Mean throughput change | Read bytes/query change |
| --- | ---: | ---: | ---: | ---: |
| Point, 8 readers | 138,877–161,839 | 169,695–187,194 | +18.7% | -9.1% |
| Length-10,000 range, 8 readers | 810.7–840.3 | 1,039.3–1,049.8 | +26.5% | -29.0% |

CPU efficiency increased 3.6% for points and 13.9% for long ranges. Baseline
rates still varied between the two repetitions, so these throughput deltas are
not precise universal estimates. The large gains and read-I/O reductions persist
after explicitly synchronizing the copies. This control does not establish that
copy writeback caused any earlier outlier, and it does not replace unmeasured
phases or the nonlexical and resident controls.

All completed samples report zero correctness violations and errors. Loads were
short (subsecond resident, a few seconds on disk), so their throughput is not a
strong write-performance conclusion. The experiment is promising for large
shared prefixes, with a demonstrated space/read-I/O tradeoff; broader adoption
still requires resolving workloads with little prefix benefit and the remaining
resident long-scan cost.

Compact CSVs, manifests, build hashes and campaign runners are retained in
`benchmarks/branch-prefix-history/results-20261011/`. Full original artifacts and images remain in
`.cache/projects-20261011/benchmarks/branch-prefix/` in the main checkout.

# Main integration, 2026-10-13

Merged `origin/main` at `7bb5fab305b9c6179e31f75998dabea833537a08`,
including caller-owned transferable cache handles, cache-read error propagation,
hierarchical blobs, and wide B-tree statistics. The merge required no source
adaptations. Lexical opt-in, per-leaf prefix encoding, full-key reconstruction,
and the independent experimental superblock format remain in place.

Debug and release each passed 77 checks: 50 public API quick tests, eight
B-tree tests, 15 superblock tests, and four blob tests. This covers compressed
point/async lookups, forward/reverse scans and seeks, reopen, iterator transfer,
three-level blob trees, wide statistics, and injected cache-read failures.
The builds use `BUILD_ROOT=build-main-7bb5fab` with the default compiler. Raw
logs, commands, binary hashes, and the source manifest are recorded in the
main checkout under `.cache/projects-20261011/coordination/main-7bb5fab/branch-prefix/`.
The performance measurements above predate this merge and do not establish the
performance of the merged implementation.

## Fresh benchmarks after main integration (2026-10-13)

Common-prefix resident point throughput improved 10.61%, while the random-key resident long-range workload lost 10.88%, both with non-overlapping sample ranges. Common-prefix disk long-range throughput improved about 14.4%. The experiment retains a workload-dependent cost; it is not performance neutral.

These are updated-feature comparisons against main `7bb5fab`, not an isolated
measurement of merge cost. Main controls drifted; observed range overlap is
descriptive, not a statistical confidence interval. Exact samples, build/source
hashes, commands, verification scope and fresh correctness evidence are in
[the integration archive](../benchmarks/branch-prefix-history/results-20261013/main-7bb5fab/README.md).

# Filtered-range and disk-format integration, 2026-10-13

Merged main `fc0bc8f7`, including asynchronous durable barriers, per-structure
on-disk versions, and predicate-based filtered iterators. Branch format `2`
identifies the prefix header layout; the superblock retains main's format `8`.
The B-tree owns compatibility validation. Existing prefix pages do not depend on
the current construction opt-in.

Debug and release each passed 137 unit cases across B-tree, superblock, merge,
public API and internal core suites. The compressed-leaf regression now checks
reopen with changed construction policy and both predicate-only and
routing-pruned forward/reverse scans. All targets, tools and examples build.
The [integration evidence](../benchmarks/results-20261013/prefix-main-fc0bc8f7/README.md)
records exact commands and source/binary hashes. Prior benchmark evidence is
preserved under `benchmarks/branch-prefix-history`; it does not establish
performance on this new main.
