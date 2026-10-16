# Branch leaf prefix experiment

`data_config.key_compare_is_lexical` opts construction into prefix compression.
It promises unsigned-byte lexicographic ordering for stored keys and query
keys, including the rule that a shorter equal prefix sorts first. The default
data configuration opts in. Applications replacing its comparator must clear
the flag unless their replacement preserves this contract.

When a branch leaf first fills, packing extracts the longest common prefix of
its first and last keys, rebuilds entries as suffixes, and continues filling the
page only if that prefix is at least four bytes long. This decision is per leaf;
readers still accept previously written shorter prefixes. A later key outside
that prefix starts another leaf. Prefix bytes occupy
the page tail, so offset-table growth never moves them. A final partially full
leaf is left uncompressed. Index pivots and mutable memtable keys stay complete.

Point reads compare the prefix once, then binary-search suffixes. A mismatch
can place the query on either side of the leaf. On compressed leaves, range
iterators reconstruct full keys into owned buffers, copying the prefix only
when changing leaves. Uncompressed leaves return keys directly from the page,
including nonlexical scans; they skip prefix reconstruction.
Prefix scratch state is allocated only after encountering a compressed leaf,
reused across positioning operations, and released during teardown. Iterators
that visit only uncompressed pages do not initialize or allocate these buffers.
The decode buffer retains its largest logical length, so shorter subsequent
keys do not trigger resizing; the returned key still has its exact length.
Allocation, growth and replacement of the per-leaf prefix stay outside the
ordinary per-key path, and suffix copies of at most 16 bytes use bounded inline
copies. Uncompressed branch-leaf iterators borrow keys from pinned pages;
mutable memtable iterators retain their existing copied-page behavior.
Positioning operations perform any allocation before returning; observing the
current tuple stays infallible. Bounds, seeks, asynchronous lookup, diagnostics,
parent pivots and logical key-byte accounting use the full key semantics.
Iterator boundary and seek probes compare checked prefix/suffix representations
directly, without reconstructing a separate boundary key. Uncompressed leaves
can use direct lexical comparison during checked binary search when the lexical
contract permits it. Nonlexical comparisons, infinity targets on uncompressed
leaves, boundary comparisons and index pivots use the configured comparator.

This prototype now uses branch format version `2` within main's superblock
format `8`. Branch readers reject format `1` before interpreting its different
page header. Earlier standalone prefix images used superblock `0x8007` and are
rejected. Page encoding is determined from the page itself; existing compressed
leaves remain readable after changing the construction opt-in on reopen.

Initial prototype validation passed all 48 public quick tests. The regression checks
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

The latest [performance confirmation](../benchmarks/results-20261015/prefix-confirmation/README.md)
records the applied decode-buffer and validation optimizations, longer paired
experiments, and the completed write-stall investigation. All planned controls
are complete, and origin/main remains `6105c49f`, already included in this branch.
On the exact adopted highwater source, shared-prefix disk long scans improve
27.53% [14.16%, 42.46%], with 29.06% fewer storage read bytes per query.
Resident shared-prefix long scans improve 2.29%, and one-key ranges improve
3.65%. Uncompressed resident long scans retain roughly 1.6–2.2% losses; all
eight targeted final-source read intervals exclude a loss above 3%. The
nonlexical one-key range estimate is +0.35% [-0.61%, +1.33%]. These are
nominal paired intervals, not universal bounds or point-lookup measurements.

Fresh random insertion remains indeterminate: exact highwater/main throughput
including close is -4.10% [-16.57%, +10.24%] with WAL, and -0.08%
[-11.02%, +12.20%] without WAL. Identical-main controls vary substantially in
both executed work and blocking time despite unchanged initialized file
extents. A review PR is reasonable, but insertion performance remains a merge
concern under a strict 3% no-harm requirement. The final confirmation is
targeted; point lookups and the remaining workload matrix retain their earlier
version labels. The historical results below do not describe the newly applied
implementation unless explicitly identified as highwater.

The [2026-10-15 slowdown diagnosis](../benchmarks/results-20261015/prefix-slowdown-diagnosis/README.md)
adds phase-scoped hardware counters and profiles for the then-remaining resident
scan and logged-overwrite concerns. That older min4lex implementation executed about 28 extra
instructions per returned key; with statistics disabled it used 1.70%
more user cycles/key with one worker and 2.56% more with eight. Keys are borrowed,
not copied. Prefix-aware validation and loss of compiler inlining explain part
of the cost. The earlier timing harness also enabled statistics, whose per-key
locking substantially reduces multithreaded throughput and introduces a
false-sharing risk. Logged-overwrite losses remain inconclusive amid changing
filesystem/direct-I/O waits and host conditions. Neither of two isolated code
probes was adopted at that stage; runtime source then matched the candidate below.

The earlier complete evaluation is the
[2026-10-15 optimization and read/write matrix](../benchmarks/results-20261015/prefix-evaluation/README.md).
It compares main `6105c49f`, the frozen branch after boundary-key removal, and
the retained four-byte per-leaf cutoff with direct lexical comparison. It covers
resident/disk point, short and long reads for long-prefix lexical, tiny-prefix
lexical and nonlexical keys; fresh insertions and overwrites with WAL off/on;
space, CPU and physical I/O; and isolated tests of the proposed optimizations.
The branch-flag investigation was an uncompressed-only upper-bound probe;
it did not implement a persisted branch flag.

The frozen min4lex candidate improved shared-prefix disk long scans by 15.27%, with
29.18% less read I/O and 24.28% less live space. The prepared-fixture random-key
disk repeat improves point/short/long throughput by 7.65% / 8.72% / 5.51%.
Single-worker resident long scans were 2.18–3.06% below main across the three
key patterns. Nonlexical multithreaded scans and logged overwrites retained
inconclusive negative averages. Short insertion results varied substantially;
both the larger logged load and a same-size repeat failed to reproduce the
initial candidate slowdown. The archive preserves every round and its limits.
At that stage a review PR was reasonable, but performance neutrality was not
established and the no-harm requirement remained a merge concern. See the
latest confirmation above for the current assessment.

The fixed-capacity decode buffer did not show a repeatable speedup and reserved
more memory for these 64-byte keys. The branch-flag upper-bound probe did not
justify persisted metadata changes. Plain runtime-sized `memcpy` still calls
libc in this build and was about 1–2% slower on single-worker compressed scans;
the bounded suffix helper remains. The frozen min4lex source and mixed-prefix
regression passed 142 test cases in each of debug and release, plus functional
log replay. The current highwater source instead has the 320 unit-test executions,
two functional log replays and four fresh production-build checks recorded in
the latest confirmation. See the archives for hashes and raw evidence.

The following measurements are historical and predate that retained policy.

The lazy-state fix is now applied. It removes unconditional prefix-buffer
initialization on uncompressed leaves and reduces additional fixed iterator
storage from 38 KiB to 2 KiB. The
[fix validation archive](../benchmarks/results-20261014/prefix-lazy-6105c49f/README.md)
contains the correctness and performance evidence for that stage. The earlier
[nonlexical diagnosis](../benchmarks/results-20261014/prefix-nonlexical-diagnosis/README.md)
and [pre-fix integration comparison](../benchmarks/results-20261014/prefix-main-6105c49f/README.md)
remain as historical evidence, along with the measurements below.

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

# Current-main integration and performance review, 2026-10-14 (before the lazy-state fix)

Merged `origin/main` at `6105c49f` (bounded automatic log groups) without
conflicts, as merge commit `97e5c0e3`. The custom IPv4 comparator example now
clears the lexical flag. Debug and release each passed 141 unit cases plus the
functional log test. The benchmark libraries exactly match the recorded main
and merge runtime sources.

The refreshed comparison completed 28 read, 16 overwrite and 24 targeted repeat
runs, all with zero errors/violations, on real ext4 storage with O_DIRECT,
serialized execution, identical harness source and fsynced fixture copies.
Shared-prefix disk point/short/long scans improved 10.45% / 10.86% / 20.29%,
and file allocation fell 34.73%.

Performance remains a merge blocker. Resident nonlexical short scans were
5.36% slower initially and 5.73% slower in three longer paired repetitions,
with nonoverlapping sample ranges in both. Common-prefix resident long scans
averaged 12.10% slower in the repeat, although their sample ranges overlap.
The initial 20.10% disabled-compression long-scan loss did not repeat; that
repeat averaged -2.38% with overlapping ranges. Logged random overwrites
including close/drain averaged -6.09% in the repeat, also with overlapping
ranges. These short writes do not establish steady-state insertion performance.

Each public iterator is also 38 KiB larger (94,592 -> 133,504 bytes on this
build), even when compression is disabled. This is an object-size comparison,
not an RSS measurement. The unchanged-cache benchmark does not impose a total
memory cap. This branch is suitable for review as a PR, but the nonlexical
regression and remaining resident scan costs should be addressed before merging
the default-enabled feature. Full results, limitations, hashes, commands and
validation are in [the current archive](../benchmarks/results-20261014/prefix-main-6105c49f/README.md).

# Lazy prefix state and performance validation, 2026-10-14

Prefix buffers and the cached prefix-page address now live in a lazily allocated
state object. An iterator creates it only when it encounters a compressed
leaf; uncompressed and nonlexical scans continue borrowing keys from the pinned
page. Selection depends on the actual page prefix, preserving reads of
compressed images reopened with construction-time compression disabled.

| Iterator object | Main | Before fix | After fix |
| --- | ---: | ---: | ---: |
| B-tree iterator | 336 B | 488 B | 344 B |
| Core range iterator | 94,592 B | 133,504 B | 96,640 B |

This removes 36 KiB of unconditional storage per public iterator and leaves
2 KiB over main. Each compressed child also allocates a 152-byte state object,
before allocator rounding/metadata and full-key buffers. Object sizes do not
establish RSS savings or constant memory savings for compressed workloads.

Debug and release each passed 141 unit cases plus functional replay of 500,000
log records. The prefix regression checks lazy allocation and reuse, unchanged
zero-copy behavior for uncompressed leaves, allocation-failure cleanup, and
compressed/uncompressed traversal and reopen. The fault-injection build and
AddressSanitizer with leak detection both passed.

The direct before/after comparison on identical images measured a 4.00% gain
for resident nonlexical short scans and 4.17% for common-prefix long scans, both
with overlapping sample ranges. Against main, the longer nonlexical short-scan
repeat is now -0.22%, also with overlap; the earlier 5.73% deficit did not persist.

The feature remains suitable for a review PR but is not performance-ready to
merge: common-prefix resident long scans were 14.71% slower than main in the
longer repeat, with nonoverlapping ranges, 14.69% lower CPU efficiency and
15.88% higher mean p99. Shared-prefix disk points, short scans and long scans
improved 16.21%, 20.13% and 26.71%; file allocation fell 34.51%. Logged random
overwrites including close/drain averaged -5.56% with overlapping ranges;
unlogged random overwrites lost 8.56% in the initial screen. These bounded
writes do not establish steady-state insertion performance.

An observed 400 MHz host-clock episode confounded the first longer nonlexical
disk comparison. Its data are retained, and the entire three-version group
was repeated with host-clock observations. The recheck avoided that episode
but retained substantial control drift: fixed-version means were 9.63%, 9.72%
and 5.36% lower than main for points, short scans and long scans, with overlapping
sample ranges. Nonlexical disk overhead remains unresolved. All 98 query/write
invocations and 17 fixture load/space pairs passed, with zero errors or
correctness violations. A final fetch still found main at `6105c49f`. The
[fix validation archive](../benchmarks/results-20261014/prefix-lazy-6105c49f/README.md)
records all results, caveats, source/binary hashes and the updated assessment.

## Boundary-key buffer removal

The subsequent boundary-comparison optimization removes `bound_key` and its
allocation, copying and teardown. The compressed iterator's scratch state is
now 80 bytes rather than 152, before allocator rounding and decoded-key storage.
The B-tree and core range iterator object sizes remain 344 and 96,640 bytes.
All branch entry validation and comparison directions are preserved.

The expanded prefix regression covers nearby and cross-leaf seeks, an aliased
current-key seek target, prefix mismatches, and finite inclusive/exclusive
bounds with present and absent endpoints in both directions. Debug and release
each passed all 79 B-tree, merge and public API test cases. The prefix regression
also passed fault injection and AddressSanitizer with leak detection. Commands,
logs and source hashes are retained in `.cache/prefix-bound-key/`.
The direct before/after comparison is now recorded in the
[boundary-key validation archive](../benchmarks/results-20261014/prefix-bound-key/README.md).
All 18 timed invocations and four harness smoke checks passed with zero errors
or correctness violations. A pinned single-worker check averaged +1.26% for
common-prefix forward long scans and +1.74% for reverse long scans, both with
overlapping sample ranges. Eight-worker results were mixed and varied widely;
the apparent +16.66% reverse-scan gain did not persist in every pair. Thermal
throttling and clock variation limit these measurements. The memory reduction
is definite, but a repeatable throughput improvement or regression is not
established. This targeted resident-scan comparison uses the frozen pre-removal
implementation as its baseline and does not clear the earlier regression
against origin/main.
