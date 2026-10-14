# On-disk versions and read configuration

Format 8 rejects files written by earlier formats. There is no migration reader.
Creation writes both superblock slots. Mount and bootstrap select the newest
checksummed record before checking its structure versions, so an unsupported
newest version cannot silently select an older checkpoint. A damaged slot still
permits recovery from its valid peer. Recognition of a future superblock layout
is limited to the current checksum layout.

| Structure | Version location | Persisted configuration required to read |
| --- | --- | --- |
| Superblock | Superblock header | Device size, page size, extent size |
| Trunk | Superblock | None beyond device geometry and application callbacks |
| Branch B-tree | Each branch reference in its trunk bundle | None beyond device geometry and application callbacks |
| Mini allocator | Each metadata page header | Entries already carry extent number, page type and batch; geometry is global |
| Routing filter | Descriptor in its trunk bundle | Fingerprint width, index width, hash seed, value width, counts and metadata address |
| Shard log | Log head in the superblock | Checksum seed and stream nonce |
| Reference-count allocator | Superblock | Device geometry; map address is already persisted |
| Blob | Existing blob descriptor version | Length and checksum already persisted; descriptor length and geometry determine the extent list |

Memtables are reconstructed and have no persistent configuration record.
Fanout, incorporation thresholds, scheduling, cache size, prefetching and
statistics settings control construction or execution, not interpretation of
existing pages. Blob allocation rounding rules are part of its versioned
encoding, rather than a mutable reader setting.

Existing routing filters use their descriptor's widths and seed for synchronous
and asynchronous reads. Extending a maplet retains its encoding and hash seed;
compaction hashes replacement branches with that seed. New maplets use current
construction settings. Split sizing respects the smallest capacity among the
existing maplets it may extend. Cardinality estimation across different seeds
uses a conservative count because their fingerprint samples cannot be compared.

An optional range predicate must be compatible with every persisted routing
hash seed: keys that satisfy the predicate must hash equally for each such
seed. A reader can then hash its filter key using each maplet's saved seed,
while an exact predicate rejects hash collisions. Equality under only one
query-time seed does not satisfy this contract. Range filtering remains a
separate project; it is not introduced by this format change.

Each recovered log reads with its saved checksum seed. Newly constructed logs
may use the current creation configuration.

The application must supply matching key comparison, key hash algorithm, merge
semantics and key/value encodings on reopen. Function pointers and application
schemas cannot be persisted as portable identities. The persisted hash seed is
passed to the supplied hash callback. This change does not introduce a portable
byte-order encoding or promise compatibility between arbitrary architectures.

## Current PR readiness and performance

The branch includes main `0fa3bd6c`. Measured source is `3e302c04`; later changes
record evidence and documentation. Debug/release each passed 150 unit-test
executions, with two expected allocation-fault-injection skips per mode, plus
full routing-filter and log suites (a 500,000-record round trip). Inspection
reads the previous format-8 fixture and clearly rejects main's format-7 fixture.
All targets/tools/benchmarks build; clean-export CI formatting and PR whitespace
checks pass. The full compiler/sanitizer CI matrix was not rerun.

**The branch is prepared for review, but its resident point-query regression
remains unresolved.** The broad resident screen measured -4.39% eight-reader
point throughput. A separate 15-second, disk-format / main / main / disk-format
repeat reproduced a -7.75% loss with non-overlapping sample ranges:

| Focused resident point queries, 8 readers | Main | Disk-format |
| --- | ---: | ---: |
| Throughput range | 4.026–4.115 million/s | 3.651–3.860 million/s |
| Mean CPU per operation | 1.965 us | 2.131 us |
| Measured physical read bytes | 0 | 0 |

This is a CPU-side performance concern; the measurements do not isolate its
source within the format changes. The formats require independently constructed
fixtures, so tree shape remains a comparability limitation. Performance neutrality
has not been established, and this evidence does not justify calling it merge-ready
on performance grounds.

| Broad-screen throughput change | Resident | Disk-backed working set |
| --- | ---: | ---: |
| Point query, 1 reader | -1.13% | Not measured |
| Point query, 8 readers | -4.39% | +4.87% |
| Range length 1, 8 readers | +0.78% | +1.41% |
| Range length 10,000, 8 readers | +0.13% | -0.79% |

All checks passed with zero query errors or result violations. The disk results
include device variation and do not establish a speedup. Each profile has two
samples per regime; observed ranges are not confidence intervals. No fresh
write-throughput matrix was run. Timings compare the whole feature with current
main, not just the main merge. All database files used O_DIRECT on physical ext4
storage, with exclusive benchmark/machine locks and explicit fsync after copying
fixtures. Source/library/executable hashes, commands, samples, control drift and
CPU/I/O are in [the current benchmark report](../benchmarks/results-20261013/predicate-disk-main-0fa3bd6c/report.txt).

A separate companion branch, `robj/filtered-ranges-disk-format` at `36183807`,
passed 125 unit cases per debug/release mode plus the full filter suite. It proves
correct predicate scans under saved seeds 11/22 with current construction default99,
cache eviction over six seeds, and copied-key ownership when a later seed is used.
The two feature PRs remain independent; this is correctness evidence only.

# Historical validation and measurements

Debug validation: 16 superblock tests, 33 internal Splinter tests, 47 public quick
tests, full functional routing-filter and log suites. Added regressions cover
unknown versions, changed routing widths/seeds across reopen and further
compaction, synchronous/asynchronous filter reads, and changed log checksum
creation defaults.

The benchmark used identical generated kvbench source, O_DIRECT files on ext4,
CPU affinity 0–11, 24-byte keys, 100-byte values and WAL disabled. A single
loader constructed each image; one memtable and three normal workers were
enabled, so incompatible baseline/candidate formats still have a concurrent
tree-shape caveat. Resident: 200,000 keys / 128 MiB cache. Disk regime: 2 million
keys / 64 MiB cache. ABBA read phases lasted seven seconds.

| Pre-scalar-reader campaign | Point, 1 reader | Point, 8 readers | Range throughput change |
| --- | ---: | ---: | ---: |
| Resident | -3.4% | -3.6% | -2.5% to +2.6% |
| Disk | +2.0% | +2.4% | +2.7% to +6.6% |

The disk samples had comparable I/O per operation. Device/run variation and
independently constructed trees prevent attributing their apparent gains to
metadata. Initial shorter resident samples also showed a 2.8–4.0% point-read
regression. All runs had zero correctness violations and errors; brief load
samples do not establish write-performance neutrality.

The final reader removes a full routing-config copy from each filter lookup,
passing the saved index width and hash seed directly. Version and bounds
validation stays at the same boundaries. A six-run, ten-second fixed-image
comparison used baseline / old reader / final reader / final reader / old
reader / baseline order. Both candidate readers used copies of exactly the same
format-8 image; baseline used its own format-7 image.

The final resident fixture copies were made with buffered `cp` and were not
explicitly fsynced before opening with O_DIRECT. Pending copy writeback, including
unreachable historical pages, is a possible measurement confound; this does not
establish that it caused any observed difference. The earlier resident/disk
matrices constructed fresh O_DIRECT databases and did not use this copy path.
No additional metadata benchmark was run after identifying this method caveat.

| Final reader, resident | Versus old reader | Versus baseline |
| --- | ---: | ---: |
| Point, 1 reader | +1.2% | -1.5% |
| Point, 8 readers | +2.0% | -6.5% |
| Length-1 range, 1 reader | -2.2% | -1.5% |
| Length-1 range, 8 readers | -1.7% | -1.0% |

The final functional filter suite and changed-configuration reopen/compaction
regression pass. Profiling the earlier reader places most samples in cache
acquisition/release and B-tree/trunk lookup. It does not isolate a single cause
for the residual point-read cost. Each bundle decode now checks its branch
versions; that added work remains a possible cost. Performance neutrality has
**not** been established. The final scalar reader has not repeated the full
disk or write matrix; the disk figures above describe the earlier reader.

Compact CSVs, manifests, profiling summaries, runners and build provenance are
committed under `benchmarks/results-20261011/`. Full campaign artifacts remain
at `.cache/projects-20261011/benchmarks/disk-format/` in the main checkout;
benchmark database files were on a verified disk filesystem, with the shared
benchmark lock excluding other benchmarks, builds and tests.

## Format ownership follow-up (2026-10-12)

Branch compatibility now belongs to the B-tree. Point queries pass the selected
branch's persisted version to synchronous/asynchronous B-tree entry points;
`trunk_ondisk_bundle_readable()` no longer scans every branch. The B-tree rejects
unsupported versions or null roots before reading pages. Range initialization,
compaction, root-height/count queries, and allocation recovery preserve the
version alongside the address. Memtables do not acquire a format requirement.

Reference acquisition needs validation before iterator initialization: a null
root would otherwise target extent zero through the mini allocator. Existing
bundle-deserialization and range-collection loops therefore call the B-tree's
root validator before admitting references. Failed collection discards the
unreferenced partial bundle, preserving correct cleanup. These checks add no
branch scan to the ordinary point-query bundle decoder.

The ownership audit also moved trunk/allocator version constants and support
checks to their modules. Superblock reading still selects the newest valid
record before invoking owner validators for trunk, allocator, and both logs;
it never silently falls back to an older record with supported components.
Routing-filter instance-capacity calculation now lives in the routing-filter
module instead of reconstructing its encoding configuration in the trunk.
Routing-filter descriptor validation and mini-allocator private-header
validation were already correctly owned. No on-disk layout or version changed
in this follow-up.

Direct B-tree regressions cover old/future versions and null roots before any
cache access, including safe iterator cleanup. The core corruption regression
covers synchronous/asynchronous point hits and ranges, and verifies that
routing-filter misses succeed without inspecting the irrelevant branch.

Final debug validation passed all 108 cases across B-tree (11), superblock (16),
core (33), quick API (47), and async trunk (1), plus the functional filter and
log suites. Release builds of all tests and the inspection tool also passed;
the earlier release correctness run passed those same 108 cases before the
last reference-admission and corruption-test extensions.

The follow-up resident comparison uses the prior final-scalar executable and
the new reader on copies of the same format-8 image, avoiding independently
built tree shapes. Every private copy is explicitly fsynced before opening.
The runner uses O_DIRECT on verified ext4 storage, 200,000 keys, 128 MiB cache,
24-byte keys, 100-byte values, WAL disabled, CPU affinity 0-11, and 15-second
phases. ABBA then BAAB gives four samples per reader for point queries and
length-1 ranges at one/eight threads. The shared benchmark and machine locks
exclude simultaneous benchmarks, builds, and tests. This compares the complete
ownership refactor, not a one-line validation-loop ablation, and does not
remeasure the original main branch.

| Ownership refactor versus prior scalar reader | Throughput | CPU/op |
| --- | ---: | ---: |
| Point, 1 reader | -1.41% | +1.38% |
| Point, 8 readers | -0.94% | +0.74% |
| Length-1 range, 1 reader | +0.04% | -0.05% |
| Length-1 range, 8 readers | +1.31% | -1.36% |

All eight runs passed with zero errors/violations. Timed read/write bytes were
zero: these are resident CPU measurements, though the database files remain
on physical disk. Every before/after sample interval overlaps, both overall
and within each block. For example, eight-reader point throughput changed
-3.56% in ABBA but +1.94% in BAAB; the prior reader's block means drifted
-8.62%. The measurements establish neither a clear benefit nor a reliably
attributable regression. Removing the eager loop has not demonstrated a cure
for the original project's overhead; neutrality versus the original main
branch remains unestablished. Disk-limited and insertion matrices were not
repeated for this ownership-only follow-up.

The final release corruption regression also passes with the reference
admission guards and filter-negative cases. Compact raw measurements, block
summaries, test logs, hashes, commands, and the measured source patch are in
[`benchmarks/results-20261011/layering-20261012/`](../benchmarks/results-20261011/layering-20261012/).
Full fixtures and preserved measured executables remain in the campaign's
`benchmarks/disk-format/resident-layering-20261012/` directory.

## Main integration (2026-10-13)

Merged `origin/main` at `7bb5fab305b9c6179e31f75998dabea833537a08`.
The reader retains module-owned version checks, saved routing-filter settings,
and saved log checksum seeds while adopting caller-owned transferable cache
handles and propagating cache errors. Log recovery preserves the upstream
invalid-group cutoff for multi-level blobs. The new large-blob statistics test
passes the branch version to B-tree range counting.

The superblock remains format 8: upstream's wider B-tree counters preserve the
legacy narrow disk encoding and add flagged high words when needed. This merge
does not make earlier format-8 pages unreadable.

Debug and release each passed 116 unit cases: B-tree 11, superblock 16, core 38,
public quick API 49, and async trunk 2. Both also passed the complete functional
routing-filter suite and the complete log suite (500,000-record round trip). These cover changed
configuration across reopen, invalid structure versions, cache read failures,
recovery, multi-level blobs, and cross-thread iterator handoff. The first core
run used the old 4 GiB test device limit and could not allocate the sparse
height-three blob fixture; the passing runs use 64 GiB logical capacity on
ext4. Debug and release inspection tools also built successfully.

Commands, logs, source and executable hashes are retained in the main checkout's
`.cache/projects-20261011/coordination/main-7bb5fab/disk-format/` directory.
The measurements above predate this integration; they do not establish current
performance neutrality.

## Fresh benchmarks after main integration (2026-10-13)

Resident point throughput changed -1.21%/-1.92% at one/eight threads, and disk point throughput changed -0.81%. These sample ranges overlap main; substantial control drift prevents precise attribution of small differences.

These are updated-feature comparisons against main `7bb5fab`, not an isolated
measurement of merge cost. Main controls drifted; observed range overlap is
descriptive, not a statistical confidence interval. Exact samples, build/source
hashes, commands, verification scope and fresh correctness evidence are in
[the integration archive](../benchmarks/results-20261013/disk-format-main-7bb5fab/README.md).

## PR preparation against async-barrier main

Merged `origin/main` at `0fa3bd6c` (async durable barriers). Module-owned format
validation and stored configuration remain intact. The main notification,
durability and checkpoint paths are retained, including the two-argument public
barrier API and blocking `NULL` notification semantics. The disk format remains 8.

Debug and release each passed 150 unit cases and the complete functional
routing-filter and log suites (500,000-record round trip). Each mode intentionally skipped the
allocation-failure notification case twice because fault injection is disabled.
Both inspection tools read the previous format-8 fixture; the release tool
clearly rejects main's format-7 fixture. All builds, tools, benchmarks and the
CI formatting check pass. The full compiler/sanitizer CI matrix was not rerun.
No new benchmark was run for this merge, so earlier timings remain scoped to
main `7bb5fab`.

Main's archived evidence retains its paths. The conflicting disk-format
integration archive is now in `disk-format-main-7bb5fab`, with all historical
bytes preserved. Commands, counts, provenance and preservation checks are in
[the PR preparation archive](../benchmarks/results-20261013/disk-format-pr-0fa3bd6c/README.md).
