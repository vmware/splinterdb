# Native blob keys (B-tree 6, trunk 5, shard log 4)

Large keys are supported through the public SplinterDB API, including B-tree
keys, trunk pivots, and log records. Keys at or below page_size / 8 remain inline.
Applications continue to pass ordinary key slices and receive logical key bytes
in comparison, hashing, merge callbacks, and iterators. No blob-aware key
callback is required.

## Encoding

Keys of at most MAX_INLINE_KEY_SIZE(page_size) remain inline. Larger keys use
bit 1 (ONDISK_KEY_IS_BLOB) of the entry's existing key-flags byte. Index pivots
use the same encoding as leaf keys. There is no extra key version, logical-length
field, prefix-length field, or descriptor-length field in an individual key.

For lexical keys, the uncompressed entry key is:

    [ P bytes of logical key prefix | existing blob descriptor ]

For nonlexical keys, P is zero and the key contains only the descriptor. The
descriptor describes the entire logical key, including the prefix. Its existing
logical length and checksum remain unchanged. Its encoded length follows from
the entry's key length and P.

A branch reference carries { root address, B-tree version, P }. P is a uint16
in that record, with the configuration constraint below enforced before
construction. Readers use the saved P, independently of the current construction
setting. Memtables use their fixed creation configuration.

The descriptor budget defaults to 256 bytes. P defaults to 256 for lexical keys
and is always zero when constructing nonlexical trees. Construction requires:

    P + blob_max_descriptor_size <= MAX_INLINE_KEY_SIZE(page_size)

The descriptor budget must also satisfy the blob builder's minimum (114 bytes).
At the public configuration boundary, zero selects the default P and
UINT64_MAX selects P=0. Internal B-tree configuration and the test CLI
--blob-key-prefix-size use literal lengths, including zero.

## Leaf common prefixes

Let C be the leaf's common-prefix length. Compression removes C bytes from the
logical inline prefix; a blob entry then stores:

    [ P-C remaining prefix bytes | unchanged blob descriptor ]

Compression never includes descriptor bytes. When a leaf contains a blob key,
C is capped at P. If an inline-only leaf already has C>P, appending a blob key
starts a new leaf. The existing leaf retains its encoding and prefix.

Small keys keep their usual leaf compression and carry no blob flag. Mutable
nodes and index pivots do not use leaf common-prefix compression.

## Comparison, iteration, ownership

Only a comparator opting into unsigned bytewise lexical ordering may use the
prefix shortcut. Ties stream the remaining bytes from the blobs, preserving
exact comparison and length ordering. Custom comparators receive materialized
full keys through the existing callback API. Descriptor bytes never determine
logical order. Packing hashes the logical source keys using the existing hash
callback and records logical key-byte totals.

B-tree searches propagate blob-read failures through their status return.
Iterator positioning materializes the current logical key before returning;
curr() performs no allocation or I/O. Uncompressed inline keys still borrow
their page storage. Blob comparisons currently use synchronous blob reads even
inside the asynchronous B-tree APIs.

Key blobs belong to the tree's existing mini allocator, alongside value blobs.
The mini allocator records compact blob-root references and owns packed-tail
storage; immutable pointer extents own their children. Pivots reference the same
tree-owned key blobs. Repacking materializes and builds
new key blobs; it does not yet share source key extents. Repeated large-key
updates can retain obsolete key storage until the owning tree is released.
These are performance limitations of this scoped implementation.

Memtable fullness charges both newly allocated extents and every successfully
attached extent reference. Shared storage is charged again for each attachment
so reusing blobs cannot bypass the memtable's compaction-input size budget.
This counter includes allocation reserves and metadata; it is not a count of
unique physical disk extents. Failed attachment does not charge the payload.
Trunk byte statistics continue to measure full logical key and value lengths.

## Format compatibility

B-tree version 6 stores each index entry as:

    [ child address | key length and flags | key bytes | counters ]

The counters are num_kvs, key_bytes, and message_bytes, stored contiguously as
three uint32 fields or three uint64 fields. Bit 0 of the index key-flags byte
selects the wide representation; bit 1 independently marks a blob key. The key
has the same offset in both representations. Counter access uses memcpy because
the key can leave the counters unaligned. Leaf entries cannot set bit 0.

In memory, counters remain uint64 and UINT64_MAX means unknown. Narrow entries
encode unknown as UINT32_MAX and exact counts through UINT32_MAX-1. If any known
counter exceeds that limit, all three counters use the wide representation.
Mutable trees, whose counters are unknown, remain narrow. Narrow entries use
23 bytes plus their key, and wide entries use 35 bytes plus their key. There is
no split-word encoding or per-statistics version.

B-tree versions 1 through 5 are rejected before page access by the versioned query,
iterator, metadata, and allocation-recovery interfaces. Trunk version 5 and
superblock version 10 identify the new branch and allocation ownership scheme.
Existing database files need rebuilding; no compatibility reader or migration
is included.

## Validation and performance evidence

The native regressions cover mixed inline/blob keys, the 512/513-byte boundary
on 4 KiB pages, lexical and custom ordering, P=0, mutable splits and updates,
full-key merge callbacks, synchronous/asynchronous lookup and iterator setup,
forward/reverse scans, seek, logical ranks, C>P leaf transitions, malformed
entries, blob-read failures, and repacking with a different P after source
release. Public reopen coverage checks that a saved branch P survives a change
in construction configuration. Direct layout tests cover the narrow/wide
boundary, contiguous unaligned counters, unknown sentinels, flag preservation,
and growth/shrinkage during entry updates and defragmentation. Old/future
versions are tested with a NULL cache to prove rejection before I/O.

The earlier [prototype results](../benchmarks/blob_keys/RESULTS.md) measured
commit da464513, which used a separate experimental codec. They are not
performance measurements of the native format. The initial native reports
measure format 3. The [complete format-4 rerun](../benchmarks/blob_keys/SMALL_KEYS_V4.md)
measures all 63 small-key B-tree cases after restoring compact counters.
Benchmark campaigns must stop all other registered projects and hold both
shared coordination locks exclusively.

Format-4 validation: the complete debug unit suite passed 295 tests with five
existing skips before the attachment-accounting correction. The new attachment
regression failed on the old code (expected 13 extent charges, observed 12) and
passed after correction. With that fix, the separate Splinter suite passed all
54 tests, including the sparse height-three blob / large-statistics test; the
public blob and quick suites passed 11 and 66 tests respectively. The 14 B-tree
layout/configuration tests and three native blob-key regressions passed in
debug, release, and GCC ASan+UBSan builds (ASan leak detection disabled).
Formatting and whitespace checks passed. Logs and hashes are retained under
.cache/compact-stats-validation.json and .cache/compact-stats-*.log in this
worktree. The separate format-4 benchmark report preserves its own raw evidence.

## Small-key performance

The initial native implementation had measured small-key regressions.
[Scan4096 profiling](../benchmarks/blob_keys/SCAN4096_PROFILE.md) identified and
fixed the large scan slowdown after rebasing onto main. The complete format-4
rerun passed all 882 checks but still found packing, point-lookup, and
custom-comparator write regressions. Subsequent [packing profiling and fixes](../benchmarks/blob_keys/PACK_PROFILE.md)
remove aggregate argument copies and track whether the current leaf contains a
blob key, avoiding the prefix-compression entry scan. All nine packing cases now
have lower median elapsed time than main (189 verified runs); the 256-byte random
case remains statistically inconclusive. Subsequent
[overwrite profiling](../benchmarks/blob_keys/OVERWRITE_PROFILE.md) removes
unnecessary persisted-entry validation and encoding-buffer setup during
mutation, and simplifies custom-comparator leaf search. Fifteen longer paired
runs reduce custom overwrite overhead from 11.7% to 5.2%, with a remaining
95% interval of +3.9% to +6.1%. The full debug suite passes 295 tests with five
existing skips after those changes. The overall no-small-key-regression
objective is not yet met. The [earlier report](../benchmarks/blob_keys/SMALL_KEYS.md) preserves
the original public API and B-tree results.

[Point-hit profiling](../benchmarks/blob_keys/POINT_HIT_PROFILE.md) adds a lexical
pivot shortcut with separate lexical/callback and checked/mutable search loops.
The retained branch takes 23.2% less time than unmodified main for 64-byte random
hits; it remains 6.0% slower than main with the same shortcut. Custom controls
show no clear new regression, but their existing overhead remains. The retained
code passed the full debug suite (295 passed, five skips), release and ASan
B-tree checks; all 428 profiling/benchmark runs verified.

[Continued profiling](../benchmarks/blob_keys/RESIDUAL_PROFILE.md) validates ordinary
narrow-counter pivots directly and prepares inline comparison arguments once per
search. Against main with the same lexical shortcut, 64-byte random and custom
hit overhead falls to about 1%, with intervals spanning zero. Custom insertion
and overwrite retain roughly 6% and 4% overhead, and 256-byte random hits retain
2.5%; the overall objective remains unmet. The current code passes the full
debug suite, release and ASan targeted tests, and all 610 profiling/benchmark
runs. Boundary validation for narrow-counter entries now has an additional
synchronous/asynchronous rejection case.

[Further mutation profiling](../benchmarks/blob_keys/MUTATION_PROFILE.md) carries
borrowed key/message objects through the insertion body, mutable pivot search,
and leaf incorporation, removing intermediate metadata copies. Longer paired
confirmation finds +0.4% custom overwrite and +1.7% insertion time versus main;
shorter controls retain a measurable insertion gap of about 2%. All 786
measurements verified, the full debug suite passed 295 tests with five skips,
and debug/release/ASan targeted tests passed. Public APIs and the B-tree format
are unchanged by this follow-up; the overall performance objective remains open.

The [full small-key rerun after these fixes](../benchmarks/blob_keys/SMALL_KEYS_FINAL.md)
completed all 63 cases and 882 measurements against the original frozen main
binary. Packing is faster in every configuration, but 64/256/512-byte shared-prefix
Scan4096 remains about 5–7% slower and custom overwrite about 2% slower. All work,
results, packed layouts, and residency checks passed, with zero measured I/O.
This measurement-only follow-up changed no production code; the small-key
performance objective remains unmet.

[Shared-prefix Scan4096 profiling](../benchmarks/blob_keys/SHARED_SCAN4096_PROFILE.md)
keeps suffix decoding and entry checks inline and moves blob/index result
construction out of the ordinary leaf result path. The retained source is
12.8–13.4% faster than original main for shared-prefix Scan4096 across all four
key sizes, while also improving random long scans. Its fresh 63-case matrix
verified all 882 runs; custom insertion, overwrite, and hits retain roughly 2%
overhead. Full debug and targeted release/ASan tests passed. No persistent
format, public API, or ownership rule changed in this follow-up.

## Container-owned blob references

The ownership refactor separates blob construction from container allocation.
`blob_build()` and `blob_clone()` allocate whole extents directly and accept a
callback only for packed-tail storage. Successful results transfer whole-extent
references to their destination container; descriptors themselves are borrowed
views. Retain the container to extend a descriptor's lifetime, or clone into
another container. There is no descriptor-level `blob_incref()`.

`mini_blob_build()` and `mini_blob_clone()` provide the mini-allocator adapter.
Failed construction releases unadopted roots, while tails and roots already
adopted by a mini allocator remain tracked for bulk cleanup. Mini admission and
space accounting still charge every referenced descendant, even when shared.
Efficient append/truncation topology is deferred. Trunk pivots use the same
container ownership contract, with their own tail allocation callback.

Validation of the initial ownership refactor passed the full debug unit suite (295
passed, five skipped) and standalone Splinter suite (56 passed). All 28 targeted
checks passed in debug, release, and AddressSanitizer builds, covering native
blob keys, public blob reuse, crash recovery, failure cleanup, and mini metadata
cursors. AddressSanitizer ran with leak detection disabled; allocation-map and
failure-injection tests separately verify extent reclamation.

New regressions verify that 600 clones retain only root references without
reading pointer extents, that recovery reproduces the entire reference-count
map, and that a custom tail allocator works without a mini allocator, including
source and destination tails in the same page. They also inject failures at
every hierarchical construction allocation and at tail allocation. Commands,
logs, hashes, and a patch isolating this refactor from earlier uncommitted work
are retained in `.cache/blob-ownership-refactor/` in this worktree.

Earlier performance reports describe format 4 before this ownership refactor;
they are not measurements of the refactored implementation.

## Ordered mini metadata and terminated pointer extents

Blob roots now use the same eight-byte entries as ordinary mini allocations.
The entry's page-type byte is split into four type bits and four pointer-depth
bits. The bidirectional cursor returns every entry in recording order, including
blob roots and packed-tail extents, with optional type/height outputs. The
special cursor blob-prefetch API and the second record region are removed.
B-tree scan prefetching consumes this ordered stream and prefetches blob roots
and tails; descendants still use the blob iterator's read-ahead.

Bulk operations use a single fallible extent callback with a height parameter.
Reclamation and recovery delegate blob entries to the blob module. References
contain only an address and height in memory. Each metadata page persists a
64-bit sum of the charges for its entries, preserving full descendant accounting
without blob reads during adoption or space queries.

Blob format 2 requires each pointer extent to end with a zero pointer unless
completely full. The terminator is initialized and persisted even at an exact
page boundary, and validation checks it before log recovery accepts a blob.
This is the only blob-format change; append/truncate topology remains deferred.
Mini format 3 and the enclosing versions reject earlier interpretations.


Validation passed the full debug unit suite (295 passed, five skipped), the
standalone Splinter suite (57 passed), and all 29 targeted checks in debug,
release and AddressSanitizer builds. Leak detection was disabled in ASan.
Routing-filter metadata corruption tests and the functional log recovery suite
also passed, including replay of all 500,000 records in its final case.

Regressions check forward/reverse cursor order over 600 shared roots across
metadata pages, preservation of the current entry on cache misses and end of
stream, accurate persisted charges without pointer-tree reads, terminators
before/at/after page boundaries and at full extent boundaries, malformed and
unreadable terminators, recovery after eviction, and complete reclamation.
The functional B-tree corruption helper was updated to use the current child
address field so the functional driver builds.

Commands, raw logs, hashes, and a patch against the previous worktree state are
retained in `.cache/blob-ordered-metadata/`. No new performance measurements
were made.


## Trunk pivots: copy all tails

Trunk format 5 stores the fixed blob-key prefix length P in each node header.
Pivot keys use the same flag and `[prefix | descriptor]` representation as
B-tree keys; P is zero for custom ordering. Small pivots remain inline.
In-memory pivots and child-reference keys use the full-width `key` length,
so materialized separators are not limited by the on-disk uint16 encoded length.

Every serialized node owns its pivot blob references. An unchanged pivot is
cloned from its source descriptor: whole extents are shared, and every tail is
copied. New pivots are built from their logical bytes. Source descriptors borrow
the same protected source-root lifetime as the node's child and branch references;
cloning acquires the new references before the old root can be released.

A layout pass reserves the metadata body, including a bound on descriptor sizes
because splitting a tail across an extent boundary can add an address. Tails
start at the next page boundary in the node's own extent. The header records
that boundary so readers reject metadata/tail overlap. Metadata pages remain
PAGE_TYPE_TRUNK; tail pages are PAGE_TYPE_BLOB. This avoids using two cache page
types for the same physical page. The allocation callback packs further tails
into overflow extents only when the current extent is exhausted. It normalizes
zero alignment to one, matching the mini allocator's callback semantics.

There is no per-node mini allocator. Normal reclamation already walks the node;
it releases each descriptor's whole-extent roots and each distinct overflow tail
extent once. Tails in the node's own extent require no additional allocator
reference. Recovery reads descriptors before accessing blob contents, rebuilds
root and tail references, and then materializes keys to validate ordering.
Failed serialization rolls back completed blob clones and all newly allocated
tail extents without publishing the node.

The first version deliberately materializes keys for in-memory node operations.
Point comparisons reuse B-tree lexical streaming or the standard custom-comparator
materialization fallback. Blob reads in the async lookup path are currently
synchronous, as in the B-tree prototype; errors still propagate to the caller.
Selective tail retention, subextent reclamation, and optimizing huge-key mutation
cost are outside this implementation.

Tests in `tests/unit/trunk_split_test.c` cover small colocated tails, overflow
tails and whole-extent sharing, keys above 64 KiB, custom comparators, cold sync
and async lookup, range bounds, allocation/retain failure rollback, read errors,
and exact allocator-reference reconstruction followed by recovery mount. These
are direct B-tree-to-trunk tests. Public large-key coverage is described below.


Validation of trunk format 5 passed 301 debug unit tests (five existing skips)
and all 57 standalone Splinter tests. The 12 trunk split/blob tests, two trunk
GC tests, and two async-lifetime tests passed in debug, release, and ASan builds
(ASan leak detection disabled). All 12 trunk split/blob tests also passed with
shared memory enabled. These tests preceded log/public API integration; the
end-to-end small-key measurements below include the trunk implementation.


## Log and public API integration

Shard-log format 4 uses the tuple key flag to distinguish inline keys from
descriptor-only blob keys. The log needs no inline prefix because replay orders
records by memtable and leaf generation. Keys above the shared inline threshold
are built with the log's mini allocator; its page-aligned tail policy applies to
key and value blobs alike. Their logical page counts both contribute to the
group-size estimate.

Preparation bounds the entire encoded record before allocating either blob,
builds key and value blobs, and enrolls both in the group's writeback set before
reserving publication. An aborted preparation leaves unused allocations tracked
by the log's mini allocator for bulk reclamation. Small keys need no additional
allocation. Memtables independently encode the logical key; sharing the log's
key extents with a memtable is a possible later optimization.

Allocation reconstruction discovers both kinds of blob in needed generations.
An unreadable blob cuts off its entire group and all subsequent groups.
Checkpointed generations do not require their blob payloads to remain readable.
The iterator validates blob checksums before accepting a group, and materializes
its current key during initialization/advance, where errors can be returned.
The infallible current-record accessor therefore always supplies logical keys.
Existing log versions are rejected, with no new blob-descriptor version.

Public tests cover the inline boundary, 4 KiB, 64 KiB, and 256 KiB keys,
lexical and custom ordering, logging on/off, insert/lookup/miss/update/delete,
fetching old values, forward/reverse/bounded iteration, flush, close/reopen,
and abrupt-exit recovery with simultaneous key and value blobs. A damaged key
tail verifies whole-group truncation. The log's indirect-blob recovery tests
also cover key blobs, missing first/later pointer pages, generation cutoff, and
reclaiming conservative allocations in a subsequent root-only rebuild.


The integrated final source passes the full debug unit suite: 303 passes and
five existing skips. Seventeen focused trunk/public checks each pass release
and ASan+UBSan (leak detection disabled), including a custom-comparator P=0
layout check and reopen with a changed configured prefix. The functional log
suite passes, including 500,000 records and indirect key-blob corruption tests.

The [public API small-key report](../benchmarks/blob_keys/PUBLIC_SMALL_KEYS.md)
records the 200-run full end-to-end comparison, focused profiling, and final
source confirmation. Most write throughput improves, but lookup misses and
custom-comparator hits retain repeatable regressions; logged shared-prefix
write latency also needs investigation. The no-small-key-regression objective
is not met. Raw measurements remain local and untracked.


[Public lookup profiling](../benchmarks/blob_keys/PUBLIC_LOOKUP_PROFILE.md)
borrows query descriptors through checked B-tree lookup and pivot search. A
seven-pair confirmation improves custom hits 3.4% and random misses 2.3% over
the pre-change branch; remaining single-thread gaps are small. The maintained
public benchmark now stores per-worker latency samples in separate segments.
Four-thread misses still regress in the tested workloads, so the overall
small-key performance objective remains open. The retained source passes 303
debug unit tests (five existing skips), and 31 focused checks each in release
and ASan+UBSan with leak detection disabled.


[Four-thread miss profiling](../benchmarks/blob_keys/PUBLIC_LOOKUP4_PROFILE.md)
identifies false sharing between adjacent batch-lock reader rows. Giving each
thread a full cache line costs 6 KiB per database and removes almost all
measured modified-line RFO traffic. The earlier final confirmation mistakenly
linked a stale static archive without this fix; the source and shared-library
correctness tests did contain it.

The [artifact audit and fresh comparison](../benchmarks/blob_keys/PUBLIC_LOOKUP_RESIDUAL.md)
explicitly rebuild both libraries and verify the linked reader stride.
Sustained four-thread misses improve 35% for random keys and 6.8% for shared
prefixes versus frozen main; custom misses improve 16% with a wide interval.
The original public workload has inconclusive miss changes of +2.8%, -1.5%,
and +1.3%, respectively for random, custom, and shared-prefix keys. All 30 runs
and 810 phase rows verify. A separate key-flags experiment was reverted after
clean controls showed no compelling incremental benefit. Other operation
regressions remain; the overall small-key performance objective is still open.


The [final full public matrix](../benchmarks/blob_keys/PUBLIC_PR_CHECK.md)
uses fresh builds of current main and this unchanged source. All 200 runs and
5,400 phase checks pass. Across 300 ordinary CRUD/scan comparisons, none has a
95% interval wholly below a -3% throughput change, but 104 remain uncertain and
11 median losses exceed 3%. Durability, optimize, and post-delete iteration
still have outliers; insertion stall bins rise from 84 to 106. This supports
progress toward a PR but does not establish the 3% limit across every operation.
