# On-disk versions and read configuration

Format 10 rejects files written by earlier formats. There is no migration reader.
Creation writes both superblock slots. Mount and bootstrap select the newest
checksummed record before checking its structure versions, so an unsupported
newest version cannot silently select an older checkpoint. A damaged slot still
permits recovery from its valid peer. Recognition of a future superblock layout
is limited to the current checksum layout.

| Structure | Version location | Persisted configuration required to read |
| --- | --- | --- |
| Superblock | Superblock header | Device size, page size, extent size |
| Trunk | Superblock | Fixed inline blob-key prefix length P in each node header |
| Branch B-tree | Each branch reference in its trunk bundle | Fixed inline blob-key prefix length P |
| Mini allocator | Each metadata page header | Entries carry extent number, page type, batch and blob pointer depth; each metadata page stores its aggregate extent charge |
| Routing filter | Descriptor in its trunk bundle | Fingerprint width, index width, hash seed, value width, counts and metadata address |
| Shard log | Log head in the superblock | Checksum seed and stream nonce |
| Reference-count allocator | Superblock | Device geometry; map address is already persisted |
| Blob | Existing blob descriptor version | Length and checksum persisted; descriptor geometry locates data; pointer extents are zero-terminated unless full |

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
passed to the supplied hash callback. Allocation snapshots use explicit
little-endian fields, but other structures retain their existing encodings;
this does not promise database compatibility between arbitrary architectures.

## Allocation snapshots

Allocator format 3 uses four-bit extent counters and a linked list of overflow
extents containing sorted `(extent_number, uint64_raw_count)` pairs. Counters
0..14 are inline; 15 redirects to an exact overflow count. This disk format is
independent of the hash-table layout used in memory. The RAM allocator also uses
four-bit counters exclusively; there is no representation configuration.

Clean shutdown writes and makes the allocation snapshot durable before publishing
its address. If snapshot allocation or writing fails, the marker remains invalid
and next mount rebuilds allocation state from the durable graph. Clean mount
validates a saved snapshot, durably invalidates its descriptor, then reclaims its
temporary overflow extents. A malformed snapshot selected by a valid marker
returns an error. This is not a concurrent allocation checkpoint.

Allocator versions 1 and 2 are rejected, including images with the same outer
superblock version. There is no migration reader. Export with the corresponding
older build before recreating a database. The complete encoding and checksums are
specified in [allocation snapshot format 3](../benchmarks/allocation_table/SNAPSHOT_FORMAT.md).

## Format ownership

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
validation were already correctly owned. The format-ownership refactoring itself
did not change layouts; allocator format 3 is described separately above.

Direct B-tree regressions cover old/future versions and null roots before any
cache access, including safe iterator cleanup. The core corruption regression
covers synchronous/asynchronous point hits and ranges, and verifies that
routing-filter misses succeed without inspecting the irrelevant branch.

## B-tree format 6, trunk format 4, and mini-allocator format 3

Native blob keys add a fixed prefix length to each branch reference. Index
statistics are three contiguous uint32 or uint64 fields after the key, selected
by an entry flag. B-tree versions 1 through 5 are rejected, including the old
split-word encoding and earlier blob ownership encodings. Superblock format 10,
trunk format 4, and shard-log format 3 identify the new ownership and pointer
termination contracts.

Mini-allocator format 3 uses one ordered stream of 8-byte entries:

| Bits | Meaning |
| --- | --- |
| 0–7 | Batch |
| 8–11 | Page type |
| 12–15 | Blob pointer depth; zero for all non-blob entries |
| 16–63 | Extent number |

Blob depth zero denotes a data extent, including container-owned tail storage.
Positive depth denotes a pointer extent whose children have depth one less.
Roots are recorded when adopted, interleaved with ordinary extent entries.
Descendants are not separate mini entries. There is no second record region.
The cursor returns the address, batch, type and height; bulk traversal uses one
fallible callback with those extent semantics.

Each metadata page stores a uint64 total of charged extents for its entries.
An ordinary entry charges one; a blob root charges itself and all descendants,
including shared extents again. The builder supplies this charge using the
descriptor's geometry, without traversing pointer extents. Appending the entry
and updating its page total use the same metadata lock. Space queries sum page
totals and metadata bytes without reading blobs.

Blob format 2 keeps the descriptor fields and pointer-array layout, but requires
a zero immediately after the valid pointers unless an entire pointer extent is
full. The builder initializes the terminator even when it falls on a new page,
and maintains a terminated prefix during failed construction. Descriptor-based
validation checks the terminator, including its disk readability during log
validation. Old blob format 1 is rejected.

A container holds references to whole-extent roots; each immutable pointer
extent owns its children, and the container owns packed-tail storage. Sharing a
root does not increment descendants. Recovery counts incoming edges and
discovers outgoing edges once. Reclamation follows zero-terminated child sets
only when the last reference disappears. The blob module's in-memory reference
contains only an address and height, with no persistent count or separate
reference-record encoding. Efficient append/truncate topology remains separate
work. See [Native blob keys](blob-keys.md).


## Trunk format 5

The node header adds a uint16 fixed blob-key prefix length before the inflight
bundle offset, and a uint32 page-aligned tail offset after that offset. The
tail offset bounds metadata traversal and prevents blob reads from addressing
metadata pages in the same extent. Pivot key bit `ONDISK_KEY_IS_BLOB` identifies a bounded encoded
key containing that prefix followed by a blob descriptor. The descriptor covers
the complete logical key; inline pivots retain their existing encoding.
Versions before 5 are rejected by the superblock's trunk-format validation.

The metadata body still fits in one extent. Blob tails occupy separate pages in
that extent, then overflow into additional extents owned by the node. Each node
copy clones blob roots and copies all tails. Overflow tail extents are enumerated
from the descriptors and deduplicated per node for reclamation and recovery;
the node's own extent is excluded from that extra ownership. No per-node mini
allocator or new blob descriptor format is introduced.


Shard-log format 4 permits blob keys using the existing tuple key flag. Their
encoded key bytes contain only a blob descriptor; no lexical prefix is stored.
Key and value blobs share the log mini allocator and durability group. Recovery
validates both before accepting a group, and skips payloads in generations
already included in the durable root. Log formats through 3 are rejected.
