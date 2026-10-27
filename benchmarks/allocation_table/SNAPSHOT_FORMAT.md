# Allocation snapshot format 3

This document specifies the current implementation in
`src/rc_allocator_disk.{h,c}`, `src/rc_allocator.c` and the clean mount/shutdown
integration in `src/core.c`. Correctness and failure validation passed as
described in [the implementation report](IMPLEMENTATION.md). The older throughput
reports measured format 2 and are not performance evidence for this encoding.

## Counts and geometry

Exact RAM refcounts and overflow values are unsigned 64-bit integers. Raw 0 means
free, raw 1 means allocated but with no logical owners while reclamation finishes,
and raw 2 (`AL_ONE_REF`) means one logical owner. There is one allocation floor
above the number of logical owners. RAM uses fixed four-bit tags and an overflow
hash table; there is no backend or counter-width configuration.

Disk tags are also four bits. Values 0..14 are exact raw counts. Tag 15 means an
exact raw count of at least 15 is stored in the overflow records. Records contain
extent identities and counts, not hash buckets or capacity; the disk encoding
is independent of the in-memory hash-table implementation.

Let `P` be page size, `E` extent size, `N = disk_size / E`, and:

```
B = ceil(N / 2)                   exact tag bytes
T = round_up(B, P)                page-padded tag bytes
F = P + T                        fixed map bytes
R = 1 + ceil(F / E)               fixed reserved extents, including extent 0
C = floor((E - 64) / 16)          overflow records per node
K = number of raw counts >= 15
Q = ceil(K / C)                  overflow chain extents
```

The shared layout validator requires nonzero disk size no greater than INT64_MAX,
a power-of-two page size of at least 120 bytes and at most 1 MiB, power-of-two
extent size no smaller than a page, exact capacity divisibility by extent size,
and `R <= N`. The storage layer can impose stricter supported page/extent sizes.
Header validation additionally requires `K + Q <= N - R`, the exact computed
`Q`, and a zero chain head if and only if `Q == 0`.

Extent 0 contains the superblock. Extents `[1,R)` reserve the header and tags.
Their raw tags, including extent 0, must all be exactly 2. Unused space after the
last tag page within the final fixed reserved extent is not part of the encoding.

## Byte encoding

Every integer field is explicit little-endian uint64; native structs are never
written directly. Checksums are pairs of little-endian uint64 values, low half
first. Magic values are integer constants, not host-endian string writes.

The map header occupies one page at address `E`:

| Offset | Length | Value |
|---:|---:|---|
| 0 | 8 | Header checksum low64 |
| 8 | 8 | Header checksum high64 |
| 16 | 8 | Magic `0x3350414d434f4c41` |
| 24 | 8 | Allocator format version 3 |
| 32 | 8 | Extent count N |
| 40 | 8 | Page size P |
| 48 | 8 | Extent size E |
| 56 | 8 | Exact tag byte count B |
| 64 | 8 | Overflow record count K |
| 72 | 8 | Chain extent count Q |
| 80 | 8 | Chain head address, or 0 |
| 88 | 8 | Aggregate tag checksum low64 |
| 96 | 8 | Aggregate tag checksum high64 |
| 104 | 8 | Aggregate chain checksum low64 |
| 112 | 8 | Aggregate chain checksum high64 |
| 120 | P-120 | Zero padding |

Tag bytes begin at `E + P`. Extent `2*i` uses byte `i`'s low nibble;
extent `2*i+1` uses its high nibble. For odd N the unused final high nibble must
be zero. Bytes `[B,T)` are also zero.

Each overflow node occupies one complete allocator extent:

| Offset | Length | Value |
|---:|---:|---|
| 0 | 8 | Node checksum low64 |
| 8 | 8 | Node checksum high64 |
| 16 | 8 | Node magic `0x33444f4e434f4c41` |
| 24 | 8 | Zero-based node index in chain traversal order |
| 32 | 8 | Next node address, or 0 on the last node |
| 40 | 8 | Record count in this node |
| 48 | 16 | Zero reserved fields |
| 64 | 16 times record count | Pairs: extent number, exact uint64 raw count |
| remainder | variable | Zero padding |

Every nonfinal node contains exactly C records; the final node contains the
remaining positive number. Records are globally sorted by extent number,
strictly increasing, and their raw counts are at least 15. Each escaped tag has
exactly one matching record, and no record may exist without an escaped tag.
No overflow record may refer to fixed reserved metadata.

Node addresses must be extent aligned, inside capacity, and outside `[0,R)`.
Node indices must be consecutive from zero; final next is zero, every earlier
next is a valid node address. A repeated address cannot pass the required next
index because its stored index is fixed. Each node's own tag must be exactly 2,
so it cannot also be an escaped-count record. Nodes need not be ordered by their
own physical addresses; only their records are sorted.

## Checksums

Header and individual node checksums use:

```
XXH3_128bits_withSeed(block + 16, block_bytes - 16, 0x1e96a12f7bb05d43)
```

The header checksum therefore covers its geometry, aggregate checksums and
padding. A node checksum covers its index, link, record count, records and
padding. The aggregate tag checksum is unseeded `XXH3_128bits` over exactly B
bytes, including any canonical unused high nibble but excluding page padding.
The aggregate chain checksum is unseeded `XXH3_128bits` over all complete sealed
node blocks in traversal order, including their individual checksums. Empty
chains use the ordinary unseeded XXH3 hash of an empty byte stream. These are
corruption checks, not cryptographic authentication.

## Publication, loading and ownership

Persistence requires a quiescent allocator and an already durably invalidated
previous snapshot descriptor. The writer first counts escaped values and
allocates Q ordinary extents with `PAGE_TYPE_MISC`; each allocation has raw count
2. It then serializes the chain, tags (which include those chain allocations),
and header, issues `io_durable_barrier`, and only on success returns address E.
The caller subsequently publishes that address through the durable superblock.
The writer alone does not publish a valid descriptor.

The writer retains an address vector for the chain. A successful load also
retains a chain-address vector while reconstructing exact RAM counts. It marks
the map valid only after the reader has verified the complete structure and
checksums. Core keeps the loaded chain allocated while opening. After its
superblock mark-dirty publication has succeeded durably, it drops each chain
extent's raw count 2 to 1 to 0 and frees the address vector. This ordering keeps
old snapshot blocks available as long as an old durable descriptor could be
selected after a crash. Allocator deinitialization frees the vector in memory
but does not free the saved snapshot's extents on disk.

Recovery after an invalid descriptor ignores the snapshot and rebuilds ownership
from durable tree/log reachability. Old snapshot-chain blocks then become free
because they are not part of that durable graph. This remains a clean-shutdown
optimization; updates and snapshot persistence do not run concurrently.

Failure during chain allocation or any write/barrier leaves the caller's output
descriptor unchanged. The writer rolls back its newly allocated unpublished
chain. A partially overwritten fixed map is not trusted because the descriptor
was already invalidated. If publication fails after successful serialization,
chain ownership is retained: the failed barrier may still have persisted the
superblock, so reclaiming immediately would be unsafe.

## Reader resource and failure contract

`rc_allocator_disk_read()` is shared by allocator loading and
`tools/splinter_inspect.c`. The read callback receives aligned buffers and
page-aligned addresses/lengths. Buffer allocation uses the supplied platform
heap ID. The codec uses at most a 1 MiB tag window, one extent-sized node buffer
and one page buffer, plus fixed-size hash state. It never allocates a full dense
map or a set of visited node addresses.

Tags are read in increasing extent order. A streaming record cursor loads chain
nodes when an escape is encountered, checks the expected extent number, and
advances once. Independently reading the containing tag page verifies each
node's own raw count. Full tag and chain checksums are checked before success.

Optional callbacks receive decoded counts and validated node addresses; a late
failure may follow partial callback delivery. Callers must discard or reset all
partial results. The output summary is unchanged on failure. The allocator's
count visitor may itself fail on sparse RAM overflow allocation; this also keeps
the imported map invalid.

The codec's bounded scratch excludes the allocator-owned chain-address vector.
Persistence allocates Q addresses (8Q bytes); the loader grows its vector by
powers of two from 16 slots, with temporary old/new allocation overlap while
growing. The inspector supplies no visitors and needs no such vector. Its totals
include both the fixed reservations and chain extents in allocated space, and
report the chain separately.

## Compatibility and validation status

The fixed-four-bit allocator uses allocation snapshot format 3.
There is no automatic format-1/2 migration. A version mismatch is rejected before
ordinary use. The inspector uses the same codec instead of interpreting native
`sizeof(refcount)` arrays, preventing a uint64 RAM type from changing disk
interpretation accidentally.

The codec and inspector compile in the final normal build. At the format-3
snapshot implementation step, eight inspector checks passed against that
revision's debug library/tool: clean public-API create/close with 1,000 records;
a separately encoded valid chain with raw counts 15, 2^40 and UINT64_MAX;
dirty allocation-state unavailability; old-format rejection; corrupted header,
tags and node; and truncated map. The valid chain's allocation total increased by
exactly its three referenced extents plus its own one extent. All eight cases
verified identical file SHA-256 before and after inspection. Fixture source,
commands, binary hashes and logs are in
`.cache/rc-snapshot-validation/inspector/validation.json` and its directory.

Historical corruption, I/O failure, rollback, shared-heap, cross-width, ASan and
public-API reopen validation is recorded in the
[snapshot validation manifest](results/snapshot-20261026/validation.json).
The experimental width variants have since been removed; see
[the final validation report](FINAL_RESULTS.md) for normal fixed-four-bit builds,
current throughput, evidence locations and limitations.
