# Four-bit allocation reference counts

SplinterDB uses one allocation-table representation: four-bit packed counters
with a sharded overflow hash table containing exact unsigned 64-bit counts.
There is no dense backend or counter-width build/runtime option. A normal
`make` builds this implementation.

## Counts and concurrency

Each 64-bit CAS word stores sixteen extent counters:

| Raw tag | Meaning |
|---|---|
| 0 | Free |
| 1 | Allocated, no logical owners; reclamation is unfinished |
| 2 (`AL_ONE_REF`) | One logical owner |
| 3..14 | Exact raw count; logical owners equal raw count minus one |
| 15 | Exact raw count of at least 15 in the overflow table |

The overflow table has 64 cache-line-aligned locked shards, full 64-bit extent
identities, linear probing, power-of-two capacities, growth before exceeding
70% occupancy, and backward-shift deletion. Tables allocate lazily. Deletion
retains capacity until reset/deinit; peak sharing can therefore leave slack.

Inline updates use CAS preserving neighboring counters. Promotion locks the
shard, rechecks the tag, reserves an overflow entry, and CASes 14 to 15. A racing
inline decrement causes the tentative entry to be removed before retry.
Demotion publishes 15 to 14 and removes the entry before releasing the lock.
Escaped readers lock and recheck the tag before looking up an entry. Thus the
escape/entry invariant holds for synchronized observers and at lock release,
including resize and demotion races. Allocation-state checks need only a nonzero
tag and do not consult the hash table.

Decrement never allocates. Retain returns `platform_status`, rejects free
extents and saturation at `UINT64_MAX`, and preserves the count and optional
output on failure. Another thread may independently change the count.

With 128 KiB extents, the packed array needs 32 KiB at 8 GiB configured capacity,
512 KiB at 128 GiB, and 4 MiB at 1 TiB, plus the fixed container and overflow
capacity. The previous 32-bit array required eight times as many array bytes.

## Ownership and failure handling

A mini allocator adopts its caller's allocated metadata head as its first owner.
Singly owned branches, filters and memtables have raw count 2. The last owner
takes the head to 1 while reclaiming its contents, then frees it. Continuation
metadata extents follow their own 2-to-1-to-0 path. Recovery's first visit
establishes the first owner; later holders add multiplicity. The log retains
its genuine extra live-handle owner.

Allocator, mini, B-tree, routing-filter and memtable retain entry points return
status and carry ignored-result warnings. Caller failure paths release acquired
prefixes and locks. Snapshot failure leaves an empty output. Serialized trunk
nodes remain private until their outgoing ownership has been acquired; abort
releases only acquired references and directly discards the provisional extent.

Ondisk-reference replacement retains the source before releasing the destination;
move transfers existing ownership without another retain. Lookup results stage
ownership before copying message descriptors. B-tree old-value capture occurs
before the callback and mutation. Iterator capture and catch-up unwind prefixes
and propagate acquisition errors. Optional maintenance failure after publication
preserves the committed tree and reports through existing logging/tracker paths.

Blob construction retains the existing mini allocation contract: successful
attachments before a later failure remain owned by the construction mini until
its teardown. No failed descriptor is published. Public SplinterDB signatures
are unchanged; existing status/errno returns carry acquisition errors. See
[FALLIBLE_RETAINS.md](FALLIBLE_RETAINS.md) for contracts and regression coverage.

## Persistence and compatibility

Allocator format **3** stores a checksummed four-bit array plus a linked chain
of extents holding sorted `(extent_number, uint64_raw_count)` records. The disk
format encodes no hash buckets or capacity, so mount reconstructs the hash table
independently of its implementation. See [SNAPSHOT_FORMAT.md](SNAPSHOT_FORMAT.md)
for the complete byte layout, checksums and validation rules.

Only clean, quiescent shutdown writes the allocation snapshot. It allocates the
whole overflow chain before serializing tags, writes chain/tags/header, issues
a durability barrier, and only then permits the caller to publish the snapshot
address in the superblock. Failed unpublished writes release their private chain
and leave the allocation marker invalid; next mount reconstructs ownership.
Publication failure after a successful write retains the chain conservatively.

A clean mount retains snapshot-chain ownership until it has durably invalidated
the saved descriptor, then releases that storage. Malformed clean snapshots
return an error; an invalid allocation marker selects graph reconstruction.
There is no concurrent allocation checkpoint protocol.

Version-1 and version-2 allocator images are rejected. No migration reader is
provided; use their corresponding build to export data before recreating a
version-3 database. The mini metadata byte layout is unchanged.

## Validation and performance evidence

[FINAL_RESULTS.md](FINAL_RESULTS.md) records the final normal-build validation
and controlled insertion comparison against `6e550e1e`. It includes exact source
and evidence digests and distinguishes insertion throughput from shutdown costs.

The earlier [fallible-retain validation](FALLIBLE_RETAINS.md) and
[format-3 snapshot validation](results/snapshot-20261026/validation.json) remain
historical evidence for their recorded source versions. The earlier
[expanded width comparison](EXPANDED_RESULTS.md) measured 32-bit exact counts,
format 2 and infallible callers; its two/eight-bit variants and dense ablations
are historical experiments, not supported build configurations.

## Reproduction

`prepare_final.py` freezes the selected baseline and current source, builds
release libraries with ordinary build flags, links the same controlled harness,
and records source/library/binary hashes. Run preparation under shared campaign
locks. `run_expanded.py` consumes its manifest and a fixed workload configuration.
Before measurement all child agents must be idle; the runner reserves
`benchmark.lock`, obtains matching pause acknowledgements from every active
project, and takes `machine.lock` exclusively. It records raw results, commands,
verification, phase markers and reservation/release evidence.

The standalone counter microbenchmark compares the four-bit container to a
clearly labelled dense64 checked-CAS control. That control is benchmark code,
not a production allocator option. Promotion/demotion uses the actual escape
boundary and warms overflow capacity before timing. Final map and overflow
counts are checked. The timed loop includes operation generation, dispatch,
checks and worker wakeup; it does not measure first-growth allocation latency.

Historical reproduction scripts are explicitly restricted to their original
source snapshots. They cannot re-enable removed production representations.
