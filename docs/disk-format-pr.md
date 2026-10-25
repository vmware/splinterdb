Persist the versions and reader settings needed to interpret existing database structures. Format 8 rejects older files clearly; reopen uses saved routing-filter widths/hash seeds and log checksum seeds, while current configuration controls newly constructed instances. Branch versions travel with their references, mini-allocator pages validate their headers, and each owning module decides which versions it supports. The newest valid superblock is selected before compatibility checks, so unsupported versions recognizable under the current checksum layout cannot silently fall back to an older checkpoint.

This branch includes main `0fa3bd6c`, retaining transferable cache handles, multi-level blobs, widened statistics and the notification-based durable-barrier API. Applications must still supply compatible comparison, hash and merge callbacks. There is no migration reader or portable cross-architecture encoding.

Regression coverage includes invalid versions, changed configuration across
reopen/compaction, synchronous/asynchronous reads, cache errors, recovery and
blobs.
