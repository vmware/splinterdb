# Cross-thread page handles

This branch is stacked on project 1, `robj/cache-errors` at `c409c6b`. Compare
performance against that commit to isolate the handoff change.

## Implementation and contract

Every successful synchronous get, completed async get, and allocation produces a
caller-owned `page_handle`. The handle records the registration ID whose cache
read-reference stripe was incremented. Releasing or upgrading the handle uses
that recorded stripe, so its owner may move it to another registered thread with
ordinary synchronization. The acquiring thread may exit and its registration ID
may be reused while the handle remains live. A copied handle does not acquire an
extra reference: ownership still transfers and the reference is released once.

`cache_alloc()` now accepts an output handle and returns that handle on success
(or NULL with a cleared handle on failure). Retained log-page handles are stored
by value, and mini-allocator metadata rollover preserves the old receipt until
its page is released. Sync and async gets never modify another user's receipt.

Public iterators may be advanced, reversed, and destroyed on a different thread;
only one thread may access an iterator at a time and it must be destroyed before
closing its database. The existing warning about a live mutable-leaf iterator
blocking an insert remains applicable.

The public handle grows from 16 to 24 bytes on this platform. Cache entries keep
a separate private 16-byte page identity and remain 64 bytes; a static assertion
checks this layout. Reference increments/decrements already used atomic
read-modify-write operations, and the stripe loads were already sequentially
consistent. Their operations and ordering are unchanged. Lock upgrade waits for
all references except the single reference represented by the given handle,
including extra references in the same stripe after registration-ID reuse.

## Validation

The final private-slot layout passed the following checks on 2026-10-11.
The full debug build passed, along with all 16 writeback-set tests, all 48
public quick tests, all 3 trunk-split tests, both async trunk tests, both log
allocation/packing tests, and the listed functional drivers. The release
library and driver also built successfully. Reproduce with:

```
make --jobserver-style=pipe -j4 BUILD_MODE=debug all
build/debug/bin/unit/writeback_set_test
build/debug/bin/unit/splinterdb_quick_test
build/debug/bin/unit/trunk_split_test
build/debug/bin/unit/trunk_async_test
build/debug/bin/driver_test log_test --cache-capacity-mib 64 --db-capacity-gib 2
build/debug/bin/unit/splinter_test test_shard_log_
build/debug/bin/driver_test cache_test --async --cache-capacity-mib 64 --db-capacity-gib 2
build/debug/bin/driver_test splinter_test --functionality 100000 100 --cache-capacity-mib 64 --db-capacity-gib 2
```

Dedicated tests exercise sync and async acquisition, extracting an async result
on the receiving thread, allocation followed by an independent get, claim/write
upgrade after transfer, release after original-thread exit and ID reuse, and
public iterator next/prev/deinit on live memtables and persisted branches.

## Performance

End-to-end comparison against project 1 is pending. No performance conclusion is
claimed yet. The campaign builds release artifacts and its common `kvbench`
against each branch's static library, preserving build provenance. All benchmark
database files use the campaign's ext4 disk directory, with exclusive benchmark
and machine locks. Build and correctness jobs also use
`.cache/projects-20261011/coordination/run_job.py` to avoid benchmark interference.
