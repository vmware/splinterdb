# Cross-thread page handles

This branch is stacked on project 1, `robj/cache-errors`, including its final
prefetch admission repair. Compare performance against its compact receipt
implementation to isolate the handoff change.

## Implementation and contract

Every successful synchronous get, completed async get, and allocation produces a
caller-owned `page_handle`. The handle records the cache read-reference stripe
that was incremented. Releasing or upgrading the handle uses
that recorded stripe, so its owner may move it to another registered thread with
ordinary synchronization. The acquiring thread may exit and its registration ID
may be reused while the handle remains live. A copied handle does not acquire an
extra reference: ownership still transfers and the reference is released once.

Project 1 supplies caller-owned storage for both `cache_get()` and
`cache_alloc()`. This change adds the acquisition stripe to each successful
receipt. Sync and async gets never modify another user's receipt.

Public iterators may be advanced, reversed, and destroyed on a different thread;
only one thread may access an iterator at a time and it must be destroyed before
closing its database. The existing warning about a live mutable-leaf iterator
blocking an insert remains applicable.

The retained handle grows from 8 to 16 bytes on this platform: a shared identity
pointer plus the acquisition reference-stripe receipt. Cache entries keep a
shared 16-byte identity and remain 64 bytes. Reference increments/decrements already used atomic
read-modify-write operations, and the stripe loads were already sequentially
consistent. Their operations and ordering are unchanged. Lock upgrade waits for
all references except the single reference represented by the given handle,
including extra references in the same stripe after registration-ID reuse.

## Regression-hook build guard

Prefetch regression hooks use `#if SPLINTER_DEBUG`, because the platform header
defines that macro as zero in release builds. `#ifdef SPLINTER_DEBUG` would
include hook fields and conditional calls in release. Debug hooks remain
available to regression tests. Handle layout, error handling, prefetch admission
protection and reference-counter operations/ordering are unchanged.
