# Fallible cache reads and caller-owned page handles

This branch changes `cache_get()` and the asynchronous cache result API to return
`platform_status` and fill a caller-owned `page_handle`. A successful handle owns
one read reference. Moving its value transfers that reference; copying the value
does not acquire another one. Cross-thread ownership transfer is a separate,
stacked experiment.

The receipt is now pointer-sized: it names a shared `cache_page` whose identity
is stable while the reference is held. `page_handle_data()` and
`page_handle_addr()` require a live handle; `cache_unget()` clears the receipt.
This avoids reconstructing the cache slot from the data-buffer address and
keeps btree/iterator handle fields at their original pointer size. Cache entries
remain 64 bytes. `cache_alloc()` also fills caller-owned storage; retained log
allocations and mini-allocator rollover preserve their receipt lifetimes.

Demand-read errors propagate through btrees, blobs, routing filters, log recovery,
trunk traversal, insertion, point lookups, and range initialization. A failed read
clears the output handle and releases its reference. The cache removes failed
loads from its directory and wakes waiters, permitting later reads to retry.
Shared cache-entry identity reads and writes use relaxed atomics because stale
lookup candidates can inspect a reused loading slot or a failed-load invalidation.
`CC_LOADING` still provides the publication barrier for bytes and read status.
No stronger ordering was added to the read-reference counters.

Nonblocking cache misses or write conflicts return `STATUS_BUSY`. Speculative
prefetch failures drop the hint; a subsequent demand read reports its own result.

Reclamation follows the agreed space-loss policy: consume the final external
reference once, retain any space left after failure, and never restart a partially
completed reclamation. The allocator remembers the first failure and refuses to
persist a clean allocation map. Close checks this after destructive cleanup,
prints an explicit warning, and leaves the recovery-required superblock marker.
A close that preserved the data can still return success under the existing
close contract. Reopen rebuilds allocation state from reachable roots and does
not read abandoned, unreachable metadata.

## Prefetch admission safety

A follow-up fixes an inherited prefetch admission race. A reader could retain an
old lookup candidate while prefetch reused that slot, set the same address, then
lost publication to another loader. The reader could acquire the unpublished
loser's reference before that slot was freed. Prefetch now keeps `WRITELOCKED`
until its directory CAS succeeds, clearing it before appending the I/O page.
Demand admission still initializes identity after its CAS and is unchanged.

Prefetch regression hooks use `#if SPLINTER_DEBUG` in the selected handoff
implementation: `#ifdef SPLINTER_DEBUG` also accepts a macro defined as zero
and therefore includes hooks in release builds.
