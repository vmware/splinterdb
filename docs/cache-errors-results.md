# Fallible cache reads and caller-owned page handles

This branch changes `cache_get()` and the asynchronous cache result API to return
`platform_status` and fill a caller-owned `page_handle`. A successful handle owns
one read reference. Moving its value transfers that reference; copying the value
does not acquire another one. Cross-thread ownership transfer is a separate,
stacked experiment.

Demand-read errors propagate through btrees, blobs, routing filters, log recovery,
trunk traversal, insertion, point lookups, and range initialization. A failed read
clears the output handle and releases its reference. The cache removes failed
loads from its directory and wakes waiters, permitting later reads to retry.
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

## Validation

Build with `make --jobserver-style=pipe -j4 BUILD_MODE=debug all`.
The following debug checks passed on 2026-10-11:

- `build/debug/bin/unit/writeback_set_test`: 14 tests, including synchronous
  read errors and retry, async initialization/append/completion failures, queued
  async waiters, a synchronous waiter on a failed async load, failed prefetch,
  and reclamation failing after an earlier group of extents was already freed.
- `build/debug/bin/unit/btree_stress_test iterator_` and `count_in_range`:
  iterator setup and rank errors preserve a distinctive `STATUS_TIMEDOUT`.
- Complete `btree_test`, `trunk_async_test`, `trunk_gc_test`, `trunk_split_test`, and
  `splinterdb_quick_test` suites (7, 2, 1, 3, and 47 tests respectively).
  The async trunk tests cover a yielded pivot-page read error and stack lifetime.
- `build/debug/bin/unit/splinter_test test_cache_read_errors_reach_core_operations`:
  insertion, synchronous point lookups through memtables/trunk/branches,
  asynchronous lookup, and range initialization return the injected status;
  the database remains usable afterward.
- `build/debug/bin/unit/splinter_test test_late_reclamation` and `test_unmount_`:
  a real metadata-read failure after durable root publication leaves no clean
  allocation marker; reopen preserves data, skips the unreadable orphan,
  reclaims its allocation, and permits the next close to become clean.
- `build/debug/bin/driver_test cache_test --async --cache-capacity-mib 64
  --db-capacity-gib 2`: concurrent readers/writers, cache pressure, early
  prefetch wakeup, and idle/exited batch owners.
- `build/debug/bin/driver_test splinter_test --functionality 100000 100
  --cache-capacity-mib 64 --db-capacity-gib 2`: shadow-verified workload.

The full release build and the release core fault-propagation test also passed.

The full aggregate unit-test binary was interrupted while running the expensive
concurrent-insertion case; it is not reported as a completed suite.

## Performance

End-to-end comparison with base `db3047a` is pending. No performance conclusion
is claimed yet. Build release with `make --jobserver-style=pipe -j4
BUILD_MODE=release all`. Benchmark database files must be on real disk and runs
must be serialized with the shared campaign benchmark lock. The campaign uses
`.cache/projects-20261011/coordination/run_job.py` and its ext4 `benchmarks/`
directory; build and test jobs also use the coordinator to avoid benchmark load.
