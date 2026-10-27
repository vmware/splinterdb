# Fallible extent retention

Implemented on `robj/allocation-table`, based on `528b5344`, validated 2026-10-27.
The final branch uses four-bit tags exclusively, with no representation build
option. Disk format 3 and exact 64-bit counts are specified in
[SNAPSHOT_FORMAT.md](SNAPSHOT_FORMAT.md). The validation below records the
2026-10-27 implementation before removing the experimental alternatives; final
normal-build validation is in [FINAL_RESULTS.md](FINAL_RESULTS.md).

## API and ownership contracts

| API | Success | Failure |
|---|---|---|
| `allocator_inc_ref(al, addr, new_count)` | Acquires one extent reference; optional output receives the new raw count | Count and output unchanged by this attempt |
| `mini_inc_ref`, `btree_inc_ref`, `routing_filter_inc_ref`, `memtable_root_inc_ref` | Acquires one object owner | Returns the underlying status without acquiring ownership |
| `ondisk_ref_init`, `ondisk_ref_copy` | Initializes an owned reference | Destination is null |
| `ondisk_ref_replace` | Acquires the source before releasing the destination | Both existing owners are preserved |
| `ondisk_ref_move` | Transfers ownership without another retain | Infallible; source becomes null |
| Trunk snapshot capture | Returns an owned root | Output root remains zero |
| Trunk branch collection | Appends owned branches | Restores the original count and leaves the original prefix untouched |

Retain status is `STATUS_NO_MEMORY` if overflow allocation fails,
`STATUS_LIMIT_EXCEEDED` at `UINT64_MAX`, or `STATUS_INVALID_STATE` for a free
extent. Other threads may independently modify counts. The four-bit container preserves failed-attempt semantics. The earlier dense
control also used a checked CAS with the same sequentially consistent success
ordering as its former atomic increment.
Optional output pointers are supported through the allocator and compact
container. Retain entry points have `warn_unused_result`; the former assertion
adapter is removed.

The changed operations are allocator-backed extent ownership. Cache read-pin
counters and in-memory task/state references are separate nonallocating counters.
No public SplinterDB function signature changes: existing status/errno returns
carry acquisition errors to the caller.

## Failure boundaries

Mini attachment acquires ownership before appending a metadata entry. A failed
retain releases the batch lock without an append; append failure releases the
acquired reference. Log construction unwinds its mini and group pools when its
extra live-handle reference cannot be acquired.

Synchronous and asynchronous blob lookup release their page handles if either
the temporary owner or result owner cannot be acquired. Lookup results stage an
owner before copying the message descriptor; message-buffer allocation failure
releases that staged owner. B-tree old-value capture precedes the insertion
callback and mutation, so a retain failure leaves the value unapplied and does
not invoke the callback.

Trunk bundle and node acquisition track successful prefixes separately for
children, maplets and branches. Failure releases only those references. A new
serialized extent remains private until every outgoing retain succeeds. Abort
frees its provisional wrapper and discards its raw extent directly, without
traversing references that its bytes merely borrowed. Private constructors return
the original status rather than conflating retain failure with allocation OOM.

Iterator capture and fetch-and-update catch-up release capture locks and acquired
prefixes on error. They return acquisition errors instead of treating OOM as a
retryable generation race. Compaction setup acquires inputs before publishing its
state or attaching tracking. Optional maintenance failure after incorporation
preserves the already published tree; existing logging/tracker paths report it
and the compaction is skipped/deferred. An already committed operation is not
reported as though its publication were rolled back.

Blob construction keeps the mini allocator's existing partial-allocation
contract: attachments completed before a later failure stay owned by the
construction mini until its normal teardown. Failed descriptors are not
published. Tests verify retry and eventual release of both partial and complete
clones; this change does not add transactional mini allocation.

## Validation

All final checks passed. No throughput benchmarks were run.

- Low-level debug checks across compact2/4/8 and dense, on private/shared heaps:
  actual overflow-allocation OOM, free-extent rejection, uint64 saturation,
  optional output, and failed-attempt count/output preservation.
- Four integration regressions on both heaps: mini lock/ownership rollback,
  log-handle construction cleanup, synchronous/asynchronous blob-owner failure,
  old-value callback exclusion, and partial blob-clone cleanup/retry.
- The complete 66-case public quick suite passed on private/shared heaps in
  debug, release and AddressSanitizer builds. New cases inject errors at both
  blob-owner acquisitions, both iterator capture positions, and both retained
  generations during a synchronized rotation/catch-up interleaving. Stored
  values, full refcount censuses and retries are checked.
- Ondisk owner tests passed in debug, release and ASan. A separate
  `BUILD_MEMORY_FAULT_INJECTION=1` run passed all five cases, including buffer
  allocation failure after owner acquisition.
- Trunk tests passed in debug, release and ASan: ten root-growth retain failure
  positions across four leaves and index parents; four populated COW/maplet/
  branch/inflight failure positions; three iterator-prefix failures; empty
  snapshot output and lock release. Tests compare the published root and every
  allocator count. Database fixtures ran with both private and shared heaps.
- Snapshot/container regressions and 17 superblock tests per heap passed;
  cache bootstrap/batch correctness checks passed on both heaps. All debug test
  targets and the inspector compile with ignored-result warnings enabled.
- Independent source review found no blocking issue in publication, prefix
  cleanup, capture locks, or compaction tracking. Changed C sources match
  clang-format-19 and whitespace checks pass.

ASan used `detect_leaks=0`; no LeakSanitizer result is claimed. These are software
failure/ownership tests, not power-loss testing. The earlier throughput results
measure earlier frozen implementations and do not quantify this revision.

Exact commands/logs are retained in `.cache/rc-fallible-low-validation/`,
`.cache/rc-fallible-core-validation/`, `.cache/rc-fallible-trunk-validation/`, and
`.cache/rc-fallible-validation/`. Final source and evidence digests, together with
per-log test summaries, are tracked in
[the validation manifest](results/fallible-20261027/validation.json). Recorded
build logs may include an earlier failed invocation; the final command records
and passing logs identify the accepted validation runs.
