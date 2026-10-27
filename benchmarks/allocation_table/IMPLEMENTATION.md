# Packed refcounts and normalized mini ownership

Implementation dated 2026-10-26 on `robj/allocation-table`, based on the original
`6e550e1e` source and the preceding design investigation `5733e6d9`.

The branch now contains a real packed backend wired into `rc_allocator`, selected
with `make BUILD_RC_COMPACT=1`. The default remains `BUILD_RC_COMPACT=0`; compact operation is selected
explicitly. Both backends now expose fallible extent retention, and ownership
callers propagate failures with rollback.
Both builds include the independently useful mini-allocator ownership cleanup.
The current implementation uses 64-bit exact refcounts and allocator disk format
3. Those changes followed the throughput campaign: the frozen results below
measured the earlier 32-bit-count, format-2 implementation, not this revision.

## Representation and concurrency

`src/rc_refcount.{h,c}` supports `BUILD_RC_BITS=2`, `4` (default), or `8`.
Each 64-bit CAS word stores 32, 16, or 8 tags respectively. For the default
four-bit representation:

- 0: free;
- 1: allocated, no logical references, awaiting final reclamation;
- 2: one reference (`AL_ONE_REF`);
- 3 through 14: inline raw counts, representing 2 through 13 logical owners;
- 15: an exact raw count >=15 in the overflow table.

The all-ones raw value is the escape at every width: 3 for two bits and 255
for eight bits. The eight-bit representation can hold up to 253 logical owners
inline after accounting for the allocation floor. Inline increments/decrements
use the same packed-word CAS protocol at every width.

The overflow table is partitioned into 64 cache-line-aligned shards. Each shard
uses a mutex, full 64-bit extent identities and exact 64-bit counts, linear
probing, a power-of-two capacity, growth before exceeding 70% occupancy, and backward-shift deletion.
Initial tables are lazy. Removal deletes the record; allocated table capacity is
retained until reset/deinit, so peak sharing can leave capacity slack afterward.
This experiment chooses linear probing; it does not compare a vectorized
power-of-two-choice table or overflow chains.

Changes wholly within the inline range use a word CAS that preserves neighboring
tags. Every operation that reads or mutates an overflow entry holds its shard
mutex and rechecks the tag. A validity check reads only the nonzero tag and does not look
up the exact count. `allocator_ops.is_allocated` is optional so test allocators
can retain the old exact-count fallback.

The escape-tag/entry equivalence holds at shard-lock release and for
synchronized observers. It cannot literally hold between two separate memory
writes within an operation:

- Promotion locks and rechecks, reserves/grows space, inserts a tentative entry,
  then CASes 14 to 15 for four-bit tags. If an inline decrement wins, promotion
  erases the tentative entry before unlocking and retries. An escape-tag reader
  must lock before using an entry, so it cannot see an unfinished insertion.
- Demotion locks, publishes 15 to 14, then erases the entry before unlocking. An
  inline decrement may proceed meanwhile; a competing promotion waits for the
  old entry's removal and rechecks afterward.
- A reader that saw 15 before a demotion waits for the mutex and rereads the tag;
  it never follows a stale pointer into an erased or resized table.

Higher counts increment/decrement under the shard lock. Decrement never allocates.
The container preserves the full uint64 count domain and reports saturation
without changing the count. Debug scheduling hooks make the boundary races
reproducible; release builds contain no hook branches.

## Mini-allocator cleanup

A new mini-allocator adopts its caller's already allocated head extent as its
first external owner. The old synthetic increment is removed. A singly owned
branch, filter, or memtable head therefore has raw count 2 instead of 3 and needs no
overflow entry. Sharing increments the inline count and promotes it when the
escape threshold is reached.

The last external release takes the head to 1 while its contents are reclaimed.
Head reclamation skips the decrement already performed; continuation metadata
extents still take their own 2-to-1-to-0 path. Recovery's first metadata-head visit
establishes the first owner, while later holders add their multiplicity. Failed
initialization leaves the original allocation owned by the caller. The log
handle's real additional owner remains counted.

The ownership cleanup first changed allocator format 1 to 2. The current
implementation now uses **allocator format 3 for every RAM backend**. Existing
version-1 and version-2 images require their corresponding build or an explicit
migration; this branch rejects them. The mini-allocator metadata byte format is
unchanged.

## Durable allocation snapshots

The on-disk representation is independent of `BUILD_RC_BITS`: a fixed four-bit
array stores raw counts 0 through 14, with tag 15 redirecting to sorted
`(extent_number, uint64_count)` records in a chain of allocator-owned extents.
The fixed map begins at extent 1 and contains one checksummed header page followed
by the page-padded nibble array. Only the four-bit array and header are fixed
reservations; the overflow chain grows with actual counts at or above 15.

Every chain extent owns raw count `AL_ONE_REF` in the snapshot itself. Clean
shutdown allocates the chain before writing the map, writes chain blocks, tags
and header, then issues the I/O durability barrier. The caller publishes the
superblock's nonzero `allocation_state_addr` only after that succeeds. This is a
quiescent clean-shutdown optimization, **not a concurrent checkpoint protocol**.

A clean mount streams and validates the snapshot, retaining its chain extents.
Only after durably invalidating the saved allocation descriptor does core release
the old chain back to the allocator. A crash before invalidation can therefore
still load the old chain. A recovery mount ignores an invalid snapshot and
reconstructs allocation from the durable graph instead.

The shared reader in `src/rc_allocator_disk.{h,c}` validates geometry, header and
block checksums, aggregate tag/chain checksums, sorted exact tag-to-record
correspondence, chain links and self-ownership, and canonical zero padding.
It uses at most a 1 MiB tag buffer, one extent buffer and one page buffer. Its
allocator callback separately retains an O(chain-extents) address vector so those
extents can be released after invalidation; the inspector needs no vector. The
writer uses a tag buffer, one extent buffer and an exact chain-address vector,
without constructing a full dense staging map.

Read/validation/import failure leaves the allocator map invalid; partial imported
counts must be reset or discarded. A failed unpublished write releases its newly
allocated chain extents, leaves the output descriptor untouched, and requires
recovery if the fixed map was partially overwritten. Publication failure after
a successful write retains chain ownership because durability can be ambiguous.
Full layout, checksum definitions and lifecycle details are in
[allocation snapshot format](SNAPSHOT_FORMAT.md).

## Fallible extent retention

`allocator_inc_ref(al, addr, new_count)` returns `platform_status`, with an
optional output count. Allocation failure, a free extent, or saturation leaves
both the count and output unchanged. Compact counts delegate to
`rc_refcount_try_inc`; dense counts use a checked CAS loop. The former fail-stop
adapter has been removed. Public internal retain entry points are marked
`warn_unused_result` so ignored failures are compiler errors in normal builds.

Mini, B-tree, routing-filter and memtable retains return status. Mini attachment
unlocks its batch without appending on retain failure; a later append failure
releases its acquired reference. Log creation unwinds its mini and groups if the
live handle cannot acquire its additional owner. B-tree blob lookup, asynchronous
lookup, and old-value capture release page handles and return acquisition errors.
Old-value capture runs before the insertion callback and mutation.

`ondisk_ref_init` and `ondisk_ref_copy` leave null destinations on failure;
`ondisk_ref_replace` preserves the old owner. `ondisk_ref_move` transfers an
already-owned reference without another fallible acquisition. Lookup results
stage their blob owner before copying the borrowed message descriptor and publish
both only after success.

Trunk bundle/node acquisition rolls back only the successful prefix. Node
serialization retains every outgoing reference before publication; a failed
attempt discards its private serialized extent without traversing borrowed
references. Snapshot failure leaves an empty snapshot. Iterator capture restores
its original branch count and releases captured references and locks. Compaction
setup propagates the original error and unwinds its acquired inputs and state.

This changes internal APIs, not disk format 3 or the public API signatures.
Existing mini allocation semantics still apply to blob construction: successful
prefix attachments remain owned by the destination mini until its teardown if a
later construction step fails. No failed descriptor is published; abort cleanup
must release the destination mini as usual.

Current failure coverage and validation are recorded in
[fallible retain implementation](FALLIBLE_RETAINS.md).

## Format-3 and 64-bit validation (2026-10-26)

Current correctness validation passed:

- 88 counter-container cases: debug 2/4/8-bit and release 4-bit, private/shared
  heaps, including crossing UINT32_MAX and saturation at UINT64_MAX.
- 104 allocation-snapshot cases: dense64 and 2/4/8-bit debug, 4-bit release,
  and 4-bit AddressSanitizer, on private/shared heaps. These cover 9,000 wide
  counts over two overflow extents, every snapshot read/write position and
  barrier failure, partial and complete disk exhaustion, import OOM, release
  ordering/preflight, 17 malformed encodings, truncation, and the 1 MiB streaming
  boundary. ASan ran with leak detection disabled; no LSan result is claimed.
- Four targeted persistence-allocation OOM runs: debug and ASan on both heaps,
  faulting the tag buffer, node buffer, and chain-address vector. Each preserves
  the live map/output descriptor, performs no I/O, and allows a successful retry.
- The 62 existing quick public-API cases and 17 superblock cases on both heaps.
  One additional public-API case on both heaps injects snapshot-header write and
  snapshot-barrier failure during close, checks the invalid durable marker, then
  reopens through recovery and verifies the stored value.
- Cache correctness and a 200,000-insert persisted-versus-rebuilt allocation-map
  comparison, each on both heaps. Recovery expectations exclude temporary
  snapshot-chain extents; those correctly become free during a graph rebuild.
- Eight offline-inspector checks, including a linked snapshot with 64-bit counts,
  corruption, truncation, dirty state and format-2 rejection. Every inspection
  preserved the input image hash.
- All test targets and the inspector build in compact-4 debug mode; changed C
  sources match clang-format-19 and whitespace checks pass.

No throughput benchmarks were run for this revision. These are software failure
injection and recovery checks, not a power-loss/torn-sector durability proof.
Exact commands and logs are retained in `.cache/rc-count64-validation/`,
`.cache/rc-snapshot-test-validation/`, `.cache/rc-snapshot-validation/`, and
`.cache/rc-snapshot-review/`. A tracked evidence digest is in
[the validation manifest](results/snapshot-20261026/validation.json).

## Earlier correctness evidence

The following completed results cover the earlier format-2/32-bit-count
prototype. They are historical validation, not a claim that format-3 persistence
has passed the same tests. The new codec and inspector objects compile cleanly. Eight inspector checks
passed on the current debug build: a fresh 1,000-record public-API database,
an independently encoded overflow chain including counts 2^40 and UINT64_MAX,
dirty-state unavailability, old-format rejection, header/tag/node corruption,
and truncation. All eight verified identical file SHA-256 hashes before and
after inspection. Evidence is `.cache/rc-snapshot-validation/inspector/`.
Additional format-3 fault, rollback, round-trip and integration validation is
recorded separately while that implementation is completed.


The new mini ownership regression fails on the old implementation: expected raw count 2,
observed3. Three new tests cover real ownership transitions, a metadata chain
crossing an extent boundary with repeated shared data, exact recovery equivalence,
and reserve-allocation failure rollback.

Dense debug passed 9 targeted executions. Compact debug passed 10, including
private/shared heaps, reclamation failures, and a 33 GiB sparse device whose durable
map crosses the 1 MiB transfer boundary. Compact release passed 87 tests: 17
superblock, 1 trunk GC, 3 trunk split, 62 quick-suite, 3 mini ownership, and 1
persisted-map/recovery comparison.

Final container release tests passed 6/6 on each of private/shared heaps.
AddressSanitizer plus LeakSanitizer passed all 10 debug cases on each heap after
rerunning outside the sandbox's ptrace restrictions. A separate 128-key image
check rejected a version-1 image without changing its bytes, then verified
normalized dense-to-packed-to-dense reopen compatibility.

Container tests include randomized differential counts, collision deletion and
growth, neighboring packed-word updates, shared-count concurrency, shared-heap
forking, OOM/saturation no-change behavior, and deterministic promotion/demotion
interleavings. Validation found and fixed freeing NULL through the shared heap,
whose free routine does not accept NULL. Page-aligned heap scratch also avoids
making streaming persistence depend on global huge-page mapping configuration.

Exact commands, logs and source hashes remain under
`.cache/mini-refcount-validation/` and `.cache/rc-validation/`. No concurrent
checkpoint protocol was added. Recovery remains serial/quiescent, and the compact
import setter does not create a parallel-rebuild contract.

## Reproducing the comparison

Use separate build roots:

```
make -j4 BUILD_ROOT=build-rc-dense BUILD_MODE=release BUILD_RC_COMPACT=0 libs
make -j4 BUILD_ROOT=build-rc-compact BUILD_MODE=release BUILD_RC_COMPACT=1 libs
```

Create an original source snapshot with `git archive 5733e6d9` and build its release
library under `build/release`. `prepare_comparison.py --baseline SNAPSHOT --output
NEW_DIRECTORY` links frozen binaries and records compiler commands, library and
binary hashes, and the modified source hashes. Run preparation under the shared
campaign locks; it does not measure performance.

`run_comparison.py` accepts that manifest, a new output directory, and the shared
coordination/lock directories. Before invoking it, stop every child agent. It
reserves `benchmark.lock`, announces a new request ID, waits for every active
project's matching paused acknowledgement, then holds `machine.lock` exclusively
through measurement. It cancels if acknowledgements do not arrive within 120 seconds.
The runner freezes binaries, alternates backend order, preserves raw results and
logs, and checks each microbenchmark's final map and paired operation checksum.

The microbenchmark covers exact lookup, allocated-state lookup, claim/retire/free,
promotion/demotion, stable high counts, and one heavily contended extent. Mutation
streams are identical between backends. Promotion tables are warmed before timing;
measured growth invalidates a sample. Therefore this case does **not** measure
first-promotion allocation/growth latency. Timings include operation generation,
dispatch, result checks and worker wakeup, but exclude initialization and final
full-map validation. Memory reports include requested container, array and retained
hash capacity, excluding heap headers and temporary growth overlap.

Database comparisons use the original build, normalized dense counts, and
normalized packed counts; this isolates packing from the shared ownership and validity-check changes. All use
the same statistics setting, cache, memtable and background-thread configuration.
Allocator statistics include an O(configured-extents) count census; it is not
continuous instrumentation. The comparison runner disables printing it before
close to avoid an untimed gap in which background work could drain.

## Measurement status

All throughput figures in this section are from frozen **32-bit exact refcounts
and format 2**, before the current 64-bit refcount and compact snapshot changes.
They remain evidence about those specific binaries. No throughput or close-time
result for format 3 is claimed here.


The expanded campaign completed 184 throughput runs (2.34 billion inserts and
reopened scans) plus three separate diagnostic runs, under exclusive locks and
fresh peer pause acknowledgements. See [expanded results](EXPANDED_RESULTS.md)
for the larger-cache/capacity matrix, paired uncertainty intervals, counter-width
comparisons and dense ablations. The original normalized-dense slowdown did not
reproduce. Four-bit counters reduced measured hash shard acquisitions by 99.40%
and met the ±3% throughput-equivalence criterion in the 30M-record/4GiB-cache case.
Other comparisons have wider intervals; no universal throughput winner is claimed.

The earlier three-repetition sample remains in [insertion results](INSERTION_RESULTS.md)
as historical evidence. Cancelled reservations ran zero measurements and are
excluded. No microbenchmark speed claims or production checkpoint changes are made.

## Expanded counter-width validation (historical 32-bit counts)

The 2/4/8-bit extension passed 66 debug container cases (11 per width on each
private/shared heap) and 42 release container cases (7 per width on each heap).
For each of four-bit and eight-bit backends, 66 integration tests passed:
three mini ownership cases, one recovery-map equivalence case, and 62 quick
public-API cases. Width-boundary tests exercise promotion, demotion, failed
promotion, stale escaped readers, saturation, and every inline count while
injected allocation failure proves that inline updates do not allocate.
Evidence is in `.cache/rc-expanded-validation/`.

The controlled macro harness uses fixed foreground CPU placement and a separate
background CPU pool, waits for foreground affinity setup before measurement,
and prints allocator statistics only after persisted verification. The seven
frozen variants include original, ownership-only, full dense, full dense with
the original validity dispatch, and packed 2/4/8-bit counters. These benchmark
ablations are isolated source snapshots; production code retains the normal
validity API. Detailed historical results are in [expanded results](EXPANDED_RESULTS.md).
