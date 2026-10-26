# Packed refcounts and normalized mini ownership

Implementation dated 2026-10-26 on `robj/allocation-table`, based on the original
`6e550e1e` source and the preceding design investigation `5733e6d9`.

The branch now contains a real packed backend wired into `rc_allocator`, selected
with `make BUILD_RC_COMPACT=1`. The default remains `BUILD_RC_COMPACT=0` while the
existing infallible ownership APIs cannot recover from overflow-table OOM.
Both builds include the independently useful mini-allocator ownership cleanup.

## Representation and concurrency

`src/rc_refcount.{h,c}` stores 32 two-bit tags per 64-bit word:

- 0: free;
- 1: allocated, no logical references, awaiting final reclamation;
- 2: one reference (`AL_ONE_REF`);
- 3: an exact raw count >=3 in the overflow table.

The overflow table is partitioned into 64 cache-line-aligned shards. Each shard
uses a mutex, full 64-bit extent identities, linear probing, a power-of-two
capacity, growth before exceeding 70% occupancy, and backward-shift deletion.
Initial tables are lazy. Removal deletes the record; allocated table capacity is
retained until reset/deinit, so peak sharing can leave capacity slack afterward.
This experiment chooses linear probing; it does not compare a vectorized
power-of-two-choice table or overflow chains.

Changes wholly within 0/1/2 use a word CAS that preserves neighboring tags. Every
operation that reads or mutates an overflow entry holds its shard mutex and
rechecks the tag. A validity check reads only the nonzero tag and does not look
up the exact count. `allocator_ops.is_allocated` is optional so test allocators
can retain the old exact-count fallback.

The user's tag-three/entry equivalence holds at shard-lock release and for
synchronized observers. It cannot literally hold between two separate memory
writes within an operation:

- Promotion locks and rechecks, reserves/grows space, inserts a tentative entry,
  then CASes 2 to 3. If an inline decrement wins, promotion erases the tentative
  entry before unlocking and retries. A tag-three reader must lock before using
  an entry, so it cannot see an unfinished insertion.
- Demotion locks, publishes 3 to 2, then erases the entry before unlocking. An
  inline decrement may proceed meanwhile; a competing promotion waits for the
  old entry's removal and rechecks afterward.
- A reader that saw 3 before a demotion waits for the mutex and rereads the tag;
  it never follows a stale pointer into an erased or resized table.

Higher counts increment/decrement under the shard lock. Decrement never allocates.
The container preserves the full uint32 count domain and reports saturation
without changing the count. Debug scheduling hooks make the boundary races
reproducible; release builds contain no hook branches.

## Mini-allocator cleanup

A new mini-allocator adopts its caller's already allocated head extent as its
first external owner. The old synthetic increment is removed. A singly owned
branch, filter, or memtable head therefore has raw count 2 instead of 3 and needs no
overflow entry. Real sharing still promotes it normally.

The last external release takes the head to 1 while its contents are reclaimed.
Head reclamation skips the decrement already performed; continuation metadata
extents still take their own 2-to-1-to-0 path. Recovery's first metadata-head visit
establishes the first owner, while later holders add their multiplicity. Failed
initialization leaves the original allocation owned by the caller. The log
handle's real additional owner remains counted.

This changes the interpretation of persisted mini-head counts. The allocator
format is bumped from 1 to 2, for **both** dense and packed builds. Existing version 1
images require the original build or an explicit migration; this branch does not
silently load their biased counts. The mini-allocator metadata byte format is
unchanged. The packed backend itself preserves the dense32 disk layout, including
reserved-map geometry. Clean load/persist stream that layout through at most 1 MiB
of page-aligned scratch memory instead of allocating a full dense staging map.

## Fallible incref

The container's `rc_refcount_try_inc()` is fallible: failed growth leaves the
logical count and output parameter unchanged. Quiescent import/recovery also
returns allocation errors and never marks an incomplete map valid.

The surrounding allocator API is still infallible. The opt-in adapter emits an
unconditional assertion diagnostic and stops on a failed retain; it does not
fabricate success. **This is an experimental OOM limitation, not recoverable
production behavior.** No dense fallback or worst-case preallocated overflow
capacity is hidden in the memory figures.

Making all retains fallible is a separate ownership-rollback change. The old four
raw increment call sites fan out through mini/btree/filter retains, `ondisk_ref`
callbacks and copy/replace, lookup-result replacement, iterator acquisition loops,
and trunk serialization. Removing the synthetic mini increment leaves three raw
call sites, but first sharing can now occur in ordinary lookup-result copies.

Required examples of correct failure behavior:

- Failed `ondisk_ref_init` must leave a null destination.
- Failed replacement must retain the old owner and message, without exposing a
  new descriptor whose owner was never acquired.
- A trunk serialization failure must release precisely the acquired prefix of
  child references before discarding its unpublished output; current code has an
  explicit boundary after which no fallible work remains.
- `mini_attach_extent` must restore its batch lock/state on a failed retain.

Sticky per-extent reservations could move allocation earlier, but retaining a
normal overflow record at count 2 conflicts with the requested encoding/invariant.
A separate reservation structure adds memory and lifetime machinery. Reserving
for every potentially shared extent also destroys the sparse-memory premise.

## Correctness evidence

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
`.cache/mini-refcount-validation/` and `.cache/rc-validation/`. No production
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
normalized packed counts; this separates normalization from compression. All use
the same statistics setting, cache, memtable and background-thread configuration.
Allocator statistics now include an O(configured-extents) count census, taken
outside the timed operation phases; it is not continuous instrumentation.

## Measurement status

The frozen comparison runner requested a quiet window, but blob-keys,
public-blobs, blob-callbacks, and insertion-stalls still registered as active
without providing fresh acknowledgements. It cancelled the reservation after
120 seconds and ran **zero measurements**. The user has been asked to confirm
whether those sessions and their children have finished or are fully paused.
Evidence is `.cache/rc-comparison-20261026-a/{request,release}.json`.

No speedup, regression, tail-latency or representative sharing-density claim is
supported yet. The implementation and correctness evidence are ready; quantitative
adoption advice awaits the coordinated comparison. The earlier sizing formulas
remain analytical estimates, not measured workload footprints.
