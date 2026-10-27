# Allocation-table representation and concurrent checkpoints

SplinterDB now uses fixed four-bit allocation counters with exact 64-bit overflow
counts. There is no selectable dense backend or counter width. Start with the
[current implementation](IMPLEMENTATION.md), [snapshot format](SNAPSHOT_FORMAT.md),
[fallible ownership contracts](FALLIBLE_RETAINS.md), and
[final validation and throughput results](FINAL_RESULTS.md).

## Original design investigation (2026-10-25)

The study below records the original `6e550e1e` behavior and proposals before
implementation. Its recommendations, API descriptions, raw-three mini heads,
and statements that production sources are unchanged are historical.

Investigation dated 2026-10-25, based on `6e550e1e`. Branch
`robj/allocation-table`; isolated worktree
`.cache/projects-20261025/worktrees/allocation-table`.

**Recommendation:** retain the current refcount array until measurements identify
which cost matters. If allocation scans are expensive, first test a derived
hierarchical free bitmap. If map memory matters, compare byte counts with overflow
against two-bit states with overflow. Concurrent allocation checkpoints are
feasible with an array, but require a map of durable ownership, not a snapshot of
today's live counters. Do not put the live map in a concurrent checkpoint.

This directory contains source-grounded design analysis, an analytical sizing
calculator, and bounded executable correctness models. It does not change the
production allocator, persistence format, or recovery path. No benchmarks or
real refcount-distribution measurements were performed. The proposed protocols
below are not implemented concurrency or crash-recovery guarantees.

## What the current code actually represents

The implementation is a dense array of 32-bit counts, not a free list. Allocation
uses a scanning hand and a CAS to claim a free extent; its worst case scans all
configured extents (`src/rc_allocator.c:652`). The existing hand avoids repeatedly
starting at the beginning, so a bitmap must beat this implementation, not a
hypothetical naive scanner.

The raw count includes an allocation floor (`src/allocator.h:18`):

| Raw value | Meaning | Proposed two-bit encoding |
|---|---|---|
| 0 | Free and reusable | 0 |
| 1 | Allocated, no logical refs; reclamation may still be pending | 1 |
| 2 | One allocator reference | 2 |
| 3 or more | Additional references | 3, exact count in overflow table |

The transition through 1 is essential: cache mappings are discarded before the
last decrement makes the extent reusable (`src/mini_allocator.c:1036`,
`src/clockcache.c:2179`). It cannot be collapsed into free space.

The assumption that only shared branch/filter roots need larger counts is too
strong:

- A mini-allocator metadata head has raw count **3 even with one external
  owner**. Its internal baseline is 2, with external references above that
  (`src/mini_allocator.c:23,349,1028`). Thus unshared mini heads already escape.
- COW trunk nodes reference shared child trunk extents
  (`src/trunk.c:2315`). Root snapshots add temporary references too.
- Blob cloning attaches shared data and pointer extents through
  `mini_attach_extent()` (`src/mini_allocator.c:728`,
  `src/blob_build.c:187,340,368`). Shared extents are not restricted to roots.
- Recovery counts multiplicity of mini entries and external holders; its
  zero-count checks also serve as a visited set
  (`src/mini_allocator.c:1421,1486`, `src/trunk.c:2338`).

The concurrently developed public-blob, blob-key, and blob-callback projects
could increase sharing further. Their proposed behavior is not assumed landed
on this base. A compact implementation must support arbitrary extent identities
and counts, independent of page type.

## Space economics and allocation search

At the current 128 KiB extent geometry, 1 TiB of configured capacity has 8,388,608
extents. These are logical metadata sizes, excluding allocation overhead,
alignment, locks, resize slack, snapshot copies, and hash bucket rounding:

| Structure | Bytes per configured extent | Metadata per TiB |
|---|---:|---:|
| Current 32-bit count array | 4 | 32 MiB |
| Byte count array, before overflow | 1 | 8 MiB |
| Two-bit state array, before overflow | 0.25 | 2 MiB |
| Additional free bitmap | 0.125 | 1 MiB |

For a 64-way bitmap hierarchy, upper summary levels add 16,648 bytes per TiB.
At 100 TiB the dense map is 3.125 GiB, so compression can matter at large capacity.
The density denominator is configured extents, including currently free ones.

Let `p` be the fraction with raw count >=3, `N` the configured extent count, `s`
the hash-slot bytes, and `a` the target load factor:

```
two-bit plus overflow bytes ~= N/4 + s*p*N/a
byte plus overflow bytes    ~= N   + s*q*N/a
```

Here `q` is the fraction with raw count >=255 if byte value 255 is the escape
marker. With illustrative full-identity 16-byte slots at 70% occupancy:

| Raw count >=3 fraction | Two-bit map plus overflow per TiB |
|---|---:|
| 0% | 2.00 MiB |
| 0.1% | 2.18 MiB |
| 1% | 3.83 MiB |
| 5% | 11.14 MiB |
| 20% | 38.57 MiB |

The two-bit design beats dense32 below approximately **16.4%**, and beats a byte
map with negligible byte overflow below approximately **3.28%**. These are size
break-even points under stated assumptions, not observed sharing or performance.
A 32-bit extent key would silently cap the representation at 512 TiB with this
extent size; retain full 64-bit identities unless such a limit is intentional.

Free-space indexing is a separate choice from ownership storage:

| Candidate | Benefit | Cost or limiting case |
|---|---|---|
| Free-ID list/vector | Cheap extraction | 8 bytes/free extent for 64-bit IDs, plus ownership state; concurrent head needs locking or ABA-safe reuse |
| Free ranges | Compact large contiguous holes | Split/coalesce/index synchronization; alternating free/used extents make about N/2 ranges, or 8N bytes even with flat 16-byte records |
| Hierarchical free bitmap | Skips occupied regions with compact summaries | Extra atomic publications and possible stale summaries; keep authoritative claim CAS and no false out-of-space |
| Byte counts plus overflow | 75% base saving; common counters update independently | Overflow lifecycle and more cache-line sharing than dense32 |
| Two-bit states plus overflow | 93.75% base saving before overflow | Packed-word CAS, common 2-to-3 promotions, hash accesses, shared entry lifetime |

The current allocation API asks for one extent, so contiguous-range machinery
has limited immediate value. Storing free-list pointers inside free disk extents
also creates I/O and persistence-ordering requirements; it is not a free RAM list.
A bitmap should initially be a derived, rebuildable index with an authoritative
scan before declaring out-of-space if concurrent hint publication can be missed.
Race tests must include claim versus free, summary clearing versus free-bit set,
full-to-nonfull transition, and freeing the last available extent.

Two-bit compression makes 32 extents share one 64-bit CAS target. Its smaller
working set may help cache behavior, but that does not establish a net CPU or
throughput win. A byte representation is an important comparison point.

## Requirements for a real sparse-count implementation

1. Make entry promotion, tag publication, reads, demotion, deletion, resize, and
   extent reuse one defined synchronization protocol. Initialize the full entry
   before publishing an escape tag; readers must not follow an entry freed by
   a concurrent demotion. Per-shard locks are a simple initial baseline, with
   measurable contention; an optimized lock-free scheme needs its own proof.
2. Resolve allocation failure. `allocator_inc_ref()` currently returns only a
   count and cannot fail (`src/allocator.h:247`). Creating an overflow entry
   cannot silently introduce an ordinary OOM return. Budgeted reserves, a
   preallocated fallback, or an API change must have explicit worst-case costs.
3. Retain full identity validation and the existing 32-bit count range. A hash
   fingerprint is only a candidate. Do not accumulate permanent escape entries
   for every extent that was ever shared.
4. Preserve recovery's first-visit and repeated-reference semantics, reserved
   metadata accounting, and the non-reusable retiring state.
5. Give page-access validation a direct nonzero-state predicate if necessary:
   `src/allocator.h:364` currently asks for an exact count just to check allocated
   state. Avoid forcing every such cache/blob check through the overflow hash.
6. Separate RAM representation from disk format initially. A compact RAM map
   can stream-expand to the existing 32-bit durable map and reconstruct on load.
   That isolates runtime experiments but does not reduce durable map bytes.

## Why a live-array checkpoint is not recovery state

Current clean shutdown quiesces work, publishes the final root, drops logs and
runtime context references, writes the map, makes it durable, and only then
publishes a nonzero `allocation_state_addr` (`src/core.c:4655`,
`src/rc_allocator.c:483`). The shutdown explicitly releases the context's extra
root reference before persisting counts.

During normal operation the live map also includes reader/snapshot pins,
unpublished compaction output, mutable memtables, spare mini-allocator extents,
and conservatively retained objects after ambiguous I/O errors. A coherent
instantaneous copy preserves those allocations after a crash even though their
owners have disappeared. This can leak both extents and excess references.
A fuzzy page-by-page copy can additionally miss newly reachable extents.
Journaling every live increment/decrement reproduces the ownership mismatch.

Today `allocation_state_addr` is specifically a **clean-shutdown** marker:
nonzero requires empty log slots (`src/superblock.c:83`), and mount then skips
recovery (`src/core.c:3770`). A concurrent recovery checkpoint needs a distinct
descriptor and format semantics, not merely setting this field while logs exist.

Dirty recovery currently performs:

1. Tree reconstruction plus discovery of all required log/blob extents, before
   replay can allocate and overwrite anything it has not read yet
   (`src/core.c:3375`).
2. Replay, incorporation, and publication of a recovered root with no logs.
3. Context teardown and cache eviction, then a second, root-only reconstruction
   to reclaim log/private/incomplete allocations (`src/core.c:3838`).

Tree reconstruction walks trunk nodes and mini-allocator extent metadata,
not every branch payload page (`src/btree.c:1825`). Log discovery also walks log
pages and blob pointer trees. The cost must be measured as these phases, not
estimated as a sequential read of the entire database.

## Feasible concurrent checkpoint designs

**First experiment: an exact root-only map.** Capture and pin an immutable tree
root and its incorporation generation. Traverse that graph into a separate
counting/visited destination while foreground updates continue. Existing walkers
need an explicit destination sink: they currently obtain the live allocator via
the cache and clear/use its map as their visited set. Calling them unchanged in
the background would corrupt live accounting.

Write a checksummed map to an inactive reserved A/B map bank, then issue the
required durability barrier. Under the publication lock, attach its descriptor
only to the same root/generation and a valid recovery-log configuration. Reject
stale attempts if the required recovery state has changed. The old bank remains
intact until the newer publication is confirmed. Reserve both banks in every map
to avoid recursively allocating the map through itself. An ambiguous publication
failure must retain all possibly published maps and their reachable objects until
a later confirmed state resolves them.

On recovery, load the root-only map and discover/protect the complete required
log/blob extent set before allowing replay allocations. Retain the final
root-only rebuild initially. This experiment therefore removes **only the first
tree reconstruction**, not log discovery, replay, or the second tree walk.

There is a substantial progress tradeoff: a map attached to an old root becomes
stale when a newer root is published. Pinning the old root alone does not retain
enough log history. The present two-log protocol retires its sealed log after
root publication (`src/core.c:790`). Keeping an older recovery base would require
retaining its matching root and all intervening replay history. A background
attempt must either be opportunistic and discard stale work, or serialize root
publishers during the necessary interval. The latter delays checkpoint/log
reclamation even if inserts remain concurrent. Bound retained space, outstanding
attempts, and delay; do not claim starvation-free progress without that contract.

**Incremental design: a versioned durable-ownership array.** An array is quite
compatible with concurrent snapshots. Partition it into chunks, freeze an epoch,
and let writers COW each shared chunk on its first update. A versioned directory
makes the checkpoint's chunk addresses stable. New chunks, directory, and data
must become durable before one superblock publication selects their matched
root/log/map generation. The packed tags and overflow table, if present, must
participate in the same generation and publication transaction.

COW alone is insufficient: introduce a separately maintained durable ownership
view. Apply committed object-graph additions/removals when publishing roots and
logs; transient reader pins stay in runtime accounting. Root changes spanning
multiple chunks need a single publication transaction. A delta journal alternative
needs committed structural operations, incarnation-qualified extent reuse,
idempotent redo, cancellation/cleanup of private allocations, and ordering
against data durability and log cuts. Current mutation events do not provide
this contract automatically. Generating exact durable deltas is an independent
implementation project, not a consequence of packing the counters.

A 4 KiB chunk covers 1,024 dense32 counts (128 MiB of device capacity) or 16,384
two-bit states (2 GiB). Sparse random updates can dirty most chunks even when
only a minority of counters change. The byte savings therefore do not directly
predict checkpoint write savings. Account for directory pages, checksums,
overflow chunks, first-write COW CPU, retained generations, and fallback storage.
Use a bounded snapshot budget and a defined retry/fallback policy.

A conservative used-space bitmap may keep startup allocation safe, but leaves
unknown multiplicities and unreachable allocations. It needs eventual exact
count reconstruction or separate durable reclamation metadata; it cannot by
itself replace this allocator's ownership contract. Copying a conservative
superset also still requires accounting for allocations after its cut.

## Decision gates and measurements

The immediate recommendation is to keep current clean-close persistence and
recovery fallback. Choose a production change only after identifying a cost:

- **Allocation latency:** count scan lengths/CAS retries and allocation latency
  near full capacity, with fragmented holes and concurrent free/allocate. Compare
  current scanner against the additional bitmap, including bitmap maintenance.
- **Memory:** census raw counts 0, 1, 2, 3..254, >=255; maximum count; 2-to-3 and
  3-to-2 transitions; hash occupancy and peak memory. Sample ingestion,
  compaction, snapshots, and blob cloning. A clean-close image misses transient
  sharing, and its count map has no per-page-type labels.
- **Recovery:** separately measure first tree walk, log/blob discovery, replay,
  eviction, and second tree walk; count metadata I/O and bytes. Compare saved
  restart time with added checkpoint CPU/I/O, foreground throughput and tails,
  stale attempt rate, and retained space. Recovery-time objectives can justify
  overhead even if failures are infrequent, but the budget must be explicit.
- **Implementation checks:** concurrent neighboring packed updates, overflow
  promotion/demotion/read/reuse and OOM; dirty and clean reopen; checkpoint
  failure at every dependency/publication boundary; torn maps; invalid pointers;
  malformed overflow records; and existing low-space/reclamation paths.

All performance runs must use the campaign coordination protocol: announce a
unique benchmark request, obtain paused acknowledgements from every active
project and all their children, then hold the shared campaign's benchmark and
machine locks exclusively. Ordinary work holds them shared. The models below
are bounded correctness checks, not timed performance runs.

## Reproducible artifacts and validation

```
python3 benchmarks/allocation_table/sizing.py
python3 benchmarks/allocation_table/sizing.py --capacity-tib 100 --escape-fraction .05
python3 benchmarks/allocation_table/model.py
```

`sizing.py` calculates the stated capacities and break-even points. Its default
1% escape example needs 4,014,576 bytes for two-bit states plus idealized hash
slots versus 33,554,432 bytes for dense32. These are formulas, not resident-memory
measurements.

`model.py` checks 80,000 sequential operations against a dense reference array,
including 3,528 reuses, packed-word boundaries, reserved extents, promotion and
demotion, and maximum counts. It demonstrates the fuzzy-copy and temporary-pin
counterexamples. Its abstract immutable-map protocol exercises nine crash
prefixes, torn manifest fallback, shared chunks, mismatched-map rejection,
publication before data durability, rejection of premature overwrite, and successful
reuse after newer publication and reader release.

The positive checkpoint model is supplied an exact durable ownership graph as
an **oracle**. It does not derive it from live counters, model log discovery or
real cache/writeback, or test concurrent COW. Atomic durable objects and invalid
manifests abstract real storage failures. The model retains old graph storage until newer publication and reader release;
the newest valid manifest is then selected. It models candidate tearing before
publication, not later corruption of already-confirmed durable state or arbitrary
historical rollback. Real A/B slot reuse and ambiguous I/O acknowledgement need
implementation-specific tests. Passing this model supports the stated abstract
ordering argument only.
