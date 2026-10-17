# Resident cache write probe

Status: source prepared and statically checked; no build or workload run yet.
This is a separate probe. The frozen read probes and production sources are unchanged.

Each completed operation gets one resident `PAGE_TYPE_MEMTABLE` page, claims it,
upgrades to a write lock, increments a protected counter and its complement,
then unlocks, unclaims, and releases the read handle. `cache_lock` marks the page
dirty through `clockcache_dirty_begin`; there is no separate dirty call. A failed
claim releases the read handle before pausing and retrying the same address.
Failed claims are counted separately from successful writes.

`disjoint` partitions one fixed total set of pages by `index % threads`; each
worker chooses randomly inside its partition. `shared` lets every worker choose
randomly from that same total set. Thus the 32-page, eight-worker case uses four
pages per worker in disjoint mode and shared contention across all 32 pages in
shared mode. This does not force collisions on every operation or emulate B-tree
key skew. The partial-partition calculation also supports page counts that are
not divisible by the worker count.
Cross-mode throughput is not a pure contention contrast: the per-worker working
set also differs. Compare library profiles within each mode.

The runner defaults to 32 pages, a 64 MiB cache, the frozen spread-address setup,
and main/original/async-mask. The C probe retains the default 75% resident set when
the working-pages environment override is absent, but the runner always records
an explicit override. All allocations, disk initialization, and cache preloading
finish before worker warmup. Each process runs exactly one scenario.

Workers register sequentially before starting, preserving their assigned cache
reference stripes. The CPU maps, phase-scoped hardware counters, user CPU time,
zero-I/O/major-fault guard, scenario-major scheduling, excluded preconditioning,
rotating mirrored blocks, process telemetry, machine lock, and hash provenance
are inherited from the controlled read probe. Eight-worker cases default to no
hardware counters because the chosen CPU set includes SMT siblings.

Mutable counter words are read only under the write lock while workers run.
Immutable identity is checked under the read handle. After every worker joins
and after the final I/O/CPU snapshot, verification checks every page identity and
counter complement and compares the sum of counters to every successful worker
update, including warmup and stopping batches. Disjoint mode also checks each
partition's sum against its owning worker. Dirty-page writeback, completion, and
eviction follow verification and output; none is part of the measured interval.

CSV retains the read probe's common columns (`async_waits` is always zero) and
adds `claim_failures`, `all_claim_failures`, `all_ops`, `warmup_ops`,
`verified_updates`, and `dirty_pages`. Throughput counts successful updates, so
retry and blocking costs are included. The worker-local measured and all-phase
failure counts allow contention to be inspected rather than hidden.
`claim_failures` counts explicit failed `cache_try_claim` calls; waits and retries
inside blocking `cache_get` are included in time but are not separately counted.
`warmup_ops` includes writes before each worker observes phase 2. Pages are
ordinarily already dirty after warmup, so this measures repeated mutation of
resident dirty pages, not their initial transition from clean to dirty.

The working payload is one cache line per page, plus real cache metadata,
directory entries, and reference stripes. Page offsets can conflict in L1;
32 logical pages do not imply all touched data fits in L1. Address layout is
identical across library profiles. The probe isolates cache mutation ownership
and dirtying cost; it does not measure allocation, splits, tree traversal, WAL,
writeback pressure, compaction, or complete database insertion throughput.

`baseline/` preserves the exact controlled-final C, runner, object, executable,
build metadata, and build script. The setup-only exact working-pages override
comes from controlled-hotset. `provenance.json` records both parents and current
source hashes; the two patches show all changes relative to controlled-final.

Root should schedule the following through the campaign job runner under the
machine lock, with no overlapping build or benchmark:

```sh
python3 .cache/invalid-address-perf-20261017/harness/controlled-writes/build_probe.py \
  .cache/invalid-address-perf-20261017/sources/async-mask \
  .cache/invalid-address-perf-20261017/builds/async-mask/release/lib/libsplinterdb.so
```

The resulting single executable is dynamically reused across all profiles.
First smoke one and eight workers in both modes using one-second warmup/timing,
then run the bounded screen (72 measured processes plus four excluded ones):

```sh
python3 .cache/invalid-address-perf-20261017/harness/controlled-writes/run_controlled.py \
  mask-final-writes-hot32 --profiles main original async-mask --apis write \
  --threads 1 8 --modes disjoint shared --working-pages 32 \
  --blocks 3 --seconds 4 --warmup 2 --precondition-warmup 30
```

This suite can test whether the selected cache change causes a repeatable
resident write/claim regression. A neutral result cannot establish global
database insertion neutrality.
