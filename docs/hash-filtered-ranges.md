# Predicate-filtered range queries

The predicate API integrates with persisted routing-filter configuration.

`splinterdb_iterator_init()` accepts optional designated-initializer
parameters (see `splinterdb_iterator_init_options`). The filter parameters are
`.predicate` (a `splinterdb_key_predicate`), `.predicate_context` (a `void *`),
and `.routing_filter_key` (a `slice`); they combine freely with the `.lower_limit`,
`.upper_limit` and `.start_position` parameters.
There is no new `data_config` callback.

| Predicate | Routing-filter key | Behavior |
| --- | --- | --- |
| NULL | NULL_SLICE | Ordinary iterator |
| Supplied | NULL_SLICE | Exact key predicate, with all relevant branches searched |
| Supplied | Supplied | Exact key predicate with routing-filter branch pruning |
| NULL | Supplied | EINVAL |

The predicate receives `(context, stored_key)` and returns a boolean. It may
run repeatedly, including on keys with delete messages. It must remain stable
for the iterator lifetime, agree for comparator-equivalent keys, and must not
mutate the database. Its context remains caller-owned and must outlive the
iterator, including use after thread handoff. Key slices passed to the callback
are borrowed only for that invocation. Bounds, inclusive/exclusive starts,
forward/reverse movement, errors and isolation follow the ordinary iterator.

A supplied routing-filter key asserts this application contract for EVERY seed:

```
predicate(key) => key_hash(key, seed) == key_hash(routing_filter_key, seed)
```

The routing key uses the query-key representation accepted by `key_hash`.
Initialization copies its bytes; the caller may immediately reuse its original
buffer. Violating hash compatibility can omit matching keys or portions of
their update histories. Predicates spanning different hash classes must omit
the routing key. The database's hash definition must remain compatible with
existing filters across reopen; changing a seed does not change membership.

For example, keys sorted by `(time, tenant)` can use a predicate comparing the
tenant, and a hash function hashing only the tenant. The query supplies time
bounds and a representative key for the desired tenant. Additional predicate
conditions are allowed if every accepted key still has that tenant hash.

The merge iterator rejects nonmatching keys BEFORE merging or finalizing
messages. Routing-filter collisions can select unrelated keys with incomplete
update histories, so this ordering is required. The predicate runs at the
ordered merge front; sources are not eagerly advanced to their next match.
All active memtables participate. Ordinary scans retain their ordinary merge
operations without predicate calls or filter-state allocation.

The iterator's private state retains the copied routing key across trunk spans,
seeks and reversals. The routing-filter module validates each persisted maplet
descriptor and hashes the routing key with that maplet's saved seed, using the
saved encoding parameters to read the filter. New-filter construction defaults
do not change how existing maplets are queried. A routing-filter query caches
four seed/hash pairs; more seeds merely replace cached entries and cause
rehashing. Hash selection and descriptor validation belong to the routing-filter
module.
Under the compatibility contract above, different saved seeds do not require
disabling branch pruning. Exact predicate checks do not hash candidates under
all encountered seeds.

The independent lazy-merge experiment also changes `merge.c`. Integration must
preserve predicate evaluation before message merging and keep the filtered
iterator operations when restoring deferred ordering.

## Combined implementation

The current implementation incorporates the earlier mixed-seed integration.
The public API and its hash-compatibility contract are unchanged. For the
interaction with saved configuration and branch versions, see
[predicate filtering with saved configuration](predicate-filter-format-integration.md).
A concise API summary is in
[the filtered-ranges PR notes](filtered-ranges-pr.md).

## Historical single-hash benchmark protocol

These build instructions describe the earlier single-hash harness and API.
Use the matching historical harness when reproducing that design.

`benchmarks/hash_range_benchmark.c` compares application-side filtering on the
base library with branch selection and exact pre-merge filtering on this library.
Compile the identical source against each static archive; define
`HASH_RANGE_FILTER_API` only for this branch:

```sh
cc -D_GNU_SOURCE -std=gnu11 -O3 -g -pthread -ffast-math -flto \
  -DSPLINTERDB_PLATFORM_DIR=platform_linux -DHASH_RANGE_FILTER_API \
  -I include benchmarks/hash_range_benchmark.c \
  build/release/lib/libsplinterdb.a -laio -lxxhash -lm \
  -o build/release/bin/hash_range_benchmark
```

The database is explicitly opened with `O_DIRECT`. Run with
`DB_PATH 16384 100000`: 32 tenants, 16-byte keys, 64-byte values, a
64 MiB cache, 4 MiB memtables, and range lengths 1/2/10/100. The first key field
sorts by time and the second identifies the tenant; the custom hash uses only
the tenant. Tenant-grouped ingestion preserves selective immutable branches,
and scans run before close. Every returned key and value is checked. CSV
includes elapsed time, visited keys, and a checksum shared by both versions.

Use a database path on a real disk and one exclusive benchmark lock across
all project worktrees. The campaign also compares the unchanged public API
using the same pinned `kvbench.c` source and build flags on both branches.
