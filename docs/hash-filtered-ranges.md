# Hash-filtered range queries

`splinterdb_iterator_init_with_hash()` and
`splinterdb_iterator_init_with_bounds_and_hash()` accept an optional hash key.
The iterator returns only keys with the same full 32-bit `data_config.key_hash`
value as that key, using the configured routing-filter seed. `NULL_SLICE`
disables filtering. The hash key is consumed during initialization; its storage
does not have to outlive that call.

Bounds, inclusive/exclusive starts, forward/reverse traversal, updates, deletes,
and errors follow the ordinary iterator contract. Filtering does not provide
additional snapshot isolation. All active memtables participate. The routing
filters select immutable candidate branches, then each source iterator discards
nonmatching keys **before merging messages**. That order matters: a skipped
branch can hold part of a nonmatching key's update history. Routing-filter
fingerprint collisions therefore cannot expose an incomplete history to an
application's merge callback.

The existing API and on-disk format are unchanged. No adapter allocation or
per-key hash is added to ordinary unfiltered scans. Empty trunk spans are
crossed iteratively to avoid stack growth when a hash is sparse or absent.

When integrating the separate persisted-configuration project, a routing filter
built with a different seed must be treated as unpruned. Exact matching is
defined using the query seed; rehashing the hash key using an old filter's seed
does not preserve collisions between distinct keys under the query seed and
could incorrectly skip a matching key. Source filtering still uses the query
seed. This independent branch retains the existing single-seed format.

## Validation

Based on `db3047a8`. All 49 debug `splinterdb_quick_test` cases and all 31
`splinter_test` core cases pass, including:

- Deliberate routing-fingerprint collisions between different full hashes;
  a merge callback rejects any nonmatching key it sees.
- Updates split between an immutable branch and the active memtable, deletes,
  close/reopen, exclusive bounds, reversals, and `NULL_SLICE` opt-out.
- A multi-level trunk with only one matching hash per 2,048 keys; both
  directions cross empty leaves, and a hash absent from every branch terminates.

Reproduce from the worktree after building the debug unit test:

```sh
build/debug/bin/unit/splinterdb_quick_test
build/debug/bin/unit/splinter_test --db-location /REAL_DISK/filtered-core.db \
  --cache-capacity-mib 128 --db-capacity-gib 4
```

## Performance reproduction

`benchmarks/hash_range_benchmark.c` compares application-side filtering on the
base library with branch selection and source filtering on this library.
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
`DB_PATH 16384 2000`: 32 tenants, 16-byte keys, 64-byte values, a
64 MiB cache, 4 MiB memtables, and range lengths 1/2/10/100. The first key field
sorts by time and the second identifies the tenant; the custom hash uses only
the tenant. Tenant-grouped ingestion preserves selective immutable branches,
and scans run before close. Every returned key and value is checked. CSV
includes elapsed time, visited keys, and a checksum shared by both versions.

Use a database path on a real disk and one exclusive benchmark lock across
all project worktrees. The campaign also compares the unchanged public API
using the same pinned `kvbench.c` source and build flags on both branches.

The campaign uses baseline commit `db3047a812d9b193b87f92fc0be7b75e923b89ee`.
The shared public-API harness is that commit's `benchmarks/kvbench.c` (SHA-256
`45cf9d2e1f74e90719885955d5a78651cc5c1d97cda2daed534a3c0cb07f48a9`), statically
linked against each release archive. These public-API runs disable logging
(`--log 0`) and use identical flags, including `-O3`,
`-flto`, and `-march=native`. Full commands and binary hashes are in each run's
`manifest.json`; `results.csv` retains latency, CPU, I/O, and correctness fields.
The campaign root is `.cache/projects-20261011` in the original checkout.

## Existing API, resident results

The unfiltered API used one identical 200,000-key image, a 128 MiB cache,
24-byte keys, 100-byte values, five seconds per phase, and ABBA order (two
samples per version). Median throughput changes were:

| Operation | 1 thread | 8 threads |
| --- | ---: | ---: |
| Point query (control) | +2.01% | +1.91% |
| Range 1 | +1.43% | -2.70% |
| Range 10,000 | +0.56% | +0.26% |

All correctness counters were zero. Long scans were neutral in this run;
the short eight-thread scan showed a small loss. The sample count is too small
to claim that every workload has zero overhead. Raw results are under
`.cache/projects-20261011/benchmarks/filtered-ranges/resident-log0-fixed-image/`.
