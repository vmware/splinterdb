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
filters select immutable candidate branches, then the merge iterator discards
nonmatching keys **before merging messages**. That order matters: a skipped
branch can hold part of a nonmatching key's update history. Routing-filter
fingerprint collisions therefore cannot expose an incomplete history to an
application's merge callback.

The predicate runs when a source reaches the front of the ordered merge. It
does not eagerly advance every source to its next matching key: that could
scan an unrelated source's entire tail before returning an earlier result.
Rejected copies advance individually without invoking application callbacks.

The existing API and on-disk format are unchanged. No per-source adapter is
allocated. Ordinary unfiltered scans use separate iterator operations that skip
predicate and hash calls. The filter callback lives in a derived merge iterator,
so an ordinary merge iterator retains its original size and field offsets. The
new range-iterator fields follow the existing fields. These layout choices do
not make the generated machine code identical to the baseline. Empty trunk
spans are crossed iteratively to avoid stack growth when a hash is sparse or
absent.

When integrating the separate persisted-configuration project, a routing filter
built with a different seed must be treated as unpruned. Exact matching is
defined using the query seed; rehashing the hash key using an old filter's seed
does not preserve collisions between distinct keys under the query seed and
could incorrectly skip a matching key. Exact filtering still uses the query
seed. This independent branch retains the existing single-seed format.

The independent lazy-merge experiment also changes `merge.c`. When combining
the branches, its deferred initial ordering must preserve this predicate's
position before message merging and retain the filtered movement operations.
Its initial operations must also preserve identification of the derived filter
object; replacing its operations with the lazy iterator's initial operations
without that distinction would lose the predicate.

## Validation

Based on `db3047a8`. The final implementation (`f30ebdf`) passes all 9 debug
merge tests and 49 `splinterdb_quick_test` cases. The preceding ordered-merge
implementation (`9062730`) also passed 31 `splinter_test` core cases; those core
cases were not rerun after the final layout change. Coverage includes:

- Deliberate routing-fingerprint collisions between different full hashes;
  a merge callback rejects any nonmatching key it sees.
- Updates split between an immutable branch and the active memtable, deletes,
  close/reopen, exclusive bounds, reversals, and `NULL_SLICE` opt-out.
- A multi-level trunk with only one matching hash per 2,048 keys; both
  directions cross empty leaves, and a hash absent from every branch terminates.
- A merge regression checks that a nonmatching source advances only as far as
  the first result requires, that its update finalizer is never called, and
  that reversals across both bounds preserve those properties.

Reproduce from the worktree after building the debug unit test:

```sh
build/debug/bin/unit/splinterdb_quick_test
build/debug/bin/unit/merge_test
build/debug/bin/unit/splinter_test --db-location /REAL_DISK/filtered-core.db \
  --cache-capacity-mib 128 --db-capacity-gib 4
```

## Performance reproduction

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

The campaign uses baseline commit `db3047a812d9b193b87f92fc0be7b75e923b89ee`.
The shared public-API harness is that commit's `benchmarks/kvbench.c` (SHA-256
`45cf9d2e1f74e90719885955d5a78651cc5c1d97cda2daed534a3c0cb07f48a9`), statically
linked against each release archive. These public-API runs disable logging
(`--log 0`) and use identical flags, including `-O3`,
`-flto`, and `-march=native`. Full commands and binary hashes are in each run's
`manifest.json`; `results.csv` retains latency, CPU, I/O, and correctness fields.
The campaign root is `.cache/projects-20261011` in the original checkout.
Databases were on ext4, backed by `/dev/mapper/snafu-root_crypt`. Builds and
tests were excluded from timing through the shared campaign lock.

## Final implementation: existing API

Commit `f30ebdf` restores the ordinary iterator layouts after the earlier
implementation showed a repeatable short-scan loss. The focused final check
uses one identical 200,000-key image, a 128 MiB cache, 24-byte keys, 100-byte
values, ten seconds per phase, eight query threads, and ABBA order (two samples
per version). Throughput is queries per second:

| Operation | Baseline samples | Candidate samples | Median change |
| --- | ---: | ---: | ---: |
| Point query (control) | 4,740,299 / 4,809,014 | 5,133,688 / 5,270,101 | +8.95% |
| Range 1 | 492,836 / 479,998 | 466,527 / 481,521 | -2.55% |

Every correctness counter was zero. The short-scan loss is smaller than the
preceding implementation's repeated 7.77% loss. The final ranges overlap, and
the point control moves in the opposite direction. Two samples per version
cannot establish performance neutrality: a residual short-scan cost remains
possible. The final layout revision was tested with this focused resident
check, not a new complete disk and long-scan matrix.

## Final implementation: matching-hash queries

The final positive probe runs 100,000 queries per range length in each ABBA
sample, with the tenant-grouped fixture described above. All samples validate
every matching key and value and produce identical checksums. Median mean
time per query:

| Returned matches | Application filtering | Native filtering | Time change |
| --- | ---: | ---: | ---: |
| 1 | 6.346 us | 4.946 us | -22.06% |
| 2 | 6.784 us | 5.502 us | -18.89% |
| 10 | 18.528 us | 15.094 us | -18.54% |
| 100 | 138.406 us | 119.890 us | -13.38% |

This is a benefit on a selective tenant-hash workload, not a guarantee for
arbitrary hash functions or branch layouts. The long sweep amortizes initial
cache warming; it does not establish that a cold one-result query is faster.
The exact predicate also protects application callbacks when routing filters
produce false-positive candidates.

## Earlier implementations and measurements

The original per-source adapter design (`6ff014b`, v1) failed the positive
hash-filtered probe: it took 21.2x, 23.0x, 17.9x, and 2.94x the
application-filtering time for range lengths 1, 2, 10, and 100. A candidate
branch could contain matching hashes only before the requested start key,
causing eager filtering to exhaust its entire unrelated tail. Ordered merge
selection repairs that behavior; the focused regression above captures it.

The ordered-merge implementation (`9062730`, v2) passed the 100,000-query
positive probe, reducing query time by 18.83%, 16.42%, 16.29%, and 13.08%.
Its shorter 2,000-query probe had a noisy first phase: range1 averaged 36–47 us
for application filtering and 38–58 us for native filtering (+15.9% median
time). Later lengths improved 6.9–9.6%.

V2 was also measured without hash filtering, using an identical image for
every sample within each regime. Resident means 200,000 keys and a 128 MiB
cache; disk means 2,000,000 keys and a 64 MiB cache. Both use 24-byte keys,
100-byte values, five-second phases, O_DIRECT, disabled logging, and ABBA
order with two samples per version. Median throughput changes:

| Operation | Resident 1T | Resident 8T | Disk 1T | Disk 8T |
| --- | ---: | ---: | ---: | ---: |
| Point query (control) | -0.47% | -1.64% | -1.31% | -3.48% |
| Range 1 | -1.57% | -5.62% | -1.78% | -2.06% |
| Range 10,000 | +0.26% | +2.47% | +1.20% | -1.28% |

Every correctness counter was zero. Resident baseline range1/8T fell from
549.6k to 516.7k queries/sec, while the candidate returned 502.3k and 504.1k.
A focused ten-second repeat still lost 7.77% for range1/8T (the point control
lost 2.31%), motivating the final layout repair. These earlier measurements
must not be presented as measurements of that repair.

The copied-image runner used buffered `cp` without an explicit `fsync` before
O_DIRECT warming and timing. Pending copy writeback might therefore overlap
the historical disk opt-out runs; this is a method limitation, not a proven
explanation of their results. No disk speedup is claimed. The final resident
check and the native hash probe, which creates its own O_DIRECT database, are
the main final evidence. Future copied-image runs use an explicit sync.

Compact raw samples, exact commands, fixture and binary hashes, build flags,
and machine/filesystem provenance are retained in
[`benchmarks/results-20261011`](../benchmarks/results-20261011/README.md).
Unsuffixed directories are the superseded v1 implementation; `v2` directories
measure ordered merge selection before the layout repair; `v3` directories
measure the final implementation. Database images and verbose logs remain in
the original campaign directory and are not committed.

## Main integration (2026-10-13)

Merged main `7bb5fab`, preserving both the hash-filtered API and main's compact
transferable cache handles and multilevel blobs. The quick-test append conflict
was resolved by retaining the filtered-range tests and the new handoff/blob
recovery tests. Selected branches retain complete update chains; unmatched
keys are still excluded before user merge callbacks.

Debug and release each pass 65 cases: nine merge tests, 51 public API tests,
the core cache-error test and four blob tests. Both routing-filter functional
runs pass. An initial core invocation had its test-name filter before the
configuration arguments and was rejected; corrected invocations passed.
Commands, logs and hashes are in
`.cache/projects-20261011/coordination/main-7bb5fab/filtered-ranges/`.
The measurements above remain historical. Fresh integration measurements are
tracked separately under `benchmarks/main-7bb5fab/` in that campaign.

## Fresh benchmarks after main integration (2026-10-13)

The exact-hash probe matched main result checksums. Query latency changed -5.70%/-16.71%/+0.56%/-9.59% for limits 1/2/10/100. Ordinary eight-thread short-range throughput changed -3.38% resident and -1.67% on disk, with sample ranges overlapping main.

These are updated-feature comparisons against main `7bb5fab`, not an isolated
measurement of merge cost. Main controls drifted; observed range overlap is
descriptive, not a statistical confidence interval. Exact samples, build/source
hashes, commands, verification scope and fresh correctness evidence are in
[the integration archive](../benchmarks/results-20261013/main-7bb5fab/README.md).
