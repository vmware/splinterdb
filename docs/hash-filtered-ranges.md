# Predicate-filtered range queries

The current `robj/filtered-ranges` PR candidate targets main `1d6ea2a3`, which
includes disk-format (#689). It adds the predicate API and the mixed-seed
integration from `36183807` on top of main's persisted configuration. The main
update leaves the previously tested implementation unchanged; see the
[main-update proof](../benchmarks/results-20261013/filtered-main-1d6ea2a3/README.txt).

`splinterdb_iterator_init()` accepts optional designated-initializer
parameters (see `splinterdb_iterator_init_options`). The filter parameters are
`.predicate` (a `splinterdb_key_predicate`), `.predicate_context` (a `void *`),
and `.routing_filter_key` (a `slice`); they combine freely with the `.min_key`,
`.min_key_comparison`, `.max_key` and `.max_key_comparison` bounds parameters.
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
The current PR description and validation status are in
[the filtered-ranges PR notes](filtered-ranges-pr.md).

The measurements below describe earlier source snapshots. They do not establish
the performance of the combined PR candidate.

## Historical standalone predicate validation and performance

The standalone predicate branch included main `0fa3bd6c` (async durable
barriers). Its measured implementation was `cf7dc4f7`; subsequent changes before
the disk-format integration recorded evidence and documentation.
Debug and release each passed 70 unit cases and four functional filter cases.
Fifteen focused ASan cases passed before this main merge. Exact commands, hashes,
initial fixture/invocation failures and corrected passing runs are in
[the predicate validation archive](../benchmarks/results-20261013/predicate-pr-0fa3bd6c/README.txt).
The CI formatter and full PR whitespace checks passed.

The earlier companion `robj/filtered-ranges-disk-format` at `36183807` validated
the interaction with persisted per-filter seeds. Debug and release each passed
125 unit cases and the full routing-filter suite. Tests cover six seeds and
hash-cache eviction, reopen with saved seeds 11/22 while the construction default is 99,
noncommutative updates, one-seed collisions, bounds and reverse traversal.
Another saved seed is first encountered after the caller overwrites its routing
key buffer; the callback verifies that the iterator retained the original bytes.
That snapshot supplied correctness evidence, not a combined performance measurement.

The standalone measurements used O_DIRECT files on ext4 on the real disk,
serialized with all other benchmarks, builds and tests. Four fixtures plus
28 comparison processes passed verification across the two independent feature branches.
Copied fixtures were explicitly fsynced and checked; source/library/executable
hashes and all raw samples are retained. Ordinary scans use main / predicate /
disk-format / disk-format / predicate / main order with two samples per profile.
The predicate and main readers use copies of the same fixture.

| Ordinary API throughput change | Resident | Disk-backed working set |
| --- | ---: | ---: |
| Point query, 1 reader | -2.53% | Not measured |
| Point query, 8 readers | +0.69% | -0.53% |
| Range length 1, 8 readers | -0.18% | -0.30% |
| Range length 10,000, 8 readers | -0.74% | -1.50% |

Every predicate-branch sample range overlaps main. This screening does not prove
performance neutrality; these are observed ranges, not confidence intervals.
Resident measured physical reads were zero. Disk samples include device variation.

The feature workload stores 32 tenants in keys sorted by `(rank, tenant)`, with
tenant-grouped ingestion preserving selective branches. Each limit uses 500
untimed warmup queries and 10,000 measured queries. The same exact predicate runs
in the application for main and inside the candidate iterator. A second fixture
forces tenant pairs to collide in their full 32-bit hash under every seed.
Mean latency changes relative to application filtering:

| Returned matches | Predicate only | Predicate + pruning | Predicate only, collisions | Pruning, collisions |
| --- | ---: | ---: | ---: | ---: |
| 1 | -3.69% | -16.30% | -7.54% | -14.19% |
| 2 | -8.42% | -22.94% | -16.22% | -28.31% |
| 10 | -13.19% | -30.83% | -14.76% | -30.21% |
| 100 | -14.22% | -32.09% | -13.03% | -30.68% |

All query assertions and independently recomputed checksums passed. These gains
apply to this selective tenant workload; they do not predict arbitrary predicates,
branch layouts or cold-query latency. No fresh write-throughput matrix was run.
Full sample ranges, CPU/I/O, callback counts, control drift and reproduction
commands are in [the standalone predicate benchmark report](../benchmarks/results-20261013/predicate-disk-main-0fa3bd6c/report.txt).

The following earlier validation and measurements describe the superseded
single-hash API. New predicate implementation results are recorded separately.

## Historical single-hash validation

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

## Historical single-hash performance reproduction

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

## Historical single-hash final implementation: existing API

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

## Historical single-hash final implementation: matching-hash queries

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
[`benchmarks/results-20261011`](../benchmarks/results-20261011/filtered-ranges-README.md).
Unsuffixed directories are the superseded v1 implementation; `v2` directories
measure ordered merge selection before the layout repair; `v3` directories
measure the final implementation. Database images and verbose logs remain in
the original campaign directory and are not committed.

## Historical single-hash main integration (2026-10-13)

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

## Historical single-hash benchmarks after main integration (2026-10-13)

The exact-hash probe matched main result checksums. Query latency changed -5.70%/-16.71%/+0.56%/-9.59% for limits 1/2/10/100. Ordinary eight-thread short-range throughput changed -3.38% resident and -1.67% on disk, with sample ranges overlapping main.

These are updated-feature comparisons against main `7bb5fab`, not an isolated
measurement of merge cost. Main controls drifted; observed range overlap is
descriptive, not a statistical confidence interval. Exact samples, build/source
hashes, commands, verification scope and fresh correctness evidence are in
[the integration archive](../benchmarks/results-20261013/filtered-ranges-main-7bb5fab/README.md).

## Historical initial predicate API validation (2026-10-13)

The single-hash initializer has been replaced by the predicate/context/routing-key
API described above. Debug and release each pass 69 unit cases (54 public quick,
10 merge, one core cache-read-error and four blob cases) and the four functional
routing-filter cases. Focused ASan passes all ten merge cases and five predicate
API cases, with `detect_leaks=0:abort_on_error=1`. Coverage includes deliberate
full-width hash collisions, exclusion before user merge/finalize callbacks,
predicate-only scans, bounds, reversals, empty results, invalid combinations,
owned routing keys across trunk leaves, accepted-key error propagation and
iterator/context transfer to another thread. One initial test fixture omitted
its required update callback; corrected runs pass and both logs are retained.

Exact commands, logs, source and binary hashes are in the campaign's
`coordination/filter-predicate-20261013/tests/` directory. These checks precede
the latest-main merge and the separate mixed-seed integration validation.
