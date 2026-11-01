# Blob-key prototype results

Blob-backed keys are feasible with the existing blob storage and B-tree
machinery. This prototype verified logical keys through 2 MiB, including
hierarchical descriptors, source-tree reclamation, repacking, and cold-cache
reads. It does **not** enable large keys through the public SplinterDB API.

The useful performance result is conditional: an inline logical prefix avoids
most blob comparisons when keys differ early. Long common prefixes still
require blob reads, and forcing already-small keys into blobs can lose the
substantial benefit of current branch prefix compression.

The subsequent [native format-3 implementation](../../docs/blob-keys.md) is
validated separately. These measurements do not describe its encoding or cost.

## Measurement

Measured implementation: local commit `da464513`, based on `6e550e1e`, branch
`robj/blob-keys`. Release binary SHA256:
`b6310c06fbc2d9ca1adeb3a9df227d6b2dd84238b4071c8cea533a65bce84036`.

There were 72 successful runs: six key lengths, early/late differences, two
representations or prefix lengths, and three repetitions. Variant order reverses
in the middle repetition. All runs verify results and reclamation. Single
foreground thread, Intel Core i7-12800H, ext4 on encrypted local block storage,
`O_DIRECT`, 4 KiB pages, 128 KiB extents, 64 MiB Splinter cache. Key/query
generation happens outside timers; queries are 50% hits and 50% misses. The
corpus occupies separate heap memory, so 64 MiB is a cache limit, not a total
process memory cap. Small-key and large-key cases use different record counts;
compare variants within the same row.

All three other active projects explicitly acknowledged that their roots and
children had stopped work. Two review-only projects were already complete.
Both machine and benchmark locks were held exclusively for the measurement
window, which lasted about 104 seconds. All blob-key child agents were idle.
This excludes registered agents' work, not arbitrary operating-system activity.

## Prefix effect

Repeated-lookup batch mean time, in microseconds per operation. Parentheses
show the minimum and maximum batch means across three runs, not latency
percentiles. Neither 4 KiB case issued disk reads during the repeated pass.

| Logical key | Difference | No prefix | 32-byte prefix |
|---|---|---:|---:|
| 4 KiB | Early | 1.413 (1.386–1.414) | 0.681 (0.659–0.805) |
| 4 KiB | Last eight bytes | 2.596 (2.501–2.603) | 2.636 (2.473–2.728) |
| 64 KiB | Early | 3.852 (3.412–4.605) | 2.899 (2.752–3.437) |
| 1 MiB | Early | 43.388 (40.759–46.479) | 39.800 (37.061–46.037) |
| 2 MiB | Early | 82.001 (72.025–91.647) | 92.028 (84.977–99.330) |

For early-difference 4 KiB keys, the prefix gave about 2.07 times the lookup
throughput. It reduced blob-comparison fallbacks from 439,721 to 16,743 per
32,768-query pass. Hits still compare the complete logical key for equality.
The 2 MiB case did not improve: only 16 records and 64 queries were used, and
full-key equality work dominates. The prefix is not a universal large-key
speedup.

Late-difference 64 KiB and 1 MiB keys exposed cache pressure. With a 32-byte
prefix, repeated lookup took 71.777 and 824.875 microseconds per operation,
respectively, and read another 14.402 and 31.984 MiB per batch. Calling those
passes entirely memory-resident would be incorrect. A cold-start batch means
the cache was evicted before the batch; it does not mean every lookup was cold.

## Inline versus forced blobs

Both variants use the same logical order and hash. The inline variant retains
normal lexical branch prefix compression. All repeated lookup passes below
had zero measured disk-read bytes.

| Key size | Difference | Inline lookup, us/op | Blob/32 lookup, us/op | Inline allocated MiB | Blob allocated MiB |
|---|---|---:|---:|---:|---:|
| 128 B | Early | 0.461 | 0.568 | 5.500 | 7.750 |
| 128 B | Last eight bytes | 0.382 | 1.175 | 1.750 | 7.750 |
| 512 B | Early | 0.596 | 0.534 | 11.625 | 10.250 |
| 512 B | Last eight bytes | 0.441 | 1.405 | 1.375 | 10.250 |

Allocation is total live allocator footprint after source release, including
prototype arena/tree metadata. It is not process RSS or key payload alone.
The 512-byte late-difference blob case was about 3.19 times slower and used
7.45 times the allocator footprint. Inline compression is particularly valuable
for that workload.

At 512 bytes with early differences, blob insertion including key construction
was 0.915 versus 1.117 microseconds per operation for inline keys. Packing
including clone and logical hashing was slower: 1.025 versus 0.809. For
128-byte early-difference keys, blob insertion was 0.766 versus 0.609 and packing
0.459 versus 0.216. These short samples support keeping an inline path, not a
universal crossover threshold.

Packing must process the logical key for hash construction even if prefix
comparison avoids its bytes. The prototype uses streaming FNV-1a for both
variants, an experimental hash rather than the production hash. Native
performance will depend on the actual hash and callback implementation.

The existing clone policy also matters: 64 KiB keys have tails to copy, whereas
1 MiB keys can share full extents. During packing, live allocation for the
64 KiB/prefix32 early-difference case reached 140.125 MiB before source release;
the 1 MiB case, with the same 64 MiB logical key payload, reached 75.875 MiB.
Both settled to 64.750 MiB after source release. This is measured allocation
behavior of these prototype arenas, not a full compaction-space bound.

## Correctness and integration boundary

Eleven correctness cases passed across debug, release, and GCC
AddressSanitizer/UndefinedBehaviorSanitizer builds. Coverage includes empty and
proper-prefix keys, embedded zero bytes, first/last-byte differences, variable
lengths, seeded hash equivalence, page/extent/hierarchy boundaries, malformed
descriptors, prefix inconsistency, clone lifetime, cache eviction, and exact
allocator-baseline restoration. LeakSanitizer was disabled; allocator checks
do not substitute for a general heap-leak test.

Each stored key contains its logical length, a prefix, and a descriptor bounded
to 128 bytes. The configured maximum encoded length is 202 bytes. The measured
32-byte-prefix representations were 76 bytes except the 1 MiB flat descriptor,
which was 132 bytes. Hierarchical encoding keeps larger descriptors compact.

The prefix shortcut lives inside this prototype's lexical comparator.
**An arbitrary application comparator must retain control over ordering.**
Native support should use its blob-aware callback, or materialize keys for its
existing callback. The current `key_compare_is_lexical` flag remains false
for the encoded blob representation; logical and stored lexical properties
are different.

This is a real-cache, real-B-tree experiment with explicit tree/key-arena
ownership pairs. Native integration still needs fallible comparison/hash
contracts, key-aware log/durability/recovery handling, pivot/trunk ownership,
public iterator and callback semantics, and concurrent progress validation.
Comparison failures currently abort the harness. No production crash recovery,
routing-filter lookup, full trunk compaction, concurrent insertion, tail-latency
distribution, or end-to-end database throughput claim follows from these runs.

## Reproduce and inspect

Build and run instructions are in [README.md](README.md). Generated evidence is
retained locally, outside version control:

- [Complete measurement manifest and pause acknowledgements](../../.cache/blob-key-results-20261025/manifest.json).
- [All phase medians and ranges](../../.cache/blob-key-results-20261025/summary.csv).
- [Readable phase tables](../../.cache/blob-key-results-20261025/summary.md).
- [Validation commands, results, compiler, and source/binary hashes](../../.cache/blob-key-validation/validation.json).

The measurement directory also contains every raw stdout/stderr file and the
request/release records. Successful scratch benchmark databases were removed;
raw evidence was retained. No additional blob-key performance experiment was run after
this window. Source and measured-binary hashes were independently audited.
