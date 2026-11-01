# Blob-backed key prototype

Latest scan follow-up: [shared-prefix Scan4096 profiling and retained fix](SHARED_SCAN4096_PROFILE.md)
makes these scans 12.8–13.4% faster than original main across all four key sizes.
The retained fix also removes the intermediate random-scan tradeoff. All 882
runs of the final full matrix verified; custom insertion, overwrite, and hits
retain roughly 2% overhead.


Native-format small-key regression results are in [SMALL_KEYS.md](SMALL_KEYS.md).
The initial native candidate regressed small-key performance. The subsequent
[Scan4096 profile](SCAN4096_PROFILE.md) identifies and fixes the large scan
slowdown. The subsequent [Scan32 remeasurement](SCAN32_RESULTS.md) confirms a
substantial improvement but still finds up to 14.1% median time overhead.
[Scan32 profiling and fixes](SCAN32_PROFILE.md) identify a tree-height threshold
and reduce the largest remaining Scan32 overhead to about 4.3%.
The [format-4 full matrix](SMALL_KEYS_V4.md) remeasures all 63 B-tree cases.
[Packing profiling and fixes](PACK_PROFILE.md) subsequently remove the measured
packing regression. [Custom-comparator overwrite profiling](OVERWRITE_PROFILE.md)
reduces its measured overhead from 11.7% to 5.2%; a regression remains.

This experiment stores large logical keys in SplinterDB's existing blobs and
indexes compact key descriptors in the real mutable and packed B-trees. It is a
standalone experimental codec. Its measured version is commit da464513.
The subsequent [native B-tree implementation](../../docs/blob-keys.md) changes
the persistent format; this harness retains its original codec and does not
measure the native encoding. Public SplinterDB large-key writes remain disabled.

Each encoded key contains its logical length, up to 64 actual leading key bytes,
and an existing blob descriptor bounded to 128 bytes. The maximum encoded key is
202 bytes, below the 512-byte inline-key limit for 4 KiB pages. The blob format's
existing hierarchical descriptors handle larger logical lengths.

The comparator uses the current `data_config` callbacks. Stored keys are encoded;
`user_key.is_query_key` identifies raw query keys. This prototype defines
unsigned-byte lexical ordering: it compares cached prefixes, then streams blob
fragments when the prefixes cannot decide. The prefix is not a fingerprint, and
a prefix match never proves that two longer keys are equal.

**Arbitrary application comparators cannot use this shortcut automatically.**
The native implementation materializes full keys for the existing custom
comparator; it does not introduce blob-aware application callbacks. The existing `key_compare_is_lexical` flag
describes stored byte representations; it is false in blob mode even though
the logical order is lexical. The inline baseline enables normal lexical
branch prefix compression.

The logical-key hash uses a simple streaming FNV-1a implementation consistently
for raw and encoded keys, independently of fragment boundaries. This hash is
experimental, not a proposed replacement for the production hash.

## Ownership and error scope

Every tree owns a separate key-blob arena. Packing and repacking clone each key
into the destination arena. Full extents can be shared by the existing
`blob_clone`; tails are copied. The harness releases the source tree and arena,
flushes and evicts the cache, and then verifies destination lookups and scans.
After releasing every tree, allocator usage must return to its initial value.
The separate arenas include some overhead that a native pack implementation
could eliminate.

The transforming iterator does work in initialization and `next()`, preserving
the observation-only contract of `curr()`. Scans reconstruct the full key,
rather than measuring descriptor traversal alone.

Comparison and hashing callbacks currently have no error return channel. When a callback detects malformed encoding or a blob-read failure, the
prototype aborts; it never manufactures a comparison result after failure. Counters and callbacks are
single-threaded.

The original prototype's integration checklist included fallible comparison/hash dispatch,
blob-aware user callbacks, key/pivot/trunk ownership, log reservation and key
blob durability, recovery validation, iterator/API materialization, logical
versus stored byte accounting, and concurrency/progress testing. This harness
does not exercise the full SplinterDB trunk, log, routing filters, crash recovery,
or simultaneous writers. The native implementation's current scope and
remaining work are documented separately above. This harness cannot establish
end-to-end database throughput
or latency guarantees.

## Build and validate

From the worktree root:

```sh
make -j4 -f Makefile -f benchmarks/blob_keys/Makefile BUILD_MODE=debug blob-key-bench
mkdir -p .cache/blob-keys
build/debug/bin/blob_key_bench --db .cache/blob-keys/check.db \
  --mode blob --records 256 --queries 512 --key-bytes 4096 \
  --prefix 32 --common-prefix 4088 --cache-mib 32
build/debug/bin/blob_key_bench --db .cache/blob-keys/codec-check.db \
  --mode blob --cache-mib 64 --checks
make -j4 -f Makefile -f benchmarks/blob_keys/Makefile BUILD_MODE=release blob-key-bench
```

Every ordinary run verifies inserts, replacements, hit/miss lookups, complete ordered
scans, cloning across source release, and final reclamation. `--variable`
varies key lengths; `--checks` instead runs focused codec boundary checks. Correctness
runs are not performance evidence.

## Coordinated measurement

Ordinary project work uses the shared benchmark and machine locks. Measurements
must acquire both exclusively after every other active project acknowledges
that its root and children have stopped all work.

```sh
python3 benchmarks/blob_keys/run.py \
  --binary build/release/bin/blob_key_bench \
  --out /path/on/disk/to/new-results-directory \
  --coordination /home/rob/projects/splinterdb/.cache/projects-20261025/coordination \
  --lock-dir /home/rob/projects/splinterdb/.cache/projects-20261011/coordination \
  --repetitions 3
python3 benchmarks/blob_keys/summarize.py /path/on/disk/to/new-results-directory
```

The requesting root must stop its own child agents before invoking `run.py`.
The runner records matching peer acknowledgements, source and binary hashes,
commands, raw output, and elapsed intervals. It refuses to measure without
acknowledgements and releases the lock on failure. Registered completed projects
are recorded separately. Concurrent new work must also obey the shared locks.
Only a run's successfully verified scratch database image is removed; logs remain.

The matrix compares inline and forced blob keys at 128 and 512 bytes. At 4 KiB,
64 KiB, 1 MiB, and 2 MiB it compares zero versus 32 prefix bytes. Keys either
differ early or share all but their final eight bytes. Each case uses randomized
insertion order, repeatable 50/50 hit/miss queries prepared outside timing, a
64 MiB Splinter cache, and alternating variant order across repetitions.

Cold-start lookup means the batch begins with an evicted Splinter cache; it does
not mean every query misses the cache. Repeated lookup is the next identical
query stream; check actual read bytes before treating it as fully memory-resident.
The corpus and query bytes are allocated separately from the Splinter cache.
The benchmark is single-threaded and does not clear the operating system's cache;
database I/O uses `O_DIRECT`.

`read_bytes` and `write_bytes` are cache I/O counters. `allocated_bytes`
includes all live prototype arenas and trees at the end of the phase; pack
temporarily retains both source and destination. `blob_bytes` counts logical
fragment spans passed to comparisons/hashing/copies, not actual DRAM or disk
traffic. `blob_fragments` counts comparator/hash fragment visits, not unique
pages; decode uses the blob materialization API. `comparisons` counts callback
calls, so inline lexical fast paths inside B-tree code need not increment it.

Detailed measurements and interpretation will be recorded in `RESULTS.md`.

The [64-byte random point-hit remeasurement](POINT_HIT_64.md) found 6.2% overhead.
Subsequent [profiling and a lexical pivot shortcut](POINT_HIT_PROFILE.md) reduce
point-hit time by 23.2% relative to unmodified main. Applying the same shortcut
to main still leaves 6.0% blob-key overhead; custom-comparator overhead remains.

[Continued profiling](RESIDUAL_PROFILE.md) streamlines ordinary pivot validation
and inline comparison. Against main with the same lexical shortcut, 64-byte
random and custom hits retain about 1% overhead, with intervals spanning zero.
Custom insertion and overwrite still regress by roughly 6% and 4%; the overall
small-key performance objective remains unmet.

[Further mutation profiling](MUTATION_PROFILE.md) removes intermediate key/message
metadata copies through insertion and its helpers. Custom overwrite is now near
main (+0.4% in the longer paired comparison); insertion retains roughly 2%
overhead. All 786 measurements verified, with no clear new control regression.
