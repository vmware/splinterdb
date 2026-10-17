# Production-cache API probe

This is an isolated adaptation of the existing `cache_lookup_benchmark.c` from
the cache-directory project, recorded in [provenance.json](provenance.json).
It uses the production clockcache, allocator, caller-owned page handles and I/O
backend. It does not modify any project branch. The author prepared and
source-reviewed this version; the coordinating root agent owns compilation,
smoke tests and measurements.

The source is frozen as v1, SHA256
`7e0c4d2f29e71a00600af0b18bd154227256056a93d5bbaa2d5d2f8f3ca0f680`.
Do not edit it after measurements begin; retain a new named version for changes.

## Build once, select the library at execution

From the branch-prefix worktree, under the root campaign's machine lock:

```sh
python3 .cache/invalid-address-perf-20261017/harness/build_probe.py \
  .cache/invalid-address-perf-20261017/sources/main \
  .cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so
```

The builder creates one object and executable, plus a JSON record containing
commands and source, object, library, binary and private-header hashes. It
refuses existing output files. The object uses `-fno-lto`, so selecting a
different shared library does not recompile or relayout the harness. The
executable contains no embedded RUNPATH. Set `LD_LIBRARY_PATH` to the selected
variant's release/lib directory and verify the loaded path in the runner.
All variants must retain the compile-time private ABI; main and the original
invalid-address branch have identical `clockcache.h`, while `cache.h` differs
only in API documentation. Passing a `.a` library to the same builder supports
a separately named static executable when needed.

## Workloads

CLI: `FILE CACHE_MIB THREADS[,THREADS...] SECONDS ACCESS API`.
`FILE` is a disposable database path: initialization truncates it. Use real
disk and unique per-run paths under the campaign scratch directory.

For example, run the same binary with each selected library:

```sh
LD_LIBRARY_PATH="$variant_lib" "$probe" "$scratch/probe.db" 64 1,8 3 both blocking
LD_LIBRARY_PATH="$variant_lib" "$probe" "$scratch/probe.db" 64 1,8 3 both nonblocking
LD_LIBRARY_PATH="$variant_lib" "$probe" "$scratch/probe.db" 64 1,8 3 both async
LD_LIBRARY_PATH="$variant_lib" "$probe" "$scratch/probe.db" 64 1,8 3 miss nonblocking
```

`both` emits independent-random and dependent-pointer-chain rows. Either can
be selected alone. Blocking and nonblocking hit calls differ only in the
cache API's blocking argument; a nonblocking hit must succeed. Async calls
include state initialization, completion and result retrieval. API selection
occurs when choosing each worker entry point; the shared always-inlined loop
receives a constant API, permitting the compiler to remove API dispatch.

`miss nonblocking` evicts the warmed pages before timing while retaining their
allocator references. Every request must return `STATUS_BUSY` and a cleared
handle. This measures the valid-address miss/allocator-validation path without
issuing demand reads. It has independent access only. Invalid or unallocated
addresses are not a fair common benchmark: main and the invalid-address branch
have different specified error behavior. Keep those cases in correctness tests.

The inherited `churn` mode uses twice cache capacity and allows storage I/O;
use blocking or async APIs for it. It is optional and is not a resident-hit
comparison. Nonblocking churn will reject expected misses as hit errors.

## Controls and interpretation

The default working set is 75% of cache capacity. Addresses are spread across
a logical disk/cache ratio of 16; `CACHE_PROBE_DISK_RATIO` and
`CACHE_PROBE_ADDRESS_LAYOUT=spread|compact` retain the original controls.
Setup writes each selected page with `O_DIRECT`, performs a durable barrier,
and loads every page before a one-second worker warmup. All extents remain
allocated until worker completion and final cache eviction.

Every hit checks page identity and the next-pointer identity, samples cache
status and read references, and releases its handle exactly once. A checksum
accumulates the observed page identity; because runs are duration-based, counts
and checksums naturally differ between variants. They are not equal-work
checksum comparisons. The per-page identity checks are the timed correctness
oracle. Successful final eviction detects retained read handles.

Async workers own one aligned state each. A callback only release-publishes a
readiness flag; its worker acquire-consumes that flag and resumes the operation
until DONE, then retrieves the result. Completed state is reinitialized only
after the result has been consumed. This also supports asynchronous churn;
resident-hit runs should finish synchronously with `async_waits=0`. That
diagnostic counts warmup plus measured waits, rather than only the timed phase.

CSV rows include exact operations, checksum, errors, elapsed seconds, throughput,
directory/entry/data/reference/pin allocation, process maximum RSS, setup time,
phase storage read/write bytes, major/minor faults, process CPU time and async
waits. Resident and nonblocking-miss samples return failure on storage I/O or
major faults. Stats are disabled in clockcache, matching the original probe.
The guard uses `/proc/self/io`, not per-call backend counters. Process CPU and
fault snapshots also include small phase-control/join overhead; use matched
samples and report the measurement scope.

Timing preserves the existing 64-operation batch handoff and duration-based
method. It includes the common consumer/checksum overhead. There is no perf
FIFO control in v1: whole-process perf would include setup and warmup and must
not be reported as instructions per measured hit. Initial short A/B runs are
screening evidence; use balanced repeated ordering and sufficiently long phases
before deciding whether a 2-3% loss is resolved. Pin comparable CPUs and keep
other builds, tests and benchmark jobs outside each timed process.
