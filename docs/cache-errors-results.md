# Fallible cache reads and caller-owned page handles

This branch changes `cache_get()` and the asynchronous cache result API to return
`platform_status` and fill a caller-owned `page_handle`. A successful handle owns
one read reference. Moving its value transfers that reference; copying the value
does not acquire another one. Cross-thread ownership transfer is a separate,
stacked experiment.

The receipt is now pointer-sized: it names a shared `cache_page` whose identity
is stable while the reference is held. `page_handle_data()` and
`page_handle_addr()` require a live handle; `cache_unget()` clears the receipt.
This avoids reconstructing the cache slot from the data-buffer address and
keeps btree/iterator handle fields at their original pointer size. Cache entries
remain 64 bytes. `cache_alloc()` also fills caller-owned storage; retained log
allocations and mini-allocator rollover preserve their receipt lifetimes.

Demand-read errors propagate through btrees, blobs, routing filters, log recovery,
trunk traversal, insertion, point lookups, and range initialization. A failed read
clears the output handle and releases its reference. The cache removes failed
loads from its directory and wakes waiters, permitting later reads to retry.
Shared cache-entry identity reads and writes use relaxed atomics because stale
lookup candidates can inspect a reused loading slot or a failed-load invalidation.
`CC_LOADING` still provides the publication barrier for bytes and read status.
No stronger ordering was added to the read-reference counters.

Nonblocking cache misses or write conflicts return `STATUS_BUSY`. Speculative
prefetch failures drop the hint; a subsequent demand read reports its own result.

Reclamation follows the agreed space-loss policy: consume the final external
reference once, retain any space left after failure, and never restart a partially
completed reclamation. The allocator remembers the first failure and refuses to
persist a clean allocation map. Close checks this after destructive cleanup,
prints an explicit warning, and leaves the recovery-required superblock marker.
A close that preserved the data can still return success under the existing
close contract. Reopen rebuilds allocation state from reachable roots and does
not read abandoned, unreachable metadata.

## Validation

Build with `make --jobserver-style=pipe -j4 BUILD_MODE=debug all`.
The following debug checks passed on 2026-10-11:

- `build/debug/bin/unit/writeback_set_test`: 14 tests, including synchronous
  read errors and retry, async initialization/append/completion failures, queued
  async waiters, a synchronous waiter on a failed async load, failed prefetch,
  and reclamation failing after an earlier group of extents was already freed.
- `build/debug/bin/unit/btree_stress_test iterator_` and `count_in_range`:
  iterator setup and rank errors preserve a distinctive `STATUS_TIMEDOUT`.
- Complete `btree_test`, `trunk_async_test`, `trunk_gc_test`, `trunk_split_test`, and
  `splinterdb_quick_test` suites (7, 2, 1, 3, and 47 tests respectively).
  The async trunk tests cover a yielded pivot-page read error and stack lifetime.
- `build/debug/bin/unit/splinter_test test_cache_read_errors_reach_core_operations`:
  insertion, synchronous point lookups through memtables/trunk/branches,
  asynchronous lookup, and range initialization return the injected status;
  the database remains usable afterward.
- `build/debug/bin/unit/splinter_test test_late_reclamation` and `test_unmount_`:
  a real metadata-read failure after durable root publication leaves no clean
  allocation marker; reopen preserves data, skips the unreadable orphan,
  reclaims its allocation, and permits the next close to become clean.
- `build/debug/bin/driver_test cache_test --async --cache-capacity-mib 64
  --db-capacity-gib 2`: concurrent readers/writers, cache pressure, early
  prefetch wakeup, and idle/exited batch owners.
- `build/debug/bin/driver_test splinter_test --functionality 100000 100
  --cache-capacity-mib 64 --db-capacity-gib 2`: shadow-verified workload.

The original implementation also passed a full release build and its release
core fault-propagation test. After the compact-receipt conversion, the full debug
build and all checks above passed again, along with the 500,000-record log driver;
the release library and functional driver rebuilt successfully. The final
validation used `BUILD_ROOT=build-receipt`, keeping earlier measured executables
unchanged.

The full aggregate unit-test binary was interrupted while running the expensive
concurrent-insertion case; it is not reported as a completed suite.

A follow-up fixes an inherited prefetch admission race. A reader could retain an
old lookup candidate while prefetch reused that slot, set the same address, then
lost publication to another loader. The reader could acquire the unpublished
loser's reference before that slot was freed. Prefetch now keeps `WRITELOCKED`
until its directory CAS succeeds, clearing it before appending the I/O page.
Demand admission still initializes identity after its CAS and is unchanged.
The deterministic `cache_test --prefetch-admission` failed with the exact stale
reference assertion on the unguarded code and passed after this repair.
The async cache driver, all 14 writeback tests, and all 47 public quick tests
also passed using `BUILD_ROOT=build-prefetch`. The first follow-up unit commands
mistakenly selected zero tests; those logs are retained as invalid validation
under `prefetch-admission/invalid-zero-selection`. The corrected manifests and
logs explicitly verify the complete nonzero suite counts. Debug-only hooks
schedule this interleaving; release builds contain neither the hooks nor their
branches.

## Performance

The implementation is **not established as performance-neutral**. The compact
receipt leaves a repeatable 2.5–2.9% single-thread resident point-read cost.
The first eight-thread point/short-range losses were smaller or absent in a
focused repeat. A longer fixed-record WAL insertion case showed a material
slowdown that did not repeat with closer baseline bracketing. The variation
across those controls prevents a strong claim about small insertion costs.

The earlier 16-byte receipt implementation (`c409c6b`) lost 5.8% in single-thread
resident point reads and 6.0% in one-row ranges in a 15-second ABBA repeat.
That prompted the pointer-sized receipt implementation (`068fef0`; `1a8190a`
adds formatting only). The results below measure the compact implementation.

Mean throughput change against the original `db3047a` baseline (two samples per
version, positive means faster):

| Workload | Resident, 15 s | Disk, 5 s |
|---|---:|---:|
| Point, 1 thread | -2.52% | +0.66% |
| Point, 8 threads | -5.63% | +0.09% |
| Range 1, 1 thread | +3.41% | +0.99% |
| Range 1, 8 threads | -5.51% | +0.73% |
| Range 10,000, 1 thread | +0.61% | +1.65% |
| Range 10,000, 8 threads | +0.04% | +4.02% |

The focused 15-second ABBA repeat measured point/1 at **-2.91%**, point/8 at
**-2.41%**, and range-1/8 at **+0.50%**. All read runs reported zero errors and
violations; resident query phases read zero disk bytes. Absolute eight-thread
rates varied substantially across campaigns, so compare versions within each
paired campaign. For example, the first point/8 baseline spanned 5.688–5.971M
ops/s and its candidate 5.471–5.531M; the repeat spanned 5.442–5.521M and
5.303–5.396M respectively.

A diagnostic `perf stat` capture on CPUs 0–11 measured point/8 at approximately
+2.63% cycles, +3.23% instructions, and +5.47% branches per reported query;
range-1/8 was +0.75%, +1.38%, and +1.29%. These whole-process counters include
warmup/startup/close and therefore provide approximate comparisons, not exact
query accounting. Only `cpu_core` counters (about 99% running time) were used;
near-zero-runtime `cpu_atom` extrapolations were excluded. Flat profiles locate
hot functions but do not by themselves establish an increase in absolute cost.

Six-second insertion runs, followed by close/drain:

| WAL | Inserters | Insert throughput | Throughput including close |
|---|---:|---:|---:|
| Off | 1 | +2.71% | +1.83% |
| Off | 8 | -2.77% | -18.52% |
| On | 1 | +0.82% | +0.97% |
| On | 8 | -1.38% | -2.69% |

The large no-WAL close difference was sensitive to where the timed run stopped:
baseline close took 0.131 and 2.733 seconds, compared with 2.617 seconds on
average for this branch. A 20-million-record control with 20 ms completion
polling measured **+4.01%** throughput including close without WAL and
**-10.25%** with WAL (8 inserters). WAL CPU efficiency differed by only -0.35%
and read/write bytes per record were essentially unchanged; the maximum recorded
insertion stall increased from 2.24 to 2.92 seconds.

The closer bracketed repeat used six original baseline runs and two samples of
this branch per WAL mode. It measured **-1.97%** insertion throughput without WAL
and **+0.62%** with WAL; including close, **-1.98%** and **+0.82%**. The six WAL
baselines ranged from 1.350M to 1.712M ops/s. Against the average of each sample's
nearest baseline on either side, this branch's WAL results were +3.08% and
-5.67%, with CPU-efficiency changes of -0.71% and -1.38%. The earlier 10% WAL
slowdown therefore did not reproduce and cannot be attributed to this branch
from these measurements. All completed insertion runs returned zero load and
close errors. An earlier fixed-record control with 1,000 ms polling rounded
completion to whole seconds and is excluded from conclusions.

All timed database images are under
`.cache/projects-20261011/benchmarks/` on ext4 backed by a real block device
(`/dev/mapper/snafu-root_crypt`). Each benchmark takes the campaign's exclusive
benchmark and machine locks; builds and correctness tests share the machine lock
and therefore cannot overlap a timed run. I/O uses `O_DIRECT`.

The same `db3047a` `benchmarks/kvbench.c` source is linked against each release
library with `-O3 -ffast-math -flto -march=native`. Each read regime uses four runs
in baseline/candidate/candidate/baseline order, copying the same immutable
baseline database before every run. CPUs 0–11 are selected with `taskset`.
The resident fixture has 200,000 keys and 128 MiB cache; the disk fixture has
2,000,000 keys and 64 MiB cache. Both use 8 configured foreground threads, 1
memtable background thread, 3 normal background threads, a 4 MiB memtable, WAL
disabled, seed 42, and one warm traversal. Point reads and ranges of 1 or 10,000
keys run with 1 and 8 threads. Resident phases last 15 seconds; disk phases last
5 seconds. Resident query I/O is checked from the recorded byte counters.

Sustained insertion uses the identical baseline `benchmarks/insert_benchmark.c`
source for baseline, this branch, the handoff branch, and both project 9 cache
candidates. Each case runs a mirrored order with two samples per implementation,
6 seconds of closed-loop insertion followed by close/drain, 1 or 8 inserters,
WAL off or on, 256 MiB cache, 8 MiB memtables, 16-byte keys and values, 1/3
background threads, `O_DIRECT`, and the same CPU set. Every run creates a fresh
database; the dedicated scratch image is reused only after a successful close.

Raw CSVs, complete commands, binary/fixture SHA-256 hashes, logs, and return codes
are retained in the campaign directories. Reproduction scripts are
`coordination/build_kvbench.py`, `coordination/compare.py`,
`coordination/build_cache_insert.py`, and `coordination/cache_insert_compare.py`.
Invoke builds/tests through `coordination/run_job.py --name NAME build|test --`
and timed runs through `coordination/run_job.py --name NAME --db-dir DIR
benchmark --`; `DIR` must be under the campaign's disk-backed benchmark root.

The read matrix covers fixed point/range workloads and the insert matrix covers
short sustained writes. It does not establish behavior for every key/value
shape, cache size, storage device, long-running update workload, or contention
pattern. Two samples per implementation expose variation but do not provide a
strong statistical guarantee.

Measured artifacts under the campaign benchmark root:

- `cache-errors/resident-log0-receipt15` and `disk-log0-receipt`.
- `cache-errors/resident-log0-receipt-repeat15` and `profile-receipt`.
- `cache-errors/insert-receipt` and `insert-receipt-fixed20m-20ms`.

The baseline query binary SHA-256 is
`40a04c4e24b3018cade6ad4a4fd2b3a39a8b4f050a364d1001c82ccc788e0d3e`;
the compact project 1 query binary is
`61f4474690a12185843a3df8bf69c1ee8e6dc40c291e65bbefe9641406abf5c4`.
Per-run manifests retain the fixture hashes and exact commands. The fixed-record
control uses the same insertion settings above, with `--records 20000000
--seconds 0 --inserters 8 --report-ms 20`. Use a new campaign/output directory
when rerunning the scripts.

The final fixed-record control is `cache-errors/insert-tagged-bracket20m`. Its
order is original baseline, project 1, baseline, 16-byte handoff, baseline,
tagged handoff, tagged handoff, baseline, 16-byte handoff, baseline, project 1,
baseline, repeated with WAL off/on. This supplies closer baseline observations
without changing the workload or measured binaries.

Compact per-run summaries, CSVs, command manifests, binary provenance, and the
machine description are committed under
[`benchmarks/results-20261011/cache-errors`](../benchmarks/results-20261011/cache-errors/README.md).
Large interval traces, logs, images, and binary profiles remain at the original
campaign paths. The measurements above precede the narrow prefetch admission
repair. Its follow-up cold-read comparison uses a separate build root, preserving
the earlier measured binaries; resident cache-hit code is unchanged.

The prefetch-guard follow-up compares `526a0a0` with this branch's own frozen
pre-guard binary, using the disk fixture, five-second range phases, and ABBA
ordering (`disk-log0-prefetch-guard`). It is inconclusive because the first
baseline run was unusually slow; the raw apparent throughput gains of 23–71%
are not evidence of a guard benefit. Throughput sample ranges were:

| Range / threads | Before guard, queries/s | With guard, queries/s |
|---|---:|---:|
| 1 / 1 | 5,315–12,859 | 12,359–12,796 |
| 1 / 8 | 11,424–67,109 | 66,793–67,811 |
| 10,000 / 1 | 88–140 | 140–140 |
| 10,000 / 8 | 213–732 | 765–781 |

All 20 recorded phases reported zero errors and violations. The repair adds one
atomic status clear per admitted prefetch page, with no resident hit-path source
change; this noisy disk control does not establish that the repair is free.
`prefetch-admission/release-manifest.json` identifies the new release build and
confirms the older measured binary was preserved. The exact regression and
corrected suite commands are in the adjacent validation manifest. Reproduce
the follow-up with `coordination/cache_prefetch_validate.py fixed`,
`cache_prefetch_release.py`, and `cache_prefetch_cold.py cache-errors` under the
same build/benchmark locking wrapper described above.
