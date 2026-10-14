# Notification sentinel API measurements

This compares the previous async-barrier branch API against the unified barrier
API and library-owned blocking optimize notifications. All 44 measured runs
passed data verification. The 32 barrier runs also verified recovery after the
writer exited without closing the database. Benchmarks ran serially under the
campaign's exclusive machine/benchmark locks, on ext4 at
`/dev/mapper/snafu-root_crypt`, using O_DIRECT and CPU affinity 0–11.

## Barrier matrix

`barrier/results.csv` contains ABBA old/new/new/old samples for each combination
of WAL off/on, zero/one normal workers, and blocking/polling barriers. Polling
has one outstanding notification. Each barrier follows 256 inserts; each run
uses 512 barriers without WAL or 2,048 with WAL. Keys are 24 bytes, values 100
bytes, and the cache is 128 MiB. Create/recovery/verification are outside timing.

New-versus-old CPU per update changed by -4.57% to +1.68% across the eight cells;
all WAL-enabled cells were within 0.55%. Polling launch means were 0.44–1.79 us.
Large wall-time shifts recur in the old and new builds: WAL/one-worker blocking
throughput ranged 75–269k updates/s for old and 122–274k for new. These samples do
not establish tight throughput neutrality. `summary.json` retains each cell's
throughput range, CPU, launch/completion latency, and write volume.

## Blocking optimize controls

`optimize/results.csv` contains ABBA samples with zero/one normal workers. Each
run loads 20,000 keys, performs a durable barrier and 100 full-range warmup
optimizations, then times 20,000 blocking full-range optimize calls. Reopen and
full data verification follow timing. These calls still perform compaction CPU
work; this is not an isolated measurement of allocation overhead.

| Normal workers | Old elapsed us/call range | New elapsed us/call range | Mean elapsed change | Mean CPU change |
| --- | ---: | ---: | ---: | ---: |
| 0 | 1502.10–1529.35 | 1538.30–1566.66 | +2.42% | +2.42% |
| 1 | 1559.39–1560.19 | 1591.95–1604.36 | +2.46% | +2.74% |

No timed writes occurred. The zero-worker samples read no disk blocks; every
one-worker sample read 122,880 bytes. The consistent ~37–38 us elapsed increase
remains unattributed.

`optimize-empty/results.csv` adds a focused ABBA control with equal range bounds,
which complete inline. Each sample makes one million blocking calls with zero
workers after the same database setup and warmup. Mean elapsed cost was 72.858
ns/call old versus 89.471 ns/call new, an increase of 16.613 ns. CPU agrees, and
no timed I/O occurred. This includes timing-loop overhead, and samples lasted
only 0.07–0.09 seconds. It measures the inline private-heap case, not queued or
cross-process completion. This small wrapper difference does not explain the
full-range optimize shift.

## Evidence and reproduction

- Each workload subdirectory contains raw CSV, per-run logs, and manifests with
  exact commands, source/header/library/binary hashes, timing, and exit status.
- `measured-initial-*` preserves the exact initial harnesses/build helper/runner.
  The initial optimize comment incorrectly called this allocation isolation;
  the observed CPU cost above establishes that it was repeated compaction work.
- `measured-empty-optimize_notification_bench.c` is the exact source used for the
  empty-range control; `OPTIMIZE_EMPTY_RANGE=1` selects it. The current harness
  also supports this option and correctly describes both modes.
- `source-provenance.json` records base HEAD, hashes of changed runtime sources,
  library hashes and physical storage. `implementation.patch` is the runtime
  source delta from that base. The preserved old library matches the prior
  campaign's `9d66eee` implementation provenance exactly.
- `old-build-config.txt`, `new-build-config.txt`, and
  `compiler-provenance.json` establish matching GCC 14.2 release compiler/link
  settings, including archive-object LTO options. The configuration differences
  are only output directory and make parallelism/jobserver.
- `notification_sentinels_compare.py --root CAMPAIGN_ROOT --workload WORKLOAD`
  replays one workload under `coordination/run_job.py` benchmark mode. It expects
  the preserved old headers/library/harnesses in
  `coordination/notification-sentinels-baseline/build/release` and the candidate
  in `worktrees/async-durable-barrier/build-sentinels/release`. Use the common
  `benchmarks/build_barrier_bench.py` to build harnesses for each API, with
  `--build-root build-sentinels` for the candidate. Original initial and empty
  controls used distinct frozen harness sources as recorded in their manifests.
- `benchmark-jobs.json` records the coordinated build/benchmark invocations.
  Full scratch-run paths were under
  `.cache/projects-20261011/benchmarks/async-durable-barrier/sentinels-20261012/`.
  Scratch databases were removed only after successful verification. Libraries,
  executables, and database images are not committed.

Correctness-test evidence is recorded separately in this directory by the
implementation task.
