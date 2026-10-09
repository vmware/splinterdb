# Insertion-spike investigation, 2026-09-23

The workload reproduces repeated near-total insertion stalls on both the current
branch and `main`. On the current branch, internal traces show all eight
inserters executing bundle and routing-filter compactions during these stalls.
Disabling ordinary foreground assistance removes the insertion stalls in a
control run, but substantially reduces query throughput. This is evidence about
the mechanism, not a proposed scheduling change.

## Workload and limits

All four runs inserted 201,326,592 unique random keys (6 GiB of logical data),
with 8 inserters, 8 random point-query threads, 16-byte keys, 16-byte values,
3072 MiB cache, 24 MiB memtables, 1 memtable background thread and 4 normal
background threads. Logging was enabled, automatic checkpoints disabled, and
statistics enabled. Unless stated otherwise, `queue_scale_percent=100`.
Readers chose already completed inserts; measured hit rate was 100%.

Runs used buffered I/O, a 64 GiB sparse image capacity, one-second sampling,
seed 1, and clang-19 release builds with O3/LTO. They ran sequentially on the
same machine (20 logical CPUs, approximately 62 GiB RAM); the OS page cache was
not capped or explicitly evicted. Consequently this does not reproduce the
memory pressure of a 60 GiB load. There was only about 53 GiB free disk initially;
each 6 GiB image used approximately 25 GiB physically. Images were removed
between runs, retaining CSVs, logs and traces. No 60 GiB run was attempted.

Key/value split, hit-only queries, buffered I/O, memtable size and queue threshold
are explicit assumptions where the original workload did not specify a value.
The benchmark defaults to 60 GiB and exposes these controls for a larger run.

## Measurements

These are individual diagnostic runs, not repeated estimates of average speed.
Means include worker completion tails; stall detection excludes those tails and
the first five seconds. Load times exclude creation and final drain/close.

| Run | Load seconds | Mean inserts/s | Mean queries/s | Maximum insert latency |
| --- | ---: | ---: | ---: | ---: |
| Current branch, untraced | 98.105 | 2.052 M | 0.755 M | 926 ms |
| Current branch, traced | 96.096 | 2.095 M | 0.772 M | 1031 ms |
| Current branch, traced, foreground threshold UINT64_MAX | 93.072 | 2.163 M | 0.411 M | 63 ms |
| `main`, untraced | 78.044 | 2.580 M | 0.975 M | 498 ms |

Current HEAD was `ee9124fb6969af4003ea7b71b6249e45e481c1b6`, plus the
benchmark/instrumentation changes. `main` was
`938676bd0e9095370f786d6e9d27ef7f62064dc0`. The main comparison used an isolated
source archive and the same benchmark, adapting only its older, void-returning
one-argument close API. No internal trace sites were added to main. The main
log's `close_rc=0` is a placeholder because that API has no return status.

On the untraced current branch, the worst groups occur around 19–20, 44–46 and
71–74 seconds, at about 1.57, 3.08 and 4.60 GiB loaded: roughly another 1.5 GiB
between groups. Individual intervals fall to 57, 27–33 and 24–25 inserts/s,
against neighboring medians around 1.8–2.5 million/s. Queries continue at
approximately 0.5–0.7 million/s during the near-zero intervals.

The traced run reproduces the pattern. During 19.031–20.031 seconds:

- Insertion throughput is 57/s; neighboring median is 2.414 million/s.
- Queries continue at 752,169/s.
- Inserters spend 6.851 thread-seconds in `bundle_compaction_task` and 1.151
  thread-seconds in `maplet_compaction_task`: essentially all eight available
  thread-seconds. The later near-zero intervals show the same occupancy.
- There are no `memtable_wait` events anywhere in the traced load.
- All ten leaf-pivot selections use height 1. Their maximum duration is 15.3 ms
  and total duration is 131 ms. The newly introduced height-0 fallback is not
  exercised here. Eight selections cluster at 18.59–18.80 seconds, before the
  first large compaction wave; later stall groups have no pivot selections.

`core_insert()` calls `task_perform_one_if_needed()` after releasing the memtable
insert lock. With threshold 100, queued work in a task group can cause an
inserter to execute a complete compaction before returning its insert. Bursts
can occupy all eight inserters at once, leaving very few completed inserts to
report even though the process remains busy. The task scheduler is unchanged
between main and the current committed branch, and the normal trunk-flush
decision thresholds are also unchanged.

Main has >50% drops around 15–17, 36–38 and 57–60 seconds, including intervals
with only 42 and 38 inserts/s. Thus the stall pattern predates this branch. The
current branch's lower overall throughput in this one pair remains unexplained;
this experiment does not attribute that difference to a particular commit.

The UINT64_MAX threshold control has no samples below half the neighboring
median, no foreground tasks and no memtable-wait events. Its mean query rate is
about 47% below the traced default-threshold run. Deferring compactions plausibly
increases lookup work, but lookup-path instrumentation is needed to establish
that explanation. Simply disabling assistance is not a demonstrated net win.

The remaining scheduling question is how to spread compaction work without
allowing long queues or making queries more expensive. A useful next experiment
would trace queue depth and compaction input size/node height at enqueue time,
then test bounded foreground work or additional background capacity. The current
changes deliberately retain the existing flushing/scheduling behavior.

## Follow-up: queue_scale_percent = 210

A further traced run changed only `--queue-scale 210`, with the same 6 GiB
workload and 8-inserter/8-reader configuration. It completed all 201,326,592
inserts, maintained 100% query hits, and closed successfully in 3.249 seconds.

| Traced run | Load seconds | Mean inserts/s | Mean queries/s | Lowest full-concurrency interval | Maximum insert latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| Queue scale 100 | 96.096 | 2.095 M | 0.772 M | 24/s | 1031 ms |
| Queue scale 210 | 94.081 | 2.140 M | 0.685 M | 24/s | 1010 ms |

These are single runs, so the 2.1% increase in mean insertion throughput and
11.3% decrease in mean query throughput are descriptive, not established tuning
effects. Means include the completion tails; the minimum interval excludes
the first five seconds and all samples after an inserter finishes.

With four normal background workers, the scheduler's integer comparison admits
foreground assistance at nine queued tasks for 210, versus four for 100. With
one memtable worker, the threshold changes from one queued task to three.
All 318 memtable tasks ran in the background at 210. Normal foreground
compactions consumed 156.565 thread-seconds, versus 218.472 at 100, a 28.3%
reduction. Nevertheless, the higher threshold still lets all eight inserters
take compaction tasks during a sufficiently large backlog.

The first large drop reached 301,001 inserts/s at 19–20 seconds (87.6% below
the neighboring median). Subsequent intervals reached 31/s at 43–44 seconds
and 27/s and 24/s at 68–70 seconds. The latter groups occur at 3.078 and 4.603
GiB inserted, matching the original pattern. During 69.060–70.060 seconds,
bundle compactions consume 4.470 inserter thread-seconds and routing-filter
compactions consume 3.531: essentially all eight available thread-seconds.
Thus 210 reduces average foreground assistance but does not eliminate these
stalls. The run does not establish long-term queue stability at 60 GiB.

One tracing limitation applies throughout this investigation: `memtable_wait`
records the outer `STATUS_BUSY` retry loop. It does not record waits internal
to `memtable_maybe_rotate_and_begin_insert()`. Absence of that event alone does
not establish absence of every memtable wait. The foreground task spans directly
account for the near-zero-throughput intervals discussed above.

Evidence is in `.cache/insert-spikes/queue210-3gib-6gib/`, including throughput,
trace, exact binary symbols, run log, analysis JSON and plot. The combined plot
is `.cache/insert-spikes/queue-scale-comparison.png`; computed metrics are in
`.cache/insert-spikes/queue-scale-comparison.json`.

## Reproduction and retained evidence

See [README.md](README.md) for building, options and trace analysis. Every run
used this base command with a fresh `RUN` directory:

```sh
build/release/bin/insert_benchmark \
  --db "$RUN/image.db" --csv "$RUN/throughput.csv" \
  --data-gib 6 --cache-mib 3072 --disk-gib 64 --report-ms 1000 \
  > "$RUN/run.log" 2>&1
```

The traced runs add `--trace "$RUN/trace.csv"`. The control additionally adds
`--queue-scale 18446744073709551615`. Save `nm -an` output from the exact binary
before rebuilding. Main used
`.cache/insert-spikes/main-source/build/release/bin/insert_benchmark`.

Artifacts are retained locally under `.cache/insert-spikes/` (git-ignored):

- `baseline-3gib-6gib/`: untraced current branch.
- `traced-3gib-6gib/`: traced current branch, including the matching symbols.
- `no-assist-3gib-6gib/`: threshold control, using the same binary/symbols.
- `main-3gib-6gib/`: untraced main.

Each has `throughput.csv`, `run.log`, `analysis.json` and `throughput.png`.
`comparison.png` compares the insertion and query time series across runs.
The analyzer marks drops below half the median of up to five neighboring samples
on each side; it accounts for events that straddle sample boundaries. Trace
records are nested, so task and wait durations must not be added together.

Validation: 36/36 quick tests and the trunk async test passed. Two 2-million-record
benchmark checks verified completed counts, both query-range modes, 100% hits for
already-inserted keys, trace output, and refusal to overwrite an existing image.
