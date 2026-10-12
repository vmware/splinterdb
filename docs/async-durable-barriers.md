# Durability notifications

`splinterdb_durable_barrier_async(kvs, notification)` accepts the same polling,
blocking and fire-and-forget notification modes as optimize. The existing
`splinterdb_durable_barrier(kvs)` remains available without allocation or task
queue overhead.

```c
splinterdb_notification done;
splinterdb_notification_init_polling(&done, NULL);
int rc = splinterdb_durable_barrier_async(kvs, &done);
if (rc == 0) {
   /* Application work may continue here. */
   rc = splinterdb_notification_wait(kvs, &done);
   if (!splinterdb_notification_poll(&done, NULL)) {
      /* A task-system error left work pending: retain done and retry waiting. */
   }
}
/* Deinitialize after failed launch or observed completion. */
```

Successful completion makes updates that linearized before launch recoverable.
Launch takes the in-memory log-group cut while protecting the live log from
rotation, then queues the slow durability wait. Its ticket pins the log even if
a checkpoint later retires that stream. A barrier whose cut races a checkpoint
also waits for the checkpoint's required superblock publication.

Without a WAL, launch captures the current memtable generation and queues a
checkpoint covering it. Checkpoint tasks can perform other queued work; they do
not wait for their own task to become quiescent. A barrier reached recursively
by that task pump requeues itself, bounding nested wait stack usage. Close
drains accepted barrier tasks before releasing database state.

Launch allocates memory and takes internal locks, but performs no device I/O.
If allocation or enqueue fails, the polling call returns an error and leaves
the notification idle. Enqueue failure drops the ticket pin while the insert
epoch still protects the live log, so this cleanup cannot reclaim a retired
stream's allocation metadata. A failed completion is reported through the
notification and does not establish a durability guarantee.

Polling only observes completion. With no normal background workers, use
`splinterdb_notification_wait()` or another operation that performs task work
to make progress. Other foreground operations may execute queued barrier work,
so moving the durability wait off launch does not make every subsequent API
call nonblocking. Notification storage must remain alive until completion;
cross-process completion requires shared notification storage. NULL launches
without retaining a completion result.

Validation covers WAL enabled/disabled, zero/one normal background workers,
multiple pending barriers, polling/blocking/fire-and-forget modes, close with
pending work, reopen and data verification, deterministic enqueue failure,
device-barrier failure, ticket reference cleanup and successful retry.
The final debug internal Splinter suite passes all 34 tests; the public quick
suite passed all 48 tests, with its notification matrix rerun after the nested
wait guard was added. Release libraries and both benchmark variants build.

The deterministic nested-wait test covers both successful deferral and an
enqueue failure during deferral, checking single completion and balanced log
references. A separate device-barrier gate test parks a worker at the device
sync, then proves a second polling launch returns while both notifications
remain pending.

`benchmarks/build_barrier_bench.py /path/to/tree` builds the same public-API
source against a baseline or candidate. Run `barrier_bench DB CSV LABEL MODE
LOG WORKERS BATCH BARRIERS`, where MODE is `sync` or `async`, LOG and WORKERS
are 0 or 1, and `BARRIER_DEPTH` optionally sets outstanding async barriers.
Use fresh files on a real disk under the campaign's exclusive benchmark lock.
The harness sets O_DIRECT, exits its writer process without a clean close,
verifies every tuple after crash recovery in the parent process, and reports
throughput, CPU time, launch latency and observed completion latency. At depth
greater than one, observed completion includes intervening application work;
it is not a device-service latency measurement.

The independent log-group-size project can be combined with this API; group-size
cuts and barrier tickets have separate purposes. That integration is not part
of this branch.

# Measurements

The common-source benchmark used 24-byte keys, 100-byte values, 256 inserts per
barrier, a 128 MiB cache, O_DIRECT files on ext4, CPU affinity 0–11 and zero
memtable background workers. It compared baseline blocking, candidate blocking,
and candidate polling with one or eight outstanding notifications. Each of the
four WAL/normal-worker configurations has three repetitions per mode. The
writer process exits without close after all barriers finish; the parent opens
and verifies every key/value before recording success. All 48 initial measured
runs and four pilot runs recovered successfully.

| Initial matrix: WAL / normal workers | Async launch mean, depth 1 / 8 | Async launch p99, depth 1 / 8 |
| --- | ---: | ---: |
| Off / 0 | 0.49 / 0.55 us | 1.20 / 1.37 us |
| Off / 1 | 1.07 / 1.13 us | 3.03 / 2.94 us |
| On / 0 | 0.80 / 0.78 us | 2.69 / 2.18 us |
| On / 1 | 1.33 / 1.43 us | 4.88 / 6.26 us |

Launch timing includes polling-notification initialization. Each cell averages
three run-level measurements, including the p99s. Throughput
was strongly affected by shared wall-time shifts. For example, WAL/one-worker
baseline blocking fell from 289k updates/s to 63k in later repetitions, while
candidate blocking fell from 262k to 62k with similar CPU time. At zero workers,
WAL baseline instead rose from 61k to 273–280k. A whole-matrix mean would falsely
attribute those shifts to the API mode. These observations motivate the fixed-
count controls below; they do not establish a throughput gain or regression.

The follow-up controls used 2,048 barriers with WAL/one worker and 1,024
barriers without WAL/workers. Each repeats all four modes three times, using
one fresh scratch image at a time and collecting process block-I/O accounting.
All 24 additional runs passed recovery. The latency shift recurred: the WAL
baseline's later sample fell from 244–251k to 62k updates/s, while the candidate
blocking sample fell from 259–270k to 73k. All modes wrote essentially identical
amounts. The table retains the full measured throughput range instead of
presenting a misleading whole-matrix mean.

| Control | Mode | Throughput range (k updates/s) | Mean CPU (us/update) | Mean written MiB/run |
| --- | --- | ---: | ---: | ---: |
| WAL / 1 worker | Baseline blocking | 61.9–250.6 | 1.294 | 103.32 |
| WAL / 1 worker | Candidate blocking | 72.9–270.1 | 1.273 | 103.32 |
| WAL / 1 worker | Async depth 1 | 63.8–274.1 | 1.385 | 103.32 |
| WAL / 1 worker | Async depth 8 | 64.3–277.5 | 1.345 | 103.32 |
| No WAL / 0 workers | Baseline blocking | 19.9–27.3 | 14.739 | 2715.91 |
| No WAL / 0 workers | Candidate blocking | 20.3–28.4 | 14.605 | 2715.91 |
| No WAL / 0 workers | Async depth 1 | 20.6–21.1 | 14.893 | 2715.91 |
| No WAL / 0 workers | Async depth 8 | 19.7–22.2 | 15.154 | 2715.94 |

The controls averaged 1.96 / 1.80 us to launch WAL barriers at depth 1 / 8,
and 0.56 / 0.63 us without WAL. Async CPU per update was approximately 2–9%
higher than candidate blocking; block-write volume differed by less than 0.002%.
The existing blocking path showed no CPU regression in these controls, but the
shared wall-time variability prevents a tight throughput-neutrality claim.
Async notification support is functionally validated and makes launch cheap;
it is not a promise to reduce the total durability work or its CPU cost.


The release-only ticket operation and queue-failure cleanup are tested; polling
launches perform no device I/O, and the deterministic gated-device test proves
launch returns while durability remains pending. Completion still performs the
same log/checkpoint work. With eight outstanding operations, observed completion
latency includes application work and queueing, so it should not be compared to
blocking call latency as if both measured device service time.

Compact raw CSVs, manifests, exact measured harness sources and build provenance
are committed in `benchmarks/results-20261011/`. Full logs remain under
`.cache/projects-20261011/benchmarks/async-durable-barrier/` in the main checkout.
The original successfully verified scratch images were reclaimed after their
runs; subsequent controls reuse a single fresh scratch path, unlinking only
after successful crash-recovery verification. No database images are committed.
