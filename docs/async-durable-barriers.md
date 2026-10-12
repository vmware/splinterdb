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
to make progress. Notification storage must remain alive until completion;
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
of this branch. The serialized WAL/worker/depth benchmark matrix is pending at
this implementation milestone; no performance-neutrality claim is made yet.
