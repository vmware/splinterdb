# Durability notifications

`splinterdb_durable_barrier(kvs, notification)` and `splinterdb_optimize()` use
the notification argument to select one of three behaviors:

| Notification argument | Behavior |
| --- | --- |
| `NULL` / `SPLINTERDB_NOTIFICATION_BLOCKING` | Wait and return the operation's status. |
| `SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET` | Submit without waiting and discard completion status. |
| An initialized notification object | Submit without waiting and record completion for `poll()` or `wait()`. |

For either nonblocking mode, a zero return means the operation was accepted;
an immediate error means submission failed. The sentinels are operation
arguments, not notification objects: do not pass them to notification init,
deinit, poll, wait, or user-data functions. The public blocking initializer and
the separately named `splinterdb_durable_barrier_async()` are removed.

Blocking barriers retain the direct synchronous implementation, without a
notification allocation or task submission:

```c
int rc = splinterdb_durable_barrier(kvs, SPLINTERDB_NOTIFICATION_BLOCKING);
```

For tracked asynchronous completion:

```c
splinterdb_notification done;
splinterdb_notification_init_polling(&done, NULL);
int rc = splinterdb_durable_barrier(kvs, &done);
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
Nonblocking launch takes the in-memory log-group cut while protecting the live
log from rotation, then queues the slow durability wait. Its ticket pins the
log even if a checkpoint later retires that stream. A barrier whose cut races a
checkpoint also waits for the checkpoint's required superblock publication.

Without a WAL, launch captures the current memtable generation and queues a
checkpoint covering it. Checkpoint tasks can perform other queued work; they do
not wait for their own task to become quiescent. A barrier reached recursively
by that task pump requeues itself, bounding nested wait stack usage. Close
drains accepted barrier tasks before releasing database state.

Nonblocking launch allocates memory and takes internal locks, but performs no
device I/O. If allocation or enqueue fails, the call returns an error and leaves
the notification idle. Enqueue failure drops the ticket pin while the insert
epoch still protects the live log, so this cleanup cannot reclaim a retired
stream's allocation metadata. A failed completion is reported through the
notification and does not establish a durability guarantee.

Polling only observes completion. With no normal background workers, use
`splinterdb_notification_wait()` or another operation that performs task work
to make progress. Other foreground operations may execute queued barrier work,
so moving the durability wait off launch does not make every subsequent API
call nonblocking. Notification storage must remain alive until completion;
cross-process completion requires shared notification storage. A `NULL`
argument to `splinterdb_notification_init_polling(&done, NULL)` supplies optional
user data and does not select a completion mode.

Fire-and-forget is explicit:

```c
int rc = splinterdb_durable_barrier(kvs,
                                    SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET);
```

It reports submission errors, but gives the caller no way to observe a later
durability failure. Existing `splinterdb_optimize(..., NULL)` calls that require
fire-and-forget behavior must likewise use the named sentinel.

# Internal blocking notification ownership

Blocking optimize still uses a notification internally. It allocates that
object from the database heap, including shared memory when configured, and
starts with two references: one for the waiter and one for completion. The
waiter releases its reference after waiting, even if a task-system error makes
the wait return before completion. Completion releases its reference after
publishing the result and unlocking the notification. The last release
destroys the synchronization state and frees the allocation. If submission is
rejected, no completion is arranged, and the submitter releases both references.
Allocation and synchronization-initialization failures return errors without
leaving an internal notification behind.

This permits either order: completion may run before the waiting caller returns,
or after it has abandoned the wait. Accepted work is not canceled by a wait
error. Close drains pending work before releasing the database heap. As with
other database operations, callers must not close the database concurrently
with an active optimize call.

Caller-owned polling notifications retain their existing ownership rules and
are never freed by completion. The completion helper snapshots the ownership
flag before unlocking, so a caller that observes completion may immediately
deinitialize and free its object without racing a later helper access. Public
sentinels are translated at the API boundary; internal workers receive either
a real notification or `NULL` for no completion recipient.

## Benchmark harness

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
