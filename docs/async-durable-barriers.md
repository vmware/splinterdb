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

# Original API validation

Before the sentinel API change, validation covered WAL enabled/disabled,
zero/one normal background workers, multiple pending barriers,
polling/blocking/fire-and-forget modes, close with pending work, reopen and data
verification, deterministic enqueue failure, device-barrier failure, ticket
reference cleanup and successful retry.
That revision's debug internal Splinter suite passed all 34 tests; the public
quick suite passed all 48 tests, with its notification matrix rerun after the
nested wait guard was added. Release libraries and both benchmark variants built.

The original deterministic nested-wait test covered both successful deferral
and an enqueue failure during deferral, checking single completion and balanced
log references. A separate device-barrier gate test parked a worker at the
device sync, then proved a second polling launch returned while both
notifications remained pending.

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

# Original API measurements

These measurements precede the sentinel API change. Candidate blocking used
the original one-argument `splinterdb_durable_barrier(kvs)`; candidate polling
used `splinterdb_durable_barrier_async(kvs, &notification)`. They establish the
original branch's behavior and are not measurements of the final two-argument
API or the new internally owned blocking-optimize notification.

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
The original async notification implementation was functionally validated and
made launch cheap; it did not promise to reduce the total durability work or its
CPU cost.


The original release-only ticket operation and queue-failure cleanup were
tested; polling launches performed no device I/O, and the deterministic
gated-device test proved launch returned while durability remained pending.
Completion still performs the same log/checkpoint work. With eight outstanding
operations, observed completion
latency includes application work and queueing, so it should not be compared to
blocking call latency as if both measured device service time.

Compact raw CSVs, manifests, exact measured harness sources and build provenance
are committed in `benchmarks/results-20261011/`. Full logs remain under
`.cache/projects-20261011/benchmarks/async-durable-barrier/` in the main checkout.
The original successfully verified scratch images were reclaimed after their
runs; subsequent controls reuse a single fresh scratch path, unlinking only
after successful crash-recovery verification. No database images are committed.

# Sentinel API validation (2026-10-12)

The final debug build includes all tests, tools and examples; release libraries
and the insert benchmark also build. The public quick suite passes 48 cases,
and the internal Splinter suite passes 34, including barrier enqueue/device
failures and nested task deferral. All nine optimize cases pass with private
and shared memory, including empty ranges, rejected ranges, fire-and-forget
with zero workers, overlapping requests, and reopen verification.

Six notification lifetime cases pass with private and shared memory. They
exercise both release orders, concurrent final releases, completion in a forked
process, and caller-owned objects freed before joining the completion thread.
Repeated early-wait and rejected-submission releases exceed a 1 MiB shared
heap's capacity if either path leaks. The early-wait test models that error at
the notification layer: the task queue has no existing hook to inject its
mutex error through the optimize facade.

AddressSanitizer passes the notification and optimize suites in both memory
modes, with allocation fault injection enabling a seventh notification case.
LeakSanitizer cannot run in this sandbox; these runs use
`ASAN_OPTIONS=detect_leaks=0:abort_on_error=1`. Exact build commands, test
summaries and logs are in
`benchmarks/results-notification-sentinels-20261012/validation.json` and its
`validation/` directory.

# Sentinel API measurements (2026-10-12)

These compare the previous notification API at `0ac64ac` with this change,
using matching GCC 14.2 release flags. All runs were serialized under the
campaign benchmark/machine locks with O_DIRECT database files on ext4 on
`/dev/mapper/snafu-root_crypt`. Each configuration ran old/new/new/old; this is
a small comparison, not a confidence bound on performance.

All 32 barrier runs passed crash recovery and tuple verification. They cover
blocking and tracked notifications at depth one, WAL on/off, and zero/one normal
workers, with 256 updates per barrier and 512 barriers without WAL or 2,048 with
WAL. Relative to the previous API, mean CPU per update changed by -4.6% to +0.3%
for blocking and -0.2% to +1.7% for tracked barriers. Tracked launch means remain
0.44–1.79 us. Wall-time shifts are substantial: WAL/one-worker blocking rates
span 75–269k updates/s before and 122–274k after. These results do not establish
tight throughput neutrality; the direct blocking barrier path has no new
notification allocation or task submission.

Blocking optimize was measured separately on a precompacted 20,000-key tree,
with 20,000 full-range calls per sample followed by reopen verification. All
eight runs passed. Repeated optimization still does substantial CPU work, so
this control does not isolate notification overhead.

| Normal workers | Previous mean call time | New mean call time | Change |
| --- | ---: | ---: | ---: |
| 0 | 1515.7 us | 1552.5 us | +2.42% |
| 1 | 1559.8 us | 1598.2 us | +2.46% |

CPU increased 2.42% and 2.74%, respectively. Zero-worker samples performed no
timed I/O; one-worker samples each read 120 KiB and wrote nothing. This small
regression is consistent across these samples and remains unattributed.

A further four-run control uses equal range bounds, which complete inline,
with one million calls per sample and no background workers. All runs passed
reopen verification and performed no timed I/O. Mean elapsed time increased
from 72.9 ns to 89.5 ns per call, about **16.6 ns**. Samples last only
0.07–0.09 seconds and include the timing loop. That increment measures the
extra wrapper work in this control; it does not explain the roughly 37–38 us
full-range shift. Matching compiler/options were checked in both saved build
configurations and archive metadata, so this change is not claimed to be
performance neutral for optimize.

Raw CSVs, manifests, exact measured harness snapshots, compiler provenance and
runtime-source hashes are retained in
`benchmarks/results-notification-sentinels-20261012/`.

# Integration with main on 2026-10-13

Merged `origin/main` at `7bb5fab305b9c6179e31f75998dabea833537a08`,
including caller-owned, fallible, transferable cache handles and multilevel
blobs. Both parents' tests remain present; new blob durability/recovery calls
use the two-argument barrier API with `NULL` for blocking completion. Notification
sentinel and ownership behavior is unchanged.

Debug and release builds passed. Each mode passed 119 tests: notification
ownership (6 each on private/shared heaps), optimize (9 each on private/shared
heaps), public quick tests (50), and core tests (39). The allocation-failure
notification case is skipped in ordinary builds. An ASan build with allocation
fault injection passed all 32 notification/optimize cases across both heap
modes (`detect_leaks=0:abort_on_error=1`). Core/public coverage includes async
barrier submission and device failures, nested waits, concurrent durability,
cache read errors, three-level blobs, transferred blob iterators, and crash
recovery of pointer trees.

Commands, binary/source hashes, test counts and raw logs are retained in the
collection's `coordination/main-7bb5fab/async-durable-barrier` directory. Previous
performance results above describe their original revisions; they do not
measure this merge.

## Fresh benchmarks after main integration (2026-10-13)

All 24 barrier runs completed crash/reopen/full-readback verification. Barrier throughput varied substantially, so no stable throughput improvement is claimed. The nearby ordinary-query repeat changed eight-thread point/short-range throughput -0.52%/-2.28%, with sample ranges overlapping main.

These are updated-feature comparisons against main `7bb5fab`, not an isolated
measurement of merge cost. Main controls drifted; observed range overlap is
descriptive, not a statistical confidence interval. Exact samples, build/source
hashes, commands, verification scope and fresh correctness evidence are in
[the integration archive](../benchmarks/results-20261013/main-7bb5fab/README.md).
