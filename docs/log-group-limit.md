# Automatic log-group durability

`splinterdb_config.log_group_size_pages` sets a best-effort durability threshold
for the current write-ahead log group. Zero selects 1024 pages (4 MiB with
4 KiB pages); `UINT64_MAX` disables automatic barriers. This policy is separate
from the existing log-size checkpoint policy. It has no effect with logging
disabled, and it does not change the on-disk format.

The accounting counts each used log staging page once, plus the rounded-up
logical page count of every blob recorded in that group. Blob pointer-tree
metadata is not included. Shared blob references are counted for each record,
so the threshold is an estimate rather than a measurement of unique allocation.
Concurrent prepared writes and individual large blobs can overshoot arbitrarily.

After each logged update, the insertion path checks a cheap threshold hint.
One thread claims a short submission guard, rechecks the current log under its
insert epoch, and submits the existing fire-and-forget durable barrier. The
guard is released after submission, without waiting for durability. The task
uses the existing log-ticket pin, ordered-prefix wait, checkpoint-publication
wait, and cleanup. There is no new background scheduler or retry engine.

The recheck prevents concurrent automatic observers from repeatedly cutting
mostly empty successors. An explicit barrier can still race the hint and cause
a redundant cut. A successor that fills while submission holds the guard is
reconsidered on a later insert. This is deliberately a best-effort threshold,
not a hard bound on outstanding bytes, crash loss, or elapsed durability time.
Raw internal `log_write()` only appends; the database core owns this policy.

Submission and fire-and-forget completion failures emit warnings. A visible
update remains a successful insert. A later size-triggered or explicit barrier
retries the incomplete prefix; failed completion does not schedule a retry by
itself. If submission failed before cutting, the current oversized group remains
eligible on the next insert. If cutting succeeded, retry waits for the next
threshold crossing or an explicit barrier.

Normal background workers execute the queued work. Existing queue-pressure
assistance can also run it on an inserting thread; with no normal workers,
foreground assistance is required. Accordingly, asynchronous submission removes
the mandatory durability wait from each crossing but does not guarantee bounded
insert latency. Close drains accepted tasks through the existing mechanism.
Applications requiring a definite durable frontier must still explicitly wait
for a barrier.

## Validation of the asynchronous policy

The functional log suite checks page/blob threshold accounting, the disabled
option, and recovery after acting on the hint. Core tests cover rejected
submission and retry, a device-barrier failure and subsequent retry, and two
successive automatic cuts while the first device barrier is blocked. The last
test also overlaps an explicit asynchronous barrier with automatic cuts.
Existing async-barrier tests cover queue rejection, ticket lifetime, checkpoint
publication, and notification completion during close.

The process-exit regression covers small records, flat blobs, and multilevel
blobs. It drains only already-enqueued tasks before exiting without close,
checkpoint, or a new explicit barrier, then reopens and verifies every value.
An exit before asynchronous completion is permitted to lose the pending suffix.

## Asynchronous performance on current main

Compared on main `fc0bc8f7`, the asynchronous 64-page policy improves insertion
throughput by **39.9% with one writer and 32.6% with eight writers** over the
synchronous policy. Including shutdown drain preserves these gains.

| Writers | Synchronous 64 pages vs main | Asynchronous 64 pages vs main | Asynchronous 1024 pages vs main |
| ---: | ---: | ---: | ---: |
| 1 | -30.5% | -2.7% | -1.3% |
| 8 | -31.7% | -9.4% | +0.6% |

These are means of three short samples per candidate on real disk, with
interleaved main controls. Main provides no size-triggered durability. Control
drift limits precise small differences: the disabled-policy follow-up reports
-6.95% in its first ABBA round and -0.05% in its second, steadier round.
The former 64-page default retains a measured eight-writer cost; the new
1024-page default is close to controls in this workload. These measurements
selected 1024 explicitly, before the default changed. All 50 insertion runs
passed API and clean-close checks, with correctness/recovery tested separately.

[Results, commands, hashes, and limitations](../benchmarks/results-20261013/log-async-main-fc0bc8f7/performance/README.txt)

## PR validation with the 1024-page default

The default is now 1024 in both public configuration normalization and internal
log configuration. Debug and release each passed 101 unit cases (44 core,
56 public API, and one process-exit test covering three data shapes), plus the
functional log suite with 500,000/500,000 records replayed. The process-exit test
uses blobs large enough to cross the new default and verifies every recovered
value. Full source formatting and PR whitespace checks passed.

A fresh 12-run screen compares the new default, explicit 1024, and main
`fc0bc8f7`. Each policy has two ten-second samples per writer count, with main
controls bracketing the candidates. The same harness, real-disk `O_DIRECT`
storage, and serialized execution are used for all runs.

| Writers | Default inserts/s | Change vs main | Change including close |
| ---: | ---: | ---: | ---: |
| 1 | 860,522 | -0.32% | -0.08% |
| 8 | 1,810,417 | -0.81% | -2.02% |

Explicit 1024 changes throughput by -0.24% and -0.01% against main. The default
and explicit configurations perform similarly. All runs pass API, insert-count,
and clean-close checks; recovery is validated separately. These short samples
do not establish exact overhead or tail-latency neutrality. At eight writers,
close time averages 1.36 seconds for the default and 1.23 seconds for main;
individual main closes range from 0.71 to 1.74 seconds. Main provides no
size-triggered durability. The full CI matrix remains separate.

[Default-change evidence and reproduction details](../benchmarks/results-20261014/log-group-limit-pr/README.md)

## Historical synchronous implementation

The following measurements and validation describe the superseded synchronous
policy. They are retained for provenance and do not measure the asynchronous
implementation. Its fixed-target driver waited for durability on the inserting
thread and returned automatic barrier errors after the update was visible.

## Performance

Measured implementation: `7c8cb3d`; baseline: `db3047a`. The candidate's
same-source insert harness was linked against both libraries (the baseline
build defines `KVBENCH_NO_LOG_GROUP_LIMIT`). On an i7-12800H with GCC 14.2,
all database files used `O_DIRECT` on ext4 on `/dev/mapper/snafu-root_crypt`.
A process lock serialized every benchmark and excluded our builds/tests.

Each cell is the mean of two six-second closed-loop runs, in forward/reverse
policy order. Configuration: 256 MiB cache, 16 GiB logical disk, 8 MiB
memtables, one memtable and three normal workers, 16-byte keys and values,
WAL enabled, no automatic checkpoints, CPU affinity 0-11. All 16 runs and
closes succeeded. These are throughput comparisons, not offered-rate latency
measurements or long-duration durability guarantees.

| Writers | Policy | Inserts/s | Change | p99 ms | p99.9 ms |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 1024pages | 865,775 | -5.5% | 0.019 | 0.037 |
| 1 | baseline | 915,999 | +0.0% | 0.019 | 0.031 |
| 1 | default64 | 538,553 | -41.2% | 0.018 | 0.058 |
| 1 | disabled | 915,142 | -0.1% | 0.018 | 0.035 |
| 8 | 1024pages | 2,465,351 | -3.6% | 0.055 | 0.186 |
| 8 | baseline | 2,558,495 | +0.0% | 0.055 | 0.180 |
| 8 | default64 | 1,262,088 | -50.7% | 0.023 | 0.097 |
| 8 | disabled | 2,533,143 | -1.0% | 0.056 | 0.184 |

The disabled policy is within 1% of baseline in this sample. Enabling the
requested 64-page default materially reduces throughput, by 41% with one
writer and 51% with eight writers; it adds frequent device durability work
that the baseline does not perform. A 1,024-page policy costs about 4-5%.
These results do not establish performance neutrality for the new default.
Lower p99 in some slower configurations does not offset the throughput cost;
rare durability/compaction stalls may lie beyond that percentile.

Raw per-run and time-series CSVs, executable hashes, commands, and machine
information are retained in
[`benchmarks/results-20261011`](../benchmarks/results-20261011).
The earlier single-owner loop was superseded because it could follow a
continuously advancing target. Its measurements are not used in this table.

## Integration with main on 2026-10-13

Merged `origin/main` at `7bb5fab305b9c6179e31f75998dabea833537a08`,
including caller-owned, fallible, transferable cache handles and multilevel
blobs. Configuration merges retain both the group threshold and the blob
descriptor-size limit. Threshold behavior, logical blob-page accounting, and
fixed-target automatic durability ownership remain unchanged.

The process-exit regression now also writes four 8 MiB blobs with a 128-byte
descriptor limit, forcing blob pointer pages. The child exits without close,
checkpoint, or a manual barrier; reopening verifies every value. The existing
small-record and flat-blob cases remain covered.

Debug and release each passed 87 unit cases (37 core, 49 public quick, and
one process-exit regression with three data shapes). Each mode also passed
the functional log suite, including recovery of 500,000/500,000 entries.
Debug additionally exercises deterministic bounded automatic waits, competing
manual cuts, and retirement with live durability tickets. Core coverage
includes injected durability failure/retry, cache read failures, large blob
trees and iterator handoff. Both builds and the release benchmark targets
succeeded. Commands, source/binary hashes, counts and raw logs are retained
in the collection's `coordination/main-7bb5fab/log-group-limit` directory.

The earlier performance table describes the pre-merge revision. It does not
measure this integration or establish that the 64-page default is neutral.

## Fresh benchmarks after main integration (2026-10-13)

The default 64-page threshold reduced short-run WAL insertion throughput by 42.70% versus main without automatic barriers. This combines added durability with mandatory foreground waits; it does not isolate unavoidable storage cost. With the threshold disabled, throughput changed -1.34% and sample ranges overlapped main. The merge also validates automatic durability of multilevel-blob pointer pages.

These are updated-feature comparisons against main `7bb5fab`, not an isolated
measurement of merge cost. Main controls drifted; observed range overlap is
descriptive, not a statistical confidence interval. Exact samples, build/source
hashes, commands, verification scope and fresh correctness evidence are in
[the integration archive](../benchmarks/results-20261013/main-7bb5fab-log-group-limit/README.md).

## Integration with main after filtered ranges

Merged main `fc0bc8f7` before replacing the synchronous policy. The merge commit
`9bc5ebf4` retains the old policy on the same current-main source for controlled
synchronous/asynchronous comparisons. Historical log evidence that collided
with archives already on main was preserved byte-for-byte under
`benchmarks/results-20261013/main-7bb5fab-log-group-limit`.
