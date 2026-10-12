# Automatic log-group durability

`splinterdb_config.log_group_size_pages` sets a best-effort durability threshold
for the current write-ahead log group. Zero selects `MAX_THREADS` pages (64 in
this build); `UINT64_MAX` disables automatic barriers. This policy is separate
from the existing log-size checkpoint policy. It has no effect with logging
disabled, and it does not change the on-disk format.

The accounting counts each used log staging page once, plus the rounded-up
logical page count of every blob recorded in that group. Blob pointer-tree
metadata is not included. Shared blob references are counted for each record,
so the threshold is an estimate rather than a measurement of unique allocation.
Concurrent prepared writes and individual large blobs can overshoot arbitrarily.

Crossing the threshold records the group that needs durability. One inserting
thread claims that target, cuts the group while its log is protected by the
insertion epoch, then waits after releasing the memtable insertion lock. Other
writers continue; a later group crossing selects its own driver. Each driver
waits only for its fixed target and cannot chase an indefinitely growing stream
of later writes. A highest-claimed-ticket atomic ensures that a crossing either
acquires a responsible driver or observes one covering that prefix. Every
successful begin pins the log until its corresponding wait completes, including
if a checkpoint retires the owner meanwhile.

A competing manual barrier may already have cut the requested group. Automatic
durability joins that group rather than cutting a partially filled successor.
An I/O failure is returned to the driving insert even though the update is
already visible. A failed latest owner clears its claim to permit a later retry;
if a newer driver has claimed a later target, it retains responsibility for the
prefix. The threshold target itself is retained.
This is not a promise that every successful insert is individually durable;
applications that require that guarantee must still use an explicit barrier.

## Validation

The functional log suite covers page and blob thresholds, disabled accounting,
concurrent manual and automatic cuts, and recovery of the automatically durable
prefix without a manual log barrier. Deterministic interleavings cover competing
manual cuts, distinct automatic owners for successive groups, a blocked later
group, and retirement while durability tickets remain live. A group's driver
must be able to return while the successor is still blocked.

The internal Splinter suite includes an injected device-barrier failure: the
insert returns the error, its value is visible, and the next insert retries
durability successfully. A test that inspects two large values in mutable log
staging explicitly disables automatic rotation.

Final validation passed the 500,000-record functional log suite, all 32
internal Splinter tests, and a process-exit recovery test. That test exits the
writer without calling close, checkpoint, or an explicit barrier, then reopens
and verifies every value. It covers a one-page threshold with small records and
the default 64-page threshold with blobs larger than 64 pages. Debug and release
builds passed. The bounded-wait regression was also run against the preceding
coalescing implementation (`ca179b2`), where it failed because the first driver
chased a peer's blocked successor. It passes with fixed-ticket ownership.

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
