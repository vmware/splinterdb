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
thread cuts that group while its log is protected by the insertion epoch, then
waits after releasing the memtable insertion lock. Other writers continue. The
driver coalesces further threshold crossings and drains them before giving up
responsibility, including when no further insert is called. A sequentially
consistent target/active handshake prevents a crossing from being stranded
between the driver's final check and a writer's decision to leave work to it.
The original durability-ticket pin keeps the log alive if a checkpoint retires
its owner during this drain.

A competing manual barrier may already have cut the requested group. Automatic
durability joins that group rather than cutting a partially filled successor.
An I/O failure is returned to the driving insert even though the update is
already visible. The outstanding target remains eligible for a later retry.
This is not a promise that every successful insert is individually durable;
applications that require that guarantee must still use an explicit barrier.

## Validation

The functional log suite covers page and blob thresholds, disabled accounting,
concurrent manual and automatic cuts, and recovery of the automatically durable
prefix without a manual log barrier. A deterministic interleaving pauses the
automatic cutter, lets a manual cutter advance the frontier, finishes a writer
in the successor, and retires the log owner before the original driver drains
the remaining target. This checks peer progress, bounded cuts, the final
crossing, and lifetime protection together.

A second deterministic test publishes a conservative reservation after sealing
has drained the groups, then retires the owner while that publication is held.
The original implementation aborted in `shard_log_deinit()`. Reservation checks
now run when the final handle pin is released, where no operation can still be
using the log; the regression passes.

The internal Splinter suite includes an injected device-barrier failure: the
insert returns the error, its value is visible, and the next insert retries
durability successfully. A test that inspects two large values in mutable log
staging explicitly disables automatic rotation.

Final debug validation passed the 500,000-record functional log suite, all 32
internal Splinter tests, and a process-exit recovery test. That test exits the
writer without calling close, checkpoint, or an explicit barrier, then reopens
and verifies every value. It covers a one-page threshold with small records and
the default 64-page threshold with blobs larger than 64 pages. Debug and release
builds passed.

Performance measurements and final validation results are recorded below after
the serialized disk benchmark campaign finishes.
