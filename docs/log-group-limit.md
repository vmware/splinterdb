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

Performance measurements and final validation results are recorded below after
the serialized disk benchmark campaign finishes.
