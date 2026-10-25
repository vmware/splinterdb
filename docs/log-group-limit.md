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
