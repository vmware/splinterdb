Schedule automatic durable barriers when the current WAL group reaches a
configurable page threshold. `splinterdb_config.log_group_size_pages` defaults
to 1024 pages (4 MiB at the default 4 KiB page size). Zero selects that default;
`UINT64_MAX` disables the policy. Without this policy, applications that do not
request barriers can accumulate a large undurable log suffix.

After publishing an update, the core submits the existing fire-and-forget
durable barrier and releases its submission guard without waiting for I/O.
The existing task queue, log tickets, checkpoint dependencies and shutdown
drain handle completion. Used log pages and rounded logical blob pages count
toward the threshold. Concurrent writers and large blobs may overshoot; blob
pointer metadata is excluded. No on-disk format change is required.

This is a best-effort threshold. Queue-pressure assistance can execute work on
foreground threads, and background completion is required before records are
durable. Failures emit warnings without turning an already-visible update into
a failed insert. A later threshold crossing or explicit barrier can retry the
pending prefix. Applications needing a definite durable frontier must still
wait for an explicit barrier.

Regression coverage includes failed submission, failed durability and retry,
overlapping automatic/manual cuts, and inserts progressing while an earlier
barrier is blocked. See the [policy description](log-group-limit.md) for the
accounting and completion contracts.
