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

The branch includes main `fc0bc8f7` (filtered ranges). See the
[policy and validation report](log-group-limit.md) for test coverage,
performance results and limitations.

Validation:

- Debug and release each pass 44 core tests, 56 public API tests, the
  process-exit recovery test with small records and both blob layouts, and the
  functional log suite's 500,000-record replay. Coverage includes failed
  submission, failed durability and retry, overlapping automatic/manual cuts,
  and inserts progressing while an earlier barrier is blocked.
- Full source formatting and PR whitespace checks pass. The complete CI
  compiler, sanitizer and stress matrix remains separate.
- Twelve fresh serialized `O_DIRECT` runs on real disk compare the new default,
  explicit 1024 and current main. Default insertion throughput changes -0.32%
  with one writer and -0.81% with eight; including close changes -0.08% and
  -2.02%. Default and explicit 1024 perform similarly. These short samples
  show no substantial throughput regression, but do not establish exact
  overhead or tail-latency neutrality. Main has no automatic durability policy.

[Commands, hashes, raw results and limitations](../benchmarks/results-20261014/log-group-limit-pr/README.md)
