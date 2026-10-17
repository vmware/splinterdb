# Async-load production review

Source: `sources/async-load/src/clockcache.c`, SHA256
`a26ce40399a60f76f8559eb82703f688bf0bd71662ce93d9ad4232f7451c107e`.

No correctness blocker found in the final diff versus the original invalid-
address branch. This is a separate review pass by the variant's preparer,
not an independent-author review. The exact source already passed 118 unit
executions, debug prefetch-admission, and formatting; performance acceptance
still depends on root's final measurements.

The cold diagnostic helper preserves lookup-check order and rejects addresses
before indexing the directory. Synchronous miss validation remains inline,
before either admission or a nonblocking BUSY return. The only semantic code
movement is the async allocation check from the parent dispatcher into the
existing disk-load subroutine, immediately after `async_begin`.

- Fresh invalid miss: parent has validated geometry and set the thread ID; the
  child validates allocation before any entry reservation, reference acquisition,
  I/O-state setup or callback registration. It sets BAD_PARAM and succeeded=true.
  Child and parent complete immediately; the top-level retry loop exits with
  its initialized null result. The cache result wrapper clears the caller's
  handle. No callback is invoked.
- Admission race: the child returns succeeded=false without a page; the outer
  loop initializes both existing coroutine levels for another lookup. A later
  miss validates allocation again.
- Suspended I/O: `async_begin` jumps to the existing await continuation on
  resumption, bypassing initial validation/reservation/setup. The original
  parent-level check was similarly bypassed on resume. The caller's existing
  contract keeps the extent allocated throughout the request and handle life.
- I/O setup, append and completion failures retain their previous status,
  load-publication and reference cleanup. Successful callbacks and publication
  are unchanged. No state layout, coroutine depth or handle ownership changes.

`async-load-production-review.json` records the reviewed cases and validation
reference. No new test or benchmark ran during this review.

## Bounded async real-I/O churn coverage

Use the existing frozen `harness/controlled-final/cache_api_probe` directly
through `job.run`, under `machine.lock`, after the timing suites finish. The
runner's CLI matrix intentionally excludes churn; the frozen C binary already
supports it, so no harness edits are required.

Recommended first smoke: exact async-load release library, 64 MiB cache,
128 MiB working set, one worker on CPU2, controller CPU12, two-second warmup,
five-second measured phase, `churn async`. Example command array:

```
[PROBE, UNIQUE_REAL_DISK_DB, "64", "1", "5", "churn", "async"]
```

Environment: `LD_LIBRARY_PATH=.../builds/async-load/release/lib`,
`CACHE_PROBE_CONTROL_CPU=12`, `CACHE_PROBE_WORKER_CPUS=2`,
`CACHE_PROBE_WARMUP_SECONDS=2`, `CACHE_PROBE_PERF=0`,
`CACHE_PROBE_DISK_RATIO=16`, `CACHE_PROBE_ADDRESS_LAYOUT=spread`.
Use a 180-second process timeout to bound setup/cleanup and unexpected progress
failure. Record the exact executable/library/source hashes. Preserve failed
scratch/logs; delete only this smoke's own database after successful completion.

A second five-second smoke with six workers pinned to `0,2,4,6,8,10` exercises
concurrent cache admission, eviction and callback completion without varying
SMT placement. Keep the same 64 MiB cache and controller CPU. This is optional
additional concurrency coverage, not necessary for a new throughput claim.

Expected evidence: exit zero; zero errors; positive operations; positive async
waits (warmup plus measured interval); positive measured read_bytes; no measured
data writes; valid per-page identity/next-pointer/status checks; each handle
released once and final cache eviction succeeds. WORKER lines must show the
requested CPUs and deterministic thread IDs. Positive async waits plus real
read traffic establish that this covered miss submission/completion rather
than only cache hits. If waits are unexpectedly zero, retain the result and
investigate before claiming asynchronous completion coverage.

This is a correctness/progress smoke. Its elapsed throughput combines device
latency, cache replacement, batching, hit ratio, polling and package state;
short samples cannot establish a 2–3% CPU regression or disk-neutrality claim.
The shared device's other I/O is another uncontrolled influence. The C probe
allows measured I/O in churn mode, but root should inspect its reported values
and keep all identity/error/progress requirements. An inherited cache-progress
stall, if observed, must be reported separately rather than hidden by reducing
the sample or calling the variant validated.
