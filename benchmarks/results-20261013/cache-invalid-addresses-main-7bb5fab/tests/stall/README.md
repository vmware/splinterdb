# Concurrent cache-test progress failure during main integration

The 32 MiB candidate debug async cache suite stalled at the **eight-reader,
96% working-set phase**, with its logged progress at 51%. The earlier 80%
phase completed. The original process was identified by its exact executable
and database path, inspected twice, then terminated with SIGTERM; its initial
validation remains a failed run. No source changes were made for diagnosis.

## Captured state

All 8,192 cache pages held read references; there were no free, unreferenced,
loading, or pinned pages. The snapshot contained 62,889 references. Each of
the eight reader threads held 7,840 completed handles, with 168 additional
async contexts ready to resume in total. Every reader was blocked inside
`clockcache_get_free_page()` from `clockcache_get_from_disk_async()`, while
the main thread joined them and the I/O cleaner waited for completions.
Reader batch positions ranged from 8,192 to 8,544; their independently moving
retained windows consumed the entire cache. No reader could return from its
current allocation to advance and release its next old batch.

This is not the historical stranded-free-batch condition. The current
allocator releases a batch reservation before returning, and these snapshots
have zero free pages to reclaim. Active batch locks are held by the spinning
allocation calls, rather than by idle or exited owners.

## Baseline comparison

Unmodified main `7bb5fab305b9c6179e31f75998dabea833537a08` and the merged
candidate passed two serial 32 MiB/1 GiB async suites each, in ABBA order.
Those successes did not eliminate the original progress failure.

To expose scheduling skew without altering source or resizing the cache,
three baseline tests used affinity to one CPU or two CPUs. **All three
baseline runs reproduced the stall**, during their 80% working-set phase.
Two corresponding candidate controls (one/two CPUs) also stalled. Every
stalled control was inspected and terminated after its 20-second deadline.
All had 8,192 referenced pages and no free/loading pages.

The baseline snapshots additionally show 4/7/6 blocked requests whose target
addresses already had valid cache mappings. The allocation path performs its
lookup before trying to acquire a free slot, then blocks waiting for that slot
without rechecking whether another loader published the target address. Slow
readers therefore remain blocked on redundant allocations while retaining
their old working sets. Faster readers cannot allocate their next absent
page because those old sets fill the cache. An async call is synchronously
blocked here, so the caller cannot resume completed contexts or release its
next old batch. The candidate controls show the same mechanism.

The requested additional candidate control at 64 MiB/1 GiB, without restricted
affinity, completed successfully. This provides additional coverage; it does
not repair or erase the reproduced 32 MiB progress failure.

The allocator function `clockcache_get_free_page()` and complete functional
cache-test source are byte-identical between current main and candidate;
`source-comparison.json` records hashes. Together with baseline reproduction,
this identifies an inherited async allocation/progress problem rather than a
new invalid-address validation failure. Its frequency under ordinary loads
was not measured, and the implementation remains unfixed. Fixing allocation
retry/backpressure or the test's overcommitted-window protocol is a separate
design task, beyond updating these project branches.

## Evidence

- `symbolic-stacks.log`: original thread stacks.
- `cache-state-with-contexts.log`: original cache counts and reader contexts.
- `initial-termination.json`: exact original process identity and termination.
- `comparison.json`: four unrestricted 32 MiB controls, commands and hashes.
- `skew-comparison.json`: five stalled affinity controls and successful 64 MiB control.
- `skew-*-stacks-state.log`: complete baseline/candidate stack and state captures.
- `summary.json`: consolidated controls and parsed snapshots.
- `compare.py`, `compare-skew.py`, `inspect.gdb`: reproduction/capture scripts.

All database files remain in per-run scratch directories on the workspace
filesystem. Test jobs used the shared `run_job.py` machine lock. These are
correctness/progress controls, not throughput benchmarks.
