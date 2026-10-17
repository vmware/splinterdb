# Production cache probe v2: phase hardware counters

This separately named v2 preserves the frozen v1 source and binary in the
parent directory. It adds per-worker hardware counters at phase boundaries,
with no new instructions inside the 64-operation batch. The source and builder
are prepared and reviewed; root owns compilation, smoke tests and measurement.

Build under the shared campaign lock using this directory's `build_probe.py`
with the same main headers and baseline shared library as v1. The builder
refuses to replace existing outputs and emits source/object/binary/header
hashes. Use the same resulting executable with each selected library through
`LD_LIBRARY_PATH`. Run it with v1's CLI and unique real-disk scratch files.
Do not compare v2 throughput directly with v1: adding instrumentation can
change the worker's compiled layout even when the hot source loop is unchanged.

By default each worker opens a four-event perf group before it reports ready,
while the group is disabled. The events count only that worker and userspace
(`pid=0`, `cpu=-1`, `exclude_kernel=1`, `exclude_hv=1`, no inheritance):

- CPU cycles
- Retired instructions
- Retired branches
- Branch misses

The worker resets/enables the group after resetting the measured phase's
input state, immediately before its first measured batch. On stopping, it
disables the group before recording its finish timestamp, reading the group,
closing its descriptors and deregistering. No setup, warmup, other threads,
control-thread sleeps, joins or teardown is counted. The few userspace
instructions around enable/disable and normal 64-operation phase polling are
included. Counter values describe the entire consumer loop, including checks,
random-address selection, checksum and cache get/unget calls, not only cache
implementation instructions. Every measured operation contributes to `ops`,
so divide raw group counts by `ops` for per-operation comparisons.

The group is pinned and event IDs are checked at readout. A missing event,
unschedulable group, short read, zero running time or any enabled/running time
difference rejects the sample. No multiplex correction or extrapolation is
performed. Thread totals are summed after joins; each worker is checked
individually before aggregation. `perf_enabled_ns` and `perf_running_ns` are
summed perf event time accounting and can exceed wall time for multiple
threads. They are not CPU-cycle denominators.

Pin runs to homogeneous CPUs; the intended single-worker comparison uses
`taskset -c 2` on the host's Intel P-core. Generic hardware events select the
CPU's PMU. Do not launch on mixed P/E CPUs or enable whole-process perf hardware
counters concurrently: those can change event scheduling and PMU semantics.
Use identical affinity and event sets across variants.

Set `CACHE_PROBE_PERF=0` to disable counters using the exact same executable;
all counter columns become zero. Only `0` and `1` are accepted. This control
can check whether enabling the PMU changes throughput; it cannot remove the
compiled instrumentation's effect on layout. Permission or PMU failures reject
the run rather than silently turning counters off. If access is unavailable,
root can continue the already functioning v1 throughput campaign.

The CSV appends `perf_enabled`, `perf_cycles`, `perf_instructions`,
`perf_branches`, `perf_branch_misses`, `perf_enabled_ns`, `perf_running_ns`.
All v1 correctness, no-storage-I/O, major-fault, resident working set, setup,
and asynchronous-completion checks remain. Compare instructions/op to detect
added dynamic work, branches/op and misses/op for control-flow cost, and
cycles/op or IPC for residual execution effects. A cycles-only regression
does not by itself prove frontend layout; pair it with code review or targeted
frontend counters in a separately named probe version.
