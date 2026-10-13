# Invalid cache address errors

`robj/cache-invalid-addresses` is based on the compact caller-owned handle branch
`robj/cache-errors` at `2907e97`. Commit `d3a4488` makes synchronous and
asynchronous demand reads return `STATUS_BAD_PARAM` (`EINVAL`) for misaligned,
out-of-range, and unallocated disk addresses. This includes zero and the reserved
first extent. The output handle is cleared and no reference or I/O request is
created on these failures.

Nonblocking reads reject invalid addresses with the same error. A valid cache
miss still returns `STATUS_BUSY`. Both prefetch APIs drop invalid hints, allowing
the subsequent demand read to report an error. Extent prefetch also drops
addresses that are not extent-aligned.

The implementation retains the existing allocation invariant: a resident mapping
belongs to an allocated extent, and extent discard removes that mapping before
the extent becomes free. Allocation membership is checked on misses, including
nonblocking misses. Valid hits do not gain an allocator lookup or any stronger
atomic operations. Callers must retain the extent throughout acquisition and the
handle's lifetime.

Internal operations on existing cache mappings retain their assertions. This
change handles invalid addresses passed to cache reads and prefetch; it is not a
general malformed-database validator. For example, existing caller-level
assertions in btree access and allocator recovery can still abort on a corrupt
pointer before it reaches the cache.

## Correctness checks

The two new tests in `tests/unit/writeback_set_test.c` cover blocking and
nonblocking reads, asynchronous reads, and both prefetch APIs. Inputs include
misaligned resident and evicted addresses, zero and the first extent, addresses
at and beyond capacity, the largest aligned 64-bit address, `UINT64_MAX`, and
never-allocated and normally freed extents. Read hooks verify that no I/O is
submitted. Poisoned output handles must be cleared, references must be released,
and subsequent valid reads and allocations must succeed.

Validation passes on the committed implementation:

| Build | Cache/writeback | Public quick tests | Core error propagation |
|---|---:|---:|---:|
| Debug | 16 | 47 | 1 |
| Release | 16 | 47 | 1 |
| AddressSanitizer | 16 | — | — |

The asynchronous cache and prefetch-admission functional drivers also pass in
debug mode. This is 144 passing unit-test cases and two functional runs. ASan
uses `detect_leaks=0`; these results do not establish leak-sanitizer coverage.

## Benchmark method

The comparison isolates this change against `2907e97`, using the same frozen
query and insertion harness sources and compiler options for both libraries.
All database files are under `.cache/projects-20261011/benchmarks/` on ext4 backed
by `/dev/mapper/snafu-root_crypt`; I/O uses `O_DIRECT`. Query runs copy and fsync
the same immutable fixture before each process. CPUs 0–11 are selected.

The campaign's exclusive benchmark and machine locks serialize timed workloads
and exclude builds and correctness tests. Each initial comparison uses
parent/candidate/candidate/parent order. The query matrix covers point reads and
ranges of 1 and 10,000 keys at 1 and 8 threads. The resident fixture has 200,000
keys and a 128 MiB cache; the disk fixture has 2,000,000 keys and a 64 MiB cache.
Resident phases run for 20 seconds and disk phases for 10 seconds. Eight-thread
insertion uses 20 million records, separately with WAL disabled and enabled.
Query keys/values are 24/100 bytes; insertion keys/values are 16/16 bytes.
Both libraries use GCC 14.2 with `-O3 -ffast-math -flto`; both frozen harnesses
add `-march=native`. The machine is an Intel Core i7-12800H. Exact flags, commands,
source inventories, and binary hashes are recorded with the results.

To investigate the initial small losses and time drift, a second resident
comparison reverses the order to candidate/parent/parent/candidate and runs
point reads and 10,000-key ranges for 30 seconds per phase at both thread counts.
Insertion is repeated in the same reversed order.

## Read results

Mean throughput change against the parent (positive is faster; two samples per
version in each comparison):

| Workload | Resident, initial 20 s | Resident, reversed 30 s | Disk, 10 s |
|---|---:|---:|---:|
| Point, 1 thread | -1.48% | +3.05% | -0.05% |
| Point, 8 threads | -2.80% | +1.75% | +0.08% |
| Range 1, 1 thread | +0.05% | — | -0.11% |
| Range 1, 8 threads | -0.69% | — | -1.02% |
| Range 10,000, 1 thread | -2.26% | -0.10% | -0.07% |
| Range 10,000, 8 threads | -1.59% | +1.23% | +0.53% |

The initial resident losses did not reproduce in the reversed comparison.
The initial parent's single-thread point rate fell from 1.268M to 1.162M ops/s
between its first and last run, bracketing both candidate samples. CPU frequency
was not fixed (the machine uses the powersave governor), and these measurements
do not establish a small fixed cost or a speedup. All read phases reported zero
errors and validation violations; timed resident reads issued zero disk-read
bytes.

## Insertion results

Eight inserters, 20 million records per process:

| WAL | Initial throughput | Reversed throughput | Initial CPU efficiency | Reversed CPU efficiency |
|---|---:|---:|---:|---:|
| Off | -3.88% | +1.23% | +1.32% | -0.34% |
| On | -2.94% | -1.77% | +0.01% | +0.01% |

CPU efficiency is operations per process CPU second. The no-WAL loss did not
reproduce. WAL-enabled insertion retained a small throughput loss in both
comparisons, despite essentially unchanged CPU efficiency and bytes read/written
per record. Including close gives -3.09% initially and -1.58% in the reversed
comparison. The reversed WAL sample ranges overlap: parent 1.651–1.715M versus
candidate 1.638–1.668M ops/s. With two samples per version, those ranges are not
confidence intervals. A small WAL throughput cost remains unresolved; this
branch is **not established as performance-neutral**.

All 28 benchmark processes completed successfully. Every insertion process
inserted all 20 million records and closed successfully. This comparison measures
ordinary valid-database workloads, not repeated invalid-address failures and
their warning output.

## Evidence

[Benchmark artifacts](../benchmarks/results-20261013/cache-invalid-addresses/)
contain the raw samples, summaries, logs, archived runners/harnesses, exact
commands, and SHA-256 inventories. Candidate production sources match `d3a4488`;
the candidate library was built before that commit, so its embedded version
string still names the dirty parent. All 106 production/Makefile source hashes
were checked against the committed implementation.

[Test evidence](../benchmarks/results-20261013/cache-invalid-addresses/tests/)
records commands, counts, logs, and hashes for the debug, release, and ASan runs.
The campaign serialization audit found zero benchmark overlaps with other
benchmarks, builds, or tests.
## Main integration (2026-10-13)

Merged main `7bb5fab`, which contains the selected compact cache-error and
handoff implementation. To resolve the duplicated history from the squash,
the runtime and tests were taken from main, then the exact invalid-address
extension was reapplied to `cache.h`, `clockcache.c`, and `writeback_set_test.c`.
This retains main's acquiring-thread reference ownership, error paths,
prefetch admission protection and multilevel blob support.

Debug and release each pass 72 cases: 18 writeback/cache tests, 49 public API
tests, the core error-propagation test and four blob tests. Invalid synchronous,
nonblocking, asynchronous and prefetch inputs are covered together with
cross-thread claim/lock/unget and handles whose acquiring thread has exited.
Commands, logs and hashes are in
`.cache/projects-20261011/coordination/main-7bb5fab/cache-invalid-addresses/`.
The first core invocation used the wrong test-filter argument order and was
rejected; the corrected invocation passed. Earlier measurements above remain
historical; fresh integration measurements are tracked under
`benchmarks/main-7bb5fab/` in the campaign.

The deterministic prefetch-admission driver passes. One concurrent async
stress run at 32 MiB cache and 96% working set stalled: all 8,192 slots were
referenced, with no free or loading pages, and all eight readers were inside
blocking free-page acquisition before their next old-window release. Exact
serial repeats passed twice on both main and this branch. This does not
establish absence of a progress problem. With one/two-CPU scheduling, unchanged
main stalled three times and this branch stalled twice, all with the same
capacity-exhaustion pattern. Some pending misses already had valid mappings
published by other readers, but blocked allocation never rechecked lookup.
The additional unrestricted 64 MiB candidate control passed. The inherited
progress bug remains unfixed; captures and commands are retained in the
integration evidence's `stall/` directory. No cache allocation redesign is
included in this merge.

## Fresh benchmarks after main integration (2026-10-13)

Resident single-thread point throughput lost 4.60% in the broad screen and 4.54% in the nearby warmed repeat, with non-overlapping observed sample ranges. The repeated eight-thread point result (-3.64%) overlaps main, and short ranges (+0.03%) do not reproduce the earlier loss. Short-run WAL inserts lost 3.00%. Control drift still limits precise attribution; performance neutrality is not established. The inherited async allocation stall documented above is a separate correctness/progress limitation.

These are updated-feature comparisons against main `7bb5fab`, not an isolated
measurement of merge cost. Main controls drifted; observed range overlap is
descriptive, not a statistical confidence interval. Exact samples, build/source
hashes, commands, verification scope and fresh correctness evidence are in
[the integration archive](../benchmarks/results-20261013/main-7bb5fab/README.md).
