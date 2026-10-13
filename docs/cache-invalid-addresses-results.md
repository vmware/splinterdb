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
