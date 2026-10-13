# Integration with origin/main

`robj/cache-handoff` merges `origin/main` at
`7d607b118b1f606c51430759adc75b069423afae` into the selected compact-handle
branch at `23d26d165923d174e4d5d5ea2a4774ece697b9aa`. A merge preserves the
complete `cache-errors` ancestry and the existing experimental evidence.

The upstream change adds multi-level blobs, configurable descriptor sizes, and
wide B-tree counters. Its new pointer-page reads and allocations now use
caller-owned handles. Blob iterators retain a separate acquisition receipt for
each data and pointer page, and traversal returns the actual cache-read error.
The upstream log-prefix recovery cutoff and the cache branch's suppression of
clean allocation-state publication after failed reclamation are both retained.

## Correctness

Fresh builds use `BUILD_ROOT=build-mainmerge`; earlier measured builds remain
unchanged. Validation passed:

- 239 debug unit cases: 203 in the combined unit executable, with three existing
  skips, and all 36 separate Splinter cases.
- 65 release cases: all 16 writeback-set and 49 public quick tests.
- 69 AddressSanitizer cases: the same 65 cases plus four blob cases. Address
  checks were enabled; leak detection was disabled under the sandbox's ptrace
  restrictions.
- Five functional drivers: deterministic prefetch admission, asynchronous
  cache operations, B-tree operations, the 500,000-entry log test, and the
  100,000-record shadow-verified database test.
- Formatting and whitespace checks for the integrated source and tests.

The height-three blob regression now injects `STATUS_TIMEDOUT` at five addresses:
both pointer levels, a lower pointer page after crossing a subtree boundary,
and the two data pages on either side. It checks exact error propagation,
released references, and successful retry. It also transfers a live iterator
holding two pointer handles and a data handle to another registered thread,
then advances across the subtree boundary and releases all references there.
The parent branches' cache-error, handoff, large-value, and crash-recovery tests
remain present.

An initial functional B-tree invocation used a 64 MiB cache even though its
rough-iterator setup retains eight 24 MiB memtables before packing them. It
timed out; the captured stack shows the allocation path searching for a free
cache page. The corrected 512 MiB invocation passed all sections. This required
no production-code change. Initial coordinator-argument and scratch-library-path
setup failures are also preserved with their successful corrected commands.

Exact commands, results, logs, and the captured stack are in
[the correctness archive](../benchmarks/results-20261012/cache-handoff-mainmerge/tests/summary.json).

## Performance

The before/after comparison measures this whole merge, including upstream blob
and B-tree changes; it does not isolate the cache API's cost. The before binary
is the frozen `23d26d1` release build. Both builds use identical compiler/linker
flags and the same frozen public benchmark source. Immutable fixtures are
copied and fsynced before each run. Database files use `O_DIRECT` on ext4 backed
by `/dev/mapper/snafu-root_crypt`. The shared benchmark and machine locks exclude
overlapping benchmark, build, and test jobs.

The initial eight processes ran in before/merged/merged/before order within
each regime. Resident phases lasted 20 seconds; disk phases lasted 10 seconds.
All runs completed with zero errors and verification violations. Resident timed
phases performed zero device reads. Mean throughput change, merged versus
before (positive means faster):

| Workload | Resident | Disk |
| --- | ---: | ---: |
| Point, 1 thread | -2.07% | — |
| Point, 8 threads | -5.92% | -0.45% |
| Range 1, 1 thread | -1.46% | — |
| Range 1, 8 threads | -4.98% | -0.36% |
| Range 10,000, 8 threads | — | -1.21% |

Every disk sample range overlaps its control. The resident point/8 control
ranged from 5.563M to 5.974M queries/s, versus 5.403M to 5.450M after the merge.
That loss and control drift prompted a separate reversed-order point/8 repeat
using the same frozen executables and fixture.

The repeat ran merged/before/before/merged, again with 20-second phases. It
measured **+2.10% throughput and +2.10% CPU efficiency** after the merge:
merged samples ranged from 4.908M to 5.081M queries/s, versus 4.651M to 5.132M
before. The initial point/8 slowdown did not reproduce, and absolute control
throughput also drifted substantially. The two campaigns remain separate;
they do not establish a fixed regression or improvement, or universal
performance neutrality. All twelve processes passed, covering 32 timed phases
and twelve warmup phases.

These are ordinary queries with 24-byte keys and 100-byte values. They do not
measure throughput of the newly supported large-blob paths. The complete
[benchmark archive](../benchmarks/results-20261012/cache-handoff-mainmerge/README.md)
records exact samples, CPU efficiency, commands, and source/build hashes.
