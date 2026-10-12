# Cross-thread page handles

This branch is stacked on project 1, `robj/cache-errors`, including its final
prefetch admission repair. Compare performance against its compact receipt
implementation to isolate the handoff change.

## Implementation and contract

Every successful synchronous get, completed async get, and allocation produces a
caller-owned `page_handle`. The handle records the cache read-reference stripe
that was incremented. Releasing or upgrading the handle uses
that recorded stripe, so its owner may move it to another registered thread with
ordinary synchronization. The acquiring thread may exit and its registration ID
may be reused while the handle remains live. A copied handle does not acquire an
extra reference: ownership still transfers and the reference is released once.

Project 1 supplies caller-owned storage for both `cache_get()` and
`cache_alloc()`. This change adds the acquisition stripe to each successful
receipt. Sync and async gets never modify another user's receipt.

Public iterators may be advanced, reversed, and destroyed on a different thread;
only one thread may access an iterator at a time and it must be destroyed before
closing its database. The existing warning about a live mutable-leaf iterator
blocking an insert remains applicable.

The retained handle grows from 8 to 16 bytes on this platform: a shared identity
pointer plus the acquisition reference-stripe receipt. Cache entries keep a
shared 16-byte identity and remain 64 bytes. A tested 8-byte alternative stored
the stripe in the pointer's low two bits; its mixed measurements did not justify
retaining the extra encoding. That experiment is preserved in commit `7c65f4b`
and the result archive, then reverted in `c08841b`.
Reference increments/decrements already used atomic
read-modify-write operations, and the stripe loads were already sequentially
consistent. Their operations and ordering are unchanged. Lock upgrade waits for
all references except the single reference represented by the given handle,
including extra references in the same stripe after registration-ID reuse.

## Validation

The 16-byte and tagged 8-byte receipt layouts passed the following checks on
2026-10-11 (local time).
The full debug build passed, along with all 16 writeback-set tests, all 48
public quick tests, all 3 trunk-split tests, both async trunk tests, both log
allocation/packing tests, and the listed functional drivers. The release
library and driver also built successfully. Reproduce with:

```
make --jobserver-style=pipe -j4 BUILD_MODE=debug all
build/debug/bin/unit/writeback_set_test
build/debug/bin/unit/splinterdb_quick_test
build/debug/bin/unit/trunk_split_test
build/debug/bin/unit/trunk_async_test
build/debug/bin/driver_test log_test --cache-capacity-mib 64 --db-capacity-gib 2
build/debug/bin/unit/splinter_test test_shard_log_
build/debug/bin/driver_test cache_test --async --cache-capacity-mib 64 --db-capacity-gib 2
build/debug/bin/driver_test splinter_test --functionality 100000 100 --cache-capacity-mib 64 --db-capacity-gib 2
```

Dedicated tests exercise sync and async acquisition, extracting an async result
on the receiving thread, allocation followed by an independent get, claim/write
upgrade after transfer, release after original-thread exit and ID reuse, and
public iterator next/prev/deinit on live memtables and persisted branches.
The rejected tagged version also passed the trunk GC test, the public cache-read fault
test, and the late-reclamation fault test. The transfer test checks that handles
with different acquisition stripes still compare as the same cached page.
Validation used `BUILD_ROOT=build-receipt` for the separate-stripe layout and
`BUILD_ROOT=build-tagged` for the tagged layout; use the corresponding executable
prefix when reproducing the commands above.

The follow-up prefetch admission repair inherited from project 1 also passed
the deterministic `cache_test --prefetch-admission` regression and async cache
driver. Complete reruns verified 16 writeback tests and 48 public quick tests,
including the cache handle and public iterator handoff tests, using
`BUILD_ROOT=build-prefetch`. The logs and exact counts are in the
`prefetch-admission` evidence directory.

## Performance

The retained 16-byte receipt has mostly small resident read changes. Storage
variability prevents a reliable disk-read attribution, and the insertion loss
that prompted the tagged experiment did not repeat. The last resident control
also showed a one-thread long-range loss absent from the first matrix, with
overlapping sample ranges. These results do not establish performance neutrality
for every workload; the report retains the variation instead of treating one
favorable matrix as clearance.

The separate-stripe code is `6c3a211`, followed by formatting-only synchronization
in `4f99413`; tagged code is `7c65f4b`. Compare against project 1's `068fef0`
compact receipt implementation (`1a8190a` adds formatting). The measured
libraries remain frozen in their distinct build roots. After reverting the tag,
the production/test code retained the validated 16-byte layout. A later merge
inherits project 1's prefetch-only publication guard and deterministic regression;
its separate follow-up builds leave the earlier measured binaries frozen.
The same public-API benchmark
source, fixed database fixtures, ABBA order, CPU selection, phase lengths, and
serialized disk-backed execution described in
[the project 1 report](cache-errors-results.md#performance) apply here. This
comparison isolates the handoff receipt and reference-stripe selection changes.

Separate-stripe 16-byte receipt, mean throughput change against project 1
(two samples per implementation):

| Workload | Resident, 15 s | Disk, 5 s |
|---|---:|---:|
| Point, 1 thread | -0.14% | -4.57% |
| Point, 8 threads | +3.62% | -4.69% |
| Range 1, 1 thread | -1.78% | -2.55% |
| Range 1, 8 threads | -3.08% | -3.14% |
| Range 10,000, 1 thread | -0.24% | -2.72% |
| Range 10,000, 8 threads | +1.79% | -2.81% |

A 20-second resident range-1/8 repeat measured **+3.60%**, so the initial
-3.08% loss did not repeat. Disk ABBA repeats remained unstable: the 10-second
point/1 baseline changed from 19.6K to 26.6K ops/s between its two samples, while
the candidate measured 20.2K and 21.7K. Reopening the same physical database in
project-1/handoff/project-1/handoff/project-1 order reversed the point differences
to +9.38% (1 thread) and +10.89% (8 threads), with range-1/8 at -2.33%. That
control still had broad baseline variation. These runs do not establish a
causal handoff benefit or regression for disk-bound reads. Every query run
reported zero errors and violations.

The shared insertion matrix additionally compares both projects with the
original baseline, using fresh disk images and 1/8 inserters with WAL off/on.
The separate-stripe receipt's six-second insertion throughput changes against
project 1 were +0.08%/+1.00% with WAL off (1/8 inserters), and +0.44%/-2.39% with
WAL on. A 20-million-record, eight-inserter control with 20 ms completion polling
measured -6.80% without WAL and +0.08% with WAL. Both no-WAL mirrored samples
were below their respective project 1 samples, prompting the tagged-layout
experiment. Longer WAL insertions initially showed a project 1 loss against the
original baseline, but that also did not repeat in the final bracketed control;
see the parent report for that separate comparison.

The tagged read control uses six mirrored runs: project 1, 16-byte handoff,
tagged handoff, tagged handoff, 16-byte handoff, project 1. All six resident
workloads run for 10 seconds. The final insertion control uses 20 million
records, eight inserters, 20 ms reporting, both WAL modes, and six original
baseline runs bracketing two samples each of project 1 and both handoff layouts.
It retains the same source, cache/memtable sizes, background threads, and CPU
affinity as the earlier insertion matrix.

Final resident control, mean throughput changes:

| Workload | Retained 16-byte vs project 1 | Tagged vs 16-byte |
|---|---:|---:|
| Point, 1 thread | +0.56% | -2.86% |
| Point, 8 threads | +2.23% | -2.44% |
| Range 1, 1 thread | -1.40% | +1.11% |
| Range 1, 8 threads | +0.66% | +1.73% |
| Range 10,000, 1 thread | -3.71% | +5.31% |
| Range 10,000, 8 threads | +3.51% | -0.11% |

The retained layout's one-thread long-range samples were 673–718 queries/s,
against project 1's 708–736. Its earlier matrix measured -0.24% for that workload.
This final control had zero query errors/violations and zero query-phase read
I/O. The tagged layout traded lower point throughput for some range gains; it
did not provide an overall reason to replace the simpler separate-stripe layout.

In the final bracketed insertion control, the retained layout differed from
project 1 by **-0.30% without WAL** and **-0.01% with WAL**; the earlier -6.80%
no-WAL difference did not repeat. This does not make the original-baseline
comparison stable: the retained layout's two no-WAL samples were -8.20% and
-3.60% against their nearby original baselines, while project 1's were +5.13%
and -3.71%. The tag measured +1.41%/-6.46% against project 1 in the overall
no-WAL/WAL means, but its nearby WAL-baseline comparisons had opposite signs
(-6.54% and +3.85%). CPU efficiency and bytes per record were generally much
closer than wall time. All 24 runs completed with zero load/close errors.

The evidence supports retaining the 16-byte prototype and its existing atomic
operations. It does not establish a statistically reliable zero-cost handoff
across every workload, especially disk reads and compaction-sensitive inserts.
The tag is an abandoned experiment, not the implementation at the branch tip.

Compact CSVs, command manifests, environment details, and binary provenance are
committed under `benchmarks/results-20261011/cache-handoff`; shared insertion
evidence is under the adjacent `cache-errors` directory inherited from project 1.
Full interval traces and logs remain in the campaign paths recorded in the
manifests. Limitations of the project 1 matrix also apply to this prototype;
two samples per implementation are insufficient for a strong statistical claim.

The later prefetch guard was measured separately against this branch's own
frozen pre-guard binary (`65b9123` with the repair; the old binary remains in
`build-receipt`). Five-second disk ABBA ranges measured -4.26%/-6.12% for one-row
ranges at 1/8 threads and -1.55%/-4.04% for 10,000-row ranges. For short ranges
at 8 threads, the old samples were 60.6–62.1K queries/s and the repaired samples
55.7–59.5K; mean CPU efficiency changed -5.91%, with nearly unchanged bytes/query.

A focused ten-second ABBA repeat of the short ranges measured **+1.67% at one
thread and +1.20% at eight threads**. CPU efficiency changed +1.75%/+1.17%;
bytes/query again barely changed. Its old/repaired eight-thread ranges were
66.3–67.2K and 66.8–68.3K queries/s. The initial short-range loss did not repeat;
these samples do not establish a guard speedup or prove that it is free. All
32 recorded phases across both follow-ups reported zero errors and violations.
The guard adds one atomic status clear per admitted prefetch page and leaves
the resident hit-path source unchanged.

The follow-up directories are `disk-log0-prefetch-guard` and
`disk-log0-prefetch-guard-short10`. Their manifests retain build/fixture hashes
and exact commands; `prefetch-admission` records the repair validation and new
release provenance. Both campaigns copied the fixture before each run but did
**not** explicitly sync that copy before opening/warming the database. Buffered
writes to historical or unreachable pages could therefore overlap timed disk
reads. This is a procedure limitation, not a demonstrated cause. Future controls should
`fsync` the copied file before opening it. The earlier query and insertion
binaries were preserved throughout this repair.
