# Predicate ranges integrated with disk-format

The PR candidate is `robj/filtered-ranges`, measured at merge `7b2c37da`.
It includes the redesigned predicate API from `d9893674`, disk-format
`7a0c8171`, and main `0fa3bd6c`. The prior filtered-ranges head is preserved at
`backup/filtered-ranges-before-disk-format-20261013`. No production runtime
change was needed to merge these heads: `src`, `include`, `tests`, `Makefile`
and `.github` exactly match the previously validated integration `36183807`.
Later changes record documentation and evidence only.

Use disk-format as the PR base for an incremental predicate review, then main
after disk-format lands. Targeting main before then includes both changes.
This work remains on local branches; nothing was pushed or merged into main.

## Correctness and API

The initializers take separate predicate, context and optional representative
key arguments. A predicate alone scans all relevant branches; a representative
key without a predicate returns `EINVAL`. The exact predicate selects keys
before message merging. Pruning additionally requires every accepted key to
hash like the representative under every possible seed. The routing-filter
module validates and uses each descriptor's saved seed and encoding settings.
The iterator copies the representative key; predicate context remains borrowed.

Source identity preserves the earlier integration's 125 debug and 125 release
unit-case passes and full routing-filter suites. Fresh checks passed 66 release
unit cases and the full routing-filter suite, plus six predicate and ten merge
cases under GCC ASan. ASan ran with `detect_leaks=0:abort_on_error=1`; this is
not LeakSanitizer coverage. C11/C++17 public-header probes passed. The initial
probe lacked the required platform macro; the corrected probe and initial
harness error are both retained. No product code changed for that correction.
The exact CI formatter passed on a complete staged export, including archived
sources; whitespace checks passed against disk-format and main. The independent
evidence audit passed all 4,634 checks across 66 verified benchmark invocations.

The tests cover persisted mixed seeds, hash-cache eviction, collisions,
noncommutative update histories, deletes, range bounds, direction changes,
copied-key ownership and cross-thread handoff. See [validation evidence](validation/README.txt)
and [API documentation](../../../docs/hash-filtered-ranges.md). The full CI
compiler/sanitizer/stress matrix remains separate from these local checks.

## Performance interpretation

The controls are main, disk-format and the combined candidate, all linked to
the same benchmark harness. Disk-format and combined read copies of the same
format-8 fixture. Main uses its own format-7 fixture. Database files are on
ext4 on `/dev/mapper/snafu-root_crypt`; fixtures are copied and fsynced before
measurement. The shared coordinator excludes other benchmarks, builds and
tests during each benchmark job. Workers have fixed CPU and Splinter thread-ID
assignments. Hardware counters exclude setup, validating scans, warmup and close.

Resident results against disk-format show no material added CPU cost: point
queries use 1.21% fewer user cycles, one-result scans use 0.50% more, and long
scans use 1.06% fewer per returned tuple. Corresponding throughput changes are
+0.59%, -1.25% and +2.54%. Disk-backed point and long-scan throughput changes
are -1.82% and -1.34%.

An initial disk-backed one-result combined scan was much slower: 19.6k
queries/sec, followed by 68.1k, versus disk-format's 71.3k. It returned correct
values and read similar bytes per query, but spent much more CPU time in the
kernel. **The original outlier remains in the report and aggregate.** Six
balanced targeted repeats averaged 73.69k combined and 73.76k disk-format
(-0.09%); four separately sampled runs also completed normally. No severe
slowdown recurred. Normal profiles show contention in the Linux AIO completion
ring, shared with main and disk-format. They identify a plausible pressure
point, not the original outlier's cause. The polling loop's existing fairness
weakness is not repaired here, and absence of rare slowdowns is not established.

Initial long disk scans also had higher p99/p99.9 latency. A separate six-run
15-second balanced repeat changed tuple throughput by -0.11%, user cycles per
tuple by -1.08%, and CPU time per tuple by -2.13%. Tail ranges overlapped:
disk-format p99.9 was 24.9–37.2 ms, combined 27.0–46.7 ms. The combined p99 and
p99.9 means were still 3.3% and 15.6% higher, respectively. Each run contains
about 13,000 queries, leaving only about 13 observations in its upper 0.1%.
This supports no sustained throughput regression; it does not establish tail
neutrality or attribute the remaining tail difference to a specific cause.

In a selective tenant workload on the combined binary, predicate-only scans
reduce latency by approximately 3–20% versus application-side filtering. Adding
pruning reduces it by 13–38%, including deliberate full-width hash collisions.
The modes verify the same expected values, counts and checksums. These gains
depend on predicate selectivity, branch layout and hash behavior.

Read the raw samples with their observed ranges. Sampling runs carry profiler
overhead and are kept separate. User cycles divided by total task-clock is not
a CPU frequency measurement for kernel-heavy disk workloads. These read tests
do not establish universal performance neutrality; no fresh write-throughput
matrix was run. Small effects, other configurations and long-running behavior
remain outside this screening's scope.

## Evidence and reproduction

The [benchmark report](benchmarks/README.txt) and [full result tables](benchmarks/report.txt)
include both original observations and targeted repeats. The `benchmarks`
directory contains scripts,
frozen harness sources, build provenance, measurements and textual profiles.
The `validation` directory contains test scripts, logs, source/library hashes
and the exact CI formatting check's staged-export record. Large database,
binary and sampling files remain outside Git with local paths and hashes.
`review` records the independent source/evidence audit. Historical integration
evidence remains in `../filter-predicate-disk-integration`.

The scripts preserve the original absolute workspace paths and shared
coordinator layout for reproducibility. Adapt those paths when moving this
archive. Run builds and tests through `coordination/run_job.py` in their
respective modes and benchmarks through its exclusive `benchmark` mode, with
`--db-dir` pointing to real disk. The archive-local formatter settings preserve
the exact historical harness and probe bytes; they do not exempt live code.
