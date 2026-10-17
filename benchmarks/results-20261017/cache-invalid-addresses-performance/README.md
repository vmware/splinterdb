Invalid-address cache performance evaluation

The applied `async-mask` patch preserves invalid-address rejection while moving diagnostics to a cold helper, async miss allocation validation to the existing disk-load coroutine, and replacing the hot alignment remainder with a mask. Initialization asserts the already required power-of-two page size. Synchronous validation stays inline; invalid misses fail before reservation/I/O, with unchanged retry/resume and callback ownership. [Source review](review/async-mask-independent-correctness-review.json), [geometry contract](review/async-mask-contract-review.json)

The final resident cache **read/hit matrix** is complete: **its largest mean loss versus main is 1.08%**, for eight-worker dependent blocking access. All other mean estimates are positive. Independent async access improves about 2% versus the unoptimized branch at both one and eight workers. Some uncertainty intervals still allow losses beyond 3%; these results are not a strict equivalence claim. **The no-WAL insertion result remains indeterminate**: the selected source is 4.09% slower through close in its final confirmation, with a wide interval. The separate resident write matrix spans −0.16% to +0.40% versus main; it does not explain the database wall-time variation.

`main` is `8e0f29b89598db1d52aa7090404b475001dbf2eb`; `original` is the unoptimized invalid-address branch, `da5757265109f3a5936e0ef20daff6a955f775a3`. Original/main measures the unoptimized branch cost; async-mask/main measures the selected implementation's remaining cost; async-mask/original measures its improvement. `async-load` is a historical intermediate implementation; the older profile named `candidate` was discarded.

The following table reports throughput change and pointwise 95% block intervals; positive means faster. Independent access uses predetermined addresses; dependent access reads the next address from the current page.

| API / workers / access | Original / main | Async-mask / main | Async-mask / original |
| --- | ---: | ---: | ---: |
| Blocking / 1 / independent | −0.14% [−2.72, +2.50] | +0.41% [−2.90, +3.84] | +0.56% [−0.19, +1.31] |
| Blocking / 1 / dependent | +0.34% [−5.45, +6.49] | +0.19% [−2.29, +2.73] | −0.15% [−3.91, +3.75] |
| Blocking / 8 / independent | −0.81% [−2.47, +0.88] | +0.83% [−2.36, +4.12] | +1.65% [−0.31, +3.65] |
| Blocking / 8 / dependent | −0.90% [−2.67, +0.90] | −1.08% [−3.31, +1.20] | −0.18% [−0.99, +0.63] |
| Async / 1 / independent | −0.36% [−4.95, +4.45] | +1.67% [−2.26, +5.75] | +2.03% [+0.59, +3.50] |
| Async / 1 / dependent | +0.23% [−4.20, +4.87] | +0.60% [−3.79, +5.19] | +0.37% [−2.71, +3.54] |
| Async / 8 / independent | −1.33% [−2.43, −0.22] | +0.58% [−0.87, +2.05] | +1.94% [+0.65, +3.24] |
| Async / 8 / dependent | −0.91% [−1.24, −0.57] | +0.50% [+0.01, +0.99] | +1.42% [+0.98, +1.87] |

Single-worker hardware counters agree with the disassembly: blocking hits retire one fewer instruction than original, but three more than main; async hits retire ten fewer than original and two fewer than main. Successful get/unget removes one geometry division, leaving two existing reference-stripe divisions. The remainder also exists in main, so masking is a general optimization. Eight-worker instruction counts were not measured. [Code review](review/async-mask-hit-codegen-review.json)

All **144 measured rows and eight excluded preconditions** passed the audit of hashes, raw logs, complete comparison blocks, identity/status and zero timed read/write bytes and major faults. No measured sample was excluded. All measured processes recorded thermal-counter increases; those process-wide snapshots do not locate throttling within a timed phase. [Audited estimates](database/analysis/mask-final-hits-audited.json), [raw CSV](probe-results/mask-final-hits/results.csv), [manifest and scheduling](probe-results/mask-final-hits/manifest.json), [independent evidence review](review/mask-final-resident-evidence-review.json)

The final **32-page hot-set** comparison also passed all 36 measured rows and two excluded preconditions, with zero timed I/O/errors. Independent blocking accesses improved **8.10% [3.17%, 13.25%] versus main** and **6.67% [5.14%, 8.21%] versus original**. Dependent access was +0.38% [−3.51%, +4.43%] and +1.21% [−2.69%, +5.26%], respectively. This supports a benefit when geometry-check cost is exposed by a very small working set; it does not predict gains in memory-latency-dominated workloads. The page-offset-zero accesses can alias L1 sets, identically across profiles. All 36 process-level thermal flags are retained. [Hot-set audit](database/analysis/mask-final-hot32-audited.json), [raw CSV](probe-results/mask-final-hot32/results.csv), [independent review](review/mask-final-hot32-evidence-review.json)

The final resident **claim/lock/write** probe repeatedly updates 32 mutable pages, using either disjoint worker partitions or a shared set. Its 72 measured runs and four excluded preconditions pass exact counter/complement checks with zero errors, timed I/O or major faults; all thermal flags are retained.

| Write workers / access | Async-mask / main | Async-mask / original |
| --- | ---: | ---: |
| 1 / disjoint | −0.02% [−1.62, +1.60] | +1.09% [+0.13, +2.05] |
| 1 / shared | +0.38% [+0.08, +0.68] | +1.00% [−0.34, +2.36] |
| 8 / disjoint | −0.16% [−2.02, +1.72] | +0.84% [−1.10, +2.82] |
| 8 / shared | +0.40% [+0.09, +0.72] | +0.74% [−0.17, +1.66] |

All four pointwise main intervals exclude a 3% loss in this probe. Single-worker instruction deltas remain +3/main and −1/original. Eight-worker shared access records about 0.085 failed claims/update; disjoint access records none. This measures repeated mutation of already-dirty resident pages, excluding allocation, tree traversal, splits, WAL and writeback. It does not establish database insertion neutrality. Compare profiles within each mode: disjoint/shared worker working sets differ. [Write audit](database/analysis/mask-final-writes-hot32-audited.json), [raw CSV](probe-results/mask-final-writes-hot32/results.csv), [independent review](review/mask-final-writes-evidence-review.json), [harness validation](harness/controlled-writes/validation-status.json)

The final always-uncached **valid nonblocking miss** comparison improves 17.84% [17.08%, 18.60%] versus original, but remains **29.83% slower [−31.07%, −28.56%] than main**. Mean worker CPU is 8.15 ns/op for main, 13.69 for original, and 11.62 for async-mask: the optimization saves 2.07 ns/op while retaining a 3.47 ns/op overhead versus main. It retires one fewer instruction than original and 58 more than main. All 18 measured rows plus one excluded precondition pass, with zero timed I/O/errors; all thermal flags remain included. [Miss audit](database/analysis/mask-final-misses-audited.json), [raw CSV](probe-results/mask-final-misses/results.csv), [independent review](review/mask-final-misses-evidence-review.json)

Main returns BUSY after a directory miss; the branch must first validate allocator membership to distinguish allocated from invalid/unallocated addresses. The current implementation therefore is not neutral on every cache API path. Membership validation is required; its implementation cost remains open to optimization. Three direct production nonblocking-get sites serve metadata cursors in `mini_allocator.c`, not each ordinary scan tuple. Their actual frequency was not counted, so this percentage is not a measured database regression or a numerical bound on one. [Call-path review](review/long-scan-path-review.json)

The final async-mask insertion confirmation completed **36 measured runs and 720 million inserts**, plus two excluded preconditions. Each run inserts 20 million records with eight workers assigned 2.5 million each, CPUs 0–11, a 15-second excluded precondition and three rotating mirrored blocks. Through-close timings and CPU/op use exact worker-start/body/close boundaries, including diagnostic summary work before close. These are startup/fixed-count experiments, not steady-state or insertion-tail measurements.

| Selected-source insertion | Async-mask / main throughput | Async-mask / original throughput | Async-mask / main CPU/op |
| --- | ---: | ---: | ---: |
| WAL disabled | **−4.09% [−24.51, +21.84]** | −2.50% [−25.08, +26.87] | −0.35% [−2.82, +2.18] |
| WAL enabled | −1.28% [−5.65, +3.30] | +0.19% [−5.94, +6.72] | −0.51% [−0.88, −0.14] |

No-WAL throughput block changes are −5.12%, −12.39% and +6.12%. Mean worker completion spread is 3.30 seconds for async-mask versus 2.95 for main. CPU/op does not show a corresponding increase, but neither that nor the broad interval clears the wall-time concern. The isolated cache-write results above do not reproduce a material write-path penalty, but do not identify the cause of this database variance. [Final insertion audit](database/analysis/mask-final-quota-audited.json), [raw logs and manifest](database/results/insert-mask-final-fixed20m/)

The earlier async-load experiment showed −9.71% through-close throughput without WAL [−29.66%, +15.89%], with all three block estimates negative. Its follow-up using a new fixed/shared-quota driver gave +1.54% [−21.36%, +31.11%] with fixed quotas and +0.51% [−16.89%, +21.54%] with shared quotas. The loss had already failed to reproduce before enabling shared quotas, so shared allocation does not explain its reversal. Shared quotas reduced completion spread but changed CPU cost; driver layout, preconditioning, key assignment, scheduling and thermal state remain possible influences. All adverse results are retained; different driver generations are not pooled. [Original adverse result](database/analysis/insert-final-fixed20m-audited.json), [fixed/shared follow-up](database/analysis/quota-fixed-shared-audited.json)

The final resident database comparison completed 18 processes and 36 timed phases over 200,000 records: 128 MiB cache, 4 MiB memtable, no WAL/background workers, one worker on CPU 2, three-second warmup and eight-second phases, with excluded 30-second preconditions.

| Resident database throughput | Async-mask / main | Async-mask / original |
| --- | ---: | ---: |
| Point queries | −1.51% [−3.61, +0.64] | −0.30% [−4.08, +3.63] |
| 10,000-row ranges | −0.03% [−0.52, +0.45] | +1.69% [−0.16, +3.56] |

The earlier long-range slowdown does not recur here; point-query uncertainty still extends beyond 3%. All timed reads report zero bytes, but some range phases record 4 KiB process writes from watchdog logging, so zero *all-I/O* is not claimed. This single-worker run does not establish short-range or eight-worker database neutrality. [Resident audit](database/analysis/mask-final-resident-audited.json), [raw manifest](database/results/resident-mask-final-single/manifest.json)

The [historical report](async-load-report.md) preserves the earlier async-load results, rejected variants and timing-boundary investigation. Smaller outlined code hurt synchronous performance; the discarded miss wrapper added overhead on cheap nonblocking misses. The selected patch avoids that wrapper.

The exact selected source passed **124 unit executions**: 18 writeback cases in release/debug/release-MSan, 62 release quick cases, core read-error propagation in release/debug, and two existing configuration-rejection cases in each mode. Debug prefetch admission and both exported-source and actual-target formatting passed; before/after source hashes match. This is targeted regression coverage, not full CI. The patch is applied to `robj/cache-invalid-addresses`; all 172 validated source-file hashes match the target worktree. [Validation and commands](validation-async-mask/validation.json), [runner](validation-async-mask/validate.py), [delivery check](delivery-source-check.json), [target formatting](jobs/target-mask-format/run.json)

Selected-source bounded async churn passed at one and six workers with a 64 MiB cache, 128 MiB working set, two-second warmup and five-second measurement. The runs completed 109,184/225,408 measured operations and read 230,002,688/464,289,792 bytes, with zero identity/status errors, timed writes or major faults, and successful final eviction. They recorded 78,628/157,289 async waits across warmup plus measurement. Independent review verified exact geometry, placement, log/CSV consistency and source/library identities. This is bounded correctness/progress coverage, not a disk-performance comparison or a resolution of the inherited progress issue. [Churn audit](review/mask-churn-audited.json), [raw run manifests](mask-churn-validation.json)

Methods: Intel Core i7-12800H, about 62.5 GiB RAM, GCC 14.2 release libraries with `-O3 -ffast-math -flto -fPIC`. The frozen cache harness uses `-O3 -ffast-math -fno-lto -march=native`; library builds do not use `-march=native`. MSan uses Clang 19.1.7. Files reside on real ext4 storage and the probe uses `O_DIRECT`. Builds, tests and benchmarks serialize under the shared machine lock. [Exact selected build configurations](review/build-configs/async-mask-manifest.json), [harness build](harness/controlled-final/cache_api_probe.json)

The cache matrix uses 64 MiB cache, 12,288 resident 4 KiB pages (48 MiB), two-second warmup and four-second measurement. Each scenario begins with a predetermined excluded 30-second warmup plus one-second sample. One worker uses CPU 2; eight workers use `0,2,4,6,8,10,1,3`, six physical P-cores plus two specified SMT siblings; the controller uses CPU 12. One frozen dynamic harness selects all three libraries. Hardware counters are enabled only for single-worker cases. Three rotating mirrored blocks average each pair before computing geometric ratios and Student-t intervals on block log ratios. [Runner methods](harness/controlled-final/README.md)

Three blocks provide limited precision; intervals are pointwise, and thermal carryover may violate independence. Cycle/CPU-time diagnostics are not calibrated frequency normalization. Resident probes do not establish disk throughput, database tails or arbitrary workload neutrality. The inherited async batch-reservation/admission-progress limitation remains unresolved: this patch does not change its ownership/reclamation mechanism, and bounded churn cannot establish progress across all pressure/lifecycle interleavings. [Scope review](review/isolated-miss-review.json)

Selected `clockcache.c` SHA256: `7f370079f60ccb1b86744a209c20dd09d0b729d512bdf3e022e04b51d23a777a`.
Selected release library SHA256: `c7b04dabd8a600500ead9095a04e0a572baf904e256f077043c7191abdd8d228`.
The base commit, normalized patch and full hash inventory identify the implementation independently of its eventual commit. Raw manifests preserve measured binaries, failures and partial attempts. [Source inventory](async-mask-source.json), [patch versus original](review/async-mask-vs-original.patch)
