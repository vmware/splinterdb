> Historical interim report: this preserves the earlier **async-load** evaluation. Its "selected" and "final" labels refer to that intermediate profile, not the subsequently selected async-mask source. Pending statements reflect that earlier state. See [README.md](README.md) for current results.

Invalid-address cache performance evaluation

The selected optimization keeps invalid-address rejection while moving
allocation validation out of the async resident-hit dispatcher into its existing
disk-load coroutine. Synchronous validation stays inline. The exact source has
passed the selected correctness checks and has been applied to the working
branch. **Overall performance acceptance remains pending:** fixed-count inserts
without WAL show a 9.71% drained-throughput loss versus main that is still being
investigated. The cache-hit and resident-read results below do not clear that
insertion concern.

`main` is commit `8e0f29b89598db1d52aa7090404b475001dbf2eb`; `original` is the
unoptimized invalid-address branch at
`da5757265109f3a5936e0ef20daff6a955f775a3`; `async-load` is that branch plus the
selected optimization. Original/main measures the branch's cost; final/main
measures its remaining cost; final/original measures the optimization. The old
profile named `candidate` is a discarded implementation and is not the final
source.

The completed resident-cache comparison has **no mean final/main throughput
loss greater than 1.5%** across its eight cases. Some intervals still permit
losses above 3%, especially the one-worker async cases, so this is not a strict
3% equivalence result. Eight-worker independent async access improved 2.06%
relative to original; its final/main estimate is +0.25%.

Each cell below gives throughput change and its pointwise 95% block interval;
positive is faster. Independent access uses predetermined addresses; dependent
access obtains the next address from the current page.

| API / workers / access | Original / main | Final / main | Final / original |
| --- | ---: | ---: | ---: |
| Blocking / 1 / independent | −0.26% [−1.43, +0.93] | −0.32% [−1.95, +1.33] | −0.06% [−1.32, +1.21] |
| Blocking / 1 / dependent | −0.56% [−2.56, +1.49] | −0.21% [−3.44, +3.13] | +0.35% [−1.74, +2.48] |
| Blocking / 8 / independent | −1.05% [−1.99, −0.10] | −1.48% [−3.81, +0.91] | −0.44% [−1.90, +1.05] |
| Blocking / 8 / dependent | −0.21% [−1.12, +0.70] | −0.15% [−1.23, +0.94] | +0.07% [−0.40, +0.53] |
| Async / 1 / independent | +0.06% [−5.73, +6.21] | +0.68% [−5.98, +7.82] | +0.62% [−2.01, +3.32] |
| Async / 1 / dependent | −0.56% [−5.73, +4.89] | +0.41% [−5.87, +7.11] | +0.98% [−0.17, +2.14] |
| Async / 8 / independent | −1.77% [−3.90, +0.41] | +0.25% [−0.77, +1.29] | +2.06% [+0.81, +3.32] |
| Async / 8 / dependent | −0.59% [−1.42, +0.26] | +0.14% [−0.92, +1.21] | +0.73% [−0.55, +2.02] |

Worker CPU/op estimates follow the same pattern: final/main ranges from +0.14%
to +1.50% for blocking access, and −0.73% to −0.14% for async access. Single-worker
counters confirm that synchronous hits retain original's four extra
instructions/op versus main. Final async hits retire one fewer than main and
nine fewer than original. The completed audit verified all 144 measured records
and raw logs plus eight excluded preconditions. No measured sample was excluded.
All 144 measured processes recorded thermal-counter increases; those whole-process
snapshots do not establish when throttling occurred within a timed phase.
[Audited estimates and CPU intervals](database/analysis/final-hits-audited.json),
[raw results](probe-results/final-hits/results.csv),
[manifest](probe-results/final-hits/manifest.json),
[independent arithmetic and claim review](review/final-cache-evidence-review.json)

The completed nonblocking-miss experiment measures allocated pages absent from
cache. All profiles return BUSY with zero timed I/O. Main returns after its
directory miss; the branch must also distinguish an allocated miss from an
invalid/unallocated address. That check is required by the new error semantics.

| Valid nonblocking miss | Throughput change | Pointwise 95% block interval |
| --- | ---: | ---: |
| Original / main | −40.70% | −41.04% to −40.35% |
| Final / main | −40.58% | −41.48% to −39.67% |
| Final / original | +0.20% | −1.89% to +2.32% |

Mean worker CPU time is 8.05 ns/op for main, 13.57 ns/op for original and
13.54 ns/op for final: about **5.5 additional nanoseconds per cold nonblocking
lookup**. Both branch versions retire approximately 59 extra instructions/op
versus main. The final implementation adds no measured instructions over
original here. Three mirrored blocks supplied six samples/profile; all samples
were retained.
[Audited analysis](database/analysis/final-misses-audited.json),
[raw results](probe-results/final-misses/results.csv),
[manifest](probe-results/final-misses/manifest.json)

There are three direct production `cache_get(..., FALSE, ...)` call sites, all
in `mini_allocator.c`: metadata-cursor initialization and crossing to the next
or previous metadata page. Moving within a pinned metadata page needs no get.
A BUSY result requests prefetch and returns WOULD_BLOCK. Deep B-tree prefetch
uses these cursors during positioning and extent progress; ordinary scan tuples
do not each perform a nonblocking metadata miss. Actual calls/misses per database
operation were not counted, and repeated polling can pay the check repeatedly.
The microbenchmark percentage therefore is not a measured database regression
or a numerical bound on one. [Scan-path review](review/long-scan-path-review.json)

The final resident database confirmation completed 18 processes and 36 timed
phases over a 200,000-record fixture with 24-byte keys and 100-byte values. It
uses a 128 MiB cache, 4 MiB memtable, no background workers or WAL, one worker
pinned to CPU 2, three-second warmup and eight-second phases. One predetermined
excluded precondition runs each phase for 30 seconds before the three rotating
mirrored blocks.

| Resident database throughput | Original / main | Final / main | Final / original |
| --- | ---: | ---: | ---: |
| Point queries | −1.71% [−3.24, −0.17] | −1.91% [−3.80, +0.02] | −0.20% [−2.60, +2.27] |
| 10,000-row ranges | −1.24% [−3.51, +1.09] | +0.37% [−0.31, +1.06] | +1.63% [−1.15, +4.48] |

The earlier 2.1%–2.5% long-range loss does not reproduce for the final source
in this single-worker confirmation. Point queries retain an approximately 1.9%
mean loss versus main, with uncertainty extending beyond 3%. These results do
not establish eight-worker or short-range database neutrality. Every timed
phase has zero read bytes. Some phases report 4 KiB of process writes from
watchdog logging during measurement; those counters include diagnostic output
and are not evidence of database writes. This database run therefore does not
support a zero-process-writes claim.
[Resident audit](database/analysis/resident-final-single-audited.json),
[manifest](database/results/resident-final-single/manifest.json),
[earlier pilot](database/results/resident-baseline/summary.csv)

The exact-source insertion confirmation completed all 36 measured runs and
720 million inserts, plus two excluded preconditions. Each run inserts exactly
20 million records, with eight workers assigned 2.5 million each. Three rotating
mirrored blocks compare both WAL modes. The precondition is three seconds;
processes share CPUs 0–11 with four background workers, allowing migration and
variable SMT placement. This is a startup/fixed-count workload, not sustained
steady-state insertion.

| Throughput through close | Original / main | Final / main | Final / original |
| --- | ---: | ---: | ---: |
| WAL disabled | −4.41% [−16.54, +9.49] | **−9.71% [−29.66, +15.89]** | −5.55% [−17.05, +7.55] |
| WAL enabled | −2.34% [−9.68, +5.60] | −1.03% [−6.64, +4.90] | +1.34% [−0.70, +3.42] |

Final/main body throughput is −9.83% without WAL and −0.81% with WAL. Full
process CPU/op through close changes by +0.84% [−6.73, +9.03] and −0.11%
[−2.89, +2.76], respectively. These figures use integer boundary clocks from
worker start through body completion or close, including summary-writing time
before close. The older sampled CPU window and rounded close durations are not
used for these headline metrics.

**The no-WAL loss remains unresolved.** All three final/main drain-throughput
block estimates are negative (−19.06%, −1.36%, −7.82%). A fast first main sample
and unequal worker completion are visible, and fixed per-worker quotas reduce
concurrency as workers finish. These are possible contributors, not a demonstrated
explanation. Neither wide intervals nor near-equal CPU/op justifies dismissing
the wall-time loss. All samples remain included.
[Full insertion audit](database/analysis/insert-final-fixed20m-audited.json),
[raw evidence](database/results/insert-final-fixed20m-retry/)

The earlier `insert-final-fixed20m` request was superseded before workload
execution because its 15-second precondition could exhaust the record cap. Its
retained rejection record is not performance evidence.

The bounded async real-I/O churn checks passed with one and six workers, a
64 MiB cache and 128 MiB working set, each with two-second warmup and five-second
measurement. They completed 188,288 and 333,760 measured operations, read about
369 and 656 MiB during measurement, and recorded 131,332 and 237,268 async waits
across warmup plus measurement. Both had zero identity/status errors, zero timed
writes and successful final eviction. This confirms exercised async misses and
callback completion in these bounded cases; it is not a disk-performance
comparison and does not resolve the inherited admission-progress limitation.
[Churn validation](churn-validation.json),
[coverage criteria](review/async-load-production-review.md)

**Follow-up experiments in progress.** The completed 32-page hot-set probe
found that replacing the cache alignment remainder with a bit mask improves
original-branch blocking throughput by 5.43% for independent access and 1.36%
for dependent access. Corresponding cycles/op decrease 6.64% and 1.27%; all
60 measured rows passed the zero-I/O and identity checks. The remainder also
exists in main, so this is a general optimization, not an operation newly
introduced by invalid-address support. The combined `async-mask` source is
frozen and has passed 124 selected unit executions, formatting and deterministic
prefetch admission. Its final cache/database performance confirmation is running;
it has not replaced the applied `async-load` source.
[Hot-set audit](database/analysis/hot32-blocking-audited.json),
[interpretation](review/hot32-evidence-review.json)

A separate shared-quota driver preserves the global key/value set while changing
worker assignment and adding one atomic batch reservation per 1024 records.
Its fixed-quota control with a 15-second precondition has completed: candidate
exact drain throughput is +1.54% versus main, with a wide [-21.36%, +31.11%]
interval; full drain CPU/op is +0.38% [-2.08%, +2.91%]. The earlier loss did not
repeat even before shared quotas were enabled. This does not establish its
cause: driver layout and preconditioning differ from the earlier experiment.
All adverse samples remain retained. The shared-quota comparison also completed:
async-load/main drain throughput is +0.51% [-16.89%, +21.54%] and drain CPU/op
is -0.83% [-4.45%, +2.92%]. Sharing reduced worker completion spread from about
three seconds to 5-7 ms, but did not remove wall-time variability and increased
CPU/op across all profiles. The earlier loss had already failed to repeat with
fixed quotas, so this does not identify its cause.
[Combined fixed/shared audit](database/analysis/quota-fixed-shared-audited.json)

Resident mappings already establish allocation validity: mappings are discarded
before the allocator can mark an extent free. Hits therefore skip allocator
membership checks. They still validate address shape/bounds, acquire a reference
and verify page identity. Initial controlled counters found four extra retired
instructions per synchronous hit and eight per async hit in original versus main,
without a corresponding large branch-miss increase. These are work counts, not
throughput predictions. [Counter evidence](harness/controlled/preliminary-analysis.json)

The final change keeps diagnostic formatting in a cold, non-inlined helper and
moves the async allocator check into the existing disk-load child before
reservation or I/O. It adds no synchronous helper call. Invalid misses still
return BAD_PARAM and a null handle before acquiring resources or registering
callbacks. Fresh retries validate again; resuming submitted I/O skips initial
validation/reservation as before. Callback publication, waiter wakeup, cleanup,
thread identity and coroutine depth are unchanged.
[Independent source review](review/async-load-independent-correctness-review.json)

Long scans acquire cache references at leaf transitions and iterator setup,
rather than each tuple. Reviewed B-tree/scan instruction sequences were unchanged
across early profiles after relocation normalization. The earlier long-scan loss
cannot be assigned to the four/eight extra hit instructions without actual call
counts. [Code comparison](review/scan-codegen-identity.json)

The exploration retained these alternatives and their reasons for rejection:

| Variant | Finding |
| --- | --- |
| `hints`, `cold`, `miss-only` | Code-generation probes, without an independently established final performance benefit |
| `outlined` | Smaller async code but roughly 14%–18% worse initial synchronous-hit throughput; code size alone did not predict speed |
| `mask-original`, `mask-main` | Alignment-mask arithmetic did not establish a repeatable throughput win |
| `isolated-miss` | Whole sync-miss helper caused output-pointer escape and stores/reloads; small timing benefits were inconclusive |
| `sequential` / formatted `candidate` | Allocation wrapper improved hit-path organization but cost 21 instructions and 4.16% throughput versus original on valid nonblocking misses; replaced by async-load |

Early two-sample screens also suffered placement/frequency drift. They explain
the investigation, but are not final acceptance evidence.
[Exploratory results](harness/controlled/preliminary-analysis.json),
[controlled screen](database/analysis/controlled-screen.json),
[discarded candidate miss comparison](database/analysis/controlled-misses-audited-analysis.json)

The insertion pilot exposed a timing-boundary interpretation problem. Without
WAL, original/main sampled throughput was −0.10%, but body throughput including
stop/join was −7.90%; mean post-sample join time was 0.762 versus 0.306 seconds.
Including close reduced the apparent loss to −3.83%. With WAL, those
sampled/body/drained deltas were −0.46%/−0.62%/−0.73%. The sampled CPU window
ended before stop, joins and close, so it could not establish total insertion
CPU neutrality. This locates the discrepancy without proving why the final
in-flight inserts took longer.
[Boundary analysis](database/analysis/insert-pilot-boundaries.json)

An initial diagnostic follow-up completed exactly 20 million inserts in 18
WAL-disabled runs of main/original/isolated-miss. Original/main paired throughput
was −2.30%, with an uninformative −26.20% to +29.35% interval; body CPU/op was
+0.74% (−1.81% to +3.36%). Deregistration took at most about 3.4 microseconds,
while worker-completion spread averaged about 3.1 seconds. Deregistration was
not a substantial cost there, but workers had finished before the stop request,
so this does not resolve the earlier timed-stop tail. Run drift and unequal
fixed-quota completion precluded attributing a few-percent cache effect. These
earlier figures are not measurements of final async-load.
[Initial fixed-count analysis](database/analysis/fixed20m-summary.json)

The exact selected source passed **118 selected unit executions**: 18 writeback
cases in each of release/debug/release-MSan, 62 release quick cases, and the core
read-error propagation case in release and debug. Debug prefetch admission and
the exported-tree formatting check also passed. Before/after source hashes
match. Coverage includes invalid/unallocated/freed addresses, immediate errors
without I/O or callbacks, setup/completion failures, waiter retries and ownership
cleanup. This is selected regression coverage, not a full CI run.
[Validation manifest](validation-async-load/validation.json),
[runner](validation-async-load/validate.py). The formatting check also passed
after applying the change to the actual working branch.
[Target-branch check](jobs/target-format/run.json)

The host is a 12th-generation Intel Core i7-12800H; Linux reports 62.5 GiB of
RAM. Production release libraries use GCC 14.2.0 with `-O3 -ffast-math -flto`
and `-fPIC`. The separately compiled, frozen cache harness uses GCC 14.2.0 with
`-O3 -ffast-math -fno-lto -march=native` and dynamically selects each shared
library. MSan validation uses Clang 19.1.7. Tests and database fixtures reside on
real ext4 storage, with `O_DIRECT` enabled. Every measured resident-cache row
reports zero read/write bytes and zero major faults; fixture setup I/O is outside
that interval.
[Harness build commands](harness/controlled-final/cache_api_probe.json),
[exact generated library configurations](review/build-configs/manifest.json),
[release build command](validation-async-load/jobs/release-build/run.json),
[storage and database manifest](database/results/resident-final-single/manifest.json)

The final cache matrix uses a 64 MiB cache and 12,288 resident 4 KiB pages
(48 MiB), with two seconds of warmup and four seconds measured per process.
Before each scenario, main runs a predetermined 30-second warmup plus one-second
measurement that is excluded from comparisons. One worker is pinned to logical
CPU 2; eight workers are pinned individually to `0,2,4,6,8,10,1,3`, using six
physical P-cores and two specified SMT siblings. The controller uses CPU 12.
Hardware counters are enabled only for single-worker cases; eight-worker
instruction differences are not directly measured.
[Exact arguments, topology and affinity](probe-results/final-hits/manifest.json)

Final cache comparisons use one frozen dynamic harness across libraries,
explicit per-worker CPU affinity, deterministic worker/thread-ID assignment,
and a predetermined excluded precondition for each scenario. Each scenario
uses three rotating mirrored blocks. Analysis averages the two mirror samples
inside each block, then computes geometric ratios and Student-t intervals on
block log ratios. Blocks are the replication unit. No sample is silently dropped
for thermal flags. The campaign lock serializes builds, tests and measurements;
the packaged interval audit retains any unknown restart endpoints.
[Runner and scheduling](harness/controlled-final/README.md)

Precision remains limited: three blocks produce weak intervals; thermal
carryover can violate independence; intervals are pointwise across many cases;
and cycle/CPU-time diagnostics are not calibrated frequency normalization.
There is no strict 3% equivalence claim. Resident probes measure cache API costs,
not database tails, durable throughput or arbitrary storage behavior. Different
harness generations must not be pooled. The failed v3 smoke wrote diagnostics
inside its I/O snapshot, corrected in v3b. An earlier fixed-count suite stopped
after its excluded precondition because of a parser bug and was rerun separately.
Failed and partial evidence is retained without counting it as successful data.
[v3b repair](harness/controlled-v3b/README.md),
[failed smoke](jobs/controlled-smoke/output.log),
[parser rejection](database/results/insert-fixed20m/REJECTED.txt)

The inherited async allocation-progress limitation remains unresolved. This
change does not alter cache batch-reservation ownership or reclamation. Short
churn smokes cannot establish progress across all cache-pressure and worker
lifecycle interleavings, and must not be presented as a repair of that separate
issue. [Scope review](review/isolated-miss-review.json)

Final `clockcache.c` SHA256:
`a26ce40399a60f76f8559eb82703f688bf0bd71662ce93d9ad4232f7451c107e`.
Final release library SHA256:
`a3cde96521c0fdd52e21e528877daac3166dd83bf5b8de462437e1c5f7f9e152`.
The base commit plus normalized patch and source inventory identify the selected
source independently of a future optimization commit. Raw manifests preserve
original absolute commands and historical hashes.
[Baseline identity](main-source.json), [branch identity](original-source.json),
[selected source identity](async-load-source.json)
