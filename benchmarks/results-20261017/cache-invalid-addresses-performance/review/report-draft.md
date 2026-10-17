Draft: invalid-address cache performance investigation

The selected source candidate is `async-load`: it keeps invalid-address errors
and moves allocation validation out of the async resident-hit dispatcher into
the existing disk-load coroutine. Synchronous allocation validation remains
inline. Its targeted correctness checks pass. The completed cold-miss comparison
shows that this version avoids the extra cost introduced by an earlier helper
variant. **Final resident-hit, database and real-I/O confirmation remain pending
in this draft; this is not yet a merge recommendation.**

This file lives in `review/`; links below are relative to that location in the
evidence archive. If promoted to the archive's top-level README, remove the
leading `../` from those links.

The comparisons distinguish three identities:

| Profile | Identity | What the comparison answers |
| --- | --- | --- |
| `main` | `8e0f29b89598db1d52aa7090404b475001dbf2eb` | Baseline before the invalid-address work |
| `original` | `da5757265109f3a5936e0ef20daff6a955f775a3` | The original invalid-address branch, including its required validation |
| `async-load` | Original plus the reviewed optimization; `clockcache.c` SHA256 `a26ce40399a60f76f8559eb82703f688bf0bd71662ce93d9ad4232f7451c107e` | Final optimization relative to both original and main |

The final release library SHA256 is
`a3cde96521c0fdd52e21e528877daac3166dd83bf5b8de462437e1c5f7f9e152`.
The source is an uncommitted variant with an exact base and hash, not a separate
commit. Full source identities are in [main-source.json](../main-source.json),
[original-source.json](../original-source.json) and
[async-load-source.json](../async-load-source.json). The earlier profile named
`candidate` is a different, discarded implementation; its results and validation
must not be attributed to `async-load`.

The important mechanism is that resident mappings already establish allocation
validity: the cache mapping is discarded before the allocator can mark an extent
free. Hits therefore do not perform the added allocator membership check.
They still validate address shape and directory bounds, acquire a reference and
verify page identity. Initial controlled instruction counts found four extra
retired instructions per synchronous hit and eight per async hit in `original`
relative to `main`, including the common probe loop. The common loop cancels in
those differences. Branch-miss counts did not show a corresponding large change.
These counts identify extra work; they do not by themselves predict throughput.
[Counter evidence](../harness/controlled/preliminary-analysis.json)

The selected change puts diagnostic formatting in a cold, non-inlined helper
and moves the async allocator check into the existing disk-load child, before
reservation or I/O. This changes the compiler's hot-path organization without
adding a new synchronous helper call. A fresh invalid miss still completes with
BAD_PARAM and a null handle before owning resources. Admission retries repeat
lookup and validation; resuming already-submitted I/O skips initial validation
and reservation exactly as before. Callback publication, waiter wakeup, cleanup,
thread identity and coroutine depth are unchanged.
[Independent correctness review](async-load-independent-correctness-review.json)

The completed nonblocking-miss experiment deliberately measures an allocated
page absent from cache. All profiles return BUSY without timed I/O. Main can
return after the directory miss; the branch must first distinguish an allocated
miss from an invalid/unallocated address. That additional check is required by
the new error semantics and should not be removed to improve this benchmark.

| Completed nonblocking-miss comparison | Throughput change | Pointwise 95% block interval |
| --- | ---: | ---: |
| Original / main | −40.70% | −41.04% to −40.35% |
| Final async-load / main | −40.58% | −41.48% to −39.67% |
| Final async-load / original | +0.20% | −1.89% to +2.32% |

Mean worker CPU time is 8.05 ns/op for main, 13.57 ns/op for original and
13.54 ns/op for async-load. Thus the large percentage represents about
5.5 additional nanoseconds per cold nonblocking lookup on this machine.
Original and final both retire about 59 additional instructions/op versus main;
the final implementation adds no measured instruction cost over original here.
Three rotating mirrored blocks supplied six samples/profile. Every sample was
retained, including thermal flags.
[Audited analysis](../database/analysis/final-misses-audited.json),
[raw results](../probe-results/final-misses/results.csv),
[manifest](../probe-results/final-misses/manifest.json)

The production source has three direct `cache_get(..., FALSE, ...)` call sites,
all in `mini_allocator.c`: metadata-cursor initialization and crossing to the
next or previous metadata page. Moving within an already pinned metadata page
does not call cache_get. A BUSY result requests metadata prefetch and returns
WOULD_BLOCK. B-tree deep prefetch uses these cursors during positioning and
extent progress; this is not a per-tuple nonblocking miss in an ordinary scan.
Actual calls and misses per database operation were not counted in this campaign,
so the microbenchmark percentage is not a measured database regression or a
numerical bound on one. A workload that repeatedly polls absent metadata can
pay the check repeatedly. The source review records the relevant scan paths.
[Scan-path review](long-scan-path-review.json)

The earlier alternatives explain why the selected source is deliberately small:

| Variant | Evidence and disposition |
| --- | --- |
| `hints`, `cold`, `miss-only` | Diagnostic/code-generation probes; not independently established final performance fixes |
| `outlined` | Smaller async code, but initial synchronous resident-hit throughput was approximately 14%–18% below main; rejected rather than treating code size as a speed result |
| `mask-original`, `mask-main` | Replaced the alignment remainder with a power-of-two mask; smaller arithmetic did not establish a repeatable throughput win, so omitted |
| `isolated-miss` | Moved the entire sync miss into a helper and async validation into the disk child. Sync output-pointer escape added stores/reloads; small timing benefits were inconclusive, so omitted |
| `sequential`, then formatted `candidate` | Improved hot-path organization but retained an allocation wrapper. The completed miss screen measured candidate/original throughput at −4.16% (95% interval −5.95% to −2.33%), with 21 extra instructions/op. Replaced by async-load |
| `async-load` | Retains the cold diagnostic helper, removes the allocation wrapper, and moves only async validation into the existing load child. Final selection still awaits the pending confirmations below |

Early two-sample screens had enough frequency/placement drift to make small
throughput deltas unreliable. Their raw results remain evidence of exploration,
not substitutes for the final controlled comparisons.
[Early analysis](../harness/controlled/preliminary-analysis.json),
[controlled screen](../database/analysis/controlled-screen.json),
[discarded candidate miss analysis](../database/analysis/controlled-misses-audited-analysis.json),
[isolated-miss review](isolated-miss-cost-and-validation-review.json)

The insertion investigation found a timing-boundary problem in interpreting the
first database pilot. With WAL disabled, original/main sampled throughput was
−0.10%, while whole-body throughput was −7.90%; mean post-sampling join time was
0.762 seconds versus 0.306 seconds. Including close reduced the apparent loss to
−3.83%. With WAL enabled, sampled/body/drained deltas were −0.46%/−0.62%/−0.73%.
The sampled process-CPU window ended before stop, joins and close, so it could
not establish total insertion CPU neutrality. These observations locate the
discrepancy at the timing boundary; they do not prove a cache cause for the
delayed in-flight insert completions.
[Pilot boundary analysis](../database/analysis/insert-pilot-boundaries.json)

A diagnostic fixed-count follow-up completed exactly 20 million inserts in each
of 18 WAL-disabled runs comparing main, original and isolated-miss. Original's
paired throughput estimate was −2.30%, with an extremely broad interval of
−26.20% to +29.35%; paired body CPU/op was +0.74% (−1.81% to +3.36%). Worker
deregistration took at most about 3.4 microseconds, and mean worker-completion
spread was about 3.1 seconds. That excludes deregistration as a substantial cost
in these fixed-count runs, but the workers had already completed when the stop
was requested, so it does not settle the earlier timed-stop tail. Thermal/run
drift and unequal fixed-quota completion prevent attribution of a few-percent
cache effect. These are original/isolated-miss results, not final async-load
insertion evidence.
[Fixed-count analysis](../database/analysis/fixed20m-summary.json)

The initial resident database pilot found original/main mean throughput changes
of +0.10% and +2.19% for point queries with one/eight workers, −1.23% and −0.80%
for one-row ranges, and −2.06% and −2.51% for 10,000-row ranges. These are four
short samples/profile, not final confidence bounds. Long scans acquire cache
references at leaf transitions and iterator setup, not each tuple. Reviewed
B-tree/scan instruction sequences were unchanged across the early profiles after
normalizing relocation addresses, so the scan loss cannot be assigned to the
four/eight extra hit instructions without actual call counts.
[Resident pilot](../database/results/resident-baseline/summary.csv),
[scan code comparison](scan-codegen-identity.json)

**Pending: final-hits.** Replace this paragraph with the completed audit and a
table containing original/main, async-load/main and async-load/original for
blocking and async access, independent and dependent access, one and eight
workers. Include throughput, worker CPU/op, pointwise block intervals and the
retired-instruction differences where available. Partial `final-hits-through-*`
analyses are snapshots of ongoing work, not a completed-suite verdict.
[Final-hit manifest](../probe-results/final-hits/manifest.json)

**Pending: bounded async churn.** Record the queued exact-profile real-I/O smoke
results, source/library/binary hashes, operations, actual read traffic, async
waits, zero identity/status errors and successful final eviction. This checks
admission, callback completion and progress with a working set larger than cache.
It does not establish disk throughput neutrality or a 2%–3% performance bound.
[Churn review and acceptance criteria](async-load-production-review.md)

**Pending: resident-final-single.** Report the queued final source comparison for
one-worker resident point queries and 10,000-row ranges, using the fixed CPU and
rotating mirrored blocks. State whether the old long-scan loss reproduces and
retain uncertainty. This experiment does not replace the missing final
eight-worker or short-range database comparisons.
[Final resident manifest](../database/results/resident-final-single/manifest.json)

**Pending: insert-final-fixed20m-retry.** Report the exact main/original/async-load
comparison with 20 million fixed records, the diagnostic driver, three rotating
mirrored blocks, both WAL modes and the corrected three-second excluded
precondition. Include body/drained throughput and CPU boundaries. The initial
`insert-final-fixed20m` request was superseded before workload execution because
its 15-second precondition could exhaust the record cap; its retained rejection
record is not performance data.
[Final insertion results directory](../database/results/insert-final-fixed20m-retry/)

The exact async-load source passed 118 selected unit executions: 18 writeback
cases each in release, debug and release-MSan; 62 release quick cases; and the
core read-error propagation case in release and debug. The debug prefetch
admission scenario and the exported-tree formatting check also passed. Source
hashes before and after validation match. These tests cover invalid shapes,
unallocated/freed addresses, immediate null-handle errors without callbacks or
I/O, setup/completion failures, waiter wakeup/retry and ownership cleanup. This
is selected regression coverage, not a full CI run.
[Validation manifest](../validation-async-load/validation.json),
[validation runner](../validation-async-load/validate.py),
[independent source review](async-load-independent-correctness-review.json)

Final cache comparisons use one frozen dynamic harness for all libraries,
explicit worker affinity and deterministic worker-to-thread-ID assignment.
Each scenario has a predetermined excluded precondition, then rotating mirrored
blocks. Analysis averages mirror pairs inside each block and computes geometric
ratios with Student-t intervals on block log ratios. Blocks are the replication
unit; adjacent samples are not independent replicates. No samples are silently
dropped for thermal flags. The shared machine lock serializes recorded campaign
builds, tests and measurements; the package audit must confirm completed
intervals and retain unknown restart endpoints.
[Final runner notes](../harness/controlled-final/README.md)

The limits remain material: three blocks give weak intervals; thermal carryover
can violate independent-block assumptions; intervals are pointwise across many
comparisons; process CPU/cycle diagnostics do not provide calibrated frequency
normalization; and early harness generations must not be pooled with the final
one. Pure resident probes establish cache API costs, not database tail latency,
durability throughput or general disk behavior. The failed v3 smoke was a
harness logging write inside the I/O snapshot, corrected in v3b. An earlier
fixed-count suite stopped after its excluded precondition because of a parser
bug and was rerun separately. All failures and partial records remain available;
they are not successful performance samples.
[v3b repair](../harness/controlled-v3b/README.md),
[failed smoke](../jobs/controlled-smoke/output.log),
[rejected fixed-count suite](../database/results/insert-fixed20m/REJECTED.txt)
