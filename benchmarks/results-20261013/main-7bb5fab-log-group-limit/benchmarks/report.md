# Fresh main integration benchmark screening

These compare current feature branches with main 7bb5fab. They do not isolate the cost of merging main. Short runs are screening evidence, not proof of performance neutrality. All databases are on verified real disk with O_DIRECT; copied fixtures are fsynced before opening. Query samples use serial-built immutable fixtures, with separate equivalent fixtures for incompatible formats. Main controls bracket forward and reverse variant sweeps.

| Workload | Phase | Branch | Change | Sample range overlap |
| --- | --- | --- | ---: | --- |
| resident | point:uniform@1 | disk-format | -1.21% | True |
| resident | point:uniform@1 | filtered-ranges | -1.11% | True |
| resident | point:uniform@1 | log-group-limit | -3.24% | True |
| resident | point:uniform@1 | lazy-merge | -2.99% | True |
| resident | point:uniform@1 | async-durable-barrier | -3.58% | True |
| resident | point:uniform@1 | cache-invalid-addresses | -4.60% | False |
| resident | point:uniform@1 | directory-direct | -3.66% | True |
| resident | point:uniform@1 | directory-bucket | -13.61% | False |
| resident | point:uniform@1 | directory-bucket-simd | -14.75% | False |
| resident | point:uniform@1 | directory-bucket-atomic | -11.59% | False |
| resident | point:uniform@8 | disk-format | -1.92% | True |
| resident | point:uniform@8 | filtered-ranges | -1.37% | True |
| resident | point:uniform@8 | log-group-limit | -3.94% | True |
| resident | point:uniform@8 | lazy-merge | -4.99% | True |
| resident | point:uniform@8 | async-durable-barrier | -5.29% | True |
| resident | point:uniform@8 | cache-invalid-addresses | -7.08% | True |
| resident | point:uniform@8 | directory-direct | -6.43% | True |
| resident | point:uniform@8 | directory-bucket | -25.62% | False |
| resident | point:uniform@8 | directory-bucket-simd | -25.20% | False |
| resident | point:uniform@8 | directory-bucket-atomic | -11.45% | False |
| resident | range:uniform:1@1 | disk-format | +1.54% | True |
| resident | range:uniform:1@1 | filtered-ranges | -0.07% | True |
| resident | range:uniform:1@1 | log-group-limit | -2.20% | True |
| resident | range:uniform:1@1 | lazy-merge | +4.48% | True |
| resident | range:uniform:1@1 | async-durable-barrier | -0.38% | True |
| resident | range:uniform:1@1 | cache-invalid-addresses | -2.25% | True |
| resident | range:uniform:1@1 | directory-direct | -0.18% | True |
| resident | range:uniform:1@1 | directory-bucket | -7.11% | False |
| resident | range:uniform:1@1 | directory-bucket-simd | -6.10% | False |
| resident | range:uniform:1@1 | directory-bucket-atomic | -5.32% | False |
| resident | range:uniform:1@8 | disk-format | -4.07% | True |
| resident | range:uniform:1@8 | filtered-ranges | -3.38% | True |
| resident | range:uniform:1@8 | log-group-limit | -4.75% | True |
| resident | range:uniform:1@8 | lazy-merge | +1.35% | True |
| resident | range:uniform:1@8 | async-durable-barrier | -6.06% | False |
| resident | range:uniform:1@8 | cache-invalid-addresses | -4.95% | True |
| resident | range:uniform:1@8 | directory-direct | -4.39% | True |
| resident | range:uniform:1@8 | directory-bucket | -18.35% | False |
| resident | range:uniform:1@8 | directory-bucket-simd | -18.65% | False |
| resident | range:uniform:1@8 | directory-bucket-atomic | -7.65% | False |
| resident | range:uniform:10000@8 | disk-format | -2.41% | True |
| resident | range:uniform:10000@8 | filtered-ranges | +1.43% | True |
| resident | range:uniform:10000@8 | log-group-limit | -1.49% | True |
| resident | range:uniform:10000@8 | lazy-merge | -5.38% | True |
| resident | range:uniform:10000@8 | async-durable-barrier | -4.49% | True |
| resident | range:uniform:10000@8 | cache-invalid-addresses | -2.56% | True |
| resident | range:uniform:10000@8 | directory-direct | -8.56% | False |
| resident | range:uniform:10000@8 | directory-bucket | -6.41% | True |
| resident | range:uniform:10000@8 | directory-bucket-simd | -6.44% | False |
| resident | range:uniform:10000@8 | directory-bucket-atomic | -6.85% | True |
| disk | point:uniform@8 | disk-format | -0.81% | True |
| disk | point:uniform@8 | filtered-ranges | -3.82% | True |
| disk | point:uniform@8 | log-group-limit | -0.25% | True |
| disk | point:uniform@8 | lazy-merge | -0.22% | True |
| disk | point:uniform@8 | async-durable-barrier | -2.05% | True |
| disk | point:uniform@8 | cache-invalid-addresses | -2.24% | True |
| disk | point:uniform@8 | directory-direct | -1.93% | True |
| disk | point:uniform@8 | directory-bucket | -2.54% | True |
| disk | point:uniform@8 | directory-bucket-simd | -2.53% | True |
| disk | point:uniform@8 | directory-bucket-atomic | -1.32% | True |
| disk | range:uniform:1@8 | disk-format | -0.78% | True |
| disk | range:uniform:1@8 | filtered-ranges | -1.67% | True |
| disk | range:uniform:1@8 | log-group-limit | -0.59% | True |
| disk | range:uniform:1@8 | lazy-merge | -0.98% | True |
| disk | range:uniform:1@8 | async-durable-barrier | -2.40% | True |
| disk | range:uniform:1@8 | cache-invalid-addresses | -2.34% | True |
| disk | range:uniform:1@8 | directory-direct | -3.11% | True |
| disk | range:uniform:1@8 | directory-bucket | -2.79% | True |
| disk | range:uniform:1@8 | directory-bucket-simd | -3.56% | True |
| disk | range:uniform:1@8 | directory-bucket-atomic | -2.25% | True |
| disk | range:uniform:10000@8 | disk-format | -0.41% | True |
| disk | range:uniform:10000@8 | filtered-ranges | -1.80% | True |
| disk | range:uniform:10000@8 | log-group-limit | +0.11% | True |
| disk | range:uniform:10000@8 | lazy-merge | -1.24% | True |
| disk | range:uniform:10000@8 | async-durable-barrier | -2.13% | True |
| disk | range:uniform:10000@8 | cache-invalid-addresses | -2.08% | True |
| disk | range:uniform:10000@8 | directory-direct | -2.14% | True |
| disk | range:uniform:10000@8 | directory-bucket | -2.08% | True |
| disk | range:uniform:10000@8 | directory-bucket-simd | -3.48% | True |
| disk | range:uniform:10000@8 | directory-bucket-atomic | -1.09% | True |
| prefix-resident-common | point:uniform@8 | branch-prefix | +10.61% | False |
| prefix-resident-common | range:uniform:1@8 | branch-prefix | +2.87% | False |
| prefix-resident-common | range:uniform:10000@8 | branch-prefix | +0.34% | True |
| prefix-resident-common | ow:uniform@8 | branch-prefix | +9.38% | False |
| prefix-resident-random | point:uniform@8 | branch-prefix | -0.30% | True |
| prefix-resident-random | range:uniform:1@8 | branch-prefix | +2.67% | True |
| prefix-resident-random | range:uniform:10000@8 | branch-prefix | -10.88% | False |
| prefix-resident-random | ow:uniform@8 | branch-prefix | -1.52% | True |
| prefix-disk-common | point:uniform@8 | branch-prefix | -2.69% | True |
| prefix-disk-common | range:uniform:1@8 | branch-prefix | +0.11% | True |
| prefix-disk-common | range:uniform:10000@8 | branch-prefix | +14.37% | False |
| prefix-disk-random | point:uniform@8 | branch-prefix | -5.57% | True |
| prefix-disk-random | range:uniform:1@8 | branch-prefix | -6.30% | True |
| prefix-disk-random | range:uniform:10000@8 | branch-prefix | -4.11% | True |
| repeat-directory-bucket-atomic | point:uniform@8 | directory-bucket-atomic | -6.18% | True |
| repeat-directory-bucket-atomic | range:uniform:1@8 | directory-bucket-atomic | -3.89% | True |
| repeat-async-durable-barrier | point:uniform@8 | async-durable-barrier | -0.52% | True |
| repeat-async-durable-barrier | range:uniform:1@8 | async-durable-barrier | -2.28% | True |
| repeat-cache-invalid-addresses | point:uniform@1 | cache-invalid-addresses | -4.54% | False |
| repeat-cache-invalid-addresses | point:uniform@8 | cache-invalid-addresses | -3.64% | True |
| repeat-cache-invalid-addresses | range:uniform:1@8 | cache-invalid-addresses | +0.03% | True |
| insert | WAL1 | disk-format | -0.88% | True |
| insert | WAL1 | filtered-ranges | -1.02% | False |
| insert | WAL1 | lazy-merge | -0.49% | True |
| insert | WAL1 | async-durable-barrier | -1.08% | True |
| insert | WAL1 | cache-invalid-addresses | -3.00% | False |
| insert | WAL1 | directory-direct | -1.91% | False |
| insert | WAL1 | directory-bucket | -5.46% | False |
| insert | WAL1 | directory-bucket-simd | -3.79% | False |
| insert | WAL1 | directory-bucket-atomic | -3.32% | False |
| insert | WAL1 | log-disabled | -1.34% | True |
| insert | WAL1 | log-default | -42.70% | False |
| insert | WAL0 | cache-invalid-addresses | +6.27% | True |
| insert | WAL0 | directory-bucket-atomic | +6.39% | False |

Detailed throughput ranges, CPU/op, I/O/op and p99 are in summary.csv. Raw commands, source/library/executable hashes, verification and logs remain in the per-workload manifest and adjacent artifacts.

{
  "insert": {
    "runs": 31,
    "complete": true,
    "verified": 31
  },
  "repeat-warmup": {
    "runs": 1,
    "complete": true,
    "verified": 1
  },
  "barrier": {
    "runs": 24,
    "complete": true,
    "verified": 24
  },
  "repeat-directory-bucket-atomic": {
    "runs": 4,
    "complete": true,
    "verified": 4
  },
  "fixtures": {
    "runs": 12,
    "complete": true,
    "verified": 12
  },
  "disk": {
    "runs": 23,
    "complete": true,
    "verified": 23
  },
  "repeat-async-durable-barrier": {
    "runs": 4,
    "complete": true,
    "verified": 4
  },
  "prefix-resident-common": {
    "runs": 4,
    "complete": true,
    "verified": 4
  },
  "hash": {
    "runs": 4,
    "complete": true,
    "verified": 4
  },
  "resident": {
    "runs": 23,
    "complete": true,
    "verified": 23
  },
  "repeat-cache-invalid-addresses": {
    "runs": 4,
    "complete": true,
    "verified": 4
  },
  "prefix-disk-random": {
    "runs": 4,
    "complete": true,
    "verified": 4
  },
  "prefix-disk-common": {
    "runs": 4,
    "complete": true,
    "verified": 4
  },
  "prefix-resident-random": {
    "runs": 4,
    "complete": true,
    "verified": 4
  }
}

## Main control drift

All controls are retained. Large variation in these unchanged-main samples prevents attributing small feature differences to code. The measured cause of this variation is unknown. Nearby ABBA repeats are separate evidence; they do not replace these results.

| Regime | Phase | First ops/s | Middle ops/s | Last ops/s | Middle vs first | Last vs first |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| resident | point:uniform@1 | 1,261,757.3 | 1,152,531.0 | 1,148,133.5 | -8.66% | -9.01% |
| resident | point:uniform@8 | 6,445,634.1 | 5,270,841.2 | 5,463,995.8 | -18.23% | -15.23% |
| resident | range:uniform:1@1 | 114,690.1 | 105,447.5 | 105,112.6 | -8.06% | -8.35% |
| resident | range:uniform:1@8 | 600,376.6 | 532,530.7 | 549,552.3 | -11.30% | -8.47% |
| resident | range:uniform:10000@8 | 4,305.3 | 3,785.8 | 3,929.7 | -12.07% | -8.72% |
| disk | point:uniform@8 | 153,061.5 | 162,899.9 | 168,110.0 | +6.43% | +9.83% |
| disk | range:uniform:1@8 | 63,458.1 | 63,625.4 | 64,975.4 | +0.26% | +2.39% |
| disk | range:uniform:10000@8 | 841.4 | 835.2 | 840.0 | -0.74% | -0.17% |

## Interpretation and limits

These samples support feature tradeoffs, not an isolated estimate of merge overhead. There are two samples per candidate and two or three main controls; non-overlap is a descriptive range check, not a statistical confidence interval. Unchanged main controls drifted substantially, including in nearby repeats. The cause is unproven.

The invalid-address checks reproduce a roughly 4.5% resident single-thread point-query loss: broad screening -4.60%, nearby warmed ABBA -4.54% with non-overlapping sample ranges. Its repeated eight-thread point query (-3.64%) overlaps main, and repeated one-result/eight-thread range query (+0.03%) does not reproduce the initial apparent loss. Atomic directory nearby repeats (-6.18% point and -3.89% short range) and async-barrier ordinary-query repeats (-0.52% point and -2.28% short range) all overlap main; their smaller effects remain inconclusive. The scalar/SIMD directory variants show larger resident feature costs in the broad screen. These are comparisons of the whole features to main, not newly introduced merge regressions.

Prefix compression improves several common-prefix workloads, but the random-key resident long-range screen loses 10.88% with non-overlapping samples. The default 64-page automatic log-group durability threshold loses 42.7% insert throughput in the short WAL-enabled screen; this is the cost of added automatic durability versus main without automatic barriers. Disabling the threshold gives -1.3% with overlapping ranges. These tradeoffs require workload-specific judgment, not a blanket performance-neutral claim.

All timed processes passed their declared verification. Query runs validate keys/values/ranges and close successfully. Insert-only runs validate API returns, positive insert counts and close, but do not perform readback. Barrier runs crash after the final completed barrier, reopen and verify every key/value. Hash-filter runs compare checksums and expected output counts. Every measured ordinary resident and focused-repeat query phase has zero physical read bytes. The retained pre-repeat warmup runs point and one-result range queries at eight threads for 10 seconds each; it is not pooled with comparison controls.

Separate correctness testing reproduced an inherited small-cache async progress stall on main and invalid-addresses under restricted CPU scheduling. That limitation is documented in the branch correctness evidence; it was not repaired in these merges, and no benchmark run stalled.

Barrier measurements are short and show substantial run-to-run I/O variation in some configurations. In particular, WAL-enabled/no-background-worker candidate sync spans about 60k to 194k updates/s, while main spans 63k to 65k; its +99% mean alone is not a stable speedup estimate. Async launch latency is distinct from durability completion. These runs use depth 1: completion is observed by waiting or polling before the next insert batch. Greater-depth harness modes may include intervening inserts in observed completion, but were not measured here.

| Barrier config | Candidate mode | Main updates/s range | Candidate updates/s range | Mean change | Mean launch p99 us | Mean observed completion p99 us |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| wal0-workers0 | sync | 31,072–40,897 | 57,552–58,998 | +61.95% | 15769.34 | 15769.34 |
| wal0-workers0 | async | 31,072–40,897 | 59,568–60,181 | +66.39% | 0.98 | 14594.80 |
| wal0-workers1 | sync | 32,686–33,069 | 32,567–33,066 | -0.19% | 21671.51 | 21671.51 |
| wal0-workers1 | async | 32,686–33,069 | 31,925–33,322 | -0.77% | 2.90 | 23050.30 |
| wal1-workers0 | sync | 63,288–64,560 | 60,090–194,153 | +98.86% | 4118.68 | 4118.68 |
| wal1-workers0 | async | 63,288–64,560 | 200,298–273,650 | +270.71% | 3.51 | 2190.87 |
| wal1-workers1 | sync | 61,607–63,583 | 67,545–69,246 | +9.27% | 4940.58 | 4940.58 |
| wal1-workers1 | async | 61,607–63,583 | 78,489–80,079 | +26.66% | 6.17 | 4128.32 |

Hash-filter API versus application filtering: mean latency changes for limits 1/2/10/100 are -5.70%/-16.71%/+0.56%/-9.59%; output checksums match. Full ranges, visited tuple counts, CPU/I/O and provenance are retained in the raw results and feature-summaries.json.
