# Scan32 remeasurement after the Scan4096 fix

The iterator assignment fix substantially improves direct B-tree Scan32, but
the branch still misses the no-small-key-regression objective. The largest
remaining median time increases are 14.1% for 64-byte random lexical keys,
12.0% for the 64-byte custom comparator, and 9.4% for 256-byte random keys.
Shared-prefix cases show 1.8–2.6% median overhead.

These are fresh October 28 measurements of main `d9a61a9a`, the rebased native
candidate before the fix (`fdbffde0`), and the fixed candidate (`737b6161`).
They reuse the frozen binaries and identical harness from the
[Scan4096 investigation](SCAN4096_PROFILE.md). Small keys remain inline.
There are no new source changes in this measurement.

This report records the pre-profiling candidate. The subsequent
[Scan32 profile and fixes](SCAN32_PROFILE.md) supersede its current-performance
conclusion; the original measurements below remain unchanged.

## Results

Each percentage is the median of five paired elapsed-time changes. Positive
means slower than main. The last column is the smallest and largest of the
five fixed/main changes, not a confidence interval.

| Key bytes | Distribution / comparator | Before fix vs main | Fixed vs main | Fixed paired range |
|---:|---|---:|---:|---:|
| 24 | Random lexical | +33.8% | +7.1% | +0.1% to +14.0% |
| 24 | Shared prefix | +21.3% | +1.8% | -0.9% to +6.4% |
| 64 | Random lexical | +36.3% | +14.1% | +10.4% to +15.3% |
| 64 | Shared prefix | +18.0% | +2.6% | +1.7% to +2.6% |
| 64 | Custom comparator | +31.1% | +12.0% | +8.9% to +18.8% |
| 256 | Random lexical | +23.8% | +9.4% | +8.0% to +11.4% |
| 256 | Shared prefix | +18.6% | +2.6% | +2.0% to +2.6% |
| 512 | Random lexical | +10.9% | +1.0% | -2.0% to +2.7% |
| 512 | Shared prefix | +17.2% | +2.5% | +2.1% to +2.9% |

In throughput terms, the largest remaining median losses are 12.4% (64 random),
10.7% (64 custom), and 8.6% (256 random). Time increases and throughput losses
use different denominators; the CSV records both.

The fix reduces Scan32 time by 9.5–18.9% versus the before-fix candidate across
these nine cases. Including that control in the same windows isolates the
benefit from the change of main baseline since the
[October 26 report](SMALL_KEYS.md). Its historical percentages should not be
treated as a paired before/after comparison with this campaign.

The residual 64-byte random and custom-comparator slowdowns occur in all five
pairs. The 24-byte cases are noisier, and the 512-byte random range includes
zero. This sweep does not diagnose the residual overhead. Iterator setup and
search are candidates for follow-up profiling because short scans repeat those
steps more often per returned tuple, but no new CPU profiles were collected here.

## Method and validation

- Intel Core i7-12800H, process pinned to CPU 2; GCC 14.2.0, matching
  `-O3 -ffast-math -flto -march=native` builds.
- Nine cases, three variants, five paired repetitions: 135 runs. Seeds 42–46;
  main/before-fix/fixed order alternates with its reverse.
- 65,536 keys for 24/64-byte cases; 32,768 for 256/512-byte cases.
  Values are 32 bytes, cache 512 MiB, configured device 2 GiB.
- Each run executes two measured batches of 131,072 range starts, returning
  exactly 32 tuples per range: 262,144 ranges and 8,388,608 tuples.
- Full replay warms the exact access sequence and verifies keys/values outside
  timing. Measured checksums, post-measurement replay, full-tree verification,
  and allocator reclamation all passed.
- All 135 runs were resident. Measured process and hooked read/write I/O,
  including asynchronous I/O, and major faults were zero.
- Leaf counts, compressed-leaf counts, prefix bytes, returned counts and
  checksums matched across all three variants for every case/seed.
- Binary hashes were checked before every window. All 110 recorded fixed
  source/header hashes also match the current checkout.
- The user confirmed no other agents were active; the live agent list contained
  only the root. Both benchmark and machine locks were held exclusively during
  each measurement window. No other work ran concurrently from this agent.

The samples measure resident single-thread B-tree scans. They do not establish
cold-cache, long-running, or other-hardware performance. The paired ranges retain
observed variation; CPU pinning and alternating order do not eliminate clock
or thermal drift. The existing correctness suite results are recorded in the
Scan4096 report; this task changed only measurement artifacts and documentation.

## Evidence

- [All 135 timing and validation rows](scan32-results-20261028/runs.csv)
- [Paired comparisons, absolute times and throughput](scan32-results-20261028/comparison.csv)
- [Commits, hashes, coordination windows and validation](scan32-results-20261028/evidence.json)
- [Raw stdout/stderr, commands, manifests, runner and summarizer](../../.cache/scan32-bench-20261028/)
- [Frozen binaries, source hashes and identical harness](../../.cache/scan4096-profile-20261028/)

The raw runner records the user's no-other-agents confirmation for this
campaign. Re-establish coordination before reusing it; that recorded assumption
does not authorize a future uncoordinated run. Each window's manifest contains
its exact invocation. The summarizer can be rerun from the worktree root with
`python3 .cache/scan32-bench-20261028/summarize.py`.
