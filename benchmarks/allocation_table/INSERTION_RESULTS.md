# Allocation-table insertion macrobenchmarks

Superseded by the [expanded controlled comparison](EXPANDED_RESULTS.md); retained as the initial unpinned sample.

Measured 2026-10-26. These are the first macro results for the allocator prototype on `robj/allocation-table` (`efde7cf1`). The packed backend has no consistent throughput advantage over the normalized dense backend in this sample. All 18 runs passed a persisted full-record scan.

## Workload and coordination

- Each new database receives 10,000,000 distinct records in randomized order: 24-byte keys and 100-byte values, 1.15 GiB logical data.
- 128 MiB cache, 16 MiB memtable, 8 GiB configured database capacity, logging enabled; one memtable background worker and four normal background workers.
- One or eight foreground insertion threads; three repetitions with matched seeds 42/43/44 and rotated original/dense/compact order. Each run closes, reopens, and scans all 10M records, checking each full key, value length and embedded value ID. The scan does not compare every payload byte.
- Original uses source snapshot `5733e6d9` (code unchanged from `6e550e1e`); normalized dense and packed use the tested prototype sources. All three frozen binaries and source hashes were verified against their manifest before running. Identical release flags and statistics settings.
- Exclusive `benchmark.lock` and `machine.lock` held for measurement. The only other active project, blob-keys, acknowledged the exact request ID with no active commands/children; the remaining registered projects had completed. Our child reviewer finished before reservation.
- Measurement window: 346.5 seconds, excluding pause-acknowledgement wait. Request `allocation-table-7db37931af2447f88c30022bd3ce16cb`, release complete with 18 commands. Both locks are released.

## Throughput

Rates below are thousands of inserts per second; the range is the minimum and maximum of three runs, not a confidence interval. Insertion+close is `10M / (insert_s + drain_s)` calculated per run, then summarized.

| Foreground threads | Allocator | Median insertion k/s | Range k/s | Median insertion+close k/s | Median close s |
|---:|---|---:|---:|---:|---:|
| 1 | original | 460.8 | 458.5–471.8 | 452.3 | 0.401 |
| 1 | dense | 441.3 | 411.1–446.3 | 433.2 | 0.422 |
| 1 | compact | 422.0 | 369.9–450.3 | 414.6 | 0.424 |
| 8 | original | 903.2 | 820.2–925.0 | 870.4 | 0.408 |
| 8 | dense | 820.9 | 815.5–893.8 | 795.4 | 0.408 |
| 8 | compact | 924.9 | 778.4–934.6 | 891.3 | 0.409 |

The packed/dense ratios of medians are -4.4% with one thread and +12.7% with eight threads. Paired runs show why these medians alone do not establish a repeatable regression or improvement:

| Threads | Packed relative to | Repetition 1 | Repetition 2 | Repetition 3 |
|---:|---|---:|---:|---:|
| 1 | original | -19.74% | -7.95% | -4.56% |
| 1 | dense | -17.13% | +2.67% | +2.03% |
| 8 | original | -5.10% | +1.03% | +2.40% |
| 8 | dense | -4.55% | +13.85% | +3.48% |

The packed build was slower than the original in all three one-thread runs; normalized dense was also slower than the original in all six matched comparisons. Neither pattern identifies a cause. In particular, original-versus-dense measures the full shared prototype changes (including ownership normalization and validity-check dispatch), not only the removed mini-root incref.

## Interpretation and limitations

- This i7-12800H has six performance cores with SMT (CPUs 0–11) and eight efficiency cores (12–19). The measured campaign allowed CPUs 0–19 without thread pinning. Scheduling on different core types is a plausible source of variance, not a demonstrated explanation. CPU frequency policy was not fixed.
- Only three samples per configuration; no confidence or tail-neutrality claim. The packed one-thread rate spans 369.9–450.3 k/s, and the eight-thread rate spans 778.4–934.6 k/s.
- At 8 GiB capacity and 128 KiB extents, the dense map is only 256 KiB; the packed tag array is 16 KiB plus its container and retained overflow tables. This workload does not test the memory-pressure benefit at much larger configured capacities. No overflow census or measured allocation-memory claim is made here.
- `KVBENCH_STATS_BEFORE_CLOSE` is unset: printing a census between insertion and close would allow background draining during an untimed gap. Statistics collection remains enabled equally in all three builds.
- Insertion+close excludes creation, small histogram/bookkeeping gaps, reopen and verification. It is not whole-process throughput or per-operation durable-commit throughput.
- All 36 load/scan CSV rows report exactly 10M operations and zero errors/violations; no forced-close or other diagnostic marker appeared. All 18 commands exited 0. Total tested: 180M inserts and 180M verified records.
- Process I/O write amplification including close was approximately 4.06–4.08 with one thread and 4.75–4.87 with eight threads. No benchmark major faults or system swap-outs were recorded. Small systemwide swap-in deltas totaled 113 pages over the full campaign; these counters are not process-specific.
- Coordination establishes that registered agents paused. The recorded process listing is sandbox-local and cannot certify absence of every unrelated host activity.

A follow-up was prepared with every variant restricted to the performance-core CPU set, six repetitions using all six variant permutations, and removal of scratch databases only after successful persisted verification. Request `allocation-table-d8ee062a2d9e4cf5b085d75ae3fc15f4` received no fresh peer acknowledgement within 180 seconds. It cancelled and released both locks with **zero measurements**. No P-core-only result is reported.

## Evidence and reproduction

Raw data and all frozen inputs remain in this worktree:

- [Raw CSV](../../.cache/rc-insertion-20261026-a/database.csv), [summary JSON](../../.cache/rc-insertion-20261026-a/summary.json), [exact commands](../../.cache/rc-insertion-20261026-a/commands.jsonl).
- [Frozen binary manifest](../../.cache/rc-insertion-20261026-a/manifest.json), [archived runner](../../.cache/rc-insertion-20261026-a/run_comparison.py), [environment](../../.cache/rc-insertion-20261026-a/environment.json), [evidence hashes](../../.cache/rc-insertion-20261026-a/evidence-sha256.json).
- [Pause acknowledgements](../../.cache/rc-insertion-20261026-a/acknowledgements.json), [completed release](../../.cache/rc-insertion-20261026-a/release.json), [cancelled follow-up](../../.cache/rc-insertion-20261026-pcore/release.json).
- Per-run logs and all 18 verified database images remain in `.cache/rc-insertion-20261026-a/`. The cancelled follow-up created no database images.

The measured command, from the topic worktree (stop local children first):

```sh
python3 benchmarks/allocation_table/run_comparison.py \
  --manifest .cache/rc-comparison-binaries-v2/manifest.json \
  --output .cache/NEW-INSERTION-OUTPUT \
  --coordination /home/rob/projects/splinterdb/.cache/projects-20261025/coordination \
  --locks /home/rob/projects/splinterdb/.cache/projects-20261011/coordination \
  --suite insertion --threads 1 8 --keys 10000000 --repetitions 3 \
  --command-timeout 600 --ack-timeout 180 --expected-minutes 20
```

For the prepared P-core follow-up on this host, prefix the command with `taskset -c 0-11`, select a new output directory, and use `--repetitions 6 --discard-verified-databases`. CPU topology must be rechecked on another host. The runner requests a fresh coordinated pause and aborts if any active project does not acknowledge.
