# Expanded allocation-counter benchmarks

Measured 2026-10-26 (America/Los_Angeles) on topic branch `robj/allocation-table`. This report supersedes the earlier unpinned three-repetition sample.

**Version scope:** these frozen benchmarks used 32-bit exact refcounts and the
format-2 dense32 disk map. The branch subsequently changed exact counts to
64 bits and introduced the format-3 four-bit disk snapshot with an overflow
chain. The results and memory figures below describe the measured older
binaries; they are not measurements of the current count width, disk format,
clean-close cost or recovery performance.

**The previous normalized-dense slowdown did not reproduce. Four-bit counters are a useful next prototype: they remove almost all observed hash activity, use one eighth of the dense array memory, and have a bounded throughput cost in the larger populated workload. No universal throughput winner is established.**

Completed 184 throughput runs with 2.34 billion inserts and full reopened scans, plus three instrumented diagnostic runs with another 30 million inserts. Every run passed its scan checks. Diagnostic timings are excluded from throughput analysis.

## Workload and controls

- Records: 24-byte keys and 100-byte values; distinct randomized insertion. 10M records are 1.15 GiB logical data; 30M are 3.46 GiB.
- Logging enabled, 16 MiB memtables, one memtable worker and four normal background workers; identical statistics settings and release compiler/link flags. All libraries use the same version string.
- Foreground workers are individually pinned to CPUs 0, 2, 4, 6, 1, 3, 5, 7; background workers inherit CPUs 8–11. This i7-12800H layout uses four performance cores with SMT for up to eight inserters and two separate performance cores for background work. Efficiency cores are excluded from the benchmark process. Frequency policy and thermal limits are not fixed.
- Every run creates a fresh database, inserts its fixed record count, performs a timed clean close, reopens, and scans every record. Verification checks the full key, value length and embedded value ID, not every payload byte. Verified new scratch images are removed only after their process stops; raw logs and evidence remain.
- Insertion timing excludes create, reopen, verification and census. Insertion+close includes clean close, excluding small histogram/bookkeeping gaps; it is not per-operation durable-commit throughput. Census printing occurs only after reopened verification.
- Six repetitions for the seven-variant baseline, with shifted variant order and matched seeds. Five repetitions for each five-variant resource case, giving each variant every order position once. Repetition counts were fixed before their respective measurements; no samples were discarded and no stopping-on-significance rule was used.
- Each successful campaign held both exclusive flock locks and obtained fresh pause acknowledgements from active projects. All local children were idle. Several unacknowledged reservations cancelled with zero measurements; they are not part of these results. All measurement locks have been released.

Configured disk capacity and populated size are distinct: the 1 TiB tests reserve a sparse capacity with 10M records, not 1 TiB of populated data. The 30M case increases populated data as well as cache and capacity. The dense map spans configured capacity; hot references need not touch every array page.

## Median throughput

Thousands of inserts per second. These medians are descriptive; paired confidence intervals below provide the uncertainty.

| Configuration: records / cache / capacity | Threads | Original | Dense | 2-bit | 4-bit | 8-bit |
|---|---:|---:|---:|---:|---:|---:|
| 10M / 128 MiB / 8 GiB | 1 | 503.7 | 514.7 | 502.2 | 509.3 | 511.4 |
| 10M / 128 MiB / 8 GiB | 8 | 857.7 | 907.2 | 891.6 | 853.2 | 913.6 |
| 10M / 1 GiB / 8 GiB | 8 | 1238.5 | 1252.3 | 1251.1 | 1249.8 | 1241.6 |
| 10M / 1 GiB / 1 TiB | 1 | 567.6 | 567.8 | 567.0 | 565.6 | 569.8 |
| 10M / 1 GiB / 1 TiB | 8 | 1216.8 | 1206.9 | 1225.5 | 1229.4 | 1216.9 |
| 30M / 4 GiB / 128 GiB | 8 | 1048.7 | 1039.2 | 1039.5 | 1035.9 | 1035.4 |

Including clean close:

| Configuration | Threads | Original | Dense | 2-bit | 4-bit | 8-bit |
|---|---:|---:|---:|---:|---:|---:|
| 10M / 128 MiB / 8 GiB | 1 | 493.0 | 503.5 | 490.8 | 498.8 | 501.0 |
| 10M / 128 MiB / 8 GiB | 8 | 827.8 | 869.9 | 857.0 | 813.8 | 874.2 |
| 10M / 1 GiB / 8 GiB | 8 | 1139.7 | 1148.6 | 1133.7 | 1140.0 | 1139.5 |
| 10M / 1 GiB / 1 TiB | 1 | 542.9 | 541.9 | 540.3 | 540.9 | 543.2 |
| 10M / 1 GiB / 1 TiB | 8 | 1106.1 | 1095.7 | 1100.0 | 1121.7 | 1101.8 |
| 30M / 4 GiB / 128 GiB | 8 | 1024.3 | 1016.2 | 1015.7 | 1010.6 | 1011.4 |

The larger cache raises observed eight-thread rates from roughly 0.85–0.91M/s at 128 MiB to 1.24–1.25M/s at 1 GiB for 10M records. This is a descriptive cross-configuration comparison from separate campaign blocks, not a paired causal estimate of cache scaling. Counter differences are much smaller than this change.

## Paired uncertainty and bounded conclusions

Changes use geometric means of matched-run throughput ratios, with Student-t 95% intervals on log ratios. Intervals are per comparison, without multiplicity adjustment; the independent-block approximation does not remove systematic bias. The prespecified equivalence band is ±3%. An interval crossing zero alone is not evidence of equivalence.

| Configuration | Threads | Comparison | Insertion change and 95% interval |
|---|---:|---|---|
| baseline | 1 | ownership / original | -0.30% [-1.63%, +1.06%] |
| baseline | 1 | dense / original | +2.23% [-0.84%, +5.40%] |
| baseline | 8 | dense / original | +5.41% [-2.99%, +14.54%] |
| baseline | 1 | compact4 / original | -0.61% [-2.54%, +1.35%] |
| capacity1t | 1 | dense / original | +0.26% [-1.73%, +2.29%] |
| capacity1t | 1 | compact2 / original | -0.08% [-1.84%, +1.73%] |
| capacity1t | 1 | compact4 / original | +0.91% [-1.93%, +3.82%] |
| capacity1t | 1 | compact8 / original | +0.68% [-0.74%, +2.12%] |
| large30m | 8 | dense / original | -0.68% [-2.22%, +0.89%] |
| large30m | 8 | compact2 / original | -2.41% [-10.42%, +6.33%] |
| large30m | 8 | compact4 / original | -0.42% [-2.09%, +1.27%] |
| large30m | 8 | compact4 / dense | +0.25% [-2.22%, +2.79%] |
| large30m | 8 | compact8 / original | -1.07% [-5.23%, +3.28%] |

Four-bit/original in the 30M case is also within ±3% including close: -0.55% [-2.33%, +1.26%]. The 1 TiB one-thread 2-bit and 8-bit comparisons also meet the ±3% criterion including close. Four-bit there has an interval extending above +3%, so strict two-sided equivalence is unresolved even though its lower bound excludes a 3% slowdown.

Eight-thread small-cache intervals remain wide. In the 30M case the 2-bit and 8-bit intervals also remain wider than ±3%; do not infer a universal tie or claim that either is definitively faster. One slow 2-bit 30M run is retained. The width-to-width intervals do not establish a throughput advantage for 4-bit over 2-bit. Four bits is the selected compact default because it greatly reduces overflow activity at a modest additional memory cost; this is a design choice, not a measured throughput win.

## Why normalized dense appeared slower

The old table reported noisy medians from three unpinned runs on a hybrid CPU. It did not establish that normalization caused a slowdown. The current controlled comparisons do not reproduce that conclusion.

Four exact source ablations isolate the shared changes:

- `original`: source snapshot 5733e6d9, whose implementation matches original 6e550e1e.
- `ownership`: only mini-allocator ownership cleanup and its required allocator format bump.
- `dense`: full prototype with the existing 32-bit array.
- `dense_direct`: full dense prototype with the original exact-refcount page-valid dispatch restored.

The ownership-only one-thread result is -0.30% [-1.63%, +1.06%], within ±3%. Dense/original is also within ±3% in both the large 30M/eight-thread case and the 1 TiB/one-thread case. The underlying dense get-refcount load has the same normal-path instruction sequence across original, ownership and dense.

The nullable allocated-state callback does add instructions on some cache-miss paths, but restoring the old dispatch did not produce an established speedup: dense_direct/dense is -4.13% [-9.17%, +1.19%] at one thread and -3.11% [-14.68%, +10.03%] at eight. Added instructions are a plausible cost, not a demonstrated explanation for the earlier table.

Exact attribution of the earlier fluctuation is unavailable. Core placement, frequency/thermal variation, I/O and scheduling are plausible contributors. Current snapshots also show substantial temperature and run-block variation, without proving a thermal cause. The defensible correction is that the original slowdown claim was unsupported, not that a particular environmental cause has been proved.

## Actual hash-path activity

Separate diagnostic binaries use global atomic counters, which perturb scheduling and performance. Their rates are invalid for comparison. Counts below cover the 10M-insert load interval with eight inserters and 128 MiB cache, including foreground/background work but excluding create, close, reopen and verification. Per-counter snapshots are atomic; the aggregate is not one instantaneous snapshot.

| Width | Exact overflow reads | Incref shard locks / increfs | Decref shard locks / decrefs | Total shard locks | Promotions |
|---|---:|---:|---:|---:|---:|
| 2-bit | 0 / 57,218 | 8,467 / 8,467 | 8,464 / 130,966 | 16,931 | 4,543 |
| 4-bit | 0 / 56,795 | 51 / 8,479 | 51 / 130,127 | 102 | 9 |
| 8-bit | 0 / 57,203 | 0 / 8,472 | 0 / 130,893 | 0 | 0 |

Four-bit reduces observed increment/decrement shard acquisitions by 99.40%, from 16,931 to 102. Eight-bit has zero observed get/inc/dec overflow paths. Four-bit therefore almost avoids the hash table, but does not eliminate it: nine promotions occurred during insertion. Even 2-bit uses only about 0.00169 shard locks per insert. Approximately 2.18M allocated-state checks per diagnostic bypass exact refcounts and the hash table by design. This low absolute hash frequency is consistent with the absence of a large throughput gain from wider counters; it is not a cycle-level attribution.

These are ordinary small-record insertion workloads. More sharing from clones, snapshots, or shared blobs can change the distribution. Zero observed overflow use is not a guarantee for all workloads.

## Space and implementation

| Configured capacity | Dense32 array | 2-bit array | 4-bit array | 8-bit array |
|---|---:|---:|---:|---:|
| 8 GiB | 256 KiB | 16 KiB | 32 KiB | 64 KiB |
| 128 GiB | 4 MiB | 256 KiB | 512 KiB | 1 MiB |
| 1 TiB | 32 MiB | 2 MiB | 4 MiB | 8 MiB |

Packed containers request an additional 8,256 bytes. After reopened scans, 2-bit retained roughly 10–12 KiB of overflow capacity at 1 TiB, with two live entries; 4/8-bit had no overflow allocation in those post-reopen snapshots. These are not peak insertion footprints. Heap headers and temporary resize overlap are excluded.

`BUILD_RC_BITS=2|4|8` selects the packed width with `BUILD_RC_COMPACT=1`; the selected default width is now 4. The measurements used explicit widths, so this default change does not alter the recorded results. The all-ones raw tag redirects to the hash table: 3/15/255. All measured variants preserve full 32-bit refcounts in overflow. Every width, including 8-bit, uses 64-bit word CAS; an independent byte-atomic 8-bit backend was not tested.

The existing experimental OOM limitation remains: surrounding ownership retains are infallible, so an overflow allocation failure still fail-stops in the opt-in adapter. Wider counters reduce this exposure without making the API fallible. No production default or checkpoint protocol changed in the measured campaign.

## Validation and evidence

Counter tests passed 66 debug cases and 42 release cases across private/shared heaps and all three widths. Four/eight-bit integration passed 66 cases each. Four ablation variants and three packed variants passed controlled-affinity create/reopen smoke checks. Modified C files match clang-format 19; diff whitespace checks passed.

Tracked raw inputs and analysis: [baseline CSV](results/expanded-20261026/baseline.csv), [resource CSV](results/expanded-20261026/resources.csv), [single-thread capacity CSV](results/expanded-20261026/capacity-single.csv), [diagnostic counts](results/expanded-20261026/diagnostic-counts.jsonl), [summary JSON](results/expanded-20261026/summary.json), [checksums](results/expanded-20261026/sha256.json).

Full local evidence:

- `.cache/rc-expanded/{baseline,resources-c,capacity-single-b}/`: exact commands, raw logs, phase markers, machine snapshots, frozen manifests, requests/ACKs/releases, verification and scratch-disposal journals.
- `.cache/rc-expanded/combined-analysis/`: complete generated analysis and census extracts.
- `.cache/rc-expanded-binaries/`: frozen common-harness binaries and source/library/binary hashes.
- `.cache/rc-expanded-ablation/`: four source snapshots, exact build commands, smoke validation and code-generation audit.
- `.cache/rc-expanded-instrument/`: separate instrumented source snapshot, patch, harness, binaries and counter semantics.
- `.cache/rc-expanded-validation/`: width-specific correctness evidence.

Reproduction scripts are `prepare_ablation.py`, `prepare_controlled.py`, `run_expanded.py`, and `analyze_expanded.py`. Exact measured runner versions are archived per campaign. The final runner now acknowledges another owner while queued for the reservation, preventing this launcher from silently waiting while another owner awaits its pause acknowledgement, and preserves releases by request ID. This coordination-only change followed the measured campaigns.
