# Final fixed-four-bit allocator validation and throughput

The final implementation replaces the dense allocator with fixed four-bit tags
and exact unsigned 64-bit overflow counts. There is no backend or counter-width
build/runtime option. The source was frozen and tested on 2026-10-27;
[validation.json](results/final-20261027/validation.json) binds the production
sources, binaries, tests and raw measurement evidence to their hashes.

## Throughput

All 48 prespecified runs completed: 720 million inserts followed by 720 million
records checked in reopened scans. The full PR is compared with main at
`6e550e1e`; this is not an attribution of cost to packing alone. Both binaries
use the same controlled harness and release compiler flags.

Rates are median thousands of inserts per second. Changes are geometric means
of six paired ratios, with per-comparison 95% Student-t intervals on log ratios.

| Records / cache / configured capacity | Threads | Main k/s | Four-bit k/s | Paired change | 95% CI |
|---|---:|---:|---:|---:|---|
| 10M / 128 MiB / 8 GiB | 1 | 477.5 | 485.5 | +0.62% | [-2.05%, +3.37%] |
| 10M / 128 MiB / 8 GiB | 8 | 837.1 | 842.7 | +4.45% | [-4.29%, +13.99%] |
| 10M / 1 GiB / 1 TiB | 8 | 1160.0 | 1161.3 | -1.37% | [-4.49%, +1.86%] |
| 30M / 4 GiB / 1 TiB | 8 | 977.6 | 980.7 | +1.07% | [-1.00%, +3.18%] |

None of the insertion intervals excludes zero, and none fits wholly within
our prespecified ±3% equivalence band. The single-thread small-cache and 30M
cases exclude a 3% slowdown, but the eight-thread small-cache and 1 GiB-cache
cases still permit slowdowns of roughly 4–5%. These samples do not establish
universal throughput neutrality or a speedup. No samples were discarded or
extra repetitions added after inspecting the results.

## Shutdown and reopen

The following paired changes include insertion and clean close. Close drains
pending work as well as writing the allocation snapshot; it is not an isolated
codec benchmark. Close/reopen durations are sample medians from CSV values
rounded to milliseconds. Reopen includes the complete database mount path.

| Case / threads | Insertion + close change (95% CI) | Close main → four-bit | Reopen main → four-bit |
|---|---|---:|---:|
| baseline / 1 | +0.40% [-2.16%, +3.03%] | 0.446 → 0.487 s | 0.009 → 0.027 s |
| baseline / 8 | +4.20% [-4.14%, +13.28%] | 0.496 → 0.495 s | 0.010 → 0.026 s |
| capacity1t / 8 | -2.18% [-5.06%, +0.79%] | 0.865 → 0.909 s | 0.510 → 0.622 s |
| large30m / 8 | +0.81% [-1.38%, +3.04%] | 0.915 → 0.941 s | 0.552 → 0.618 s |

Reopen medians increased by about 17–112 ms in these samples. The snapshot
format saves array space and decouples disk records from hash-table layout; this
campaign does not demonstrate a faster mount. These are same-process clean
reopens, not cold-start measurements.

Exploratory paired timing diagnostics (unadjusted 95% intervals) found mean
reopen increases of 95 ms [41, 150] for the 10M/1 GiB case and 53 ms [9, 96] for
the 30M/4 GiB case. The 10M/1 GiB close increased by 93 ms [6, 181]. These measure
whole operations and do not isolate the snapshot codec's contribution.

## Measurement controls and limits

- Fixed plan: [plan.json](results/final-20261027/plan.json) and
  [configs.json](results/final-20261027/configs.json). Six pairs per case;
  variant order balanced across repetitions; matched seeds within each pair.
- Intel Core i7-12800H; foreground CPU pool `0,2,4,6,1,3,5,7`, background
  pool `8,9,10,11`; logging enabled, 16 MiB memtable, one memtable worker
  and four normal background workers. CPU clocks and thermal state were not
  fixed. The governor was `powersave`; sampled CPU0 frequencies ranged from
  3.54 to 4.67 GHz, with CPU/package temperatures reaching approximately 99°C.
  The first current run in each reservation started cooler than later blocks.
  Raw snapshots retain these conditions; balanced ordering does not eliminate
  their possible effect on confidence intervals or establish thermal causality.
- Both benchmark windows held `benchmark.lock` and `machine.lock` exclusively.
  All child agents were complete/idle; every active project had a matching
  pause acknowledgement. Each window has its own request and release record.
- 1 TiB is configured sparse capacity, not populated data size. Records use
  24-byte keys and 100-byte values. This workload does not stress heavy sharing
  from clones, snapshots or shared blobs.
- Reopened scans check every key, value length and embedded ID, not every
  payload byte. Short macro runs do not establish tail-latency neutrality.
- Confidence intervals are per-comparison, with no multiplicity adjustment.
  Pairing and intervals do not remove systematic machine drift.

## Correctness validation

- **606/606 integration cases:** debug, release and ASan on private/shared
  heaps; public quick (66), ondisk ownership (4), trunk split (6), GC (2),
  async trunk (2), superblock (17), and four retain-failure cases per mode/heap.
- **128 focused allocator/container cases passed**, with six existing optional
  allocation-interposer skips, across debug/release/ASan and both heaps.
  The four focused sanitizer suites passed ASan plus LeakSanitizer outside
  the sandbox. The initial sandbox ptrace failure is retained separately.
- **32 microbenchmark correctness cases passed** with timing disabled. Promotion
  tests cross raw 14↔15 and verify overflow entry creation/removal; high/hot
  cases remain in overflow. The dense64 checked-CAS control exists only in the
  standalone benchmark, never as a production backend.
- Fresh builds use ordinary flags, with no allocator selector. All debug test
  targets and tools compile. Production/test scans find no removed selector
  macros; changed C files pass formatting and diff whitespace checks.
- Integration ASan disables LeakSanitizer; the focused LSan result does not
  extend to the full integration suite. These are software failure/recovery
  checks, not power-loss or torn-sector testing.

## Evidence and reproduction

Raw [baseline CSV](results/final-20261027/baseline.csv),
[resource CSV](results/final-20261027/resources.csv),
[analysis](results/final-20261027/summary.json), and
[source/evidence manifest](results/final-20261027/validation.json) are tracked.
Full local commands, logs, frozen binaries and machine snapshots remain in:

- `.cache/rc-final/{baseline,resources,analysis}/`;
- `.cache/rc-final-binaries/`;
- `.cache/rc-final-integration/`;
- `.cache/rc-fixed4-validation/`.

Preparation uses `prepare_final.py --output NEW_DIRECTORY`; it freezes main
`6e550e1e` and the current working source and builds release binaries. Run it
under the shared activity locks. Then, with all children idle, invoke
`run_expanded.py` with the prepared manifest, six repetitions and variants
`original current`: baseline config with `--threads 1 8`, resource configs
with `--threads 8`, and fresh output directories. The runner handles exclusive
reservations and matching pause acknowledgements. Analyze both CSVs with
`analyze_expanded.py --baseline original` and the recorded configurations.
