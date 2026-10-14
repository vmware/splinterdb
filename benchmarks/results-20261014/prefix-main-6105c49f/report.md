# Prefix benchmark comparison against main 6105c49f

Main source: `6105c49f66a24d3006ba0464d9558b52e692e27c`. Candidate source: `97e5c0e38b818ae43cd3abdcf8d4489b7dbe3789`.

This compares the integrated prefix feature with current main; it does not isolate the cost of merging main.

CPU affinity 0–11, real disk with O_DIRECT, 64-byte keys, 100-byte values, fixed per-format images, fsynced copies, seed 42, one load thread, no background workers, and a 4 MiB memtable. Resident uses 200,000 keys and 128 MiB cache; disk uses 2 million keys and 64 MiB cache. Read phases have one second of warmup each. Query checks validate values and ordered ranges; overwrite checks up to 4,096 measured keys per worker after timing.

ABBA provides two samples per version in the main matrix. Repeats are shown separately. Means and sample ranges are descriptive; overlapping ranges are not confidence intervals. Small differences and tail estimates need cautious interpretation.

## Reads

Status: complete. 28 verified query invocations; measured phases 7 seconds. All included rows have zero API errors and correctness violations.

| Case / WAL | Phase | Main ops/s range | Candidate ops/s range | Mean throughput | CPU efficiency | Read B/op | Mean p99 | Ranges overlap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| resident-common / 0 | point:uniform@8 | 4,074,604.5–4,926,481.6 | 4,492,319.2–4,989,583.4 | +5.34% | +5.32% | n/a | -13.58% | yes |
| resident-common / 0 | range:uniform:1@8 | 1,072,409.9–1,256,510.3 | 1,165,847.2–1,185,895.4 | +0.98% | +0.97% | n/a | -1.32% | yes |
| resident-common / 0 | range:uniform:10000@8 | 3,877.3–5,473.7 | 3,481.6–4,469.4 | -14.97% | -14.98% | n/a | -11.32% | yes |
| resident-random / 0 | point:uniform@8 | 3,854,472.2–4,165,500.4 | 4,068,384.3–4,115,031.1 | +2.04% | +2.04% | n/a | -4.88% | yes |
| resident-random / 0 | range:uniform:1@8 | 1,063,328.2–1,092,590.9 | 1,057,405.9–1,091,214.3 | -0.34% | -0.34% | n/a | -1.23% | yes |
| resident-random / 0 | range:uniform:10000@8 | 3,106.0–3,484.7 | 3,293.9–3,752.7 | +6.92% | +6.91% | n/a | -11.00% | yes |
| resident-random / 0 | range:uniform:10000@1 | 1,159.1–1,328.0 | 1,218.0–1,286.0 | +0.68% | +0.68% | n/a | -9.24% | yes |
| resident-nonlexical / 0 | point:uniform@8 | 4,028,951.9–4,364,191.6 | 3,943,007.4–4,254,213.6 | -2.33% | -2.34% | n/a | +1.28% | yes |
| resident-nonlexical / 0 | range:uniform:1@8 | 1,024,771.4–1,058,596.2 | 985,488.4–986,207.5 | -5.36% | -5.35% | n/a | +4.82% | no |
| resident-nonlexical / 0 | range:uniform:10000@8 | 3,201.0–3,520.9 | 2,868.6–3,527.0 | -4.85% | -4.80% | n/a | +5.00% | yes |
| disk-common / 0 | point:uniform@8 | 163,698.1–176,798.2 | 181,356.1–194,725.4 | +10.45% | +10.96% | -9.06% | -16.48% | no |
| disk-common / 0 | range:uniform:1@8 | 68,021.3–73,146.7 | 76,025.5–80,473.3 | +10.86% | +11.42% | -22.75% | -2.50% | no |
| disk-common / 0 | range:uniform:10000@8 | 900.0–918.3 | 1,061.8–1,125.4 | +20.29% | +12.02% | -29.09% | -13.68% | no |
| disk-random / 0 | point:uniform@8 | 132,155.7–136,666.5 | 133,577.2–134,838.6 | -0.15% | +1.26% | +0.02% | -0.84% | yes |
| disk-random / 0 | range:uniform:1@8 | 53,370.4–56,396.0 | 55,082.8–56,052.0 | +1.25% | +1.14% | -0.13% | -6.25% | yes |
| disk-random / 0 | range:uniform:10000@8 | 788.7–801.1 | 787.5–796.8 | -0.35% | -0.14% | +0.28% | -2.65% | yes |
| disk-nonlexical / 0 | point:uniform@8 | 133,323.3–158,039.7 | 130,352.7–143,969.4 | -5.85% | -7.69% | +0.00% | +5.50% | yes |
| disk-nonlexical / 0 | range:uniform:1@8 | 54,996.1–65,537.3 | 55,657.5–60,041.5 | -4.01% | -3.95% | -0.05% | +0.00% | yes |
| disk-nonlexical / 0 | range:uniform:10000@8 | 766.3–829.0 | 780.0–820.7 | +0.34% | +1.50% | +0.19% | -15.57% | yes |
| resident-common-disabled / 0 | point:uniform@8 | 3,544,771.5–3,809,599.8 | 3,784,307.2–3,941,791.0 | +5.05% | +5.05% | n/a | -8.89% | yes |
| resident-common-disabled / 0 | range:uniform:1@8 | 892,793.3–1,004,344.2 | 913,518.7–977,689.1 | -0.31% | -0.32% | n/a | +0.00% | yes |
| resident-common-disabled / 0 | range:uniform:10000@8 | 3,405.3–4,432.8 | 2,982.7–3,280.3 | -20.10% | -20.09% | n/a | +2.85% | no |
| resident-common-disabled / 0 | range:uniform:10000@1 | 1,248.7–1,329.5 | 1,243.4–1,301.1 | -1.31% | -1.31% | n/a | +0.00% | yes |

## Writes

Status: complete. 16 verified query invocations; measured phases 8 seconds. All included rows have zero API errors and correctness violations.

| Case / WAL | Phase | Main ops/s range | Candidate ops/s range | Mean throughput | CPU efficiency | Read B/op | Mean p99 | Ranges overlap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| resident-common / 0 | ow:uniform@8 | 1,651,774.3–2,281,818.6 | 1,951,505.2–2,078,472.6 | +2.45% | +3.82% | -40.13% | +1.02% | yes |
| resident-random / 0 | ow:uniform@8 | 1,417,795.4–1,646,774.8 | 1,583,743.5–1,645,339.0 | +5.37% | -0.54% | -8.30% | +1.06% | yes |
| resident-common / 1 | ow:uniform@8 | 578,949.9–680,238.2 | 691,557.9–806,782.3 | +18.99% | -0.83% | -40.55% | -5.66% | no |
| resident-random / 1 | ow:uniform@8 | 570,998.2–581,315.3 | 484,679.1–565,478.7 | -8.87% | -19.90% | -14.58% | +10.00% | no |

Overwrite rates including close/drain:

| Case / WAL | Main ops/s | Candidate ops/s | Change |
| --- | ---: | ---: | ---: |
| resident-common / 0 | 1,932,737.1 | 1,975,842.9 | +2.23% |
| resident-random / 0 | 1,498,383.2 | 1,588,280.8 | +6.00% |
| resident-common / 1 | 620,393.3 | 735,223.1 | +18.51% |
| resident-random / 1 | 566,929.2 | 510,361.0 | -9.98% |

These bounded overwrites include one second of unmeasured warmup and post-measurement verification before close. The close-inclusive denominator adds measured time and close/drain time; it excludes warmup and verifier time. It is not a sustained-ingest or steady-state write-throughput measurement. WAL=1 exercises current main's automatic durable barriers; this harness does not measure recovery or count barriers.

## Repeat

Status: complete. 24 verified query invocations; measured phases 15 seconds. All included rows have zero API errors and correctness violations.

| Case / WAL | Phase | Main ops/s range | Candidate ops/s range | Mean throughput | CPU efficiency | Read B/op | Mean p99 | Ranges overlap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| resident-common / 0 | range:uniform:10000@8 | 3,909.3–4,235.1 | 3,136.6–4,012.2 | -12.10% | -12.10% | n/a | -4.03% | yes |
| resident-common-disabled / 0 | range:uniform:10000@8 | 3,365.5–4,357.7 | 3,613.8–3,853.3 | -2.38% | -2.39% | n/a | +15.11% | yes |
| resident-common-disabled / 0 | range:uniform:10000@1 | 1,334.6–1,377.4 | 1,373.1–1,381.2 | +1.89% | +1.88% | n/a | -7.48% | yes |
| resident-nonlexical / 0 | range:uniform:1@8 | 995,559.2–1,081,379.8 | 944,712.2–990,583.5 | -5.73% | -5.74% | n/a | +7.23% | no |
| resident-random / 1 | ow:uniform@8 | 771,902.2–1,050,078.9 | 809,333.6–880,444.9 | -5.90% | -1.00% | +0.07% | +1.59% | yes |

Overwrite rates including close/drain:

| Case / WAL | Main ops/s | Candidate ops/s | Change |
| --- | ---: | ---: | ---: |
| resident-random / 1 | 882,006.7 | 828,312.1 | -6.09% |

These bounded overwrites include one second of unmeasured warmup and post-measurement verification before close. The close-inclusive denominator adds measured time and close/drain time; it excludes warmup and verifier time. It is not a sustained-ingest or steady-state write-throughput measurement. WAL=1 exercises current main's automatic durable barriers; this harness does not measure recovery or count barriers.

## Fixture construction and space

Each fixture is constructed once. Load/drain numbers are setup observations, not repeated write benchmarks. Allocated file bytes include historical written pages; mounted live extents show allocator ownership. Per-page-type counters are not durable and are not used here.

| Fixture | Load + drain seconds | File allocation bytes | Live extents |
| --- | ---: | ---: | ---: |
| main-resident-common-wal0 | 0.352 | 37,408,768 | 342 |
| candidate-resident-common-wal0 | 0.346 | 26,128,384 | 256 |
| main-resident-random-wal0 | 0.443 | 37,408,768 | 342 |
| candidate-resident-random-wal0 | 0.422 | 37,408,768 | 342 |
| main-resident-nonlexical-wal0 | 0.379 | 37,408,768 | 342 |
| candidate-resident-nonlexical-wal0 | 0.409 | 37,408,768 | 342 |
| main-disk-common-wal0 | 4.245 | 833,761,280 | 3,414 |
| candidate-disk-common-wal0 | 4.071 | 544,198,656 | 2,585 |
| main-disk-random-wal0 | 4.740 | 833,708,032 | 3,415 |
| candidate-disk-random-wal0 | 4.872 | 833,789,952 | 3,415 |
| main-disk-nonlexical-wal0 | 4.913 | 833,634,304 | 3,414 |
| candidate-disk-nonlexical-wal0 | 4.847 | 834,351,104 | 3,414 |
| candidate-resident-common-wal0-disabled | 0.425 | 37,408,768 | 342 |
| main-resident-common-wal1 | 0.706 | 76,468,224 | 347 |
| candidate-resident-common-wal1 | 0.657 | 65,187,840 | 261 |
| main-resident-random-wal1 | 0.856 | 76,468,224 | 347 |
| candidate-resident-random-wal1 | 0.695 | 76,468,224 | 347 |

Raw per-phase samples and latency percentiles are in the mode CSV files. `summary.csv` retains mean and min/max throughput, CPU efficiency, read/write bytes per operation and p99/p99.9. Manifests retain commands, environment controls, timings, source and binary hashes, fixture hashes, and verification outcomes. Database images and binaries are excluded from the evidence archive.
