# async-durable-barrier: main integration evidence

Source merge: `7c2fee09f88290d249be6bcdf4ae6ab2527b167f`; main: `7bb5fab305b9c6179e31f75998dabea833537a08`.

[Correctness checks](tests/README.md) include exact commands, hashes and retained initial failures. [Benchmark report](benchmarks/report.md), [project summary](benchmarks/summary.csv), and [verification status](benchmarks/status.json) describe the fresh real-disk screening.

All 24 barrier runs completed crash/reopen/full-readback verification. Barrier throughput varied substantially, so no stable throughput improvement is claimed. The nearby ordinary-query repeat changed eight-thread point/short-range throughput -0.52%/-2.28%, with sample ranges overlapping main.

The campaign ran 133 comparison processes, 12 fixture creators and one recorded warmup through 11 exclusive jobs. These compare each updated feature with main; they do not isolate merge cost. Main controls drifted; sample-range overlap is descriptive, not a statistical confidence interval. Query/hash/barrier runs validate data, while insert-only runs check API returns, positive counts and successful close without readback. All benchmark databases were on ext4 `/dev/mapper/snafu-root_crypt` with O_DIRECT and fsynced fixture copies.

`tests/archive-manifest.json` and `benchmarks/inventory.json` verify the small archived files. No database images, executables or core dumps are committed. Archived scripts retain original paths and must be adapted before replay outside the original campaign.
