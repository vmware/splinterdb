# log-group-limit: main integration evidence

Source merge: `b4bc273a1beda264aa1220b32978ba9d66ba4050`; main: `7bb5fab305b9c6179e31f75998dabea833537a08`.

[Correctness checks](tests/README.md) include exact commands, hashes and retained initial failures. [Benchmark report](benchmarks/report.md), [project summary](benchmarks/summary.csv), and [verification status](benchmarks/status.json) describe the fresh real-disk screening.

The default 64-page threshold reduced short-run WAL insertion throughput by 42.70% versus main without automatic barriers. This is the cost of added durability. With the threshold disabled, throughput changed -1.34% and sample ranges overlapped main. The merge also validates automatic durability of multilevel-blob pointer pages.

The campaign ran 133 comparison processes, 12 fixture creators and one recorded warmup through 11 exclusive jobs. These compare each updated feature with main; they do not isolate merge cost. Main controls drifted; sample-range overlap is descriptive, not a statistical confidence interval. Query/hash/barrier runs validate data, while insert-only runs check API returns, positive counts and successful close without readback. All benchmark databases were on ext4 `/dev/mapper/snafu-root_crypt` with O_DIRECT and fsynced fixture copies.

`tests/archive-manifest.json` and `benchmarks/inventory.json` verify the small archived files. No database images, executables or core dumps are committed. Archived scripts retain original paths and must be adapted before replay outside the original campaign.
