# cache-invalid-addresses: main integration evidence

Source merge: `f2dc7bf6dcd21214b03fd7e18ec91b940d50f30a`; main: `7bb5fab305b9c6179e31f75998dabea833537a08`.

[Correctness checks](tests/README.md) include exact commands, hashes and retained initial failures. [Benchmark report](benchmarks/report.md), [project summary](benchmarks/summary.csv), and [verification status](benchmarks/status.json) describe the fresh real-disk screening.

Resident single-thread point throughput lost 4.60% in the broad screen and 4.54% in the nearby warmed repeat, with non-overlapping observed sample ranges. The repeated eight-thread point result (-3.64%) overlaps main, and short ranges (+0.03%) do not reproduce the earlier loss. Short-run WAL inserts lost 3.00%. Control drift still limits precise attribution; performance neutrality is not established. The inherited async allocation stall documented above is a separate correctness/progress limitation.

The campaign ran 133 comparison processes, 12 fixture creators and one recorded warmup through 11 exclusive jobs. These compare each updated feature with main; they do not isolate merge cost. Main controls drifted; sample-range overlap is descriptive, not a statistical confidence interval. Query/hash/barrier runs validate data, while insert-only runs check API returns, positive counts and successful close without readback. All benchmark databases were on ext4 `/dev/mapper/snafu-root_crypt` with O_DIRECT and fsynced fixture copies.

`tests/archive-manifest.json` and `benchmarks/inventory.json` verify the small archived files. No database images, executables or core dumps are committed. Archived scripts retain original paths and must be adapted before replay outside the original campaign.
