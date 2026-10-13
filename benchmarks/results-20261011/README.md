# Durable-barrier evidence, 2026-10-11

See ../../docs/async-durable-barriers.md for interpretation and validation.
Baseline is db3047a; production implementation is 9d66eee. The final changes
outside that commit only extend the benchmark/report. Exact measured harness
sources are retained here because the final source also received whitespace
formatting. Build JSON files and manifests identify their source/binary hashes.

The four initial matrices contain 48 measured runs plus four pilot runs. The
two *-io-control matrices add 24 measured runs with getrusage block-I/O counters.
All 72 measured runs and four pilots passed process-exit crash recovery and full
key/value verification. Commands, original absolute paths, timestamps and run
orders are in manifests; the archived runner documents the campaign layout.

The initial verified scratch images were deleted only after completed runs;
that reclamation is marked in their manifests. The controls use one scratch
path per campaign, unlinking after each successfully verified subprocess and
creating a fresh image next time. No databases are included. Full logs remain
in the original campaign directory under the main checkout's .cache.
