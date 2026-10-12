# Project 2 measurement evidence

See [the report](../../../docs/cache-handoff-results.md) for implementation,
validation, and interpretation. Each read campaign contains per-phase CSVs and
the command/executable-hash manifest. `environment.json` records the machine and
disk. `binaries/` records the common-source builds for the project 1 baseline,
the earlier 16-byte handoff receipt, and the 8-byte tagged receipt experiment.

Here `baseline` in a read CSV means project 1's compact implementation, not the
original database. In `resident-tagged10`, `unpacked` is the 16-byte handoff
receipt and `candidate` is the 8-byte tagged receipt. The other read directories
compare the 16-byte receipt with project 1. The branch retains the 16-byte
receipt; the tagged layout was reverted after mixed measurements. Shared insertion evidence lives in
the adjacent `cache-errors` directory inherited from project 1; its manifests
name all implementations explicitly.

Full logs, interval traces, images, and executables remain in the original
campaign paths named by the manifests. To reproduce, build the stated revisions
and common baseline benchmark sources, then replay the recorded commands with
local executable/output paths. Preserve the ordering, storage, affinity, cache
sizes, and input fixture. Serialize benchmarks against other work and keep the
database on a real disk. The campaign's `coordination/run_job.py` enforces this.
The same-file disk campaign intentionally reopens one physical file; the other
read campaigns copy the baseline fixture separately for each run.
