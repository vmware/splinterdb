# Project 1 measurement evidence

See [the report](../../../docs/cache-errors-results.md) for interpretation and
validation. `environment.json` describes the machine and storage. Read campaigns
contain per-phase CSVs and manifests with exact commands and executable hashes.
Insertion campaigns contain one summary per run, an aggregate `summary.csv`,
and the command/hash manifest. Full interval traces, logs, database images, and
binary profiles remain in the original campaign directories named in those
manifests; they are not included in this compact export.

`binaries/` records how the identical baseline benchmark sources were linked
against the original and project 1 libraries. The original source revision is
`db3047a8`. The measured project 1 implementation is `068fef0`, followed by
formatting-only commit `1a8190a`. Later report/comment commits do not change the
measured executable. The `resident-log0-repeat15` directory records the abandoned
16-byte receipt implementation (`c409c6b`); the directories containing `receipt`
record the final 8-byte implementation.

For reproduction, build the libraries at the stated revisions, link the common
baseline sources using the commands in `binaries/`, and replay the manifest
commands with local executable/output paths. Each read run must start from a
copy of the same clean baseline fixture. Insertion runs start with a fresh file.
Keep database files on a real disk, use the recorded affinity and settings, and
serialize timed runs against other builds/tests. The campaign's
`coordination/run_job.py` enforces both constraints. Do not combine results from
different campaign directories as if they were contemporaneous samples.

The profile text locates sampled functions. `cpu_core` stat counters are usable;
near-zero-runtime `cpu_atom` extrapolations are not. Counters include process
startup, warmup, and close; the report states the resulting attribution limit.

`prefetch-admission/` records the later unpublished-prefetch race reproduction,
repair, corrected full-suite validation, and new release provenance. Its
`invalid-zero-selection/` logs are explicitly invalid validation.
`disk-log0-prefetch-guard/` compares that repair with this branch's own frozen
pre-guard executable; its exceptionally slow first baseline prevents causal
performance attribution. The earlier campaign binaries were preserved.
