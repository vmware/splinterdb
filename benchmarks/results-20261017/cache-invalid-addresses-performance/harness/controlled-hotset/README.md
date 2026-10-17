# Controlled cache probe with an explicit working set

This separate variant copies the frozen `../controlled-final` probe and runner.
All original C, binary, object, build metadata, and runner files are preserved
unchanged in `baseline/`; `provenance.json` records their hashes. The new probe
has not been built or run during source preparation. Root owns serialized
builds, smoke tests, and measurement under the campaign machine lock.

`CACHE_PROBE_WORKING_PAGES` selects an exact number of pages during setup,
between 32 and 75% of the configured cache's page capacity. It supports resident
and nonblocking-miss scenarios and rejects churn. Unset retains the original
working-set calculation and rounding. A partial final extent is written in full
and zero-filled beyond the requested set; only requested pages enter the
identity chain, prewarm, or measurement. Setup logs distinguish the logical
`working_set_bytes` from actual full-extent `disk_bytes` written.

The runner exposes `--working-pages`, records it in manifest arguments and each
child's environment, and verifies actual CSV `working_pages`. Without that
option, it rejects an inherited override rather than silently measuring a
different working set. Existing affinity, deterministic thread registration,
identity checks, timing boundaries, counters, no-I/O checks, preconditions,
rotating mirrored blocks, and raw telemetry are unchanged. Every C function
preceding `main`, including the timed worker, is text-identical to the baseline.

Build once, after the active job finishes and while holding the machine lock:

```sh
python3 .cache/invalid-address-perf-20261017/harness/controlled-hotset/build_probe.py \
  .cache/invalid-address-perf-20261017/sources/main \
  .cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so
```

Use a new label for every campaign. A bounded one-worker screen with all five
profiles, three rotating mirrored blocks, and two access patterns is:

```sh
python3 .cache/invalid-address-perf-20261017/harness/controlled-hotset/run_controlled.py hotset32-screen \
  --profiles main original async-load mask-original mask-main \
  --working-pages 32 --cache-mib 64 --apis blocking --threads 1 \
  --modes independent dependent --blocks 3 --seconds 4 --warmup 2
```

The 60 measured processes take approximately six minutes of warmup/measurement,
plus two excluded 31-second preconditions and setup. Five blocks instead of
three would rotate every profile through every mirrored slot; no measured
sample is excluded by outcome. The runner holds the machine lock itself.

At default 4 KiB pages and 128 KiB extents, 32 pages occupy one allocated extent:
128 KiB of logical page data. The worker reads 24 bytes at offset zero of each
page, nominally 2 KiB of first cache lines, plus approximately 2 KiB of cache-entry
metadata and directory/refcount lines. Identical offsets can conflict in L1
cache sets, so this is a small hot working set, not a claim that every touched
line fits in L1. All profiles use the same deterministic spread layout and
allocation sequence. Cache capacity, directory size, and disk geometry remain
the same as the larger probe.

Main already performs an always-on address-alignment division. This experiment
tests the cost of validation, instruction scheduling, layout, and masking; it
does not test a claim that invalid-address handling introduced the division.
A read-only resident result cannot establish insertion neutrality: mutable
write claims, compaction, and worker-quota imbalance are absent here.
