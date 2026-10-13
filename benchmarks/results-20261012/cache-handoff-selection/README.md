# Selected cache-error and handoff branch validation

See the [handoff report](../../../docs/cache-handoff-results.md) for interpretation.
This follow-up measures only the release-hook guard correction on the selected
16-byte shared-identity/stripe handle implementation.

- `tests/`: 110 debug, 64 release and 17 ASan cases, three functional workloads,
  ancestry checks, commands, logs and testing limits.
- `resident/`: four before/selected/selected/before runs, ten-second point/1,
  point/8, range-1/1 and range-1/8 phases.
- `disk/`: four runs in the same order, five-second point/8, range-1/8 and
  range-10,000/8 phases.
- `resident-point8-repeat20/`: four twenty-second point/8 runs in the reversed
  selected/before/before/selected order.
- `provenance/`: library/harness hashes, matched build flags, runtime source
  identity, 106 source/build-file hashes, the exact six-guard patch, disassembly
  and audit notes.
- `scripts/`: snapshots of the exact campaign build, benchmark, locking and
  archival scripts.
- `archive-inventory.json`: original paths and SHA-256 hashes for archived
  benchmark/provenance files. Test evidence is indexed separately.

All twelve processes completed successfully: 32 timed query phase cells plus
twelve warm phases, with zero errors/violations and zero resident timed device
reads. Images were stored on the real ext4 block device and every fixture copy
was fsynced before opening. Images and executable binaries remain outside Git.

The original control is the preserved `build-prefetch` executable from
`65b9123c237ed27a95e27179564c7ef21dd162fd`; runtime source under `src/`, `include/`
and `Makefile` is identical between that commit and parent `f0641f9`. The selected
library is built in `build-selected`, with the guard-only patch recorded here.
Both use the same frozen public `kvbench.c` source and equivalent compiler/linker
flags. Historical controls were not rebuilt or overwritten.

Scripts resolve paths relative to the original campaign's `coordination/`
directory. They are archived here for review; install them in that location to
reproduce within the existing campaign, using new output/build roots. From the
campaign directory, after building the release library:

```sh
python3 coordination/run_job.py --name handoff-selection-harness-build build -- \
  python3 coordination/build_handoff_selection.py
python3 coordination/run_job.py --name handoff-selection-benchmark \
  --db-dir "$CAMPAIGN/benchmarks/cache-handoff-selection" benchmark -- \
  python3 coordination/run_handoff_selection.py
python3 coordination/run_job.py --name handoff-selection-point8-repeat \
  --db-dir "$CAMPAIGN/benchmarks/cache-handoff-selection" benchmark -- \
  python3 coordination/handoff_selection_point8.py resident
```

`CAMPAIGN` identifies the real-disk campaign directory. The locking wrapper
checks physical storage and excludes other benchmarks, builds and correctness
tests. Frozen fixture hashes, exact arguments and per-run return codes are in
the corresponding manifests.
