# Cache-handoff integration evidence

See [the integration report](../../../docs/cache-handoff-mainmerge.md) for the
merge scope, validation results, and performance interpretation.

- `tests/` contains correctness commands, counts, logs, and the diagnosis of
  the initially undersized functional B-tree cache.
- `resident/` and `disk/` contain the initial before/merged/merged/before runs.
- `resident-point8-repeat20/` contains the separate reversed-order repeat.
- `provenance/` records the source patch, file and executable hashes, compiler
  configuration, and coordinator logs.
- `scripts/` preserves the campaign runners and the shared frozen harness.
- `archive-inventory.json` records verified benchmark artifact checksums;
  `tests/inventory.json` records the correctness archive's checksums.

The before executable is the preserved `build-selected` release build for
`23d26d1`; the merged executable is `build-mainmerge`, incorporating upstream
`7d607b1` and the recorded integration patch. Large database fixtures and build
outputs remain outside Git in the local campaign. The runners expect that
campaign layout. They fsync copied fixtures and run under the campaign's
exclusive benchmark/machine locks on the verified ext4 disk filesystem.

All 12 benchmark processes passed (32 timed phases and 12 warm phases). The
initial point/8 change was -5.92%, while its reversed-order repeat was +2.10%.
Keep these campaigns separate: substantial drift prevents a fixed performance
conclusion. The workload uses 24-byte keys and 100-byte values, so it does not
measure large-blob throughput.
