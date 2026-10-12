Compact evidence for hash-filtered ranges. See
[`docs/hash-filtered-ranges.md`](../../docs/hash-filtered-ranges.md) for results
and their limits.

- Unsuffixed directories measure the superseded eager per-source adapter (v1,
  `6ff014b`).
- Directories with `v2` measure filtering during ordered merge selection
  (`9062730`), before the ordinary iterator layout repair.
- Directories with `v3` measure the final implementation (`f30ebdf`): the
  focused ordinary range1/8-thread check and the 100,000-query native hash
  probe. Earlier full matrices were not rerun on this revision.

CSVs retain individual samples and checksums. Manifests retain exact commands,
binary hashes, and fixture hashes where a shared fixture was used. Build
metadata and `source-provenance.json` identify source and compiler flags.
Machine metadata records the campaign hardware; physical memory is not the
available memory during every run. All database paths were on real ext4 disk,
with O_DIRECT and one exclusive timing lock shared across the projects.

The copied-image runner did not explicitly fsync its buffered copies before
warming and timing. Pending writeback might overlap the historical disk
opt-out runs; no disk speedup is claimed. The native hash probe creates its
own O_DIRECT databases. Later campaign runners sync copied fixtures explicitly.

Database images and verbose logs remain under
`.cache/projects-20261011/benchmarks/filtered-ranges` in the original checkout
and are intentionally omitted.
