# Bounded fsynced-fixture control

One ABBA campaign, using frozen baseline/final-prefix binaries and the same
per-format disk-common images as disk-common-fixed-final. Each buffered copy is
fsynced before any O_DIRECT open/warm/timing. Seven seconds per point8 and
range10000@8 phase; 2M common-prefix keys, 64MiB cache, zero background workers.
All4 runs have zero errors/violations. No databases are archived.

Manifest includes commands, checked binary hashes, fixture path/size/mtime and
copy/fsync timestamps. summary.csv derives means/ranges from the individual
results.csv samples; old campaigns remain preserved separately. This control
checks persistence of the large common-prefix disk gains, not a full matrix or
proof that previous outliers were caused by buffered-copy writeback.
