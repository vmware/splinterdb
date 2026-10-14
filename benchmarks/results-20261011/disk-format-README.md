# Metadata benchmark evidence, 2026-10-11

See ../../docs/on-disk-format.md for interpretation and validation.
CSV files and manifests preserve the raw samples, commands, timestamps and
binary/source provenance. Absolute paths in manifests identify the original
workspace. Database images and perf recordings are deliberately omitted.

The final implementation uses the scalar reader. `resident-log0` is the initial
reader; the two `*-final-singleload` campaigns and `resident-profile` precede
the scalar change. `resident-scalar-fixed` compares the old and final readers
on copies of exactly the same candidate image, with its own incompatible
baseline image. The retained runners document fixture sources and run order.
Baseline source is db3047a; the final implementation milestone is 33389f3.

Method caveat: `resident-scalar-fixed` used buffered fixture copies without an
explicit fsync before O_DIRECT open/warm/timing. Pending copy writeback could
confound those samples; it is not an established explanation of any difference.
The earlier fresh-create matrices did not use copied fixtures. Original evidence
is preserved without modification; there is no fsynced metadata repeat.
