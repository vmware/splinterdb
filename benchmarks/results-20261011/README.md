# Prefix experiment evidence, 2026-10-11

See ../../docs/branch-prefix-experiment.md for interpretation and validation.
Baseline is db3047a; the final implementation milestone is 1ef1644. Manifests
preserve absolute original paths, commands, timestamps and binary provenance.
No database images are included. Retained campaign runners document the original
workspace layout and fixture sources; the portable workload builder is at
../build_prefix_bench.py and accepts --build-root.

The seven initial pattern/control directories measure the previous candidate.
The three *-fixed-final directories measure the final reconstruction path.
The resident common-prefix repeat includes the old candidate too, with identical
candidate images for both binaries. Incompatible baseline and candidate formats
use their own images, loaded serially with no background workers. File allocation
and live extent measurements come from the initial, format-identical images.
