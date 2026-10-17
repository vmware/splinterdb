# Prefix integration with main fc0bc8f7

Merged main `fc0bc8f7e39566f14822bb598376458bdf1e28b3` into the prefix
experiment, preserving its old head `100800cb` at
`backup/branch-prefix-before-main-fc0bc8f7`.

The integrated experiment uses branch format 2 with main's superblock format 8.
Format 1 branch references are rejected before page access; old standalone
prefix superblocks (0x8007) are rejected. The lexical flag remains a construction
policy: a new regression reopens compressed and uncompressed leaves with the
opposite flag, then checks full-key predicate-only and routing-pruned iteration
in both directions. The existing roundtrip still verifies actual compressed
pages, point/async reads, forward/reverse scans and repeated seeks.

The iterator merge initializes its owned reconstruction buffers before checking
branch compatibility, preserving safe destruction after rejected versions.

`make -j4 BUILD_ROOT=build-main-fc0bc8f7 BUILD_MODE={debug,release} all`
built libraries, tools, examples and all test binaries. Debug and release each
passed 137 unit cases: B-tree 12, superblock 17, merge 10, public quick API 57,
and internal Splinter 41. All builds and tests used the campaign coordinator.
The first debug run exposed an obsolete test expectation: unsupported formats
now return INVALID_STATE rather than NOT_FOUND. The diagnostic log is retained;
the corrected test passed in both modes. An initial runner also mistakenly
requested a standalone blob test executable; blob cases are part of the core
suite and passed in the final runs.

Changed C sources pass clang-format-19 and the source/doc diff passes whitespace
checks. `source-manifest.json` hashes the tested runtime and test sources;
per-mode results record commands, timestamps, executable hashes and log hashes.
Historical prefix evidence moved unchanged under `benchmarks/branch-prefix-history`
to avoid collisions with main's archives; historical-archive-proof.json verifies
all 292 original archive files byte-for-byte. No historical timings were rerun or
reinterpreted in this integration; fresh performance is coordinated separately.
