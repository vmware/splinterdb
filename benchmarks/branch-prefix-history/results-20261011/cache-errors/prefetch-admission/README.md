# Prefetch admission regression evidence

The original library at `1da0450`, plus `old-test-and-hooks.diff`, fails the
scheduled stale-candidate interleaving with the expected assertion (signal 6).
The fix in `526a0a0` passes that test, the async cache driver, 14 writeback tests,
and 47 public quick tests. The corrected manifests record the source revision,
commands, driver and shared-library hashes. The old library hash was not
captured; the saved source patch reconstructs its test setup. The driver is
dynamically linked, so its unchanged hash alone does not identify the library.

The first follow-up unit invocations selected zero tests because CTest treated
the supplied database path as a test filter. `invalid-zero-selection/` preserves
those logs and their original manifest; they are invalid validation. The
parent-directory `fixed-*` evidence contains the corrected complete-suite runs.
