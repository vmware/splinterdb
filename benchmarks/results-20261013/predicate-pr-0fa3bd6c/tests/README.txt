Predicate iterator validation, before merging main 0fa3bd6c

All builds and tests ran through coordination/run_job.py using its shared
machine lock. BUILD_ROOT=build-filter-predicate; default cc (GCC), debug and
release; focused debug ASan adds BUILD_ASAN=1. Tests used separate per-mode
scratch directories under this directory. A build-filter-predicate symlink
in each scratch directory resolves the build's relative shared-library path.
No benchmarks were run by this test campaign.

Results:
  debug: 69 unit cases, zero failures/skips; 4 functional filter cases
  release: 69 unit cases, zero failures/skips; 4 functional filter cases
  debug-asan: 15 unit cases, zero failures/skips
ASAN_OPTIONS=detect_leaks=0:abort_on_error=1. The ASan run covers the complete
merge suite and all five public predicate tests; it does not cover the full
quick/core/functional suites.

Commands, timings, return codes, nonzero test counts and log hashes are in
<mode>-build.json and <mode>-test.json. Source hashes are source-manifest.json;
binary hashes are build-sha256.json. Full command admission history is also
recorded by the shared coordination/jobs.jsonl.

Initial failures are preserved rather than hidden:
  * initial-loader: both test launches failed before running tests because
    the scratch directories lacked the relative shared-library symlink.
  * initial-fixture: quick had 53 passed / 1 failed; the new matching-error
    test set a finalizer but omitted merge_tuples, so update correctly
    returned EINVAL before iteration. The fixture now sets merge_tuples.
The final complete debug/release suites passed after those harness/fixture
repairs. No runtime repair was needed. source-manifest-before-fixture-fix.json
preserves the earlier test source snapshot. Source hashes should be compared
before reusing these results after the pending main merge.
