Predicate API correctness and source review

Measured runtime head after latest main merge: cf7dc4f710aad91dfad9050dca0d326ff0fccac5.
Base main: 0fa3bd6c7723690c760294d33ba610e3724bd8d2.
tests-main-0fa3bd6c contains the current debug/release results: 70 unit cases and
four functional filter cases in each mode. tests contains pre-merge results:
69 unit cases and four functional filter cases per mode, plus 15 focused ASan
cases (detect_leaks=0:abort_on_error=1). The ASan run preceded the main merge.
Initial loader invocation and missing test update-callback failures are retained;
corrected invocations/fixtures passed. Database images and executables are excluded.
independent-review records source review, full PR whitespace checks and the exact
CI formatter on a clean tracked export. archive-preservation.json proves all 464
prior filtered archive files and all 563 main archive files were retained unchanged.
Fresh measurements are in the sibling predicate-disk-main-0fa3bd6c archive.
Mixed-seed combination tests live on the separate filtered-ranges-disk-format branch.
