Predicate ranges + disk-format PR validation

Validated source: 7b2c37dad3a19945c29d2f5675cc327ef76c46d5.
The final PR adds documentation and evidence after this runtime snapshot.

validation-source-identity.json proves that src/, include/, tests/, Makefile,
and .github/ are byte-identical to companion 361838074656ddf04ad37c0452f4373da9211269.
That companion's existing archive at ../../filter-predicate-disk-integration/
contains 125 passing unit cases in each of debug and release plus the full
routing-filter functional suite. Those results apply to this same source.

Fresh checks passed:
  * Release: 56 quick API cases and 10 merge cases, plus the full functional
    routing-filter suite (four basic cases and structural/saved-seed checks).
  * GCC debug AddressSanitizer: all six filtered-range API cases and ten merge
    cases, including persisted mixed seeds and iterator thread handoff.
  * C11 and C++17 public-header probes for ordinary and predicate iterator APIs.
  * Active-source clang-format-19 and PR/working-tree whitespace checks.

ASan used detect_leaks=0:abort_on_error=1 because sandbox process inspection
limits LeakSanitizer. This is address-safety coverage, not leak coverage. The
normal full CI compiler/sanitizer/stress matrix remains separate.

The initial public-header probe omitted the required SPLINTERDB_PLATFORM_DIR
definition. The two *missing-platform* files preserve that harness error.
Adding the platform macro made both C and C++ probes pass; no product code
changed. The final exact CI-format export also passed, including all archived C/header
sources. Files whose names start validation-final- record that check, staged
index hash, formatting-guard hashes, whitespace checks against disk-format and
main, and the final runtime-identity proof. Those result files are evidence-only
additions after the checked staged snapshot.

Provenance

validation-summary.json summarizes the results. Per-check JSON files record
commands, working directories, exit statuses, and log hashes. The release and
ASan provenance files contain all 167 compiled source/Makefile hashes, binary
hashes, and exact build configurations. original-file-manifest.json maps copied
files to their original workspace locations and hashes. It excludes itself
and generated packaging documentation. No executables, objects, databases, or
exported checkout trees are included. The local .clang-format preserves the
exact API probe and scripts used in these checks.

Reproduction

Paths below are relative to a campaign workspace rooted in the repository.
The original scripts retain absolute paths in provenance and their constants;
when using a different checkout, adjust ROOT/TREE in copies of the scripts to
the repository and target worktree. Keep the archived originals unchanged.

  1. Put run_job.py in .cache/projects-20261011/coordination/ and the
     validation-*.py and validation-api.c files in
     .cache/projects-20261011/coordination/filtered-disk-pr-20261013/.
     Point TREE at the checkout under test. The original checkout was
     .cache/projects-20261011/worktrees/filtered-ranges/.
  2. From repository root, run:

python3 .cache/projects-20261011/coordination/run_job.py --name reproduce-filtered-disk-release-build build -- python3 .cache/projects-20261011/coordination/filtered-disk-pr-20261013/validation-run.py build release merge_test splinterdb_quick_test
python3 .cache/projects-20261011/coordination/run_job.py --name reproduce-filtered-disk-release-checks test -- python3 .cache/projects-20261011/coordination/filtered-disk-pr-20261013/validation-run.py test release merge quick filter
python3 .cache/projects-20261011/coordination/run_job.py --name reproduce-filtered-disk-asan-build build -- python3 .cache/projects-20261011/coordination/filtered-disk-pr-20261013/validation-run.py build debug-asan
python3 .cache/projects-20261011/coordination/run_job.py --name reproduce-filtered-disk-asan-checks test -- python3 .cache/projects-20261011/coordination/filtered-disk-pr-20261013/validation-run.py test debug-asan
python3 .cache/projects-20261011/coordination/run_job.py --name reproduce-filtered-disk-static build -- python3 .cache/projects-20261011/coordination/filtered-disk-pr-20261013/validation-static.py

The shared wrapper excludes concurrent timed benchmarks during builds/tests.
Test databases use private directories below the campaign benchmarks directory;
the validation script verifies they reside on a real disk filesystem.

To repeat the final exact CI-format check, stage all intended PR content first,
then invoke validation-final-export.py through run_job.py in test mode. It
exports the complete staged index, preserving archive formatting guards, runs
./format-check.sh, checks whitespace against disk-format and origin/main, and
verifies the unchanged runtime against the source-identical companion commit.
