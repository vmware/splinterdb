#!/bin/bash
# ##############################################################################
# test.sh - Driver script to invoke SplinterDB test suites.
# ##############################################################################

Me=$(basename "$0")
set -euo pipefail

# Name of /tmp file to record test-execution times
test_exec_log_file="/tmp/${Me}.$$.log"

# Global, that will be re-set at the start of each test's execution
start_seconds=0

# ##################################################################
# Compute elapsed time for full run, and convert to units of h, m, s
# This function also logs a line-entry to a /tmp-file, which will be
# emitted later as a summary report.
# ##################################################################
function record_elapsed_time() {
   local start_sec=$1
   local test_tag=$2

   # Computed elapsed hours, mins, seconds from total elapsed seconds
   total_seconds=$((SECONDS - start_sec))
   el_h=$((total_seconds / 3600))
   el_m=$((total_seconds % 3600 / 60))
   el_s=$((total_seconds % 60))

   echo "${Me}: ${test_tag}: ${total_seconds} s [ ${el_h}h ${el_m}m ${el_s}s ]"

   # Log a line in the /tmp log-file; for future cat of summary output
   echo $total_seconds, $el_h, $el_m, $el_s \
        | awk -va_msg="${test_tag}" '{printf " %-40s: %4ds [ %2dh %2dm %2ds ]\n", a_msg, $1, $2, $3, $4}' \
         >> "${test_exec_log_file}"
}

# ########################################################################
# Wrapper to run a test w/ parameters, and record test execution metrics
# ########################################################################
function run_with_timing() {
   local test_tag="$1"
   shift

   # Starting a new test batch. So inject blank link for this chunk of output
   start_seconds=$SECONDS
   echo
   set -x
   "$@"
   set +x
   record_elapsed_time $start_seconds "${test_tag}"
}

# ########################################################################
# cat contents of test execution log file, and delete it.
# ########################################################################
function cat_exec_log_file() {
    # Display summary test-execution metrics to stdout from /tmp file
    if [ -f "${test_exec_log_file}" ]; then
        cat "${test_exec_log_file}"
        rm -f "${test_exec_log_file}"
   fi
   echo
   echo "$Me: $(date) End SplinterDB Test Suite Execution."
}

# ##################################################################
# main() begins here
# ##################################################################

echo "$Me: $(date) Start SplinterDB Test Suite Execution."
set -x

SEED="${SEED:-135}"

INCLUDE_SLOW_TESTS="${INCLUDE_SLOW_TESTS:-false}"
RUN_NIGHTLY_TESTS="${RUN_NIGHTLY_TESTS:-false}"

if [ "$RUN_NIGHTLY_TESTS" == "true" ]; then
   echo "$Me: Running nightly tests script ..."
   exit 0
fi

set +x

# Track total elapsed time for entire test-suite's run
testRunStartSeconds=$SECONDS

# Initialize test-execution timing log file
echo "      **** SplinterDB Test Suite Execution Times **** " > "${test_exec_log_file}"
echo >> "${test_exec_log_file}"

if [ "$INCLUDE_SLOW_TESTS" != "true" ]; then
   # For some coverage, exercise --help, --list args for unit test binaries
   set -x
   bin/unit_test --help

   bin/unit_test --list

   bin/unit_test --list splinterdb_quick

   bin/unit/btree_test --help

   bin/unit/splinterdb_quick_test --list

   set +x

   echo
   echo "NOTE: **** Only running fast unit tests ****"
   echo "To run all tests, set the env var, and re-run: $ INCLUDE_SLOW_TESTS=true ./$Me"
   echo
   start_seconds=$SECONDS

   set -x
   bin/unit/splinterdb_quick_test
   bin/unit/btree_test
   bin/unit/util_test
   bin/unit/misc_test
   set +x

   echo "Fast tests passed"
   record_elapsed_time ${start_seconds} "Fast unit tests"
   cat_exec_log_file
   exit 0
fi

# Run all the unit-tests first, to get basic coverage
run_with_timing "Fast unit tests" bin/unit_test

# ------------------------------------------------------------------------
# Explicitly run individual cases from specific slow running unit-tests,
# where appropriate with a different test-configuration that has been found to
# provide the required coverage.
run_with_timing "Splinter inserts test" bin/unit/splinter_test test_inserts

# Use fewer rows for this case, to keep elapsed times of MSAN runs reasonable.
run_with_timing "Splinter lookups test" bin/unit/splinter_test --num-inserts 2000000 test_lookups

UNIT_TESTS_DB_DEV="unit_tests_db"
if [ -f ${UNIT_TESTS_DB_DEV} ]; then
    rm ${UNIT_TESTS_DB_DEV}
fi

run_with_timing "Functionality test" bin/driver_test splinter_test --functionality 1000000 100 --seed "$SEED"

run_with_timing "Performance test" bin/driver_test splinter_test --perf --max-async-inflight 0 --num-insert-threads 4 --num-lookup-threads 4 --num-range-lookup-threads 0 --tree-size-gib 2 --cache-capacity-mib 512

run_with_timing "Cache test" bin/driver_test cache_test --seed "$SEED"

run_with_timing "BTree test" bin/driver_test btree_test --seed "$SEED"

run_with_timing "Log test" bin/driver_test log_test --seed "$SEED"

run_with_timing "Filter test" bin/driver_test filter_test --seed "$SEED"

record_elapsed_time ${testRunStartSeconds} "All Tests"
echo ALL PASSED

cat_exec_log_file
