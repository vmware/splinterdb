#!/bin/bash
# ##############################################################################
# test.sh - Driver script to invoke SplinterDB test suites.
# ##############################################################################

Me=$(basename "$0")
set -euo pipefail

# ----
# Top-level env-vars controlling test execution logic. CI sets these, too.
INCLUDE_SLOW_TESTS="${INCLUDE_SLOW_TESTS:-false}"
RUN_NIGHTLY_TESTS="${RUN_NIGHTLY_TESTS:-false}"
RUN_NIGHTLY_TESTS_DEBUG="${RUN_NIGHTLY_TESTS_DEBUG:-false}"

# Name of sub-script that is invoked when running nightly tests to monitor
# OS-level memory usage
#
monOSMemScript="monOSmem.sh"
monOSMem="scripts/${monOSMemScript}"
tmp_mon_out="/tmp/monOSMem.$$.out"

# ----
# Setup a script global, if we are running / debugging nightly run-test execution
Run_nightly_tests=0
Debug_nightly_tests=0
if [ "$RUN_NIGHTLY_TESTS" = "true" ] || [ "$RUN_NIGHTLY_TESTS_DEBUG" = "true" ]; then
    Run_nightly_tests=1
    if [ "$RUN_NIGHTLY_TESTS_DEBUG" = "true" ]; then
        Debug_nightly_tests=1
   fi

    # Check for monitoring script ..., used when running nightly tests.
    if [ ! -x ${monOSMem} ]; then
        echo "$Me: Error, OS-memory monitoring script, '${monOSMem}' not found. Exiting."
        exit 1
   fi
fi

# Name of /tmp file to record test-execution times
test_exec_log_file="/tmp/${Me}.$$.log"

# Global, that will be re-set at the start of each test's execution
start_seconds=0

# ##################################################################
# Print help / usage
# ##################################################################
function usage() {

   # Computed elapsed hours, mins, seconds from total elapsed seconds
   echo "Usage: $Me [--help]"
   echo "To run quick smoke tests          : ./${Me}"
   echo "To run CI-regression tests        : INCLUDE_SLOW_TESTS=true ./${Me}"
   echo "To run nightly regression tests   : RUN_NIGHTLY_TESTS=true ./${Me}"
   echo "To debug nightly regression tests : RUN_NIGHTLY_TESTS_DEBUG=true ./${Me}"

   echo
   echo "Note(s):"
   echo "
- Option 'RUN_NIGHTLY_TESTS_DEBUG=true' is mainly intended for internal use,
  to debug logic in this, and associated scripts, while executing larger
  collection of nightly stress & performance tests.
"
}

# ##################################################################
# record_elapsed_time(start-seconds-count, test-tag-string, rc-code)
#
# Compute elapsed time for full run, and convert to units of h, m, s
# This function also logs a line-entry to a /tmp-file, which will be
# emitted later as a summary report.
# ##################################################################
function record_elapsed_time() {
   local start_sec=$1
   local test_tag=$2
   local rc=$3

   # Computed elapsed hours, mins, seconds from total elapsed seconds
   total_seconds=$((SECONDS - start_sec))
   el_h=$((total_seconds / 3600))
   el_m=$((total_seconds % 3600 / 60))
   el_s=$((total_seconds % 60))

   local result=""
   if [ "$rc" -ne 0 ]; then result="FAILED"; fi

   # Ended this test batch. So inject blank link after this chunk of output
   echo "${Me}: ${test_tag}: ${total_seconds} s [ ${el_h}h ${el_m}m ${el_s}s ] ${result}"
   echo

   # Construct print format string for use by awk
   local fmtstr=": %4ds [ %2dh %2dm %2ds ] %s\n"
   if [ $Run_nightly_tests -eq 1 ]; then
      # Provide wider test-tag for nightly tests which print verbose descriptions
      fmtstr="%-80s""${fmtstr}"
   else
      fmtstr="%-40s""${fmtstr}"
   fi

   # Log a line in the /tmp log-file; for future cat of summary output
   echo $total_seconds, $el_h, $el_m, $el_s \
        | awk -va_msg="${test_tag}" -va_fmt="${fmtstr}" -va_res="${result}" \
            '{printf a_fmt, a_msg, $1, $2, $3, $4, a_res}' \
         >> "${test_exec_log_file}"
}

# ########################################################################
# Wrapper to run a test w/ parameters, and record test execution metrics
# ########################################################################
function run_with_timing() {
   local test_tag="$1"
   shift

   local rc=0
   start_seconds=$SECONDS
   set -x
   "$@"
   rc=$?
   set +x
   record_elapsed_time $start_seconds "${test_tag}" $rc
}

# ########################################################################
# cat contents of test execution log file, and delete it.
# ########################################################################
function cat_exec_log_file() {
    # Display summary test-execution metrics to stdout from /tmp file
    if [ -f "${test_exec_log_file}" ]; then
        # cat contents of log-file, skipping 'Cumulative elapsed time' lines
        # These were included to get run-time accounting; not interesting now.
        grep -v "Cumulative elapsed time" "${test_exec_log_file}"
        rm -f "${test_exec_log_file}"
   fi
   echo
   echo "$(TZ='America/Los_Angeles' date) End SplinterDB Test Suite Execution."
}

# #############################################################################
# Batch of tests run nightly. These take too long. Some are like
# stress tests, some are performance oriented tests.
#
#       ---- NOTE ABOUT STRESS / PERF TEST CONFIG PARAMETERS ----
#
# The test-execution config / parameters are chosen to get a good coverage of
# a reasonable range of configurations. Some of these require big VMs/machines
# with adequate memory & cores to run cleanly.
#
# **** Testing Hardware requirements for adequate turnaround ****
#  - 16 - 32 GiB RAM (or higher)
#  - 8 cores, even if they are VMs. Some tests fire up multiple threads
# #############################################################################

# #############################################################################
# Wait-for OS-memory monitoring script to complete, and then cat its outputs
# #############################################################################
function wait_for_OS_mem_monitoring() {

    while [ "$(pgrep --count ${monOSMemScript})" -gt 0 ]; do
        sleep 5
   done
    if [ -f ${tmp_mon_out} ]; then cat ${tmp_mon_out}; fi
}

# #############################################################################
# Functionality stress test:
#
# We exercise large'ish # of inserts, 100 million, with different cache sizes,
# to get some coverage on core functionality in stress workloads.
# #############################################################################
function nightly_functionality_stress_tests() {

    local debug_nightly_test=$1
    # Future: We want to crank this up to 100 mil rows, but assertions around
    # trunk bundle mgmt prevent that.
    local n_mills=10
    local num_rows=$((n_mills * 1000 * 1000))
    local nrows_h="${n_mills} mil"

    # For script debugging, drop # of rows to smaller count
    if [ "$debug_nightly_test" == "1" ]; then
        n_mills=2
        num_rows=$((n_mills * 1000 * 1000))
        nrows_h="${n_mills} mil"
   fi

    local ntables=1
    local test_name="splinter_test --functionality"

    # ----
    local cache_size=4  # GB
    local test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} GiB cache"
    local dbname="splinter_test.functionality.db"
    echo "$Me: Run ${test_name} with ${n_mills} rows, on ${ntables} tables, with ${cache_size} GiB cache"
    ${monOSMem} driver_test 0 "splinter_test --functionality" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Functionality Stress test ${test_descr}" \
            bin/driver_test splinter_test --functionality  ${num_rows} 1000 \
                                          --num-tables ${ntables} \
                                          --cache-capacity-gib ${cache_size} \
                                          --db-location ${dbname}
    wait_for_OS_mem_monitoring

    # ----
    ntables=2
    local test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} GiB cache"
    local dbname="splinter_test.functionality.db"
    echo "$Me: Run ${test_name} with ${n_mills} million rows, on ${ntables} tables, with ${cache_size} GiB cache"
    ${monOSMem} driver_test 0 "splinter_test --functionality" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Functionality Stress test ${test_descr}" \
            bin/driver_test splinter_test --functionality  ${num_rows} 1000 \
                                          --num-tables ${ntables} \
                                          --cache-capacity-gib ${cache_size} \
                                          --db-location ${dbname}
    wait_for_OS_mem_monitoring

    # ----
    cache_size=1        # GiB

    # Remove this block once issue #322 is fixed.
    n_mills=1
    num_rows=$((n_mills * 1000 * 1000))
    nrows_h="${n_mills} mil"

    test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} MiB cache"
    echo "$Me: Run with ${n_mills} million rows, on ${ntables} tables, with default ${cache_size} GiB cache"
    ${monOSMem} driver_test 0 "splinter_test --functionality" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Functionality Stress test ${test_descr}" \
            bin/driver_test splinter_test --functionality ${num_rows} 1000 \
                                          --num-tables ${ntables} \
                                          --cache-capacity-gib ${cache_size} \
                                          --db-location ${dbname}
    wait_for_OS_mem_monitoring

    # ----
    ntables=4
    cache_size=1        # GiB
    test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} MiB cache"
    echo "$Me: Run with ${n_mills} million rows, on ${ntables} tables, with default ${cache_size} GiB cache"
    ${monOSMem} driver_test 0 "splinter_test --functionality" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Functionality Stress test ${test_descr}" \
            bin/driver_test splinter_test --functionality ${num_rows} 1000 \
                                          --num-tables ${ntables} \
                                          --cache-capacity-gib ${cache_size} \
                                          --db-location ${dbname}
    wait_for_OS_mem_monitoring

    # ----
    cache_size=512      # MiB
    test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} MiB cache"
    # echo "$Me: Run with ${n_mills} million rows, on ${ntables} tables, with small ${cache_size} MiB cache"
    # ${monOSMem} driver_test 0 "splinter_test --functionality" > ${tmp_mon_out} 2>&1 &

    # Commented out, because we run into issue # 322.
    # run_with_timing "Functionality Stress test ${test_descr}" \
    #       bin/driver_test splinter_test --functionality ${num_rows} 1000 \
                                        # --num-tables ${ntables} \
                                        # --cache-capacity-mib ${cache_size} \
                                        # --db-location ${dbname}
    # wait_for_OS_mem_monitoring

    rm ${dbname}

    record_elapsed_time "${testRunStartSeconds}" "Cumulative elapsed time" 0
}

# Run through collection of nightly stress tests
function run_nightly_stress_tests() {

    local debug_nightly_test=$1
    nightly_functionality_stress_tests "$debug_nightly_test"
}

# #############################################################################
# Performance stress test:
#
# Exercise two sets of performance-related tests: sync and async
#
# Async-systems are a bit unstable now, so will online them shortly in future.
# #############################################################################
function nightly_sync_perf_tests() {

    local debug_nightly_test=$1
    local npthreads=10
    local dbname="splinter_test.perf.db"

    # Different #s of threads. --perf test runs in phases, where some # of
    # threads are setup. Insert / lookup / range-lookups are run. Then, the
    # threads are destroyed.
    local nins_t=8
    local nlookup_t=8
    local nrange_lookup_t=8

    # For script debugging, drop # of rows to smaller count, indirectly controlled
    # by --tree-size-gib arg.
    local tree_size_gib=40  # Affects num_inserts_arg"
    if [ "$debug_nightly_test" == "1" ]; then
        tree_size_gib=1
   fi

    local test_descr="${nins_t} insert, ${nlookup_t} lookup, ${nrange_lookup_t} range lookup threads"

    ${monOSMem} driver_test 0 "splinter_test --perf" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Performance (sync) test ${test_descr}" \
            bin/driver_test splinter_test --perf \
                                          --max-async-inflight 0 \
                                          --num-insert-threads ${nins_t} \
                                          --num-lookup-threads ${nlookup_t} \
                                          --num-range-lookup-threads ${nrange_lookup_t} \
                                          --lookup-positive-percent 10 \
                                          --tree-size-gib ${tree_size_gib} \
                                          --db-capacity-gib 60 \
                                          --db-location ${dbname}
    wait_for_OS_mem_monitoring
    rm ${dbname}

    dbname="splinter_test.pll_perf.db"
    test_descr="${npthreads} pthreads"
    ${monOSMem} driver_test 0 "splinter_test --parallel-perf" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Parallel Performance (sync) test ${test_descr}" \
            bin/driver_test splinter_test --parallel-perf \
                                          --max-async-inflight 0 \
                                          --num-pthreads ${npthreads} \
                                          --lookup-positive-percent 10 \
                                          --tree-size-gib ${tree_size_gib} \
                                          --db-capacity-gib 60 \
                                          --db-location ${dbname}
    wait_for_OS_mem_monitoring
    rm ${dbname}

    record_elapsed_time "${testRunStartSeconds}" "Cumulative elapsed time" 0
}

# #############################################################################
# Nightly Cache Performance tests with async disabled
# #############################################################################
function nightly_cache_perf_tests() {

    local debug_nightly_test=$1
    local dbname="cache_test.perf.db"
    local test_descr="default cache size"
    local cache_size=256  # MiB
    ${monOSMem} driver_test 0 "cache_test --perf" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Cache Performance test, ${test_descr}" \
            bin/driver_test cache_test --perf \
                                       --cache-capacity-mib ${cache_size} \
                                       --db-location ${dbname}

    wait_for_OS_mem_monitoring

    # ----
    # Test larger cache size. Also a verification for fix #312, where we used
    # to get a seg-fault if (db-size == (5 * cache-size))
    cache_size=6  # GiB
    if [ "$debug_nightly_test" == "1" ]; then
        cache_size=1
   fi
    test_descr="${cache_size} GiB cache"
    ${monOSMem} driver_test 0 "cache_test --perf" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Cache Performance test, ${test_descr}" \
            bin/driver_test cache_test --perf \
                                       --db-location ${dbname} \
                                       --cache-capacity-gib ${cache_size} \
                                       --db-capacity-gib 30
    wait_for_OS_mem_monitoring
    rm ${dbname}

    record_elapsed_time "${testRunStartSeconds}" "Cumulative elapsed time" 0
}

# #############################################################################
# Nightly Performance tests with async enabled - Currently not being invoked.
# #############################################################################
function nightly_async_perf_tests() {

    # TODO: When these tests are onlined, drop these counts, so that we can run
    # on even 8-core machines. Review usage of test-execution params in the
    # test_splinter_parallel_perf(), and fix accordingly.
    local npthreads=20
    local nbgthreads=20
    local nasync=10
    local test_descr="${npthreads} pthreads,bgt=${nbgthreads},async=${nasync}"
    local dbname="splinter_test.perf.db"
    ${monOSMem} driver_test 0 "splinter_test --parallel-perf" > ${tmp_mon_out} 2>&1 &

    run_with_timing "Parallel Async Performance test ${test_descr}" \
            bin/driver_test splinter_test --parallel-perf \
                                          --num-bg-threads ${nbgthreads} \
                                          --max-async-inflight ${nasync} \
                                          --num-pthreads ${npthreads} \
                                          --db-capacity-gib 60 \
                                          --db-location ${dbname}
    wait_for_OS_mem_monitoring
    rm ${dbname}

    record_elapsed_time "${testRunStartSeconds}" "Cumulative elapsed time" 0
}

# Run through collection of nightly Performance-oriented tests
function run_nightly_perf_tests() {

    local debug_nightly_test=$1
    nightly_sync_perf_tests "$debug_nightly_test"
    nightly_cache_perf_tests "$debug_nightly_test"

    # nightly_async_perf_tests

}

# ##################################################################
# main() begins here
# ##################################################################

if [ $# -eq 1 ] && [ "$1" == "--help" ]; then
    usage
    exit 0
fi

echo "$Me: $(TZ='America/Los_Angeles' date) Start SplinterDB Test Suite Execution."
set -x
SEED="${SEED:-135}"
set +x

run_type=" "
if [ $Run_nightly_tests -eq 1 ]; then run_type=" Nightly "; fi
if [ $Debug_nightly_tests -eq 1 ]; then run_type="${run_type}(Debugging) "; fi

# Track total elapsed time for entire test-suite's run
testRunStartSeconds=$SECONDS

# Initialize test-execution timing log file
echo "$(TZ='America/Los_Angeles' date) **** SplinterDB${run_type}Test Suite Execution Times **** " > "${test_exec_log_file}"
echo >> "${test_exec_log_file}"

# ---- Nightly Stress and Performance test runs ----
if [ $Run_nightly_tests -eq 1 ]; then

    ./scripts/osinfo.sh 1

    set +e
    run_nightly_stress_tests $Debug_nightly_tests

    run_nightly_perf_tests $Debug_nightly_tests
    set -e

    record_elapsed_time ${testRunStartSeconds} "Nightly Stress & Performance Tests" 0
    cat_exec_log_file
    exit 0
fi

# ---- Fast running Smoke test runs ----
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
   record_elapsed_time ${start_seconds} "Fast unit tests" 0
   cat_exec_log_file
   exit 0
fi

# ---- Rest of the coverage runs included in CI test runs ----

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

record_elapsed_time ${testRunStartSeconds} "All Tests" 0
echo ALL PASSED

cat_exec_log_file
