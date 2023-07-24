#!/bin/bash
# ##############################################################################
# test.sh - Driver script to invoke SplinterDB test suites.
# ##############################################################################

Me=$(basename "$0")
set -euo pipefail

# Location of binaries, which live under $BUILD_ROOT, if set.
build_dir="${BUILD_ROOT:-build}"
build_mode="${BUILD_MODE:-release}"
BINDIR="${BINDIR:-${build_dir}/${build_mode}/bin}"

# Location of binaries, which live under $BUILD_ROOT, if BINDIR is not set.
# If BINDIR -was- set in the user's env, we need to drive off of that.
# To avoid raising a script error by referencing this variable if it was
# -not-set- we just initialized it above to this value. Thus, if the
# following is true, it means the env-var BINDIR was -not- set already.
if [ "${BINDIR}" == "${build_dir}/${build_mode}/bin" ]; then

   # Fix build-dir path based on BUILD_MODE, if -set-.
   build_mode="${BUILD_MODE:-release}"
   build_dir="${build_dir}/${build_mode}"

   # If either one of Asan / Msan build options is -set-, fix build-dir path.
   build_asan="${BUILD_ASAN:-0}"
   build_msan="${BUILD_MSAN:-0}"
   if [ "${build_asan}" == "1" ]; then
      build_dir="${build_dir}-asan"
   elif [ "${build_msan}" == "1" ]; then
      build_dir="${build_dir}-msan"
   fi

   # Establish bin/ dir-path, to account for the other build parameters
   BINDIR="${build_dir}/bin"
fi

echo "$Me: build_dir='${build_dir}', BINDIR='${BINDIR}'"

# Top-level env-vars controlling test execution logic. CI sets these, too.
INCLUDE_SLOW_TESTS="${INCLUDE_SLOW_TESTS:-false}"
RUN_NIGHTLY_TESTS="${RUN_NIGHTLY_TESTS:-false}"
RUN_MAKE_TESTS="${RUN_MAKE_TESTS:-false}"

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
   echo "To run quick smoke tests         : ./${Me}"
   echo "To run CI-regression tests       : INCLUDE_SLOW_TESTS=true ./${Me}"
   echo "To run nightly regression tests  : RUN_NIGHTLY_TESTS=true ./${Me}"
   echo "To run make build-and-test tests : RUN_MAKE_TESTS=true ./${Me}"
   echo
   echo "To run a smaller collection of slow running tests,
name the function that drives the test execution.
Examples:"
   echo "  INCLUDE_SLOW_TESTS=true ./test.sh run_btree_tests"
   echo "  INCLUDE_SLOW_TESTS=true ./test.sh run_splinter_functionality_tests"
   echo "  INCLUDE_SLOW_TESTS=true ./test.sh nightly_cache_perf_tests"
}

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

   # Construct print format string for use by awk
   local fmtstr=": %4ds [ %2dh %2dm %2ds ]\n"
   if [ "$RUN_NIGHTLY_TESTS" == "true" ]; then
      # Provide wider test-tag for nightly tests which print verbose descriptions
      fmtstr="%-80s""${fmtstr}"
   else
      fmtstr="%-70s""${fmtstr}"
   fi

   # Log a line in the /tmp log-file; for future cat of summary output
   echo $total_seconds, $el_h, $el_m, $el_s \
        | awk -va_msg="${test_tag}" -va_fmt="${fmtstr}" '{printf a_fmt, a_msg, $1, $2, $3, $4}' \
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
   echo "$(TZ="America/Los_Angeles" date) End SplinterDB Test Suite Execution."
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
# Functionality stress test:
#
# We exercise large'ish # of inserts, 100 million, with different cache sizes,
# to get some coverage on core functionality in stress workloads.
# #############################################################################
function nightly_functionality_stress_tests() {

    # Future: We want to crank this up to 100 mil rows, but assertions around
    # trunk bundle mgmt prevent that.
    local n_mills=10
    local num_rows=$((n_mills * 1000 * 1000))
    local nrows_h="${n_mills} mil"

    local ntables=1
    local test_name="splinter_test --functionality"

    # ----
    local cache_size=4  # GB
    local test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} GiB cache"
    local dbname="splinter_test.functionality.db"
    echo "$Me: Run ${test_name} with ${n_mills} million rows, on ${ntables} tables, with ${cache_size} GiB cache"
    run_with_timing "Functionality Stress test ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --functionality  ${num_rows} 1000 \
                                                --num-tables ${ntables} \
                                                --cache-capacity-gib ${cache_size} \
                                                --db-location ${dbname}

    # ----
    ntables=2
    local test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} GiB cache"
    local dbname="splinter_test.functionality.db"
    echo "$Me: Run ${test_name} with ${n_mills} million rows, on ${ntables} tables, with ${cache_size} GiB cache"
    run_with_timing "Functionality Stress test ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --functionality  ${num_rows} 1000 \
                                                --num-tables ${ntables} \
                                                --cache-capacity-gib ${cache_size} \
                                                --db-location ${dbname}

    # ----
    cache_size=1        # GiB

    # Remove this block once issue #322 is fixed.
    n_mills=1
    num_rows=$((n_mills * 1000 * 1000))
    nrows_h="${n_mills} mil"

    test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} MiB cache"
    echo "$Me: Run with ${n_mills} million rows, on ${ntables} tables, with default ${cache_size} GiB cache"
    run_with_timing "Functionality Stress test ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --functionality ${num_rows} 1000 \
                                                --num-tables ${ntables} \
                                                --cache-capacity-gib ${cache_size} \
                                                --db-location ${dbname}

    # ----
    ntables=4
    cache_size=1        # GiB
    test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} MiB cache"
    echo "$Me: Run with ${n_mills} million rows, on ${ntables} tables, with default ${cache_size} GiB cache"
    run_with_timing "Functionality Stress test ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --functionality ${num_rows} 1000 \
                                                --num-tables ${ntables} \
                                                --cache-capacity-gib ${cache_size} \
                                                --db-location ${dbname}
    # ----
    cache_size=512      # MiB
    test_descr="${nrows_h} rows, ${ntables} tables, ${cache_size} MiB cache"
    # echo "$Me: Run with ${n_mills} million rows, on ${ntables} tables, with small ${cache_size} MiB cache"
    # Commented out, because we run into issue # 322.
    # run_with_timing "Functionality Stress test ${test_descr}" \
    #       "$BINDIR"/driver_test splinter_test --functionality ${num_rows} 1000 \
                                        #       --num-tables ${ntables} \
                                        #       --cache-capacity-mib ${cache_size} \
                                        #       --db-location ${dbname}
    rm ${dbname}
}

# #############################################################################
# Run through collection of nightly stress tests
# #############################################################################
function run_nightly_stress_tests() {

    nightly_functionality_stress_tests
}

# #############################################################################
# Performance stress test:
#
# Exercise two sets of performance-related tests: sync and async
#
# Async-systems are a bit unstable now, so will online them shortly in future.
# #############################################################################
function nightly_sync_perf_tests() {

    local dbname="splinter_test.perf.db"

    # Different #s of threads. --perf test runs in phases, where some # of
    # threads are setup. Insert / lookup / range-lookups are run in separate
    # phases. Then, the threads are destroyed.
    local nins_t=8
    local nlookup_t=8
    local nrange_lookup_t=8
    local test_descr="${nins_t} insert, ${nlookup_t} lookup, ${nrange_lookup_t} range lookup threads"

    # ----
    run_with_timing "Performance (sync) test ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --perf \
                                                --max-async-inflight 0 \
                                                --num-insert-threads ${nins_t} \
                                                --num-lookup-threads ${nlookup_t} \
                                                --num-range-lookup-threads ${nrange_lookup_t} \
                                                --lookup-positive-percent 10 \
                                                --tree-size-gib 4 \
                                                --db-capacity-gib 60 \
                                                --db-location ${dbname} \
                                                --verbose-progress
    rm ${dbname}

    local npthreads=8
    local tree_size=8 # GiB
    test_descr="tree-size ${tree_size} GiB, ${npthreads} pthreads"
    dbname="splinter_test.pll_perf.db"

    run_with_timing "Parallel Performance (sync) test ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --parallel-perf \
                                                --max-async-inflight 0 \
                                                --num-pthreads ${npthreads} \
                                                --lookup-positive-percent 10 \
                                                --tree-size-gib ${tree_size} \
                                                --db-capacity-gib 60 \
                                                --db-location ${dbname}
    rm ${dbname}

    # Exercise a case with max # of insert-threads which tripped an assertion
    # This isn't really a 'perf' test but a regression / stability test exec
    nins_t=63
    nrange_lookup_t=0
    test_descr="${nins_t} insert threads"
    dbname="splinter_test.max_threads.db"

    run_with_timing "Performance with max-threads ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --perf \
                                                --num-insert-threads ${nins_t} \
                                                --num-range-lookup-threads ${nrange_lookup_t} \
                                                --tree-size-gib 1 \
                                                --db-location ${dbname}
    rm ${dbname}
}

# #############################################################################
# Nightly Cache Performance tests with async disabled
# #############################################################################
function nightly_cache_perf_tests() {

    local dbname="cache_test.perf.db"
    local test_descr="default cache size"
    run_with_timing "Cache Performance test, ${test_descr}" \
            "$BINDIR"/driver_test cache_test --perf \
                                             --db-location ${dbname}

    cache_size=6  # GiB
    test_descr="${cache_size} GiB cache"
    run_with_timing "Cache Performance test, ${test_descr}" \
            "$BINDIR"/driver_test cache_test --perf \
                                             --db-location ${dbname} \
                                             --cache-capacity-gib ${cache_size} \
                                             --db-capacity-gib 60
    rm ${dbname}
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
    run_with_timing "Parallel Async Performance test ${test_descr}" \
            "$BINDIR"/driver_test splinter_test --parallel-perf \
                                                --num-bg-threads ${nbgthreads} \
                                                --max-async-inflight ${nasync} \
                                                --num-pthreads ${npthreads} \
                                                --db-capacity-gib 60 \
                                                --db-location ${dbname}
    rm ${dbname}
}

# #############################################################################
# Run through collection of nightly Performance-oriented tests
# #############################################################################
function run_nightly_perf_tests() {

    nightly_sync_perf_tests
    nightly_cache_perf_tests

    # nightly_async_perf_tests

}

# #############################################################################
# Method to check that the command actually does fail; Otherwise it's an error.
function run_check_rc() {
    echo
    set +e
    "$@"
    local rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
        echo "$Me: Test is expected to fail, but did not:" "$@"
        exit 1
   fi
}

# ##################################################################
# Exercise some common testing programs to validate that certain
# hard-limits are being correctly enforced. These don't change
# very often, so it's sufficient to test them in nightly runs.
# ##################################################################
function nightly_test_limitations() {

    # All these invocations are expected to raise an error. If they
    # sneak-through that's a test failure.
    run_check_rc "${BINDIR}"/driver_test splinter_test --page-size 1024
    run_check_rc "${BINDIR}"/driver_test splinter_test --page-size 2000
    run_check_rc "${BINDIR}"/driver_test splinter_test --page-size 2048
    run_check_rc "${BINDIR}"/driver_test splinter_test --page-size 8192

    # Only --extent-size 131072 (i.e. 32 pages/extent) is valid.
    run_check_rc "${BINDIR}"/driver_test splinter_test --extent-size 2048
    run_check_rc "${BINDIR}"/driver_test splinter_test --extent-size 4096
    run_check_rc "${BINDIR}"/driver_test splinter_test --extent-size 40960
    run_check_rc "${BINDIR}"/driver_test splinter_test --extent-size 8192
    run_check_rc "${BINDIR}"/driver_test splinter_test --extent-size 135168
    run_check_rc "${BINDIR}"/driver_test splinter_test --extent-size 262144

    # Validate that test-configs honor min / max key-sizes
    run_check_rc "${BINDIR}"/driver_test splinter_test --key-size 0
    run_check_rc "${BINDIR}"/driver_test splinter_test --key-size 7
    run_check_rc "${BINDIR}"/driver_test splinter_test --key-size 122
    run_check_rc "${BINDIR}"/driver_test splinter_test --key-size 200

    # Invoke help usage; to confirm that it still works.
    "${BINDIR}"/driver_test splinter_test --help
}

# ##################################################################
# run_build_and_test -- Driver to exercise build in different modes
# and do basic validation that build succeeded.
#
# This function manages the build output-dirs for different modes
# to enable parallel execution of test-builds. This way, we do not
# clobber build outputs across different build-modes when this test
# below runs 'make clean'.
# ##################################################################
function run_build_and_test() {

    local build_root=$1
    local build_mode=$2
    local asan_mode=$3
    local msan_mode=$4

    local binroot="${build_mode}"   # Will be 'debug' or 'release'
    local compiler="gcc"
    local outfile="${Me}.${build_mode}"
    local san=""

    if [ "${asan_mode}" == 1 ]; then
        san="asan"
        outfile="${outfile}.${san}"
        binroot="${binroot}-${san}"
   elif  [ "${msan_mode}" == 1 ]; then
        san="msan"
        outfile="${outfile}.${san}"
        binroot="${binroot}-${san}"
        compiler="clang"
   fi
    local bindir="${build_root}/${binroot}/bin"
    outfile="${outfile}.out"
    echo "${Me}: Test ${build_mode} ${san} build; tail -f $outfile"

    # --------------------------------------------------------------------------
    # Do a build in the requested mode. Some gotchas on this execution:
    #  - This step will recursively execute this script, so provide env-vars
    #    that will avoid falling into an endless recursion.
    #  - Specify a diff build-dir to test out make functionality, so that we
    #    do not clobber user's existing build/ outputs by 'make clean'
    #  - Verify that couple of build-artifacts are found in bin/ dir as expected.
    #  - Just check for the existence of driver_test, but do -not- try to run
    #    'driver_test --help', as that command exits with non-zero $rc
    # --------------------------------------------------------------------------
    {
        INCLUDE_SLOW_TESTS=false \
         RUN_MAKE_TESTS=false \
         BUILD_ROOT=${build_root} \
         BUILD_MODE=${build_mode} \
         CC=${compiler} \
         LD=${compiler} \
         BUILD_ASAN=${asan_mode} \
         BUILD_MSAN=${msan_mode} \
         make all

        echo "${Me}: Basic checks to verify few build artifacts:"
        ls -l "${bindir}"/driver_test
        "${bindir}"/unit_test --help
        "${bindir}"/unit/splinter_test --help

   }  >> "${outfile}" 2>&1
}

# ##################################################################
# test_make_all_build_modes: Basic sanity verification of builds.
# Test the 'make' interfaces for various build-modes.
# ##################################################################
function test_make_all_build_modes() {

    # clean build root dir, so we can do parallel builds below.
    local build_root=$1
    BUILD_ROOT=${build_root} make clean

    set +x
    echo "$Me: Test 'make' and ${Me} integration for various build modes."

    local build_modes="release debug optimized-debug"
    for build_mode in ${build_modes}; do
        #                                                 asan msan
        run_build_and_test "${build_root}" "${build_mode}"  0    0 &
        run_build_and_test "${build_root}" "${build_mode}"  1    0 &
        run_build_and_test "${build_root}" "${build_mode}"  0    1 &
   done
    wait
}

# ##################################################################
# Basic test to verify that the Makefile's config-conflict checks
# are working as designed.
# ##################################################################
function test_make_config_conflicts() {
    set +x
    local build_root=$1
    BUILD_ROOT=${build_root} make clean

    local outfile="${Me}.config_conflicts.out"
    echo "${Me}: test Makefile config-conflict detection ... tail -f ${outfile}"

    # Should succeed
    BUILD_ROOT=${build_root} CC=gcc LD=gcc make libs >> "${outfile}" 2>&1

    # Should also succeed in another build-mode
    BUILD_ROOT=${build_root} CC=gcc LD=gcc BUILD_MODE=debug make libs >> "${outfile}" 2>&1

    # These commands are supposed to fail, so turn this OFF
    set +e
    BUILD_ROOT=${build_root} CC=clang LD=clang make libs >> "${outfile}" 2>&1
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        echo "$Me:${LINENO}: Test is expected to fail, but did not."
        exit 1
   fi
    BUILD_ROOT=${build_root} CC=clang LD=clang BUILD_MODE=debug make libs >> "${outfile}" 2>&1
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        echo "$Me:${LINENO} Test is expected to fail, but did not."
        exit 1
   fi
}

# ##################################################################
# Driver function to exercise 'make' build-tests
# ##################################################################
function test_make_run_tests() {
    local build_root="/tmp/test-builds"
    test_make_config_conflicts "${build_root}"
    test_make_all_build_modes "${build_root}"

    if [ -d ${build_root} ]; then rm -rf "${build_root}"; fi
}

# ##################################################################
# Smoke Tests: Run a small collection of fast-running unit-tests
# ##################################################################
function run_fast_unit_tests() {

   "$BINDIR"/unit/splinterdb_quick_test
   "$BINDIR"/unit/btree_test
   "$BINDIR"/unit/util_test
   "$BINDIR"/unit/misc_test
   "$BINDIR"/unit/limitations_test
   "$BINDIR"/unit/task_system_test

   "$BINDIR"/driver_test io_apis_test
}

# ##################################################################
# Run mini-unit-tests that were excluded from bin/unit_test binary:
# Explicitly run individual cases from specific slow running unit-tests,
# where appropriate with a different test-configuration that has been
# found to provide the required coverage.
# ##################################################################
function run_slower_unit_tests() {

    run_with_timing "Splinter inserts test" "$BINDIR"/unit/splinter_test test_inserts

    # Use fewer rows for this case, to keep elapsed times of MSAN runs reasonable.
    run_with_timing "Splinter lookups test" \
        "$BINDIR"/unit/splinter_test --num-inserts 2000000 test_lookups

    run_with_timing "Splinter print diagnostics test" \
        "$BINDIR"/unit/splinter_test test_splinter_print_diags
}

# ##################################################################
# Execute a few variations of splinter_test --functionality tests
# ##################################################################
function run_splinter_functionality_tests() {
    key_size=8
    run_with_timing "Functionality test, key size=${key_size} bytes" \
        "$BINDIR"/driver_test splinter_test --functionality 1000000 100 \
                                            --key-size ${key_size} --seed "$SEED"

    run_with_timing "Functionality test, with default key size" \
        "$BINDIR"/driver_test splinter_test --functionality 1000000 100 \
                                            --seed "$SEED"

    run_with_timing "Functionality test, default key size, with background threads" \
        "$BINDIR"/driver_test splinter_test --functionality 1000000 100 \
                                            --num-normal-bg-threads 4 --num-memtable-bg-threads 2 \
                                            --seed "$SEED"

    max_key_size=102
    run_with_timing "Functionality test, key size=maximum (${max_key_size} bytes)" \
        "$BINDIR"/driver_test splinter_test --functionality 1000000 100 \
                                            --key-size ${max_key_size} --seed "$SEED"
}

# ##################################################################
# Execute a few variations of splinter_test --perf tests
# ##################################################################
function run_splinter_perf_tests() {

   # Validate use of small # of --num-inserts, and --verbose-progress
   # Test-case basically is for functional testing of interfaces.
   run_with_timing "Very quick Performance test" \
        "$BINDIR"/driver_test splinter_test --perf \
                                            --max-async-inflight 0 \
                                            --num-insert-threads 4 \
                                            --num-lookup-threads 4 \
                                            --num-range-lookup-threads 4 \
                                            --lookup-positive-percent 10 \
                                            --num-inserts 10000 \
                                            --cache-capacity-mib 512 \
                                            --verbose-progress

   # Re-run small perf test configuring background threads. This scenario
   # validates that we can configure bg- and user-threads in one go.
   run_with_timing "Quick Performance test with bg-threads" \
        "$BINDIR"/driver_test splinter_test --perf \
                                            --num-insert-threads 4 \
                                            --num-lookup-threads 4 \
                                            --num-inserts 10000 \
                                            --cache-capacity-mib 512 \
                                            --num-normal-bg-threads 1 \
                                            --num-memtable-bg-threads 1

   run_with_timing "Performance test" \
        "$BINDIR"/driver_test splinter_test --perf \
                                            --max-async-inflight 0 \
                                            --num-insert-threads 4 \
                                            --num-lookup-threads 4 \
                                            --num-range-lookup-threads 0 \
                                            --tree-size-gib 2 \
                                            --cache-capacity-mib 512
}

# ##################################################################
# Execute BTree tests, including BTree perf test case
# ##################################################################
function run_btree_tests() {
    key_size=8
    run_with_timing "BTree test, key size=${key_size} bytes" \
        "$BINDIR"/driver_test btree_test --key-size ${key_size} \
                                         --seed "$SEED"

    run_with_timing "BTree test, with default key size" \
        "$BINDIR"/driver_test btree_test --seed "$SEED"

    key_size=100
    run_with_timing "BTree test, key size=${key_size} bytes" \
        "$BINDIR"/driver_test btree_test --key-size ${key_size} --seed "$SEED"

    run_with_timing "BTree Perf test"
        "$BINDIR"/driver_test btree_test --perf \
                                         --cache-capacity-gib 4 \
                                         --seed "$SEED"
}

# ##################################################################
# Run remaining functionality-related tests from driver_test
# ##################################################################
function run_other_driver_tests() {

    run_with_timing "Cache test" \
        "$BINDIR"/driver_test cache_test --seed "$SEED"

    run_with_timing "Log test" \
        "$BINDIR"/driver_test log_test --seed "$SEED"

    run_with_timing "Filter test" \
        "$BINDIR"/driver_test filter_test --seed "$SEED"
}

# ##################################################################
# main() begins here
# ##################################################################

if [ $# -eq 1 ] && [ "$1" == "--help" ]; then
    usage
    exit 0
fi

echo "$Me: $(TZ="America/Los_Angeles" date) Start SplinterDB Test Suite Execution."
set -x

SEED="${SEED:-135}"

run_type=" "
if [ "$RUN_NIGHTLY_TESTS" == "true" ]; then
   run_type=" Nightly "
fi

set +x

# Track total elapsed time for entire test-suite's run
testRunStartSeconds=$SECONDS

# Initialize test-execution timing log file
echo "$(TZ="America/Los_Angeles" date) **** SplinterDB${run_type}Test Suite Execution Times **** " > "${test_exec_log_file}"
echo >> "${test_exec_log_file}"

# ---- Nightly Stress and Performance test runs ----
if [ "$RUN_NIGHTLY_TESTS" == "true" ]; then

    set +e
    run_with_timing "Check limits, error conditions." nightly_test_limitations

    run_nightly_stress_tests

    run_nightly_perf_tests
    set -e

    record_elapsed_time ${testRunStartSeconds} "Nightly Stress & Performance Tests"
    cat_exec_log_file
    exit 0
fi

# ---- Fast running Smoke test runs ----
if [ "$INCLUDE_SLOW_TESTS" != "true" ]; then

   # For some coverage, exercise --help, --list args for unit test binaries
   set -x
   "$BINDIR"/unit_test --help
   "$BINDIR"/unit_test --list
   "$BINDIR"/unit_test --list splinterdb_quick
   "$BINDIR"/unit/btree_test --help
   "$BINDIR"/unit/splinterdb_quick_test --list
   set +x

   echo
   echo "NOTE: **** Only running fast unit tests ****"
   echo "To run all tests, set the env var, and re-run: $ INCLUDE_SLOW_TESTS=true ./$Me"
   echo

   # Exercise config-parsing test case. Here, we feed-in a set of
   # --config-params that the test code knows to "expect" and validates.
   # These options can come in any order.
   set -x
   run_with_timing "Config-params parsing test"
            "$BINDIR"/unit/config_parse_test --log \
                                             --max-branches-per-node 42 \
                                             --num-inserts 20 \
                                             --rough-count-height 11 \
                                             --stats \
                                             --verbose-logging \
                                             --verbose-progress
   set +x

   start_seconds=$SECONDS

   run_with_timing "Smoke tests" run_fast_unit_tests

   if [ "$RUN_MAKE_TESTS" == "true" ]; then
      run_with_timing "Basic build-and-test tests" test_make_run_tests
   fi

   cat_exec_log_file
   exit 0
fi

# ---- Rest of the coverage runs included in CI test runs ----

# ------------------------------------------------------------------------
# Fast-path execution support. You can invoke this script specifying the
# name of one of the functions to execute a specific set of tests. If the
# function takes arguments, pass them on the command-line. This way, one
# can debug script changes to ensure that test-execution still works.
#
# Examples:
#      INCLUDE_SLOW_TESTS=true ./test.sh run_btree_tests
# ------------------------------------------------------------------------
if [ $# -ge 1 ]; then

   # shellcheck disable=SC2048
   $*
   record_elapsed_time ${testRunStartSeconds} "All Tests"
   cat_exec_log_file
   exit 0
fi

# Run all the unit-tests first, to get basic coverage
run_with_timing "Fast unit tests" "$BINDIR"/unit_test

# ------------------------------------------------------------------------
# Run mini-unit-tests that were excluded from bin/unit_test binary:
# ------------------------------------------------------------------------
run_slower_unit_tests

UNIT_TESTS_DB_DEV="unit_tests_db"
if [ -f ${UNIT_TESTS_DB_DEV} ]; then
    rm ${UNIT_TESTS_DB_DEV}
fi

run_splinter_functionality_tests

run_splinter_perf_tests

run_btree_tests

run_other_driver_tests

record_elapsed_time ${testRunStartSeconds} "All Tests"
echo ALL PASSED

cat_exec_log_file
