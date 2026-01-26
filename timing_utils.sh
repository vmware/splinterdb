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

   echo "${Me}: $(TZ="America/Los_Angeles" date) ${test_tag}: ${total_seconds} s [ ${el_h}h ${el_m}m ${el_s}s ]"

   # Construct print format string for use by awk
   local fmtstr="%-105s: %4ds [ %2dh %2dm %2ds ]\n"

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
   echo " "
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
   echo " "
   echo "$(TZ="America/Los_Angeles" date) End SplinterDB Test Suite Execution."
}

