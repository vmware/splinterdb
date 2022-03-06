#!/bin/bash
# ##############################################################################
# monOSMem.sh: Script to run in the background and monitor OS memory usage
# Useful to track failures while executing large tests, especially in CI-envs
# ##############################################################################

set -euo pipefail
Me=$(basename "$0")

Usage="$Me <procTag-to-loop-for> [ <numLoops> [ <proc-args> ] ]

Examples:
    $Me <process-name>
    $Me <process-name> 20
    $Me <process-name> 0 <args>
    $Me <process-name> 20 <args>

Defaults:
 - Uses 'free -g' to track memory usage, so min-memory usage is 1G
 - Monitor given process till it exits
 - Specify <numLoops> as 0, if <proc-args> filter is also specified
   to monitor process till it exits
 - E.g. To monitor test execution: $ $Me 0 \"splinter_test --functionality\"
"

if [ $# -eq 0 ]; then
    echo "$Usage"
    exit 1
fi

# Setup defaults
numLoops=0
sleepSecs=5

# Pick-up user arguments
if [ $# -ge 1 ]; then procTag=$1;       fi
procArgs=${procTag}

if [ $# -ge 2 ]; then numLoops=$2;      fi
if [ $# -ge 3 ]; then procArgs="$3";    fi

echo "---- OS-Memory usage report ----------------------------------------------------"
echo "$Me: $(TZ='America/Los_Angeles' date) Monitor OS-memory usage for process(es) '${procTag}', args '${procArgs}' ..."

# Wait for processes being monitored to start up
sleep 5

memTitle=$(free | head -1)

loopCtr=0
HWMloopCtr=0
usedMemHWM=0
usedMemHWMLine=""

totalMemGiB=$(free -g | grep "^Mem:" | awk '{print $2}')

# ---- ACTION IS HERE ----
# Loop around probing for memory usage till the tagged process is still running.
# Filter out our own process's line in 'ps' which will show up with $procTag.
pgrep -a "${procTag}" | grep -E "${procArgs}"
os_pid=$(pgrep -a "${procTag}" | cut -f1 -d' ')

# The same proces may be re-started, when running tests, with the same args.
# Filter on OS-pid to collect memusage stats till that OS-pid exits.
echo "${memTitle}   loopCtr"
while [ "$(pgrep -a "${procTag}" | grep "${os_pid}" | grep -c -E "${procArgs}")" -gt 0 ]; do
    freeMemLine=$(free -g | grep "^Mem:")
    usedMem=$(echo "${freeMemLine}" | awk '{print $3}')

    if [ "${usedMem}" -gt ${usedMemHWM} ]; then
        usedMemHWM=${usedMem}
        usedMemHWMLine=${freeMemLine}

        # Extract HWM-stats, for reporting ...
        usedMemHWMGiB=$(echo "${usedMemHWMLine}" | awk '{print $3}')
        HWMloopCtr=${loopCtr}
        echo "${freeMemLine}   [${loopCtr}: ${usedMemHWMGiB} of ${totalMemGiB} GiB at $(TZ='America/Los_Angeles' date)]"
   fi

    sleep ${sleepSecs}

    loopCtr=$((loopCtr + 1))
    if [ ${loopCtr} -eq "${numLoops}" ]; then break; fi
done
set +x

echo "--- HWM of OS-Memory usage (GiB) for OS-pid=${os_pid} ${usedMemHWM} [at loopCtr = ${HWMloopCtr} of ${loopCtr}]"
echo "${memTitle}"
echo "${usedMemHWMLine}"
echo "---- ---------------------------------------------------------------------------"
echo
