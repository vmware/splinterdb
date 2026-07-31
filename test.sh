#!/bin/bash

set -e

source timing_utils.sh

DB_CAPACITY="--db-capacity-gib 25"

function run()
{
    run_with_timing "$*" ${BINDIR}/$@ $DB_CAPACITY
    rm -f db
}

# Run a workload that is expected to complete at least one checkpoint, and fail
# if it completes none.
#
# A plain run() would pass whether or not checkpointing ever fired, so a
# regression that quietly stopped taking checkpoints would look green.  The
# caller must pass --log (to enable the log, and hence auto-checkpointing) and
# --stats (so core_print_insertion_stats() emits the "| checkpoints:" line this
# parses).  Several drivers dump stats more than once and the counter is
# cumulative, so take the largest value printed.
function run_checkpointed()
{
    local logfile
    logfile=$(mktemp)

    set +e
    run_with_timing "$*" ${BINDIR}/$@ $DB_CAPACITY > "$logfile" 2>&1
    local rc=$?
    set -e

    cat "$logfile"
    rm -f db

    if [ "$rc" -ne 0 ]; then
        rm -f "$logfile"
        echo "FAILED: $* exited with status $rc"
        exit 1
    fi

    local checkpoints
    checkpoints=$(grep -oE '\| checkpoints: +[0-9]+' "$logfile" \
                  | grep -oE '[0-9]+' | sort -rn | head -1)
    rm -f "$logfile"

    if [ -z "$checkpoints" ]; then
        echo "FAILED: $* printed no checkpoint statistics" \
             "(both --log and --stats are required)"
        exit 1
    fi
    if [ "$checkpoints" -eq 0 ]; then
        echo "FAILED: $* completed 0 checkpoints;" \
             "lower --checkpoint-log-size-mib or grow the workload"
        exit 1
    fi
    echo "PASSED: $* completed ${checkpoints} checkpoints"
}

# 14 minutes
function cache_tests_1() {
    # 25 sec each
    run driver_test cache_test --perf
    run driver_test cache_test --perf     --use-shmem
    # 3 sec each
    run driver_test cache_test --seed 135
    run driver_test cache_test --seed 135 --use-shmem

    # 8 sec
    run driver_test cache_test --async

}

function cache_tests_2() {
    # 390 sec each
    run driver_test cache_test --perf --cache-capacity-gib 4
}

function cache_tests_3() {
        # 390 sec each
    run driver_test cache_test --perf --cache-capacity-gib 4 --use-shmem
}

# 12 minutes
function functionality_tests() {
    # 50 sec each
    run driver_test splinter_test --functionality  1000000  100                                                                                                            --seed 135
    run driver_test splinter_test --functionality  1000000  100                                                      --num-normal-bg-threads 4 --num-memtable-bg-threads 2 --seed 135
    run driver_test splinter_test --functionality  1000000  100             --key-size 102                                                                                 --seed 135
    run driver_test splinter_test --functionality  1000000  100             --key-size 8                                                                                   --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem                                                                                                --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem                                          --num-normal-bg-threads 4 --num-memtable-bg-threads 2 --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem --key-size 102                                                                                 --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem --key-size 8                                                                                   --seed 135
    run driver_test splinter_test --functionality  1000000 1000                            --cache-capacity-mib 1024
    run driver_test splinter_test --functionality  1000000 1000                            --cache-capacity-mib 512
    run driver_test splinter_test --functionality 10000000 1000                            --cache-capacity-mib 4096
}

# 8 minutes
#
# Logging (and hence auto-checkpointing) is off by default in every other group
# here, so without these the checkpoint machinery goes essentially unexercised
# outside the unit tests.  --checkpoint-log-size-mib is deliberately far below
# the production default (one cache's worth) so that checkpoints fire many times
# during a short run; run_checkpointed() asserts they actually did.
function checkpoint_tests() {
    # 45 sec each.  Correctness under checkpointing: the shadow-verified
    # workload, which will catch a checkpoint that corrupts or loses data.
    run_checkpointed driver_test splinter_test --functionality 500000 100 --seed 135 --log --checkpoint-log-size-mib 2 --stats
    run_checkpointed driver_test splinter_test --functionality 500000 100 --use-shmem --seed 135 --log --checkpoint-log-size-mib 2 --stats

    # 60 sec.  Concurrent inserts/lookups/range-lookups across a log cut.
    run_checkpointed driver_test splinter_test --perf --max-async-inflight 0 --num-insert-threads 4 --num-lookup-threads 4 --num-range-lookup-threads 4 --num-inserts 300000 --cache-capacity-mib 512 --log --checkpoint-log-size-mib 2 --stats --num-normal-bg-threads 2 --num-memtable-bg-threads 2

    # 90 sec each.  Regression coverage for the pending_gcs crash: a checkpoint
    # retiring the old root while an async lookup still pins it in the cache
    # deferred the node's destruction onto a scratch context that was freed
    # immediately after.  Needs async lookups AND frequent checkpoints together.
    run_checkpointed driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 8 --tree-size-mib 512 --num-normal-bg-threads 4 --num-memtable-bg-threads 2 --log --checkpoint-log-size-mib 2 --stats
    run_checkpointed driver_test splinter_test --parallel-perf --max-async-inflight 0 --num-pthreads 8 --lookup-positive-percent 10 --tree-size-mib 512 --log --checkpoint-log-size-mib 2 --stats

    # 45 sec.  Deletes and repeated overwrite rounds; the overwrite workload is
    # the one that never advances the memtable generation, so it exercises the
    # log-size trigger rather than the generation-count one.
    run_checkpointed driver_test splinter_test --delete --tree-size-mib 512 --log --checkpoint-log-size-mib 2 --stats
    run_checkpointed driver_test splinter_test --periodic --tree-size-mib 256 --log --checkpoint-log-size-mib 2 --stats

    # 60 sec.  A realistic cadence rather than a pathological one, so the
    # coverage does not depend solely on a tiny threshold.
    run_checkpointed driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 8 --tree-size-mib 512 --num-normal-bg-threads 4 --num-memtable-bg-threads 2 --log --checkpoint-log-size-mib 64 --stats
}

function parallel_perf_test_1() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight  0 --num-pthreads  8 --lookup-positive-percent 10 --tree-size-gib 8                                                        
}

function parallel_perf_test_2() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight  0 --num-pthreads  8 --lookup-positive-percent 10 --tree-size-gib 8                                                        --use-shmem
}

function parallel_perf_test_3() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 20                              --tree-size-gib 5 --num-normal-bg-threads 20 --num-memtable-bg-threads 2 
}

function parallel_perf_test_4() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 20                              --tree-size-gib 5 --num-normal-bg-threads 20 --num-memtable-bg-threads 2 
}


# 10 minutes
function perf_tests_1() {
    # 60 sec each
    run driver_test splinter_test --perf                                    --num-insert-threads 4  --num-lookup-threads 4                                                                             --num-inserts 10000 --cache-capacity-mib 512                    --num-normal-bg-threads 1 --num-memtable-bg-threads 1
    run driver_test splinter_test --perf                                    --num-insert-threads 63                        --num-range-lookup-threads 0                              --tree-size-gib 1
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 0                              --tree-size-gib 2                     --cache-capacity-mib 512
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 4 --lookup-positive-percent 10                   --num-inserts 10000 --cache-capacity-mib 512 --verbose-progress
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 8  --num-lookup-threads 8 --num-range-lookup-threads 8 --lookup-positive-percent 10 --tree-size-gib 4                                              --verbose-progress
}

# 10 minutes
function perf_tests_2() {
    # 60 sec each
    run driver_test splinter_test --perf --use-shmem                        --num-insert-threads 4  --num-lookup-threads 4                                                                             --num-inserts 10000 --cache-capacity-mib 512                    --num-normal-bg-threads 1 --num-memtable-bg-threads 1
    run driver_test splinter_test --perf --use-shmem                        --num-insert-threads 63                        --num-range-lookup-threads 0                              --tree-size-gib 1
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 0                              --tree-size-gib 2                     --cache-capacity-mib 512
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 4 --lookup-positive-percent 10                   --num-inserts 10000 --cache-capacity-mib 512 --verbose-progress
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 8  --num-lookup-threads 8 --num-range-lookup-threads 8 --lookup-positive-percent 10 --tree-size-gib 4                                              --verbose-progress
}

# 2.5 minutes
function splinter_misc_tests_1()
{
    # 30 sec each
    run driver_test splinter_test --delete        --tree-size-gib 1
    run driver_test splinter_test --seq-perf      --tree-size-gib 1
    run driver_test splinter_test --semiseq-perf  --tree-size-gib 1
}

function splinter_misc_tests_2()
{
    # 60 sec
    run driver_test splinter_test --periodic      --tree-size-gib 1
}

function large_insert_stress_tests_1() {
    # 25, 50 sec each
    run unit/large_inserts_stress_test             --num-inserts  1000000
    run unit/large_inserts_stress_test             --num-inserts  2000000 --num-normal-bg-threads  4 --num-memtable-bg-threads 3
    run unit/large_inserts_stress_test --use-shmem --num-inserts  1000000
    run unit/large_inserts_stress_test --use-shmem --num-inserts  2000000 --num-normal-bg-threads  4 --num-memtable-bg-threads 3
}

function large_insert_stress_tests_2() {
    # 250 sec
    run unit/large_inserts_stress_test             --num-inserts  6000000 --num-normal-bg-threads 20 --num-memtable-bg-threads 8
}

function large_insert_stress_tests_3() {
    # 250 sec
    run unit/large_inserts_stress_test --use-shmem --num-inserts  6000000 --num-normal-bg-threads 20 --num-memtable-bg-threads 8
}

function filter_tests() {
    # 2 sec each
    run driver_test filter_test --seed 135
    run driver_test filter_test --seed 135 --use-shmem
    # 255 sec
    run driver_test filter_test --perf

}

function unit_tests() {
    # 40 sec each
    run unit_test
    run unit_test --use-shmem
}

# 8 minutes
function misc_tests() {
    # 15 sec each
    #                                          default:  24
    run driver_test btree_test                            --seed 135
    run driver_test btree_test                --use-shmem --seed 135
    run driver_test btree_test --key-size   8             --seed 135
    run driver_test btree_test --key-size   8 --use-shmem --seed 135
    run driver_test btree_test --key-size 100             --seed 135
    run driver_test btree_test --key-size 100 --use-shmem --seed 135

    # 17 sec each
    #                                               default: 1 (but --perf requires >= 4)
    run driver_test btree_test --perf --cache-capacity-gib 4 --seed 135
    run driver_test btree_test --perf --cache-capacity-gib 4 --seed 135 --use-shmem

    # 1 sec each
    run driver_test log_test --seed 135
    run driver_test log_test --seed 135 --use-shmem
    run driver_test log_test --crash
    # 14 sec
    run driver_test log_test --perf

    # 12 sec each
    run unit/splinter_test             --num-inserts 2000000
    run unit/splinter_test --use-shmem --num-inserts 2000000
}

function all_tests() {
    cache_tests_1
    cache_tests_2
    cache_tests_3
    functionality_tests
    checkpoint_tests
    parallel_perf_test_1
    parallel_perf_test_2
    parallel_perf_test_3
    parallel_perf_test_4
    perf_tests_1
    perf_tests_2
    splinter_misc_tests_1
    splinter_misc_tests_2
    large_insert_stress_tests_1
    large_insert_stress_tests_2
    large_insert_stress_tests_3
    misc_tests
    filter_tests
    unit_tests
}

function main() {
    if [ -z "$TESTS_FUNCTION" ]; then
        TESTS_FUNCTION="all_tests"
    fi
    $TESTS_FUNCTION
    cat_exec_log_file
}

main "$@"
