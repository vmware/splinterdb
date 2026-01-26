#!/bin/bash

set -e

source timing_utils.sh

function run()
{
    run_with_timing "$*" ${BINDIR}/$@
    rm -f db
}

# 14 minutes
function cache_tests_1() {
    # 25 sec each
    run driver_test cache_test --perf
    run driver_test cache_test --perf                                             --use-shmem
    # 3 sec each
    run driver_test cache_test --seed 135
    run driver_test cache_test --seed 135 --use-shmem

    # 8 sec
    run driver_test cache_test --async

}

function cache_tests_2() {
    # 390 sec each
    run driver_test cache_test --perf --cache-capacity-gib 6 --db-capacity-gib 60
}

function cache_tests_3() {
        # 390 sec each
    run driver_test cache_test --perf --cache-capacity-gib 6 --db-capacity-gib 60 --use-shmem
}

# 12 minutes
function functionality_tests() {
    # 50 sec each
    run driver_test splinter_test --functionality  1000000  100                                                                                                                           --seed 135
    run driver_test splinter_test --functionality  1000000  100                                                                     --num-normal-bg-threads 4 --num-memtable-bg-threads 2 --seed 135
    run driver_test splinter_test --functionality  1000000  100             --key-size 102                                                                                                --seed 135
    run driver_test splinter_test --functionality  1000000  100             --key-size 8                                                                                                  --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem                                                                                                               --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem                                                         --num-normal-bg-threads 4 --num-memtable-bg-threads 2 --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem --key-size 102                                                                                                --seed 135
    run driver_test splinter_test --functionality  1000000  100 --use-shmem --key-size 8                                                                                                  --seed 135
    run driver_test splinter_test --functionality  1000000 1000                            --num-tables 2 --cache-capacity-mib 1024
    run driver_test splinter_test --functionality  1000000 1000                            --num-tables 4 --cache-capacity-mib 1024
    run driver_test splinter_test --functionality  1000000 1000                            --num-tables 4 --cache-capacity-mib 512
    run driver_test splinter_test --functionality 10000000 1000                            --num-tables 1 --cache-capacity-mib 4096
    run driver_test splinter_test --functionality 10000000 1000                            --num-tables 2 --cache-capacity-mib 4096
}

function parallel_perf_test_1() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight  0 --num-pthreads  8 --lookup-positive-percent 10 --tree-size-gib 8                                                        --db-capacity-gib 60
}

function parallel_perf_test_2() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight  0 --num-pthreads  8 --lookup-positive-percent 10 --tree-size-gib 8                                                        --db-capacity-gib 60 --use-shmem
}

function parallel_perf_test_3() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 20                              --tree-size-gib 5 --num-normal-bg-threads 20 --num-memtable-bg-threads 2 --db-capacity-gib 60
}

function parallel_perf_test_4() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 20                              --tree-size-gib 5 --num-normal-bg-threads 20 --num-memtable-bg-threads 2 --db-capacity-gib 60
}


# 10 minutes
function perf_tests_1() {
    # 60 sec each
    run driver_test splinter_test --perf                                    --num-insert-threads 4  --num-lookup-threads 4                                                                             --num-inserts 10000 --cache-capacity-mib 512                                         --num-normal-bg-threads 1 --num-memtable-bg-threads 1
    run driver_test splinter_test --perf                                    --num-insert-threads 63                        --num-range-lookup-threads 0                              --tree-size-gib 1
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 0                              --tree-size-gib 2                     --cache-capacity-mib 512
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 4 --lookup-positive-percent 10                   --num-inserts 10000 --cache-capacity-mib 512                      --verbose-progress
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 8  --num-lookup-threads 8 --num-range-lookup-threads 8 --lookup-positive-percent 10 --tree-size-gib 4                                              --db-capacity-gib 60 --verbose-progress
}

# 10 minutes
function perf_tests_2() {
    # 60 sec each
    run driver_test splinter_test --perf --use-shmem                        --num-insert-threads 4  --num-lookup-threads 4                                                                             --num-inserts 10000 --cache-capacity-mib 512                                         --num-normal-bg-threads 1 --num-memtable-bg-threads 1
    run driver_test splinter_test --perf --use-shmem                        --num-insert-threads 63                        --num-range-lookup-threads 0                              --tree-size-gib 1
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 0                              --tree-size-gib 2                     --cache-capacity-mib 512
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 4 --lookup-positive-percent 10                   --num-inserts 10000 --cache-capacity-mib 512                      --verbose-progress
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 8  --num-lookup-threads 8 --num-range-lookup-threads 8 --lookup-positive-percent 10 --tree-size-gib 4                                              --db-capacity-gib 60 --verbose-progress
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
    run unit/large_inserts_stress_test                                    --num-inserts  1000000
    run unit/large_inserts_stress_test                                    --num-inserts  2000000 --num-normal-bg-threads  4 --num-memtable-bg-threads 3
    run unit/large_inserts_stress_test --use-shmem                        --num-inserts  1000000
    run unit/large_inserts_stress_test --use-shmem                        --num-inserts  2000000 --num-normal-bg-threads  4 --num-memtable-bg-threads 3
}

function large_insert_stress_tests_2() {
    # 250 sec
    run unit/large_inserts_stress_test             --shmem-capacity-gib 8 --num-inserts 10000000 --num-normal-bg-threads 20 --num-memtable-bg-threads 8
}

function large_insert_stress_tests_3() {
    # 250 sec
    run unit/large_inserts_stress_test --use-shmem --shmem-capacity-gib 8 --num-inserts 10000000 --num-normal-bg-threads 20 --num-memtable-bg-threads 8
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
    run unit/splinter_test             --num-inserts 2000000 test_lookups
    run unit/splinter_test --use-shmem --num-inserts 2000000 test_lookups

    # 2 sec each
    run unit/splinter_test             test_inserts
    run unit/splinter_test             test_splinter_print_diags
    run unit/splinter_test --use-shmem test_inserts
    run unit/splinter_test --use-shmem test_splinter_print_diags
}

function all_tests() {
    cache_tests_1
    cache_tests_2
    cache_tests_3
    functionality_tests
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
