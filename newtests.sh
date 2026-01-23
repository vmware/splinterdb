#!/bin/bash

set -e

source timing_utils.sh

function run()
{
    run_with_timing "$*" ${BINDIR}/$@
    rm -f db
}

# 14 minutes
function cache_tests() {
    # 25 sec each
    run driver_test cache_test --perf
    run driver_test cache_test --perf                                             --use-shmem
    # 390 sec each
    run driver_test cache_test --perf --cache-capacity-gib 6 --db-capacity-gib 60
    run driver_test cache_test --perf --cache-capacity-gib 6 --db-capacity-gib 60 --use-shmem
    # 3 sec each
    run driver_test cache_test --seed 135
    run driver_test cache_test --seed 135 --use-shmem

    # 8 sec
    run driver_test cache_test --async

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

# 8 minutes
function parallel_perf_tests() {
    # 115 sec each
    run driver_test splinter_test --parallel-perf --max-async-inflight  0 --num-pthreads  8 --lookup-positive-percent 10 --tree-size-gib 8                                                        --db-capacity-gib 60
    run driver_test splinter_test --parallel-perf --max-async-inflight  0 --num-pthreads  8 --lookup-positive-percent 10 --tree-size-gib 8                                                        --db-capacity-gib 60 --use-shmem
    run driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 20                              --tree-size-gib 5 --num-normal-bg-threads 20 --num-memtable-bg-threads 2 --db-capacity-gib 60
    run driver_test splinter_test --parallel-perf --max-async-inflight 10 --num-pthreads 20                              --tree-size-gib 5 --num-normal-bg-threads 20 --num-memtable-bg-threads 2 --db-capacity-gib 60
}

# 10 minutes
function perf_tests() {
    # 60 sec each
    run driver_test splinter_test --perf                                    --num-insert-threads 4  --num-lookup-threads 4                                                                             --num-inserts 10000 --cache-capacity-mib 512                                         --num-normal-bg-threads 1 --num-memtable-bg-threads 1
    run driver_test splinter_test --perf                                    --num-insert-threads 63                        --num-range-lookup-threads 0                              --tree-size-gib 1
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 0                              --tree-size-gib 2                     --cache-capacity-mib 512
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 4 --lookup-positive-percent 10                   --num-inserts 10000 --cache-capacity-mib 512                      --verbose-progress
    run driver_test splinter_test --perf             --max-async-inflight 0 --num-insert-threads 8  --num-lookup-threads 8 --num-range-lookup-threads 8 --lookup-positive-percent 10 --tree-size-gib 4                                              --db-capacity-gib 60 --verbose-progress
    run driver_test splinter_test --perf --use-shmem                        --num-insert-threads 4  --num-lookup-threads 4                                                                             --num-inserts 10000 --cache-capacity-mib 512                                         --num-normal-bg-threads 1 --num-memtable-bg-threads 1
    run driver_test splinter_test --perf --use-shmem                        --num-insert-threads 63                        --num-range-lookup-threads 0                              --tree-size-gib 1
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 0                              --tree-size-gib 2                     --cache-capacity-mib 512
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 4  --num-lookup-threads 4 --num-range-lookup-threads 4 --lookup-positive-percent 10                   --num-inserts 10000 --cache-capacity-mib 512                      --verbose-progress
    run driver_test splinter_test --perf --use-shmem --max-async-inflight 0 --num-insert-threads 8  --num-lookup-threads 8 --num-range-lookup-threads 8 --lookup-positive-percent 10 --tree-size-gib 4                                              --db-capacity-gib 60 --verbose-progress
}

# 2.5 minutes
function splinter_misc_tests()
{
    # 30 sec each
    run driver_test splinter_test --delete        --tree-size-gib 1
    run driver_test splinter_test --seq-perf      --tree-size-gib 1
    run driver_test splinter_test --semiseq-perf  --tree-size-gib 1
    # 60 sec
    run driver_test splinter_test --periodic      --tree-size-gib 1

}

# 11 minutes
function large_insert_stress_tests() {
    # 25, 50, 250 sec each (prob. total of 650 sec for all 6 tests)
    run unit/large_inserts_stress_test                                    --num-inserts  1000000
    run unit/large_inserts_stress_test                                    --num-inserts  2000000 --num-normal-bg-threads  4 --num-memtable-bg-threads 3
    run unit/large_inserts_stress_test             --shmem-capacity-gib 8 --num-inserts 10000000 --num-normal-bg-threads 20 --num-memtable-bg-threads 8
    run unit/large_inserts_stress_test --use-shmem                        --num-inserts  1000000
    run unit/large_inserts_stress_test --use-shmem                        --num-inserts  2000000 --num-normal-bg-threads  4 --num-memtable-bg-threads 3
    run unit/large_inserts_stress_test --use-shmem --shmem-capacity-gib 8 --num-inserts 10000000 --num-normal-bg-threads 20 --num-memtable-bg-threads 8
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

    # 2 sec each
    run driver_test filter_test --seed 135
    run driver_test filter_test --seed 135 --use-shmem
    # 255 sec
    run driver_test filter_test --perf

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

    # 40 sec each
    run unit_test
    run unit_test --use-shmem
}

function all_tests() {
    cache_tests
    functionality_tests
    parallel_perf_tests
    perf_tests
    splinter_misc_tests
    large_insert_stress_tests
    misc_tests
}

function main() {
    $NEWTESTS_FUNCTION
    cat_exec_log_file
}

main "$@"
