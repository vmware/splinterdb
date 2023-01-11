#!/bin/bash

set -ex

if [ "$FORCE" == "" ]; then
    FORCE=0
fi

ITERATIONS=5

PMEM_ROOT=/tmp/atc23-persistron

function workload_args () {
    local result=
    for w in $*; do
        result="$result -W workloads/$w.spec"
    done
    echo "$result"
}

function run_if_needed () {
    local file=$1
    shift
    local cmd=$*

    if [[ ! -e "$file" || "$FORCE" == 1 ]]; then
        $cmd > "$file" 2>&1
    fi
}

CGROUP_NAME=persistron-atc23
CGROUP_OVERHEAD_MIB=4096

function configure_cgroup () {
    local size_mib=$1
    shift
    local size_b=$(( ($size_mib + $CGROUP_OVERHEAD_MIB) * 1024 * 1024 ))
    echo 0 > /sys/fs/cgroup/memory/$CGROUP_NAME/memory.swappiness
    echo $size_b > /sys/fs/cgroup/memory/$CGROUP_NAME/memory.limit_in_bytes
}

SPLINTER_DIR=.
MATRIXKV_DIR=../MatrixKV
YCSBC_DIR=../YCSB-C

export COMPILER=gcc
export CC=$COMPILER
export LD=$COMPILER

function build_splinterdb_ycsb() {
    local name=$1
    shift
    local cflags=$*
    (cd $SPLINTER_DIR; make clean; DEFAULT_CFLAGS="$cflags" make -j8; make install) > raw-data/${name}.build 2>&1
    (cd $YCSBC_DIR; make clean; make) > raw-data/${name}_ycsb.build 2>&1
}

function execute_splinterdb_ycsb() {
    local name=$1
    shift
    local nthreads=$1
    shift
    local dram_cache_size_mib=$1
    shift
    local pmem_cache_size_mib=$1
    shift
    local log_flush_interval=$1
    shift
    local workloads=$*

    if [ $dram_cache_size_mib != 0 ]; then
       dram_cache_config_args="-dram_cache_size_mb $dram_cache_size_mib"
    else
        dram_cache_config_args=
    fi

    if [ $pmem_cache_size_mib != 0 ]; then
       pmem_cache_config_args="-pmem_cache_file $PMEM_ROOT/persistron.pmemcache \
               				         -pmem_cache_size_mb $pmem_cache_size_mib"
    else
        pmem_cache_config_args=
    fi

    if [ $log_flush_interval != 0 ]; then
        log_flush_interval_args="-cache_log_checkpoint_interval $log_flush_interval"
    else
        log_flush_interval_args=
    fi

    local wargs=`workload_args $workloads`

    cmd="cgexec -g memory:$CGROUP_NAME \
       $YCSBC_DIR/ycsbc \
       -threads $nthreads \
       -db classic_splinterdb \
       $dram_cache_config_args \
       $pmem_cache_config_args \
       $log_flush_interval_args \
       -L workloads/load.spec \
       $wargs"

    for iter in `seq 1 $ITERATIONS`; do
        rm -f splinterdb.db
        file="raw-data/${name}_threads_${nthreads}_dram_cache_size_mib_${dram_cache_size_mib}_pmem_cache_size_mib_${pmem_cache_size_mib}_log_flush_interval_${log_flush_interval}_iter_${iter}.out"
        configure_cgroup $dram_cache_size_mib
        run_if_needed $file $cmd
    done
}

function build_matrixkv_ycsb() {
    local name=$1
    shift
    (cd $MATRIXKV_DIR; make clean; DEBUG_LEVEL=0 make -j 8 release; DEBUG_LEVEL=0 make install) > raw-data/${name}.build 2>&1
    (cd $YCSBC_DIR; make clean; make) > raw-data/${name}_ycsb.build 2>&1
}

function execute_matrixkv_ycsb() {
    local name=$1
    shift
    local nthreads=$1
    shift
    local dram_cache_size_mib=$1
    shift
    local pmem_cache_size_mib=$1
    shift
    local workloads=$*

    if [ $dram_cache_size != 0 ]; then
       dram_cache_config_args="-p rocksdb.block_cache_size_mib $dram_cache_size_mib"
    else
        dram_cache_config_args=
    fi

    if [ $pmem_cache_size != 0 ]; then
       trigger_size_mib=$(( $pmem_cache_size_mib - 1024 ))
       slowdown_size_mib=$(( $pmem_cache_size_mib - 512 ))
       pmem_cache_config_args="-p matrixkv.use_nvm_module 1 \
                               -p matrixkv.pmem_path $PMEM_ROOT/matrixkv.pmemcache \
                               -p matrixkv.level0_column_compaction_trigger_size_mib $trigger_size_mib \
                               -p matrixkv.level0_column_compaction_slowdown_size_mib $slowdown_size_mib \
                               -p matrixkv.level0_column_compaction_stop_size_mib $pmem_cache_size_mib"
    else
        pmem_cache_config_args=
    fi

    local wargs=`workload_args $workloads`

    cmd="PMEM_NO_FLUSH=1 \
       $YCSBC_DIR/ycsbc \
       -threads $nthreads \
       -db rocksdb \
       -p rocksdb.config_file matrixkv.ini \
       $dram_cache_config_args \
       $pmem_cache_config_args \
       -L workloads/load.spec \
       $wargs"

    for iter in `seq 1 $ITERATIONS`; do
        rm -rf rocksdb.db
        file="raw-data/${name}_threads_${nthreads}_dram_cache_size_mib_${dram_cache_size_mib}_pmem_cache_size_mib_${pmem_cache_size_mib}_iter_${iter}.out"
        configure_cgroup $dram_cache_size_mib
        run_if_needed $file $cmd
    done
}


#### Plain SplinterDB: no DEFAULT_CFLAGS
build_splinterdb_ycsb "splinterdb" ""
#                       name                 threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "splinterdb"          2       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"          4       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"          6       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"          8       8192              0               0                   a b c d e f
execute_splinterdb_ycsb "splinterdb"         10       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"         12       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"         14       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"         16       8192              0               0                   a c

#### PMEM only?
# uses same build as plain SplinterDB
#                       name                 threads  dram_cache_size   pmem_cache_size log_flush_interval  workloads
execute_splinterdb_ycsb "pmem-only"          8        0                 16384           0                   a c

#### Persistron
build_splinterdb_ycsb "persistron" "-DPMEM_COW -DNON_TX_OPT -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT"
#                       name                 threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "persistron"         2        4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"         4        4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"         6        4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"         8        2048              24576           0                   a c
execute_splinterdb_ycsb "persistron"         8        4096              16384           0                   a b c d e f
execute_splinterdb_ycsb "persistron"         8        6144               8192           0                   a c
execute_splinterdb_ycsb "persistron"         10       4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"         12       4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"         14       4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"         16       4096              16384           0                   a c

#### PMEM-CoW
build_splinterdb_ycsb "pmem-cow" "-DPMEM_COW"
#                       name                 threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "pmem-cow"           8        0                 16384           0                   a c

#### Non-Txn
build_splinterdb_ycsb "non-txn" "-DPMEM_COW -DNON_TX_OPT"
#                       name                 threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "non-txn"            8        0                 16384           0                   a c

#### SplinterDB-withlog
build_splinterdb_ycsb "splinterdb-withlog" "-DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT -DLOG_CHECKPOINT"
#                       name                 threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "splinterdb-withlog" 8        0                 16384           10000               a c


#### MatrixKV
build_matrixkv_ycsb
#                       name                 threads  dram_cache_size   pmem_cache_size                     workloads
execute_matrixkv_ycsb   "matrixkv"           8        2048              24576                               a c
execute_matrixkv_ycsb   "matrixkv"           8        4096              16384                               a c
execute_matrixkv_ycsb   "matrixkv"           8        6144              8192                                a b c d e f
