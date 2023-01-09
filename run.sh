#!/bin/bash

set -ex

# EVAL=$1

# if [ $EVAL == 'iso' ]; then
# 	DRAM_CAPACITY=(6144 4096 2048 8192)
# 	PMEM_CAPACITY=(8192 16384 24576 0)
# else
# 	DRAM_CAPACITY="4096"
# 	PMEM_CAPACITY="16384"
# fi

# if [ $EVAL == 'workload' ]; then
# 	WORKLOAD="load workloada workloadb workloadc workloadd workloade workloadf"
# else
# 	WORKLOAD="load workloada workloadb"
# fi

# if [ $EVAL == 'threading' ]; then
#         THREADS="1 2 4 8 16"
# else
#         THREADS="16"
# fi

# if [ $EVAL == 'sys-compare' ]; then
#         BENCHMARKS="SplinterDB-withLog SplinterDB PMEM-Only PMEM-CoW Non-Txn PERSISTRON"
# elif [ $EVAL ==  'threading' ]; then
# 	BENCHMARKS="SplinterDB PERSISTRON"
# else
# 	#FIXME: build and run matrixkv & spitfire
#         BENCHMARKS="SplinterDB PERSISTRON MatrixKV Spitfire"
# fi

if [ "$FORCE" == "" ]; then
    FORCE=0
fi

ITERATIONS=5

PMEM_ROOT=/tmp/atc23-persistron

SPLINTER_DIR=.
YCSBC_DIR=../YCSB-C

function workload_args () {
    local result=
    for w in $*; do
        result="$result -W workloads/$w.spec"
    done
    echo "$result"
}

function run_if_needed () {
    file=$1
    shift
    cmd=$*

    if [[ ! -e "$file" || "$FORCE" == 1 ]]; then
        $cmd > "$file" 2>&1
    fi
}

export COMPILER=gcc
export CC=$COMPILER
export LD=$COMPILER

function build_splinterdb_ycsb() {
    name=$1
    shift
    cflags=$*
    (cd $SPLINTER_DIR; make clean; DEFAULT_CFLAGS="$cflags" make -j8; make install) > raw-data/${name}.build 2>&1
    (cd $YCSBC_DIR; make clean; make) > raw-data/${name}_ycsb.build 2>&1
}

function execute_splinterdb_ycsb() {
    local name=$1
    shift
    local nthreads=$1
    shift
    local dram_cache_size=$1
    shift
    local pmem_cache_size=$1
    shift
    local log_flush_interval=$1
    shift
    local workloads=$*

    if [ $dram_cache_size != 0 ]; then
       dram_cache_config_args="-dram_cache_size_mb $dram_cache_size"
    else
        dram_cache_config_args=
    fi

    if [ $pmem_cache_size != 0 ]; then
       pmem_cache_config_args="-pmem_cache_file $PMEM_ROOT/persistron.pmemcache \
               				         -pmem_cache_size_mb $pmem_cache_size"
    else
        pmem_cache_config_args=
    fi

    if [ $log_flush_interval != 0 ]; then
        log_flush_interval_args="-cache_log_checkpoint_interval $log_flush_interval"
    else
        log_flush_interval_args=
    fi

    local wargs=`workload_args $workloads`

    cmd="$YCSBC_DIR/ycsbc \
       -threads $nthreads \
       -db classic_splinterdb \
       $dram_cache_config_args \
       $pmem_cache_config_args \
       $log_flush_interval_args \
       -L workloads/load.spec \
       $wargs"

    for iter in `seq 1 $ITERATIONS`; do
        rm -f splinterdb.db
        file="raw-data/${name}_threads_${nthreads}_dram_cache_size_${dram_cache_size}_pmem_cache_size_${pmem_cache_size}_log_flush_interval_${log_flush_interval}_iter_${iter}.out"
        run_if_needed $file $cmd
    done
}



#### Plain SplinterDB: no DEFAULT_CFLAGS
build_splinterdb_ycsb "splinterdb" ""
#                       name            threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "splinterdb"     2       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"     4       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"     6       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"     8       8192              0               0                   a b c d e f
execute_splinterdb_ycsb "splinterdb"    10       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"    12       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"    14       8192              0               0                   a c
execute_splinterdb_ycsb "splinterdb"    16       8192              0               0                   a c

#### PMEM only?
# uses same build as plain SplinterDB
#                       name            threads  dram_cache_size   pmem_cache_size workloads
execute_splinterdb_ycsb "pmem-only"      8        0                 16384          0                   a c

#### Persistron
build_splinterdb_ycsb "persistron" "-DPMEM_COW -DNON_TX_OPT -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT"
#                       name            threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "persistron"    2        4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"    4        4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"    6        4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"    8        2048              24576           0                   a c
execute_splinterdb_ycsb "persistron"    8        4096              16384           0                   a b c d e f
execute_splinterdb_ycsb "persistron"    8        6144               8192           0                   a c
execute_splinterdb_ycsb "persistron"    10       4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"    12       4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"    14       4096              16384           0                   a c
execute_splinterdb_ycsb "persistron"    16       4096              16384           0                   a c

#### PMEM-CoW
build_splinterdb_ycsb "pmem-cow" "-DPMEM_COW"
#                       name            threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "pmem-cow"      8        0                 16384           0                   a c

#### Non-Txn
build_splinterdb_ycsb "non-txn" "-DPMEM_COW -DNON_TX_OPT"
#                       name            threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "non-txn"       8        0                 16384           0                   a c

#### SplinterDB-withlog
build_splinterdb_ycsb "splinterdb-withlog" "-DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT -DLOG_CHECKPOINT"
#                       name                       threads  dram_cache_size   pmem_cache_size log_flush_internval workloads
execute_splinterdb_ycsb "splinterdb-withlog"       8        0                 16384           10000               a c


exit

{
for B in $BENCHMARKS; do
    case $B in
        "SplinterDB")
            echo "Running SplinterDB"
            ;;
        "PMEM-Only")
            echo "Running PMEM-Only"
            ;;
        "PMEM-CoW")
            echo "Running PMEM-CoW"
            DEFAULT_CFLAGS=-DPMEM_COW
            ;;
        "Non-Txn")
            echo "Running Non-Txn"
            DEFAULT_CFLAGS=-DPMEM_COW -DNON_TX_OPT
            ;;
        "PERSISTRON")
            echo "Running PERSISTRON"
            DEFAULT_CFLAGS=-DPMEM_COW -DNON_TX_OPT -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT
            ;;
        "SplinterDB-withLog")
            echo "Running SplinterDB-withLog"
            DEFAULT_CFLAGS=-DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT -DLOG_CHECKPOINT
            ;;
        *)
            echo "Not a valid argument"
            echo
            ;;
    esac
    make clean
    make
    sudo make install

    cd ../YCSB-C/
    make clean
    make -j18


    for W in $WORKLOAD; do
        for T in $THREADS; do
	          for C in `seq 1 ${#DRAM_CAPACITY[@]}`; do
    		        for I in `seq 0 4`; do
                	  rm -rf ${MOUNT_POINT}/*
			              rm splinterdb.db
			              if [ $W == load ]; then
				                if [ $B == SplinterDB ]; then
					                  if [[ ${#DRAM_CAPACITY[@]} == 1 || ${PMEM_CAPACITY[$C-1]} == 0 ]]; then
			        		              ./ycsbc -db classic_splinterdb -L workloads/$W.spec -threads $T \
							                          -dram_cache_size_mb  ${DRAM_CAPACITY[$C-1]} >&data.log
						                    RATE=`cat data.log | grep 'workloads/load.spec' | awk '{ print $4 }'`
						                    echo "${B},${W},${T},${PMEM_CAPACITY[$C-1]},${DRAM_CAPACITY[$C-1]},${I},${RATE}">> result.csv
					                  fi
				                elif [ $B == SplinterDB-withLog ]; then
					                  if [ ${PMEM_CAPACITY[$C-1]} != 0 ]; then
			        		              ./ycsbc -db classic_splinterdb -L workloads/$W.spec -threads $T \
							                          -pmem_cache_size_mb ${PMEM_CAPACITY[$C-1]} \
                         				        -dram_cache_size_mb  ${DRAM_CAPACITY[$C-1]}\
							                          -cache_log_checkpoint_interval 10000\
							                          -pmem_cache_file ${MOUNT_POINT}/pmemcache \
                         				        >&data.log
						                    RATE=`cat data.log | grep 'workloads/load.spec' | awk '{ print $4 }'`
						                    echo "${B},${W},${T},${PMEM_CAPACITY[$C-1]},${DRAM_CAPACITY[$C-1]},${I},${RATE}">> result.csv
					                  fi
				                else
					                  if [ ${PMEM_CAPACITY[$C-1]} != 0 ]; then
						                    ./ycsbc -db classic_splinterdb -L workloads/$W.spec -threads $T \
							                          -pmem_cache_size_mb ${PMEM_CAPACITY[$C-1]} \
							                          -dram_cache_size_mb  ${DRAM_CAPACITY[$C-1]}\
							                          -pmem_cache_file ${MOUNT_POINT}/pmemcache >&data.log
						                    RATE=`cat data.log | grep 'workloads/load.spec' | awk '{ print $4 }'`
						                    echo "${B},${W},${T},${PMEM_CAPACITY[$C-1]},${DRAM_CAPACITY[$C-1]},${I},${RATE}">> result.csv
					                  fi
				                fi
			              else
				                if [ $B == SplinterDB ]; then
					                  if [[ ${#DRAM_CAPACITY[@]} == 1 || ${PMEM_CAPACITY[$C-1]} == 0 ]]; then
						                    ./ycsbc -db classic_splinterdb -L workloads/load.spec -W workloads/$W.spec -threads $T \
							                          -dram_cache_size_mb ${DRAM_CAPACITY[$C-1]} >&data.log
						                    RATE=`cat data.log | grep 'workloads/workload' | awk '{ print $4 }'`
						                    echo "${B},${W},${T},${PMEM_CAPACITY[$C-1]},${DRAM_CAPACITY[$C-1]},${I},${RATE}">> result.csv
					                  fi
				                elif [ $B == SplinterDB-withLog ]; then
					                  if [ ${PMEM_CAPACITY[$C-1]} != 0 ]; then
						                    ./ycsbc -db classic_splinterdb -L workloads/load.spec -W workloads/$W.spec -threads $T \
							                          -pmem_cache_size_mb ${PMEM_CAPACITY[$C-1]}\
					       		                    -dram_cache_size_mb ${DRAM_CAPACITY[$C-1]}\
							                          -cache_log_checkpoint_interval 10000\
							                          -pmem_cache_file ${MOUNT_POINT}/pmemcache >&data.log
						                    RATE=`cat data.log | grep 'workloads/workload' | awk '{ print $4 }'`
						                    echo "${B},${W},${T},${PMEM_CAPACITY[$C-1]},${DRAM_CAPACITY[$C-1]},${I},${RATE}">> result.csv
					                  fi
				                else
					                  if [ ${PMEM_CAPACITY[$C-1]} != 0 ]; then
						                    ./ycsbc -db classic_splinterdb -L workloads/load.spec -W workloads/$W.spec -threads $T \
							                          -pmem_cache_size_mb ${PMEM_CAPACITY[$C-1]} \
						       	                    -dram_cache_size_mb ${DRAM_CAPACITY[$C-1]} \
							                          -pmem_cache_file ${MOUNT_POINT}/pmemcache >&data.log
						                    RATE=`cat data.log | grep 'workloads/workload' | awk '{ print $4 }'`
						                    echo "${B},${W},${T},${PMEM_CAPACITY[$C-1]},${DRAM_CAPACITY[$C-1]},${I},${RATE}">> result.csv
					                  fi
				                fi
			              fi
                done
	          done
        done
    done

    cd ../splinterdb

    case $B in
        "SplinterDB")
            echo "Finished running SplinterDB"
            ;;
        "PMEM-Only")
            echo "Finished running PMEM-Only"
            #sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "PMEM-CoW")
            echo "Finished running PMEM-CoW"
            #sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "Non-Txn")
            echo "Finished running Non-Txn"
            #sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW -DNON_TX_OPT:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "PERSISTRON")
            echo "Finished running PERSISTRON"
            #sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW -DNON_TX_OPT -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "SplinterDB-withLog")
            echo "Finished running SplinterDB-withLog"
            #sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT -DLOG_CHECKPOINT:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        *)
            echo "Not a valid argument"
            echo
            ;;
    esac
done
} 
>&output.log
rm output.log
