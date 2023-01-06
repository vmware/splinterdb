#!/bin/bash
BENCHMARKS="SplinterDB-withLog SplinterDB PMEM-Only PMEM-CoW Non-Txn PERSISTRON"
THREADS="1 2 4 8 16"
WORKLOAD="load workloada workloadb"

MOUNT_POINT=/mnt/pmem0
TIMEOUT=10m

export COMPILER=gcc-9

export CC=$COMPILER
export LD=$COMPILER

{
for B in $BENCHMARKS; do
    case $B in
        "SplinterDB")
            echo "Running SplinterDB"
            ;;
        "PMEM-Only")
            echo "Running PMEM-Only"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "PMEM-CoW")
            echo "Running PMEM-CoW"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW:g' Makefile
            ;;
        "Non-Txn")
            echo "Running Non-Txn"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW -DNON_TX_OPT:g' Makefile
            ;;
        "PERSISTRON")
            echo "Running PERSISTRON"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW -DNON_TX_OPT -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT:g' Makefile
            ;;
        "SplinterDB-withLog")
            echo "Running SplinterDB-withLog"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT -DLOG_CHECKPOINT:g' Makefile
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
                for I in `seq 0 4`; do
                    	rm -rf ${MOUNT_POINT}/*
			            rm splinterdb.db
			            if [ $W == load ]; then
			               if [ $B == SplinterDB ]; then
			                  ./ycsbc -db classic_splinterdb -L workloads/$W.spec -threads $T \
					               -dram_cache_size_mb 1024 >&data.log
			               elif [ $B == SplinterDB-withLog ]; then
			                  ./ycsbc -db classic_splinterdb -L workloads/$W.spec -threads $T \
					               -pmem_cache_size_mb 4096 -dram_cache_size_mb 1024\
					               -cache_log_checkpoint_interval 10000\
				       	         -pmem_cache_file ${MOUNT_POINT}/pmemcache >&data.log
			               else
                           ./ycsbc -db classic_splinterdb -L workloads/$W.spec -threads $T \
                              -pmem_cache_size_mb 4096 -dram_cache_size_mb 1024\
                              -pmem_cache_file ${MOUNT_POINT}/pmemcache >&data.log
		                  fi
                        RATE=`cat data.log | grep 'workloads/load.spec' | awk '{ print $4 }'`
			            else
			               if [ $B == SplinterDB ]; then
                           ./ycsbc -db classic_splinterdb -L workloads/load.spec -W workloads/$W.spec -threads $T \
                               -dram_cache_size_mb 1024 >&data.log
                        elif [ $B == SplinterDB-withLog ]; then
                           ./ycsbc -db classic_splinterdb -L workloads/load.spec -W workloads/$W.spec -threads $T \
                              -pmem_cache_size_mb 4096 -dram_cache_size_mb 1024\
                              -cache_log_checkpoint_interval 10000\
                              -pmem_cache_file ${MOUNT_POINT}/pmemcache >&data.log
                        else
			                  ./ycsbc -db classic_splinterdb -L workloads/load.spec -W workloads/$W.spec -threads $T \
				                  -pmem_cache_size_mb 4096 -dram_cache_size_mb 1024 \
				                  -pmem_cache_file ${MOUNT_POINT}/pmemcache >&data.log
			               fi
                        RATE=`cat data.log | grep 'workloads/workload' | awk '{ print $4 }'`
			            fi
                     echo "${B},${W},${T},${I},${RATE}">> result.csv
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
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "PMEM-CoW")
            echo "Finished running PMEM-CoW"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "Non-Txn")
            echo "Finished running Non-Txn"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW -DNON_TX_OPT:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "PERSISTRON")
            echo "Finished running PERSISTRON"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DPMEM_COW -DNON_TX_OPT -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
            ;;
        "SplinterDB-withLog")
            echo "Finished running SplinterDB-withLog"
            sed -i -e 's:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS) -DDRAM_CACHE -DPAGE_MIGRATION -DEVICTION_OPT -DLOG_CHECKPOINT:DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS):g' Makefile
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
