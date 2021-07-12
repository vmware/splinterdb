#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# - Need to run this script as sudo for cgroup stuff
# - Assume that the traces are all generated somewhere
#
# $1 is where the traces live
# $3 is M, or the memory cgroup allotment
# $4 is the directory to save in
# $5 is a unique identifier for the log files, for example the run #

trace_dir=$1
mem=$2
save_dir=$3
id=$4

mkdir -p $save_dir

for workload in seq100
do
   if [ ! -f $trace_dir/replay_$workload.lc ]; then
      total_ops=$(wc -l < $trace_dir/replay_$workload)
      echo $total_ops > $trace_dir/replay_$workload.lc
   else
      total_ops=$(cat $trace_dir/replay_$workload.lc)
   fi
   ops=$total_ops

   # set the trace_prefix
   trace_prefix="$trace_dir/replay_${workload}_"

   echo "About to start workload $workload into Splinter"
   iostat > $save_dir/iostat_start_${file_postfix}

   # Run the benchmark
   cgexec -g memory:benchmark ./ycsb_test $save_dir/${file_postfix} $trace_prefix $threads $(./config.sh splinter_test.cfg)
   #./ycsb_test $save_dir/${file_postfix} $trace_prefix $threads $(./config.sh splinter_test.cfg)
   #gdb --args ./ycsb_test $save_dir/${file_postfix} $trace_prefix $threads $(./config.sh splinter_test.cfg)

   iostat > $save_dir/iostat_end_${file_postfix}

   throughput=$(grep throughput $save_dir/$file_postfix.statistics | awk '{ print $2 }')
   echo "Results for workload $workload" > $save_dir/$file_postfix.quick
   ./compute_amp.sh $save_dir $file_postfix $total_ops $throughput >> $save_dir/$file_postfix.quick
done
