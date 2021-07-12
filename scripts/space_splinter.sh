#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# - Need to run this script as sudo for cgroup stuff
# - Assume that the traces are all generated somewhere
#
# $1 is where the traces live
# $2 is # threads
# $3 is M, or the memory cgroup allotment
# $4 is the directory to save in
# $5 is a unique identifier for the log files, for example the run #

trace_dir=$1
threads=$2
mem=$3
space=$4
save_dir=$5
id=$6

mkdir -p $save_dir

for workload in zipf_updates
do
   # set the trace_filename
   trace_filename="$trace_dir/replay_${workload}"
   thread_pretty=$(printf "%02d" $threads)
   file_postfix="${workload}_${thread_pretty}_${mem}_${space}_$id"

   # get the total ops
   if [ ! -f $trace_filename.lc ]; then
      total_ops=$(wc -l < $trace_filename)
      echo $total_ops > $trace_filename.lc
   else
      total_ops=$(cat $trace_filename.lc)
   fi

   echo "About to start workload $workload into Splinter"
   iostat > $save_dir/iostat_start_${file_postfix}

   # Run the benchmark
   cgexec -g memory:benchmark ./run_test.sh ycsb_test $save_dir/${file_postfix} \
      $trace_filename $threads \ $total_ops $mem -e \
      | tee $save_dir/${file_postfix}.output
   #./run_test.sh ycsb_test $save_dir/${file_postfix} $trace_filename $threads \
   #   $total_ops $mem -e | tee $save_dir/${file_postfix}.output

   iostat > $save_dir/iostat_end_${file_postfix}

   throughput=$(grep throughput $save_dir/$file_postfix.statistics \
      | awk '{ print $2 }')
   echo "Results for workload $workload" > $save_dir/$file_postfix.quick
   ./compute_amp.sh $save_dir $file_postfix $total_ops $throughput
   ./compute_amp.sh $save_dir $file_postfix $total_ops $throughput \
      >> $save_dir/$file_postfix.quick
done
