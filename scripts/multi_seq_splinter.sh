#!/bin/bash -x

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
threads=18
mem=$2
save_dir=$3
id=$4

mkdir -p $save_dir

ops=38485370
total_ops=$(($ops * $threads))

# Run the experiment
# First concate the trace files together as a flag to pass
files=""
for i in $(seq -f "%02g" 0 $(($threads - 1)))
do
	files+="$trace_dir/replay_multiseq_$i "
done
#echo $files

file_postfix="multi_seq_${threads}_${mem}_$id"
#echo $file_postfix

echo "About to start multi_seq into Splinter"
iostat > $save_dir/iostat_start_${file_postfix}

# Run the multi_seq benchmark
cgexec -g memory:benchmark ./ycsb_test -l -p $save_dir/${file_postfix} $files
#./ycsb_test -l -p $save_dir/${file_postfix} $files
#gdb --args ./ycsb_test -l -p $save_dir/${file_posfix} $files

iostat > $save_dir/iostat_end_${file_postfix}

throughput=$(grep throughput $save_dir/$file_postfix.statistics | awk '{ print $2 }')
echo "Results for workload multi_seq" > $save_dir/$file_postfix.quick
./compute_amp.sh $save_dir $file_postfix $total_ops $throughput > $save_dir/$file_postfix.quick
./compute_amp.sh $save_dir $file_postfix $total_ops $throughput
