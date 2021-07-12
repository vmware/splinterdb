#!/bin/bash -x

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

dir=$1
postfix=$2
ops=$3
throughput=$(bc <<< "scale=0; $4 / 1")

write_start=$(grep nvme $dir/iostat_start_$postfix | awk '{ print $7 }')
write_end=$(grep nvme $dir/iostat_end_$postfix | awk '{ print $7 }')
read_start=$(grep nvme $dir/iostat_start_$postfix | awk '{ print $6 }')
read_end=$(grep nvme $dir/iostat_end_$postfix | awk '{ print $6 }')

write_total=$(($write_end - $write_start))
read_total=$(($read_end - $read_start))

tuple_size=$((24 + 100))
logical_total=$(bc <<< "scale=2; $ops * $tuple_size / 1024")

write_amp=$(bc <<< "scale=2; $write_total/$logical_total")
read_amp=$(bc <<< "scale=2; $read_total/$logical_total")
io_amp=$(bc <<< "scale=2; $write_amp + $read_amp")

bandwidth=$(bc <<< "scale=2; $throughput * $io_amp * $tuple_size / 1024 / 1024")

echo "throughput: $throughput ops/sec"
echo "write amp:  $write_amp"
echo "read amp:   $read_amp"
echo "io amp:     $io_amp"
echo "bandwidth:  $bandwidth mib/sec"
