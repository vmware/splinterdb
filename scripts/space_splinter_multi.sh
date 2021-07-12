#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0


if [ "$#" -ne 3 ]; then
   echo "3 arguments required, $# provided"
   exit 1
fi

name=$1
trace_dir=$2
memory=$3

mkdir -p $name
#for i in 8 10 12 14 16 18 20 22 24 26 28 30 32
#for i in 16
for space in none
do
   ./load_splinter.sh $trace_dir 16 $((20 * 1024)) /home/aconway/junk/load 0
   ./space_splinter.sh $trace_dir 16 $memory $space $name 0
done
