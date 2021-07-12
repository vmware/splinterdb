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

set -e

mkdir -p $name
for i in 28
do
   ./load_splinter.sh $trace_dir 20 $memory $name 0
   ./read_splinter.sh $trace_dir $i $memory $name 0
done
