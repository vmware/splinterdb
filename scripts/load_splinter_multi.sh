#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0


if [ "$#" -ne 2 ]; then
   echo "2 arguments required, $# provided"
   exit 1
fi

name=$1
trace_dir=$2
for i in 16 18 20
do
   ./load_splinter.sh $trace_dir $i $((4 * 1024)) $name 0
done
