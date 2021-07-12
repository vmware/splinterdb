#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0


# Run splinter tests by the options passed in
# Intentionally skips looking at any test config

# Example usage:
# ./test_runner.sh -t precheckin

set -o nounset
set -o errexit

scripts_dir=$(dirname $0)

. "$scripts_dir/splinter_test_common"

test_finder=$scripts_dir/list_test.py

check_exe_exists $test_finder

tests=$($test_finder "$@")
# return code of 0 indicates find tests to run
if [ $? != 0 ] ; then
   echo "$tests"
   exit 1
fi

# split by newline to get array of test args
SAVEIFS=$IFS
IFS=$'\n'
args=($tests)
IFS=$SAVEIFS

for (( i=0; i<${#args[@]}; i++ )); do
   run_splinter_test ${args[$i]}
done
