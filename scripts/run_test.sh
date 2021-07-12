#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0



# Run individual(perf) tests of splinter

# Example usage:
# ./run_test.sh splinter_test --perf --tree-size-gib 1
# By default it uses config from splinter_test.cfg

set -o nounset
set -o errexit

scripts_dir=$(dirname $0)
splinter_dir=$(dirname $0)/..

. "$scripts_dir/splinter_test_common"

config_reader=$scripts_dir/config_reader.py
test_cfg=$splinter_dir/splinter_test.cfg

check_exe_exists $config_reader
check_file_exists $test_cfg

run_splinter_test $($config_reader $test_cfg $@)
