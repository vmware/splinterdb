#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Asserts that git-clang-format returns an empty diff
#
# See git-clang-format -h for details
#
# To compare based on a particular commit, provide an argument
#   ./format-check.sh [<commit>]
# This is useful to check the combined effect of several commits.

# Safer bash
set -eu -o pipefail

# Redirect output to stderr
exec 1>&2

# Check if git-clang-format exists
if ! command -v git-clang-format &> /dev/null; then
   cat << \EOF
Error: missing required tool git-clang-format

This tool is typically provided by the clang-format package
EOF
   exit 1
fi

# bash variable set/unset stuff: https://serverfault.com/a/382740
if [ -z "${1+set}" ]; then
   # If there's not an argument
   diff="$(git-clang-format --diff --quiet)"
else
   BASE_REF="$1"
   diff="$(git-clang-format "$BASE_REF" --diff --quiet)"
fi

if [ -z "${diff-unset}" ]; then
   echo Format OK
   exit 0
else
   echo "Error: Code formatting
To fix, run

   git-clang-format ${BASE_REF:-}

"
   echo "${diff}"
   exit 1
fi
