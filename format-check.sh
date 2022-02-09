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

TOOL="clang-format-10"

# Check if TOOL exists
if ! command -v "$TOOL" &> /dev/null; then
   echo "Error: missing required tool $TOOL

This tool is typically provided by the clang-format package
"
   exit 1
fi

diff="$("$TOOL"  --Werror --dry-run $(find . -name "*.[ch]"))"

if [ -z "${diff-unset}" ]; then
   echo Format OK
   exit 0
elif [[ ${diff} == *"no modified files to format"* ]]; then
   echo No modified files to format
   exit 0
else
   echo "Error: Code formatting
To fix, run

   $TOOL ${BASE_REF:-}

"
   echo "${diff}"
   exit 1
fi
