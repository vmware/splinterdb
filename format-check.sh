#!/bin/bash

# Copyright 2018-2022 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Checks that C source files follow our formatting conventions

set -eu -o pipefail

# different tool versions yield different results
# so we standardize on this version
TOOL="clang-format-13"

# Check if TOOL exists
if ! command -v "$TOOL" &> /dev/null; then
   echo "Error: missing required tool $TOOL

This tool is typically provided by the clang-format package"
   exit 1
fi

if [ "${1:-""}" = "fixall" ]; then
   # shellcheck disable=SC2046
   "$TOOL" -i $(find . -name "*.[ch]")
   exit 0
fi

# shellcheck disable=SC2046
if "$TOOL"  --Werror --dry-run $(find . -name "*.[ch]"); then
   echo Format OK
   exit 0
else
   echo "Source files must be formatted.  Try running

   ${0} fixall
"

   exit 1
fi
