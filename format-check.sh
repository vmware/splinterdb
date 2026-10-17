#!/bin/bash

# Copyright 2018-2026 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Checks that C source files follow our formatting conventions

set -eu -o pipefail

# Run from the source root, regardless of the caller's working directory.
cd "$(dirname "$0")"

# different tool versions yield different results
# so we standardize on this version
TOOL="clang-format-19"

# Check if TOOL exists
if ! command -v "$TOOL" &> /dev/null; then
   echo "Error: missing required tool $TOOL

This tool is typically provided by the clang-format package"
   exit 1
fi

source_files() {
   # Keep generated builds and frozen benchmark source snapshots out of both
   # checking and fixall. Include new files in the maintained source directories.
   find src include tests examples tools benchmarks \
      -type d \( -path benchmarks/branch-prefix-history \
                 -o -path 'benchmarks/results-*' \) -prune -o \
      -type f \( -name '*.c' -o -name '*.h' \) -print0
}

if [ "${1:-""}" = "fixall" ]; then
   source_files | xargs -0 -r "$TOOL" -i
   exit 0
fi

if source_files | xargs -0 -r "$TOOL" --Werror --dry-run; then
   echo Format OK
   exit 0
else
   echo "Source files must be formatted.  Try running

   ${0} fixall
"

   exit 1
fi
