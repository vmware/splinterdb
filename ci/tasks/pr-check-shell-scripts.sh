#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Lints and formats any shell scripts changed by a PR
#
# Relies on shellcheck and shfmt

# Safer bash
set -eu -o pipefail

# Redirect output to stderr
exec 1>&2

check_exists() {
   cmd="${1}"
   if ! command -v "$cmd" &> /dev/null; then
      echo "Error: missing required tool $cmd"
      exit 1
   fi
   printf "%s: " "${cmd}"
   "${cmd}" --version
}

check_exists shfmt
check_exists shellcheck

# sorted list of files changed by PR: https://github.com/telia-oss/github-pr-resource#get
PR_CHANGED_FILES=pr-changed-files
sort github-pull-request/.git/resource/changed_files > $PR_CHANGED_FILES

# sorted list files that are scripts
SCRIPT_FILES=script-files
pushd github-pull-request > /dev/null
   shfmt -f . | sort > ../${SCRIPT_FILES}
popd > /dev/null

# intersect the two lists
CHECK_FILES=check-files
comm -1 -2 $PR_CHANGED_FILES $SCRIPT_FILES > $CHECK_FILES

if ! [ -s "$CHECK_FILES" ]; then
   echo No shell scripts were changed by this PR.
   exit 0
fi

cd github-pull-request

if ! [ -s ".editorconfig" ]; then
   echo Missing expected .editorconfig file in source repo root
   exit 1
fi

while read -r f; do
   printf "checking %s ...\n" "$f"

   printf "   shellcheck..."
   shellcheck "$f"
   printf "OK\n"

   printf "   shfmt..."
   shfmt -d "$f"
   printf "OK\n"
done < "../${CHECK_FILES}"

echo all checks succeeded
