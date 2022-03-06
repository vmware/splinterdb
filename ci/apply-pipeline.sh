#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

TARGET_CONCOURSE=runway

RENDERED_PIPELINE="$(mktemp -t pipeline-XXXXXX.yml)"

cd "$(dirname "$0")"

ytt -f . > "$RENDERED_PIPELINE"

echo ytt rendered pipeline template to "$RENDERED_PIPELINE"

fly -t $TARGET_CONCOURSE set-pipeline \
   --pipeline splinterdb \
   --config "$RENDERED_PIPELINE"
