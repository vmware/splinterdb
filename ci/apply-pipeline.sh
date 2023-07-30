#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Reconfigure the Concourse CI pipeline for this project.
# See https://runway.eng.vmware.com/docs/
#
# Pre-reqs:
# - Install Carvel ytt: https://carvel.dev/ytt/
# - Connect to the VMware network
# - Download the `fly` CLI by clicking the icon on the bottom-right
#   corner of this UI: https://runway-ci.eng.vmware.com
# - Set a fly target and log in:
#     fly -t runway login -c https://runway-ci.eng.vmware.com/ -n splinterdb
#
# For more info, see: https://runway.eng.vmware.com/docs/#/getting_started/hello_world

set -euo pipefail

TARGET_CONCOURSE=runway

RENDERED_PIPELINE="$(mktemp -t pipeline-XXXXXX.yml)"

cd "$(dirname "$0")"

ytt -f . > "$RENDERED_PIPELINE"

echo ytt rendered pipeline template to "$RENDERED_PIPELINE"

fly -t $TARGET_CONCOURSE set-pipeline \
   --pipeline splinterdb \
   --config "$RENDERED_PIPELINE"
