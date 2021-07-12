#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

set -x
while $1
do
	echo "Press [CTRL+C] to stop.."
	sleep 30
done
