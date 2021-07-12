#!/bin/bash

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

cat "$@"              \
  | grep -v '^//'     \
  | grep -v '^#'      \
  | sed 's/^ *//'     \
  | sed 's/ *$//'     \
  | sed 's/;$//'      \
  | grep '\S'         \
  | sed 's/^/--/'     \
  | sed 's/ *= */\n/' \
# empty line on purpose makes every filter end in backslash
