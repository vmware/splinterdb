// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data_blob_build.h --
 *
 * Functions for building blobs from key and message types
 */

#pragma once

#include "data_internal.h"
#include "blob_build.h"

platform_status
message_to_blob(const blob_build_config *cfg,
                cache                   *cc,
                mini_allocator          *mini,
                key                      tuple_key,
                message                  msg,
                merge_accumulator       *ma);

platform_status
message_clone(const blob_build_config *cfg,
              cache                   *cc,
              mini_allocator          *mini,
              key                      tuple_key,
              message                  msg,
              merge_accumulator       *result);

platform_status
merge_accumulator_convert_to_blob(const blob_build_config *cfg,
                                  cache                   *cc,
                                  mini_allocator          *mini,
                                  key                      tuple_key,
                                  merge_accumulator       *ma);
