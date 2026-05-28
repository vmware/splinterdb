// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * data_blob_build.h --
 *
 * Functions for building blob-backed messages.
 */

#pragma once

#include "blob_build.h"
#include "data_internal.h"

platform_status
message_to_blob(const blob_build_config *cfg,
                cache                   *cc,
                mini_allocator          *mini,
                message                  msg,
                merge_accumulator       *ma);

platform_status
message_clone(const blob_build_config *cfg,
              cache                   *cc,
              mini_allocator          *mini,
              message                  msg,
              merge_accumulator       *result);

platform_status
merge_accumulator_convert_to_blob(const blob_build_config *cfg,
                                  cache                   *cc,
                                  mini_allocator          *mini,
                                  merge_accumulator       *ma);
