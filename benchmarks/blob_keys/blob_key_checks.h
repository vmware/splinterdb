// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "blob_key.h"

/* Untimed codec checks. The caller owns and later releases the supplied arena.
 */
platform_status
blob_key_checks(blob_key_config         *cfg,
                const blob_build_config *build_cfg,
                mini_allocator          *mini,
                platform_heap_id         hid);
