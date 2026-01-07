// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

// A default data_config suitable for simple key/value applications
// using a lexicographical sort-order (memcmp)
//
// This data_config does not support blind mutation operations, except
// plain overwrites of values.

#pragma once

#include "data.h"

void
default_data_config_init(const uint64 max_key_size, // IN
                         data_config *out_cfg       // OUT
);
