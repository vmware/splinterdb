// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __TEST_DATA_H
#define __TEST_DATA_H

#include "splinterdb/data.h"
#include "util.h"

extern data_config *test_data_config;

typedef struct PACKED data_handle {
   int8  ref_count;
   uint8 data[0];
} data_handle;

void
test_data_generate_message(const data_config *cfg,
                           message_type       type,
                           uint8              ref_count,
                           merge_accumulator *msg);

static inline void
test_data_print_key(const void *key, platform_log_handle *log_handle)
{
   const uint64 *key_p = key;
   platform_log(log_handle,
                "0x%016lx%016lx%016lx",
                be64toh(key_p[0]),
                key_p[1],
                key_p[2]);
}

#endif
