// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __TEST_DATA_H
#define __TEST_DATA_H

#include "data.h"
#include "util.h"

extern data_config *test_data_config;

typedef struct PACKED data_handle {
   uint8 message_type;
   int8  ref_count;
   uint8 data[0];
} data_handle;

static inline void
test_data_set_insert_flag(void *raw_data)
{
   data_handle *data = (data_handle *)raw_data;
   data->message_type = MESSAGE_TYPE_INSERT;
}

static inline void
test_data_set_delete_flag(void *raw_data)
{
   data_handle *data = (data_handle *)raw_data;
   data->message_type = MESSAGE_TYPE_DELETE;
}

static inline void
test_data_set_insert(void  *raw_data,
                int8   ref_count,
                char  *val,
                uint64 data_size)
{
   memset(raw_data, 0, data_size);
   data_handle *data = (data_handle *)raw_data;
   test_data_set_insert_flag(data);
   data->ref_count = ref_count;
   memmove(data->data, val, data_size - 2);
}

static inline void
test_data_print_key(const void *key)
{
   const uint64 *key_p = key;
   platform_log("0x%016lx%016lx%016lx", be64toh(key_p[0]), key_p[1], key_p[2]);
}

#endif
