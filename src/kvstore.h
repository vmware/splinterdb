// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore.h --
 *
 *     This file contains the external kvstore interfaces based on splinterdb
 */

#ifndef _KVSTORE_H_
#define _KVSTORE_H_

#include "data.h"

typedef struct kvstore_config {
   const char *filename;
   uint64      cache_size;
   uint64      disk_size;

   data_config data_cfg;

   void *heap_handle;
   void *heap_id;
} kvstore_config;

typedef struct kvstore *kvstore_handle;

int kvstore_init(const kvstore_config *cfg, kvstore_handle *h);

void kvstore_deinit(kvstore_handle h);

void kvstore_register_thread(const kvstore_handle h);

// FIXME: key/value can't be marked const until splinter API's are fixed
int kvstore_insert(const kvstore_handle h, char *key, char *message);

int kvstore_lookup(const kvstore_handle h,
                   char *               key,
                   char *               message,
                   bool *               found);

#endif // _KVSTORE_H_
