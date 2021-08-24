// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore.h --
 *
 *     This file contains the external kvstore interfaces
 *     based on splinterdb.
 *     The user must provide a data_config that encodes
 *     values and message-types.
 *
 *     For simple use cases, start with kvstore_basic
 *
 */

#ifndef _KVSTORE_H_
#define _KVSTORE_H_

#include "data.h"

typedef struct {
   const char *filename;
   uint64      cache_size;
   uint64      disk_size;

   data_config data_cfg;

   void *heap_handle;
   void *heap_id;
} kvstore_config;

typedef struct kvstore kvstore;

// Initialize a kvstore from a given config
//
// The library will allocate and own the memory for kvstore
// and will free it on kvstore_deinit
//
// cfg may be stack-allocated, since it is not stored on kvs.
int kvstore_init(const kvstore_config *cfg, kvstore **kvs);

void kvstore_deinit(kvstore *kvs);

void kvstore_register_thread(const kvstore *kvs);

int kvstore_insert(const kvstore *kvs, char *key, char *message);

int kvstore_lookup(const kvstore *kvs,     // IN
                   char *         key,     // IN
                   char *         message, // OUT
                   bool *         found    // OUT
);

/*
KVStore Iterator API

This API is modeled after the RocksDB Iterator and is intended to allow drop-in
replacement for most scenarios.

This documentation is heavily inspired by
  https://github.com/facebook/rocksdb/wiki/Iterator

The biggest difference is that RocksDB's Iterator exposes a Seek(key) method,
while SplinterDB expects the starting key to be provided at the time the
iterator is initialized.

Similar to RocksDB, if there is no error, then status()==0.  If status() != 0,
then valid() == false.  In other words, valid()==true implies status()== 0,
which means it is safe to proceed with other operations without checking
status().

On the other hand, if valid() == false, then there are two possibilities:
(1) We reached the end of the data. In this case, status() == 0;
(2) there is an error. In this case status() != 0;
It is always a good practice to check status() if the iterator is invalidated.

Sample application code:

   kvstore_iterator* it;
   int rc = kvstore_iterator_init(kvs, &it, NULL);
   if (rc != 0) { ... handle error ... }

   const char* key;
   const char* msg;
   for(; kvstore_iterator_valid(it); kvstore_iterator_next(it)) {
      rc = kvstore_iterator_get_current(it, &key, &msg);
      if (rc != 0) { ... handle error ... }
      // read key and msg ...
   }
   // loop exit may mean error, or just that we've reached the end of the range
   rc = kvstore_iterator_status(it);
   if (rc != 0) { ... handle error ... }
*/

typedef struct kvstore_iterator kvstore_iterator;

int kvstore_iterator_init(const kvstore *    kvs,      // IN
                          kvstore_iterator **iter,     // OUT
                          char *             start_key // IN
);

void kvstore_iterator_deinit(kvstore_iterator *iter);

// checks that the iterator status is OK (no errors) and that get_current will
// succeed If false, there are two possibilities:
// 1. Iterator has passed the final item.  In this case, status() == 0
// 2. Iterator has encountered an error.  In this case, status() != 0
bool kvstore_iterator_valid(kvstore_iterator *iter);

// attempts to advance the iterator to the next item
// any error will cause valid() == false and be visible with status()
void kvstore_iterator_next(kvstore_iterator *iter);

// Sets *key and *message to the locations of the current item
// Callers must not modify that memory
//
// If valid() == false, then behavior is undefined.
void kvstore_iterator_get_current(kvstore_iterator *iter,   // IN
                                  const char **     key,    // OUT
                                  const char **     message // OUT
);

// Returns an error encountered from iteration, or 0 if successful.
//
// End-of-range is not an error
int kvstore_iterator_status(const kvstore_iterator *iter);

#endif // _KVSTORE_H_
