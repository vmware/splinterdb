// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinterdb.h --
 *
 *     This file contains the external splinterdb interfaces
 *     based on splinterdb.
 *     The user must provide a data_config that encodes
 *     values and message-types.
 *
 *     For simple use cases, start with splinterdb_kv
 *
 */

#ifndef _SPLINTERDB_H_
#define _SPLINTERDB_H_

#include "splinterdb/data.h"

typedef struct {
   const char *filename;
   uint64      cache_size;
   uint64      disk_size;

   data_config data_cfg;

   void *heap_handle;
   void *heap_id;
} splinterdb_config;

typedef struct splinterdb splinterdb;

// Create a new splinterdb from a given config
//
// The library will allocate and own the memory for splinterdb
// and will free it on splinterdb_close
//
// It is ok for the caller to stack-allocate cfg, since it is not retained
int
splinterdb_create(const splinterdb_config *cfg, splinterdb **kvs);

// Open an existing splinterdb, using the provided config
//
// The library will allocate and own the memory for splinterdb
// and will free it on splinterdb_close
//
// It is ok for the caller to stack-allocate cfg, since it is not retained
int
splinterdb_open(const splinterdb_config *cfg, splinterdb **kvs);

// Close a splinterdb
//
// This will flush everything to disk and release all resources
void
splinterdb_close(splinterdb *kvs);

// Register the current thread so that it can be used for
// operations against the splinterdb.
//
// It must be called no more than once per thread, and before
// any operation on the database, including splinterdb_close.
//
// Note: splinterdb_create causes an implicit splinterdb_register_thread
// So it is safe to call splinterdb_close from the same thread
// that called splinterdb_create without an exra splinterdb_register_thread.
void
splinterdb_register_thread(const splinterdb *kvs);

int
splinterdb_insert(const splinterdb *kvs, char *key, char *message);

int
splinterdb_lookup(const splinterdb *kvs,     // IN
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

   splinterdb_iterator* it;
   int rc = splinterdb_iterator_init(kvs, &it, NULL);
   if (rc != 0) { ... handle error ... }

   const char* key;
   const char* msg;
   for(; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      splinterdb_iterator_get_current(it, &key, &msg);
      // read key and msg ...
   }
   // loop exit may mean error, or just that we've reached the end of the range
   rc = splinterdb_iterator_status(it);
   if (rc != 0) { ... handle error ... }
*/

typedef struct splinterdb_iterator splinterdb_iterator;

int
splinterdb_iterator_init(const splinterdb *    kvs,      // IN
                      splinterdb_iterator **iter,     // OUT
                      char *             start_key // IN
);

void
splinterdb_iterator_deinit(splinterdb_iterator *iter);

// checks that the iterator status is OK (no errors) and that get_current will
// succeed If false, there are two possibilities:
// 1. Iterator has passed the final item.  In this case, status() == 0
// 2. Iterator has encountered an error.  In this case, status() != 0
bool
splinterdb_iterator_valid(splinterdb_iterator *iter);

// attempts to advance the iterator to the next item
// any error will cause valid() == false and be visible with status()
void
splinterdb_iterator_next(splinterdb_iterator *iter);

// Sets *key and *message to the locations of the current item
// Callers must not modify that memory
//
// If valid() == false, then behavior is undefined.
void
splinterdb_iterator_get_current(splinterdb_iterator *iter,   // IN
                             const char **     key,    // OUT
                             const char **     message // OUT
);

// Returns an error encountered from iteration, or 0 if successful.
//
// End-of-range is not an error
int
splinterdb_iterator_status(const splinterdb_iterator *iter);

#endif // _SPLINTERDB_H_
