// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinterdb_kv.h --
 *
 *     Simplified key-value store interface
 *
 */

#ifndef _SPLINTERDB_KV_H_
#define _SPLINTERDB_KV_H_

#include <stddef.h> // for size_t
#include "splinterdb/limits.h"

// Length-prefix encoding of a variable-sized key
// Should be == sizeof(basic_key_encoding), which is enforced elsewhere
#define SPLINTERDB_KV_KEY_HDR_SIZE ((int)sizeof(uint8_t))

// Minimum size of a key, in bytes
#define SPLINTERDB_KV_MIN_KEY_SIZE 2

// Max size of a key, in bytes
// Must always be = ( MAX_KEY_SIZE - sizeof(basic_key_encoding) )
#define SPLINTERDB_KV_MAX_KEY_SIZE (MAX_KEY_SIZE - SPLINTERDB_KV_KEY_HDR_SIZE)

// Should be == sizeof(basic_message), which is enforced elsewhere
#define SPLINTERDB_KV_MSG_HDR_SIZE ((int)sizeof(void *))

// Maximum size of a value, in bytes
// Must always == ( MAX_MESSAGE_SIZE - sizeof(basic_message) )
#define SPLINTERDB_KV_MAX_VALUE_SIZE                                           \
   (MAX_MESSAGE_SIZE - SPLINTERDB_KV_MSG_HDR_SIZE)

typedef int (*key_comparator_fn)(const void *context,
                                 const void *key1,
                                 size_t      key1_len,
                                 const void *key2,
                                 size_t      key2_len);


// Configuration that the application must provide
typedef struct {

   // path to file on disk to store data in
   const char *filename;

   // size of in-memory cache, in bytes
   size_t cache_size;

   // size of file to use on disk, in bytes
   size_t disk_size;

   // maximum length of keys, in bytes.
   // must be <= MAX_KEY_SIZE
   size_t max_key_size;

   // maximum length of values, in bytes.
   // must be <= MAX_MESSAGE_SIZE - sizeof(basic_message)
   size_t max_value_size;

   // optional custom comparator for keys
   // if NULL, the default comparator (memcmp-based) will be used
   key_comparator_fn key_comparator;

   // optional context to pass to key_comparator
   void *key_comparator_context;

   // Reserved, set them to NULL
   void *heap_handle;
   void *heap_id;
} splinterdb_kv_cfg;

// Handle to a live instance of splinterdb
typedef struct splinterdb_kv splinterdb_kv;

// Create a new splinterdb_kv from the provided config
int
splinterdb_kv_create(const splinterdb_kv_cfg *cfg, // IN
                     splinterdb_kv **         kvsb // OUT
);

// Open an existing splinterdb_kv using the provided config
int
splinterdb_kv_open(const splinterdb_kv_cfg *cfg, // IN
                   splinterdb_kv **         kvsb // OUT
);

// De-init a handle and associated in-memory resources
void
splinterdb_kv_close(splinterdb_kv *kvsb);

// Call this once for each thread that needs to use the db
void
splinterdb_kv_register_thread(const splinterdb_kv *kvsb);

// Insert a key:value pair
int
splinterdb_kv_insert(const splinterdb_kv *kvsb,
                     const char *         key,
                     size_t               key_len,
                     const char *         value,
                     size_t               val_len);

// Delete a given key and associated value
int
splinterdb_kv_delete(const splinterdb_kv *kvsb,
                     const char *         key,
                     size_t               key_len);


// Lookup a given key
//
// Set val_max_len to the size of the val buffer
// If found, val_bytes will hold the size of the value
int
splinterdb_kv_lookup(const splinterdb_kv *kvsb,
                     const char *         key,           // IN
                     size_t               key_len,       // IN
                     char *               val,           // OUT
                     size_t               val_max_len,   // IN
                     size_t *             val_bytes,     // OUT
                     _Bool *              val_truncated, // OUT
                     _Bool *              found          // OUT
);

/*
KVStore Basic Iterator API

See the doc comments in splinterdb.h for details.

Sample application code:

   splinterdb_kv_iterator* it;
   const char* start_key = NULL;
   int rc = splinterdb_kv_iter_init(kvsb, &it, start_key);
   if (rc != 0) { ... handle error ... }

   const char* key;
   const char* val;
   size_t key_len, val_len;
   for(; splinterdb_kv_iter_valid(it); splinterdb_kv_iter_next(it)) {
      splinterdb_kv_iter_get_current(it, &key, &key_len, &val, &val_len);

      // read key and val, but do not modify them
      printf("key=%.*s val=%.*s", (int)(key_len), key, (int)(val_len), val);
   }

   // loop exit may mean error, or just that we've reached the end of the range
   rc = splinterdb_iter_status(it);
   if (rc != 0) { ... handle error ... }
*/

typedef struct splinterdb_kv_iterator splinterdb_kv_iterator;

int
splinterdb_kv_iter_init(const splinterdb_kv *    kvsb,         // IN
                        splinterdb_kv_iterator **iter,         // OUT
                        const char *             start_key,    // IN
                        size_t                   start_key_len // IN
);

void
splinterdb_kv_iter_deinit(splinterdb_kv_iterator **iterpp);

// checks that the iterator status is OK (no errors) and that get_current will
// succeed If false, there are two possibilities:
// 1. Iterator has passed the final item.  In this case, status() == 0
// 2. Iterator has encountered an error.  In this case, status() != 0
_Bool
splinterdb_kv_iter_valid(splinterdb_kv_iterator *iter);

// attempts to advance the iterator to the next item
// any error will cause valid() == false and be visible with status()
void
splinterdb_kv_iter_next(splinterdb_kv_iterator *iter);

// Sets *key and *message to the locations of the current item
// Callers must not modify that memory
//
// If valid() == false, then behavior is undefined.
void
splinterdb_kv_iter_get_current(splinterdb_kv_iterator *iter,    // IN
                               const char **           key,     // OUT
                               size_t *                key_len, // OUT
                               const char **           value,   // OUT
                               size_t *                val_len  // OUT
);

// Returns an error encountered from iteration, or 0 if successful.
//
// End-of-range is not an error
int
splinterdb_kv_iter_status(const splinterdb_kv_iterator *iter);

#endif // _SPLINTERDB_KV_H_
