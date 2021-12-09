// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * kvstore_basic.h --
 *
 *     Simplified key-value store interface
 *
 */

#ifndef _KVSTORE_BASIC_H_
#define _KVSTORE_BASIC_H_

#include <stddef.h> // for size_t
#include "splinterdb/limits.h"

// Length-prefix encoding of a variable-sized key
// Should be == sizeof(basic_key_encoding), which is enforced elsewhere
#define KVSTORE_BASIC_KEY_HDR_SIZE ((int)sizeof(uint8_t))

// Minimum size of a key, in bytes
#define KVSTORE_BASIC_MIN_KEY_SIZE 2

// Max size of a key, in bytes
// Must always be = ( MAX_KEY_SIZE - sizeof(basic_key_encoding) )
#define KVSTORE_BASIC_MAX_KEY_SIZE (MAX_KEY_SIZE - KVSTORE_BASIC_KEY_HDR_SIZE)

// Should be == sizeof(basic_message), which is enforced elsewhere
#define KVSTORE_BASIC_MSG_HDR_SIZE ((int)sizeof(void *))

// Maximum size of a value, in bytes
// Must always == ( MAX_MESSAGE_SIZE - sizeof(basic_message) )
#define KVSTORE_BASIC_MAX_VALUE_SIZE                                           \
   (MAX_MESSAGE_SIZE - KVSTORE_BASIC_MSG_HDR_SIZE)

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
} kvstore_basic_cfg;

// Handle to a live instance of splinterdb
typedef struct kvstore_basic kvstore_basic;

// Create a new kvstore_basic from the provided config
int
kvstore_basic_create(const kvstore_basic_cfg *cfg, // IN
                     kvstore_basic **         kvsb // OUT
);

// Open an existing kvstore_basic using the provided config
int
kvstore_basic_open(const kvstore_basic_cfg *cfg, // IN
                   kvstore_basic **         kvsb // OUT
);

// De-init a handle and associated in-memory resources
void
kvstore_basic_close(kvstore_basic *kvsb);

// Register the current thread so that it can be used with the kvstore_basic.
// This causes scratch space to be allocated for the thread.
//
// Any thread that uses a kvstore_basic must first be registered with it.
//
// The only exception is the initial thread which called create or open,
// as that thread is implicitly registered.  Re-registering it will leak memory.
//
// A thread should not be registered more than once; that would leak memory.
//
// kvstore_basic_close will use scratch space, so the thread that calls it must
// have been registered (or implicitly registered by being the initial thread).
//
// Note: There is currently a limit of MAX_THREADS registered at a given time
void
kvstore_basic_register_thread(const kvstore_basic *kvsb);

// Deregister the current thread and free its scratch space.
//
// Call this function before exiting a registered thread.
// Otherwise, you'll leak memory.
void
kvstore_basic_deregister_thread(const kvstore_basic *kvsb);

// Insert a key:value pair
int
kvstore_basic_insert(const kvstore_basic *kvsb,
                     const char *         key,
                     size_t               key_len,
                     const char *         value,
                     size_t               val_len);

// Delete a given key and associated value
int
kvstore_basic_delete(const kvstore_basic *kvsb,
                     const char *         key,
                     size_t               key_len);


// Lookup a given key
//
// Set val_max_len to the size of the val buffer
// If found, val_bytes will hold the size of the value
int
kvstore_basic_lookup(const kvstore_basic *kvsb,
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

See the doc comments in kvstore.h for details.

Sample application code:

   kvstore_basic_iterator* it;
   const char* start_key = NULL;
   int rc = kvstore_basic_iter_init(kvsb, &it, start_key);
   if (rc != 0) { ... handle error ... }

   const char* key;
   const char* val;
   size_t key_len, val_len;
   for(; kvstore_basic_iter_valid(it); kvstore_basic_iter_next(it)) {
      kvstore_basic_iter_get_current(it, &key, &key_len, &val, &val_len);

      // read key and val, but do not modify them
      printf("key=%.*s val=%.*s", (int)(key_len), key, (int)(val_len), val);
   }

   // loop exit may mean error, or just that we've reached the end of the range
   rc = kvstore_iter_status(it);
   if (rc != 0) { ... handle error ... }
*/

typedef struct kvstore_basic_iterator kvstore_basic_iterator;

int
kvstore_basic_iter_init(const kvstore_basic *    kvsb,         // IN
                        kvstore_basic_iterator **iter,         // OUT
                        const char *             start_key,    // IN
                        size_t                   start_key_len // IN
);

void
kvstore_basic_iter_deinit(kvstore_basic_iterator **iterpp);

// checks that the iterator status is OK (no errors) and that get_current will
// succeed If false, there are two possibilities:
// 1. Iterator has passed the final item.  In this case, status() == 0
// 2. Iterator has encountered an error.  In this case, status() != 0
_Bool
kvstore_basic_iter_valid(kvstore_basic_iterator *iter);

// attempts to advance the iterator to the next item
// any error will cause valid() == false and be visible with status()
void
kvstore_basic_iter_next(kvstore_basic_iterator *iter);

// Sets *key and *message to the locations of the current item
// Callers must not modify that memory
//
// If valid() == false, then behavior is undefined.
void
kvstore_basic_iter_get_current(kvstore_basic_iterator *iter,    // IN
                               const char **           key,     // OUT
                               size_t *                key_len, // OUT
                               const char **           value,   // OUT
                               size_t *                val_len  // OUT
);

// Returns an error encountered from iteration, or 0 if successful.
//
// End-of-range is not an error
int
kvstore_basic_iter_status(const kvstore_basic_iterator *iter);

// Returns a C string with the build version of this library
const char *
kvstore_basic_get_version();

#endif // _KVSTORE_BASIC_H_
