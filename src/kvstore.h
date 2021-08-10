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

typedef struct KVStoreConfig {
   const char *             filename;
   uint64                   cacheSize;
   uint64                   diskSize;
   uint64                   keySize;
   uint64                   dataSize;
   key_compare_fn           keyCompare;
   key_hash_fn              keyHash;
   message_class_fn         messageClass;
   merge_tuple_fn           mergeTuples;
   merge_tuple_final_fn     mergeTuplesFinal;
   key_or_message_to_str_fn keyToStr;
   key_or_message_to_str_fn messageToStr;
   void *                   heapHandle;
   void *                   heapID;
} KVStoreConfig;

typedef struct KVStore *KVStoreHandle;

int KVStore_Init(const KVStoreConfig *kvsCfg, KVStoreHandle *kvsHandle);

void KVStore_Deinit(KVStoreHandle kvsHandle);

void KVStore_RegisterThread(const KVStoreHandle kvsHandle);

// FIXME: key/value can't be marked const until splinter API's are fixed
int KVStore_Insert(const KVStoreHandle kvsHandle, char *key, char *message);

int KVStore_Lookup(const KVStoreHandle kvsHandle,
                   char *              key,
                   char *              message,
                   bool *              found);

#endif // _KVSTORE_H_
