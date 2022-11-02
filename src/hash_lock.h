// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * hash_lock.h --
 *
 *     This file contains the hash_lock interface.
 */

#ifndef HASH_LOCK_H
#define HASH_LOCK_H

#include "platform.h"
#include "splinterdb/data.h"

typedef struct hash_lock_config {
   uint64       size;
   unsigned int seed;
   hash_fn      hash;
   uint64       sleep_in_ns;
} hash_lock_config;

extern hash_lock_config default_hash_lock_config;

typedef struct hash_lock {
   hash_lock_config *cfg;
   char             *slots;
} hash_lock;

void
hash_lock_init(hash_lock *hl, hash_lock_config *cfg);

void
hash_lock_deinit(hash_lock *hl);

void
hash_lock_acquire(hash_lock *hl, slice key);

void
hash_lock_release(hash_lock *hl, slice key);

#endif
