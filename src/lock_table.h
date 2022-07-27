// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _LOCK_TABLE_H_
#define _LOCK_TABLE_H_

#include "splinterdb/data.h"

typedef struct lock_table lock_table;

typedef void *range_lock;

lock_table *
lock_table_create();

void
lock_table_destroy(lock_table *lock_tbl);

range_lock
lock_table_acquire_range_lock(lock_table *lock_tbl, slice start, slice last);

void
lock_table_release_range_lock(lock_table *lock_tbl, range_lock rng_lock);

int
lock_table_is_range_locked(lock_table *lock_tbl, slice start, slice last);

#endif // _LOCK_TABLE_H_
