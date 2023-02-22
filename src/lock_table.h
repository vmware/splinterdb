// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/data.h"
#include "util.h"
#include "experimental_mode.h"

// read_set and write_set entry stored locally
typedef struct rw_entry {
   slice     key;
   message   msg; // value + op
   timestamp wts;
   timestamp rts;

   uint64 owner;

   bool is_read;
   bool need_to_keep_key;
   bool need_to_decrease_refcount;
} rw_entry;

typedef enum lock_table_rc {
   LOCK_TABLE_RC_INVALID = 0,
   LOCK_TABLE_RC_OK,
   LOCK_TABLE_RC_BUSY,
   LOCK_TABLE_RC_DEADLK,
   LOCK_TABLE_RC_NODATA
} lock_table_rc;

/*
 * Lock Table Functions
 */

typedef struct lock_table lock_table;

lock_table *
lock_table_create();
void
lock_table_destroy(lock_table *lock_tbl);

lock_table_rc
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, rw_entry *entry);
void
lock_table_release_entry_lock(lock_table *lock_tbl, rw_entry *entry);
lock_table_rc
lock_table_is_entry_locked(lock_table *lock_tbl, rw_entry *entry);
