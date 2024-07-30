// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/data.h"
#include "platform.h"

// This is the shared information for the lock table
typedef struct lock_table_shared_entry {
   int64 id;
} lock_table_shared_entry;

// It contains inputs for the lock table
typedef struct {
   slice                    key;
   char                     is_locked; // can be removed
   int64                    id;
   lock_table_shared_entry *shared_lock;
} lock_table_entry;

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
lock_table_create(const data_config *spl_data_config);
void
lock_table_destroy(lock_table *lock_tbl);

lock_table_rc
lock_table_try_acquire_entry_lock(lock_table       *lock_tbl,
                                  lock_table_entry *entry);
lock_table_rc
lock_table_try_acquire_entry_lock_timeouts(lock_table       *lock_tbl,
                                           lock_table_entry *entry,
                                           timestamp         timeout_ns);
lock_table_rc
lock_table_release_entry_lock(lock_table *lock_tbl, lock_table_entry *entry);
lock_table_rc
lock_table_get_entry_lock_state(lock_table *lock_tbl, lock_table_entry *entry);
