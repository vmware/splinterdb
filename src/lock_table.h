// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/data.h"
#include "interval_tree/interval_tree_generic.h"
#include "util.h"
#include "experimental_mode.h"

typedef struct interval_tree_key {
   slice              data;
   const data_config *app_data_cfg;
} interval_tree_key;

static inline interval_tree_key
interval_tree_key_create(slice data, const data_config *app_data_cfg)
{
   return (interval_tree_key){.data = data, .app_data_cfg = app_data_cfg};
}

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

   struct rb_node    rb;
   interval_tree_key start;
   interval_tree_key last;
   interval_tree_key __subtree_last;
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