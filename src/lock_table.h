// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _LOCK_TABLE_H_
#define _LOCK_TABLE_H_

#include "splinterdb/data.h"
#include "interval_tree/interval_tree_generic.h"
#include "util.h"

/*
 * Lock Table Entry call tictoc_rw_entry
 */

typedef struct interval_tree_key {
   slice              data;
   const data_config *app_data_cfg;
} interval_tree_key;

inline interval_tree_key
interval_tree_key_create(slice data, const data_config *app_data_cfg)
{
   return (interval_tree_key){.data = data, .app_data_cfg = app_data_cfg};
}

// read_set and write_set entry stored locally
typedef struct tictoc_rw_entry {
   message_type    op;
   writable_buffer key;
   writable_buffer key_last; // The upper bound of a range, which can be empty
                             // in the case of a point key
   writable_buffer tuple;

   struct rb_node    rb;
   interval_tree_key start;
   interval_tree_key last;
   interval_tree_key __subtree_last;
} tictoc_rw_entry;

/*
 * Lock Table Functions
 */

typedef struct lock_table lock_table;

lock_table *
lock_table_create();

void
lock_table_destroy(lock_table *lock_tbl);

void
lock_table_acquire_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry);

bool
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry);

void
lock_table_release_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry);

bool
lock_table_is_entry_locked(lock_table *lock_tbl, tictoc_rw_entry *entry);

#endif // _LOCK_TABLE_H_
