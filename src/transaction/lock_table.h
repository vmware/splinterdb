// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _LOCK_TABLE_H_
#define _LOCK_TABLE_H_

#include "splinterdb/data.h"
#include "platform.h"

#include "interval_tree/interval_tree_generic.h"

typedef struct lock_table_entry {
   transaction_id txn_id;
   message_type   op;
} lock_table_entry;

typedef struct lock_table lock_table;

lock_table *
lock_table_create();

void
lock_table_destroy(lock_table *lock_tbl);

void
lock_table_insert(lock_table    *lock_tbl,
                  uint64         start,
                  uint64         last,
                  transaction_id txn_id,
                  message_type   op);

lock_table_entry *
lock_table_lookup(lock_table *lock_tbl, uint64 start, uint64 last);

void
lock_table_delete(lock_table *lock_tbl, uint64 start, uint64 last);

#endif // _LOCK_TABLE_H_
