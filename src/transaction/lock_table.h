// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _LOCK_TABLE_H_
#define _LOCK_TABLE_H_

#include "splinterdb/data.h"
#include "platform.h"
#include "mvcc_data.h"

typedef struct lock_table lock_table;

lock_table *
lock_table_create();

void
lock_table_destroy(lock_table *lock_tbl);

void
lock_table_insert(lock_table          *lock_tbl,
                  slice                start,
                  slice                last,
                  transaction_op_meta *meta);

transaction_op_meta *
lock_table_lookup(lock_table *lock_tbl, slice start, slice last);

void
lock_table_delete(lock_table *lock_tbl, slice start, slice last);

#endif // _LOCK_TABLE_H_
