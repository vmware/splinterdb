// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _TRANSACTION_TABLE_QUEUE_H_
#define _TRANSACTION_TABLE_QUEUE_H_

#include "transaction_table.h"

transaction_table *
transaction_table_queue_init();

void
transaction_table_queue_deinit(transaction_table *txn_tbl);

int
transaction_table_queue_insert(transaction_table *txn_tbl,
                               transaction_id     txn_id);

int
transaction_table_queue_delete(transaction_table *txn_tbl,
                               transaction_id     txn_id);

transaction_table_tuple *
transaction_table_queue_lookup(transaction_table *txn_tbl,
                               transaction_id     txn_id);

int
transaction_table_queue_update(transaction_table            *txn_tbl,
                               transaction_id                txn_id,
                               transaction_table_update_func do_update);

#endif // _TRANSACTION_TABLE_QUEUE_H_
