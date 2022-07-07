// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _TRANSACTION_TABLE_H_
#define _TRANSACTION_TABLE_H_

#include "splinterdb/platform_linux/public_platform.h"
#include "simple_set.h"

typedef enum transaction_table_type {
   TRANSACTION_TABLE_TYPE_INVALID = 0,
   TRANSACTION_TABLE_TYPE_QUEUE
} transaction_table_type;

typedef enum transaction_state {
   TRANSACTION_STATE_INVALID = 0,
   TRANSACTION_STATE_PROVISIONAL,
   TRANSACTION_STATE_PARTIALLY_COMMITTED,
   TRANSACTION_STATE_COMMITTED,
   TRANSACTION_STATE_FAILED,
   TRANSACTION_STATE_ABORTED
} transaction_state;

typedef uint64 timestamp;

typedef struct transaction_table_tuple {
   transaction_id    txn_id;
   timestamp         end_ts;
   transaction_state state;
   simple_set        read_set;
   simple_set        write_set;

   struct transaction_table_tuple *next;
   // TODO: add the fields of a tuple if necessary
} transaction_table_tuple;

void
transaction_table_tuple_init(transaction_table_tuple *tuple,
                             transaction_id           txn_id);

// Transition of the transaction state
void
transaction_table_tuple_state_partially_committed(
   transaction_table_tuple *tuple);

void
transaction_table_tuple_state_committed(transaction_table_tuple *tuple);

void
transaction_table_tuple_state_failed(transaction_table_tuple *tuple);

void
transaction_table_tuple_state_aborted(transaction_table_tuple *tuple);

typedef void (*transaction_table_update_func)(transaction_table_tuple *);

typedef struct transaction_table {
   int (*insert)(struct transaction_table *, transaction_id);
   int (*delete)(struct transaction_table *, transaction_id);
   transaction_table_tuple *(*lookup)(struct transaction_table *,
                                      transaction_id);
   int (*update)(struct transaction_table *,
                 transaction_id,
                 transaction_table_update_func);
   void (*deinit)(struct transaction_table *);

   transaction_table_tuple *(*first)(struct transaction_table *);

   uint64 size;
} transaction_table;

transaction_table *
transaction_table_create(transaction_table_type type);

void
transaction_table_destroy(transaction_table *txn_tbl);

int
transaction_table_insert(transaction_table *txn_tbl, transaction_id txn_id);

int
transaction_table_delete(transaction_table *txn_tbl, transaction_id txn_id);

transaction_table_tuple *
transaction_table_lookup(transaction_table *txn_tbl, transaction_id txn_id);

int
transaction_table_update(transaction_table            *txn_tbl,
                         transaction_id                txn_id,
                         transaction_table_update_func do_update);

transaction_table_tuple *
transaction_table_first(transaction_table *txn_tbl);

#endif // _TRANSACTION_TABLE_H_
