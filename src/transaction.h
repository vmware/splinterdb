// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * transaction.h --
 */

#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#include "splinterdb/splinterdb.h"

typedef struct transaction_handle transaction_handle;

transaction_handle *
splinterdb_transaction_init(const splinterdb *kvsb, data_config *cfg);

void
splinterdb_transaction_deinit(transaction_handle *txn_hdl);

typedef struct transaction transaction;

transaction *
splinterdb_transaction_begin(transaction_handle *txn_hdl);

int
splinterdb_transaction_commit(transaction_handle *txn_hdl, transaction *txn);

int
splinterdb_transaction_abort(transaction_handle *txn_hdl, transaction *txn);

int
splinterdb_transaction_insert(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key,
                              slice               value);

int
splinterdb_transaction_delete(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key);

int
splinterdb_transaction_update(transaction_handle *txn_hdl,
                              transaction        *txn,
                              slice               key,
                              slice               delta);

int
splinterdb_transaction_lookup(transaction_handle       *txn_hdl,
                              transaction              *txn,
                              slice                     key,
                              splinterdb_lookup_result *result);

#endif // _TRANSACTION_H_
