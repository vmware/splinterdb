// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * transaction.h --
 */

#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#include "splinterdb/splinterdb.h"

void
splinterdb_transaction_init();

void
splinterdb_transaction_deinit();

transaction_id
splinterdb_transaction_begin();

int
splinterdb_transaction_can_commit(transaction_id txn_id);

int
splinterdb_transaction_commit(transaction_id txn_id);

int
splinterdb_transaction_abort(transaction_id txn_id);

int
splinterdb_transaction_insert(const splinterdb *kvsb,
                              slice             key,
                              slice             value,
                              transaction_id    txn_id);

int
splinterdb_transaction_delete(const splinterdb *kvsb,
                              slice             key,
                              transaction_id    txn_id);

int
splinterdb_transaction_update(const splinterdb *kvsb,
                              slice             key,
                              slice             delta,
                              transaction_id    txn_id);

int
splinterdb_transaction_lookup(const splinterdb         *kvsb,
                              slice                     key,
                              splinterdb_lookup_result *result,
                              transaction_id            txn_id);

#endif // _TRANSACTION_H_
