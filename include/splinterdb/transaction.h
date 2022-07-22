// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * transaction.h --
 */

#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#include "splinterdb/splinterdb.h"
#include "tictoc_data.h"

typedef struct transactional_splinterdb transactional_splinterdb;

int
splinterdb_transaction_init(const splinterdb          *kvsb,
                            transactional_splinterdb **txn_kvsb);

void
splinterdb_transaction_deinit(transactional_splinterdb *txn_kvsb);

typedef struct transaction {
   tictoc_transaction tictoc;
} transaction;

int
splinterdb_transaction_begin(transactional_splinterdb *txn_kvsb,
                             transaction              *txn);

int
splinterdb_transaction_commit(transactional_splinterdb *txn_kvsb,
                              transaction              *txn);

int
splinterdb_transaction_abort(transactional_splinterdb *txn_kvsb,
                             transaction              *txn);

int
splinterdb_transaction_insert(transactional_splinterdb *txn_kvsb,
                              transaction              *txn,
                              slice                     key,
                              slice                     value);

int
splinterdb_transaction_delete(transactional_splinterdb *txn_kvsb,
                              transaction              *txn,
                              slice                     key);

int
splinterdb_transaction_update(transactional_splinterdb *txn_kvsb,
                              transaction              *txn,
                              slice                     key,
                              slice                     delta);

int
splinterdb_transaction_lookup(transactional_splinterdb *txn_kvsb,
                              transaction              *txn,
                              slice                     key,
                              splinterdb_lookup_result *result);

#endif // _TRANSACTION_H_
