// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * transaction.h --
 */

#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#include "splinterdb/splinterdb.h"

typedef struct transactional_splinterdb transactional_splinterdb;

// Create a new SplinterDB instance, erasing any existing file or block device.
//
// The library will allocate and own the memory for splinterdb
// and will free it on splinterdb_close().
//
// It is ok for the caller to stack-allocate cfg, since it is not retained
int
transactional_splinterdb_create(const splinterdb_config   *kvsb_cfg,
                                transactional_splinterdb **txn_kvsb);

// Open an existing splinterdb from a file/device on disk
//
// The library will allocate and own the memory for splinterdb
// and will free it on splinterdb_close().
//
// It is ok for the caller to stack-allocate cfg, since it is not retained
int
transactional_splinterdb_open(const splinterdb_config   *kvsb_cfg,
                              transactional_splinterdb **txn_kvsb);

// Close a splinterdb
//
// This will flush all data to disk and release all resources
void
transactional_splinterdb_close(transactional_splinterdb **txn_kvsb);

typedef struct tictoc_rw_entry tictoc_rw_entry;

// TODO: use interval_tree_node for tictoc_rw_entry
typedef struct tictoc_transaction {
   tictoc_rw_entry *read_write_set;
   tictoc_rw_entry *read_set;
   tictoc_rw_entry *write_set;
   uint64           read_cnt;
   uint64           write_cnt;
   uint64           commit_ts;
} tictoc_transaction;

typedef struct transaction {
   tictoc_transaction tictoc;
} transaction;

int
transactional_splinterdb_begin(transactional_splinterdb *txn_kvsb,
                               transaction              *txn);

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn);

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn);

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     value);

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key);

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     delta);

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                splinterdb_lookup_result *result);

// XXX: These functions wouldn't be necessary if txn_kvsb were public
void
transactional_splinterdb_lookup_result_init(
   transactional_splinterdb *txn_kvsb,   // IN
   splinterdb_lookup_result *result,     // IN/OUT
   uint64                    buffer_len, // IN
   char                     *buffer      // IN
);

int
transactional_splinterdb_lookup_result_value(
   transactional_splinterdb       *txn_kvsb,
   const splinterdb_lookup_result *result, // IN
   slice                          *value   // OUT
);

#endif // _TRANSACTION_H_
