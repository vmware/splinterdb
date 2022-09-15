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

// Register the current thread so that it can be used with splinterdb.
// This causes scratch space to be allocated for the thread.
//
// Any thread that uses a splinterdb must first be registered with it.
//
// The only exception is the initial thread which called create or open,
// as that thread is implicitly registered.  Re-registering it will leak memory.
//
// A thread should not be registered more than once; that would leak memory.
//
// splinterdb_close will use scratch space, so the thread that calls it must
// have been registered (or implicitly registered by being the initial thread).
//
// Note: There is currently a limit of MAX_THREADS registered at a given time
void
transactional_splinterdb_register_thread(transactional_splinterdb *kvs);

// Deregister the current thread and free its scratch space.
//
// Call this function before exiting a registered thread.
// Otherwise, you'll leak memory.
void
transactional_splinterdb_deregister_thread(transactional_splinterdb *kvs);

typedef enum {
   TRANSACTION_ISOLATION_LEVEL_INVALID = 0,
   TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE,
   TRANSACTION_ISOLATION_LEVEL_SNAPSHOT,
   TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ,
   TRANSACTION_ISOLATION_LEVEL_MAX_VALID
} transaction_isolation_level;

typedef struct tictoc_rw_entry tictoc_rw_entry;

#define TICTOC_RW_SET_SIZE_LIMIT 32

// TODO: use interval_tree_node for tictoc_rw_entry
typedef struct tictoc_transaction {
   tictoc_rw_entry            *read_write_set[TICTOC_RW_SET_SIZE_LIMIT];
   tictoc_rw_entry           **read_set;
   tictoc_rw_entry           **write_set;
   uint64                      read_cnt;
   uint64                      write_cnt;
   uint64                      commit_rts;
   uint64                      commit_wts;
   transaction_isolation_level isol_level;
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

void
transactional_splinterdb_set_isolation_level(
   transactional_splinterdb   *txn_kvsb,
   transaction_isolation_level isol_level);

#endif // _TRANSACTION_H_
