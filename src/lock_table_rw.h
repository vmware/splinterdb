// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform.h"
#include "isketch/iceberg_table.h"
#include "splinterdb/transaction.h"
#include "experimental_mode.h"

/*
 * Implements a lock table that uses READ/WRITE locks and 3 locking policies:
 * NO_WAIT, WAIT-DIE, and WOUND-WAIT
 */

#define LOCK_TABLE_DEBUG   0
#define WOUND_WAIT_TIMEOUT 10

// The lock table is just a hash map
typedef struct lock_table_rw {
   iceberg_table table;
} lock_table_rw;

typedef enum lock_type {
   READ_LOCK = 0, // shared lock
   WRITE_LOCK     // exclusive lock
} lock_type;

typedef struct lock_req {
   lock_type        lt;
   transaction     *txn;  // access to transaction ts as well
   struct lock_req *next; // to form a linked list
} lock_req;

// Each lock_entry in this lock table contains some certain state required to
// implement the chosen locking policy
typedef struct lock_entry {
#if EXPERIMENTAL_MODE_2PL_NO_WAIT == 1
   platform_mutex latch;
#endif
   lock_req *owners;
#if EXPERIMENTAL_MODE_2PL_WOUND_WAIT == 1 || EXPERIMENTAL_MODE_2PL_WAIT_DIE == 1
   platform_condvar condvar;
#endif
} lock_entry;

// FIXME: This lock table assumes rw_entry,
// which is defined in the transactional layer (2pl_internal.h),
// has 'is_locked' field.
// Currently, make sure that the rw_entry struct has 'is_locked'
// and a pointer to a lock_state stucture.
typedef struct rw_entry rw_entry;

typedef enum lock_table_rw_rc {
   LOCK_TABLE_RW_RC_INVALID = 0,
   LOCK_TABLE_RW_RC_OK,
   LOCK_TABLE_RW_RC_BUSY,
   LOCK_TABLE_RW_RC_DEADLK,
   LOCK_TABLE_RW_RC_NODATA
} lock_table_rw_rc;

/*
 * Lock Table Functions
 */
lock_table_rw *
lock_table_rw_create(const data_config *spl_data_config);
void
lock_table_rw_destroy(lock_table_rw *lock_tbl);

// Assumption: transaction contains a TS field
lock_table_rw_rc
lock_table_rw_try_acquire_entry_lock(lock_table_rw *lock_tbl,
                                     rw_entry      *entry,
                                     lock_type      lt,
                                     transaction   *txn);

lock_table_rw_rc
lock_table_rw_release_entry_lock(lock_table_rw *lock_tbl,
                                 rw_entry      *entry,
                                 lock_type      lt,
                                 transaction   *txn);

// lock_table_rw_rc
// lock_table_rw_get_entry_lock_state(lock_table_rw *lock_tbl, rw_entry *entry);
