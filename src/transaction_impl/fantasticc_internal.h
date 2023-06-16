#pragma once

#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "isketch/iceberg_table.h"
#include "lock_table.h"

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   uint64                      tscache_log_slots;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
   iceberg_table                   *tscache;
} transactional_splinterdb;

// This causes a lot of delta overflow with tictoc
/* typedef struct timestamp_set { */
/*    txn_timestamp delta : 15; // rts = wts + delta */
/*    txn_timestamp wts : 49; */
/* } timestamp_set __attribute__((aligned(sizeof(txn_timestamp)))); */


// TODO we don't need to use delta, use rts
typedef struct {
   txn_timestamp delta : 64;
   txn_timestamp wts : 64;
} timestamp_set __attribute__((aligned(sizeof(txn_timestamp))));

// read_set and write_set entry stored locally
typedef struct rw_entry {
   slice          key;
   message        msg; // value + op
   txn_timestamp  wts;
   txn_timestamp  rts;
   timestamp_set *tuple_ts;
   char           is_read;
   char           need_to_keep_key;
   char           is_locked;
} rw_entry;

static inline txn_timestamp
timestamp_set_get_rts(timestamp_set *ts)
{
#if EXPERIMENTAL_MODE_SILO == 1
   return ts->wts;
#else
   return ts->wts + ts->delta;
#endif
}

static inline void
timestamp_set_load(timestamp_set *tuple_ts, timestamp_set *v)
{
   __atomic_load(
      (txn_timestamp *)tuple_ts, (txn_timestamp *)v, __ATOMIC_RELAXED);
}
