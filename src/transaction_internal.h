#pragma once

#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "platform_linux/platform.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "iceberg_table.h"
#include "lock_table.h"
#include "sketch.h"

#if EXPERIMENTAL_MODE_TICTOC_DISK
#   include "transactional_data_config.h"
#endif

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;

#if EXPERIMENTAL_MODE_TICTOC_DISK
   transactional_data_config *txn_data_cfg;
#else
   uint64 tscache_log_slots;
#endif
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
#if EXPERIMENTAL_MODE_TICTOC_DISK == 0
   iceberg_table *tscache;
#endif
#if EXPERIMENTAL_MODE_SKETCH
   sketch *sket;
#endif
} transactional_splinterdb;

#if EXPERIMENTAL_MODE_TICTOC_DISK
typedef struct PACKED timestamp_set {
   txn_timestamp wts;
   txn_timestamp rts;
} timestamp_set;
#else
typedef struct PACKED timestamp_set {
   txn_timestamp refcount : 6;
   txn_timestamp delta : 15; // rts = wts + delta
   txn_timestamp wts : 43;
} timestamp_set;
#endif

extern timestamp_set ZERO_TIMESTAMP_SET;

typedef struct PACKED tuple_header {
#if EXPERIMENTAL_MODE_TICTOC_DISK
   timestamp_set ts;
   char          value[];
#endif
} tuple_header;

#if EXPERIMENTAL_MODE_TICTOC_DISK == 0
static inline txn_timestamp
timestamp_set_get_rts(timestamp_set *ts)
{
#   if EXPERIMENTAL_MODE_SILO == 1
   return ts->wts;
#   else
   return ts->wts + ts->delta;
#   endif
}

static inline txn_timestamp
timestamp_set_get_delta(txn_timestamp wts, txn_timestamp rts)
{
#   if EXPERIMENTAL_MODE_SILO == 1
   return 0;
#   else
   platform_assert(rts >= wts);
   return rts - wts;
#   endif
}
#endif

static inline bool
is_serializable(transactional_splinterdb_config *tcfg)
{
   return (tcfg->isol_level == TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE);
}

static inline bool
is_snapshot_isolation(transactional_splinterdb_config *tcfg)
{
   return (tcfg->isol_level == TRANSACTION_ISOLATION_LEVEL_SNAPSHOT);
}

static inline bool
is_repeatable_read(transactional_splinterdb_config *tcfg)
{
   return (tcfg->isol_level == TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ);
}
