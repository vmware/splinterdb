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

typedef struct PACKED timestamp_set {
   timestamp refcount : 6;
   timestamp delta : 15; // rts = wts + delta
   timestamp wts : 43;
} timestamp_set;

extern timestamp_set ZERO_TIMESTAMP_SET;

static inline timestamp
timestamp_set_get_rts(timestamp_set *ts)
{
#if EXPERIMENTAL_MODE_SILO == 1
   return ts->wts;
#else
   return ts->wts + ts->delta;
#endif
}

static inline timestamp
timestamp_set_get_delta(timestamp wts, timestamp rts)
{
#if EXPERIMENTAL_MODE_SILO == 1
   return 0;
#else
   platform_assert(rts >= wts);
   return rts - wts;
#endif
}

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
