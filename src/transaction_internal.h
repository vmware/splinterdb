#pragma once

#include "platform.h"
#include "data_internal.h"
#include "tictoc_data.h"
#include "splinterdb/transaction.h"
#include "platform_linux/platform.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "iceberg_table.h"

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
