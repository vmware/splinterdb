#pragma once

#include "splinterdb/data.h"
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
   timestamp                   lock_timeout_ns;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
} transactional_splinterdb;

typedef struct rw_entry {
   slice   key;
   message msg; // value + op
   bool    is_read;
   bool    is_locked;
} rw_entry;
