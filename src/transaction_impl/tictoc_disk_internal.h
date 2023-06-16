#pragma once

#include "platform.h"
#include "data_internal.h"
#include "splinterdb/transaction.h"
#include "util.h"
#include "experimental_mode.h"
#include "splinterdb_internal.h"
#include "isketch/iceberg_table.h"
#include "lock_table.h"
#include "transactional_data_config.h"

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transaction_isolation_level isol_level;
   transactional_data_config  *txn_data_cfg;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
} transactional_splinterdb;

typedef struct ONDISK timestamp_set {
   txn_timestamp wts;
   txn_timestamp rts;
} timestamp_set __attribute__((aligned(64)));

// read_set and write_set entry stored locally
typedef struct rw_entry {
   slice         key;
   message       msg; // value + op
   txn_timestamp wts;
   txn_timestamp rts;
   char          is_read;
   char          is_locked;
} rw_entry;

typedef struct ONDISK tuple_header {
   timestamp_set ts;
   char          value[];
} tuple_header;
