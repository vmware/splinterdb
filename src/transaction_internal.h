#pragma once

#include "platform.h"
#include "transactional_data_config.h"
#include "tictoc_data.h"
#include "hash_lock.h"

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transactional_data_config  *txn_data_cfg;
   transaction_isolation_level isol_level;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
   hash_lock                        hash_lock;
} transactional_splinterdb;
