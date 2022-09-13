#ifndef _TRANSACTION_PRIVATE_H_
#define _TRANSACTION_PRIVATE_H_

#include "platform.h"
#include "transactional_data_config.h"
#include "tictoc_data.h"

typedef struct transactional_splinterdb_config {
   splinterdb_config           kvsb_cfg;
   transactional_data_config  *txn_data_cfg;
   transaction_isolation_level isol_level;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   lock_table                      *lock_tbl;
   platform_mutex                   g_lock;
} transactional_splinterdb;


uint64
transactional_splinterdb_key_size(transactional_splinterdb *txn_kvsb);

#endif
