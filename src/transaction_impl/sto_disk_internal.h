#pragma once

#include "../experimental_mode.h"
#include "splinterdb_internal.h"
#include "splinterdb/transaction.h"
#include "../lock_table.h"
#include "../transactional_data_config.h"

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

#define TIMESTAMP_UPDATE_MAGIC 0xdeadbeef
#define TIMESTAMP_UPDATE_RTS (1 << 0)
#define TIMESTAMP_UPDATE_WTS (1 << 1)
#define TIMESTAMP_UPDATE_BOTH (TIMESTAMP_UPDATE_RTS | TIMESTAMP_UPDATE_WTS)

typedef struct ONDISK timestamp_set {
   uint32 magic; // to indicate valid timestamp update. The size is 32bits to align.
   uint32 type;  // to indicate which timestamp is updated. The size is 32bits to align.
   txn_timestamp wts;
   txn_timestamp rts;
} timestamp_set __attribute__((aligned(64)));

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