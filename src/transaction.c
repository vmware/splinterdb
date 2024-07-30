#include "experimental_mode.h"

// Define struct transaction

#if EXPERIMENTAL_MODE_KR_OCC || EXPERIMENTAL_MODE_KR_OCC_PARALLEL
typedef struct transaction {
   void *internal;
} transaction;
#else
#   define RW_SET_SIZE_LIMIT 64
typedef struct rw_entry rw_entry;
typedef struct transaction {
   rw_entry *rw_entries[RW_SET_SIZE_LIMIT];
   uint64    num_rw_entries;
   uint128   ts;
#   if EXPERIMENTAL_MODE_2PL_WAIT_DIE || EXPERIMENTAL_MODE_2PL_WOUND_WAIT
   bool      wounded;
#   endif
} transaction;
#endif

// Include the header file for implementation

#if EXPERIMENTAL_MODE_TICTOC_DISK
#   include "transaction_impl/transaction_tictoc_disk.h"
#elif EXPERIMENTAL_MODE_TICTOC_MEMORY
#   include "transaction_impl/transaction_tictoc_memory.h"
#elif EXPERIMENTAL_MODE_TICTOC_COUNTER
#   include "transaction_impl/transaction_tictoc_sketch.h"
#elif EXPERIMENTAL_MODE_TICTOC_SKETCH
#   include "transaction_impl/transaction_tictoc_sketch.h"
#elif EXPERIMENTAL_MODE_STO_DISK
#   include "transaction_impl/transaction_sto_disk.h"
#elif EXPERIMENTAL_MODE_STO_MEMORY
#   include "transaction_impl/transaction_sto_memory.h"
#elif EXPERIMENTAL_MODE_STO_COUNTER
#   include "transaction_impl/transaction_sto.h"
#elif EXPERIMENTAL_MODE_STO_SKETCH
#   include "transaction_impl/transaction_sto.h"
#elif EXPERIMENTAL_MODE_MVCC_DISK
#   include "transaction_impl/transaction_mvcc_disk.h"
#elif EXPERIMENTAL_MODE_MVCC_MEMORY
#   include "transaction_impl/transaction_mvcc_memory.h"
#elif EXPERIMENTAL_MODE_MVCC_COUNTER
#   include "transaction_impl/transaction_mvcc_sketch.h"
#elif EXPERIMENTAL_MODE_MVCC_SKETCH
#   include "transaction_impl/transaction_mvcc_sketch.h"
#elif EXPERIMENTAL_MODE_2PL_NO_WAIT || EXPERIMENTAL_MODE_2PL_WAIT_DIE          \
   || EXPERIMENTAL_MODE_2PL_WOUND_WAIT
#   include "transaction_impl/transaction_2pl.h"
#elif EXPERIMENTAL_MODE_KR_OCC || EXPERIMENTAL_MODE_KR_OCC_PARALLEL
#   include "transaction_impl/transaction_kr_occ.h"
#elif EXPERIMENTAL_MODE_SILO_MEMORY
#   include "transaction_impl/transaction_tictoc_lock_silo.h"
#else
#   error "Unknown experimental mode"
#endif

const splinterdb *
transactional_splinterdb_get_db(transactional_splinterdb *txn_kvsb)
{
   return txn_kvsb->kvsb;
}

void
transactional_splinterdb_lookup_result_init(
   transactional_splinterdb *txn_kvsb,   // IN
   splinterdb_lookup_result *result,     // IN/OUT
   uint64                    buffer_len, // IN
   char                     *buffer      // IN
)
{
   return splinterdb_lookup_result_init(
      txn_kvsb->kvsb, result, buffer_len, buffer);
}

void
transactional_splinterdb_set_isolation_level(
   transactional_splinterdb   *txn_kvsb,
   transaction_isolation_level isol_level)
{
   platform_assert(isol_level > TRANSACTION_ISOLATION_LEVEL_INVALID);
   platform_assert(isol_level < TRANSACTION_ISOLATION_LEVEL_MAX_VALID);

   txn_kvsb->tcfg->isol_level = isol_level;
}

void
transactional_splinterdb_disable_upsert(transactional_splinterdb *txn_kvsb)
{
   txn_kvsb->tcfg->is_upsert_disabled = TRUE;
}

transaction *
transaction_create()
{
   transaction *txn;
   txn = TYPED_ZALLOC(0, txn);
   return txn;
}

void
transaction_destroy(transaction *txn)
{
   platform_free(0, txn);
}