#include "experimental_mode.h"

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
#elif EXPERIMENTAL_MODE_2PL_NO_WAIT || EXPERIMENTAL_MODE_2PL_WAIT_DIE          \
   || EXPERIMENTAL_MODE_2PL_WOUND_WAIT
#   include "transaction_impl/transaction_2pl.h"
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
