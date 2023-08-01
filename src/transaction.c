#include "experimental_mode.h"

#if EXPERIMENTAL_MODE_STO
#   if EXPERIMENTAL_MODE_DISK
#      error "Not implemented yet"
#   elif EXPERIMENTAL_MODE_MEMORY
#      include "transaction_impl/transaction_sto_memory.h"
#   else
#      include "transaction_impl/transaction_sto.h"
#   endif
#elif EXPERIMENTAL_MODE_TICTOC
#   if EXPERIMENTAL_MODE_DISK
#      include "transaction_impl/transaction_tictoc_disk.h"
#   elif EXPERIMENTAL_MODE_MEMORY
#      include "transaction_impl/transaction_tictoc_memory.h"
#   else
#      include "transaction_impl/transaction_sketch.h"
#   endif
#elif EXPERIMENTAL_MODE_SILO
#   include "transaction_impl/transaction_tictoc_lock_silo.h"
#else
#   error "Unknown experimental mode"
#endif

const splinterdb *
transactional_splinterdb_get_db(transactional_splinterdb *txn_kvsb)
{
   return txn_kvsb->kvsb;
}
