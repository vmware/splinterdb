#include "experimental_mode.h"

#if EXPERIMENTAL_MODE_SKETCH
#   include "transaction_impl/transaction_sketch.h"
#else
#   if EXPERIMENTAL_MODE_STO_SKETCH
#      include "transaction_impl/transaction_sto.h"
#   else
#      if EXPERIMENTAL_MODE_STO_MEMORY
#         include "transaction_impl/transaction_sto_memory.h"
#      else
#         if EXPERIMENTAL_MODE_TICTOC_DISK
#            include "transaction_impl/transaction_tictoc_disk.h"
#         else
#            if EXPERIMENTAL_MODE_TICTOC_MEMORY
#               include "transaction_impl/transaction_tictoc_memory.h"
#            else
#               include "transaction_impl/transaction_tictoc_lock_silo.h"
#            endif
#         endif
#      endif
#   endif
#endif

const splinterdb *
transactional_splinterdb_get_db(transactional_splinterdb *txn_kvsb)
{
   return txn_kvsb->kvsb;
}
