#include "transaction.h"
#include "platform_linux/platform.h"
#include "data_internal.h"
#include "util.h"
#include "tictoc.h"
#include <stdlib.h>

typedef struct transaction_handle {
   const splinterdb        *kvsb;
} transaction_handle;

transaction_handle *
splinterdb_transaction_init(const splinterdb *kvsb, data_config *cfg)
{
   transaction_handle *txn_hdl =
      (transaction_handle *)malloc(sizeof(transaction_handle));

   txn_hdl->kvsb = kvsb;

   return txn_hdl;
}

void
splinterdb_transaction_deinit(transaction_handle *txn_hdl)
{
   if (!txn_hdl) {
      return;
   }

   free(txn_hdl);
}

typedef struct transaction {
  tictoc_transaction tictoc;
} transaction;

transaction *
splinterdb_transaction_begin(transaction_handle *txn_hdl)
{
  transaction *txn = (transaction *)malloc(sizeof(transaction));
  tictoc_transaction_init(&txn->tictoc);
  return txn;
}

int
splinterdb_transaction_commit(transaction_handle *txn_hdl,
                              transaction *txn)
{
  int rc = 0;
  if (tictoc_validation(&txn->tictoc)) {
    tictoc_write(&txn->tictoc);
  } else {
    rc = -1;
    splinterdb_transaction_abort(txn_hdl, txn);
  }

  return rc;
}

int
splinterdb_transaction_abort(transaction_handle *txn_hdl, transaction *txn)
{
  return 0;
}

static platform_status
singleton_tictoc_tuple(transaction *txn, message msg, writable_buffer *wb)
{
   return STATUS_OK;
}


static int
splinterdb_transaction_insert_message(transaction_handle *txn_hdl,
				      transaction *txn,
                                      slice               key,
                                      message             msg)
{
   tictoc_local_write(&txn->tictoc, msg);
   return 0;
}

int
splinterdb_transaction_insert(transaction_handle *txn_hdl,
			      transaction *txn,
                              slice               key,
                              slice               value)
{
   message msg = message_create(MESSAGE_TYPE_INSERT, value);
   return splinterdb_transaction_insert_message(txn_hdl, txn, key, msg);
}

int
splinterdb_transaction_delete(transaction_handle *txn_hdl,
			      transaction *txn,
                              slice               key)
{
   return splinterdb_transaction_insert_message(
      txn_hdl, txn, key, DELETE_MESSAGE);
}

int
splinterdb_transaction_update(transaction_handle *txn_hdl,
                              transaction *txn,
                              slice               key,
                              slice               delta)
{
   message msg = message_create(MESSAGE_TYPE_UPDATE, delta);
   return splinterdb_transaction_insert_message(txn_hdl, txn, key, msg);
}

int
splinterdb_transaction_lookup(transaction_handle       *txn_hdl,
			      transaction *txn,
                              slice                     key,
                              splinterdb_lookup_result *result)
{
  return 0;
}
