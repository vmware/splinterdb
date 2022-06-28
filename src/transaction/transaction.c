#include "splinterdb/transaction.h"
#include "transaction_table.h"
#include "lock_table.h"
#include "atomic_counter.h"

// TODO: implement these functions

static bool               transaction_initialized = FALSE;
static transaction_table *txn_tbl;

static lock_table *lock_tbl;

static atomic_counter g_counter;

void
splinterdb_transaction_init()
{
   if (transaction_initialized)
      return;

   txn_tbl = transaction_table_create(TRANSACTION_TABLE_TYPE_QUEUE);

   lock_tbl = lock_table_create();

   atomic_counter_init(&g_counter);

   transaction_initialized = TRUE;
}

void
splinterdb_transaction_deinit()
{
   atomic_counter_deinit(&g_counter);

   lock_table_destroy(lock_tbl);

   transaction_table_destroy(txn_tbl);

   transaction_initialized = FALSE;
}


transaction_id
splinterdb_transaction_begin()
{
   transaction_id txn_id = atomic_counter_get_next(&g_counter);
   transaction_table_insert(txn_tbl, txn_id);

   return txn_id;
}


static int
_can_commit(transaction_id txn_id)
{
   return TRUE;
}

int
splinterdb_transaction_commit(transaction_id txn_id)
{
   transaction_table_update(txn_tbl, txn_id, &transaction_table_tuple_commit);

   return 0;
}

int
splinterdb_transaction_abort(transaction_id txn_id)
{
   transaction_table_update(txn_tbl, txn_id, &transaction_table_tuple_abort);

   return 0;
}

int
splinterdb_transaction_insert(const splinterdb *kvsb,
                              slice             key,
                              slice             value,
                              transaction_id    txn_id)
{
   (void)lock_tbl;
   splinterdb_insert(kvsb, key, value);

   return 0;
}

int
splinterdb_transaction_delete(const splinterdb *kvsb,
                              slice             key,
                              transaction_id    txn_id)
{
   splinterdb_delete(kvsb, key);

   return 0;
}

int
splinterdb_transaction_update(const splinterdb *kvsb,
                              slice             key,
                              slice             delta,
                              transaction_id    txn_id)
{
   splinterdb_update(kvsb, key, delta);

   return 0;
}

int
splinterdb_transaction_lookup(const splinterdb         *kvsb,
                              slice                     key,
                              splinterdb_lookup_result *result,
                              transaction_id            txn_id)
{
   splinterdb_lookup(kvsb, key, result);

   return 0;
}
