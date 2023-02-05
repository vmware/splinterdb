#include "tictoc_data.h"

#include "data_internal.h"
#include "splinterdb/data.h"
#include "splinterdb/public_util.h"

bool
tictoc_rw_entry_is_invalid(tictoc_rw_entry *entry)
{
   return entry == NULL;
}

tictoc_rw_entry *
tictoc_rw_entry_create()
{
   tictoc_rw_entry *new_entry;
   new_entry = TYPED_ZALLOC(0, new_entry);
   platform_assert(new_entry != NULL);

   RB_CLEAR_NODE(&new_entry->rb);

   return new_entry;
}

static void
tictoc_rw_entry_deinit(tictoc_rw_entry *entry)
{
   bool can_key_free = !slice_is_null(entry->key) && !entry->need_to_keep_key;
   if (can_key_free) {
      platform_free_from_heap(0, (void *)slice_data(entry->key));
   }

   if (!message_is_null(entry->msg)) {
      platform_free_from_heap(0, (void *)message_data(entry->msg));
   }
}

tictoc_rw_entry *
tictoc_get_new_read_set_entry(tictoc_transaction *tt_txn)
{
   if (tt_txn->read_cnt == TICTOC_RW_SET_SIZE_LIMIT / 2) {
      return NULL;
   }

   tictoc_rw_entry *new_entry           = tictoc_rw_entry_create();
   tt_txn->read_set[tt_txn->read_cnt++] = new_entry;

   return new_entry;
}

tictoc_rw_entry *
tictoc_get_read_set_entry(tictoc_transaction *tt_txn, uint64 i)
{
   return i < tt_txn->read_cnt ? tt_txn->read_set[i] : NULL;
}

tictoc_rw_entry *
tictoc_get_new_write_set_entry(tictoc_transaction *tt_txn)
{
   if (tt_txn->write_cnt == TICTOC_RW_SET_SIZE_LIMIT / 2) {
      return NULL;
   }

   tictoc_rw_entry *new_entry             = tictoc_rw_entry_create();
   tt_txn->write_set[tt_txn->write_cnt++] = new_entry;

   return new_entry;
}

tictoc_rw_entry *
tictoc_get_write_set_entry(tictoc_transaction *tt_txn, uint64 i)
{
   return i < tt_txn->write_cnt ? tt_txn->write_set[i] : NULL;
}

bool
tictoc_rw_entry_is_not_in_write_set(tictoc_transaction *tt_txn,
                                    tictoc_rw_entry    *entry,
                                    const data_config  *cfg)
{
   key ekey = key_create_from_slice(entry->key);
   // TODO: feel free to implement binary search
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *w    = tictoc_get_write_set_entry(tt_txn, i);
      key              wkey = key_create_from_slice(w->key);
      if (data_key_compare(cfg, ekey, wkey) == 0) {
         return FALSE;
      }
   }

   return TRUE;
}

void
tictoc_transaction_init(tictoc_transaction         *tt_txn,
                        transaction_isolation_level isol_level)
{
   tt_txn->read_set           = &tt_txn->read_write_set[0];
   const uint64 read_set_size = TICTOC_RW_SET_SIZE_LIMIT / 2;
   tt_txn->write_set          = &tt_txn->read_write_set[read_set_size];
   tt_txn->read_cnt           = 0;
   tt_txn->write_cnt          = 0;
   tt_txn->commit_rts         = 0;
   tt_txn->commit_wts         = 0;
   tt_txn->isol_level         = isol_level;
}

void
tictoc_transaction_deinit(tictoc_transaction *tt_txn, lock_table *lock_tbl)
{
   for (uint64 i = 0; i < tt_txn->read_cnt; ++i) {
      tictoc_rw_entry *r = tictoc_get_read_set_entry(tt_txn, i);
      tictoc_rw_entry_deinit(r);
      platform_free(0, r);
   }

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *w = tictoc_get_write_set_entry(tt_txn, i);
      tictoc_rw_entry_deinit(w);
      platform_free(0, w);
   }
}

static int
tictoc_rw_entry_key_comp(const void *elem1, const void *elem2, void *args)
{
   tictoc_rw_entry  **a   = (tictoc_rw_entry **)elem1;
   tictoc_rw_entry  **b   = (tictoc_rw_entry **)elem2;
   const data_config *cfg = (const data_config *)args;

   key akey = key_create_from_slice((*a)->key);
   key bkey = key_create_from_slice((*b)->key);

   return data_key_compare(cfg, akey, bkey);
}

void
tictoc_transaction_sort_write_set(tictoc_transaction *tt_txn,
                                  const data_config  *cfg)
{
   platform_sort_slow(tt_txn->write_set,
                      tt_txn->write_cnt,
                      sizeof(tictoc_rw_entry *),
                      tictoc_rw_entry_key_comp,
                      (void *)cfg,
                      NULL);
}

bool
tictoc_transaction_lock_all_write_set(tictoc_transaction *tt_txn,
                                      lock_table         *lock_tbl)
{
   uint64 locked_cnt = 0;
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      bool is_acquired = lock_table_try_acquire_entry_lock(
         lock_tbl, tictoc_get_write_set_entry(tt_txn, i));
      if (!is_acquired) {
         for (uint64 j = 0; j < locked_cnt; ++j) {
            lock_table_release_entry_lock(
               lock_tbl, tictoc_get_write_set_entry(tt_txn, j));
         }

         return FALSE;
      }

      ++locked_cnt;
   }

   return TRUE;
}

void
tictoc_transaction_unlock_all_write_set(tictoc_transaction *tt_txn,
                                        lock_table         *lock_tbl)
{
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *we = tictoc_get_write_set_entry(tt_txn, i);
      lock_table_release_entry_lock(lock_tbl, we);
   }
}
