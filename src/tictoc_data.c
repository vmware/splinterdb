#include "tictoc_data.h"

#include "data_internal.h"

tictoc_timestamp_set
get_ts_from_tictoc_rw_entry(tictoc_rw_entry *entry)
{
   tictoc_timestamp_set ts;
   memcpy(&ts, writable_buffer_data(&entry->tuple), sizeof(ts));
   return ts;
}

bool
tictoc_rw_entry_is_invalid(tictoc_rw_entry *entry)
{
   return entry == NULL;
}

static void
tictoc_rw_entry_deinit(tictoc_rw_entry *entry)
{
   writable_buffer_deinit(&entry->key);
   writable_buffer_deinit(&entry->tuple);
   entry->rng_lock = NULL;
}

tictoc_rw_entry *
tictoc_get_new_read_set_entry(tictoc_transaction *tt_txn)
{
   return tt_txn->read_cnt == SET_SIZE_LIMIT
             ? NULL
             : &tt_txn->read_set[tt_txn->read_cnt++];
}

tictoc_rw_entry *
tictoc_get_read_set_entry(tictoc_transaction *tt_txn, uint64 i)
{
   return i < tt_txn->read_cnt ? &tt_txn->read_set[i] : NULL;
}

void
tictoc_delete_last_read_set_entry(tictoc_transaction *tt_txn)
{
  platform_assert(tt_txn->read_cnt > 0);
  --tt_txn->read_cnt;
}

tictoc_rw_entry *
tictoc_get_new_write_set_entry(tictoc_transaction *tt_txn)
{
   return tt_txn->write_cnt == SET_SIZE_LIMIT
             ? NULL
             : &tt_txn->write_set[tt_txn->write_cnt++];
}

tictoc_rw_entry *
tictoc_get_write_set_entry(tictoc_transaction *tt_txn, uint64 i)
{
   return i < tt_txn->write_cnt ? &tt_txn->write_set[i] : NULL;
}

bool
tictoc_rw_entry_is_not_in_write_set(tictoc_transaction *tt_txn,
                                    tictoc_rw_entry    *entry)
{
   // TODO: feel free to implement binary search
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *w = &tt_txn->write_set[i];
      if (slices_equal(writable_buffer_to_slice(&w->tuple),
                       writable_buffer_to_slice(&entry->tuple)))
      {
         return FALSE;
      }
   }

   return TRUE;
}

void
tictoc_transaction_init(tictoc_transaction *tt_txn)
{
   memset(tt_txn->entries, 0, 2 * SET_SIZE_LIMIT * sizeof(tictoc_rw_entry));
   tt_txn->read_set  = &tt_txn->entries[0];
   tt_txn->write_set = &tt_txn->entries[SET_SIZE_LIMIT];
   tt_txn->read_cnt  = 0;
   tt_txn->write_cnt = 0;
   tt_txn->commit_ts = 0;
}

void
tictoc_transaction_deinit(tictoc_transaction *tt_txn, lock_table *lock_tbl)
{
   for (uint64 i = 0; i < tt_txn->read_cnt; ++i) {
      tictoc_rw_entry_deinit(tictoc_get_read_set_entry(tt_txn, i));
   }

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry_deinit(tictoc_get_write_set_entry(tt_txn, i));
   }
}

static int
tictoc_rw_entry_key_comp(const void *elem1, const void *elem2, void *args)
{
   tictoc_rw_entry   *a   = (tictoc_rw_entry *)elem1;
   tictoc_rw_entry   *b   = (tictoc_rw_entry *)elem2;
   const data_config *cfg = (const data_config *)args;

   slice akey = writable_buffer_to_slice(&a->key);
   slice bkey = writable_buffer_to_slice(&b->key);

   return data_key_compare(cfg, akey, bkey);
}

void
tictoc_transaction_sort_write_set(tictoc_transaction *tt_txn,
                                  const data_config  *cfg)
{
   platform_sort_slow(tt_txn->write_set,
                      tt_txn->write_cnt,
                      sizeof(tictoc_rw_entry),
                      tictoc_rw_entry_key_comp,
                      (void *)cfg,
                      NULL);
}

void
tictoc_transaction_lock_all_write_set(tictoc_transaction *tt_txn,
                                      lock_table         *lock_tbl)
{
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *we   = &tt_txn->write_set[i];
      slice            wkey = writable_buffer_to_slice(&we->key);
      we->rng_lock = lock_table_acquire_range_lock(lock_tbl, wkey, wkey);
   }
}

void
tictoc_transaction_unlock_all_write_set(tictoc_transaction *tt_txn,
                                        lock_table         *lock_tbl)
{
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *we = &tt_txn->write_set[i];
      lock_table_release_range_lock(lock_tbl, we->rng_lock);
      we->rng_lock = NULL;
   }
}