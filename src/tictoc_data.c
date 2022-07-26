#include "tictoc_data.h"

tictoc_timestamp_set
get_ts_from_entry(entry *entry)
{
   tictoc_timestamp_set ts;
   memcpy(&ts, writable_buffer_data(&entry->tuple), sizeof(ts));
   return ts;
}

bool
tictoc_entry_is_invalid(entry *entry)
{
   return entry == 0;
}

static void
tictoc_entry_deinit(entry *e)
{
   writable_buffer_deinit(&e->key);
   writable_buffer_deinit(&e->tuple);
   e->latch = 0;
}

entry *
tictoc_get_new_read_set_entry(tictoc_transaction *tt_txn)
{
   return tt_txn->read_cnt == SET_SIZE_LIMIT
             ? 0
             : &tt_txn->read_set[tt_txn->read_cnt++];
}

entry *
tictoc_get_read_set_entry(tictoc_transaction *tt_txn, uint64 i)
{
   return i < tt_txn->read_cnt ? &tt_txn->read_set[i] : 0;
}

entry *
tictoc_get_new_write_set_entry(tictoc_transaction *tt_txn)
{
   return tt_txn->write_cnt == SET_SIZE_LIMIT
             ? 0
             : &tt_txn->write_set[tt_txn->write_cnt++];
}

entry *
tictoc_get_write_set_entry(tictoc_transaction *tt_txn, uint64 i)
{
   return i < tt_txn->write_cnt ? &tt_txn->write_set[i] : 0;
}

bool
tictoc_entry_is_not_in_write_set(tictoc_transaction *tt_txn, entry *e)
{
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      entry *we = &tt_txn->write_set[i];
      if (slices_equal(writable_buffer_to_slice(&we->tuple),
                       writable_buffer_to_slice(&e->tuple)))
      {
         return FALSE;
      }
   }

   return TRUE;
}

void
tictoc_transaction_init(tictoc_transaction *tt_txn)
{
   memset(tt_txn->entries, 0, 2 * SET_SIZE_LIMIT * sizeof(entry));
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
      tictoc_entry_deinit(tictoc_get_read_set_entry(tt_txn, i));
   }

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_entry_deinit(tictoc_get_write_set_entry(tt_txn, i));
   }
}

static int
entry_key_comp(const void *elem1, const void *elem2)
{
   entry *a = (entry *)elem1;
   entry *b = (entry *)elem2;

   slice akey = writable_buffer_to_slice(&a->key);
   slice bkey = writable_buffer_to_slice(&b->key);

   // FIXME: use user-defined key-comapare function
   return slice_lex_cmp(akey, bkey);
}

void
tictoc_transaction_sort_write_set(tictoc_transaction *tt_txn)
{
   qsort(tt_txn->write_set,
         tt_txn->write_cnt * sizeof(entry),
         sizeof(entry),
         entry_key_comp);
}

void
tictoc_transaction_lock_all_write_set(tictoc_transaction *tt_txn,
                                      lock_table         *lock_tbl)
{
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      entry *we   = &tt_txn->write_set[i];
      slice  wkey = writable_buffer_to_slice(&we->key);
      we->latch   = lock_table_lock_range(lock_tbl, wkey, wkey);
   }
}

void
tictoc_transaction_unlock_all_write_set(tictoc_transaction *tt_txn,
                                        lock_table         *lock_tbl)
{
   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      entry *we = &tt_txn->write_set[i];
      lock_table_unlock_latch(lock_tbl, we->latch);
      we->latch = 0;
   }
}
