#include "tictoc.h"
#include "util.h"
#include <string.h>

void
tuple_init(tuple *tuple)
{
  memset(&tuple->ts_word, 0, sizeof(tuple->ts_word));
}

static inline char
tuple_is_locked(tuple *tuple)
{
  return tuple->ts_word.lock_bit == 1;
}

static void
tuple_lock(tuple *tuple)
{
  //  while (__sync_lock_test_and_set(&tuple->ts_word.lock_bit, 1)) {}
  // TODO: check if this is atomic
  
  while (tuple_is_locked(tuple)) {}
  tuple->ts_word.lock_bit = 1;
}

static void
tuple_unlock(tuple *tuple)
{
  //  __sync_lock_release(&tuple->ts_word);
  tuple->ts_word.lock_bit = 0;
}

static inline uint64
tuple_get_rts(tuple *tuple)
{
  return tuple->ts_word.wts + tuple->ts_word.delta;
}

static inline void
tuple_set_rts(tuple *tuple, uint64 rts)
{
  tuple->ts_word.delta = rts - tuple->ts_word.wts;
}

static inline uint64
tuple_get_wts(tuple *tuple)
{
  return tuple->ts_word.wts;
}

static inline void
tuple_set_wts(tuple *tuple, uint64 wts)
{
  tuple->ts_word.wts = wts;
}


// FIXME: data goes away
static inline void
tuple_set_data(tuple *tuple, char *data, uint64 data_size)
{
  memcpy(tuple->data, data, data_size);
  tuple->data_size = data_size;
}

/*
 * Algorithm 4: Atomically Load Tuple Data and Timestamps
 */
static void
entry_atomic_load_from_tuple(tuple *tuple, entry *r)
{
  TS_word v1, v2;
  do {
    v1 = tuple->ts_word;
    memcpy(r->data, tuple->data, tuple->data_size); // FIXME: data goes away
    r->data_size = tuple->data_size;
    v2 = tuple->ts_word;
  } while(memcmp(&v1, &v2, sizeof(TS_word)) != 0 || v1.lock_bit == 1);

  r->rts = v1.wts + v1.delta;
  r->wts = v1.wts;
}

void
tictoc_transaction_init(tictoc_transaction *txn)
{
  memset(txn->entries, 0, 2 * SET_SIZE_LIMIT * sizeof(entry));

  txn->read_set = &txn->entries[0];
  txn->write_set = &txn->entries[SET_SIZE_LIMIT];
  txn->read_cnt = 0;
  txn->write_cnt = 0;
}

static inline entry *
get_new_read_set_entry(tictoc_transaction *txn)
{
  entry *r = &txn->read_set[txn->read_cnt++];
  r->type = ENTRY_TYPE_READ;
  return r;
}

static inline entry *
get_read_set_entry(tictoc_transaction *txn, uint64 i)
{
  return (i < txn->read_cnt) ? &txn->read_set[i] : 0;
}

static inline entry *
get_new_write_set_entry(tictoc_transaction *txn)
{
  entry *w = &txn->write_set[txn->write_cnt++];
  w->type = ENTRY_TYPE_WRITE;
  return w;
}

static inline entry *
get_write_set_entry(tictoc_transaction *txn, uint64 i)
{
  return (i < txn->write_cnt) ? &txn->write_set[i] : 0;
}

static inline void
sort_write_set(tictoc_transaction *txn)
{
}

static inline entry *
get_entry(tictoc_transaction *txn, uint64 i)
{
  return (i < txn->read_cnt + txn->write_cnt) ? &txn->entries[i] : 0;
}

static inline char
tuple_is_not_in_write_set(tictoc_transaction *txn, tuple *tuple)
{
  for(uint64 i = 0; i < txn->write_cnt; ++i) {
    entry *w = get_write_set_entry(txn, i);
    if (w->tuple == tuple) {
      return 0;
    }
  }

  return 1;
}

/*
 * Algorithm 1: Read Phase
 */

void
titoc_read(tictoc_transaction *txn, tuple *t)
{
  entry *r = get_new_read_set_entry(txn);
  r->tuple = t;
  entry_atomic_load_from_tuple(t, r);
}

/*
 * Algorithm 2: Validation Phase
 */
char
tictoc_validation(tictoc_transaction *txn)
{
  // Step 1: Lock Write Set
  sort_write_set(txn);
  
  for(uint64 i = 0; i < txn->write_cnt; ++i) {
    entry *r = get_write_set_entry(txn, i);
    tuple_lock(r->tuple);
  }

  // Step 2: Compute the Commit Timestamp
  txn->commit_ts = 0;

  uint64 total_size = txn->write_cnt + txn->read_cnt;
  for(uint64 i = 0; i < total_size; ++i) {
    entry *e = get_entry(txn, i);
    if (e->type == ENTRY_TYPE_WRITE) {
      txn->commit_ts = MAX(txn->commit_ts, tuple_get_rts(e->tuple) + 1);
    } else if (e->type == ENTRY_TYPE_READ) {
      txn->commit_ts = MAX(txn->commit_ts, tuple_get_wts(e->tuple));
    } else {
      // invaild
    }
  }

  // Step 3: Validate the Read Set
  for (uint64 i = 0; i < txn->read_cnt; ++i) {
    entry *r = get_read_set_entry(txn, i);
    if (r->rts < txn->commit_ts) {
      // TODO: Begin atomic section
      if ((r->wts != tuple_get_wts(r->tuple)) ||
	  (tuple_get_rts(r->tuple) <= txn->commit_ts &&
	   tuple_is_locked(r->tuple) &&
	   tuple_is_not_in_write_set(txn, r->tuple))) {
	return 0;
      } else {
	uint64 tuple_rts = tuple_get_rts(r->tuple);
	tuple_set_rts(r->tuple, MAX(txn->commit_ts, tuple_rts));
      }
      // TODO: End atomic section
    }
  }

  return 1;
}

/*
 * Algorithm 3: Write Phase
 */
void
tictoc_write(tictoc_transaction *txn)
{
  for (uint64 i = 0; i < txn->write_cnt; ++i) {
    entry *w = get_write_set_entry(txn, i);
    tuple_set_data(w->tuple, w->data, w->data_size); // FIXME: data is gone away
    // TODO: merge messages in the write set and write to splinterdb
    tuple_set_wts(w->tuple, txn->commit_ts);
    tuple_set_rts(w->tuple, txn->commit_ts);
    tuple_unlock(w->tuple);
  }
}


void
tictoc_local_write(tictoc_transaction *txn, message msg)
{
  entry *w = get_new_write_set_entry(txn);
  w->msg = msg;
}
