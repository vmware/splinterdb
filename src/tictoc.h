#ifndef _TICTOC_H_
#define _TICTOC_H_

#include "splinterdb/public_platform.h"

typedef struct TS_word {
  uint8 lock_bit:1;
  uint16 delta:15; // delta = rts - wts
  uint64 wts:48;
} TS_word;

typedef struct tuple {
  TS_word ts_word;
  uint64 data_size;
  char data[];
} tuple;

void tuple_init(tuple *tuple);
  
typedef enum entry_type {
  ENTRY_TYPE_INVALID = 0,
  ENTRY_TYPE_WRITE,
  ENTRY_TYPE_READ
} entry_type;

typedef struct entry {
  uint64 rts;
  uint64 wts;
  tuple *tuple;
  entry_type type;
  message msg;
} entry;

#define SET_SIZE_LIMIT 1024

typedef struct tictoc_transaction {
  entry entries[2 * SET_SIZE_LIMIT];
  entry *read_set;
  entry *write_set;
  uint64 read_cnt;
  uint64 write_cnt;
  uint64 commit_ts;
} tictoc_transaction;

void
tictoc_transaction_init(tictoc_transaction *txn);

void
titoc_read(tictoc_transaction *txn, tuple *t);

char
tictoc_validation(tictoc_transaction *txn);

void
tictoc_write(tictoc_transaction *txn);

void
tictoc_local_write(tictoc_transaction *txn, message msg);

#endif // _TICTOC_H_
