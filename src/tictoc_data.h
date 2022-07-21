#ifndef _TICTOC_DATA_H_
#define _TICTOC_DATA_H_

#include "splinterdb/public_platform.h"
#include "splinterdb/public_util.h"
#include "util.h"

#define TIMESTAMP_SIZE sizeof(uint32)

typedef struct ONDISK TS_word {
   uint32 rts;
   uint32 wts;
} TS_word;

extern TS_word ZERO_TS_WORD;

typedef struct ONDISK tictoc_tuple {
   TS_word ts_word;
   // char absent; // to indicate whether this tuple is deleted or not
   char value[]; // value provided by application
} tictoc_tuple;

typedef enum entry_type {
   ENTRY_TYPE_INVALID = 0,
   ENTRY_TYPE_WRITE,
   ENTRY_TYPE_READ
} entry_type;

// read_set and write_set entry stored locally
typedef struct entry {
   entry_type      type;
   message_type    op;
   slice           key;
   writable_buffer tuple;
} entry;

#define SET_SIZE_LIMIT 1024

typedef struct tictoc_transaction {
   entry  entries[2 * SET_SIZE_LIMIT];
   entry *read_set;
   entry *write_set;
   uint64 read_cnt;
   uint64 write_cnt;
   uint64 commit_ts;
} tictoc_transaction;

#endif // _TICTOC_DATA_H_
