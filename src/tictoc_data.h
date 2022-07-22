#ifndef _TICTOC_DATA_H_
#define _TICTOC_DATA_H_

#include "splinterdb/public_platform.h"
#include "splinterdb/public_util.h"
#include "util.h"

typedef uint32 tictoc_timestamp;

typedef struct ONDISK tictoc_timestamp_set {
   tictoc_timestamp rts;
   tictoc_timestamp wts;
} tictoc_timestamp_set;

extern tictoc_timestamp_set ZERO_TICTOC_TIMESTAMP_SET;

typedef struct ONDISK tictoc_tuple {
   tictoc_timestamp_set ts_word;
   // char absent; // to indicate whether this tuple is deleted or not
   char value[]; // value provided by application
} tictoc_tuple;

inline uint64
tictoc_tuple_header_size()
{
   return sizeof(tictoc_timestamp_set);
}

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
   void           *latch;
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
