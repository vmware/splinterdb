#ifndef _MVCC_DATA_H_
#define _MVCC_DATA_H_

#include "util.h"
#include "data_internal.h"

typedef struct ONDISK mvcc_entry {
   transaction_id txn_id;
   message_type   op;
   uint64         len;
   char           data[]; // user value
} mvcc_entry;

typedef struct ONDISK mvcc_message {
   uint64     num_entries;
   mvcc_entry entries[];
} mvcc_message;

uint64
sizeof_mvcc_entry(const mvcc_entry *entry);

uint64
mvcc_entry_size(slice value);

const mvcc_entry *
next_mvcc_entry(const mvcc_entry *entry);

message
mvcc_entry_message(const mvcc_entry *entry);

mvcc_entry
mvcc_create_header(transaction_id tid, message msg);

typedef struct transaction_op_meta {
   slice          key;
   transaction_id txn_id;
   message_type   op;
   uint64         ref_cnt;
} transaction_op_meta;

transaction_op_meta *
transaction_op_meta_create(transaction_id txn_id, slice key, message_type op);

int
transaction_op_meta_destroy(transaction_op_meta *meta);

void
transaction_op_meta_inc_ref(transaction_op_meta *meta);

void
transaction_op_meta_dec_ref(transaction_op_meta *meta);

bool
transaction_op_meta_is_key_equal(transaction_op_meta *m1,
                                 transaction_op_meta *m2);

#endif // _MVCC_DATA_H_
