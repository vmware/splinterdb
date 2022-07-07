#include "mvcc_data.h"

uint64
sizeof_mvcc_entry(const mvcc_entry *entry)
{
   return sizeof(*entry) + entry->len;
}

uint64
mvcc_entry_size(slice value)
{
   return sizeof(mvcc_entry) + slice_length(value);
}

const mvcc_entry *
next_mvcc_entry(const mvcc_entry *entry)
{
   return (const mvcc_entry *)((const char *)entry + sizeof_mvcc_entry(entry));
}

message
mvcc_entry_message(const mvcc_entry *entry)
{
   message out_message;
   out_message.type = entry->op;
   out_message.data = slice_create(entry->len, entry->data);
   return out_message;
}

mvcc_entry
mvcc_create_header(transaction_id txn_id, message msg)
{
   mvcc_entry entry;
   entry.txn_id = txn_id;
   entry.op     = message_class(msg);
   entry.len    = message_length(msg);
   return entry;
}


transaction_op_meta *
transaction_op_meta_create(transaction_id txn_id, slice key, message_type op)
{
   writable_buffer meta_buf;
   writable_buffer_init(&meta_buf, 0); // FIXME: use a valid heap_id
   writable_buffer_resize(&meta_buf, sizeof(transaction_op_meta));
   transaction_op_meta *meta = writable_buffer_data(&meta_buf);

   meta->key     = key;
   meta->txn_id  = txn_id;
   meta->op      = op;
   meta->ref_cnt = 0;

   return meta;
}

int
transaction_op_meta_destroy(transaction_op_meta *meta)
{
   if (meta->ref_cnt == 0) {
      writable_buffer_deinit((writable_buffer *)meta);
      return 0;
   }

   return -1;
}

void
transaction_op_meta_inc_ref(transaction_op_meta *meta)
{
   ++meta->ref_cnt;
}

void
transaction_op_meta_dec_ref(transaction_op_meta *meta)
{
   --meta->ref_cnt;
   transaction_op_meta_destroy(meta);
}

int
transaction_op_meta_is_key_equal(transaction_op_meta *m1,
                                 transaction_op_meta *m2)
{
   return slice_lex_cmp(m1->key, m2->key);
}
