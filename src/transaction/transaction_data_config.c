#include "transaction_data_config.h"
#include "mvcc_data.h"

static int
merge_mvcc_message(const data_config *cfg,
                   slice              key,         // IN
                   message            old_message, // IN
                   merge_accumulator *new_message) // IN/OUT
{
   const transaction_data_config *tcfg = (const transaction_data_config *)cfg;

   merge_accumulator out_message;
   merge_accumulator_init(
      &out_message, new_message->data.heap_id); // FIXME: use a correct heap_id

   merge_accumulator_resize(&out_message, sizeof(mvcc_message));

   merge_accumulator tmp_new_message;
   merge_accumulator_init(
      &tmp_new_message,
      new_message->data.heap_id); // FIXME: use a correct heap_id

   const mvcc_message *old_mvcc_message = message_data(old_message);
   const mvcc_message *new_mvcc_message = merge_accumulator_data(new_message);

   const mvcc_entry *old_entry = old_mvcc_message->entries;
   const mvcc_entry *new_entry = new_mvcc_message->entries;

   uint64 old_entry_index = 0;
   uint64 new_entry_index = 0;

   uint64 num_entries = 0;

   while (old_entry_index < old_mvcc_message->num_entries
          && new_entry_index < new_mvcc_message->num_entries)
   {
      if (old_entry->txn_id < new_entry->txn_id) {
         writable_buffer_append(
            &out_message.data, sizeof_mvcc_entry(old_entry), old_entry);
         old_entry = next_mvcc_entry(old_entry);
         ++old_entry_index;
      } else if (old_entry->txn_id > new_entry->txn_id) {
         writable_buffer_append(
            &out_message.data, sizeof_mvcc_entry(new_entry), new_entry);
         new_entry = next_mvcc_entry(new_entry);
         ++new_entry_index;
      } else {
         merge_accumulator_copy_message(&tmp_new_message,
                                        mvcc_entry_message(new_entry));

         data_merge_tuples(tcfg->application_data_config,
                           key,
                           mvcc_entry_message(old_entry),
                           &tmp_new_message);

         mvcc_entry new_entry_header = mvcc_create_header(
            new_entry->txn_id, merge_accumulator_to_message(&tmp_new_message));

         writable_buffer_append(
            &out_message.data, sizeof(new_entry_header), &new_entry_header);

         writable_buffer_append(&out_message.data,
                                merge_accumulator_length(&tmp_new_message),
                                merge_accumulator_data(&tmp_new_message));

         old_entry = next_mvcc_entry(old_entry);
         ++old_entry_index;

         new_entry = next_mvcc_entry(new_entry);
         ++new_entry_index;
      }

      ++num_entries;
   }

   while (old_entry_index < old_mvcc_message->num_entries) {
      writable_buffer_append(
         &out_message.data, sizeof_mvcc_entry(old_entry), old_entry);
      old_entry = next_mvcc_entry(old_entry);
      ++old_entry_index;

      ++num_entries;
   }

   while (new_entry_index < new_mvcc_message->num_entries) {
      writable_buffer_append(
         &out_message.data, sizeof_mvcc_entry(new_entry), new_entry);
      new_entry = next_mvcc_entry(new_entry);
      ++new_entry_index;

      ++num_entries;
   }

   mvcc_message *out_message_header = merge_accumulator_data(&out_message);
   out_message_header->num_entries  = num_entries;

   merge_accumulator_copy_message(new_message,
                                  merge_accumulator_to_message(&out_message));

   merge_accumulator_deinit(&tmp_new_message);
   merge_accumulator_deinit(&out_message);

   return 0;
}

// TODO: implement this
static int
merge_mvcc_message_final(const data_config *cfg,
                         slice              key,
                         merge_accumulator *oldest_message)
{
   const transaction_data_config *tcfg = (const transaction_data_config *)cfg;

   merge_accumulator out_message;
   merge_accumulator_init(
      &out_message,
      oldest_message->data.heap_id); // FIXME: use a correct heap_id

   merge_accumulator_resize(&out_message, sizeof(mvcc_message));

   merge_accumulator tmp_oldest_message;
   merge_accumulator_init(
      &tmp_oldest_message,
      oldest_message->data.heap_id); // FIXME: use a correct heap_id

   const mvcc_message *oldest_mvcc_message =
      merge_accumulator_data(oldest_message);
   const mvcc_entry *oldest_entry = oldest_mvcc_message->entries;

   uint64 oldest_entry_index = 0;

   while (oldest_entry_index < oldest_mvcc_message->num_entries) {
      merge_accumulator_copy_message(&tmp_oldest_message,
                                     mvcc_entry_message(oldest_entry));

      data_merge_tuples_final(
         tcfg->application_data_config, key, &tmp_oldest_message);

      mvcc_entry oldest_entry_header =
         mvcc_create_header(oldest_entry->txn_id,
                            merge_accumulator_to_message(&tmp_oldest_message));

      writable_buffer_append(
         &out_message.data, sizeof(oldest_entry_header), &oldest_entry_header);

      writable_buffer_append(&out_message.data,
                             merge_accumulator_length(&tmp_oldest_message),
                             merge_accumulator_data(&tmp_oldest_message));

      oldest_entry = next_mvcc_entry(oldest_entry);
      ++oldest_entry_index;
   }

   mvcc_message *out_message_header = merge_accumulator_data(&out_message);
   out_message_header->num_entries  = oldest_entry_index;

   merge_accumulator_copy_message(oldest_message,
                                  merge_accumulator_to_message(&out_message));

   merge_accumulator_deinit(&tmp_oldest_message);
   merge_accumulator_deinit(&out_message);

   return 0;
}

static transaction_data_config template_cfg = {
   .super = {.merge_tuples       = merge_mvcc_message,
             .merge_tuples_final = merge_mvcc_message_final},
};

void
transaction_data_config_init(data_config             *in_cfg, // IN
                             transaction_data_config *out_cfg // OUT
)
{
   memcpy(out_cfg, &template_cfg, sizeof(template_cfg));
   out_cfg->application_data_config = in_cfg;
}
