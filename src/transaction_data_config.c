#include "transaction_data_config.h"
#include "data_internal.h"
#include "tictoc_data.h"
#include <string.h>

static int
merge_tictoc_tuple(const data_config *cfg,
                   slice              key,         // IN
                   message            old_message, // IN
                   merge_accumulator *new_message) // IN/OUT
{
   const transaction_data_config *tcfg = (const transaction_data_config *)cfg;

   uint64 old_message_size = message_length(old_message);
   uint64 new_message_size = merge_accumulator_length(new_message);

   tictoc_tuple *old_tuple = (tictoc_tuple *)message_data(old_message);
   tictoc_tuple *new_tuple =
      (tictoc_tuple *)merge_accumulator_data(new_message);

   if (old_message_size == sizeof(tictoc_timestamp)) {
      memcpy(&new_tuple->ts_word.rts,
             message_data(old_message),
             sizeof(tictoc_timestamp));
   } else if (new_message_size == sizeof(tictoc_timestamp)) {
      tictoc_timestamp_set ts_word;
      ts_word.rts = MAX(old_tuple->ts_word.rts, new_tuple->ts_word.rts);
      ts_word.wts = new_tuple->ts_word.wts;

      merge_accumulator_resize(new_message, message_length(old_message));
      new_tuple = merge_accumulator_data(new_message);
      uint64 value_size =
         message_length(old_message) - sizeof(tictoc_timestamp_set);
      memcpy(&new_tuple->ts_word, &ts_word, sizeof(ts_word));
      memcpy(new_tuple->value, old_tuple->value, value_size);
   } else {
      merge_accumulator new_value_ma;
      merge_accumulator_init(
         &new_value_ma,
         new_message->data.heap_id); // FIXME: use a correct heap_id

      uint64 old_message_value_size =
         old_message_size - sizeof(tictoc_timestamp_set);
      uint64 new_message_value_size =
         new_message_size - sizeof(tictoc_timestamp_set);

      merge_accumulator_resize(&new_value_ma, new_message_value_size);

      message old_value_message =
         message_create(message_class(old_message),
                        slice_create(old_message_value_size, old_tuple->value));
      message new_value_message =
         message_create(merge_accumulator_message_class(new_message),
                        slice_create(new_message_value_size, new_tuple->value));
      merge_accumulator_copy_message(&new_value_ma, new_value_message);
      data_merge_tuples(
         tcfg->application_data_config, key, old_value_message, &new_value_ma);

      tictoc_timestamp_set ts_word;
      ts_word.rts = MAX(old_tuple->ts_word.rts, new_tuple->ts_word.rts);
      ts_word.wts = MAX(old_tuple->ts_word.wts, new_tuple->ts_word.wts);

      merge_accumulator_resize(new_message,
                               sizeof(ts_word)
                                  + merge_accumulator_length(&new_value_ma));

      new_tuple = merge_accumulator_data(new_message);
      memcpy(&new_tuple->ts_word, &ts_word, sizeof(ts_word));
      memcpy(&new_tuple->value,
             merge_accumulator_data(&new_value_ma),
             merge_accumulator_length(&new_value_ma));

      merge_accumulator_deinit(&new_value_ma);
   }

   return 0;
}

static int
merge_tictoc_tuple_final(const data_config *cfg,
                         slice              key,
                         merge_accumulator *oldest_message)
{
   const transaction_data_config *tcfg = (const transaction_data_config *)cfg;

   uint64 oldest_message_size = merge_accumulator_length(oldest_message);

   if (oldest_message_size == sizeof(tictoc_timestamp)) {
      // Do nothing
      return 0;
   }

   tictoc_tuple *tuple = merge_accumulator_data(oldest_message);

   merge_accumulator app_oldest_message;
   merge_accumulator_init(&app_oldest_message, app_oldest_message.data.heap_id);

   uint64 oldest_message_value_size =
      oldest_message_size - sizeof(tictoc_timestamp_set);
   merge_accumulator_resize(&app_oldest_message, oldest_message_value_size);

   message oldest_message_value =
      message_create(merge_accumulator_message_class(oldest_message),
                     slice_create(oldest_message_value_size, tuple->value));
   merge_accumulator_copy_message(&app_oldest_message, oldest_message_value);

   data_merge_tuples_final(
      tcfg->application_data_config, key, &app_oldest_message);

   tictoc_timestamp_set ts_word = tuple->ts_word;

   uint64 new_tuple_size =
      sizeof(ts_word) + merge_accumulator_length(&app_oldest_message);

   merge_accumulator_resize(oldest_message, new_tuple_size);
   tuple = merge_accumulator_data(oldest_message);

   memcpy(&tuple->ts_word, &ts_word, sizeof(ts_word));
   memcpy(&tuple->value,
          merge_accumulator_data(&app_oldest_message),
          new_tuple_size - sizeof(ts_word));

   merge_accumulator_deinit(&app_oldest_message);

   return 0;
}

static transaction_data_config template_cfg = {
   .super = {.merge_tuples       = merge_tictoc_tuple,
             .merge_tuples_final = merge_tictoc_tuple_final},
};

void
transaction_data_config_init(data_config             *in_cfg, // IN
                             transaction_data_config *out_cfg // OUT
)
{
   memcpy(out_cfg, &template_cfg, sizeof(template_cfg));
   out_cfg->application_data_config = in_cfg;
}
