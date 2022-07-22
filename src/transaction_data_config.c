#include "transaction_data_config.h"
#include "data_internal.h"
#include "tictoc_data.h"
#include <string.h>

static inline bool
is_message_rts_update(message msg)
{
   return message_length(msg) == sizeof(tictoc_timestamp);
}

static inline bool
is_merge_accumulator_rts_update(merge_accumulator *ma)
{
   return merge_accumulator_length(ma) == sizeof(tictoc_timestamp);
}

static inline message
get_app_value_from_message(message msg)
{
   return message_create(
      message_class(msg),
      slice_create(message_length(msg) - tictoc_tuple_header_size(),
                   message_data(msg) + tictoc_tuple_header_size()));
}

static inline message
get_app_value_from_merge_accumulator(merge_accumulator *ma)
{
   return message_create(
      merge_accumulator_message_class(ma),
      slice_create(merge_accumulator_length(ma) - tictoc_tuple_header_size(),
                   merge_accumulator_data(ma) + tictoc_tuple_header_size()));
}

static int
merge_tictoc_tuple(const data_config *cfg,
                   slice              key,         // IN
                   message            old_message, // IN
                   merge_accumulator *new_message) // IN/OUT
{
   if (is_message_rts_update(old_message)) {
      // Just discard
      return 0;
   }

   if (is_merge_accumulator_rts_update(new_message)) {
      tictoc_tuple *new_tuple = merge_accumulator_data(new_message);
      timestamp     new_rts   = new_tuple->ts_word.rts;
      merge_accumulator_copy_message(new_message, old_message);
      new_tuple = (tictoc_tuple *)merge_accumulator_data(new_message);
      new_tuple->ts_word.rts = new_rts;

      return 0;
   }

   message old_value_message = get_app_value_from_message(old_message);
   message new_value_message =
      get_app_value_from_merge_accumulator(new_message);

   merge_accumulator new_value_ma;
   merge_accumulator_init_from_message(
      &new_value_ma,
      new_message->data.heap_id,
      new_value_message); // FIXME: use a correct heap_id

   data_merge_tuples(
      ((const transaction_data_config *)cfg)->application_data_config,
      key,
      old_value_message,
      &new_value_ma);

   merge_accumulator_resize(new_message,
                            tictoc_tuple_header_size()
                               + merge_accumulator_length(&new_value_ma));

   tictoc_tuple *new_tuple = merge_accumulator_data(new_message);
   memcpy(&new_tuple->value,
          merge_accumulator_data(&new_value_ma),
          merge_accumulator_length(&new_value_ma));

   merge_accumulator_deinit(&new_value_ma);

   return 0;
}

static int
merge_tictoc_tuple_final(const data_config *cfg,
                         slice              key,
                         merge_accumulator *oldest_message)
{
   if (is_merge_accumulator_rts_update(oldest_message)) {
      // Do nothing
      return 0;
   }

   message oldest_message_value =
      get_app_value_from_merge_accumulator(oldest_message);
   merge_accumulator app_oldest_message;
   merge_accumulator_init_from_message(
      &app_oldest_message,
      app_oldest_message.data.heap_id, // FIXME: use a correct heap id
      oldest_message_value);

   data_merge_tuples_final(
      ((const transaction_data_config *)cfg)->application_data_config,
      key,
      &app_oldest_message);

   merge_accumulator_resize(oldest_message,
                            tictoc_tuple_header_size()
                               + merge_accumulator_length(&app_oldest_message));
   tictoc_tuple *tuple = merge_accumulator_data(oldest_message);
   memcpy(&tuple->value,
          merge_accumulator_data(&app_oldest_message),
          merge_accumulator_length(&app_oldest_message));

   merge_accumulator_deinit(&app_oldest_message);

   return 0;
}

/* static transaction_data_config template_cfg = { */
/*    .super = {.merge_tuples       = merge_tictoc_tuple, */
/*              .merge_tuples_final = merge_tictoc_tuple_final}, */
/* }; */

void
transaction_data_config_init(data_config             *in_cfg, // IN
                             transaction_data_config *out_cfg // OUT
)
{
   memcpy(&out_cfg->super, in_cfg, sizeof(out_cfg->super));
   out_cfg->super.merge_tuples       = merge_tictoc_tuple;
   out_cfg->super.merge_tuples_final = merge_tictoc_tuple_final;
   out_cfg->application_data_config  = in_cfg;
}
