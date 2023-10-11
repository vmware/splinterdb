#include "experimental_mode.h"

#if EXPERIMENTAL_MODE_TICTOC_DISK

#   include "transactional_data_config.h"
#   include "data_internal.h"
#   include "transaction_impl/tictoc_disk_internal.h"
#   include <string.h>

static inline bool
is_message_rts_update(message msg)
{
   return message_length(msg) == sizeof(txn_timestamp);
}

static inline bool
is_merge_accumulator_rts_update(merge_accumulator *ma)
{
   return merge_accumulator_length(ma) == sizeof(txn_timestamp);
}

static inline message
get_app_value_from_message(message msg)
{
   return message_create(
      message_class(msg),
      slice_create(message_length(msg) - sizeof(tuple_header),
                   message_data(msg) + sizeof(tuple_header)));
}

static inline message
get_app_value_from_merge_accumulator(merge_accumulator *ma)
{
   return message_create(
      merge_accumulator_message_class(ma),
      slice_create(merge_accumulator_length(ma) - sizeof(tuple_header),
                   merge_accumulator_data(ma) + sizeof(tuple_header)));
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
      txn_timestamp new_rts =
         *((txn_timestamp *)merge_accumulator_data(new_message));
      merge_accumulator_copy_message(new_message, old_message);
      tuple_header *new_tuple =
         (tuple_header *)merge_accumulator_data(new_message);
      new_tuple->ts.rts = new_rts;

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
      ((const transactional_data_config *)cfg)->application_data_config,
      key_create_from_slice(key),
      old_value_message,
      &new_value_ma);

   merge_accumulator_resize(new_message,
                            sizeof(tuple_header)
                               + merge_accumulator_length(&new_value_ma));

   tuple_header *new_tuple = merge_accumulator_data(new_message);
   memcpy(&new_tuple->value,
          merge_accumulator_data(&new_value_ma),
          merge_accumulator_length(&new_value_ma));

   merge_accumulator_deinit(&new_value_ma);

   merge_accumulator_set_class(new_message, message_class(old_message));

   return 0;
}

static int
merge_tictoc_tuple_final(const data_config *cfg,
                         slice              key,
                         merge_accumulator *oldest_message)
{
   platform_assert(!is_merge_accumulator_rts_update(oldest_message),
                   "oldest_message shouldn't be a rts update\n");

   message oldest_message_value =
      get_app_value_from_merge_accumulator(oldest_message);
   merge_accumulator app_oldest_message;
   merge_accumulator_init_from_message(
      &app_oldest_message,
      app_oldest_message.data.heap_id, // FIXME: use a correct heap id
      oldest_message_value);

   data_merge_tuples_final(
      ((const transactional_data_config *)cfg)->application_data_config,
      key_create_from_slice(key),
      &app_oldest_message);

   merge_accumulator_resize(oldest_message,
                            sizeof(tuple_header)
                               + merge_accumulator_length(&app_oldest_message));
   tuple_header *tuple = merge_accumulator_data(oldest_message);
   memcpy(&tuple->value,
          merge_accumulator_data(&app_oldest_message),
          merge_accumulator_length(&app_oldest_message));

   merge_accumulator_deinit(&app_oldest_message);

   return 0;
}

void
transactional_data_config_init(data_config               *in_cfg, // IN
                               transactional_data_config *out_cfg // OUT
)
{
   memcpy(&out_cfg->super, in_cfg, sizeof(out_cfg->super));
   out_cfg->super.merge_tuples       = merge_tictoc_tuple;
   out_cfg->super.merge_tuples_final = merge_tictoc_tuple_final;
   out_cfg->application_data_config  = in_cfg;
}

#elif EXPERIMENTAL_MODE_STO_DISK

#   include "transactional_data_config.h"
#   include "data_internal.h"
#   include "transaction_impl/sto_disk_internal.h"
#   include <string.h>

static inline bool
is_message_rts_update(message msg)
{
   tuple_header *tuple = (tuple_header *)message_data(msg);
   return tuple->ts.magic == TIMESTAMP_UPDATE_MAGIC && ((tuple->ts.type & TIMESTAMP_UPDATE_RTS) == TIMESTAMP_UPDATE_RTS);
}

static inline bool
is_message_wts_update(message msg)
{
   tuple_header *tuple = (tuple_header *)message_data(msg);
   return tuple->ts.magic == TIMESTAMP_UPDATE_MAGIC && ((tuple->ts.type & TIMESTAMP_UPDATE_WTS) == TIMESTAMP_UPDATE_WTS);
}

static inline bool
is_merge_accumulator_rts_update(merge_accumulator *ma)
{
   tuple_header *tuple = (tuple_header *)merge_accumulator_data(ma);
   return tuple->ts.magic == TIMESTAMP_UPDATE_MAGIC && ((tuple->ts.type & TIMESTAMP_UPDATE_RTS) == TIMESTAMP_UPDATE_RTS);
}

static inline bool
is_merge_accumulator_wts_update(merge_accumulator *ma)
{
   tuple_header *tuple = (tuple_header *)merge_accumulator_data(ma);
   return tuple->ts.magic == TIMESTAMP_UPDATE_MAGIC && ((tuple->ts.type & TIMESTAMP_UPDATE_WTS) == TIMESTAMP_UPDATE_WTS);
}

static inline message
get_app_value_from_message(message msg)
{
   return message_create(
      message_class(msg),
      slice_create(message_length(msg) - sizeof(tuple_header),
                   message_data(msg) + sizeof(tuple_header)));
}

static inline message
get_app_value_from_merge_accumulator(merge_accumulator *ma)
{
   return message_create(
      merge_accumulator_message_class(ma),
      slice_create(merge_accumulator_length(ma) - sizeof(tuple_header),
                   merge_accumulator_data(ma) + sizeof(tuple_header)));
}

static int
merge_sto_tuple(const data_config *cfg,
                slice              key,         // IN
                message            old_message, // IN
                merge_accumulator *new_message) // IN/OUT
{
   // old message is timestamp updates --> merge the timestamp to new message
   if (is_message_rts_update(old_message) || is_message_wts_update(old_message)) {
      timestamp_set *old_ts = (timestamp_set *)message_data(old_message);
      timestamp_set *new_ts = (timestamp_set *)merge_accumulator_data(new_message);
      if (is_message_rts_update(old_message))
      {
         new_ts->rts = MAX(new_ts->rts, old_ts->rts);
      }
      
      if (is_message_wts_update(old_message))
      {
         new_ts->wts = MAX(new_ts->wts, old_ts->wts);
      }

      if (is_merge_accumulator_rts_update(new_message) || is_merge_accumulator_wts_update(new_message))
      {
         new_ts->magic = TIMESTAMP_UPDATE_MAGIC;
         new_ts->type = new_ts->type | new_ts->type;
      } else {
         new_ts->magic = 0;
         new_ts->type = 0;
      }

      return 0;
   }

   // old message is not timestamp updates, but new message is timestamp updates.
   if (is_merge_accumulator_rts_update(new_message)) {
      timestamp_set new_ts =
         *((timestamp_set *)merge_accumulator_data(new_message));
      merge_accumulator_copy_message(new_message, old_message);
      timestamp_set *new_tuple =
         (timestamp_set *)merge_accumulator_data(new_message);
      new_tuple->rts = new_ts.rts;
      new_tuple->magic = 0;

      return 0;
   }

   if (is_merge_accumulator_wts_update(new_message)) {
      timestamp_set new_ts =
         *((timestamp_set *)merge_accumulator_data(new_message));
      merge_accumulator_copy_message(new_message, old_message);
      timestamp_set *new_tuple =
         (timestamp_set *)merge_accumulator_data(new_message);
      new_tuple->wts = new_ts.wts;
      new_tuple->magic = 0;

      return 0;
   }

   // both are not timestamp updates.
   message old_value_message = get_app_value_from_message(old_message);
   message new_value_message =
      get_app_value_from_merge_accumulator(new_message);

   merge_accumulator new_value_ma;
   merge_accumulator_init_from_message(
      &new_value_ma,
      new_message->data.heap_id,
      new_value_message); // FIXME: use a correct heap_id

   data_merge_tuples(
      ((const transactional_data_config *)cfg)->application_data_config,
      key_create_from_slice(key),
      old_value_message,
      &new_value_ma);

   merge_accumulator_resize(new_message,
                            sizeof(tuple_header)
                               + merge_accumulator_length(&new_value_ma));

   tuple_header *new_tuple = merge_accumulator_data(new_message);
   memcpy(&new_tuple->value,
          merge_accumulator_data(&new_value_ma),
          merge_accumulator_length(&new_value_ma));

   merge_accumulator_deinit(&new_value_ma);

   merge_accumulator_set_class(new_message, message_class(old_message));

   return 0;
}

static int
merge_sto_tuple_final(const data_config *cfg,
                      slice              key,
                      merge_accumulator *oldest_message)
{
   // platform_assert(!is_merge_accumulator_rts_update(oldest_message)
   //                    && !is_merge_accumulator_wts_update(oldest_message),
   //                 "oldest_message shouldn't be a rts/wts update\n");

   if (!is_merge_accumulator_rts_update(oldest_message)
                      && !is_merge_accumulator_wts_update(oldest_message)) {
      // TODO
      // The transaction inserts timestamps during execution, but it is aborted.
      // So, the record remains in the database.
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
      ((const transactional_data_config *)cfg)->application_data_config,
      key_create_from_slice(key),
      &app_oldest_message);

   merge_accumulator_resize(oldest_message,
                            sizeof(tuple_header)
                               + merge_accumulator_length(&app_oldest_message));
   tuple_header *tuple = merge_accumulator_data(oldest_message);
   memcpy(&tuple->value,
          merge_accumulator_data(&app_oldest_message),
          merge_accumulator_length(&app_oldest_message));

   merge_accumulator_deinit(&app_oldest_message);

   return 0;
}

void
transactional_data_config_init(data_config               *in_cfg, // IN
                               transactional_data_config *out_cfg // OUT
)
{
   memcpy(&out_cfg->super, in_cfg, sizeof(out_cfg->super));
   out_cfg->super.merge_tuples       = merge_sto_tuple;
   out_cfg->super.merge_tuples_final = merge_sto_tuple_final;
   out_cfg->application_data_config  = in_cfg;
}

#endif
