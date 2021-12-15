// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

#include "test_functionality.h"
#include "splinter.h"
#include "clockcache.h"
#include "rc_allocator.h"
#include "log.h"
#include "splinterdb/data.h"
#include "test.h"
#include "random.h"
#include "util.h"
#include "test_splinter_shadow.h"
#include "test_async.h"

#include "poison.h"

void
destroy_test_splinter_shadow_array(test_splinter_shadow_array *sharr)
{
   platform_buffer_destroy(sharr->buffer);
   sharr->nkeys = 0;
}

static void
verify_tuple(splinter_handle *spl,
             char *           keybuf,
             slice            message,
             int8             refcount,
             platform_status *result)
{
   const data_handle *msg   = slice_data(message);
   bool               found = msg != NULL;
   uint64             key   = be64toh(*(uint64 *)keybuf);

   if (msg && slice_length(message) < sizeof(data_handle)) {
      platform_error_log("ERROR: Short message of length %ld, key = 0x%08lx, ",
                         slice_length(message),
                         key);
      platform_assert(0);
   }

   if (refcount && !found) {

      platform_error_log(
         "ERROR: A key not found in Splinter which is present in shadow tree: "
         "key = 0x%08lx, "
         "shadow refcount = 0x%08x\n",
         key,
         refcount);
      *result = STATUS_NOT_FOUND;
      splinter_print_lookup(spl, keybuf);
      platform_assert(0);
   } else if (refcount == 0 && found && msg->ref_count != 0) {
      platform_error_log(
         "ERROR: A key found in the Splinter has refcount 0 in shadow tree. "
         "key = 0x%08lx, "
         "splinter flags = %d, "
         "splinter refcount = 0x%08x\n",
         key,
         msg->message_type,
         msg->ref_count);
      *result = STATUS_INVALID_STATE;
      splinter_print_lookup(spl, keybuf);
      platform_assert(0);
   } else if (found && msg->ref_count != refcount) {
      platform_error_log(
         "ERROR: Refcount mismatch between a key returned by Splinter"
         " (ref: %4d) and shadow tree (ref: %4d) key = 0x%08lx\n",
         msg->ref_count,
         refcount,
         key);
      *result = STATUS_INVALID_STATE;
      splinter_print_lookup(spl, keybuf);
      platform_assert(0);
   }
}

static void
verify_tuple_callback(splinter_handle *spl, test_async_ctxt *ctxt, void *arg)
{
   platform_status *result = arg;

   verify_tuple(spl,
                ctxt->key,
                writable_buffer_slice(&ctxt->data),
                ctxt->refcount,
                result);
}


/*
 *-----------------------------------------------------------------------------
 * verify_against_shadow --
 *
 *      Verify that keys in the shadow with non-zero refcounts are in splinter.
 *      Verify that keys in the shadow with zero refcounts are not in splinter.
 *
 * Results:
 *      Return STATUS_OK if successful, non-OK otherwise.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
platform_status
verify_against_shadow(splinter_handle *           spl,
                      char *                      keybuf,
                      test_splinter_shadow_array *sharr,
                      test_async_lookup *         async_lookup)
{
   uint64 key_size  = spl->cfg.data_cfg->key_size;
   uint64 data_size = spl->cfg.data_cfg->message_size;

   platform_assert(key_size >= sizeof(uint64));
   platform_assert(data_size >= sizeof(data_handle));
   platform_assert(sizeof(data_handle) <= sizeof(void *));

   platform_status rc, result = STATUS_OK;

   uint64 i;
   for (i = 0; i < sharr->nkeys; i++) {
      uint64           key      = sharr->keys[i];
      int8             refcount = sharr->ref_counts[i];
      test_async_ctxt *ctxt;
      writable_buffer  message;

      writable_buffer_create(&message, spl->heap_id);
      if (async_lookup) {
         ctxt = async_ctxt_get(async_lookup);
      } else {
         ctxt = NULL;
      }
      if (ctxt == NULL) {
         test_int_to_key(keybuf, key, key_size);

         rc = splinter_lookup(spl, keybuf, &message);
         if (!SUCCESS(rc)) {
            return rc;
         }
         slice message_slice = writable_buffer_slice(&message);
         verify_tuple(spl, keybuf, message_slice, refcount, &result);
      } else {
         test_int_to_key(ctxt->key, key, key_size);
         ctxt->refcount = refcount;
         async_ctxt_process_one(
            spl, async_lookup, ctxt, NULL, verify_tuple_callback, &result);
      }
      writable_buffer_reset_to_null(&message);
   }
   if (async_lookup) {
      // Rough detection of stuck contexts
      const timestamp ts = platform_get_timestamp();
      while (async_ctxt_process_ready(
         spl, async_lookup, NULL, verify_tuple_callback, &result))
      {
         cache_cleanup(spl->cc);
         platform_assert(platform_timestamp_elapsed(ts)
                         < TEST_STUCK_IO_TIMEOUT);
      }
   }

   return result;
}

/*
 * Verify that a range query in Splinter matches the corresponding
 * range in the shadow.
 */
platform_status
verify_range_against_shadow(splinter_handle *           spl,
                            test_splinter_shadow_array *sharr,
                            char *                      start_key,
                            char *                      end_key,
                            platform_heap_id            hid,
                            uint64                      start_index,
                            uint64                      end_index)
{
   platform_status    status;
   slice              splinter_keybuf;
   slice              splinter_message;
   const data_handle *splinter_data_handle;
   uint64             splinter_key;
   uint64             i;
   bool               at_end;

   platform_assert(start_index <= sharr->nkeys);
   platform_assert(end_index <= sharr->nkeys);

   splinter_range_iterator *range_itor = TYPED_MALLOC(hid, range_itor);
   platform_assert(range_itor != NULL);
   status = splinter_range_iterator_init(
      spl, range_itor, start_key, end_key, end_index - start_index);
   if (!SUCCESS(status)) {
      platform_error_log("failed to create range itor: %s\n",
                         platform_status_to_string(status));
      goto out;
   }

   for (i = start_index; i < end_index; i++) {
      uint64 shadow_key      = sharr->keys[i];
      int8   shadow_refcount = sharr->ref_counts[i];

      if (shadow_refcount == 0) {
         continue;
      }

      status = iterator_at_end((iterator *)range_itor, &at_end);
      if (!SUCCESS(status) || at_end) {
         platform_error_log(
            "ERROR: range itor failed or terminated early (at_end = %d): %s\n",
            at_end,
            platform_status_to_string(status));
         if (SUCCESS(status)) {
            status = STATUS_NO_PERMISSION;
         }
         goto destroy;
      }

      iterator_get_curr(
         (iterator *)range_itor, &splinter_keybuf, &splinter_message);
      splinter_key         = be64toh(*(uint64 *)slice_data(splinter_keybuf));
      splinter_data_handle = slice_data(splinter_message);

      // platform_log("Range test %d: Shadow: 0x%08lx, Tree: 0x%08lx\n",
      //   i,
      //   be64toh(*(uint64 *)shadow_key),
      //   be64toh(*(uint64 *)key_p));

      if (splinter_key == shadow_key) {
         status = STATUS_OK;
      } else {
         platform_error_log(
            "ERROR: Key mismatch: "
            "Shadow Key: 0x%08lx, Shadow Refcount: %3d, "
            "Tree Key: 0x%08lx, Tree Msg Type: 0x%02x, Tree Refcount: %3d\n",
            shadow_key,
            shadow_refcount,
            splinter_key,
            splinter_data_handle->message_type,
            splinter_data_handle->ref_count);
         platform_assert(0);
         status = STATUS_INVALID_STATE;
         goto destroy;
      }

      if (shadow_refcount == splinter_data_handle->ref_count) {
         status = STATUS_OK;
      } else {
         platform_error_log("ERROR: Refcount mismatch: "
                            "key: 0x%08lx, Shadow refcount: %3d, "
                            "Tree Msg Type: 0x%02x, Tree Refcount %3d\n",
                            shadow_key,
                            shadow_refcount,
                            splinter_data_handle->message_type,
                            splinter_data_handle->ref_count);
         splinter_print_lookup(spl, (char *)slice_data(splinter_keybuf));
         platform_assert(0);
         status = STATUS_INVALID_STATE;
         goto destroy;
      }

      status = iterator_advance((iterator *)range_itor);
      if (!SUCCESS(status)) {
         goto destroy;
      }
   }

   while (SUCCESS(iterator_at_end((iterator *)range_itor, &at_end)) && !at_end)
   {
      status = STATUS_LIMIT_EXCEEDED;
      iterator_get_curr(
         (iterator *)range_itor, &splinter_keybuf, &splinter_message);
      splinter_key = be64toh(*(uint64 *)slice_data(splinter_keybuf));

      platform_log("Range iterator EXTRA KEY: %08lx \n"
                   "Tree Msg Type: 0x%02x, Tree Refcount %3d\n",
                   splinter_key,
                   splinter_data_handle->message_type,
                   splinter_data_handle->ref_count);
      if (!SUCCESS(iterator_advance((iterator *)range_itor))) {
         goto destroy;
      }
   }

destroy:
   splinter_range_iterator_deinit(range_itor);

out:
   platform_free(hid, range_itor);

   return status;
}

#define VERIFY_RANGE_ENDPOINT_NULL  (1)
#define VERIFY_RANGE_ENDPOINT_MIN   (2)
#define VERIFY_RANGE_ENDPOINT_MAX   (3)
#define VERIFY_RANGE_ENDPOINT_RAND  (4)
#define VERIFY_RANGE_ENDPOINT_EQUAL (5)
#define VERIFY_RANGE_ENDPOINT_LESS  (6)

static void *
choose_key(data_config *               cfg,         // IN
           test_splinter_shadow_array *sharr,       // IN
           random_state *              prg,         // IN/OUT
           int                         type,        // IN
           bool                        is_start,    // IN
           void *                      startkey,    // IN
           int                         start_index, // IN
           int *                       index,       // OUT
           void *                      keybuf)                            // OUT
{
   uint64 num_keys = sharr->nkeys;

   switch (type) {
      case VERIFY_RANGE_ENDPOINT_NULL:
         *index = is_start ? 0 : num_keys;
         return NULL;
      case VERIFY_RANGE_ENDPOINT_MIN:
         *index = 0;
         return cfg->min_key;
      case VERIFY_RANGE_ENDPOINT_MAX:
         *index = num_keys;
         return cfg->max_key;
      case VERIFY_RANGE_ENDPOINT_RAND:
      {
         // Pick in the middle 3/5ths of the array
         uint64 pos;
         pos = num_keys / 5
               + (random_next_uint64(prg)
                  % ((num_keys < 5) ? (num_keys + 1) / 2 : (3 * num_keys / 5)));
         uint64 key = sharr->keys[pos];
         if (random_next_uint64(prg) % 2 && pos < sharr->nkeys) {
            key++;
            pos++;
         }
         *index = pos;
         test_int_to_key(keybuf, key, cfg->key_size);
         return keybuf;
      }
      case VERIFY_RANGE_ENDPOINT_EQUAL:
         platform_assert(!is_start && startkey);
         *index = start_index;
         return startkey;
      case VERIFY_RANGE_ENDPOINT_LESS:
         platform_assert(!is_start && startkey);
         *index = start_index ? (random_next_uint64(prg) % start_index) : 0;
         test_int_to_key(keybuf, sharr->keys[*index], cfg->key_size);
         return keybuf;
      default:
         platform_assert(0);
   }
   platform_assert(0);
   return NULL;
}

platform_status
verify_range_against_shadow_all_types(splinter_handle *           spl,
                                      random_state *              prg,
                                      test_splinter_shadow_array *sharr,
                                      platform_heap_id            hid,
                                      bool                        do_it)
{
   int             begin_type;
   int             end_type;
   platform_status rc, result = STATUS_OK;
   char            startkey_buf[MAX_KEY_SIZE];
   char            endkey_buf[MAX_KEY_SIZE];
   char *          start_key;
   char *          end_key;
   int             start_index;
   int             end_index;

   for (begin_type = VERIFY_RANGE_ENDPOINT_NULL;
        begin_type <= VERIFY_RANGE_ENDPOINT_RAND;
        begin_type++)
   {
      for (end_type = VERIFY_RANGE_ENDPOINT_NULL;
           end_type <= VERIFY_RANGE_ENDPOINT_RAND;
           end_type++)
      {
         start_key = choose_key(spl->cfg.data_cfg,
                                sharr,
                                prg,
                                begin_type,
                                1,
                                NULL,
                                0,
                                &start_index,
                                startkey_buf);
         end_key   = choose_key(spl->cfg.data_cfg,
                              sharr,
                              prg,
                              end_type,
                              0,
                              start_key,
                              start_index,
                              &end_index,
                              endkey_buf);
         if (do_it) {
            rc = verify_range_against_shadow(
               spl, sharr, start_key, end_key, hid, start_index, end_index);
            if (!SUCCESS(rc)) {
               result = rc;
            }
         }
      }
   }

   begin_type = VERIFY_RANGE_ENDPOINT_RAND;
   for (end_type = VERIFY_RANGE_ENDPOINT_EQUAL;
        end_type <= VERIFY_RANGE_ENDPOINT_LESS;
        end_type++)
   {
      start_key = choose_key(spl->cfg.data_cfg,
                             sharr,
                             prg,
                             begin_type,
                             1,
                             NULL,
                             0,
                             &start_index,
                             startkey_buf);
      end_key   = choose_key(spl->cfg.data_cfg,
                           sharr,
                           prg,
                           end_type,
                           0,
                           start_key,
                           start_index,
                           &end_index,
                           endkey_buf);
      if (do_it) {
         rc = verify_range_against_shadow(
            spl, sharr, start_key, end_key, hid, start_index, end_index);
         if (!SUCCESS(rc)) {
            result = rc;
         }
      }
   }

   return result;
}

static platform_status
validate_tree_against_shadow(splinter_handle *          spl,
                             random_state *             prg,
                             char *                     keybuf,
                             test_splinter_shadow_tree *shadow,
                             platform_heap_handle       hh,
                             platform_heap_id           hid,
                             bool                       do_it,
                             test_async_lookup *        async_lookup)
{
   test_splinter_shadow_array dry_run_sharr = {
      .nkeys = 1, .keys = (uint64[]){0}, .ref_counts = (int8[]){0}};
   test_splinter_shadow_array sharr;
   platform_status            rc = STATUS_OK;

   if (test_splinter_shadow_count(shadow) == 0) {
      return rc;
   }

   memset(&sharr, 0, sizeof(sharr));
   if (do_it) {
      rc = test_splinter_build_shadow_array(shadow, &sharr, hh);
      if (!SUCCESS(rc)) {
         // might need to cleanup a partially allocated shadow array.
         platform_error_log("Failed to build shadow array: %s\n",
                            platform_status_to_string(rc));
         return rc;
      }
   } else {
      memcpy(&sharr, &dry_run_sharr, sizeof(sharr));
   }

   rc = verify_against_shadow(spl, keybuf, &sharr, async_lookup);
   if (!SUCCESS(rc)) {
      platform_free(hid, async_lookup);
      platform_error_log("Failed to verify inserted items in Splinter: %s\n",
                         platform_status_to_string(rc));
      goto cleanup;
   }

   rc = verify_range_against_shadow_all_types(spl, prg, &sharr, hid, do_it);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to verify range iteration over inserted items "
                         "in Splinter: %s\n",
                         platform_status_to_string(rc));
      goto cleanup;
   }

cleanup:
   if (do_it) {
      destroy_test_splinter_shadow_array(&sharr);
   }

   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * Insert several messages of the given type into the splinter and the shadow --
 *
 * Results:
 *      Return 0 if all operations are successful.  Appropriate error code
 *      otherwise.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static platform_status
insert_random_messages(splinter_handle *          spl,
                       test_splinter_shadow_tree *shadow,
                       random_state *             prg,
                       char *                     keybuf,
                       data_handle *              msg,
                       int                        num_messages,
                       message_type               op,
                       uint64                     minkey,
                       uint64                     maxkey,
                       int64                      mindelta,
                       int64                      maxdelta)
{
   uint64 key_size  = spl->cfg.data_cfg->key_size;
   uint64 data_size = spl->cfg.data_cfg->message_size;

   platform_assert(key_size >= sizeof(uint64));
   platform_assert(data_size >= sizeof(data_handle));
   platform_assert(sizeof(data_handle) <= sizeof(void *));

   int             i;
   platform_status rc;
   uint64          key;

   key = minkey;
   for (i = 0; i < num_messages; i++) {
      // Generate a random message (op, key).
      // (the refcount field of our messages are always 1)
      key = minkey
            + (((key - minkey) + mindelta
                + (random_next_uint64(prg) % (maxdelta - mindelta + 1)))
               % (maxkey - minkey + 1));

      // Insert message into Splinter
      test_int_to_key(keybuf, key, key_size);

      msg->message_type = op;
      if (op != MESSAGE_TYPE_DELETE)
         msg->ref_count = ((int)(random_next_uint64(prg) % 256)) - 127;
      else
         msg->ref_count = 0;

      // if (key == 0x02f90065)
      //   platform_log("Inserting message: %8d OP=%d Key=0x%08lx Value=%8d\n",
      //   i, op, key, msg->ref_count);
      slice message = slice_create(data_size, msg);
      rc            = splinter_insert(spl, keybuf, message);
      if (!SUCCESS(rc)) {
         return rc;
      }

      // Now apply same operation to the shadow
      int8 new_refcount = msg->ref_count;
      if (op == MESSAGE_TYPE_UPDATE) {
         uint64 old_ref_count;
         if (test_splinter_shadow_lookup(shadow, &key, &old_ref_count)) {
            new_refcount = old_ref_count + msg->ref_count;
         }
      }

      rc = test_splinter_shadow_add(shadow, &key, new_refcount);
      if (!SUCCESS(rc)) {
         platform_error_log("Failed to insert key to shadow: %s\n",
                            platform_status_to_string(rc));
         return rc;
      }
   }

   return STATUS_OK;
}

int
cmp_ptrs(const void *a, const void *b)
{
   return a - b;
}

/*
 *-----------------------------------------------------------------------------
 * test_functionality --
 *
 * Randomly performs sequences of operations of the form
 *
 * - OP single random key
 * - OP many random keys over large or small intervals
 * - OP long or short sequential run of keys
 * - random point query
 * - random range query
 *
 * where OP is insert, delete, increment, or decrement.
 * Verifies the results against the shadow.
 *
 * The test does each of these operations for each of the "num_tables" passed
 * in as argument.
 *-----------------------------------------------------------------------------
 */
platform_status
test_functionality(allocator *          al,
                   io_handle *          io,
                   cache *              cc[],
                   splinter_config *    cfg,
                   uint64               seed,
                   uint64               num_inserts,
                   uint64               correctness_check_frequency,
                   task_system *        state,
                   platform_heap_handle hh,
                   platform_heap_id     hid,
                   uint8                num_tables,
                   uint8                num_caches,
                   uint32               max_async_inflight)
{
   platform_error_log("Functional test started with %d tables\n", num_tables);
   platform_assert(cc != NULL);

   splinter_handle **spl_tables = platform_aligned_malloc(
      hid, PLATFORM_CACHELINE_SIZE, sizeof(splinter_handle *) * num_tables);
   platform_assert(spl_tables != NULL);

   test_splinter_shadow_tree **shadows =
      platform_aligned_malloc(hid,
                              PLATFORM_CACHELINE_SIZE,
                              sizeof(test_splinter_shadow_tree *) * num_tables);
   platform_assert(shadows != NULL);

   allocator_root_id *splinters = platform_aligned_malloc(
      hid, PLATFORM_CACHELINE_SIZE, sizeof(allocator_root_id) * num_tables);
   platform_assert(splinters != NULL);
   test_async_lookup *async_lookup;
   if (max_async_inflight > 0) {
      async_ctxt_init(
         hid, max_async_inflight, cfg->data_cfg->message_size, &async_lookup);
   } else {
      async_lookup = NULL;
   }

   random_state    prg;
   platform_status status;
   char *          keybuf = NULL;
   data_handle *   msgbuf = NULL;

   random_init(&prg, seed, 0);

   // Initialize the splinter/shadow for each splinter table.
   for (uint8 idx = 0; idx < num_tables; idx++) {
      cache *cache_to_use = num_caches > 1 ? cc[idx] : *cc;
      status = test_splinter_shadow_create(&shadows[idx], hh, hid, num_inserts);
      if (!SUCCESS(status)) {
         platform_error_log("Failed to init shadow for splinter: %s\n",
                            platform_status_to_string(status));
         goto cleanup;
      }
      splinters[idx] = test_generate_allocator_root_id();

      spl_tables[idx] = splinter_create(
         &cfg[idx], al, cache_to_use, state, splinters[idx], hid);
      if (spl_tables[idx] == NULL) {
         status = STATUS_NO_MEMORY;
         platform_error_log("Sumpin failed.\n");
         goto cleanup;
      }
   }

   // Validate each tree against an empty shadow.
   for (uint8 idx = 0; idx < num_tables; idx++) {
      splinter_handle *          spl    = spl_tables[idx];
      test_splinter_shadow_tree *shadow = shadows[idx];
      status                            = validate_tree_against_shadow(
         spl, &prg, keybuf, shadow, hh, hid, TRUE, async_lookup);
      if (!SUCCESS(status)) {
         platform_error_log("Failed to validate empty tree against shadow: \
                            %s\n",
                            platform_status_to_string(status));
         goto cleanup;
      }
   }

   // Run the test
   uint64 i             = 0;
   uint64 total_inserts = 0;
   while (total_inserts < num_inserts) {
      int          randop;
      message_type op;
      int          num_messages;
      int          sign_mindelta;
      uint64       delta_range_size;
      int64        mindelta, maxdelta;
      int64        average_delta;
      uint64       minkey, maxkey;
      uint64       key_range_size;

      // We pick different operations with different probabilities.
      // This is choice from the probability space.
      randop = random_next_uint64(&prg) % 100;
      // Favor inserts
      if (randop < 80) {
         op = MESSAGE_TYPE_INSERT;
      } else if (randop < 90) {
         op = MESSAGE_TYPE_DELETE;
      } else {
         op = MESSAGE_TYPE_UPDATE;
      }

      // Numer of messages geometrically distributed.
      // Make num_messages not always a power of 2.
      num_messages = 1 << (2 * (random_next_uint64(&prg) % 10));
      num_messages = num_messages + (random_next_uint64(&prg) % num_messages);
      if (num_messages > num_inserts - total_inserts) {
         num_messages = num_inserts - total_inserts;
      }

      // Size of the deltas geometrically distributed.
      mindelta         = 1 << (random_next_uint64(&prg) % 26);
      delta_range_size = 1 << (2 * (random_next_uint64(&prg) % 13));
      sign_mindelta    = random_next_uint64(&prg) % 2;
      mindelta         = (2 * sign_mindelta - 1) * mindelta;
      maxdelta         = mindelta + delta_range_size;
      average_delta    = (mindelta + maxdelta) / 2;

      // minkey uniformly distributed.  size of key range geometrically
      // distributed.
      minkey         = random_next_uint64(&prg) % (1ULL << 25);
      key_range_size = delta_range_size > int64abs(num_messages * average_delta)
                          ? delta_range_size
                          : int64abs(num_messages * average_delta);
      key_range_size &= (1 << 26) - 1;
      maxkey = minkey + key_range_size;

      platform_log("Round i=%8lu, op=%2d, nummsgs = %8d, minkey=%8lu, "
                   "maxkey=%8lu, mindelta = %9ld, maxdelta=%9ld\n",
                   i,
                   op,
                   num_messages,
                   minkey,
                   maxkey,
                   mindelta,
                   maxdelta);

      // Run the main test loop for each table.
      for (uint8 idx = 0; idx < num_tables; idx++) {
         // cache *cache_to_use = num_caches > 1 ? cc[idx] : *cc;
         splinter_handle *          spl    = spl_tables[idx];
         test_splinter_shadow_tree *shadow = shadows[idx];
         // allocator_root_id spl_id = splinters[idx];

         keybuf = TYPED_ARRAY_MALLOC(hid, keybuf, spl->cfg.data_cfg->key_size);
         platform_assert(keybuf);
         msgbuf = TYPED_FLEXIBLE_STRUCT_MALLOC(
            hid, msgbuf, data, spl->cfg.data_cfg->message_size);
         platform_assert(msgbuf);

         status = insert_random_messages(spl,
                                         shadow,
                                         &prg,
                                         keybuf,
                                         msgbuf,
                                         num_messages,
                                         op,
                                         minkey,
                                         maxkey,
                                         mindelta,
                                         maxdelta);
         if (!SUCCESS(status)) {
            platform_error_log("Sumpin failed inserting messages: %s\n",
                               platform_status_to_string(status));
            goto cleanup;
         }

         status = validate_tree_against_shadow(
            spl,
            &prg,
            keybuf,
            shadow,
            hh,
            hid,
            correctness_check_frequency
               && (i % correctness_check_frequency) == 0,
            async_lookup);
         if (!SUCCESS(status)) {
            platform_log("Failed to validate tree against shadow: %s\n",
                         platform_status_to_string(status));
            goto cleanup;
         }

         /* if (correctness_check_frequency && i != 0 && */
         /*     (i % correctness_check_frequency) == 0) { */
         /*    platform_assert(splinter_verify_tree(spl)); */
         /*    platform_log("Dismount and remount\n"); */
         /*    rc_allocator_config *al_cfg  = ((rc_allocator *)al)->cfg; */
         /*    uint64 prev_root_addr = spl->root_addr; */
         /*    splinter_dismount(spl); */
         /*    rc_allocator_dismount((rc_allocator *)al); */
         /*    rc_allocator_mount((rc_allocator *)al, al_cfg, io, hh, hid, */
         /*                       platform_get_module_id()); */
         /*    spl = splinter_mount(&cfg[idx], al, cache_to_use, state, spl_id,
          */
         /*                         hid); */
         /*    spl_tables[idx] = spl; */
         /*    if (spl->root_addr != prev_root_addr) { */
         /*       platform_error_log("Mismatch in root addr across mount\n"); */
         /*       status = STATUS_TEST_FAILED; */
         /*       goto cleanup; */
         /*    } */
         /* } */
         platform_free(hid, keybuf);
         platform_free(hid, msgbuf);
      }

      total_inserts += num_messages;
      i++;
      // char key[splinter_key_size(spl)];
      // test_int_to_key(key, 0x0073bc3b, splinter_key_size(spl));
      // splinter_print_lookup(spl, key);
   }

   // Validate each tree against the shadow one last time.
   for (uint8 idx = 0; idx < num_tables; idx++) {
      splinter_handle *          spl    = spl_tables[idx];
      test_splinter_shadow_tree *shadow = shadows[idx];
      keybuf = TYPED_ARRAY_MALLOC(hid, keybuf, spl->cfg.data_cfg->key_size);
      platform_assert(keybuf);
      msgbuf = TYPED_FLEXIBLE_STRUCT_MALLOC(
         hid, msgbuf, data, spl->cfg.data_cfg->message_size);
      platform_assert(msgbuf);

      status = validate_tree_against_shadow(
         spl,
         &prg,
         keybuf,
         shadow,
         hh,
         hid,
         correctness_check_frequency
            && ((i - 1) % correctness_check_frequency) != 0,
         async_lookup);
      platform_free(hid, keybuf);
      platform_free(hid, msgbuf);
      if (!SUCCESS(status)) {
         platform_error_log("Failed to validate tree against shadow one \
                            last time: %s\n",
                            platform_status_to_string(status));
         goto cleanup;
      }
   }

cleanup:
   for (uint8 idx = 0; idx < num_tables; idx++) {
      if (spl_tables[idx] != NULL) {
         splinter_destroy(spl_tables[idx]);
      }
      if (shadows[idx] != NULL) {
         test_splinter_shadow_destroy(hid, shadows[idx]);
      }
   }

   if (async_lookup) {
      async_ctxt_deinit(hid, async_lookup);
   }
   platform_free(hid, spl_tables);
   platform_free(hid, splinters);
   platform_free(hid, shadows);
   return status;
}
