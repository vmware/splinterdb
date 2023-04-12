// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * btree_test.c --
 *
 *     This file contains the test interfaces for Alex's B-tree.
 */
#include "platform.h"

#include "splinterdb/data.h"
#include "btree.h"
#include "merge.h"
#include "test.h"
#include "io.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "task.h"

#define TEST_MAX_ASYNC_INFLIGHT 63
static uint64 max_async_inflight = 32;

#include "poison.h"

typedef struct test_btree_config {
   memtable_config        *mt_cfg;
   test_key_type           type;
   uint64                  semiseq_freq;
   uint64                  period;
   test_message_generator *msggen;
} test_btree_config;

typedef struct test_memtable_context {
   cache             *cc;
   test_btree_config *cfg;
   platform_heap_id   heap_id;
   memtable_context  *mt_ctxt;
   uint64             max_generation;
} test_memtable_context;

btree_config *
test_memtable_context_btree_config(test_memtable_context *ctxt)
{
   return ctxt->cfg->mt_cfg->btree_cfg;
}

void
test_btree_process_noop(void *arg, uint64 generation)
{
   // really a no-op
}

test_memtable_context *
test_memtable_context_create(cache             *cc,
                             test_btree_config *cfg,
                             uint64             num_mt,
                             platform_heap_id   hid)
{
   test_memtable_context *ctxt = TYPED_ZALLOC(hid, ctxt);
   platform_assert(ctxt);
   ctxt->cc      = cc;
   ctxt->cfg     = cfg;
   ctxt->heap_id = hid;
   ctxt->mt_ctxt = memtable_context_create(
      hid, cc, cfg->mt_cfg, test_btree_process_noop, NULL);
   ctxt->max_generation = num_mt;
   return ctxt;
}

void
test_memtable_context_destroy(test_memtable_context *ctxt, platform_heap_id hid)
{
   memtable_context_destroy(hid, ctxt->mt_ctxt);
   platform_free(hid, ctxt);
}

platform_status
test_btree_insert(test_memtable_context *ctxt, key tuple_key, message data)
{
   uint64          generation;
   page_handle    *lock_page = NULL;
   platform_status rc        = memtable_maybe_rotate_and_get_insert_lock(
      ctxt->mt_ctxt, &generation, &lock_page);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (generation >= ctxt->max_generation) {
      rc = STATUS_NO_SPACE;
      goto out;
   }

   // dummy leaf generation (unused in this test)
   uint64 dummy_leaf_generation;
   rc = memtable_insert(ctxt->mt_ctxt,
                        &ctxt->mt_ctxt->mt[generation],
                        ctxt->heap_id,
                        tuple_key,
                        data,
                        &dummy_leaf_generation);

out:
   memtable_unget_insert_lock(ctxt->mt_ctxt, lock_page);
   return rc;
}

bool
test_btree_lookup(cache           *cc,
                  btree_config    *cfg,
                  platform_heap_id hid,
                  uint64           root_addr,
                  key              target,
                  message          expected_data)
{
   platform_status   rc;
   merge_accumulator result;
   bool              ret;

   merge_accumulator_init(&result, hid);

   rc = btree_lookup(cc, cfg, root_addr, PAGE_TYPE_MEMTABLE, target, &result);
   platform_assert_status_ok(rc);

   message data = merge_accumulator_to_message(&result);

   if (message_is_null(data) || message_is_null(expected_data)) {
      ret = message_is_null(data) == message_is_null(expected_data);
   } else {
      ret = message_lex_cmp(data, expected_data) == 0;
   }

   merge_accumulator_deinit(&result);
   return ret;
}

bool
test_memtable_lookup(test_memtable_context *ctxt,
                     uint64                 mt_no,
                     key                    target,
                     message                expected_data)
{
   btree_config *btree_cfg = test_memtable_context_btree_config(ctxt);
   uint64        root_addr = ctxt->mt_ctxt->mt[mt_no].root_addr;
   cache        *cc        = ctxt->cc;
   return test_btree_lookup(
      cc, btree_cfg, ctxt->heap_id, root_addr, target, expected_data);
}

void
test_btree_tuple(test_memtable_context *ctxt,
                 key_buffer            *keybuf,
                 merge_accumulator     *data,
                 uint64                 seq,
                 uint64                 thread_id)
{
   test_btree_config *cfg = ctxt->cfg;
   key_buffer_resize(keybuf, cfg->mt_cfg->btree_cfg->data_cfg->max_key_size);
   test_key(keybuf,
            cfg->type,
            seq,
            thread_id,
            cfg->semiseq_freq,
            cfg->mt_cfg->btree_cfg->data_cfg->max_key_size,
            cfg->period);

   if (data != NULL) {
      generate_test_message(cfg->msggen, seq, data);
   }
}

typedef struct test_btree_thread_params {
   test_memtable_context *ctxt;
   test_btree_config     *cfg;
   uint64                 num_ops;
   platform_thread        thread;
   uint64                 thread_id;
   uint64                 time_elapsed;
   platform_status        rc;
} test_btree_thread_params;

void
test_btree_insert_thread(void *arg)
{
   test_btree_thread_params *params      = (test_btree_thread_params *)arg;
   test_memtable_context    *ctxt        = params->ctxt;
   uint64                    thread_id   = params->thread_id;
   uint64                    num_inserts = params->num_ops;

   uint64 start_time = platform_get_timestamp();
   DECLARE_AUTO_KEY_BUFFER(keybuf, ctxt->heap_id);
   merge_accumulator data;

   merge_accumulator_init(&data, ctxt->heap_id);

   uint64 start_num = thread_id * num_inserts;
   uint64 end_num   = (thread_id + 1) * num_inserts;
   for (uint64 insert_num = start_num; insert_num < end_num; insert_num++) {
      test_btree_tuple(ctxt, &keybuf, &data, insert_num, thread_id);
      platform_status rc = test_btree_insert(
         ctxt, key_buffer_key(&keybuf), merge_accumulator_to_message(&data));
      if (!SUCCESS(rc)) {
         params->rc = rc;
         goto out;
      }
   }

out:
   params->time_elapsed = platform_timestamp_elapsed(start_time);
   merge_accumulator_deinit(&data);
}

static platform_status
test_btree_perf(cache             *cc,
                test_btree_config *cfg,
                uint64             num_inserts,
                uint64             num_threads,
                uint64             num_trees,
                task_system       *ts,
                platform_heap_id   hid)
{
   platform_default_log("btree_test: btree performance test started\n");

   test_memtable_context *ctxt =
      test_memtable_context_create(cc, cfg, num_trees, hid);

   uint64          num_inserts_per_thread = num_inserts / num_threads;
   platform_status ret                    = STATUS_OK;

   platform_memfrag          memfrag_params = {0};
   platform_memfrag         *mf             = &memfrag_params;
   test_btree_thread_params *params =
      TYPED_ARRAY_ZALLOC(hid, params, num_threads);
   platform_assert(params);

   for (uint64 thread_no = 0; thread_no < num_threads; thread_no++) {
      params[thread_no].ctxt      = ctxt;
      params[thread_no].cfg       = cfg;
      params[thread_no].num_ops   = num_inserts_per_thread;
      params[thread_no].thread_id = thread_no;
   }

   for (uint64 thread_no = 0; thread_no < num_threads; thread_no++) {
      ret = task_thread_create("insert thread",
                               test_btree_insert_thread,
                               &params[thread_no],
                               0,
                               ts,
                               hid,
                               &params[thread_no].thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }

   for (uint64 thread_no = 0; thread_no < num_threads; thread_no++) {
      platform_thread_join(params[thread_no].thread);
   }

   for (uint64 thread_no = 0; thread_no < num_threads; thread_no++) {
      if (!SUCCESS(params[thread_no].rc)) {
         ret = params[thread_no].rc;
         goto destroy_btrees;
      }
   }

   uint64 total_inserts = num_inserts_per_thread * num_threads;
   uint64 total_time    = 0;
   for (uint64 thread_no = 0; thread_no < num_threads; thread_no++) {
      total_time += params[thread_no].time_elapsed;
   }
   uint64 average_time = total_time / num_threads;
   platform_default_log("btree per-thread insert time per tuple %luns\n",
                        total_time / total_inserts);
   platform_default_log("btree total insertion rate: %lu insertions/second\n",
                        SEC_TO_NSEC(total_inserts) / average_time);

   memtable_print_stats(Platform_default_log_handle, cc, &ctxt->mt_ctxt->mt[0]);
   // for (i = 0; i < num_trees; i++) {
   //   if (!btree_verify_tree(cc, cfg, root_addr[i]))
   //      btree_print_tree(cc, cfg, root_addr[i], PAGE_TYPE_BRANCH);
   //}

   // for (i = 0; i < num_threads; i++) {
   //   ret = task_thread_create("lookup thread", test_btree_lookup_thread,
   //   &params[i]); if (ret < 0) {
   //      return EPERM;
   //   }
   //}
   // for (i = 0; i < num_threads; i++) {
   //   platform_semaphore_wait(&sema);
   //}

destroy_btrees:
   test_memtable_context_destroy(ctxt, hid);

   if (SUCCESS(ret)) {
      platform_default_log("btree_test: btree performance test succeeded\n");
   } else {
      platform_default_log("btree_test: btree performance test failed\n");
   }
   platform_default_log("\n");

   platform_free(hid, mf);

   return ret;
}

// A single async context
typedef struct {
   btree_async_ctxt  ctxt;
   cache_async_ctxt  cache_ctxt;
   bool              ready;
   key_buffer        keybuf;
   merge_accumulator result;
} btree_test_async_ctxt;

// Per-table array of async contexts
typedef struct {
   // 1: async index is available, 0: async index is used
   uint64                ctxt_bitmap;
   btree_test_async_ctxt ctxt[TEST_MAX_ASYNC_INFLIGHT];
} btree_test_async_lookup;

static void
btree_test_async_callback(btree_async_ctxt *btree_ctxt)
{
   btree_test_async_ctxt *ctxt =
      container_of(btree_ctxt, btree_test_async_ctxt, ctxt);

   //   platform_default_log("%s:%d tid %2lu: ctxt %p callback rcvd\n",
   //                __FILE__, __LINE__, platform_get_tid(), ctxt);
   platform_assert(!ctxt->ready);
   /*
    * A real application callback will need to requeue the async
    * lookup back to the per-thread async task queue. But our test
    * just retries anything that's ready, so this callback is simple.
    */
   ctxt->ready = TRUE;
}

static btree_test_async_ctxt *
btree_test_get_async_ctxt(btree_config            *cfg,
                          platform_heap_id         hid,
                          btree_test_async_lookup *async_lookup)
{
   btree_test_async_ctxt *ctxt;
   int                    idx;
   uint64                 old = async_lookup->ctxt_bitmap;

   idx = __builtin_ffsl(old);
   if (idx == 0) {
      return NULL;
   }
   idx                       = idx - 1;
   async_lookup->ctxt_bitmap = old & ~(1UL << idx);
   ctxt                      = &async_lookup->ctxt[idx];
   btree_ctxt_init(&ctxt->ctxt, &ctxt->cache_ctxt, btree_test_async_callback);
   ctxt->ready = FALSE;
   key_buffer_init(&ctxt->keybuf, hid);
   merge_accumulator_init(&ctxt->result, hid);

   return ctxt;
}

static void
btree_test_put_async_ctxt(btree_test_async_lookup *async_lookup,
                          btree_test_async_ctxt   *ctxt)
{
   int idx = ctxt - async_lookup->ctxt;

   debug_assert(idx >= 0 && idx < max_async_inflight);
   key_buffer_deinit(&ctxt->keybuf);
   merge_accumulator_deinit(&ctxt->result);
   async_lookup->ctxt_bitmap |= (1UL << idx);
}

static void
btree_test_async_ctxt_init(btree_test_async_lookup *async_lookup)
{
   _Static_assert(8 * sizeof(async_lookup->ctxt_bitmap)
                     > TEST_MAX_ASYNC_INFLIGHT,
                  "Not enough bits for bitmap");
   async_lookup->ctxt_bitmap = (1UL << max_async_inflight) - 1;
}

static bool
btree_test_async_ctxt_is_used(const btree_test_async_lookup *async_lookup,
                              int                            ctxt_idx)
{
   debug_assert(ctxt_idx >= 0 && ctxt_idx < max_async_inflight);
   return async_lookup->ctxt_bitmap & (1UL << ctxt_idx) ? FALSE : TRUE;
}

static bool
btree_test_async_ctxt_any_used(const btree_test_async_lookup *async_lookup)
{
   debug_assert((async_lookup->ctxt_bitmap & ~((1UL << max_async_inflight) - 1))
                == 0);
   return async_lookup->ctxt_bitmap != (1UL << max_async_inflight) - 1;
}

static bool
btree_test_run_pending(cache                   *cc,
                       btree_config            *cfg,
                       uint64                   root_addr,
                       btree_test_async_lookup *async_lookup,
                       btree_test_async_ctxt   *skip_ctxt,
                       bool                     expected_found)
{
   int i;

   if (!btree_test_async_ctxt_any_used(async_lookup)) {
      return FALSE;
   }
   for (i = 0; i < max_async_inflight; i++) {
      if (!btree_test_async_ctxt_is_used(async_lookup, i)) {
         continue;
      }
      cache_async_result     res;
      btree_test_async_ctxt *ctxt = &async_lookup->ctxt[i];
      // We skip skip_ctxt, because that it just asked us to retry.
      if (ctxt == skip_ctxt || !ctxt->ready) {
         continue;
      }
      ctxt->ready = FALSE;
      key target  = key_buffer_key(&ctxt->keybuf);
      res         = btree_lookup_async(
         cc, cfg, root_addr, target, &ctxt->result, &ctxt->ctxt);
      bool local_found = btree_found(&ctxt->result);
      switch (res) {
         case async_locked:
         case async_no_reqs:
            ctxt->ready = TRUE;
            break;
         case async_io_started:
            break;
         case async_success:
            if (local_found ^ expected_found) {
               btree_print_tree(Platform_default_log_handle,
                                cc,
                                cfg,
                                root_addr,
                                PAGE_TYPE_BRANCH);
               char key_string[128];
               data_key_to_string(cfg->data_cfg,
                                  key_buffer_key(&ctxt->keybuf),
                                  key_string,
                                  128);
               platform_default_log("key %s expect %u found %u\n",
                                    key_string,
                                    expected_found,
                                    local_found);
               platform_assert(0);
            }
            btree_test_put_async_ctxt(async_lookup, ctxt);
            break;
         default:
            platform_assert(0);
      }
   }

   return TRUE;
}


static void
btree_test_wait_pending(cache                   *cc,
                        btree_config            *cfg,
                        uint64                   root_addr,
                        btree_test_async_lookup *async_lookup,
                        bool                     expected_found)
{
   // Rough detection of stuck contexts
   const timestamp ts = platform_get_timestamp();
   while (btree_test_run_pending(cc, cfg, root_addr, async_lookup, NULL, TRUE))
   {
      cache_cleanup(cc);
      platform_assert(platform_timestamp_elapsed(ts) < TEST_STUCK_IO_TIMEOUT);
   }
}

cache_async_result
test_btree_async_lookup(cache                   *cc,
                        btree_config            *cfg,
                        btree_test_async_ctxt   *async_ctxt,
                        btree_test_async_lookup *async_lookup,
                        uint64                   root_addr,
                        bool                     expected_found,
                        bool                    *correct)
{
   cache_async_result res;
   btree_ctxt_init(
      &async_ctxt->ctxt, &async_ctxt->cache_ctxt, btree_test_async_callback);
   key target = key_buffer_key(&async_ctxt->keybuf);

   res = btree_lookup_async(
      cc, cfg, root_addr, target, &async_ctxt->result, &async_ctxt->ctxt);

   switch (res) {
      case async_locked:
      case async_no_reqs:
         async_ctxt->ready = TRUE;
         break;
      case async_io_started:
         async_ctxt = NULL;
         break;
      case async_success:
         *correct = btree_found(&async_ctxt->result) == expected_found;
         btree_test_put_async_ctxt(async_lookup, async_ctxt);
         async_ctxt = NULL;
         goto out;
         break;
      default:
         platform_assert(0);
   }

out:
   return res;
}

cache_async_result
test_memtable_async_lookup(test_memtable_context   *ctxt,
                           btree_test_async_ctxt   *async_ctxt,
                           btree_test_async_lookup *async_lookup,
                           uint64                   mt_no,
                           bool                     expected_found,
                           bool                    *correct)
{
   memtable     *mt        = &ctxt->mt_ctxt->mt[mt_no];
   btree_config *btree_cfg = mt->cfg;
   cache        *cc        = ctxt->cc;
   return test_btree_async_lookup(cc,
                                  btree_cfg,
                                  async_ctxt,
                                  async_lookup,
                                  mt->root_addr,
                                  expected_found,
                                  correct);
}

static platform_status
test_btree_basic(cache             *cc,
                 test_btree_config *cfg,
                 platform_heap_id   hid,
                 uint64             num_inserts)
{
   platform_default_log("btree_test: btree basic test started\n");

   test_memtable_context *ctxt = test_memtable_context_create(cc, cfg, 1, hid);
   memtable              *mt   = &ctxt->mt_ctxt->mt[0];
   data_config           *data_cfg       = mt->cfg->data_cfg;
   btree_test_async_lookup *async_lookup = TYPED_MALLOC(hid, async_lookup);

   platform_assert(async_lookup);

   btree_test_async_ctxt_init(async_lookup);

   uint64 start_time = platform_get_timestamp();
   DECLARE_AUTO_KEY_BUFFER(keybuf, ctxt->heap_id);
   merge_accumulator expected_data;
   merge_accumulator_init(&expected_data, ctxt->heap_id);

   platform_status rc = STATUS_OK;
   for (uint64 insert_num = 0; insert_num < num_inserts; insert_num++) {
      test_btree_tuple(ctxt, &keybuf, &expected_data, insert_num, 0);
      rc = test_btree_insert(ctxt,
                             key_buffer_key(&keybuf),
                             merge_accumulator_to_message(&expected_data));
      if (!SUCCESS(rc)) {
         goto destroy_btree;
      }
   }

   cache_assert_free(cc);

   platform_default_log("btree insert time per tuple %luns\n",
                        platform_timestamp_elapsed(start_time) / num_inserts);

   bool correct = memtable_verify(cc, mt);
   if (!correct) {
      memtable_print(Platform_default_log_handle, cc, mt);
   }
   platform_assert(correct);
   cache_assert_free(cc);

   uint64 num_async = 0;

   start_time = platform_get_timestamp();
   for (uint64 insert_num = 0; insert_num < num_inserts; insert_num++) {
      btree_test_async_ctxt *async_ctxt = btree_test_get_async_ctxt(
         test_memtable_context_btree_config(ctxt), hid, async_lookup);

      if (async_ctxt == NULL) {
         test_btree_tuple(ctxt, &keybuf, &expected_data, insert_num, 0);
         bool correct =
            test_memtable_lookup(ctxt,
                                 0,
                                 key_buffer_key(&keybuf),
                                 merge_accumulator_to_message(&expected_data));
         if (!correct) {
            memtable_print(Platform_default_log_handle, cc, mt);
            key target = key_buffer_key(&keybuf);
            platform_default_log("key number %lu, %s not found\n",
                                 insert_num,
                                 key_string(data_cfg, target));
         }
         platform_assert(correct);
      } else {
         num_async++;
         bool correct;
         test_btree_tuple(
            ctxt, &async_ctxt->keybuf, &expected_data, insert_num, 0);
         cache_async_result res = test_memtable_async_lookup(
            ctxt, async_ctxt, async_lookup, 0, TRUE, &correct);
         if (res == async_success) {
            if (!correct) {
               memtable_print(Platform_default_log_handle, cc, mt);
               key target = key_buffer_key(&async_ctxt->keybuf);
               platform_default_log("key number %lu, %s not found\n",
                                    insert_num,
                                    key_string(data_cfg, target));
            }
         }
      }
      btree_test_run_pending(
         cc, mt->cfg, mt->root_addr, async_lookup, async_ctxt, TRUE);
   }
   btree_test_wait_pending(cc, mt->cfg, mt->root_addr, async_lookup, TRUE);
   platform_default_log("btree positive lookup time per tuple %luns\n",
                        platform_timestamp_elapsed(start_time) / num_inserts);
   platform_default_log("%lu%% lookups were async\n",
                        num_async * 100 / num_inserts);
   cache_assert_free(cc);

   start_time       = platform_get_timestamp();
   uint64 start_num = num_inserts;
   uint64 end_num   = 2 * num_inserts;
   for (uint64 insert_num = start_num; insert_num < end_num; insert_num++) {
      test_btree_tuple(ctxt, &keybuf, &expected_data, insert_num, 0);
      bool correct =
         test_memtable_lookup(ctxt, 0, key_buffer_key(&keybuf), NULL_MESSAGE);
      if (!correct) {
         memtable_print(Platform_default_log_handle, cc, mt);
         key target = key_buffer_key(&keybuf);
         platform_default_log("key number %lu, %s not found\n",
                              insert_num,
                              key_string(data_cfg, target));
         platform_assert(0);
      }
   }
   platform_default_log("btree negative lookup time per tuple %luns\n",
                        platform_timestamp_elapsed(start_time) / num_inserts);

   cache_assert_free(cc);
   memtable_verify(cc, mt);

   btree_config  *btree_cfg = test_memtable_context_btree_config(ctxt);
   uint64         root_addr = memtable_root_addr(mt);
   btree_iterator itor;
   start_time = platform_get_timestamp();
   btree_iterator_init(cc,
                       btree_cfg,
                       &itor,
                       root_addr,
                       PAGE_TYPE_MEMTABLE,
                       NEGATIVE_INFINITY_KEY,
                       POSITIVE_INFINITY_KEY,
                       FALSE,
                       0);
   platform_default_log("btree iterator init time %luns\n",
                        platform_timestamp_elapsed(start_time));
   btree_pack_req req;
   rc = btree_pack_req_init(
      &req, cc, btree_cfg, (iterator *)&itor, UINT64_MAX, NULL, 0, NULL);
   platform_assert_status_ok(rc);

   btree_print_tree_stats(
      Platform_default_log_handle, cc, btree_cfg, root_addr);

   start_time = platform_get_timestamp();
   rc         = btree_pack(&req);
   platform_assert_status_ok(rc);

   btree_iterator_deinit(&itor);
   uint64 packed_root_addr = req.root_addr;
   platform_default_log("btree itor/pack time per tuple %luns\n",
                        platform_timestamp_elapsed(start_time) / num_inserts);
   cache_assert_free(cc);

   num_async  = 0;
   start_time = platform_get_timestamp();
   for (uint64 insert_num = 0; insert_num < num_inserts; insert_num++) {
      btree_test_async_ctxt *async_ctxt = btree_test_get_async_ctxt(
         test_memtable_context_btree_config(ctxt), hid, async_lookup);

      if (async_ctxt == NULL) {
         test_btree_tuple(ctxt, &keybuf, &expected_data, insert_num, 0);
         bool correct =
            test_btree_lookup(cc,
                              btree_cfg,
                              hid,
                              packed_root_addr,
                              key_buffer_key(&keybuf),
                              merge_accumulator_to_message(&expected_data));
         if (!correct) {
            btree_print_tree(Platform_default_log_handle,
                             cc,
                             btree_cfg,
                             packed_root_addr,
                             PAGE_TYPE_BRANCH);
            char key_string[128];
            key  target = key_buffer_key(&keybuf);
            btree_key_to_string(btree_cfg, target, key_string);
            platform_default_log(
               "key number %lu, %s not found\n", insert_num, key_string);
         }
         platform_assert(correct);
      } else {
         num_async++;
         bool correct;
         test_btree_tuple(
            ctxt, &async_ctxt->keybuf, &expected_data, insert_num, 0);
         cache_async_result res = test_btree_async_lookup(cc,
                                                          btree_cfg,
                                                          async_ctxt,
                                                          async_lookup,
                                                          packed_root_addr,
                                                          TRUE,
                                                          &correct);
         if (res == async_success) {
            if (!correct) {
               btree_print_tree(Platform_default_log_handle,
                                cc,
                                btree_cfg,
                                packed_root_addr,
                                PAGE_TYPE_BRANCH);
               char key_string[128];
               key  target = key_buffer_key(&async_ctxt->keybuf);
               btree_key_to_string(btree_cfg, target, key_string);
               platform_default_log(
                  "key number %lu, %s not found\n", insert_num, key_string);
            }
         }
      }
      btree_test_run_pending(
         cc, btree_cfg, packed_root_addr, async_lookup, async_ctxt, TRUE);
   }
   btree_test_wait_pending(cc, btree_cfg, packed_root_addr, async_lookup, TRUE);
   platform_default_log("btree packed positive lookup time per tuple %luns\n",
                        platform_timestamp_elapsed(start_time) / num_inserts);
   platform_default_log("%lu%% lookups were async\n",
                        num_async * 100 / num_inserts);
   cache_assert_free(cc);

   start_time = platform_get_timestamp();
   start_num  = num_inserts;
   end_num    = 2 * num_inserts;
   for (uint64 insert_num = start_num; insert_num < end_num; insert_num++) {
      test_btree_tuple(ctxt, &keybuf, &expected_data, insert_num, 0);
      bool correct = test_btree_lookup(cc,
                                       btree_cfg,
                                       hid,
                                       packed_root_addr,
                                       key_buffer_key(&keybuf),
                                       NULL_MESSAGE);
      if (!correct) {
         btree_print_tree(Platform_default_log_handle,
                          cc,
                          btree_cfg,
                          packed_root_addr,
                          PAGE_TYPE_BRANCH);
         char key_string[128];
         key  target = key_buffer_key(&keybuf);
         btree_key_to_string(btree_cfg, target, key_string);
         platform_default_log(
            "key number %lu, %s found (negative)\n", insert_num, key_string);
         platform_assert(0);
      }
   }
   platform_default_log("btree packed negative lookup time per tuple %luns\n",
                        platform_timestamp_elapsed(start_time) / num_inserts);
   cache_assert_free(cc);

   btree_print_tree_stats(
      Platform_default_log_handle, cc, btree_cfg, packed_root_addr);

   btree_dec_ref_range(cc,
                       btree_cfg,
                       packed_root_addr,
                       NEGATIVE_INFINITY_KEY,
                       POSITIVE_INFINITY_KEY);

destroy_btree:
   if (SUCCESS(rc))
      platform_default_log("btree_test: btree basic test succeeded\n");
   else
      platform_default_log("btree_test: btree basic test failed\n");
   platform_default_log("\n");
   test_memtable_context_destroy(ctxt, hid);
   platform_free(hid, async_lookup);
   merge_accumulator_deinit(&expected_data);
   return rc;
}

uint64
test_btree_create_packed_trees(cache             *cc,
                               test_btree_config *cfg,
                               platform_heap_id   hid,
                               uint64             num_trees,
                               uint64            *root_addr)
{
   test_memtable_context *ctxt =
      test_memtable_context_create(cc, cfg, num_trees, hid);

   // fill the memtables
   DECLARE_AUTO_KEY_BUFFER(keybuf, hid);
   merge_accumulator data;
   merge_accumulator_init(&data, hid);

   uint64          insert_no;
   platform_status rc = STATUS_OK;
   for (insert_no = 0; SUCCESS(rc); insert_no++) {
      test_btree_tuple(ctxt, &keybuf, &data, insert_no, 0);
      rc = test_btree_insert(
         ctxt, key_buffer_key(&keybuf), merge_accumulator_to_message(&data));
   }

   platform_assert(STATUS_IS_EQ(rc, STATUS_NO_SPACE));
   rc                = STATUS_OK;
   uint64 num_tuples = insert_no;

   btree_config *btree_cfg = test_memtable_context_btree_config(ctxt);
   for (uint64 tree_no = 0; tree_no < num_trees; tree_no++) {
      memtable      *mt = &ctxt->mt_ctxt->mt[tree_no];
      btree_iterator itor;
      btree_iterator_init(cc,
                          btree_cfg,
                          &itor,
                          memtable_root_addr(mt),
                          PAGE_TYPE_MEMTABLE,
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY,
                          FALSE,
                          0);

      btree_pack_req req;
      rc = btree_pack_req_init(
         &req, cc, btree_cfg, &itor.super, UINT64_MAX, NULL, 0, hid);
      platform_assert_status_ok(rc);

      rc = btree_pack(&req);
      platform_assert_status_ok(rc);
      btree_iterator_deinit(&itor);
      root_addr[tree_no] = req.root_addr;
   }

   merge_accumulator_deinit(&data);
   test_memtable_context_destroy(ctxt, hid);
   return num_tuples;
}


static inline platform_status
test_count_tuples_in_range(cache        *cc,
                           btree_config *cfg,
                           uint64       *root_addr,
                           page_type     type,
                           uint64        num_trees,
                           key           low_key,
                           key           high_key,
                           uint64       *count) // OUTPUT
{
   btree_iterator itor;
   uint64         i;
   *count = 0;
   for (i = 0; i < num_trees; i++) {
      if (!btree_verify_tree(cc, cfg, root_addr[i], type)) {
         btree_print_tree(Platform_default_log_handle,
                          cc,
                          cfg,
                          root_addr[i],
                          PAGE_TYPE_BRANCH);
         platform_assert(0);
      }
      btree_iterator_init(
         cc, cfg, &itor, root_addr[i], type, low_key, high_key, TRUE, 0);
      bool at_end;
      iterator_at_end(&itor.super, &at_end);
      key last_key = NULL_KEY;
      while (!at_end) {
         key     curr_key;
         message data;
         iterator_get_curr(&itor.super, &curr_key, &data);
         if (!key_is_null(last_key)
             && data_key_compare(cfg->data_cfg, last_key, curr_key) > 0)
         {
            char last_key_str[128], key_str[128];
            data_key_to_string(cfg->data_cfg, last_key, last_key_str, 128);
            data_key_to_string(cfg->data_cfg, curr_key, key_str, 128);
            btree_print_tree(Platform_default_log_handle,
                             cc,
                             cfg,
                             root_addr[i],
                             PAGE_TYPE_BRANCH);
            platform_default_log(
               "test_count_tuples_in_range: key out of order\n");
            platform_default_log("last %s\nkey %s\n", last_key_str, key_str);
            platform_assert(0);
         }
         if (data_key_compare(cfg->data_cfg, low_key, curr_key) > 0) {
            char low_key_str[128], key_str[128], high_key_str[128];
            data_key_to_string(cfg->data_cfg, low_key, low_key_str, 128);
            data_key_to_string(cfg->data_cfg, curr_key, key_str, 128);
            data_key_to_string(cfg->data_cfg, high_key, high_key_str, 128);
            btree_print_tree(Platform_default_log_handle,
                             cc,
                             cfg,
                             root_addr[i],
                             PAGE_TYPE_BRANCH);
            platform_default_log(
               "test_count_tuples_in_range: key out of range\n");
            platform_default_log(
               "low %s\nkey %s\nmax %s\n", low_key_str, key_str, high_key_str);
            platform_assert(0);
         }
         if (!key_is_null(high_key)
             && data_key_compare(cfg->data_cfg, curr_key, high_key) > 0)
         {
            char low_key_str[128], key_str[128], high_key_str[128];
            data_key_to_string(cfg->data_cfg, low_key, low_key_str, 128);
            data_key_to_string(cfg->data_cfg, curr_key, key_str, 128);
            data_key_to_string(cfg->data_cfg, high_key, high_key_str, 128);
            btree_print_tree(Platform_default_log_handle,
                             cc,
                             cfg,
                             root_addr[i],
                             PAGE_TYPE_BRANCH);
            platform_default_log(
               "test_count_tuples_in_range: key out of range\n");
            platform_default_log(
               "low %s\nkey %s\nmax %s\n", low_key_str, key_str, high_key_str);
            platform_assert(0);
         }
         (*count)++;
         iterator_advance(&itor.super);
         iterator_at_end(&itor.super, &at_end);
      }
      btree_iterator_deinit(&itor);
   }

   return STATUS_OK;
}

static inline int
test_btree_print_all_keys(cache        *cc,
                          btree_config *cfg,
                          uint64       *root_addr,
                          page_type     type,
                          uint64        num_trees,
                          key           low_key,
                          key           high_key)
{
   btree_iterator itor;
   uint64         i;
   for (i = 0; i < num_trees; i++) {
      platform_default_log("tree number %lu\n", i);
      btree_iterator_init(
         cc, cfg, &itor, root_addr[i], type, low_key, high_key, TRUE, 0);
      bool at_end;
      iterator_at_end(&itor.super, &at_end);
      while (!at_end) {
         key     curr_key;
         message data;
         iterator_get_curr(&itor.super, &curr_key, &data);
         char key_str[128];
         data_key_to_string(cfg->data_cfg, curr_key, key_str, 128);
         platform_default_log("%s\n", key_str);
         iterator_advance(&itor.super);
         iterator_at_end(&itor.super, &at_end);
      }
      btree_iterator_deinit(&itor);
   }
   return 0;
}

static platform_status
test_btree_merge_basic(cache             *cc,
                       test_btree_config *cfg,
                       platform_heap_id   hid,
                       uint64             arity)
{
   platform_default_log("btree_test: btree merge test started\n");

   btree_config *btree_cfg = cfg->mt_cfg->btree_cfg;

   platform_memfrag memfrag_root_addr = {0};
   uint64          *root_addr = TYPED_ARRAY_MALLOC(hid, root_addr, arity);
   platform_assert(root_addr);

   test_btree_create_packed_trees(cc, cfg, hid, arity, root_addr);

   platform_memfrag memfrag_output_addr = {0};
   uint64          *output_addr = TYPED_ARRAY_MALLOC(hid, output_addr, arity);
   platform_assert(output_addr);

   platform_status rc;

   uint64 max_key = (uint64)-1;

   platform_memfrag memfrag_pivot = {0};
   uint64          *pivot         = TYPED_ARRAY_MALLOC(hid, pivot, arity);
   platform_assert(pivot);

   platform_memfrag memfrag_pivot_key = {0};
   key_buffer      *pivot_key = TYPED_ARRAY_MALLOC(hid, pivot_key, arity + 1);
   platform_assert(pivot_key);

   for (uint64 pivot_no = 0; pivot_no < arity; pivot_no++) {
      key_buffer_init(&pivot_key[pivot_no], hid);
      pivot[pivot_no] = pivot_no * (max_key / arity + 1);
      test_int_to_key(&pivot_key[pivot_no],
                      pivot[pivot_no],
                      btree_cfg->data_cfg->max_key_size);
   }
   key_buffer_init_from_key(&pivot_key[arity], hid, POSITIVE_INFINITY_KEY);

   platform_memfrag memfrag_btree_itor_arr;
   btree_iterator  *btree_itor_arr =
      TYPED_ARRAY_MALLOC(hid, btree_itor_arr, arity);
   platform_assert(btree_itor_arr);

   platform_memfrag memfrag_itor_arr;
   iterator       **itor_arr = TYPED_ARRAY_MALLOC(hid, itor_arr, arity);
   platform_assert(itor_arr);

   for (uint64 pivot_no = 0; pivot_no < arity; pivot_no++) {
      key lo = key_buffer_key(&pivot_key[pivot_no]);
      key hi = key_buffer_key(&pivot_key[pivot_no + 1]);
      for (uint64 tree_no = 0; tree_no < arity; tree_no++) {
         btree_iterator_init(cc,
                             btree_cfg,
                             &btree_itor_arr[tree_no],
                             root_addr[tree_no],
                             PAGE_TYPE_BRANCH,
                             lo,
                             hi,
                             TRUE,
                             0);
         itor_arr[tree_no] = &btree_itor_arr[tree_no].super;
      }
      merge_iterator *merge_itor;
      rc = merge_iterator_create(
         hid, btree_cfg->data_cfg, arity, itor_arr, MERGE_FULL, &merge_itor);
      if (!SUCCESS(rc)) {
         goto destroy_btrees;
      }

      btree_pack_req req;
      rc = btree_pack_req_init(
         &req, cc, btree_cfg, &merge_itor->super, UINT64_MAX, NULL, 0, hid);
      platform_assert_status_ok(rc);
      btree_pack(&req);
      output_addr[pivot_no] = req.root_addr;

      for (uint64 tree_no = 0; tree_no < arity; tree_no++) {
         btree_iterator_deinit(&btree_itor_arr[tree_no]);
      }

      uint64 input_count = 0;
      rc                 = test_count_tuples_in_range(cc,
                                      btree_cfg,
                                      root_addr,
                                      PAGE_TYPE_BRANCH,
                                      arity,
                                      lo,
                                      hi,
                                      &input_count);
      if (!SUCCESS(rc)) {
         merge_iterator_destroy(hid, &merge_itor);
         goto destroy_btrees;
      }

      uint64 output_count = 0;
      rc                  = test_count_tuples_in_range(cc,
                                      btree_cfg,
                                      &req.root_addr,
                                      PAGE_TYPE_BRANCH,
                                      1,
                                      lo,
                                      hi,
                                      &output_count);
      if (!SUCCESS(rc)) {
         merge_iterator_destroy(hid, &merge_itor);
         goto destroy_btrees;
      }

      merge_iterator_destroy(hid, &merge_itor);
      if (input_count != output_count) {
         test_btree_print_all_keys(
            cc, btree_cfg, root_addr, PAGE_TYPE_BRANCH, arity, lo, hi);
         platform_default_log("****\n");
         test_btree_print_all_keys(
            cc, btree_cfg, &req.root_addr, PAGE_TYPE_BRANCH, 1, lo, hi);
         platform_default_log(
            "test_btree_merge_basic: input and output counts do not match\n");
         platform_default_log(
            "input count %lu output count %lu\n", input_count, output_count);
         platform_default_log("btree_test: btree merge test failed\n");
         return STATUS_INVALID_STATE;
      }
   }

destroy_btrees:
   for (uint64 tree_no = 0; tree_no < arity; tree_no++) {
      btree_dec_ref_range(cc,
                          btree_cfg,
                          root_addr[tree_no],
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY);
      btree_dec_ref_range(cc,
                          btree_cfg,
                          output_addr[tree_no],
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY);
   }
   if (SUCCESS(rc)) {
      platform_default_log("btree_test: btree merge test succeeded\n");
   } else {
      platform_default_log("btree_test: btree merge test failed\n");
   }
   platform_default_log("\n");

   platform_memfrag *mf = &memfrag_root_addr;
   platform_free(hid, mf);

   mf = &memfrag_output_addr;
   platform_free(hid, mf);

   mf = &memfrag_pivot;
   platform_free(hid, pivot);
   for (uint64 pivot_no = 0; pivot_no < arity + 1; pivot_no++) {
      key_buffer_deinit(&pivot_key[pivot_no]);
   }

   mf = &memfrag_pivot_key;
   platform_free(hid, pivot_key);

   mf = &memfrag_btree_itor_arr;
   platform_free(hid, mf);

   mf = &memfrag_itor_arr;
   platform_free(hid, mf);
   return rc;
}

static platform_status
test_btree_count_in_range(cache             *cc,
                          test_btree_config *cfg,
                          platform_heap_id   hid,
                          uint64             iterations)
{
   platform_default_log("btree_test: btree_count_in_range test started\n");

   uint64 root_addr;
   test_btree_create_packed_trees(cc, cfg, hid, 1, &root_addr);
   btree_config *btree_cfg = cfg->mt_cfg->btree_cfg;

   platform_memfrag memfrag_bound_key;
   key_buffer      *bound_key = TYPED_ARRAY_MALLOC(hid, bound_key, 2);
   platform_assert(bound_key);

   key_buffer_init(&bound_key[0], hid);
   key_buffer_init(&bound_key[1], hid);

   platform_status rc = STATUS_OK;
   for (uint64 i = 0; i < iterations; i++) {
      test_key(&bound_key[0],
               TEST_RANDOM,
               2 * i,
               0,
               0,
               btree_cfg->data_cfg->max_key_size,
               0);
      test_key(&bound_key[1],
               TEST_RANDOM,
               2 * i + 1,
               0,
               0,
               btree_cfg->data_cfg->max_key_size,
               0);

      btree_pivot_stats stats;
      key               min_key = key_buffer_key(&bound_key[0]);
      key               max_key = key_buffer_key(&bound_key[1]);
      btree_count_in_range(cc, btree_cfg, root_addr, min_key, max_key, &stats);
      if (btree_key_compare(btree_cfg, min_key, max_key) > 0) {
         if (stats.num_kvs != 0) {
            platform_default_log("btree_count_in_range did not return 0 for "
                                 "out-of-order keys.  Returned count: %d\n",
                                 stats.num_kvs);
            rc = STATUS_TEST_FAILED;
            goto destroy_btree;
         } else {
            continue;
         }
      }

      uint64 iterator_count;
      rc = test_count_tuples_in_range(cc,
                                      btree_cfg,
                                      &root_addr,
                                      PAGE_TYPE_BRANCH,
                                      1,
                                      min_key,
                                      max_key,
                                      &iterator_count);
      platform_assert_status_ok(rc);
      if (stats.num_kvs != iterator_count) {
         platform_default_log(
            "btree_count_in_range returned different number of keys from "
            "iterator count.  Returned: %d, iterator count: %ld\n",
            stats.num_kvs,
            iterator_count);
         rc = STATUS_TEST_FAILED;
         goto destroy_btree;
      }
   }

destroy_btree:
   btree_dec_ref_range(
      cc, btree_cfg, root_addr, NEGATIVE_INFINITY_KEY, POSITIVE_INFINITY_KEY);

   key_buffer_deinit(&bound_key[0]);
   key_buffer_deinit(&bound_key[1]);

   platform_memfrag *mf = &memfrag_bound_key;
   platform_free(hid, mf);
   if (SUCCESS(rc))
      platform_default_log("btree_test: btree_count_in_range test succeeded\n");
   else
      platform_default_log("btree_test: btree_count_in_range test failed\n");

   return rc;
}

static platform_status
test_btree_rough_iterator(cache             *cc,
                          test_btree_config *cfg,
                          platform_heap_id   hid,
                          uint64             num_trees)
{
   platform_default_log("btree_test: btree rough iterator test started\n");

   platform_memfrag memfrag_root_addr;
   uint64          *root_addr = TYPED_ARRAY_MALLOC(hid, root_addr, num_trees);
   platform_assert(root_addr);

   test_btree_create_packed_trees(cc, cfg, hid, num_trees, root_addr);
   btree_config *btree_cfg = cfg->mt_cfg->btree_cfg;

   platform_status rc = STATUS_OK;

   uint64 num_pivots = 2 * num_trees;

   platform_memfrag memfrag_pivot;
   key_buffer      *pivot = TYPED_ARRAY_MALLOC(hid, pivot, num_pivots + 1);
   platform_assert(pivot);

   platform_memfrag memfrag_rough_btree_itor;
   btree_iterator  *rough_btree_itor =
      TYPED_ARRAY_MALLOC(hid, rough_btree_itor, num_trees);
   platform_assert(rough_btree_itor);

   platform_memfrag memfrag_rough_itor;
   iterator       **rough_itor = TYPED_ARRAY_MALLOC(hid, rough_itor, num_trees);
   platform_assert(rough_itor);

   bool at_end;
   for (uint64 tree_no = 0; tree_no < num_trees; tree_no++) {
      btree_iterator_init(cc,
                          btree_cfg,
                          &rough_btree_itor[tree_no],
                          root_addr[tree_no],
                          PAGE_TYPE_BRANCH,
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY,
                          TRUE,
                          1);
      if (SUCCESS(iterator_at_end(&rough_btree_itor[tree_no].super, &at_end))
          && !at_end)
      {
         key     curr_key;
         message msg;
         iterator_get_curr(&rough_btree_itor[tree_no].super, &curr_key, &msg);
         platform_default_log("key size: %lu\n", key_length(curr_key));
      }
      rough_itor[tree_no] = &rough_btree_itor[tree_no].super;
   }

   merge_iterator *rough_merge_itor;
   rc = merge_iterator_create(hid,
                              btree_cfg->data_cfg,
                              num_trees,
                              rough_itor,
                              MERGE_RAW,
                              &rough_merge_itor);
   platform_assert_status_ok(rc);
   // uint64 target_num_pivots =
   //   cfg->mt_cfg->max_tuples_per_memtable / btree_cfg->tuples_per_leaf;

   iterator_at_end(&rough_merge_itor->super, &at_end);

   uint64 pivot_no;
   for (pivot_no = 0; !at_end; pivot_no++) {
      // uint64 rough_count_pivots = 0;
      key     curr_key;
      message dummy_data;
      iterator_get_curr(&rough_merge_itor->super, &curr_key, &dummy_data);
      if (key_length(curr_key) != btree_cfg->data_cfg->max_key_size) {
         platform_default_log("Weird key length: %lu should be: %lu\n",
                              key_length(curr_key),
                              btree_cfg->data_cfg->max_key_size);
      }
      if (message_length(dummy_data) != sizeof(btree_pivot_data)) {
         platform_default_log("Weird data length: %lu should be == "
                              "sizeof(btree_pivot_data), %lu\n",
                              message_length(dummy_data),
                              sizeof(btree_pivot_data));
      }
      rc = key_buffer_init_from_key(&pivot[pivot_no], hid, curr_key);
      platform_assert_status_ok(rc);
      at_end = TRUE;
      // char key_str[128];
      // btree_key_to_string(btree_cfg, pivot[pivot_no].k, key_str);
      // platform_default_log("pivot_no %lu rc_pivots: %lu curr_key %s\n",
      //      pivot_no, rough_count_pivots, key_str);
      // while (!at_end && rough_count_pivots < target_num_pivots) {
      //   iterator_advance(&rough_merge_itor->super);
      //   iterator_at_end(&rough_merge_itor->super, &at_end);
      //   rough_count_pivots++;
      //}
   }
   // num_pivots = pivot_no;
   ////memmove(pivot[0].k, min_key, btree_key_size(btree_cfg));
   // memmove(pivot[num_pivots].k, max_key, btree_key_size(btree_cfg));

   rc = merge_iterator_destroy(hid, &rough_merge_itor);
   platform_assert_status_ok(rc);
   // for (uint64 tree_no = 0; tree_no < num_trees; tree_no++) {
   //   btree_iterator_deinit(&rough_btree_itor[tree_no]);
   //}

   // for (uint64 pivot_no = 0; pivot_no < num_pivots; pivot_no++) {
   //   uint64 num_tuples;
   //   char key_str[128];
   //   btree_key_to_string(btree_cfg, pivot[pivot_no].k, key_str);
   //   platform_default_log("min_key %s ", key_str);
   //   btree_key_to_string(btree_cfg, pivot[pivot_no + 1].k, key_str);
   //   platform_default_log("max_key %s ", key_str);
   //   test_count_tuples_in_range(cc, btree_cfg, root_addr, PAGE_TYPE_BRANCH,
   //         num_trees, pivot[pivot_no].k, pivot[pivot_no+1].k, &num_tuples);
   //   platform_default_log("target %lu actual %lu\n",
   //         cfg->mt_cfg->max_tuples_per_memtable, num_tuples);
   //}

   // for (uint64 tree_no = 0; tree_no < num_trees; tree_no++) {
   //   btree_zap(cc, btree_cfg, root_addr[tree_no], PAGE_TYPE_BRANCH);
   //}

   platform_memfrag *mf = &memfrag_root_addr;
   platform_free(hid, mf);

   for (int i = 0; i < pivot_no; i++) {
      key_buffer_deinit(&pivot[i]);
   }
   mf = &memfrag_pivot;
   platform_free(hid, mf);

   mf = &memfrag_rough_btree_itor;
   platform_free(hid, mf);

   mf = &memfrag_rough_itor;
   platform_free(hid, mf);

   if (SUCCESS(rc)) {
      platform_default_log("btree_test: btree rough iterator test succeeded\n");
   } else {
      platform_default_log("btree_test: btree rough iterator test failed\n");
   }
   return rc;
}

static platform_status
test_btree_merge_perf(cache             *cc,
                      test_btree_config *cfg,
                      platform_heap_id   hid,
                      uint64             arity,
                      uint64             num_merges)
{
   platform_default_log("btree_test: btree merge perf test started\n");

   btree_config *btree_cfg = cfg->mt_cfg->btree_cfg;

   platform_memfrag memfrag_root_addr;
   uint64           num_trees = arity * num_merges;
   uint64          *root_addr = TYPED_ARRAY_MALLOC(hid, root_addr, num_trees);
   platform_assert(root_addr);

   uint64 total_tuples =
      test_btree_create_packed_trees(cc, cfg, hid, num_trees, root_addr);

   platform_memfrag memfrag_output_addr;
   uint64 *output_addr = TYPED_ARRAY_MALLOC(hid, output_addr, num_trees);
   platform_assert(output_addr);

   platform_status rc = STATUS_OK;

   uint64 max_key = (uint64)-1;

   platform_memfrag memfrag_pivot;
   uint64          *pivot = TYPED_ARRAY_MALLOC(hid, pivot, arity);
   platform_assert(pivot);

   platform_memfrag memfrag_pivot_key;
   key_buffer      *pivot_key = TYPED_ARRAY_MALLOC(hid, pivot_key, arity + 1);
   platform_assert(pivot_key);

   uint64 start_time = platform_get_timestamp();

   for (uint64 pivot_no = 0; pivot_no < arity; pivot_no++) {
      key_buffer_init(&pivot_key[pivot_no], hid);
      pivot[pivot_no] = pivot_no * (max_key / arity + 1);
      test_int_to_key(&pivot_key[pivot_no],
                      pivot[pivot_no],
                      btree_cfg->data_cfg->max_key_size);
   }
   key_buffer_init_from_key(&pivot_key[arity], hid, POSITIVE_INFINITY_KEY);

   platform_memfrag memfrag_btree_itor_arr;
   btree_iterator  *btree_itor_arr =
      TYPED_ARRAY_MALLOC(hid, btree_itor_arr, arity);
   platform_assert(btree_itor_arr);

   platform_memfrag memfrag_itor_arr;
   iterator       **itor_arr = TYPED_ARRAY_MALLOC(hid, itor_arr, arity);
   platform_assert(itor_arr);

   for (uint64 merge_no = 0; merge_no < num_merges; merge_no++) {
      for (uint64 pivot_no = 0; pivot_no < arity; pivot_no++) {
         key min_key = key_buffer_key(&pivot_key[pivot_no]);
         key max_key = key_buffer_key(&pivot_key[pivot_no + 1]);
         for (uint64 tree_no = 0; tree_no < arity; tree_no++) {
            uint64 global_tree_no = merge_no * num_merges + tree_no;
            btree_iterator_init(cc,
                                btree_cfg,
                                &btree_itor_arr[tree_no],
                                root_addr[global_tree_no],
                                PAGE_TYPE_BRANCH,
                                min_key,
                                max_key,
                                TRUE,
                                0);
            itor_arr[tree_no] = &btree_itor_arr[tree_no].super;
         }
         merge_iterator *merge_itor;
         rc = merge_iterator_create(
            hid, btree_cfg->data_cfg, arity, itor_arr, MERGE_FULL, &merge_itor);
         if (!SUCCESS(rc)) {
            goto destroy_btrees;
         }

         btree_pack_req req;
         rc = btree_pack_req_init(
            &req, cc, btree_cfg, &merge_itor->super, UINT64_MAX, NULL, 0, hid);
         platform_assert_status_ok(rc);

         btree_pack(&req);
         output_addr[merge_no * num_merges + pivot_no] = req.root_addr;
         for (uint64 tree_no = 0; tree_no < arity; tree_no++) {
            btree_iterator_deinit(&btree_itor_arr[tree_no]);
         }
         merge_iterator_destroy(hid, &merge_itor);
      }
   }

   platform_default_log("btree merge time per tuple %lu\n",
                        platform_timestamp_elapsed(start_time) / total_tuples);

destroy_btrees:
   for (uint64 tree_no = 0; tree_no < num_trees; tree_no++) {
      btree_dec_ref_range(cc,
                          btree_cfg,
                          root_addr[tree_no],
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY);
      btree_dec_ref_range(cc,
                          btree_cfg,
                          output_addr[tree_no],
                          NEGATIVE_INFINITY_KEY,
                          POSITIVE_INFINITY_KEY);
   }
   if (SUCCESS(rc)) {
      platform_default_log("btree_test: btree merge perf test succeeded\n");
   } else {
      platform_default_log("btree_test: btree merge perf test failed\n");
   }
   platform_default_log("\n");

   platform_memfrag *mf = &memfrag_root_addr;
   platform_free(hid, mf);

   mf = &memfrag_output_addr;
   platform_free(hid, mf);
   for (uint64 pivot_no = 0; pivot_no < arity + 1; pivot_no++) {
      key_buffer_deinit(&pivot_key[pivot_no]);
   }

   mf = &memfrag_pivot;
   platform_free(hid, mf);

   mf = &memfrag_pivot_key;
   platform_free(hid, mf);

   mf = &memfrag_btree_itor_arr;
   platform_free(hid, btree_itor_arr);

   mf = &memfrag_itor_arr;
   platform_free(hid, itor_arr);

   return rc;
}

static void
usage(const char *argv0)
{
   platform_error_log("Usage:\n"
                      "\t%s --max-async-inflight [num]\n"
                      "\t%s --perf\n",
                      argv0,
                      argv0);
   config_usage();
}

int
btree_test(int argc, char *argv[])
{
   io_config              io_cfg;
   allocator_config       al_cfg;
   clockcache_config      cache_cfg;
   shard_log_config       log_cfg;
   task_system_config     task_cfg;
   int                    config_argc;
   char                 **config_argv;
   bool                   run_perf_test;
   platform_status        rc;
   uint64                 seed;
   task_system           *ts = NULL;
   test_message_generator gen;

   if (argc > 1 && strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
      run_perf_test = TRUE;
      config_argc   = argc - 2;
      config_argv   = argv + 2;
   } else {
      run_perf_test = FALSE;
      config_argc   = argc - 1;
      config_argv   = argv + 1;
   }
   if (config_argc > 0
       && strncmp(config_argv[0],
                  "--max-async-inflight",
                  sizeof("--max-async-inflight"))
             == 0)
   {
      if (!try_string_to_uint64(config_argv[1], &max_async_inflight)) {
         usage(argv[0]);
         return -1;
      }
      config_argc -= 2;
      config_argv += 2;
   }

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;
   rc =
      platform_heap_create(platform_get_module_id(), 1 * GiB, FALSE, &hh, &hid);
   platform_assert_status_ok(rc);

   uint64 num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads

   data_config  *data_cfg;
   trunk_config *cfg = TYPED_MALLOC(hid, cfg);

   rc = test_parse_args(cfg,
                        &data_cfg,
                        &io_cfg,
                        &al_cfg,
                        &cache_cfg,
                        &log_cfg,
                        &task_cfg,
                        &seed,
                        &gen,
                        &num_bg_threads[TASK_TYPE_MEMTABLE],
                        &num_bg_threads[TASK_TYPE_NORMAL],
                        config_argc,
                        config_argv);

   memtable_config *mt_cfg    = &cfg->mt_cfg;
   mt_cfg->max_memtables      = 128;
   test_btree_config test_cfg = {
      .mt_cfg = mt_cfg, .type = TEST_RANDOM, .semiseq_freq = 0, .msggen = &gen};
   if (!SUCCESS(rc)) {
      platform_error_log("btree_test: failed to parse config: %s\n",
                         platform_status_to_string(rc));
      /*
       * Provided arguments but set things up incorrectly.
       * Print usage so client can fix commandline.
       */
      usage(argv[0]);
      goto cleanup;
   }

   if (run_perf_test) {
      // For default test execution parameters, we need a reasonably big
      // enough cache to handle the Memtable being pinned.
      int reqd_cache_GiB = 4;
      if (cache_cfg.capacity < (reqd_cache_GiB * GiB)) {
         platform_error_log(
            "Warning! Your configured cache size, %lu GiB, may be "
            "insufficient to run the 'btree_test --perf' test. "
            "Recommend a minimum of '--cache-capacity-gib %d' GiB. "
            "If you change the key / message size, or the number "
            "of inserts, you may also need to increase the cache "
            "size appropriately.\n",
            B_TO_GiB(cache_cfg.capacity),
            reqd_cache_GiB);
      }
   }

   platform_io_handle *io = TYPED_MALLOC(hid, io);
   platform_assert(io != NULL);
   rc = io_handle_init(io, &io_cfg, hh, hid);
   if (!SUCCESS(rc)) {
      goto free_iohandle;
   }

   rc = test_init_task_system(hid, io, &ts, &task_cfg);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(rc));
      goto deinit_iohandle;
   }

   rc_allocator al;
   rc_allocator_init(
      &al, &al_cfg, (io_handle *)io, hid, platform_get_module_id());

   clockcache *cc = TYPED_MALLOC(hid, cc);
   rc             = clockcache_init(cc,
                        &cache_cfg,
                        (io_handle *)io,
                        (allocator *)&al,
                        "test",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);
   cache *ccp = (cache *)cc;

   uint64 max_tuples_per_memtable =
      test_cfg.mt_cfg->max_extents_per_memtable
      * cache_config_extent_size((cache_config *)&cache_cfg) / 3
      / (data_cfg->max_key_size + generator_average_message_size(&gen));
   if (run_perf_test) {
      uint64 total_inserts = 64 * max_tuples_per_memtable;

      rc = test_btree_perf(ccp, &test_cfg, total_inserts, 10, 128, ts, hid);
      platform_assert_status_ok(rc);

      rc = test_btree_merge_perf(ccp, &test_cfg, hid, 8, 8);
      platform_assert_status_ok(rc);
   } else {
      uint64 total_inserts =
         max_tuples_per_memtable - (MAX_THREADS * (64 / sizeof(uint32)));
      rc = test_btree_basic(ccp, &test_cfg, hid, total_inserts);
      platform_assert_status_ok(rc);

      /*
       * Iterators can hold on to a large no. of pages, and would cause
       * cache lockup for low cache sizes.
       */
      if (cache_cfg.capacity > 4 * MiB) {
         rc = test_btree_rough_iterator(ccp, &test_cfg, hid, 8);
         platform_assert_status_ok(rc);

         rc = test_btree_merge_basic(ccp, &test_cfg, hid, 8);
         platform_assert_status_ok(rc);

         rc = test_btree_count_in_range(ccp, &test_cfg, hid, 10000);
         platform_assert_status_ok(rc);
      }
   }

   clockcache_deinit(cc);
   platform_free(hid, cc);
   rc_allocator_deinit(&al);
   test_deinit_task_system(hid, &ts);
   rc = STATUS_OK;
deinit_iohandle:
   io_handle_deinit(io);
free_iohandle:
   platform_free(hid, io);
cleanup:
   platform_free(hid, cfg);
   platform_heap_destroy(&hh);

   return SUCCESS(rc) ? 0 : -1;
}
