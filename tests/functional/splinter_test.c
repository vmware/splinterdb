// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinter_test.c --
 *
 *     This file contains the test interfaces for SplinterDB.
 */

#include "platform.h"

#include "trunk.h"
#include "merge.h"
#include "test.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "splinterdb/data.h"
#include "task.h"
#include "test_functionality.h"
#include "util.h"
#include "splinter_test.h"
#include "test_async.h"
#include "test_common.h"

#include "random.h"
#include "poison.h"

#define TEST_INSERT_GRANULARITY 4096
#define TEST_RANGE_GRANULARITY  512

// operaton types supported in parallel test
typedef enum test_splinter_pthread_op_type {
   OP_INSERT = 0,
   OP_LOOKUP,

   NUM_OP_TYPES
} test_splinter_pthread_op_type;

typedef enum lookup_type {
   SYNC_LU = 0,
   ASYNC_LU,

   NUM_LOOKUP_TYPES
} lookup_type;

typedef struct stats_insert {
   uint64 latency_max;
   uint64 duration;
} stats_insert;

typedef struct test_splinter_thread_params {
   platform_thread thread;
   trunk_handle  **spl;
   test_config    *test_cfg;
   uint64         *total_ops;
   uint64         *curr_op;
   uint64          op_granularity;
   uint8           num_tables;
   uint64          thread_number;
   task_system    *ts;
   platform_status rc;
   uint64          range_min;
   uint64          range_max;
   stats_insert    insert_stats;
   // message_type       message_type;
   uint64             num_ops_per_thread[NUM_OP_TYPES]; // in each round
   bool               expected_found;
   test_async_lookup *async_lookup[8]; // async lookup state per table
   uint64             insert_rate;
   stats_lookup       lookup_stats[NUM_LOOKUP_TYPES];
   uint8              lookup_positive_pct; // parallel lookup positive %
   uint64             seed;
} test_splinter_thread_params;

static inline bool
test_is_done(const uint8 done, const uint8 n)
{
   return ((done >> n) & 1);
}

static inline void
test_set_done(uint8 *done, const uint8 n)
{
   *done |= 1 << n;
}

static inline bool
test_all_done(const uint8 done, const uint8 num_tables)
{
   return (done == ((1 << num_tables) - 1));
}

void
test_trunk_insert_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   trunk_handle     **spl_tables     = params->spl;
   const test_config *test_cfg       = params->test_cfg;
   const uint64      *total_ops      = params->total_ops;
   uint64            *curr_op        = params->curr_op;
   uint64             op_granularity = params->op_granularity;
   uint64             thread_number  = params->thread_number;
   uint8              num_tables     = params->num_tables;

   platform_assert(num_tables <= 8);
   uint64 *insert_base =
      TYPED_ARRAY_ZALLOC(platform_get_heap_id(), insert_base, num_tables);
   uint8 done = 0;

   uint64    num_inserts     = 0;
   timestamp next_check_time = platform_get_timestamp();
   // divide the per second insert rate into periods of 10 milli seconds.
   uint64            insert_rate = params->insert_rate / 100;
   merge_accumulator msg;
   merge_accumulator_init(&msg, NULL);

   while (1) {
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx)) {
            continue;
         }
         platform_default_log(PLATFORM_CR
                              "inserting %3lu%% complete for table %u",
                              insert_base[spl_idx] / (total_ops[spl_idx] / 100),
                              spl_idx);
         insert_base[spl_idx] =
            __sync_fetch_and_add(&curr_op[spl_idx], op_granularity);
         if (insert_base[spl_idx] >= total_ops[spl_idx]) {
            test_set_done(&done, spl_idx);
         }
         if (test_all_done(done, num_tables)) {
            platform_default_log(" Test done for all %d tables.\n", num_tables);
            goto out;
         }
      }
      for (uint64 op_offset = 0; op_offset != op_granularity; op_offset++) {
         for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
            uint64 insert_num = insert_base[spl_idx] + op_offset;
            if (test_is_done(done, spl_idx)) {
               continue;
            }
            trunk_handle *spl = spl_tables[spl_idx];
            char          key[MAX_KEY_SIZE];

            timestamp ts;
            if (spl->cfg.use_stats) {
               ts = platform_get_timestamp();
            }
            test_key(key,
                     test_cfg[spl_idx].key_type,
                     insert_num,
                     thread_number,
                     test_cfg[spl_idx].semiseq_freq,
                     trunk_key_size(spl),
                     test_cfg[spl_idx].period);
            generate_test_message(test_cfg->gen, insert_num, &msg);
            platform_status rc =
               trunk_insert(spl, key, merge_accumulator_to_message(&msg));
            platform_assert_status_ok(rc);
            if (spl->cfg.use_stats) {
               ts = platform_timestamp_elapsed(ts);
               if (ts > params->insert_stats.latency_max) {
                  params->insert_stats.latency_max = ts;
               }
            }

            // Throttle thread based on insert rate if needed.
            if (insert_rate != 0) {
               timestamp now = platform_get_timestamp();
               if (now <= next_check_time) {
                  num_inserts++;
                  if (num_inserts >= insert_rate) {
                     platform_sleep(next_check_time - now);
                  }
               } else {
                  // reset and check again after 10 msec.
                  num_inserts     = 0;
                  next_check_time = now + (USEC_TO_NSEC(10000));
               }
            }
         }
      }
   }
out:
   merge_accumulator_deinit(&msg);
   params->rc = STATUS_OK;
   platform_free(platform_get_heap_id(), insert_base);
   for (uint64 i = 0; i < num_tables; i++) {
      trunk_handle *spl = spl_tables[i];
      trunk_perform_tasks(spl);
   }
}

void
test_trunk_lookup_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   trunk_handle     **spl_tables     = params->spl;
   const test_config *test_cfg       = params->test_cfg;
   const uint64      *total_ops      = params->total_ops;
   uint64            *curr_op        = params->curr_op;
   uint64             op_granularity = params->op_granularity;
   uint64             thread_number  = params->thread_number;
   bool               expected_found = params->expected_found;
   uint8              num_tables     = params->num_tables;
   verify_tuple_arg   vtarg          = {.expected_found = expected_found,
                                        .stats = &params->lookup_stats[ASYNC_LU]};

   platform_assert(num_tables <= 8);
   uint64 *lookup_base =
      TYPED_ARRAY_ZALLOC(platform_get_heap_id(), lookup_base, num_tables);
   uint8 done = 0;

   merge_accumulator data;
   merge_accumulator_init(&data, NULL);


   while (1) {
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx))
            continue;
         platform_throttled_error_log(
            DEFAULT_THROTTLE_INTERVAL_SEC,
            PLATFORM_CR "lookups %3lu%% complete for table %u",
            lookup_base[spl_idx] / (total_ops[spl_idx] / 100),
            spl_idx);
         lookup_base[spl_idx] =
            __sync_fetch_and_add(&curr_op[spl_idx], op_granularity);
         if (lookup_base[spl_idx] >= total_ops[spl_idx])
            test_set_done(&done, spl_idx);
         if (test_all_done(done, num_tables))
            goto out;
      }
      for (uint64 op_offset = 0; op_offset != op_granularity; op_offset++) {
         uint8 spl_idx;
         for (spl_idx = 0; spl_idx < num_tables; spl_idx++) {
            if (test_is_done(done, spl_idx))
               continue;
            trunk_handle      *spl          = spl_tables[spl_idx];
            test_async_lookup *async_lookup = params->async_lookup[spl_idx];
            test_async_ctxt   *ctxt;
            uint64             lookup_num = lookup_base[spl_idx] + op_offset;
            timestamp          ts;

            if (async_lookup->max_async_inflight == 0) {
               platform_status rc;
               char            key[MAX_KEY_SIZE];

               test_key(key,
                        test_cfg[spl_idx].key_type,
                        lookup_num,
                        thread_number,
                        test_cfg[spl_idx].semiseq_freq,
                        trunk_key_size(spl),
                        test_cfg[spl_idx].period);
               ts = platform_get_timestamp();
               rc = trunk_lookup(spl, key, &data);
               ts = platform_timestamp_elapsed(ts);
               if (ts > params->lookup_stats[SYNC_LU].latency_max) {
                  params->lookup_stats[SYNC_LU].latency_max = ts;
               }
               platform_assert(SUCCESS(rc));
               verify_tuple(spl,
                            test_cfg->gen,
                            lookup_num,
                            key,
                            merge_accumulator_to_message(&data),
                            expected_found);
            } else {
               ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg);
               test_key(ctxt->key,
                        test_cfg[spl_idx].key_type,
                        lookup_num,
                        thread_number,
                        test_cfg[spl_idx].semiseq_freq,
                        trunk_key_size(spl),
                        test_cfg[spl_idx].period);
               ctxt->lookup_num = lookup_num;
               async_ctxt_process_one(
                  spl,
                  async_lookup,
                  ctxt,
                  &params->lookup_stats[ASYNC_LU].latency_max,
                  verify_tuple_callback,
                  &vtarg);
            }
         }
      }
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx))
            continue;
         trunk_handle      *spl          = spl_tables[spl_idx];
         test_async_lookup *async_lookup = params->async_lookup[spl_idx];
         test_wait_for_inflight(spl, async_lookup, &vtarg);
      }
   }
out:
   merge_accumulator_deinit(&data);
   params->rc = STATUS_OK;
   platform_free(platform_get_heap_id(), lookup_base);
}

static void
nop_tuple_func(slice key, message value, void *arg)
{}

void
test_trunk_range_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   trunk_handle     **spl_tables     = params->spl;
   const test_config *test_cfg       = params->test_cfg;
   const uint64      *total_ops      = params->total_ops;
   uint64            *curr_op        = params->curr_op;
   uint64             op_granularity = params->op_granularity;
   uint64             thread_number  = params->thread_number;
   uint64             range_min      = params->range_min;
   uint64             range_max      = params->range_max;
   uint8              num_tables     = params->num_tables;

   platform_assert(num_tables <= 8);
   uint64 *range_base =
      TYPED_ARRAY_ZALLOC(platform_get_heap_id(), range_base, num_tables);
   uint8 done = 0;

   while (1) {
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx))
            continue;
         platform_throttled_error_log(
            DEFAULT_THROTTLE_INTERVAL_SEC,
            PLATFORM_CR "range lookups %3lu%% complete for table %u",
            range_base[spl_idx] / (total_ops[spl_idx] / 100),
            spl_idx);
         range_base[spl_idx] =
            __sync_fetch_and_add(&curr_op[spl_idx], op_granularity);
         if (range_base[spl_idx] >= total_ops[spl_idx])
            test_set_done(&done, spl_idx);
         if (test_all_done(done, num_tables))
            goto out;
      }
      for (uint64 op_offset = 0; op_offset != op_granularity; op_offset++) {
         for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
            if (test_is_done(done, spl_idx))
               continue;
            trunk_handle *spl = spl_tables[spl_idx];

            char   start_key[MAX_KEY_SIZE];
            uint64 range_num = range_base[spl_idx] + op_offset;
            test_key(start_key,
                     test_cfg[spl_idx].key_type,
                     range_num,
                     thread_number,
                     test_cfg[spl_idx].semiseq_freq,
                     trunk_key_size(spl),
                     test_cfg[spl_idx].period);
            uint64 range_tuples = test_range(range_num, range_min, range_max);
            platform_status rc =
               trunk_range(spl, start_key, range_tuples, nop_tuple_func, NULL);
            platform_assert_status_ok(rc);
         }
      }
   }
out:
   params->rc = STATUS_OK;
   platform_free(platform_get_heap_id(), range_base);
}

/*
 *-----------------------------------------------------------------------------
 *
 * advance_base
 *
 *      advance the base operation number synchronously
 *      lookup operation randomly selects base number either has contention or
 *      not with insert by lookup positive %
 *
 * Returns: TRUE if all tests are done
 *
 *-----------------------------------------------------------------------------
 */

static bool
advance_base(const test_splinter_thread_params *params,
             uint64                            *curr_op,
             uint64                            *base,
             uint8                             *done,
             random_state                      *rs,
             test_splinter_pthread_op_type      type)
{
   const uint64 *total_ops           = params->total_ops;
   const uint64  op_granularity      = params->op_granularity;
   const uint8   num_tables          = params->num_tables;
   const uint8   lookup_positive_pct = params->lookup_positive_pct;

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      if (test_is_done(*done, spl_idx))
         continue;

      if (type == OP_INSERT) {
         platform_throttled_error_log(
            DEFAULT_THROTTLE_INTERVAL_SEC,
            PLATFORM_CR "inserting/lookups %3lu%% complete for table %u",
            base[spl_idx] / (total_ops[spl_idx] / 100),
            spl_idx);

         base[spl_idx] =
            __sync_fetch_and_add(&curr_op[spl_idx], op_granularity);
         if (base[spl_idx] >= total_ops[spl_idx])
            test_set_done(done, spl_idx);
         if (test_all_done(*done, num_tables))
            return TRUE;
      } else {
         /* lookup */
         uint64 random_base;
         uint64 local_curr_op = curr_op[spl_idx];
         // pick an random number to determine
         // if positive or negative lookup
         // [0, lookup_positive_pct) is positive lookup
         if (random_next_uint64(rs) % 100 < lookup_positive_pct) {
            if (local_curr_op == op_granularity) {
               random_base = 0;
            } else {
               // positvie lookup by selecting random base
               // [op_granularity, local_curr_op)
               random_base = ((random_next_uint64(rs)
                               % (local_curr_op / op_granularity - 1))
                              + 1)
                             * op_granularity;
            }
         } else {
            if (local_curr_op >= total_ops[spl_idx] - op_granularity) {
               // one interval left,
               // may lookup the key never be inserted
               random_base =
                  (random_next_uint64(rs) / op_granularity) * op_granularity
                  + local_curr_op;
            } else {
               // negative lookup by select random base
               // [local_curr_op + op_granularity, total_ops - op_granularity]
               random_base =
                  ((random_next_uint64(rs)
                    % ((total_ops[spl_idx] - local_curr_op) / op_granularity
                       - 1))
                   + 1)
                     * op_granularity
                  + local_curr_op;
            }
         }
         base[spl_idx] = random_base;
      }
   }

   if (test_all_done(*done, num_tables)) {
      return TRUE;
   }

   return FALSE;
}

/*
 *-----------------------------------------------------------------------------
 *
 * do_operation
 *
 *      do num_ops(assume it is valid and not exceed op granularity) inserts or
 *lookups
 *
 *-----------------------------------------------------------------------------
 */

static void
do_operation(test_splinter_thread_params *params,
             const uint64                *base,
             uint64                       num_ops,
             uint64                       op_offset,
             const uint8                 *done,
             bool                         is_insert)
{
   trunk_handle     **spl_tables     = params->spl;
   const test_config *test_cfg       = params->test_cfg;
   uint64             op_granularity = params->op_granularity;
   uint64             thread_number  = params->thread_number;
   uint8              num_tables     = params->num_tables;
   verify_tuple_arg   vtarg          = {.stats_only = TRUE,
                                        .stats      = &params->lookup_stats[ASYNC_LU]};
   merge_accumulator  msg;
   merge_accumulator_init(&msg, NULL);

   for (uint64 op_idx = op_offset; op_idx != op_offset + num_ops; op_idx++) {
      if (op_idx >= op_granularity) {
         return;
      }

      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(*done, spl_idx)) {
            continue;
         }
         trunk_handle *spl = spl_tables[spl_idx];
         char          key[MAX_KEY_SIZE];
         uint64        op_num = base[spl_idx] + op_idx;
         timestamp     ts;

         if (is_insert) {
            test_key(key,
                     test_cfg[spl_idx].key_type,
                     op_num,
                     thread_number,
                     test_cfg[spl_idx].semiseq_freq,
                     trunk_key_size(spl),
                     test_cfg[spl_idx].period);
            generate_test_message(test_cfg->gen, op_num, &msg);
            ts = platform_get_timestamp();
            platform_status rc =
               trunk_insert(spl, key, merge_accumulator_to_message(&msg));
            platform_assert_status_ok(rc);
            ts = platform_timestamp_elapsed(ts);
            params->insert_stats.duration += ts;
            if (ts > params->insert_stats.latency_max) {
               params->insert_stats.latency_max = ts;
            }
         } else {
            test_async_lookup *async_lookup = params->async_lookup[spl_idx];
            test_async_ctxt   *ctxt;

            if (async_lookup->max_async_inflight == 0) {
               platform_status rc;
               bool            found;

               test_key(key,
                        test_cfg[spl_idx].key_type,
                        op_num,
                        thread_number,
                        test_cfg[spl_idx].semiseq_freq,
                        trunk_key_size(spl),
                        test_cfg[spl_idx].period);
               ts = platform_get_timestamp();
               rc = trunk_lookup(spl, key, &msg);
               platform_assert(SUCCESS(rc));
               ts = platform_timestamp_elapsed(ts);
               if (ts > params->lookup_stats[SYNC_LU].latency_max) {
                  params->lookup_stats[SYNC_LU].latency_max = ts;
               }
               found = trunk_lookup_found(&msg);
               if (found) {
                  params->lookup_stats[SYNC_LU].num_found++;
               } else {
                  params->lookup_stats[SYNC_LU].num_not_found++;
               }
            } else {
               ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg);
               test_key(ctxt->key,
                        test_cfg[spl_idx].key_type,
                        op_num,
                        thread_number,
                        test_cfg[spl_idx].semiseq_freq,
                        trunk_key_size(spl),
                        test_cfg[spl_idx].period);
               ctxt->lookup_num = op_num;
               async_ctxt_process_one(
                  spl,
                  async_lookup,
                  ctxt,
                  &params->lookup_stats[ASYNC_LU].latency_max,
                  verify_tuple_callback,
                  &vtarg);
            }
         }
      }
   }

   merge_accumulator_deinit(&msg);
}

/*
 *-----------------------------------------------------------------------------
 *
 * test_trunk_insert_lookup_thread
 *
 *      do number of insert_granularity inserts followed by number of
 *      lookup_granularity repeatedly until insert the full table
 *      lookup will be restarted from beginning until insert is done
 *
 *-----------------------------------------------------------------------------
 */
void
test_trunk_insert_lookup_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   trunk_handle **spl_tables     = params->spl;
   uint8          num_tables     = params->num_tables;
   uint64         op_granularity = params->op_granularity;
   uint64         seed           = params->seed;

   platform_assert(num_tables <= 8);

   uint64      *bases[NUM_OP_TYPES];
   uint64       granularities[NUM_OP_TYPES];
   uint64       offsets[NUM_OP_TYPES];
   uint8        insert_done;
   uint64       num_ops = 0;
   random_state rs;

   random_init(&rs, seed, 0);

   for (uint8 i = 0; i < NUM_OP_TYPES; i++) {
      bases[i] =
         TYPED_ARRAY_ZALLOC(platform_get_heap_id(), bases[i], num_tables);

      granularities[i] = params->num_ops_per_thread[i];
      offsets[i]       = 0;
   }
   for (uint8 i = 0; i < NUM_OP_TYPES; i++) {
      advance_base(params, params->curr_op, bases[i], &insert_done, &rs, i);
   }

   while (1) {
      for (uint8 op_type = 0; op_type != NUM_OP_TYPES; op_type++) {
         // granularities[] defines number of operations
         // either insert or lookup in each round
         // op_granularity defines the size of interval or bucket
         // that holds many of rounds, each thread inserts on different
         // op_granularity bucket so that there won't be overlap
         if (offsets[op_type] + granularities[op_type] >= op_granularity) {
            num_ops = op_granularity - offsets[op_type];
         } else {
            num_ops = granularities[op_type];
         }
         if (num_ops > 0) {
            do_operation(params,
                         bases[op_type],
                         num_ops,
                         offsets[op_type],
                         &insert_done,
                         op_type == OP_INSERT);
            offsets[op_type] += num_ops;
         }

         if (num_ops < granularities[op_type]
             || offsets[op_type] >= op_granularity) {
            num_ops = granularities[op_type] - num_ops;
            if (advance_base(params,
                             params->curr_op,
                             bases[op_type],
                             &insert_done,
                             &rs,
                             op_type))
            {
               goto out;
            }
            offsets[op_type] = 0;
            if (num_ops > 0) {
               platform_assert(num_ops < granularities[op_type]);
               do_operation(params,
                            bases[op_type],
                            num_ops,
                            offsets[op_type],
                            &insert_done,
                            op_type == OP_INSERT);
               offsets[op_type] += num_ops;
            }
         }
      }
   }

out:
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle      *spl          = spl_tables[spl_idx];
      verify_tuple_arg   vtarg        = {.stats_only = TRUE,
                                         .stats = &params->lookup_stats[ASYNC_LU]};
      test_async_lookup *async_lookup = params->async_lookup[spl_idx];
      test_wait_for_inflight(spl, async_lookup, &vtarg);
   }

   params->rc = STATUS_OK;
   for (uint8 i = 0; i < NUM_OP_TYPES; i++) {
      platform_free(platform_get_heap_id(), bases[i]);
   }
}


static platform_status
test_trunk_create_tables(trunk_handle  ***spl_handles,
                         trunk_config    *cfg,
                         allocator       *al,
                         cache           *cc[],
                         task_system     *ts,
                         platform_heap_id hid,
                         uint8            num_tables,
                         uint8            num_caches)
{
   trunk_handle **spl_tables = TYPED_ARRAY_ZALLOC(hid, spl_tables, num_tables);
   if (spl_tables == NULL) {
      return STATUS_NO_MEMORY;
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      cache *cache_to_use = num_caches > 1 ? cc[spl_idx] : *cc;
      spl_tables[spl_idx] = trunk_create(&cfg[spl_idx],
                                         al,
                                         cache_to_use,
                                         ts,
                                         test_generate_allocator_root_id(),
                                         hid);
      if (spl_tables[spl_idx] == NULL) {
         for (uint8 del_idx = 0; del_idx < spl_idx; del_idx++) {
            trunk_destroy(spl_tables[del_idx]);
         }
         platform_free(hid, spl_tables);
         return STATUS_NO_MEMORY;
      }
   }
   *spl_handles = spl_tables;
   return STATUS_OK;
}

static void
test_trunk_destroy_tables(trunk_handle   **spl_tables,
                          platform_heap_id hid,
                          uint8            num_tables)
{
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_destroy(spl_tables[spl_idx]);
   }
   platform_free(hid, spl_tables);
}

platform_status
test_splinter_perf(trunk_config    *cfg,
                   test_config     *test_cfg,
                   allocator       *al,
                   cache           *cc[],
                   uint64           num_insert_threads,
                   uint64           num_lookup_threads,
                   uint64           num_range_threads,
                   uint32           max_async_inflight,
                   task_system     *ts,
                   platform_heap_id hid,
                   uint8            num_tables,
                   uint8            num_caches,
                   uint64           insert_rate)
{
   platform_default_log(
      "splinter_test: SplinterDB performance test started with "
      "%d tables\n",
      num_tables);
   trunk_handle  **spl_tables;
   platform_status rc;

   rc = test_trunk_create_tables(
      &spl_tables, cfg, al, cc, ts, hid, num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to create splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64  tuple_size, num_inserts;
   uint64 *per_table_inserts =
      TYPED_ARRAY_MALLOC(hid, per_table_inserts, num_tables);
   uint64 *per_table_ranges =
      TYPED_ARRAY_MALLOC(hid, per_table_ranges, num_tables);
   uint64 *curr_op       = TYPED_ARRAY_ZALLOC(hid, curr_op, num_tables);
   uint64  total_inserts = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size
                   + generator_average_message_size(test_cfg->gen);
      num_inserts = test_cfg[i].tree_size / tuple_size;
      if (test_cfg[i].key_type == TEST_PERIODIC) {
         test_cfg[i].period = num_inserts;
         num_inserts *= test_cfg[i].num_periods;
      }
      per_table_inserts[i] = ROUNDUP(num_inserts, TEST_INSERT_GRANULARITY);
      total_inserts += per_table_inserts[i];
   }

   uint64 num_threads = num_insert_threads;

   if (num_lookup_threads > num_threads) {
      num_threads = num_lookup_threads;
   }
   if (num_range_threads > num_threads) {
      num_threads = num_range_threads;
   }

   test_splinter_thread_params *params =
      TYPED_ARRAY_ZALLOC(hid, params, num_threads);
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl            = spl_tables;
      params[i].test_cfg       = test_cfg;
      params[i].total_ops      = per_table_inserts;
      params[i].curr_op        = curr_op;
      params[i].op_granularity = TEST_INSERT_GRANULARITY;
      params[i].thread_number  = i;
      params[i].expected_found = TRUE;
      params[i].num_tables     = num_tables;
      params[i].ts             = ts;
      params[i].insert_rate    = insert_rate / num_insert_threads;
   }

   uint64 start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("insert_thread",
                               test_trunk_insert_thread,
                               &params[i],
                               trunk_get_scratch_size(),
                               ts,
                               hid,
                               &params[i].thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   for (uint64 i = 0; i < num_tables; i++) {
      task_wait_for_completion(ts);
   }

   uint64    total_time         = platform_timestamp_elapsed(start_time);
   timestamp insert_latency_max = 0;
   uint64    read_io_bytes, write_io_bytes;
   cache_io_stats(cc[0], &read_io_bytes, &write_io_bytes);
   uint64 io_mib    = (read_io_bytes + write_io_bytes) / MiB;
   uint64 bandwidth = io_mib / NSEC_TO_SEC(total_time);

   for (uint64 i = 0; i < num_insert_threads; i++) {
      if (!SUCCESS(params[i].rc)) {
         rc = params[i].rc;
         goto destroy_splinter;
      }
      if (params[i].insert_stats.latency_max > insert_latency_max) {
         insert_latency_max = params[i].insert_stats.latency_max;
      }
   }

   rc = STATUS_OK;

   if (total_inserts > 0) {
      platform_default_log(
         "\nper-splinter per-thread insert time per tuple %lu ns\n",
         total_time * num_insert_threads / total_inserts);
      platform_default_log(
         "splinter total insertion rate: %lu insertions/second\n",
         SEC_TO_NSEC(total_inserts) / total_time);
      platform_default_log("splinter bandwidth: %lu megabytes/second\n",
                           bandwidth);
      platform_default_log("splinter max insert latency: %lu msec\n",
                           NSEC_TO_MSEC(insert_latency_max));
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle *spl = spl_tables[spl_idx];
      cache_assert_free(spl->cc);
      platform_assert(trunk_verify_tree(spl));
      trunk_print_insertion_stats(Platform_default_log_handle, spl);
      cache_print_stats(Platform_default_log_handle, spl->cc);
      trunk_print_space_use(Platform_default_log_handle, spl);
      cache_reset_stats(spl->cc);
      // trunk_print(spl);
   }

   ZERO_CONTENTS_N(curr_op, num_tables);
   if (num_lookup_threads != 0) {
      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_lookup_threads; i++) {
         platform_status ret;

         for (uint8 j = 0; j < num_tables; j++) {
            async_ctxt_init(
               hid, max_async_inflight, &params[i].async_lookup[j]);
         }
         ret = task_thread_create("lookup thread",
                                  test_trunk_lookup_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      total_time = platform_timestamp_elapsed(start_time);

      uint64    num_async_lookups       = 0;
      timestamp sync_lookup_latency_max = 0, async_lookup_latency_max = 0;
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         for (uint8 j = 0; j < num_tables; j++) {
            async_ctxt_deinit(hid, params[i].async_lookup[j]);
            params[i].async_lookup[j] = NULL;
         }
         num_async_lookups += params[i].lookup_stats[ASYNC_LU].num_found
                              + params[i].lookup_stats[ASYNC_LU].num_not_found;
         if (params[i].lookup_stats[SYNC_LU].latency_max
             > sync_lookup_latency_max) {
            sync_lookup_latency_max =
               params[i].lookup_stats[SYNC_LU].latency_max;
         }
         if (params[i].lookup_stats[ASYNC_LU].latency_max
             > async_lookup_latency_max) {
            async_lookup_latency_max =
               params[i].lookup_stats[ASYNC_LU].latency_max;
         }
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread lookup time per tuple %lu ns\n",
         total_time * num_lookup_threads / total_inserts);
      platform_default_log("splinter total lookup rate: %lu lookups/second\n",
                           SEC_TO_NSEC(total_inserts) / total_time);
      platform_default_log("%lu%% lookups were async\n",
                           num_async_lookups * 100 / total_inserts);
      platform_default_log("max lookup latency ns (sync=%lu, async=%lu)\n",
                           sync_lookup_latency_max,
                           async_lookup_latency_max);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

   uint64 total_ranges = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      per_table_ranges[i] =
         ROUNDUP(per_table_inserts[i] / 128, TEST_RANGE_GRANULARITY);
      total_ranges += per_table_ranges[i];
   }

   for (uint64 i = 0; i < num_threads; i++) {
      params[i].total_ops      = per_table_ranges;
      params[i].op_granularity = TEST_RANGE_GRANULARITY;
      params[i].range_min      = 1;
      params[i].range_max      = 100;
   }

   ZERO_CONTENTS_N(curr_op, num_tables);
   if (num_range_threads != 0) {
      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread",
                                  test_trunk_range_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      total_time = platform_timestamp_elapsed(start_time);

      for (uint64 i = 0; i < num_range_threads; i++) {
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread range time per tuple %lu ns\n",
         total_time * num_range_threads / total_ranges);
      platform_default_log("splinter total range rate: %lu ops/second\n",
                           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] =
            ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops      = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min      = 512;
         params[i].range_max      = 1024;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread",
                                  test_trunk_range_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_range_threads; i++)
         platform_thread_join(params[i].thread);

      total_time = platform_timestamp_elapsed(start_time);

      for (uint64 i = 0; i < num_range_threads; i++) {
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread range time per tuple %lu ns\n",
         total_time * num_range_threads / total_ranges);
      platform_default_log("splinter total range rate: %lu ops/second\n",
                           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] =
            ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops      = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min      = 131072 - 16384;
         params[i].range_max      = 131072;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread",
                                  test_trunk_range_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_range_threads; i++)
         platform_thread_join(params[i].thread);

      total_time = platform_timestamp_elapsed(start_time);

      for (uint64 i = 0; i < num_range_threads; i++) {
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread range time per tuple %lu ns\n",
         total_time * num_range_threads / total_ranges);
      platform_default_log("splinter total range rate: %lu ops/second\n",
                           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

destroy_splinter:
   test_trunk_destroy_tables(spl_tables, hid, num_tables);
   platform_default_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(Platform_default_log_handle, cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_op);
   platform_free(hid, per_table_ranges);
   platform_free(hid, per_table_inserts);
   return rc;
}

platform_status
test_splinter_periodic(trunk_config    *cfg,
                       test_config     *test_cfg,
                       allocator       *al,
                       cache           *cc[],
                       uint64           num_insert_threads,
                       uint64           num_lookup_threads,
                       uint64           num_range_threads,
                       uint32           max_async_inflight,
                       task_system     *ts,
                       platform_heap_id hid,
                       uint8            num_tables,
                       uint8            num_caches,
                       uint64           insert_rate)
{
   platform_default_log(
      "splinter_test: SplinterDB performance test (periodic) started with "
      "%d tables\n",
      num_tables);
   trunk_handle  **spl_tables;
   platform_status rc;

   rc = test_trunk_create_tables(
      &spl_tables, cfg, al, cc, ts, hid, num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to create splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64  tuple_size, num_inserts;
   uint64 *per_table_inserts =
      TYPED_ARRAY_MALLOC(hid, per_table_inserts, num_tables);
   uint64 *per_table_ranges =
      TYPED_ARRAY_MALLOC(hid, per_table_ranges, num_tables);
   uint64 *curr_op       = TYPED_ARRAY_ZALLOC(hid, curr_op, num_tables);
   uint64  total_inserts = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size
                   + generator_average_message_size(test_cfg->gen);
      num_inserts = test_cfg[i].tree_size / tuple_size;
      if (test_cfg[i].key_type == TEST_PERIODIC) {
         test_cfg[i].period = num_inserts;
         num_inserts *= test_cfg[i].num_periods;
      }
      per_table_inserts[i] = ROUNDUP(num_inserts, TEST_INSERT_GRANULARITY);
      total_inserts += per_table_inserts[i];
   }

   uint64 num_threads = num_insert_threads;

   if (num_lookup_threads > num_threads) {
      num_threads = num_lookup_threads;
   }
   if (num_range_threads > num_threads) {
      num_threads = num_range_threads;
   }

   test_splinter_thread_params *params =
      TYPED_ARRAY_ZALLOC(hid, params, num_threads);
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl            = spl_tables;
      params[i].test_cfg       = test_cfg;
      params[i].total_ops      = per_table_inserts;
      params[i].curr_op        = curr_op;
      params[i].op_granularity = TEST_INSERT_GRANULARITY;
      params[i].thread_number  = i;
      params[i].expected_found = TRUE;
      params[i].num_tables     = num_tables;
      params[i].ts             = ts;
      params[i].insert_rate    = insert_rate / num_insert_threads;
   }
   uint64 start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("insert_thread",
                               test_trunk_insert_thread,
                               &params[i],
                               trunk_get_scratch_size(),
                               ts,
                               hid,
                               &params[i].thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   for (uint64 i = 0; i < num_tables; i++) {
      task_wait_for_completion(ts);
   }

   uint64    total_time         = platform_timestamp_elapsed(start_time);
   timestamp insert_latency_max = 0;
   uint64    read_io_bytes, write_io_bytes;
   cache_io_stats(cc[0], &read_io_bytes, &write_io_bytes);
   uint64 io_mib    = (read_io_bytes + write_io_bytes) / MiB;
   uint64 bandwidth = io_mib / NSEC_TO_SEC(total_time);

   for (uint64 i = 0; i < num_insert_threads; i++) {
      if (!SUCCESS(params[i].rc)) {
         rc = params[i].rc;
         goto destroy_splinter;
      }
      if (params[i].insert_stats.latency_max > insert_latency_max) {
         insert_latency_max = params[i].insert_stats.latency_max;
      }
   }

   rc = STATUS_OK;

   if (total_inserts > 0) {
      platform_default_log(
         "\nper-splinter per-thread insert time per tuple %lu ns\n",
         total_time * num_insert_threads / total_inserts);
      platform_default_log(
         "splinter total insertion rate: %lu insertions/second\n",
         SEC_TO_NSEC(total_inserts) / total_time);
      platform_default_log("splinter bandwidth: %lu megabytes/second\n",
                           bandwidth);
      platform_default_log("splinter max insert latency: %lu msec\n",
                           NSEC_TO_MSEC(insert_latency_max));
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle *spl = spl_tables[spl_idx];
      cache_assert_free(spl->cc);
      platform_assert(trunk_verify_tree(spl));
      trunk_print_insertion_stats(Platform_default_log_handle, spl);
      cache_print_stats(Platform_default_log_handle, spl->cc);
      trunk_print_space_use(Platform_default_log_handle, spl);
      cache_reset_stats(spl->cc);
   }

   ZERO_CONTENTS_N(curr_op, num_tables);

   for (uint64 repeat_round = 0; repeat_round < 10; repeat_round++) {
      platform_default_log("Beginning repeat round %lu\n", repeat_round);
      platform_error_log("Beginning repeat round %lu\n", repeat_round);
      /*
       * **********
       */
      for (uint64 i = 0; i < num_insert_threads; i++) {
         platform_status ret;
         ret = task_thread_create("insert_thread",
                                  test_trunk_insert_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_insert_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      for (uint64 i = 0; i < num_tables; i++) {
         task_wait_for_completion(ts);
      }

      total_time         = platform_timestamp_elapsed(start_time);
      insert_latency_max = 0;
      cache_io_stats(cc[0], &read_io_bytes, &write_io_bytes);
      io_mib    = (read_io_bytes + write_io_bytes) / MiB;
      bandwidth = io_mib / NSEC_TO_SEC(total_time);

      for (uint64 i = 0; i < num_insert_threads; i++) {
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
         if (params[i].insert_stats.latency_max > insert_latency_max) {
            insert_latency_max = params[i].insert_stats.latency_max;
         }
      }

      rc = STATUS_OK;

      if (total_inserts > 0) {
         platform_default_log(
            "\nper-splinter per-thread insert time per tuple %lu ns\n",
            total_time * num_insert_threads / total_inserts);
         platform_default_log(
            "splinter total insertion rate: %lu insertions/second\n",
            SEC_TO_NSEC(total_inserts) / total_time);
         platform_default_log("splinter bandwidth: %lu megabytes/second\n",
                              bandwidth);
         platform_default_log("splinter max insert latency: %lu msec\n",
                              NSEC_TO_MSEC(insert_latency_max));
      }

      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         platform_assert(trunk_verify_tree(spl));
         trunk_print_insertion_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         trunk_print_space_use(Platform_default_log_handle, spl);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);

      /*
       * **********
       */
   }

   if (num_lookup_threads != 0) {
      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_lookup_threads; i++) {
         platform_status ret;

         for (uint8 j = 0; j < num_tables; j++) {
            async_ctxt_init(
               hid, max_async_inflight, &params[i].async_lookup[j]);
         }
         ret = task_thread_create("lookup thread",
                                  test_trunk_lookup_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      total_time = platform_timestamp_elapsed(start_time);

      uint64    num_async_lookups       = 0;
      timestamp sync_lookup_latency_max = 0, async_lookup_latency_max = 0;
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         for (uint8 j = 0; j < num_tables; j++) {
            async_ctxt_deinit(hid, params[i].async_lookup[j]);
            params[i].async_lookup[j] = NULL;
         }
         num_async_lookups += params[i].lookup_stats[ASYNC_LU].num_found
                              + params[i].lookup_stats[ASYNC_LU].num_not_found;
         if (params[i].lookup_stats[SYNC_LU].latency_max
             > sync_lookup_latency_max) {
            sync_lookup_latency_max =
               params[i].lookup_stats[SYNC_LU].latency_max;
         }
         if (params[i].lookup_stats[ASYNC_LU].latency_max
             > async_lookup_latency_max) {
            async_lookup_latency_max =
               params[i].lookup_stats[ASYNC_LU].latency_max;
         }
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread lookup time per tuple %lu ns\n",
         total_time * num_lookup_threads / total_inserts);
      platform_default_log("splinter total lookup rate: %lu lookups/second\n",
                           SEC_TO_NSEC(total_inserts) / total_time);
      platform_default_log("%lu%% lookups were async\n",
                           num_async_lookups * 100 / total_inserts);
      platform_default_log("max lookup latency ns (sync=%lu, async=%lu)\n",
                           sync_lookup_latency_max,
                           async_lookup_latency_max);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

   uint64 total_ranges = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      per_table_ranges[i] =
         ROUNDUP(per_table_inserts[i] / 128, TEST_RANGE_GRANULARITY);
      total_ranges += per_table_ranges[i];
   }

   for (uint64 i = 0; i < num_threads; i++) {
      params[i].total_ops      = per_table_ranges;
      params[i].op_granularity = TEST_RANGE_GRANULARITY;
      params[i].range_min      = 1;
      params[i].range_max      = 100;
   }

   ZERO_CONTENTS_N(curr_op, num_tables);
   if (num_range_threads != 0) {
      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread",
                                  test_trunk_range_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      total_time = platform_timestamp_elapsed(start_time);

      for (uint64 i = 0; i < num_range_threads; i++) {
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread range time per tuple %lu ns\n",
         total_time * num_range_threads / total_ranges);
      platform_default_log("splinter total range rate: %lu ops/second\n",
                           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] =
            ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops      = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min      = 512;
         params[i].range_max      = 1024;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread",
                                  test_trunk_range_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_range_threads; i++)
         platform_thread_join(params[i].thread);

      total_time = platform_timestamp_elapsed(start_time);

      for (uint64 i = 0; i < num_range_threads; i++) {
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread range time per tuple %lu ns\n",
         total_time * num_range_threads / total_ranges);
      platform_default_log("splinter total range rate: %lu ops/second\n",
                           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] =
            ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops      = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min      = 131072 - 16384;
         params[i].range_max      = 131072;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread",
                                  test_trunk_range_thread,
                                  &params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &params[i].thread);
         if (!SUCCESS(ret)) {
            return ret;
         }
      }
      for (uint64 i = 0; i < num_range_threads; i++)
         platform_thread_join(params[i].thread);

      total_time = platform_timestamp_elapsed(start_time);

      for (uint64 i = 0; i < num_range_threads; i++) {
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_default_log(
         "\nper-splinter per-thread range time per tuple %lu ns\n",
         total_time * num_range_threads / total_ranges);
      platform_default_log("splinter total range rate: %lu ops/second\n",
                           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

destroy_splinter:
   test_trunk_destroy_tables(spl_tables, hid, num_tables);
   platform_default_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(Platform_default_log_handle, cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_op);
   platform_free(hid, per_table_ranges);
   platform_free(hid, per_table_inserts);
   return rc;
}
platform_status
test_splinter_parallel_perf(trunk_config    *cfg,
                            test_config     *test_cfg,
                            allocator       *al,
                            cache           *cc[],
                            uint64           seed,
                            const uint64     num_threads,
                            const uint64     num_inserts_per_thread,
                            const uint64     num_lookups_per_thread,
                            uint32           max_async_inflight,
                            uint8            lookup_positive_pct,
                            task_system     *ts,
                            platform_heap_id hid,
                            uint8            num_tables,
                            uint8            num_caches)
{
   platform_default_log(
      "splinter_test: SplinterDB parallel performance test started with "
      "%d tables\n",
      num_tables);
   trunk_handle  **spl_tables;
   platform_status rc;

   platform_assert(num_inserts_per_thread <= num_lookups_per_thread);

   rc = test_trunk_create_tables(
      &spl_tables, cfg, al, cc, ts, hid, num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to create splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64  tuple_size, num_inserts;
   uint64 *per_table_inserts =
      TYPED_ARRAY_MALLOC(hid, per_table_inserts, num_tables);
   uint64 *curr_insert_op = TYPED_ARRAY_ZALLOC(hid, curr_insert_op, num_tables);
   uint64  total_inserts  = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size
                   + generator_average_message_size(test_cfg->gen);
      num_inserts          = test_cfg[i].tree_size / tuple_size;
      per_table_inserts[i] = ROUNDUP(num_inserts, TEST_INSERT_GRANULARITY);
      total_inserts += per_table_inserts[i];
   }

   test_splinter_thread_params *params =
      TYPED_ARRAY_ZALLOC(hid, params, num_threads);
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl                           = spl_tables;
      params[i].test_cfg                      = test_cfg;
      params[i].total_ops                     = per_table_inserts;
      params[i].op_granularity                = TEST_INSERT_GRANULARITY;
      params[i].thread_number                 = i;
      params[i].num_tables                    = num_tables;
      params[i].ts                            = ts;
      params[i].curr_op                       = curr_insert_op;
      params[i].num_ops_per_thread[OP_INSERT] = num_inserts_per_thread;
      params[i].num_ops_per_thread[OP_LOOKUP] = num_lookups_per_thread;
      params[i].lookup_positive_pct           = lookup_positive_pct;
      params[i].seed                          = seed + i; // unique seed
   }

   uint64 start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_threads; i++) {
      platform_status ret;

      for (uint8 j = 0; j < num_tables; j++) {
         async_ctxt_init(hid, max_async_inflight, &params[i].async_lookup[j]);
      }
      ret = task_thread_create("insert/lookup thread",
                               test_trunk_insert_lookup_thread,
                               &params[i],
                               trunk_get_scratch_size(),
                               ts,
                               hid,
                               &params[i].thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }

   for (uint64 i = 0; i < num_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   uint64    total_time                = platform_timestamp_elapsed(start_time);
   timestamp insert_latency_max        = 0;
   uint64    total_sync_lookups_found  = 0;
   uint64    total_async_lookups_found = 0;
   uint64    total_sync_lookups_not_found  = 0;
   uint64    total_async_lookups_not_found = 0;
   timestamp sync_lookup_latency_max = 0, async_lookup_latency_max = 0;
   uint64    total_insert_duration = 0;

   for (uint64 i = 0; i < num_threads; i++) {
      for (uint8 j = 0; j < num_tables; j++) {
         async_ctxt_deinit(hid, params[i].async_lookup[j]);
         params[i].async_lookup[j] = NULL;
      }
      if (params[i].insert_stats.latency_max > insert_latency_max) {
         insert_latency_max = params[i].insert_stats.latency_max;
      }
      if (params[i].lookup_stats[SYNC_LU].latency_max > sync_lookup_latency_max)
      {
         sync_lookup_latency_max = params[i].lookup_stats[SYNC_LU].latency_max;
      }
      if (params[i].lookup_stats[ASYNC_LU].latency_max
          > async_lookup_latency_max) {
         async_lookup_latency_max =
            params[i].lookup_stats[ASYNC_LU].latency_max;
      }
      if (!SUCCESS(params[i].rc)) {
         rc = params[i].rc;
         goto destroy_splinter;
      }

      total_sync_lookups_found += params[i].lookup_stats[SYNC_LU].num_found;
      total_sync_lookups_not_found +=
         params[i].lookup_stats[SYNC_LU].num_not_found;
      total_async_lookups_found += params[i].lookup_stats[ASYNC_LU].num_found;
      total_async_lookups_not_found +=
         params[i].lookup_stats[ASYNC_LU].num_not_found;
      total_insert_duration += params[i].insert_stats.duration;
   }

   if (num_threads > 0) {
      platform_default_log(
         "\nper-splinter per-thread insert time per tuple %lu ns\n",
         total_time * num_threads / total_inserts);
      platform_default_log(
         "splinter total insertion rate: %lu insertions/second\n",
         SEC_TO_NSEC(total_inserts) / total_time);
      platform_default_log(
         "splinter pure insertion rate: %lu insertions/second\n",
         SEC_TO_NSEC(total_inserts) / total_insert_duration * num_threads);
      platform_default_log("splinter max insert latency: %lu msec\n",
                           NSEC_TO_MSEC(insert_latency_max));
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle *spl = spl_tables[spl_idx];
      trunk_print_insertion_stats(Platform_default_log_handle, spl);
   }

   if (num_threads > 0) {
      uint64 num_async_lookups =
         total_async_lookups_found + total_async_lookups_not_found;
      uint64 total_lookups = total_sync_lookups_found
                             + total_sync_lookups_not_found + num_async_lookups;
      platform_default_log(
         "\nper-splinter per-thread lookup time per tuple %lu ns\n",
         total_time * num_threads / total_lookups);
      platform_default_log("splinter total lookup rate: %lu lookups/second\n",
                           SEC_TO_NSEC(total_lookups) / total_time);
      platform_default_log(
         "splinter total lookup found: (sync=%lu, async=%lu)\n",
         total_sync_lookups_found,
         total_async_lookups_found);
      platform_default_log(
         "splinter total lookup not found: (sync=%lu, async=%lu)\n",
         total_sync_lookups_not_found,
         total_async_lookups_not_found);
      platform_default_log("%lu%% lookups were async\n",
                           num_async_lookups * 100 / total_lookups);
      platform_default_log("max lookup latency ns (sync=%lu, async=%lu)\n",
                           sync_lookup_latency_max,
                           async_lookup_latency_max);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         trunk_handle *spl = spl_tables[spl_idx];
         trunk_print_lookup_stats(Platform_default_log_handle, spl);
         cache_print_stats(Platform_default_log_handle, spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

destroy_splinter:
   test_trunk_destroy_tables(spl_tables, hid, num_tables);
   platform_default_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(Platform_default_log_handle, cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_insert_op);
   platform_free(hid, per_table_inserts);
   return rc;
}

platform_status
test_splinter_delete(trunk_config    *cfg,
                     test_config     *test_cfg,
                     allocator       *al,
                     cache           *cc[],
                     uint64           num_insert_threads,
                     uint64           num_lookup_threads,
                     uint32           max_async_inflight,
                     task_system     *ts,
                     platform_heap_id hid,
                     uint8            num_tables,
                     uint8            num_caches,
                     uint64           insert_rate)
{
   platform_default_log("splinter_test: SplinterDB deletion test started with "
                        "%d tables\n",
                        num_tables);
   trunk_handle  **spl_tables;
   platform_status rc;

   rc = test_trunk_create_tables(
      &spl_tables, cfg, al, cc, ts, hid, num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to initialize splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64  tuple_size, num_inserts;
   uint64 *per_table_inserts =
      TYPED_ARRAY_MALLOC(hid, per_table_inserts, num_tables);
   uint64 *curr_op       = TYPED_ARRAY_ZALLOC(hid, curr_op, num_tables);
   uint64  total_inserts = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size
                   + generator_average_message_size(test_cfg->gen);
      num_inserts          = test_cfg[i].tree_size / tuple_size;
      per_table_inserts[i] = ROUNDUP(num_inserts, TEST_INSERT_GRANULARITY);
      total_inserts += per_table_inserts[i];
   }
   uint64 num_threads = num_insert_threads;

   if (num_lookup_threads > num_threads) {
      num_threads = num_lookup_threads;
   }
   test_splinter_thread_params *params =
      TYPED_ARRAY_MALLOC(hid, params, num_threads);
   platform_assert(params);

   ZERO_CONTENTS_N(params, num_threads);
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl            = spl_tables;
      params[i].test_cfg       = test_cfg;
      params[i].total_ops      = per_table_inserts;
      params[i].curr_op        = curr_op;
      params[i].op_granularity = TEST_INSERT_GRANULARITY;
      params[i].thread_number  = i;
      params[i].num_tables     = num_tables;
      params[i].ts             = ts;
      params[i].insert_rate    = insert_rate / num_insert_threads;
   }

   uint64 total_time, start_time = platform_get_timestamp();

   // Insert: round 1
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("insert thread",
                               test_trunk_insert_thread,
                               &params[i],
                               trunk_get_scratch_size(),
                               ts,
                               hid,
                               &params[i].thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   total_time = platform_timestamp_elapsed(start_time);
   platform_default_log(
      "\nper-splinter per-thread insert time per tuple %lu ns\n",
      total_time * num_insert_threads / total_inserts);
   platform_default_log(
      "splinter total insertion rate: %lu insertions/second\n",
      SEC_TO_NSEC(total_inserts) / total_time);
   platform_default_log("After inserts:\n");
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle *spl = spl_tables[spl_idx];
      trunk_print_insertion_stats(Platform_default_log_handle, spl);
      cache_print_stats(Platform_default_log_handle, spl->cc);
   }

   for (uint64 i = 0; i < num_insert_threads; i++) {
      if (!SUCCESS(params[i].rc)) {
         rc = params[i].rc;
         goto destroy_splinter;
      }
   }

   rc = STATUS_OK;

   // Deletes
   message_generate_set_message_type(test_cfg->gen, MESSAGE_TYPE_DELETE);
   ZERO_CONTENTS_N(curr_op, num_tables);
   start_time = platform_get_timestamp();
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("delete thread",
                               test_trunk_insert_thread,
                               &params[i],
                               trunk_get_scratch_size(),
                               ts,
                               hid,
                               &params[i].thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++)
      platform_thread_join(params[i].thread);

   total_time = platform_timestamp_elapsed(start_time);
   platform_default_log(
      "\nper-splinter per-thread delete time per tuple %lu ns\n",
      total_time * num_insert_threads / total_inserts);
   platform_default_log("splinter total deletion rate: %lu insertions/second\n",
                        SEC_TO_NSEC(total_inserts) / total_time);
   platform_default_log("After deletes:\n");
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle *spl = spl_tables[spl_idx];
      trunk_print_insertion_stats(Platform_default_log_handle, spl);
      cache_print_stats(Platform_default_log_handle, spl->cc);
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle *spl = spl_tables[spl_idx];
      trunk_force_flush(spl);
      platform_default_log("After flushing table %d:\n", spl_idx);
      trunk_print_insertion_stats(Platform_default_log_handle, spl);
      cache_print_stats(Platform_default_log_handle, spl->cc);
   }

   for (uint64 i = 0; i < num_insert_threads; i++) {
      if (!SUCCESS(params[i].rc)) {
         rc = params[i].rc;
         goto destroy_splinter;
      }
   }

   rc = STATUS_OK;

   // Lookups
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].expected_found = FALSE;
   }
   ZERO_CONTENTS_N(curr_op, num_tables);
   start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_lookup_threads; i++) {
      for (uint8 j = 0; j < num_tables; j++) {
         async_ctxt_init(hid, max_async_inflight, &params[i].async_lookup[j]);
      }
      rc = task_thread_create("lookup thread",
                              test_trunk_lookup_thread,
                              &params[i],
                              trunk_get_scratch_size(),
                              ts,
                              hid,
                              &params[i].thread);
      if (!SUCCESS(rc)) {
         for (uint64 j = 0; j < i; j++) {
            platform_thread_join(params[i].thread);
         }
         goto destroy_splinter;
      }
   }
   for (uint64 i = 0; i < num_lookup_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   total_time = platform_timestamp_elapsed(start_time);

   uint64 num_async_lookups = 0;
   for (uint64 i = 0; i < num_lookup_threads; i++) {
      for (uint8 j = 0; j < num_tables; j++) {
         async_ctxt_deinit(hid, params[i].async_lookup[j]);
         params[i].async_lookup[j] = NULL;
      }
      num_async_lookups += params[i].lookup_stats[ASYNC_LU].num_found
                           + params[i].lookup_stats[ASYNC_LU].num_not_found;
      if (!SUCCESS(params[i].rc)) {
         // XXX cleanup properly
         rc = params[i].rc;
         goto destroy_splinter;
      }
   }

   rc = STATUS_OK;

   platform_default_log(
      "\nper-splinter per-thread lookup time per tuple %lu ns\n",
      total_time * num_lookup_threads / total_inserts);
   platform_default_log("splinter total lookup rate: %lu lookups/second\n",
                        SEC_TO_NSEC(total_inserts) / total_time);
   platform_default_log("%lu%% lookups were async\n",
                        num_async_lookups * 100 / total_inserts);
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      trunk_handle *spl = spl_tables[spl_idx];
      trunk_print_lookup_stats(Platform_default_log_handle, spl);
      cache_print_stats(Platform_default_log_handle, spl->cc);
   }

destroy_splinter:
   test_trunk_destroy_tables(spl_tables, hid, num_tables);
   platform_default_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(Platform_default_log_handle, cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_op);
   platform_free(hid, per_table_inserts);
   return rc;
}

static void
usage(const char *argv0)
{
   platform_error_log(
      "Usage:\n"
      "\t%s --perf --max-async-inflight [num] --num-insert-threads [num]\n"
      "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
      "\t%s --delete --max-async-inflight [num] --num-insert-threads [num]\n"
      "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
      "\t%s --seq-perf --max-async-inflight [num] --num-insert-threads [num]\n"
      "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
      "\t%s --semiseq-perf --max-async-inflight [num] --num-insert-threads "
      "[num]\n"
      "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
      "\t%s --functionality NUM_INSERTS CORRECTNESS_CHECK_FREQUENCY\n"
      "\t   --max-async-inflight [num]\n"
      "\t%s --num-tables (number of tables to use for test)\n"
      "\t%s --cache-per-table\n"
      "\t%s --parallel-perf --max-async-inflight [num] --num-pthreads [num] "
      "--lookup-positive-percent [num] --seed [num]\n"
      "\t%s --num-bg-threads (number of background threads)\n"
      "\t%s --insert-rate (inserts_done_by_all_threads in a second)\n",
      argv0,
      argv0,
      argv0,
      argv0,
      argv0,
      argv0,
      argv0,
      argv0,
      argv0,
      argv0);
   platform_error_log("\nNOTE: splinter_basic basic has been refactored"
                      " to run as a stand-alone unit-test.\n");
   test_config_usage();
   config_usage();
}

static int
splinter_test_parse_perf_args(char ***argv,
                              int    *argc,
                              uint32 *max_async_inflight,
                              uint32 *num_insert_threads,
                              uint32 *num_lookup_threads,
                              uint32 *num_range_lookup_threads,
                              uint32 *num_pthreads,
                              uint8  *lookup_positive_pct)
{
   if (*argc > 1
       && strncmp(
             (*argv)[0], "--max-async-inflight", sizeof("--max-async-inflight"))
             == 0)
   {
      if (!try_string_to_uint32((*argv)[1], max_async_inflight)) {
         return -1;
      }
      if (*max_async_inflight > TEST_MAX_ASYNC_INFLIGHT) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1
       && strncmp(
             (*argv)[0], "--num-insert-threads", sizeof("--num-insert-threads"))
             == 0)
   {
      if (!try_string_to_uint32((*argv)[1], num_insert_threads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1
       && strncmp(
             (*argv)[0], "--num-lookup-threads", sizeof("--num-lookup-threads"))
             == 0)
   {
      if (!try_string_to_uint32((*argv)[1], num_lookup_threads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1
       && strncmp((*argv)[0],
                  "--num-range-lookup-threads",
                  sizeof("--num-range-lookup-threads"))
             == 0)
   {
      if (!try_string_to_uint32((*argv)[1], num_range_lookup_threads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1
       && strncmp((*argv)[0], "--num-pthreads", sizeof("--num-pthreads")) == 0)
   {
      if (!try_string_to_uint32((*argv)[1], num_pthreads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1
       && strncmp((*argv)[0],
                  "--lookup-positive-percent",
                  sizeof("--lookup-positive-percent"))
             == 0)
   {
      if (!try_string_to_uint8((*argv)[1], lookup_positive_pct)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }

   return 0;
}

int
splinter_test(int argc, char *argv[])
{
   io_config           io_cfg;
   rc_allocator_config al_cfg;
   shard_log_config    log_cfg;
   int                 config_argc;
   char              **config_argv;
   test_type           test;
   platform_status     rc;
   uint64              seed = 0;
   uint64              test_ops;
   uint64              correctness_check_frequency;
   // Max async IOs inflight per-thread
   uint32 num_insert_threads, num_lookup_threads;
   uint32 num_range_lookup_threads, max_async_inflight;
   uint32 num_pthreads    = 0;
   uint8  num_tables      = 1;
   bool   cache_per_table = FALSE;
   // no bg threads by default.
   uint8                  num_bg_threads[NUM_TASK_TYPES] = {0};
   uint64                 insert_rate = 0; // no rate throttling by default.
   task_system           *ts;
   uint8                  lookup_positive_pct = 0;
   test_message_generator gen;

   // Defaults
   num_insert_threads = num_lookup_threads = num_range_lookup_threads = 1;
   max_async_inflight                                                 = 64;
   /*
    * 1. Parse splinter_test options, see usage()
    */
   if (argc > 1 && strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
      test                     = perf;
      config_argc              = argc - 2;
      config_argv              = argv + 2;
      num_insert_threads       = 14;
      num_lookup_threads       = 20;
      num_range_lookup_threads = 10;
   } else if (argc > 1 && strncmp(argv[1], "--delete", sizeof("--delete")) == 0)
   {
      test               = delete;
      config_argc        = argc - 2;
      config_argv        = argv + 2;
      num_insert_threads = 14;
   } else if (argc > 1
              && strncmp(argv[1], "--seq-perf", sizeof("--seq-perf")) == 0)
   {
      test        = seq_perf;
      config_argc = argc - 2;
      config_argv = argv + 2;
   } else if (argc > 1
              && strncmp(argv[1], "--semiseq-perf", sizeof("--semiseq-perf"))
                    == 0)
   {
      test        = semiseq_perf;
      config_argc = argc - 2;
      config_argv = argv + 2;
   } else if (argc > 1
              && strncmp(argv[1], "--parallel-perf", sizeof("--parallel-perf"))
                    == 0)
   {
      test                = parallel_perf;
      config_argc         = argc - 2;
      config_argv         = argv + 2;
      num_pthreads        = 10;
      lookup_positive_pct = 50;
   } else if (argc > 1
              && strncmp(argv[1], "--functionality", sizeof("--functionality"))
                    == 0)
   {
      if (argc < 4) {
         usage(argv[0]);
         return -1;
      }
      test = functionality;
      if (0 || !try_string_to_uint64(argv[2], &test_ops)
          || !try_string_to_uint64(argv[3], &correctness_check_frequency))
      {
         usage(argv[0]);
         return -1;
      }
      config_argc = argc - 4;
      config_argv = argv + 4;
   } else if (argc > 1
              && strncmp(argv[1], "--periodic", sizeof("--periodic")) == 0)
   {
      test                     = periodic;
      config_argc              = argc - 2;
      config_argv              = argv + 2;
      num_insert_threads       = 14;
      num_lookup_threads       = 0;
      num_range_lookup_threads = 0;
   } else {
      test        = basic;
      config_argc = argc - 1;
      config_argv = argv + 1;
   }
   if (config_argc > 0
       && strncmp(config_argv[0], "--num-tables", sizeof("--num-tables")) == 0)
   {
      if (config_argc < 2) {
         usage(argv[0]);
         return -1;
      }
      if (!try_string_to_uint8(config_argv[1], &num_tables)) {
         usage(argv[0]);
         return -1;
      }
      config_argc -= 2;
      config_argv += 2;
   }
   if (config_argc > 0
       && strncmp(
             config_argv[0], "--cache-per-table", sizeof("--cache-per-table"))
             == 0)
   {
      cache_per_table = TRUE;
      config_argc -= 1;
      config_argv += 1;
   }
   if (config_argc > 0
       && strncmp(
             config_argv[0], "--num-bg-threads", sizeof("--num-bg-threads"))
             == 0)
   {
      if (!try_string_to_uint8(config_argv[1],
                               &num_bg_threads[TASK_TYPE_NORMAL])) {
         usage(argv[0]);
         return -1;
      }
      config_argc -= 2;
      config_argv += 2;
   }
   if (config_argc > 0
       && strncmp(config_argv[0],
                  "--num-memtable-bg-threads",
                  sizeof("--num-bg-threads"))
             == 0)
   {
      if (!try_string_to_uint8(config_argv[1],
                               &num_bg_threads[TASK_TYPE_MEMTABLE])) {
         usage(argv[0]);
         return -1;
      }
      config_argc -= 2;
      config_argv += 2;
   }
   if (splinter_test_parse_perf_args(&config_argv,
                                     &config_argc,
                                     &max_async_inflight,
                                     &num_insert_threads,
                                     &num_lookup_threads,
                                     &num_range_lookup_threads,
                                     &num_pthreads,
                                     &lookup_positive_pct)
       != 0)
   {
      usage(argv[0]);
      return -1;
   }

   if (config_argc > 0
       && strncmp(config_argv[0], "--insert-rate", sizeof("--insert-rate"))
             == 0)
   {
      if (config_argc < 2) {
         usage(argv[0]);
         return -1;
      }

      if (!try_string_to_uint64(config_argv[1], &insert_rate)) {
         usage(argv[0]);
         return -1;
      }

      config_argc -= 2;
      config_argv += 2;
   }

   /*
    * Allocate 1 GiB for each cache, or 0.5 GiB for each splinter table,
    * whichever is greater.
    * Heap capacity should be within [2 * GiB, UINT32_MAX].
    */
   uint8  num_caches    = cache_per_table ? num_tables : 1;
   uint64 heap_capacity = MAX(1024 * MiB * num_caches, 512 * MiB * num_tables);
   heap_capacity        = MIN(heap_capacity, UINT32_MAX);
   heap_capacity        = MAX(heap_capacity, 2 * GiB);

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;
   rc =
      platform_heap_create(platform_get_module_id(), heap_capacity, &hh, &hid);
   platform_assert_status_ok(rc);

   /*
    * 2. Parse test_config options, see test_config_usage()
    */
   test_config *test_cfg = TYPED_ARRAY_MALLOC(hid, test_cfg, num_tables);
   for (uint8 i = 0; i < num_tables; i++) {
      test_config_set_defaults(test, &test_cfg[i]);
      test_cfg[i].gen = &gen;
   }
   rc = test_config_parse(test_cfg, num_tables, &config_argc, &config_argv);
   if (!SUCCESS(rc)) {
      platform_error_log("splinter_test: failed to parse config: %s\n",
                         platform_status_to_string(rc));
      usage(argv[0]);
      goto heap_destroy;
   }

   /*
    * 3. Parse trunk_config options, see config_usage()
    */
   trunk_config *splinter_cfg =
      TYPED_ARRAY_MALLOC(hid, splinter_cfg, num_tables);
   data_config       *data_cfg;
   clockcache_config *cache_cfg =
      TYPED_ARRAY_MALLOC(hid, cache_cfg, num_tables);
   test_exec_config test_exec_cfg;
   ZERO_STRUCT(test_exec_cfg);

   rc = test_parse_args_n(splinter_cfg,
                          &data_cfg,
                          &io_cfg,
                          &al_cfg,
                          cache_cfg,
                          &log_cfg,
                          &test_exec_cfg,
                          &gen,
                          num_tables,
                          config_argc,
                          config_argv);
   // if there are multiple cache capacity, cache_per_table needs to be TRUE
   bool multi_cap = FALSE;
   for (uint8 i = 0; i < num_tables; i++) {
      if (cache_cfg[i].capacity != cache_cfg[0].capacity) {
         multi_cap = TRUE;
         break;
      }
   }
   if (multi_cap && !cache_per_table) {
      platform_error_log("Multiple cache-capacity set but cache-per-table is "
                         "not set\n");
      rc = STATUS_BAD_PARAM;
   }
   if (!SUCCESS(rc)) {
      platform_error_log("splinter_test: failed to parse config: %s\n",
                         platform_status_to_string(rc));
      usage(argv[0]);
      goto cfg_free;
   }

   seed = test_exec_cfg.seed;

   // Max active threads
   uint32 total_threads = num_lookup_threads;
   if (total_threads < num_insert_threads) {
      total_threads = num_insert_threads;
   }
   if (total_threads < num_pthreads) {
      total_threads = num_pthreads;
   }
   if (num_bg_threads[TASK_TYPE_NORMAL] != 0
       && num_bg_threads[TASK_TYPE_MEMTABLE] == 0)
   {
      num_bg_threads[TASK_TYPE_MEMTABLE] = num_tables;
   }
   for (task_type type = 0; type != NUM_TASK_TYPES; type++) {
      total_threads += num_bg_threads[type];
   }
   // Check if IO subsystem has enough reqs for max async IOs inflight
   if (io_cfg.async_queue_size < total_threads * max_async_inflight) {
      io_cfg.async_queue_size = ROUNDUP(total_threads * max_async_inflight, 32);
      platform_default_log("Bumped up IO queue size to %lu\n",
                           io_cfg.async_queue_size);
   }
   if (io_cfg.kernel_queue_size < total_threads * max_async_inflight) {
      io_cfg.kernel_queue_size =
         ROUNDUP(total_threads * max_async_inflight, 32);
      platform_default_log("Bumped up IO queue size to %lu\n",
                           io_cfg.kernel_queue_size);
   }

   platform_io_handle *io = TYPED_MALLOC(hid, io);
   platform_assert(io != NULL);
   rc = io_handle_init(io, &io_cfg, hh, hid);
   if (!SUCCESS(rc)) {
      goto io_free;
   }

   bool use_bg_threads = num_bg_threads[TASK_TYPE_NORMAL] != 0;

   rc = test_init_task_system(
      hid, io, &ts, splinter_cfg->use_stats, use_bg_threads, num_bg_threads);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(rc));
      goto handle_deinit;
   }

   rc_allocator al;
   rc_allocator_init(
      &al, &al_cfg, (io_handle *)io, hh, hid, platform_get_module_id());

   platform_error_log("Running splinter_test with %d caches\n", num_caches);
   clockcache *cc = TYPED_ARRAY_MALLOC(hid, cc, num_caches);
   platform_assert(cc != NULL);
   for (uint8 idx = 0; idx < num_caches; idx++) {
      rc = clockcache_init(&cc[idx],
                           &cache_cfg[idx],
                           (io_handle *)io,
                           (allocator *)&al,
                           "test",
                           ts,
                           hh,
                           hid,
                           platform_get_module_id());
      platform_assert_status_ok(rc);
   }
   allocator *alp = (allocator *)&al;

   // Allocate an array of cache pointers to pass around.
   cache **caches = TYPED_ARRAY_MALLOC(hid, caches, num_caches);
   platform_assert(caches != NULL);
   for (uint8 i = 0; i < num_caches; i++) {
      caches[i] = (cache *)&cc[i];
   }

   switch (test) {
      case perf:
         rc = test_splinter_perf(splinter_cfg,
                                 test_cfg,
                                 alp,
                                 caches,
                                 num_insert_threads,
                                 num_lookup_threads,
                                 num_range_lookup_threads,
                                 max_async_inflight,
                                 ts,
                                 hid,
                                 num_tables,
                                 num_caches,
                                 insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case delete:
         rc = test_splinter_delete(splinter_cfg,
                                   test_cfg,
                                   alp,
                                   caches,
                                   num_insert_threads,
                                   num_lookup_threads,
                                   max_async_inflight,
                                   ts,
                                   hid,
                                   num_tables,
                                   num_caches,
                                   insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case seq_perf:
         rc = test_splinter_perf(splinter_cfg,
                                 test_cfg,
                                 alp,
                                 caches,
                                 num_insert_threads,
                                 num_lookup_threads,
                                 num_range_lookup_threads,
                                 max_async_inflight,
                                 ts,
                                 hid,
                                 num_tables,
                                 num_caches,
                                 insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case semiseq_perf:
         rc = test_splinter_perf(splinter_cfg,
                                 test_cfg,
                                 alp,
                                 caches,
                                 num_insert_threads,
                                 num_lookup_threads,
                                 num_range_lookup_threads,
                                 max_async_inflight,
                                 ts,
                                 hid,
                                 num_tables,
                                 num_caches,
                                 insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case parallel_perf:
         platform_assert(max_async_inflight == 0 || use_bg_threads);
         rc = test_splinter_parallel_perf(splinter_cfg,
                                          test_cfg,
                                          alp,
                                          caches,
                                          seed,
                                          num_pthreads,
                                          3,
                                          7,
                                          max_async_inflight,
                                          lookup_positive_pct,
                                          ts,
                                          hid,
                                          num_tables,
                                          num_caches);
         platform_assert_status_ok(rc);
         break;
      case periodic:
         rc = test_splinter_periodic(splinter_cfg,
                                     test_cfg,
                                     alp,
                                     caches,
                                     num_insert_threads,
                                     num_lookup_threads,
                                     num_range_lookup_threads,
                                     max_async_inflight,
                                     ts,
                                     hid,
                                     num_tables,
                                     num_caches,
                                     insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case functionality:
         for (uint8 i = 0; i < num_tables; i++) {
            splinter_cfg[i].data_cfg->key_to_string =
               test_data_config->key_to_string;
         }
         rc = test_functionality(alp,
                                 (io_handle *)io,
                                 caches,
                                 splinter_cfg,
                                 seed,
                                 test_ops,
                                 correctness_check_frequency,
                                 ts,
                                 hh,
                                 hid,
                                 num_tables,
                                 num_caches,
                                 max_async_inflight);
         platform_assert_status_ok(rc);
         break;

      case basic:
         platform_assert(
            FALSE,
            "Error in argument processing."
            "splinter_test:'basic' test case has been refactored to"
            " run as a unit-test.");
         break;

      default:
         platform_assert(0);
   }

   for (uint8 idx = 0; idx < num_caches; idx++) {
      clockcache_deinit(&cc[idx]);
   }
   platform_free(hid, caches);
   platform_free(hid, cc);
   allocator_assert_noleaks(alp);
   rc_allocator_deinit(&al);
   test_deinit_task_system(hid, ts);
handle_deinit:
   io_handle_deinit(io);
io_free:
   platform_free(hid, io);
cfg_free:
   platform_free(hid, cache_cfg);
   platform_free(hid, splinter_cfg);
   platform_free(hid, test_cfg);
heap_destroy:
   platform_heap_destroy(&hh);

   return SUCCESS(rc) ? 0 : -1;
}
