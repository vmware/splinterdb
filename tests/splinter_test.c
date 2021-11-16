// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinter_test.c --
 *
 *     This file contains the test interfaces for SplinterDB.
 */

#include "platform.h"

#include "splinter.h"
#include "btree.h"
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

#include "random.h"
#include "poison.h"

#define TEST_INSERT_GRANULARITY 4096
#define TEST_RANGE_GRANULARITY  512
#define TEST_VERIFY_GRANULARITY 100000

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

typedef struct stats_lookup {
   uint64               num_found;
   uint64               num_not_found;
   uint64               latency_max;
} stats_lookup;

typedef struct stats_insert {
   uint64               latency_max;
   uint64               duration;
} stats_insert;

typedef struct test_splinter_thread_params {
   platform_thread     thread;
   splinter_handle   **spl;
   test_config        *test_cfg;
   uint64             *total_ops;
   uint64             *curr_op;
   uint64              op_granularity;
   uint8               num_tables;
   uint64              thread_number;
   task_system        *ts;
   platform_status     rc;
   uint64              range_min;
   uint64              range_max;
   stats_insert        insert_stats;
   message_type        message_type;
   uint64              num_ops_per_thread[NUM_OP_TYPES];   // in each round
   bool                expected_found;
   test_async_lookup  *async_lookup[8];       // async lookup state per table
   uint64              insert_rate;
   stats_lookup        lookup_stats[NUM_LOOKUP_TYPES];
   uint8               lookup_positive_pct;   // parallel lookup positive %
   uint64              seed;
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
test_splinter_insert_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   splinter_handle            **spl_tables     = params->spl;
   const test_config           *test_cfg       = params->test_cfg;
   const uint64                *total_ops      = params->total_ops;
   uint64                      *curr_op        = params->curr_op;
   uint64                       op_granularity = params->op_granularity;
   uint64                       thread_number  = params->thread_number;
   message_type                 type           = params->message_type;
   uint8                        num_tables     = params->num_tables;

   platform_assert(num_tables <= 8);
   uint64 *insert_base = TYPED_ARRAY_ZALLOC(platform_get_heap_id(),
                                            insert_base, num_tables);
   uint8 done = 0;

   uint64 num_inserts = 0;
   timestamp next_check_time = platform_get_timestamp();
   // divide the per second insert rate into periods of 10 milli seconds.
   uint64 insert_rate = params->insert_rate / 100;

   while (1) {
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx))
            continue;
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"inserting %3lu%% complete for table %u",
               insert_base[spl_idx] / (total_ops[spl_idx] / 100), spl_idx);
         insert_base[spl_idx] = __sync_fetch_and_add(&curr_op[spl_idx], op_granularity);
         if (insert_base[spl_idx] >= total_ops[spl_idx])
            test_set_done(&done, spl_idx);
         if (test_all_done(done, num_tables))
            goto out;
      }
      for (uint64 op_offset = 0; op_offset != op_granularity; op_offset++) {
         for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
            uint64 insert_num = insert_base[spl_idx] + op_offset;
            if (test_is_done(done, spl_idx)) {
               continue;
            }
            splinter_handle *spl = spl_tables[spl_idx];
            char key[MAX_KEY_SIZE], data[MAX_MESSAGE_SIZE];

            timestamp ts;
            if (spl->cfg.use_stats) {
               ts = platform_get_timestamp();
            }
            test_key(key, test_cfg[spl_idx].key_type, insert_num,
                     thread_number, test_cfg[spl_idx].semiseq_freq,
                     splinter_key_size(spl), test_cfg[spl_idx].period);
            test_insert_data((data_handle *)data, 1, (char *)&insert_num,
                             8, splinter_message_size(spl), type);
            platform_status rc = splinter_insert(spl, key, data);
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
                  num_inserts = 0;
                  next_check_time = now + (USEC_TO_NSEC(10000));
               }
            }
         }
      }
   }
out:
   params->rc = STATUS_OK;
   platform_free(platform_get_heap_id(), insert_base);
   for (uint64 i = 0; i < num_tables; i++) {
      splinter_handle *spl = spl_tables[i];
      splinter_perform_tasks(spl);
   }
}

static void
verify_tuple(splinter_handle *spl,
             uint64           lookup_num,
             char            *key,
             const char      *data,
             char            *expected_data,
             size_t           data_size,
             bool             expected_found,
             bool             found)
{
   if (found != expected_found) {
      char key_str[128];
      splinter_key_to_string(spl, key, key_str);
      platform_default_log("(%2lu) key %lu (%s): found %d (expected:%d)\n",
                           platform_get_tid(), lookup_num, key_str, found,
                           expected_found);
      splinter_print_lookup(spl, key);
      platform_assert(0);
   }
   if (found && expected_data) {
      char data_str[128];
      test_insert_data((data_handle *)expected_data, 1, (char *)&lookup_num,
                       sizeof(lookup_num), data_size, MESSAGE_TYPE_INSERT);
      if (memcmp(expected_data, data, data_size) != 0) {
         splinter_message_to_string(spl, data, data_str);
         platform_log("key found with data: %s\n", data_str);
         platform_assert(0);
      }
   }
}

typedef struct {
   char         *expected_data;
   size_t        data_size;
   bool          expected_found;
   bool          stats_only;  // update statistic only
   stats_lookup *stats;
} verify_tuple_arg;

static void
verify_tuple_callback(splinter_handle *spl,
                      test_async_ctxt *ctxt,
                      bool             found,
                      void            *arg)
{
   verify_tuple_arg *vta = arg;

   if (vta->stats != NULL) {
      if (found) {
         vta->stats->num_found++;
      } else {
         vta->stats->num_not_found++;
      }
      if (vta->stats_only) {
         return;
      }
   }

   verify_tuple(spl, ctxt->lookup_num, ctxt->key, ctxt->data,
                vta->expected_data, vta->data_size, vta->expected_found, found);
}

static void
test_wait_for_inflight(splinter_handle   *spl,
                       test_async_lookup *async_lookup,
                       verify_tuple_arg  *vtarg)
{
   const timestamp ts = platform_get_timestamp();
   uint64 *latency_max = NULL;
   if (vtarg->stats != NULL) {
      latency_max = &vtarg->stats->latency_max;
   }

   // Rough detection of stuck contexts
   while (async_ctxt_process_ready(spl, async_lookup, latency_max,
                                   verify_tuple_callback,
                                   vtarg)) {
      cache_cleanup(spl->cc);
      platform_assert(platform_timestamp_elapsed(ts) < TEST_STUCK_IO_TIMEOUT);
   }
}

static test_async_ctxt *
test_async_ctxt_get(splinter_handle   *spl,
                    test_async_lookup *async_lookup,
                    verify_tuple_arg  *vtarg)
{
   test_async_ctxt *ctxt;

   ctxt = async_ctxt_get(async_lookup);
   if (LIKELY(ctxt != NULL)) {
      return ctxt;
   }
   // Out of async contexts; process all inflight ones.
   test_wait_for_inflight(spl, async_lookup, vtarg);
   /*
    * Guaranteed to get a context because this thread doesn't issue while
    * it drains inflight ones.
    */
   ctxt = async_ctxt_get(async_lookup);
   platform_assert(ctxt);

   return ctxt;
}

void
test_splinter_lookup_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   splinter_handle            **spl_tables     = params->spl;
   const test_config           *test_cfg       = params->test_cfg;
   const uint64                *total_ops      = params->total_ops;
   uint64                      *curr_op        = params->curr_op;
   uint64                       op_granularity = params->op_granularity;
   uint64                       thread_number  = params->thread_number;
   bool                         expected_found = params->expected_found;
   uint8                        num_tables     = params->num_tables;
   verify_tuple_arg             vtarg = {
      .expected_data = NULL,
      .data_size = 0,
      .expected_found = expected_found,
      .stats = &params->lookup_stats[ASYNC_LU]
   };

   platform_assert(num_tables <= 8);
   uint64 *lookup_base = TYPED_ARRAY_ZALLOC(platform_get_heap_id(),
                                            lookup_base, num_tables);
   uint8 done = 0;

   while (1) {
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx))
            continue;
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"lookups %3lu%% complete for table %u",
               lookup_base[spl_idx] / (total_ops[spl_idx] / 100), spl_idx);
         lookup_base[spl_idx] = __sync_fetch_and_add(&curr_op[spl_idx], op_granularity);
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
            splinter_handle *spl = spl_tables[spl_idx];
            test_async_lookup *async_lookup = params->async_lookup[spl_idx];
            test_async_ctxt *ctxt;
            uint64 lookup_num = lookup_base[spl_idx] + op_offset;
            timestamp ts;

            if (async_lookup->max_async_inflight == 0) {
               platform_status rc;
               char key[MAX_KEY_SIZE], data[MAX_MESSAGE_SIZE];
               bool found;

               test_key(key, test_cfg[spl_idx].key_type, lookup_num,
                        thread_number, test_cfg[spl_idx].semiseq_freq,
                        splinter_key_size(spl), test_cfg[spl_idx].period);
               ts = platform_get_timestamp();
               rc = splinter_lookup(spl, key, data, &found);
               ts = platform_timestamp_elapsed(ts);
               if (ts > params->lookup_stats[SYNC_LU].latency_max) {
                  params->lookup_stats[SYNC_LU].latency_max = ts;
               }
               platform_assert(SUCCESS(rc));
               verify_tuple(spl, lookup_num, key, data, NULL, 0, expected_found,
                            found);
            } else {
               ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg);
               test_key(ctxt->key, test_cfg[spl_idx].key_type, lookup_num,
                        thread_number, test_cfg[spl_idx].semiseq_freq,
                        splinter_key_size(spl), test_cfg[spl_idx].period);
               ctxt->lookup_num = lookup_num;
               async_ctxt_process_one(spl, async_lookup, ctxt,
                     &params->lookup_stats[ASYNC_LU].latency_max,
                     verify_tuple_callback, &vtarg);
            }
         }
      }
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx))
            continue;
         splinter_handle *spl = spl_tables[spl_idx];
         test_async_lookup *async_lookup = params->async_lookup[spl_idx];
         test_wait_for_inflight(spl, async_lookup, &vtarg);
      }
   }
out:
   params->rc = STATUS_OK;
   platform_free(platform_get_heap_id(), lookup_base);
}

void
test_splinter_range_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   splinter_handle            **spl_tables     = params->spl;
   const test_config           *test_cfg       = params->test_cfg;
   const uint64                *total_ops      = params->total_ops;
   uint64                      *curr_op        = params->curr_op;
   uint64                       op_granularity = params->op_granularity;
   uint64                       thread_number = params->thread_number;
   uint64                       range_min      = params->range_min;
   uint64                       range_max      = params->range_max;
   uint8                        num_tables     = params->num_tables;
   uint64                       tuple_size;

   platform_assert(num_tables <= 8);
   uint64 *range_base = TYPED_ARRAY_ZALLOC(platform_get_heap_id(),
                                           range_base, num_tables);
   uint8 done = 0;

   while (1) {
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(done, spl_idx))
            continue;
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"range lookups %3lu%% complete for table %u",
               range_base[spl_idx] / (total_ops[spl_idx] / 100), spl_idx);
         range_base[spl_idx] = __sync_fetch_and_add(&curr_op[spl_idx], op_granularity);
         if (range_base[spl_idx] >= total_ops[spl_idx])
            test_set_done(&done, spl_idx);
         if (test_all_done(done, num_tables))
            goto out;
      }
      for (uint64 op_offset = 0; op_offset != op_granularity; op_offset++) {
         for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
            if (test_is_done(done, spl_idx))
               continue;
            splinter_handle *spl = spl_tables[spl_idx];
            tuple_size = splinter_key_size(spl) + splinter_message_size(spl);
            char *range_output =
               TYPED_ARRAY_MALLOC(platform_get_heap_id(), range_output,
                                  range_max * tuple_size);
            platform_assert(range_output);

            char start_key[MAX_KEY_SIZE];
            uint64 range_num = range_base[spl_idx] + op_offset;
            test_key(start_key, test_cfg[spl_idx].key_type, range_num,
                     thread_number, test_cfg[spl_idx].semiseq_freq,
                     splinter_key_size(spl), test_cfg[spl_idx].period);
            uint64 range_tuples = test_range(range_num, range_min, range_max);
            uint64 returned_tuples;
            platform_status rc = splinter_range(spl, start_key, range_tuples,
                                                &returned_tuples, range_output);
            platform_assert_status_ok(rc);
            platform_free(platform_get_heap_id(), range_output);
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
             uint64 *                           curr_op,
             uint64 *                           base,
             uint8 *                            done,
             random_state *                     rs,
             test_splinter_pthread_op_type      type)
{
   const uint64          *total_ops           = params->total_ops;
   const uint64           op_granularity      = params->op_granularity;
   const uint8            num_tables          = params->num_tables;
   const uint8            lookup_positive_pct = params->lookup_positive_pct;

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      if (test_is_done(*done, spl_idx))
         continue;

      if (type == OP_INSERT) {
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"inserting/lookups %3lu%% complete for table %u",
               base[spl_idx] / (total_ops[spl_idx] / 100),
               spl_idx);

         base[spl_idx] = __sync_fetch_and_add(&curr_op[spl_idx],
                                                     op_granularity);
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
               random_base = ((random_next_uint64(rs) %
                               (local_curr_op / op_granularity - 1)) +
                              1) *
                             op_granularity;
            }
         } else {
            if (local_curr_op >= total_ops[spl_idx] - op_granularity) {
               // one interval left,
               // may lookup the key never be inserted
               random_base =
                  (random_next_uint64(rs) / op_granularity) * op_granularity +
                  local_curr_op;
            } else {
               // negative lookup by select random base
               // [local_curr_op + op_granularity, total_ops - op_granularity]
               random_base =
                  ((random_next_uint64(rs) %
                    ((total_ops[spl_idx] - local_curr_op) / op_granularity -
                     1)) +
                   1) *
                     op_granularity +
                  local_curr_op;
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
 *      do num_ops(assume it is valid and not exceed op granularity) inserts or lookups
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
   splinter_handle            **spl_tables     = params->spl;
   const test_config           *test_cfg       = params->test_cfg;
   uint64                       op_granularity = params->op_granularity;
   uint64                       thread_number  = params->thread_number;
   message_type                 type           = params->message_type;
   uint8                        num_tables     = params->num_tables;
   verify_tuple_arg vtarg = {
      .stats_only = TRUE,
      .stats = &params->lookup_stats[ASYNC_LU]
   };

   for (uint64 op_idx = op_offset; op_idx != op_offset + num_ops; op_idx++) {
      if (op_idx >= op_granularity) {
         return;
      }

      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         if (test_is_done(*done, spl_idx)) {
            continue;
         }
         splinter_handle *spl = spl_tables[spl_idx];
         char key[MAX_KEY_SIZE], data[MAX_MESSAGE_SIZE];
         uint64 op_num = base[spl_idx] + op_idx;
         timestamp ts;

         if (is_insert) {
            test_key(key, test_cfg[spl_idx].key_type, op_num,
                  thread_number, test_cfg[spl_idx].semiseq_freq,
                  splinter_key_size(spl), test_cfg[spl_idx].period);
            test_insert_data((data_handle *)data, 1, (char *)&op_num,
                 8, splinter_message_size(spl), type);
            ts = platform_get_timestamp();
            platform_status rc = splinter_insert(spl, key, data);
            platform_assert_status_ok(rc);
            ts = platform_timestamp_elapsed(ts);
            params->insert_stats.duration += ts;
            if (ts > params->insert_stats.latency_max) {
               params->insert_stats.latency_max = ts;
            }
         } else {
            test_async_lookup *async_lookup = params->async_lookup[spl_idx];
            test_async_ctxt *ctxt;

            if (async_lookup->max_async_inflight == 0) {
               platform_status rc;
               bool found;

               test_key(key, test_cfg[spl_idx].key_type, op_num,
                        thread_number, test_cfg[spl_idx].semiseq_freq,
                        splinter_key_size(spl), test_cfg[spl_idx].period);
               ts = platform_get_timestamp();
               rc = splinter_lookup(spl, key, data, &found);
               platform_assert(SUCCESS(rc));
               ts = platform_timestamp_elapsed(ts);
               if (ts > params->lookup_stats[SYNC_LU].latency_max) {
                  params->lookup_stats[SYNC_LU].latency_max = ts;
               }

               if (found) {
                  params->lookup_stats[SYNC_LU].num_found++;
               } else {
                  params->lookup_stats[SYNC_LU].num_not_found++;
               }
            } else {
               ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg);
               test_key(ctxt->key, test_cfg[spl_idx].key_type, op_num,
                        thread_number, test_cfg[spl_idx].semiseq_freq,
                        splinter_key_size(spl), test_cfg[spl_idx].period);
               ctxt->lookup_num = op_num;
               async_ctxt_process_one(spl, async_lookup, ctxt,
                     &params->lookup_stats[ASYNC_LU].latency_max,
                     verify_tuple_callback,
                     &vtarg);
            }
         }
      }
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * test_splinter_insert_lookup_thread
 *
 *      do number of insert_granularity inserts followed by number of
 *      lookup_granularity repeatedly until insert the full table
 *      lookup will be restarted from beginning until insert is done
 *
 *-----------------------------------------------------------------------------
 */
void
test_splinter_insert_lookup_thread(void *arg)
{
   test_splinter_thread_params *params = (test_splinter_thread_params *)arg;

   splinter_handle            **spl_tables         = params->spl;
   uint8                        num_tables         = params->num_tables;
   uint64                       op_granularity     = params->op_granularity;
   uint64                       seed               = params->seed;

   platform_assert(num_tables <= 8);

   uint64 *bases[NUM_OP_TYPES];
   uint64 granularities[NUM_OP_TYPES];
   uint64 offsets[NUM_OP_TYPES];
   uint8  insert_done;
   uint64 num_ops = 0;
   random_state rs;

   random_init(&rs, seed, 0);

   for (uint8 i = 0; i < NUM_OP_TYPES; i++) {
      bases[i] =  TYPED_ARRAY_ZALLOC(platform_get_heap_id(),
                                     bases[i], num_tables);

      granularities[i] = params->num_ops_per_thread[i];
      offsets[i] = 0;
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
            do_operation(params, bases[op_type], num_ops, offsets[op_type],
                         &insert_done, op_type == OP_INSERT);
            offsets[op_type] += num_ops;
         }

         if (num_ops < granularities[op_type] ||
             offsets[op_type] >= op_granularity) {
            num_ops = granularities[op_type] - num_ops;
            if (advance_base(params, params->curr_op, bases[op_type],
                             &insert_done, &rs, op_type)) {
               goto out;
            }
            offsets[op_type] = 0;
            if (num_ops > 0) {
               platform_assert(num_ops < granularities[op_type]);
               do_operation(params, bases[op_type], num_ops, offsets[op_type],
                            &insert_done, op_type == OP_INSERT);
               offsets[op_type] += num_ops;
            }
         }
      }
   }

out:
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      verify_tuple_arg vtarg = {
         .stats_only = TRUE,
         .stats = &params->lookup_stats[ASYNC_LU]
      };
      test_async_lookup *async_lookup = params->async_lookup[spl_idx];
      test_wait_for_inflight(spl, async_lookup, &vtarg);
   }

   params->rc = STATUS_OK;
   for (uint8 i = 0; i < NUM_OP_TYPES; i++) {
      platform_free(platform_get_heap_id(), bases[i]);
   }
}


static platform_status
test_splinter_create_tables(splinter_handle  ***spl_handles,
                            splinter_config    *cfg,
                            allocator          *al,
                            cache              *cc[],
                            task_system        *ts,
                            platform_heap_id    hid,
                            uint8               num_tables,
                            uint8               num_caches)
{
   uint32 size = sizeof(splinter_handle *) * num_tables;
   splinter_handle **spl_tables;
   spl_tables = platform_aligned_malloc(hid, PLATFORM_CACHELINE_SIZE, size);
   if (spl_tables == NULL) {
      return STATUS_NO_MEMORY;
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      cache *cache_to_use = num_caches > 1 ? cc[spl_idx] : *cc;
      spl_tables[spl_idx] =
         splinter_create(&cfg[spl_idx], al, cache_to_use, ts,
                         test_generate_allocator_root_id(), hid);
      if (spl_tables[spl_idx] == NULL) {
         for (uint8 del_idx = 0; del_idx < spl_idx; del_idx++) {
            splinter_destroy(spl_tables[del_idx]);
         }
         platform_free(hid, spl_tables);
         return STATUS_NO_MEMORY;
      }
   }
   *spl_handles = spl_tables;
   return STATUS_OK;
}

static void
test_splinter_destroy_tables(splinter_handle  **spl_tables,
                             platform_heap_id   hid,
                             uint8              num_tables)
{
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_destroy(spl_tables[spl_idx]);
   }
   platform_free(hid, spl_tables);
}

platform_status
test_splinter_perf(splinter_config  *cfg,
                   test_config      *test_cfg,
                   allocator        *al,
                   cache            *cc[],
                   uint64            num_insert_threads,
                   uint64            num_lookup_threads,
                   uint64            num_range_threads,
                   uint32            max_async_inflight,
                   task_system      *ts,
                   platform_heap_id  hid,
                   uint8             num_tables,
                   uint8             num_caches,
                   uint64            insert_rate)
{
   platform_log("splinter_test: splinter performance test started with %d\
                tables\n", num_tables);
   splinter_handle **spl_tables;
   platform_status rc;

   rc = test_splinter_create_tables(&spl_tables, cfg, al, cc, ts, hid,
                                    num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to create splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64 tuple_size, num_inserts;
   uint64 *per_table_inserts = TYPED_ARRAY_MALLOC(hid, per_table_inserts,
                                                  num_tables);
   uint64 *per_table_ranges = TYPED_ARRAY_MALLOC(hid, per_table_ranges,
                                                 num_tables);
   uint64 *curr_op = TYPED_ARRAY_ZALLOC(hid, curr_op, num_tables);
   uint64 total_inserts = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size + cfg[i].data_cfg->message_size;
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

   test_splinter_thread_params *params = TYPED_ARRAY_ZALLOC(hid, params,
                                                            num_threads);
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl            = spl_tables;
      params[i].test_cfg       = test_cfg;
      params[i].total_ops      = per_table_inserts;
      params[i].curr_op        = curr_op;
      params[i].op_granularity = TEST_INSERT_GRANULARITY;
      params[i].thread_number  = i;
      params[i].message_type   = MESSAGE_TYPE_INSERT;
      params[i].expected_found = TRUE;
      params[i].num_tables     = num_tables;
      params[i].ts             = ts;
      params[i].insert_rate    = insert_rate / num_insert_threads;
   }

   uint64 start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("insert_thread", test_splinter_insert_thread,
            &params[i], splinter_get_scratch_size(), ts, hid,
            &params[i].thread);
      if (!SUCCESS(ret)) {
         // FIXME: [yfogel 2020-03-30] need to clean up properly
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   for (uint64 i = 0; i < num_tables; i++) {
      task_wait_for_completion(ts);
   }

   uint64 total_time = platform_timestamp_elapsed(start_time);
   timestamp insert_latency_max = 0;
   // FIXME: [aconway 2021-07-30] Only reporting io stats for first cache.
   uint64 read_io_bytes, write_io_bytes;
   cache_io_stats(cc[0], &read_io_bytes, &write_io_bytes);
   uint64 io_mib = (read_io_bytes + write_io_bytes) / MiB;
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
      platform_log("\nper-splinter per-thread insert time per tuple %lu ns\n",
             total_time * num_insert_threads / total_inserts);
      platform_log("splinter total insertion rate: %lu insertions/second\n",
             SEC_TO_NSEC(total_inserts) / total_time);
      platform_log("splinter bandwidth: %lu megabytes/second\n", bandwidth);
      platform_log("splinter max insert latency: %lu msec\n",
                   NSEC_TO_MSEC(insert_latency_max));
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      cache_assert_free(spl->cc);
      platform_assert(splinter_verify_tree(spl));
      splinter_print_insertion_stats(spl);
      cache_print_stats(spl->cc);
      splinter_print_space_use(spl);
      cache_reset_stats(spl->cc);
      //splinter_print(spl);
   }

   ZERO_CONTENTS_N(curr_op, num_tables);
   if (num_lookup_threads != 0) {
      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_lookup_threads; i++) {
         platform_status ret;

         for (uint8 j = 0; j < num_tables; j++) {
            async_ctxt_init(hid, max_async_inflight, cfg[j].data_cfg->message_size,
                            &params[i].async_lookup[j]);
         }
         ret = task_thread_create("lookup thread", test_splinter_lookup_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
            return ret;
         }
      }
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      total_time = platform_timestamp_elapsed(start_time);

      uint64 num_async_lookups = 0;
      timestamp sync_lookup_latency_max = 0, async_lookup_latency_max = 0;
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         for (uint8 j = 0; j < num_tables; j++) {
            async_ctxt_deinit(hid, params[i].async_lookup[j]);
            params[i].async_lookup[j] = NULL;
         }
         num_async_lookups += params[i].lookup_stats[ASYNC_LU].num_found +
                              params[i].lookup_stats[ASYNC_LU].num_not_found;
         if (params[i].lookup_stats[SYNC_LU].latency_max >
             sync_lookup_latency_max) {
            sync_lookup_latency_max = params[i].lookup_stats[SYNC_LU].latency_max;
         }
         if (params[i].lookup_stats[ASYNC_LU].latency_max >
             async_lookup_latency_max) {
            async_lookup_latency_max =
                  params[i].lookup_stats[ASYNC_LU].latency_max;
         }
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_log("\nper-splinter per-thread lookup time per tuple %lu ns\n",
            total_time * num_lookup_threads / total_inserts);
      platform_log("splinter total lookup rate: %lu lookups/second\n",
           SEC_TO_NSEC(total_inserts) / total_time);
      platform_log("%lu%% lookups were async\n",
                   num_async_lookups * 100 / total_inserts);
      platform_log("max lookup latency ns (sync=%lu, async=%lu)\n",
                   sync_lookup_latency_max, async_lookup_latency_max);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

   uint64 total_ranges = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      per_table_ranges[i] = ROUNDUP(per_table_inserts[i] / 128, TEST_RANGE_GRANULARITY);
      total_ranges += per_table_ranges[i];
   }

   for (uint64 i = 0; i < num_threads; i++) {
      params[i].total_ops = per_table_ranges;
      params[i].op_granularity = TEST_RANGE_GRANULARITY;
      params[i].range_min = 1;
      params[i].range_max = 100;
   }

   ZERO_CONTENTS_N(curr_op, num_tables);
   if (num_range_threads != 0) {
      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread", test_splinter_range_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
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

      platform_log("\nper-splinter per-thread range time per tuple %lu ns\n",
            total_time * num_range_threads / total_ranges);
      platform_log("splinter total range rate: %lu ops/second\n",
           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] = ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min = 512;
         params[i].range_max = 1024;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread", test_splinter_range_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
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

      platform_log("\nper-splinter per-thread range time per tuple %lu ns\n",
            total_time * num_range_threads / total_ranges);
      platform_log("splinter total range rate: %lu ops/second\n",
           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] = ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min = 131072 - 16384;
         params[i].range_max = 131072;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread", test_splinter_range_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
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

      platform_log("\nper-splinter per-thread range time per tuple %lu ns\n",
            total_time * num_range_threads / total_ranges);
      platform_log("splinter total range rate: %lu ops/second\n",
           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

destroy_splinter:
   test_splinter_destroy_tables(spl_tables, hid, num_tables);
   platform_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_op);
   platform_free(hid, per_table_ranges);
   platform_free(hid, per_table_inserts);
   return rc;
}

platform_status
test_splinter_periodic(splinter_config  *cfg,
                       test_config      *test_cfg,
                       allocator        *al,
                       cache            *cc[],
                       uint64            num_insert_threads,
                       uint64            num_lookup_threads,
                       uint64            num_range_threads,
                       uint32            max_async_inflight,
                       task_system      *ts,
                       platform_heap_id  hid,
                       uint8             num_tables,
                       uint8             num_caches,
                       uint64            insert_rate)
{
   platform_log("splinter_test: splinter performance test started with %d\
                tables\n", num_tables);
   splinter_handle **spl_tables;
   platform_status rc;

   rc = test_splinter_create_tables(&spl_tables, cfg, al, cc, ts, hid,
                                    num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to create splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64 tuple_size, num_inserts;
   uint64 *per_table_inserts = TYPED_ARRAY_MALLOC(hid, per_table_inserts,
                                                  num_tables);
   uint64 *per_table_ranges = TYPED_ARRAY_MALLOC(hid, per_table_ranges,
                                                 num_tables);
   uint64 *curr_op = TYPED_ARRAY_ZALLOC(hid, curr_op, num_tables);
   uint64 total_inserts = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size + cfg[i].data_cfg->message_size;
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

   test_splinter_thread_params *params = TYPED_ARRAY_ZALLOC(hid, params,
                                                            num_threads);
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl            = spl_tables;
      params[i].test_cfg       = test_cfg;
      params[i].total_ops      = per_table_inserts;
      params[i].curr_op        = curr_op;
      params[i].op_granularity = TEST_INSERT_GRANULARITY;
      params[i].thread_number  = i;
      params[i].message_type   = MESSAGE_TYPE_INSERT;
      params[i].expected_found = TRUE;
      params[i].num_tables     = num_tables;
      params[i].ts             = ts;
      params[i].insert_rate    = insert_rate / num_insert_threads;
   }
   uint64 start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("insert_thread", test_splinter_insert_thread,
            &params[i], splinter_get_scratch_size(), ts, hid,
            &params[i].thread);
      if (!SUCCESS(ret)) {
         // FIXME: [yfogel 2020-03-30] need to clean up properly
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   for (uint64 i = 0; i < num_tables; i++) {
      task_wait_for_completion(ts);
   }

   uint64 total_time = platform_timestamp_elapsed(start_time);
   timestamp insert_latency_max = 0;
   // FIXME: [aconway 2021-07-30] Only reporting io stats for first cache.
   uint64 read_io_bytes, write_io_bytes;
   cache_io_stats(cc[0], &read_io_bytes, &write_io_bytes);
   uint64 io_mib = (read_io_bytes + write_io_bytes) / MiB;
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
      platform_log("\nper-splinter per-thread insert time per tuple %lu ns\n",
             total_time * num_insert_threads / total_inserts);
      platform_log("splinter total insertion rate: %lu insertions/second\n",
             SEC_TO_NSEC(total_inserts) / total_time);
      platform_log("splinter bandwidth: %lu megabytes/second\n", bandwidth);
      platform_log("splinter max insert latency: %lu msec\n",
                   NSEC_TO_MSEC(insert_latency_max));
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      cache_assert_free(spl->cc);
      platform_assert(splinter_verify_tree(spl));
      splinter_print_insertion_stats(spl);
      cache_print_stats(spl->cc);
      splinter_print_space_use(spl);
      cache_reset_stats(spl->cc);
   }

   ZERO_CONTENTS_N(curr_op, num_tables);

   for (uint64 repeat_round = 0; repeat_round < 10; repeat_round++) {
      platform_log("Beginning repeat round %lu\n", repeat_round);
      platform_error_log("Beginning repeat round %lu\n", repeat_round);
      /*
       * **********
       */
      for (uint64 i = 0; i < num_insert_threads; i++) {
         platform_status ret;
         ret = task_thread_create("insert_thread", test_splinter_insert_thread,
               &params[i], splinter_get_scratch_size(), ts, hid,
               &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
            return ret;
         }
      }
      for (uint64 i = 0; i < num_insert_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      for (uint64 i = 0; i < num_tables; i++) {
         task_wait_for_completion(ts);
      }

      total_time = platform_timestamp_elapsed(start_time);
      insert_latency_max = 0;
      // FIXME: [aconway 2021-07-30] Only reporting io stats for first cache.
      cache_io_stats(cc[0], &read_io_bytes, &write_io_bytes);
      io_mib = (read_io_bytes + write_io_bytes) / MiB;
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
         platform_log("\nper-splinter per-thread insert time per tuple %lu ns\n",
                total_time * num_insert_threads / total_inserts);
         platform_log("splinter total insertion rate: %lu insertions/second\n",
                SEC_TO_NSEC(total_inserts) / total_time);
         platform_log("splinter bandwidth: %lu megabytes/second\n", bandwidth);
         platform_log("splinter max insert latency: %lu msec\n",
                      NSEC_TO_MSEC(insert_latency_max));
      }

      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         platform_assert(splinter_verify_tree(spl));
         splinter_print_insertion_stats(spl);
         cache_print_stats(spl->cc);
         splinter_print_space_use(spl);
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
            async_ctxt_init(hid, max_async_inflight, cfg[j].data_cfg->message_size,
                            &params[i].async_lookup[j]);
         }
         ret = task_thread_create("lookup thread", test_splinter_lookup_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
            return ret;
         }
      }
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         platform_thread_join(params[i].thread);
      }

      total_time = platform_timestamp_elapsed(start_time);

      uint64 num_async_lookups = 0;
      timestamp sync_lookup_latency_max = 0, async_lookup_latency_max = 0;
      for (uint64 i = 0; i < num_lookup_threads; i++) {
         for (uint8 j = 0; j < num_tables; j++) {
            async_ctxt_deinit(hid, params[i].async_lookup[j]);
            params[i].async_lookup[j] = NULL;
         }
         num_async_lookups += params[i].lookup_stats[ASYNC_LU].num_found +
                              params[i].lookup_stats[ASYNC_LU].num_not_found;
         if (params[i].lookup_stats[SYNC_LU].latency_max >
             sync_lookup_latency_max) {
            sync_lookup_latency_max = params[i].lookup_stats[SYNC_LU].latency_max;
         }
         if (params[i].lookup_stats[ASYNC_LU].latency_max >
             async_lookup_latency_max) {
            async_lookup_latency_max =
                  params[i].lookup_stats[ASYNC_LU].latency_max;
         }
         if (!SUCCESS(params[i].rc)) {
            rc = params[i].rc;
            goto destroy_splinter;
         }
      }

      rc = STATUS_OK;

      platform_log("\nper-splinter per-thread lookup time per tuple %lu ns\n",
            total_time * num_lookup_threads / total_inserts);
      platform_log("splinter total lookup rate: %lu lookups/second\n",
           SEC_TO_NSEC(total_inserts) / total_time);
      platform_log("%lu%% lookups were async\n",
                   num_async_lookups * 100 / total_inserts);
      platform_log("max lookup latency ns (sync=%lu, async=%lu)\n",
                   sync_lookup_latency_max, async_lookup_latency_max);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

   uint64 total_ranges = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      per_table_ranges[i] = ROUNDUP(per_table_inserts[i] / 128, TEST_RANGE_GRANULARITY);
      total_ranges += per_table_ranges[i];
   }

   for (uint64 i = 0; i < num_threads; i++) {
      params[i].total_ops = per_table_ranges;
      params[i].op_granularity = TEST_RANGE_GRANULARITY;
      params[i].range_min = 1;
      params[i].range_max = 100;
   }

   ZERO_CONTENTS_N(curr_op, num_tables);
   if (num_range_threads != 0) {
      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread", test_splinter_range_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
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

      platform_log("\nper-splinter per-thread range time per tuple %lu ns\n",
            total_time * num_range_threads / total_ranges);
      platform_log("splinter total range rate: %lu ops/second\n",
           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] = ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min = 512;
         params[i].range_max = 1024;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread", test_splinter_range_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
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

      platform_log("\nper-splinter per-thread range time per tuple %lu ns\n",
            total_time * num_range_threads / total_ranges);
      platform_log("splinter total range rate: %lu ops/second\n",
           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }

      ZERO_CONTENTS_N(curr_op, num_tables);
      total_ranges = 0;
      for (uint8 i = 0; i < num_tables; i++) {
         per_table_ranges[i] = ROUNDUP(per_table_ranges[i] / 4, TEST_RANGE_GRANULARITY);
         total_ranges += per_table_ranges[i];
      }
      for (uint64 i = 0; i < num_range_threads; i++) {
         params[i].total_ops = per_table_ranges;
         params[i].op_granularity = TEST_RANGE_GRANULARITY;
         params[i].range_min = 131072 - 16384;
         params[i].range_max = 131072;
      }

      start_time = platform_get_timestamp();

      for (uint64 i = 0; i < num_range_threads; i++) {
         platform_status ret;
         ret = task_thread_create("range thread", test_splinter_range_thread,
               &params[i], splinter_get_scratch_size(), ts, hid, &params[i].thread);
         if (!SUCCESS(ret)) {
            // FIXME: [yfogel 2020-03-30] need to clean up properly
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

      platform_log("\nper-splinter per-thread range time per tuple %lu ns\n",
            total_time * num_range_threads / total_ranges);
      platform_log("splinter total range rate: %lu ops/second\n",
           SEC_TO_NSEC(total_ranges) / total_time);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         cache_assert_free(spl->cc);
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

destroy_splinter:
   test_splinter_destroy_tables(spl_tables, hid, num_tables);
   platform_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_op);
   platform_free(hid, per_table_ranges);
   platform_free(hid, per_table_inserts);
   return rc;
}
platform_status
test_splinter_parallel_perf(splinter_config  *cfg,
                            test_config      *test_cfg,
                            allocator        *al,
                            cache            *cc[],
                            uint64            seed,
                            const uint64      num_threads,
                            const uint64      num_inserts_per_thread,
                            const uint64      num_lookups_per_thread,
                            uint32            max_async_inflight,
                            uint8             lookup_positive_pct,
                            task_system      *ts,
                            platform_heap_id  hid,
                            uint8             num_tables,
                            uint8             num_caches)
{
   platform_log("splinter_test: splinter performance test started with %d\
                tables\n", num_tables);
   splinter_handle **spl_tables;
   platform_status rc;

   platform_assert(num_inserts_per_thread <= num_lookups_per_thread);

   rc = test_splinter_create_tables(&spl_tables, cfg, al, cc, ts, hid,
                                    num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to create splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64 tuple_size, num_inserts;
   uint64 *per_table_inserts = TYPED_ARRAY_MALLOC(hid, per_table_inserts,
                                                  num_tables);
   uint64 *curr_insert_op = TYPED_ARRAY_ZALLOC(hid, curr_insert_op, num_tables);
   uint64 total_inserts = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size + cfg[i].data_cfg->message_size;
      num_inserts = test_cfg[i].tree_size / tuple_size;
      per_table_inserts[i] = ROUNDUP(num_inserts, TEST_INSERT_GRANULARITY);
      total_inserts += per_table_inserts[i];
   }

   test_splinter_thread_params *params = TYPED_ARRAY_ZALLOC(hid, params,
                                                            num_threads);
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl                           = spl_tables;
      params[i].test_cfg                      = test_cfg;
      params[i].total_ops                     = per_table_inserts;
      params[i].op_granularity                = TEST_INSERT_GRANULARITY;
      params[i].thread_number                 = i;
      params[i].message_type                  = MESSAGE_TYPE_INSERT;
      params[i].num_tables                    = num_tables;
      params[i].ts                            = ts;
      params[i].curr_op                       = curr_insert_op;
      params[i].num_ops_per_thread[OP_INSERT] = num_inserts_per_thread;
      params[i].num_ops_per_thread[OP_LOOKUP] = num_lookups_per_thread;
      params[i].lookup_positive_pct           = lookup_positive_pct;
      params[i].seed                          = seed + i;   // unique seed
   }

   uint64 start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_threads; i++) {
      platform_status ret;

      for (uint8 j = 0; j < num_tables; j++) {
         async_ctxt_init(hid, max_async_inflight, cfg[j].data_cfg->message_size,
                         &params[i].async_lookup[j]);
      }
      ret = task_thread_create("insert/lookup thread",
            test_splinter_insert_lookup_thread, &params[i],
            splinter_get_scratch_size(), ts, hid, &params[i].thread);
      if (!SUCCESS(ret)) {
         // FIXME: [yfogel 2020-03-30] need to clean up properly
         return ret;
      }
   }

   for (uint64 i = 0; i < num_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   uint64 total_time = platform_timestamp_elapsed(start_time);
   timestamp insert_latency_max = 0;
   uint64 total_sync_lookups_found = 0;
   uint64 total_async_lookups_found = 0;
   uint64 total_sync_lookups_not_found = 0;
   uint64 total_async_lookups_not_found = 0;
   timestamp sync_lookup_latency_max = 0, async_lookup_latency_max = 0;
   uint64 total_insert_duration = 0;

   for (uint64 i = 0; i < num_threads; i++) {
      for (uint8 j = 0; j < num_tables; j++) {
         async_ctxt_deinit(hid, params[i].async_lookup[j]);
         params[i].async_lookup[j] = NULL;
      }
      if (params[i].insert_stats.latency_max > insert_latency_max) {
         insert_latency_max = params[i].insert_stats.latency_max;
      }
      if (params[i].lookup_stats[SYNC_LU].latency_max >
          sync_lookup_latency_max) {
         sync_lookup_latency_max = params[i].lookup_stats[SYNC_LU].latency_max;
      }
      if (params[i].lookup_stats[ASYNC_LU].latency_max >
          async_lookup_latency_max) {
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
      platform_log("\nper-splinter per-thread insert time per tuple %lu ns\n",
             total_time * num_threads / total_inserts);
      platform_log("splinter total insertion rate: %lu insertions/second\n",
             SEC_TO_NSEC(total_inserts) / total_time);
      platform_log("splinter pure insertion rate: %lu insertions/second\n",
             SEC_TO_NSEC(total_inserts) / total_insert_duration * num_threads);
      platform_log("splinter max insert latency: %lu msec\n",
                   NSEC_TO_MSEC(insert_latency_max));
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      splinter_print_insertion_stats(spl);
   }

   if (num_threads > 0) {
      uint64 num_async_lookups = total_async_lookups_found +
                                   total_async_lookups_not_found;
      uint64 total_lookups = total_sync_lookups_found +
                             total_sync_lookups_not_found +
                             num_async_lookups;
      platform_log("\nper-splinter per-thread lookup time per tuple %lu ns\n",
            total_time * num_threads / total_lookups);
      platform_log("splinter total lookup rate: %lu lookups/second\n",
            SEC_TO_NSEC(total_lookups) / total_time);
      platform_log("splinter total lookup found: (sync=%lu, async=%lu)\n",
            total_sync_lookups_found, total_async_lookups_found);
      platform_log("splinter total lookup not found: (sync=%lu, async=%lu)\n",
            total_sync_lookups_not_found, total_async_lookups_not_found);
      platform_log("%lu%% lookups were async\n",
            num_async_lookups * 100 / total_lookups);
      platform_log("max lookup latency ns (sync=%lu, async=%lu)\n",
            sync_lookup_latency_max, async_lookup_latency_max);
      for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
         splinter_handle *spl = spl_tables[spl_idx];
         splinter_print_lookup_stats(spl);
         cache_print_stats(spl->cc);
         cache_reset_stats(spl->cc);
      }
   }

destroy_splinter:
   test_splinter_destroy_tables(spl_tables, hid, num_tables);
   platform_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_insert_op);
   platform_free(hid, per_table_inserts);
   return rc;
}

platform_status
test_splinter_delete(splinter_config  *cfg,
                     test_config      *test_cfg,
                     allocator        *al,
                     cache            *cc[],
                     uint64            num_insert_threads,
                     uint64            num_lookup_threads,
                     uint32            max_async_inflight,
                     task_system      *ts,
                     platform_heap_id  hid,
                     uint8             num_tables,
                     uint8             num_caches,
                     uint64            insert_rate)
{
   platform_log("splinter_test: splinter deletion test started with %d\
                tables\n", num_tables);
   splinter_handle **spl_tables;
   platform_status rc;

   rc = test_splinter_create_tables(&spl_tables, cfg, al, cc, ts, hid,
                                    num_tables, num_caches);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to initialize splinter table(s): %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   uint64 tuple_size, num_inserts;
   uint64 *per_table_inserts = TYPED_ARRAY_MALLOC(hid, per_table_inserts,
                                                  num_tables);
   uint64 *curr_op = TYPED_ARRAY_ZALLOC(hid, curr_op, num_tables);
   uint64 total_inserts = 0;

   for (uint8 i = 0; i < num_tables; i++) {
      tuple_size = cfg[i].data_cfg->key_size + cfg[i].data_cfg->message_size;
      num_inserts = test_cfg[i].tree_size / tuple_size;
      per_table_inserts[i] = ROUNDUP(num_inserts, TEST_INSERT_GRANULARITY);
      total_inserts += per_table_inserts[i];
   }
   uint64 num_threads = num_insert_threads;

   if (num_lookup_threads > num_threads) {
      num_threads = num_lookup_threads;
   }
   test_splinter_thread_params *params = TYPED_ARRAY_MALLOC(hid, params,
                                                            num_threads);
   platform_assert(params);

   memset(params, 0, sizeof(*params));
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].spl            = spl_tables;
      params[i].test_cfg       = test_cfg;
      params[i].total_ops      = per_table_inserts;
      params[i].curr_op        = curr_op;
      params[i].op_granularity = TEST_INSERT_GRANULARITY;
      params[i].thread_number  = i;
      params[i].message_type   = MESSAGE_TYPE_INSERT;
      params[i].num_tables     = num_tables;
      params[i].ts             = ts;
      params[i].insert_rate    = insert_rate / num_insert_threads;
   }

   uint64 total_time, start_time = platform_get_timestamp();

   // Insert: round 1
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("insert thread", test_splinter_insert_thread,
            &params[i], splinter_get_scratch_size(), ts, hid,
            &params[i].thread);
      if (!SUCCESS(ret)) {
         // FIXME: [yfogel 2020-03-31] need to cleanup
         //    goto destroy_splinter with some status?
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_thread_join(params[i].thread);
   }

   total_time = platform_timestamp_elapsed(start_time);
   platform_log("\nper-splinter per-thread insert time per tuple %lu ns\n",
         total_time * num_insert_threads / total_inserts);
   platform_log("splinter total insertion rate: %lu insertions/second\n",
         SEC_TO_NSEC(total_inserts) / total_time);
   platform_log("After inserts:\n");
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      splinter_print_insertion_stats(spl);
      cache_print_stats(spl->cc);
   }

   for (uint64 i = 0; i < num_insert_threads; i++) {
      if (!SUCCESS(params[i].rc)) {
         rc = params[i].rc;
         goto destroy_splinter;
      }
   }

   rc = STATUS_OK;

   // Deletes
   for (uint64 i = 0; i < num_threads; i++) {
      params[i].message_type = MESSAGE_TYPE_DELETE;
   }
   ZERO_CONTENTS_N(curr_op, num_tables);
   start_time = platform_get_timestamp();
   for (uint64 i = 0; i < num_insert_threads; i++) {
      platform_status ret;
      ret = task_thread_create("delete thread", test_splinter_insert_thread,
            &params[i], splinter_get_scratch_size(), ts, hid,
            &params[i].thread);
      if (!SUCCESS(ret)) {
         // FIXME: [yfogel 2020-03-31] need to cleanup
         //    goto destroy_splinter with some status?
         return ret;
      }
   }
   for (uint64 i = 0; i < num_insert_threads; i++)
      platform_thread_join(params[i].thread);

   total_time = platform_timestamp_elapsed(start_time);
   platform_log("\nper-splinter per-thread delete time per tuple %lu ns\n",
         total_time * num_insert_threads / total_inserts);
   platform_log("splinter total deletion rate: %lu insertions/second\n",
         SEC_TO_NSEC(total_inserts) / total_time);
   platform_log("After deletes:\n");
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      splinter_print_insertion_stats(spl);
      cache_print_stats(spl->cc);
   }

   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      splinter_force_flush(spl);
      platform_log("After flushing table %d:\n", spl_idx);
      splinter_print_insertion_stats(spl);
      cache_print_stats(spl->cc);
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
      params[i].expected_found   = FALSE;
   }
   ZERO_CONTENTS_N(curr_op, num_tables);
   start_time = platform_get_timestamp();

   for (uint64 i = 0; i < num_lookup_threads; i++) {
      for (uint8 j = 0; j < num_tables; j++) {
         async_ctxt_init(hid, max_async_inflight, cfg[j].data_cfg->message_size,
                         &params[i].async_lookup[j]);
      }
      rc = task_thread_create("lookup thread", test_splinter_lookup_thread,
            &params[i], splinter_get_scratch_size(), ts, hid,
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
      num_async_lookups += params[i].lookup_stats[ASYNC_LU].num_found +
                           params[i].lookup_stats[ASYNC_LU].num_not_found;
      if (!SUCCESS(params[i].rc)) {
         // XXX cleanup properly
         rc = params[i].rc;
         goto destroy_splinter;
      }
   }

   rc = STATUS_OK;

   platform_log("\nper-splinter per-thread lookup time per tuple %lu ns\n",
                total_time * num_lookup_threads / total_inserts);
   platform_log("splinter total lookup rate: %lu lookups/second\n",
                SEC_TO_NSEC(total_inserts) / total_time);
   platform_log("%lu%% lookups were async\n",
                num_async_lookups * 100 / total_inserts);
   for (uint8 spl_idx = 0; spl_idx < num_tables; spl_idx++) {
      splinter_handle *spl = spl_tables[spl_idx];
      splinter_print_lookup_stats(spl);
      cache_print_stats(spl->cc);
   }

destroy_splinter:
   test_splinter_destroy_tables(spl_tables, hid, num_tables);
   platform_log("After destroy:\n");
   for (uint8 idx = 0; idx < num_caches; idx++) {
      cache_print_stats(cc[idx]);
   }
   platform_free(hid, params);
   platform_free(hid, curr_op);
   platform_free(hid, per_table_inserts);
   return rc;
}

void
test_splinter_shadow_insert(splinter_handle *spl,
                            char            *shadow,
                            char            *key,
                            char            *data,
                            uint64           idx)
{
   uint64 byte_pos = idx * (splinter_key_size(spl) + splinter_message_size(spl));
   memmove(&shadow[byte_pos], key, splinter_key_size(spl));
   byte_pos += splinter_key_size(spl);
   memmove(&shadow[byte_pos], data, splinter_message_size(spl));
}

int
test_splinter_key_compare(const void *left,
      const void *right,
      void       *spl)
{
   return splinter_key_compare((splinter_handle *)spl,
         (const char *)left, (const char *)right);
}

void *
test_splinter_bsearch(register const void *key,
      void *base0, size_t nmemb, register size_t size,
      register int (*compar)(const void *, const void *, void *),
      void *ctxt)
{
   register char *base = (char *) base0;
   register int lim, cmp;
   register void *p;

   platform_assert(nmemb != 0);
   for (lim = nmemb; lim != 0; lim >>= 1) {
      p = base + (lim >> 1) * size;
      cmp = (*compar)(key, p, ctxt);
      if (cmp >= 0) {   /* key > p: move right */
         base = (char *)p + size;
         lim--;
      } /* else move left */
   }
   if (cmp < 0) {
      return (void *)p;
   }
   return (void *)p + size;
}

static platform_status
test_splinter_basic(allocator        *al,
                    cache            *cc,
                    splinter_config  *cfg,
                    uint64            num_inserts,
                    uint32            max_async_inflight,
                    task_system      *ts,
                    platform_heap_id  hid)
{
   platform_log("splinter_test: splinter basic test started\n");
   splinter_handle *spl =
      splinter_create(cfg, al, cc, ts, test_generate_allocator_root_id(), hid);
   platform_status rc;

   uint64 start_time = platform_get_timestamp();
   uint64 insert_num;
   char key[MAX_KEY_SIZE];
   const size_t key_size = splinter_key_size(spl);
   const size_t data_size = splinter_message_size(spl);
   char *data = TYPED_ARRAY_MALLOC(hid, data, data_size);
   platform_assert(data != NULL);
   test_async_lookup *async_lookup;
   if (max_async_inflight > 0) {
      async_ctxt_init(hid, max_async_inflight, data_size, &async_lookup);
   } else {
      async_lookup = NULL;
   }
   // 1 extra for temp argument to sort
   uint64 tuple_size = key_size + data_size;
   uint64 shadow_size = num_inserts * tuple_size;
   char *shadow = TYPED_ARRAY_ZALLOC(hid, shadow, shadow_size);
   char *temp = TYPED_ARRAY_ZALLOC(hid, temp, tuple_size);

   for (insert_num = 0; insert_num < num_inserts; insert_num++) {
      if (insert_num % (num_inserts / 100) == 0) {
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"inserting %3lu%% complete",insert_num / (num_inserts / 100));
      }
      if (insert_num != 0 && insert_num % TEST_VERIFY_GRANULARITY == 0) {
         platform_assert(splinter_verify_tree(spl));
      }
      test_key(key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
      test_insert_data((data_handle *)data, 1, (char *)&insert_num,
            sizeof(uint64), data_size, MESSAGE_TYPE_INSERT);
      rc = splinter_insert(spl, key, data);
      if (!SUCCESS(rc)) {
         platform_error_log("FAILURE: %s\n", platform_status_to_string(rc));
         goto destroy_splinter;
      }
      test_splinter_shadow_insert(spl, shadow, key, data, insert_num);
   }

   platform_log("\nsplinter insert time per tuple %lu ns\n",
         platform_timestamp_elapsed(start_time) / num_inserts);

   platform_assert(splinter_verify_tree(spl));
   cache_assert_free(cc);

   char *expected_data = TYPED_ARRAY_MALLOC(hid, expected_data, data_size);
   platform_assert(expected_data != NULL);
   verify_tuple_arg vtarg_true = {
      .expected_data = expected_data,
      .data_size = data_size,
      .expected_found = TRUE
   };
   bool found;
   start_time = platform_get_timestamp();
   for (insert_num = 0; insert_num < num_inserts; insert_num++) {
      test_async_ctxt *ctxt;
      if (insert_num % (num_inserts / 100) == 0)
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"positive lookups %3lu%% complete",
               insert_num / (num_inserts / 100));
      if (max_async_inflight == 0) {
         test_key(key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
         memset(data, 0, data_size);
         rc = splinter_lookup(spl, key, data, &found);
         if (!SUCCESS(rc)) {
            platform_error_log("FAILURE: %s\n", platform_status_to_string(rc));
            goto destroy_splinter;
         }
         verify_tuple(spl, insert_num, key, data, expected_data, data_size,
                      TRUE, found);
      } else {
         ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg_true);
         test_key(ctxt->key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
         memset(ctxt->data, 0, data_size);
         ctxt->lookup_num = insert_num;
         async_ctxt_process_one(spl, async_lookup, ctxt, NULL,
                                verify_tuple_callback,
                                &vtarg_true);
      }
   }
   if (max_async_inflight != 0) {
      test_wait_for_inflight(spl, async_lookup, &vtarg_true);
   }
   cache_assert_free(cc);
   platform_free(hid, expected_data);
   platform_log("\nsplinter positive lookup time per tuple %lu ns\n",
         platform_timestamp_elapsed(start_time) / num_inserts);
   platform_log("\n");

   verify_tuple_arg vtarg_false = {
      .expected_data = NULL,
      .data_size = 0,
      .expected_found = FALSE
   };
   start_time = platform_get_timestamp();
   for (insert_num = num_inserts; insert_num < 2 * num_inserts; insert_num++) {
      test_async_ctxt *ctxt;
      if (insert_num % (num_inserts / 100) == 0)
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"negative lookups %3lu%% complete",
               (insert_num - num_inserts) / (num_inserts / 100));
      if (max_async_inflight == 0) {
         test_key(key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
         memset(data, 0, data_size);
         rc = splinter_lookup(spl, key, data, &found);
         if (!SUCCESS(rc)) {
            platform_error_log("FAILURE: %s\n", platform_status_to_string(rc));
            goto destroy_splinter;
         }
         verify_tuple(spl, insert_num, key, data, NULL, 0, FALSE, found);
      } else {
         ctxt = test_async_ctxt_get(spl, async_lookup, &vtarg_false);
         test_key(ctxt->key, TEST_RANDOM, insert_num, 0, 0, key_size, 0);
         memset(ctxt->data, 0, data_size);
         ctxt->lookup_num = insert_num;
         async_ctxt_process_one(spl, async_lookup, ctxt, NULL,
                                verify_tuple_callback,
                                &vtarg_false);
      }
   }
   if (max_async_inflight != 0) {
      test_wait_for_inflight(spl, async_lookup, &vtarg_false);
   }
   cache_assert_free(cc);
   platform_log("\nsplinter negative lookup time per tuple %lu ns\n",
         platform_timestamp_elapsed(start_time) / num_inserts);
   platform_log("\n");

   platform_sort_slow(shadow, num_inserts,
                      tuple_size,
                      test_splinter_key_compare, spl, temp);

   uint64 num_ranges = num_inserts / 128;
   start_time = platform_get_timestamp();

   char *range_output = TYPED_ARRAY_MALLOC(hid, range_output,
                                           100 * tuple_size);

   for (uint64 range_num = 0; range_num != num_ranges; range_num++) {
      if (range_num % (num_ranges / 100) == 0)
         platform_throttled_error_log(DEFAULT_THROTTLE_INTERVAL_SEC,
               PLATFORM_CR"range lookups %3lu%% complete",
               range_num / (num_ranges / 100));
      char start_key[MAX_KEY_SIZE];
      test_key(start_key, TEST_RANDOM, num_inserts + range_num,
            0, 0, key_size, 0);
      uint64 range_tuples = test_range(range_num, 1, 100);
      uint64 returned_tuples = 0;
      rc = splinter_range(spl, start_key, range_tuples,
            &returned_tuples, range_output);
      platform_assert_status_ok(rc);
      char *shadow_start = test_splinter_bsearch(start_key, shadow, num_inserts,
            tuple_size, test_splinter_key_compare, spl);
      uint64 start_idx = (shadow_start - shadow) / tuple_size;
      uint64 expected_returned_tuples = num_inserts - start_idx > range_tuples ?
         range_tuples : num_inserts - start_idx;
      if (returned_tuples != expected_returned_tuples
            || memcmp(shadow_start, range_output,
               returned_tuples * tuple_size) != 0) {
         platform_log("range lookup: incorrect return\n");
         char start[128];
         splinter_key_to_string(spl, start_key, start);
         platform_log("start_key: %s\n", start);
         platform_log("tuples returned: expected %lu actual %lu\n",
               expected_returned_tuples, returned_tuples);
         for (uint64 i = 0; i < expected_returned_tuples; i++) {
            char expected[128];
            char actual[128];
            uint64 offset = i * tuple_size;
            splinter_key_to_string(spl, shadow_start + offset, expected);
            splinter_key_to_string(spl, range_output + offset, actual);
            char expected_data[128];
            char actual_data[128];
            offset += key_size;
            splinter_message_to_string(spl, shadow_start + offset, expected_data);
            splinter_message_to_string(spl, range_output + offset, actual_data);
            if (i < returned_tuples) {
               platform_log("expected %s | %s\n", expected, expected_data);
               platform_log("actual   %s | %s\n", actual, actual_data);
            } else {
               platform_log("expected %s | %s\n", expected, expected_data);
            }
         }
         platform_assert(0);
      }

   }
   platform_log("\nsplinter range time per operation %lu ns\n",
         platform_timestamp_elapsed(start_time) / num_ranges);
   platform_log("\n");

   cache_assert_free(cc);
   platform_free(hid, range_output);

destroy_splinter:
   if (async_lookup) {
      async_ctxt_deinit(hid, async_lookup);
   }
   platform_free(hid, temp);
   platform_free(hid, shadow);
   splinter_destroy(spl);
   if (SUCCESS(rc)) {
      platform_log("splinter_test: splinter basic test succeeded\n");
   } else {
      platform_log("splinter_test: splinter basic test failed\n");
   }
   platform_log("\n");
   platform_free(hid, data);
   return rc;
}


static void
usage(const char *argv0) {
   platform_error_log("Usage:\n"
    "\t%s\n"
    "\t%s --perf --max-async-inflight [num] --num-insert-threads [num]\n"
    "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
    "\t%s --delete --max-async-inflight [num] --num-insert-threads [num]\n"
    "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
    "\t%s --seq-perf --max-async-inflight [num] --num-insert-threads [num]\n"
    "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
    "\t%s --semiseq-perf --max-async-inflight [num] --num-insert-threads [num]\n"
    "\t   --num-lookup-threads [num] --num-range-lookup-threads [num]\n"
    "\t%s --functionality NUM_INSERTS CORRECTNESS_CHECK_FREQUENCY\n"
    "\t   --max-async-inflight [num]\n"
    "\t%s --num-tables (number of tables to use for test)\n"
    "\t%s --cache-per-table\n"
    "\t%s --parallel-perf --max-async-inflight [num] --num-pthreads [num] --lookup-positive-percent [num] --seed [num]\n"
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
    argv0,
    argv0);
   test_config_usage();
   config_usage();
}

static int
splinter_test_parse_perf_args(char ***argv,
                              int *argc,
                              uint32 *max_async_inflight,
                              uint32 *num_insert_threads,
                              uint32 *num_lookup_threads,
                              uint32 *num_range_lookup_threads,
                              uint32 *num_pthreads,
                              uint8  *lookup_positive_pct)
{
   if (*argc > 1 && strncmp((*argv)[0], "--max-async-inflight",
                        sizeof("--max-async-inflight")) == 0) {
      if (!try_string_to_uint32((*argv)[1], max_async_inflight)) {
         return -1;
      }
      if (*max_async_inflight > TEST_MAX_ASYNC_INFLIGHT) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1 && strncmp((*argv)[0], "--num-insert-threads",
                        sizeof("--num-insert-threads")) == 0) {
      if (!try_string_to_uint32((*argv)[1], num_insert_threads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1 && strncmp((*argv)[0], "--num-lookup-threads",
                        sizeof("--num-lookup-threads")) == 0) {
      if (!try_string_to_uint32((*argv)[1], num_lookup_threads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1 && strncmp((*argv)[0], "--num-range-lookup-threads",
                        sizeof("--num-range-lookup-threads")) == 0) {
      if (!try_string_to_uint32((*argv)[1], num_range_lookup_threads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1 && strncmp((*argv)[0], "--num-pthreads",
                        sizeof("--num-pthreads")) == 0) {
      if (!try_string_to_uint32((*argv)[1], num_pthreads)) {
         return -1;
      }
      *argc -= 2;
      *argv += 2;
   }
   if (*argc > 1 && strncmp((*argv)[0], "--lookup-positive-percent",
                        sizeof("--lookup-positive-percent")) == 0) {
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
   io_config             io_cfg;
   rc_allocator_config   al_cfg;
   shard_log_config      log_cfg;
   int                   config_argc;
   char                **config_argv;
   test_type             test;
   platform_status       rc;
   uint64                seed = 0;
   uint64                test_ops;
   uint64                correctness_check_frequency;
   // Max async IOs inflight per-thread
   uint32                num_insert_threads, num_lookup_threads;
   uint32                num_range_lookup_threads, max_async_inflight;
   uint32                num_pthreads = 0;
   uint8                 num_tables = 1;
   bool                  cache_per_table = FALSE;
                         // no bg threads by default.
   uint8                 num_bg_threads[NUM_TASK_TYPES] = { 0 };
   uint64                insert_rate = 0; // no rate throttling by default.
   task_system          *ts;
   uint8                 lookup_positive_pct = 0;

   // Defaults
   num_insert_threads = num_lookup_threads = num_range_lookup_threads = 1;
   max_async_inflight = 64;
   /*
    * 1. Parse splinter_test options, see usage()
    */
   if (argc > 1 && strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
      test = perf;
      config_argc = argc - 2;
      config_argv = argv + 2;
      num_insert_threads = 14;
      num_lookup_threads = 20;
      num_range_lookup_threads = 10;
   } else if (argc > 1 && strncmp(argv[1],
            "--delete", sizeof("--delete")) == 0) {
      test = delete;
      config_argc = argc - 2;
      config_argv = argv + 2;
      num_insert_threads = 14;
   } else if (argc > 1 && strncmp(argv[1],
            "--seq-perf", sizeof("--seq-perf")) == 0) {
      test = seq_perf;
      config_argc = argc - 2;
      config_argv = argv + 2;
   } else if (argc > 1 && strncmp(argv[1],
            "--semiseq-perf", sizeof("--semiseq-perf")) == 0) {
      test        = semiseq_perf;
      config_argc = argc - 2;
      config_argv = argv + 2;
   } else if (argc > 1 && strncmp(argv[1],
            "--parallel-perf", sizeof("--parallel-perf")) == 0) {
      test        = parallel_perf;
      config_argc = argc - 2;
      config_argv = argv + 2;
      num_pthreads = 10;
      lookup_positive_pct = 50;
   } else if (argc > 1 && strncmp(argv[1],
            "--functionality", sizeof("--functionality")) == 0) {
      if (argc < 4) {
         usage(argv[0]);
         return -1;
      }
      test = functionality;
      if (0
          || !try_string_to_uint64(argv[2], &test_ops)
          || !try_string_to_uint64(argv[3], &correctness_check_frequency))
      {
         usage(argv[0]);
         return -1;
      }
      config_argc = argc - 4;
      config_argv = argv + 4;
   } else if (argc > 1 && strncmp(argv[1],
            "--periodic", sizeof("--periodic")) == 0) {
      test        = periodic;
      config_argc = argc - 2;
      config_argv = argv + 2;
      num_insert_threads = 14;
      num_lookup_threads = 0;
      num_range_lookup_threads = 0;
   } else {
      test = basic;
      config_argc = argc - 1;
      config_argv = argv + 1;
   }
   if (config_argc > 0 && strncmp(config_argv[0], "--num-tables",
                                  sizeof("--num-tables")) == 0) {
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
   if (config_argc > 0 && strncmp(config_argv[0], "--cache-per-table",
                                  sizeof("--cache-per-table")) == 0) {
      cache_per_table = TRUE;
      config_argc -= 1;
      config_argv += 1;
   }
   if (config_argc > 0 && strncmp(config_argv[0], "--num-bg-threads",
                                 sizeof("--num-bg-threads")) == 0) {
      if (!try_string_to_uint8(config_argv[1], &num_bg_threads[TASK_TYPE_NORMAL])) {
         usage(argv[0]);
         return -1;
      }
      config_argc -= 2;
      config_argv += 2;
   }
   if (config_argc > 0 && strncmp(config_argv[0], "--num-memtable-bg-threads",
                                 sizeof("--num-bg-threads")) == 0) {
      if (!try_string_to_uint8(config_argv[1], &num_bg_threads[TASK_TYPE_MEMTABLE])) {
         usage(argv[0]);
         return -1;
      }
      config_argc -= 2;
      config_argv += 2;
   }
   if (splinter_test_parse_perf_args(&config_argv, &config_argc,
                                     &max_async_inflight,
                                     &num_insert_threads,
                                     &num_lookup_threads,
                                     &num_range_lookup_threads,
                                     &num_pthreads,
                                     &lookup_positive_pct) != 0) {
      usage(argv[0]);
      return -1;
   }

   if (config_argc > 0 && strncmp(config_argv[0], "--insert-rate",
                                  sizeof("--insert-rate")) == 0) {
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
   uint8 num_caches = cache_per_table ? num_tables : 1;
   uint64 heap_capacity = MAX(1024 * MiB * num_caches, 512 * MiB * num_tables);
   heap_capacity = MIN(heap_capacity, UINT32_MAX);
   heap_capacity = MAX(heap_capacity, 2 * GiB);

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id hid;
   rc = platform_heap_create(platform_get_module_id(), heap_capacity, &hh,
                             &hid);
   platform_assert_status_ok(rc);

   /*
    * 2. Parse test_config options, see test_config_usage()
    */
   test_config *test_cfg = TYPED_ARRAY_MALLOC(hid, test_cfg, num_tables);
   for (uint8 i = 0; i < num_tables; i++) {
      test_config_set_defaults(test, &test_cfg[i]);
   }
   rc = test_config_parse(test_cfg, num_tables, &config_argc, &config_argv);
   if (!SUCCESS(rc)) {
      platform_error_log("splinter_test: failed to parse config: %s\n",
                         platform_status_to_string(rc));
      usage(argv[0]);
      goto heap_destroy;
   }

   /*
    * 3. Parse splinter_config options, see config_usage()
    */
   splinter_config *splinter_cfg = TYPED_ARRAY_MALLOC(hid, splinter_cfg,
                                                      num_tables);
   data_config *data_cfg = TYPED_ARRAY_MALLOC(hid, data_cfg, num_tables);
   clockcache_config *cache_cfg = TYPED_ARRAY_MALLOC(hid, cache_cfg,
                                                     num_tables);
   rc = test_parse_args_n(splinter_cfg, data_cfg, &io_cfg, &al_cfg, cache_cfg,
                          &log_cfg, &seed, num_tables, config_argc, config_argv);
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

   // Max active threads
   uint32 total_threads = num_lookup_threads;
   if (total_threads < num_insert_threads) {
      total_threads = num_insert_threads;
   }
   if (total_threads < num_pthreads) {
      total_threads = num_pthreads;
   }
   if (num_bg_threads[TASK_TYPE_NORMAL] != 0 &&
         num_bg_threads[TASK_TYPE_MEMTABLE] == 0) {
      num_bg_threads[TASK_TYPE_MEMTABLE] = num_tables;
   }
   for (task_type type = 0; type != NUM_TASK_TYPES; type++) {
      total_threads += num_bg_threads[type];
   }
   // Check if IO subsystem has enough reqs for max async IOs inflight
   if (io_cfg.async_queue_size < total_threads * max_async_inflight) {
      io_cfg.async_queue_size = ROUNDUP(total_threads * max_async_inflight, 32);
      platform_log("Bumped up IO queue size to %lu\n", io_cfg.async_queue_size);
   }
   if (io_cfg.kernel_queue_size < total_threads * max_async_inflight) {
      io_cfg.kernel_queue_size = ROUNDUP(total_threads * max_async_inflight,
                                         32);
      platform_log("Bumped up IO queue size to %lu\n", io_cfg.kernel_queue_size);
   }

   platform_io_handle *io = TYPED_MALLOC(hid, io);
   platform_assert(io != NULL);
   rc = io_handle_init(io, &io_cfg, hh, hid);
   if (!SUCCESS(rc)) {
      goto io_free;
   }

   bool use_bg_threads = num_bg_threads[TASK_TYPE_NORMAL] != 0;

   rc = test_init_splinter(hid, io, &ts, splinter_cfg->use_stats,
         use_bg_threads, num_bg_threads);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(rc));
      goto handle_deinit;
   }

   rc_allocator al;
   rc_allocator_init(&al, &al_cfg, (io_handle *)io, hh, hid,
                     platform_get_module_id());

   platform_error_log("Running splinter_test with %d caches\n", num_caches);
   clockcache *cc = TYPED_ARRAY_MALLOC(hid, cc, num_caches);
   platform_assert(cc != NULL);
   for (uint8 idx = 0; idx < num_caches; idx++) {
      rc = clockcache_init(&cc[idx], &cache_cfg[idx], (io_handle *)io,
                           (allocator *)&al, "test", ts, hh, hid,
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
         rc = test_splinter_perf(splinter_cfg, test_cfg, alp, caches,
                                 num_insert_threads, num_lookup_threads,
                                 num_range_lookup_threads, max_async_inflight,
                                 ts, hid, num_tables, num_caches,
                                 insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case delete:
         rc = test_splinter_delete(splinter_cfg, test_cfg, alp, caches,
                                   num_insert_threads, num_lookup_threads,
                                   max_async_inflight, ts, hid, num_tables,
                                   num_caches, insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case seq_perf:
         rc = test_splinter_perf(splinter_cfg, test_cfg, alp, caches,
                                 num_insert_threads, num_lookup_threads,
                                 num_range_lookup_threads, max_async_inflight,
                                 ts, hid, num_tables, num_caches,
                                 insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case semiseq_perf:
         rc = test_splinter_perf(splinter_cfg, test_cfg, alp, caches,
                                 num_insert_threads, num_lookup_threads,
                                 num_range_lookup_threads, max_async_inflight,
                                 ts, hid, num_tables, num_caches,
                                 insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case parallel_perf:
         platform_assert(max_async_inflight == 0 || use_bg_threads);
         rc = test_splinter_parallel_perf(splinter_cfg, test_cfg, alp, caches,
                                          seed, num_pthreads, 3, 7,
                                          max_async_inflight,
                                          lookup_positive_pct,
                                          ts, hid, num_tables, num_caches);
         platform_assert_status_ok(rc);
         break;
      case periodic:
         rc = test_splinter_periodic(splinter_cfg, test_cfg, alp, caches,
                                     num_insert_threads, num_lookup_threads,
                                     num_range_lookup_threads,
                                     max_async_inflight, ts, hid, num_tables,
                                     num_caches, insert_rate);
         platform_assert(SUCCESS(rc));
         break;
      case functionality:
         for (uint8 i = 0; i < num_tables; i++) {
            splinter_cfg[i].data_cfg->key_to_string = test_data_config.key_to_string;
         }
         rc = test_functionality(alp, (io_handle *)io, caches,
                                 splinter_cfg, seed, test_ops,
                                 correctness_check_frequency, ts, hh, hid,
                                 num_tables, num_caches, max_async_inflight);
         platform_assert_status_ok(rc);
         break;
      case basic:
         platform_assert(num_caches == 1);
         test_ops = splinter_cfg[0].max_tuples_per_node * splinter_cfg[0].fanout;
         rc = test_splinter_basic(alp, (cache *)cc, splinter_cfg, test_ops,
                                  max_async_inflight, ts, hid);
         platform_assert_status_ok(rc);
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
   test_deinit_splinter(hid, ts);
handle_deinit:
   io_handle_deinit(io);
io_free:
   platform_free(hid, io);
cfg_free:
   platform_free(hid, cache_cfg);
   platform_free(hid, data_cfg);
   platform_free(hid, splinter_cfg);
   platform_free(hid, test_cfg);
heap_destroy:
   platform_heap_destroy(&hh);

   return SUCCESS(rc)? 0 : -1;
}
