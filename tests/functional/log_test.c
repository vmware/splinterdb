// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * log_test.c --
 *
 *     This file contains tests for Alex's log
 */
#include "platform_time.h"
#include "platform_sleep.h"
#include "log.h"
#include "shard_log.h"
#include "platform_io.h"
#include "allocator.h"
#include "rc_allocator.h"
#include "cache.h"
#include "clockcache.h"
#include "core.h"
#include "test.h"

#include "poison.h"

#define LOG_TEST_LEAVES_PER_MEMTABLE 97

int
test_log_crash(clockcache             *cc,
               clockcache_config      *cache_cfg,
               io_handle              *io,
               allocator              *al,
               shard_log_config       *cfg,
               task_system            *ts,
               platform_heap_id        hid,
               test_message_generator *gen,
               uint64                  key_size,
               uint64                  num_entries,
               bool32                  crash)

{
   platform_status   rc;
   log_handle       *logh;
   uint64            i;
   key               returned_key;
   message           returned_message;
   log_head          segment;
   log_iterator     *itor;
   char              key_str[128];
   char              data_str[128];
   merge_accumulator msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);

   platform_assert(cc != NULL);
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &logh));

   // The identity is fixed at creation; capture it before writing/sealing.
   segment = log_get_head(logh);

   merge_accumulator_init(&msg, hid);

   for (i = 0; i < num_entries; i++) {
      uint64 entry_num = i;
      if (2 * LOG_TEST_LEAVES_PER_MEMTABLE <= num_entries
          && i < 2 * LOG_TEST_LEAVES_PER_MEMTABLE)
      {
         /*
          * Write the first two generations in the opposite order, and each
          * generation in reverse leaf order.  The iterator must restore the
          * (memtable_generation, leaf_generation) order below.
          */
         uint64 memtable_generation = 1 - i / LOG_TEST_LEAVES_PER_MEMTABLE;
         uint64 leaf_generation =
            LOG_TEST_LEAVES_PER_MEMTABLE - 1 - i % LOG_TEST_LEAVES_PER_MEMTABLE;
         entry_num = memtable_generation * LOG_TEST_LEAVES_PER_MEMTABLE
                     + leaf_generation;
      }
      key skey = test_key(&keybuffer,
                          TEST_RANDOM,
                          entry_num,
                          0,
                          0,
                          1 + (entry_num % key_size),
                          0);
      generate_test_message(gen, entry_num, &msg);
      int log_rc = log_write(logh,
                             skey,
                             merge_accumulator_to_message(&msg),
                             entry_num / LOG_TEST_LEAVES_PER_MEMTABLE,
                             entry_num % LOG_TEST_LEAVES_PER_MEMTABLE);
      platform_assert(log_rc == 0);
   }

   rc = log_seal(logh); // identity captured above
   platform_assert_status_ok(rc);
   log_deinit(logh);
   rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   if (crash) {
      clockcache_deinit(cc);
      rc = clockcache_init(
         cc, cache_cfg, io, al, "crashed", hid, platform_get_module_id());
      platform_assert_status_ok(rc);
   }

   platform_assert_status_ok(
      shard_log_iterator_create((cache *)cc, cfg, hid, segment, 0, &itor));
   // The stream was sealed, so replay must be able to see that it is whole.
   platform_assert(log_iterator_stream_complete(itor));

   for (i = 0; i < num_entries && log_iterator_can_next(itor); i++) {
      key skey =
         test_key(&keybuffer, TEST_RANDOM, i, 0, 0, 1 + (i % key_size), 0);
      generate_test_message(gen, i, &msg);
      message mmessage = merge_accumulator_to_message(&msg);
      log_iterator_curr(itor, &returned_key, &returned_message);
      uint64 memtable_generation;
      uint64 leaf_generation;
      log_iterator_curr_generations(
         itor, &memtable_generation, &leaf_generation);
      platform_assert(memtable_generation == i / LOG_TEST_LEAVES_PER_MEMTABLE);
      platform_assert(leaf_generation == i % LOG_TEST_LEAVES_PER_MEMTABLE);
      if (data_key_compare(cfg->data_cfg, skey, returned_key)
          || message_lex_cmp(mmessage, returned_message))
      {
         platform_default_log("log_test_basic: key or data mismatch\n");
         data_key_to_string(cfg->data_cfg, skey, key_str, 128);
         data_message_to_string(cfg->data_cfg, mmessage, data_str, 128);
         platform_default_log("expected: %s -- %s\n", key_str, data_str);
         data_key_to_string(cfg->data_cfg, returned_key, key_str, 128);
         data_message_to_string(cfg->data_cfg, returned_message, data_str, 128);
         platform_default_log("actual: %s -- %s\n", key_str, data_str);
         platform_assert(0);
      }
      rc = log_iterator_next(itor);
      platform_assert_status_ok(rc);
   }

   platform_default_log("log returned %lu of %lu entries\n", i, num_entries);
   platform_assert(i == num_entries);
   platform_assert(!log_iterator_can_next(itor));

   merge_accumulator_deinit(&msg);

   log_iterator_deinit(itor);
   shard_log_dec_ref((cache *)cc, &segment);

   return 0;
}

static void
test_log_write_range(log_handle             *logh,
                     test_message_generator *gen,
                     platform_heap_id        hid,
                     uint64                  key_size,
                     uint64                  first_entry,
                     uint64                  num_entries)
{
   merge_accumulator msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);
   merge_accumulator_init(&msg, hid);

   for (uint64 i = 0; i < num_entries; i++) {
      uint64 entry_num = first_entry + i;
      key    skey      = test_key(&keybuffer,
                          TEST_RANDOM,
                          entry_num,
                          0,
                          0,
                          1 + (entry_num % key_size),
                          0);
      generate_test_message(gen, entry_num, &msg);
      int log_rc = log_write(
         logh, skey, merge_accumulator_to_message(&msg), entry_num, 0);
      platform_assert(log_rc == 0);
   }

   merge_accumulator_deinit(&msg);
}

static void
test_log_verify_segment(cache                  *cc,
                        shard_log_config       *cfg,
                        const log_head         *segment,
                        test_message_generator *gen,
                        platform_heap_id        hid,
                        uint64                  key_size,
                        uint64                  first_entry,
                        uint64                  num_entries)
{
   log_iterator     *itor;
   merge_accumulator msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);
   key     returned_key;
   message returned_message;

   platform_assert(segment->addr != 0);
   platform_assert(segment->meta_addr != 0);
   platform_assert_status_ok(
      shard_log_iterator_create(cc, cfg, hid, *segment, 0, &itor));
   platform_assert(log_iterator_stream_complete(itor));

   merge_accumulator_init(&msg, hid);
   for (uint64 i = 0; i < num_entries; i++) {
      uint64 entry_num = first_entry + i;
      platform_assert(log_iterator_can_next(itor));
      key skey = test_key(&keybuffer,
                          TEST_RANDOM,
                          entry_num,
                          0,
                          0,
                          1 + (entry_num % key_size),
                          0);
      generate_test_message(gen, entry_num, &msg);
      log_iterator_curr(itor, &returned_key, &returned_message);
      uint64 memtable_generation;
      uint64 leaf_generation;
      log_iterator_curr_generations(
         itor, &memtable_generation, &leaf_generation);
      platform_assert(memtable_generation == entry_num);
      platform_assert(leaf_generation == 0);
      platform_assert(data_key_compare(cfg->data_cfg, skey, returned_key) == 0);
      platform_assert(
         message_lex_cmp(merge_accumulator_to_message(&msg), returned_message)
         == 0);
      platform_status rc = log_iterator_next(itor);
      platform_assert_status_ok(rc);
   }
   platform_assert(!log_iterator_can_next(itor));

   merge_accumulator_deinit(&msg);
   log_iterator_deinit(itor);
}

typedef struct test_log_reserved_writer_params {
   log_handle             *log;
   platform_thread         thread;
   test_message_generator *gen;
   platform_heap_id        hid;
   uint64                  key_size;
   uint64                  entry;
   volatile bool32         reserved;
   volatile bool32         release;
   int                     append_rc;
} test_log_reserved_writer_params;

static void
test_log_reserved_writer(void *arg)
{
   test_log_reserved_writer_params *params = arg;
   merge_accumulator                msg;
   DECLARE_AUTO_KEY_BUFFER(keybuffer, params->hid);

   merge_accumulator_init(&msg, params->hid);
   key skey = test_key(&keybuffer,
                       TEST_RANDOM,
                       params->entry,
                       0,
                       0,
                       1 + (params->entry % params->key_size),
                       0);
   generate_test_message(params->gen, params->entry, &msg);

   log_write_token reserved;
   log_write_reserve(params->log, &reserved);
   __atomic_store_n(&params->reserved, TRUE, __ATOMIC_RELEASE);
   while (!__atomic_load_n(&params->release, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }

   params->append_rc = log_write_reserved(
      &reserved, skey, merge_accumulator_to_message(&msg), params->entry, 0);
   platform_assert(reserved.log == NULL);
   platform_assert(reserved.internal == NULL);
   merge_accumulator_deinit(&msg);
}

static bool32
test_log_wait_for_flag(volatile bool32 *flag)
{
   uint64 start = platform_get_timestamp();
   while (!__atomic_load_n(flag, __ATOMIC_ACQUIRE)
          && platform_timestamp_elapsed(start) < SEC_TO_NSEC(10))
   {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return __atomic_load_n(flag, __ATOMIC_ACQUIRE);
}

/*
 * log_make_durable() mid-stream must produce a stream of several groups that
 * still replays as one sequence.
 *
 * This is the only coverage of the reader's multi-group path: that group ids
 * are dense, that a run of them is accepted in order, and that the records of
 * every closed group survive.  Sealing alone leaves a single group, so nothing
 * else reaches it.
 */
static int
test_log_multiple_groups(clockcache             *cc,
                         clockcache_config      *cache_cfg,
                         io_handle              *io,
                         allocator              *al,
                         shard_log_config       *cfg,
                         platform_heap_id        hid,
                         test_message_generator *gen,
                         uint64                  key_size)
{
   const uint64 num_groups = 4;
   const uint64 per_group  = 32;
   log_head     segment;

   log_handle *log;
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   platform_assert(log_is_empty(log));
   platform_assert_status_ok(log_make_durable(log));
   platform_assert(log_is_empty(log));
   segment = log_get_head(log);

   for (uint64 g = 0; g < num_groups; g++) {
      test_log_write_range(log, gen, hid, key_size, g * per_group, per_group);
      platform_assert(!log_is_empty(log));
      // Ends this group and starts the next; the stream stays open.
      platform_assert_status_ok(log_make_durable(log));
      platform_assert(!log_is_empty(log));
   }

   platform_assert_status_ok(log_seal(log));
   platform_assert(!log_is_empty(log));
   log_deinit(log);

   platform_status rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   // Re-read from a cold cache so only persisted pages are consulted.
   clockcache_deinit(cc);
   rc = clockcache_init(
      cc, cache_cfg, io, al, "multi-group", hid, platform_get_module_id());
   platform_assert_status_ok(rc);

   // Every record of every group must come back, as one sequence.
   test_log_verify_segment((cache *)cc,
                           cfg,
                           &segment,
                           gen,
                           hid,
                           key_size,
                           0,
                           num_groups * per_group);

   shard_log_dec_ref((cache *)cc, &segment);
   return 0;
}

/*
 * Split-phase durability must permit several cuts to be staged before any
 * caller waits.  Waiting for the newest ticket first makes all earlier groups
 * durable with one contiguous barrier; the older tickets then merely consume
 * their pins.  Enough records are used to force ordinary data-page graduation
 * in later groups before their explicit close, exercising physical ordering as
 * well as the partial-page close path.
 */
static int
test_log_pipelined_groups(clockcache             *cc,
                          clockcache_config      *cache_cfg,
                          io_handle              *io,
                          allocator              *al,
                          shard_log_config       *cfg,
                          platform_heap_id        hid,
                          test_message_generator *gen,
                          uint64                  key_size)
{
   const uint64       per_group = 256;
   log_handle        *log;
   log_head           segment;
   log_durable_ticket first_ticket;
   log_durable_ticket second_ticket;

   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   segment = log_get_head(log);

   /*
    * Leave the last record reserved across the cut. It must remain attached to
    * the group selected by reserve, and the cut must not graduate that group
    * until log_write_reserved() consumes the reservation.
    */
   test_log_write_range(log, gen, hid, key_size, 0, per_group - 1);
   test_log_reserved_writer_params writer = {
      .log       = log,
      .gen       = gen,
      .hid       = hid,
      .key_size  = key_size,
      .entry     = per_group - 1,
      .append_rc = -1,
   };
   platform_assert_status_ok(platform_thread_create(
      &writer.thread, FALSE, test_log_reserved_writer, &writer, hid));
   bool32 writer_reserved = test_log_wait_for_flag(&writer.reserved);
   if (!writer_reserved) {
      __atomic_store_n(&writer.release, TRUE, __ATOMIC_RELEASE);
      platform_thread_join(&writer.thread);
   }
   platform_assert(writer_reserved,
                   "reserved writer did not publish its reservation");

   platform_assert_status_ok(log_make_durable_begin(log, &first_ticket));
   platform_assert(first_ticket != 0);
   __atomic_store_n(&writer.release, TRUE, __ATOMIC_RELEASE);
   platform_thread_join(&writer.thread);
   platform_assert(writer.append_rc == 0);

   test_log_write_range(log, gen, hid, key_size, per_group, per_group);
   platform_assert_status_ok(log_make_durable_begin(log, &second_ticket));
   platform_assert(second_ticket > first_ticket);

   test_log_write_range(log, gen, hid, key_size, 2 * per_group, per_group);

   /* The newer waiter may drive and cover the whole contiguous prefix. */
   platform_assert_status_ok(log_make_durable_wait(log, second_ticket));
   platform_assert_status_ok(log_make_durable_wait(log, first_ticket));
   platform_assert_status_ok(log_seal(log));
   log_deinit(log);

   platform_status rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(
      cc, cache_cfg, io, al, "pipelined-groups", hid, platform_get_module_id());
   platform_assert_status_ok(rc);

   test_log_verify_segment(
      (cache *)cc, cfg, &segment, gen, hid, key_size, 0, 3 * per_group);
   shard_log_dec_ref((cache *)cc, &segment);
   return 0;
}

typedef struct test_log_concurrent_begin_params {
   log_handle        *log;
   platform_thread    thread;
   volatile bool32   *start;
   volatile bool32   *release;
   volatile bool32    began;
   platform_status    begin_rc;
   platform_status    wait_rc;
   log_durable_ticket ticket;
} test_log_concurrent_begin_params;

static void
test_log_concurrent_begin(void *arg)
{
   test_log_concurrent_begin_params *params = arg;
   while (!__atomic_load_n(params->start, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }

   params->begin_rc = log_make_durable_begin(params->log, &params->ticket);
   __atomic_store_n(&params->began, TRUE, __ATOMIC_RELEASE);
   while (!__atomic_load_n(params->release, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }

   params->wait_rc = params->begin_rc;
   if (SUCCESS(params->begin_rc)) {
      params->wait_rc = log_make_durable_wait(params->log, params->ticket);
   }
}

static bool32
test_log_wait_for_begins(test_log_concurrent_begin_params *params,
                         uint64                            num_threads)
{
   uint64 start = platform_get_timestamp();
   while (platform_timestamp_elapsed(start) < SEC_TO_NSEC(10)) {
      bool32 all_began = TRUE;
      for (uint64 i = 0; i < num_threads; i++) {
         all_began &= __atomic_load_n(&params[i].began, __ATOMIC_ACQUIRE);
      }
      if (all_began) {
         return TRUE;
      }
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return FALSE;
}

static log_durable_ticket
test_log_concurrent_begin_round(log_handle      *log,
                                platform_heap_id hid,
                                uint64           num_threads)
{
   platform_assert(num_threads <= MAX_THREADS);
   test_log_concurrent_begin_params params[MAX_THREADS] = {0};
   volatile bool32                  start               = FALSE;
   volatile bool32                  release             = FALSE;

   for (uint64 i = 0; i < num_threads; i++) {
      params[i] = (test_log_concurrent_begin_params){
         .log      = log,
         .start    = &start,
         .release  = &release,
         .begin_rc = STATUS_INVALID_STATE,
         .wait_rc  = STATUS_INVALID_STATE,
      };
      platform_assert_status_ok(platform_thread_create(
         &params[i].thread, FALSE, test_log_concurrent_begin, &params[i], hid));
   }

   __atomic_store_n(&start, TRUE, __ATOMIC_RELEASE);
   bool32 all_began = test_log_wait_for_begins(params, num_threads);
   bool32 coalesced = all_began;
   for (uint64 i = 1; i < num_threads && coalesced; i++) {
      coalesced = params[i].ticket == params[0].ticket;
   }

   __atomic_store_n(&release, TRUE, __ATOMIC_RELEASE);
   for (uint64 i = 0; i < num_threads; i++) {
      platform_thread_join(&params[i].thread);
   }

   platform_assert(all_began, "concurrent durability begin timed out");
   platform_assert(coalesced, "concurrent durability begins did not coalesce");
   platform_assert(params[0].ticket != 0);
   for (uint64 i = 0; i < num_threads; i++) {
      platform_assert_status_ok(params[i].begin_rc);
      platform_assert_status_ok(params[i].wait_rc);
   }
   return params[0].ticket;
}

/*
 * Concurrent begin calls racing to cut one used group must all cover the same
 * frontier.  Repeating the race after appending to the successor verifies that
 * the versioned installation claim hands off to the next group rather than
 * looking like a stale claim left behind by the previous installer.
 */
static int
test_log_concurrent_begin_handoff(clockcache             *cc,
                                  clockcache_config      *cache_cfg,
                                  io_handle              *io,
                                  allocator              *al,
                                  shard_log_config       *cfg,
                                  platform_heap_id        hid,
                                  test_message_generator *gen,
                                  uint64                  key_size)
{
   const uint64 num_cutters = 8;
   log_handle  *log;

   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   log_head segment = log_get_head(log);

   shard_log       *slog = (shard_log *)log;
   shard_log_group *initial =
      __atomic_load_n(&slog->accepting.group, __ATOMIC_ACQUIRE);
   platform_assert(initial != NULL);
   platform_assert(initial->id == SHARD_LOG_FIRST_GROUP_ID);
   platform_assert(__atomic_load_n(&slog->accepting.id, __ATOMIC_ACQUIRE)
                   == initial->id);
   platform_assert(__atomic_load_n(&slog->install.state, __ATOMIC_ACQUIRE)
                   == initial->id);

   test_log_write_range(log, gen, hid, key_size, 0, 1);
   log_durable_ticket first =
      test_log_concurrent_begin_round(log, hid, num_cutters);
   platform_assert(first == SHARD_LOG_FIRST_GROUP_ID);
   shard_log_group *successor =
      __atomic_load_n(&slog->accepting.group, __ATOMIC_ACQUIRE);
   platform_assert(successor != NULL);
   platform_assert(successor->id == first + 1);
   platform_assert(__atomic_load_n(&slog->accepting.id, __ATOMIC_ACQUIRE)
                   == successor->id);

   test_log_write_range(log, gen, hid, key_size, 1, 1);
   log_durable_ticket second =
      test_log_concurrent_begin_round(log, hid, num_cutters);
   platform_assert(second == first + 1);

   platform_assert_status_ok(log_seal(log));
   log_deinit(log);

   platform_status rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(cc,
                        cache_cfg,
                        io,
                        al,
                        "concurrent-begin-handoff",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);
   test_log_verify_segment(
      (cache *)cc, cfg, &segment, gen, hid, key_size, 0, 2);
   shard_log_dec_ref((cache *)cc, &segment);
   return 0;
}

typedef struct test_log_begin_actor {
   log_handle        *log;
   platform_thread    thread;
   volatile bool32    entered;
   volatile bool32    began;
   volatile bool32    done;
   platform_status    begin_rc;
   platform_status    wait_rc;
   log_durable_ticket ticket;
} test_log_begin_actor;

static void
test_log_begin_actor_run(void *arg)
{
   test_log_begin_actor *actor = arg;

   __atomic_store_n(&actor->entered, TRUE, __ATOMIC_RELEASE);
   actor->begin_rc = log_make_durable_begin(actor->log, &actor->ticket);
   __atomic_store_n(&actor->began, TRUE, __ATOMIC_RELEASE);
   actor->wait_rc = actor->begin_rc;
   if (SUCCESS(actor->begin_rc)) {
      actor->wait_rc = log_make_durable_wait(actor->log, actor->ticket);
   }
   __atomic_store_n(&actor->done, TRUE, __ATOMIC_RELEASE);
}

typedef struct test_log_seal_actor {
   log_handle     *log;
   platform_thread thread;
   volatile bool32 entered;
   volatile bool32 done;
   platform_status rc;
} test_log_seal_actor;

static void
test_log_seal_actor_run(void *arg)
{
   test_log_seal_actor *actor = arg;

   __atomic_store_n(&actor->entered, TRUE, __ATOMIC_RELEASE);
   actor->rc = log_seal(actor->log);
   __atomic_store_n(&actor->done, TRUE, __ATOMIC_RELEASE);
}

static bool32
test_log_wait_for_install_state(shard_log *log,
                                uint64     expected,
                                bool32     accepting_is_null)
{
   uint64 start = platform_get_timestamp();
   while (platform_timestamp_elapsed(start) < SEC_TO_NSEC(10)) {
      uint64 state = __atomic_load_n(&log->install.state, __ATOMIC_ACQUIRE);
      shard_log_group *accepting =
         __atomic_load_n(&log->accepting.group, __ATOMIC_ACQUIRE);
      if (state == expected && ((accepting == NULL) == accepting_is_null)) {
         return TRUE;
      }
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   return FALSE;
}

static void
test_log_assert_begin_actor(const test_log_begin_actor *actor)
{
   platform_assert(__atomic_load_n(&actor->began, __ATOMIC_ACQUIRE));
   platform_assert(__atomic_load_n(&actor->done, __ATOMIC_ACQUIRE));
   platform_assert_status_ok(actor->begin_rc);
   platform_assert(actor->ticket != 0);
   platform_assert_status_ok(actor->wait_rc);
}

static void
test_log_finish_claim_race(clockcache             *cc,
                           clockcache_config      *cache_cfg,
                           io_handle              *io,
                           allocator              *al,
                           shard_log_config       *cfg,
                           platform_heap_id        hid,
                           test_message_generator *gen,
                           uint64                  key_size,
                           log_handle             *log,
                           const log_head         *segment,
                           char                   *cache_name,
                           uint64                  first_entry,
                           uint64                  num_entries)
{
   /* A second seal must remain a successful no-op after either race. */
   platform_assert_status_ok(log_seal(log));
   log_deinit(log);

   platform_assert_status_ok(cache_writeback_dirty((cache *)cc));
   platform_assert_status_ok(cache_durable_barrier((cache *)cc));

   clockcache_deinit(cc);
   platform_assert_status_ok(clockcache_init(
      cc, cache_cfg, io, al, cache_name, hid, platform_get_module_id()));
   test_log_verify_segment(
      (cache *)cc, cfg, segment, gen, hid, key_size, first_entry, num_entries);
   shard_log_dec_ref((cache *)cc, segment);
}

/*
 * Pin group_lock after a record is present, so make_durable_begin() can win
 * the atomic successor-installation claim but cannot publish that successor.
 * Starting seal only after observing that claim makes this ordering
 * deterministic; releasing group_lock then lets begin publish and seal claim
 * and terminate the successor.
 */
static int
test_log_begin_claim_precedes_seal(clockcache             *cc,
                                   clockcache_config      *cache_cfg,
                                   io_handle              *io,
                                   allocator              *al,
                                   shard_log_config       *cfg,
                                   platform_heap_id        hid,
                                   test_message_generator *gen,
                                   uint64                  key_size)
{
   log_handle *log;
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   log_head segment = log_get_head(log);
   test_log_write_range(log, gen, hid, key_size, 0, 1);

   shard_log       *slog = (shard_log *)log;
   shard_log_group *accepting_before =
      __atomic_load_n(&slog->accepting.group, __ATOMIC_ACQUIRE);
   uint64 accepting_ticket_before =
      __atomic_load_n(&slog->accepting.id, __ATOMIC_ACQUIRE);
   platform_assert(accepting_before != NULL);
   platform_assert(__atomic_load_n(&slog->install.state, __ATOMIC_ACQUIRE)
                   == accepting_before->id);

   platform_mutex_lock(&slog->group_lock);
   test_log_begin_actor begin = {
      .log      = log,
      .begin_rc = STATUS_INVALID_STATE,
      .wait_rc  = STATUS_INVALID_STATE,
   };
   platform_assert_status_ok(platform_thread_create(
      &begin.thread, FALSE, test_log_begin_actor_run, &begin, hid));

   bool32 begin_claimed =
      test_log_wait_for_install_state(slog, accepting_before->id + 1, FALSE);
   if (!begin_claimed) {
      platform_mutex_unlock(&slog->group_lock);
      platform_thread_join(&begin.thread);
   }
   platform_assert(begin_claimed,
                   "make_durable_begin did not publish its install claim");
   platform_assert(__atomic_load_n(&slog->accepting.group, __ATOMIC_ACQUIRE)
                   == accepting_before);
   platform_assert(__atomic_load_n(&slog->accepting.id, __ATOMIC_ACQUIRE)
                   == accepting_ticket_before);

   test_log_seal_actor seal = {
      .log = log,
      .rc  = STATUS_INVALID_STATE,
   };
   platform_assert_status_ok(platform_thread_create(
      &seal.thread, FALSE, test_log_seal_actor_run, &seal, hid));
   bool32 seal_entered = test_log_wait_for_flag(&seal.entered);
   platform_mutex_unlock(&slog->group_lock);

   platform_thread_join(&begin.thread);
   platform_thread_join(&seal.thread);
   platform_assert(seal_entered, "seal worker did not start");
   test_log_assert_begin_actor(&begin);
   platform_assert(__atomic_load_n(&seal.done, __ATOMIC_ACQUIRE));
   platform_assert_status_ok(seal.rc);

   test_log_finish_claim_race(cc,
                              cache_cfg,
                              io,
                              al,
                              cfg,
                              hid,
                              gen,
                              key_size,
                              log,
                              &segment,
                              "begin-claim-before-seal",
                              0,
                              1);
   return 0;
}

/*
 * Hold a real write reservation so seal can publish its terminal claim and
 * remove the accepting group but cannot finish graduating it.  A concurrent
 * begin must return the seal ticket while both its wait and seal itself remain
 * blocked on that reservation.  Consuming the reservation then releases both.
 */
static int
test_log_seal_claim_precedes_begin(clockcache             *cc,
                                   clockcache_config      *cache_cfg,
                                   io_handle              *io,
                                   allocator              *al,
                                   shard_log_config       *cfg,
                                   platform_heap_id        hid,
                                   test_message_generator *gen,
                                   uint64                  key_size)
{
   log_handle *log;
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   log_head segment = log_get_head(log);
   test_log_write_range(log, gen, hid, key_size, 1, 1);

   test_log_reserved_writer_params writer = {
      .log       = log,
      .gen       = gen,
      .hid       = hid,
      .key_size  = key_size,
      .entry     = 2,
      .append_rc = -1,
   };
   platform_assert_status_ok(platform_thread_create(
      &writer.thread, FALSE, test_log_reserved_writer, &writer, hid));
   bool32 writer_reserved = test_log_wait_for_flag(&writer.reserved);
   if (!writer_reserved) {
      __atomic_store_n(&writer.release, TRUE, __ATOMIC_RELEASE);
      platform_thread_join(&writer.thread);
   }
   platform_assert(writer_reserved,
                   "writer did not publish its reservation before seal");

   shard_log       *slog = (shard_log *)log;
   shard_log_group *current =
      __atomic_load_n(&slog->accepting.group, __ATOMIC_ACQUIRE);
   platform_assert(current != NULL);
   uint64 sealing_state = SHARD_LOG_INSTALL_SEALING_BIT | current->id;

   test_log_seal_actor seal = {
      .log = log,
      .rc  = STATUS_INVALID_STATE,
   };
   platform_assert_status_ok(platform_thread_create(
      &seal.thread, FALSE, test_log_seal_actor_run, &seal, hid));
   bool32 seal_claimed =
      test_log_wait_for_install_state(slog, sealing_state, TRUE);
   if (!seal_claimed) {
      __atomic_store_n(&writer.release, TRUE, __ATOMIC_RELEASE);
      platform_thread_join(&writer.thread);
      platform_thread_join(&seal.thread);
   }
   platform_assert(seal_claimed,
                   "seal did not publish its terminal installation claim");
   platform_assert(!__atomic_load_n(&seal.done, __ATOMIC_ACQUIRE),
                   "seal ignored an outstanding write reservation");

   test_log_begin_actor begin = {
      .log      = log,
      .begin_rc = STATUS_INVALID_STATE,
      .wait_rc  = STATUS_INVALID_STATE,
   };
   platform_assert_status_ok(platform_thread_create(
      &begin.thread, FALSE, test_log_begin_actor_run, &begin, hid));
   bool32 begin_returned = test_log_wait_for_flag(&begin.began);
   platform_assert(begin_returned,
                   "begin did not return the in-progress seal ticket");
   platform_assert_status_ok(begin.begin_rc);
   platform_assert(begin.ticket != 0);
   platform_assert(!__atomic_load_n(&begin.done, __ATOMIC_ACQUIRE),
                   "begin wait ignored an outstanding write reservation");

   __atomic_store_n(&writer.release, TRUE, __ATOMIC_RELEASE);
   platform_thread_join(&writer.thread);
   platform_thread_join(&begin.thread);
   platform_thread_join(&seal.thread);
   platform_assert(writer.append_rc == 0);
   test_log_assert_begin_actor(&begin);
   platform_assert(__atomic_load_n(&seal.done, __ATOMIC_ACQUIRE));
   platform_assert_status_ok(seal.rc);

   test_log_finish_claim_race(cc,
                              cache_cfg,
                              io,
                              al,
                              cfg,
                              hid,
                              gen,
                              key_size,
                              log,
                              &segment,
                              "seal-claim-before-begin",
                              1,
                              2);
   return 0;
}

/*
 * A log append happens after its corresponding memtable update is visible, so
 * any append failure permanently invalidates that durability group. Verify
 * that the first failure is retained by both wait and seal, that a clean
 * predecessor can still become durable, and that neither records already
 * staged in the poisoned group nor records accepted into a later group become
 * replayable.
 */
static int
test_log_append_failure_poison(clockcache             *cc,
                               clockcache_config      *cache_cfg,
                               io_handle              *io,
                               allocator              *al,
                               shard_log_config       *cfg,
                               platform_heap_id        hid,
                               test_message_generator *gen,
                               uint64                  key_size)
{
   cache        *cacheh = (cache *)cc;
   log_handle   *log;
   log_iterator *itor;

   platform_assert_status_ok(shard_log_create(cacheh, cfg, hid, &log));
   log_head segment = log_get_head(log);

   /* Leave the clean predecessor unwaited so the poisoned wait must drive it.
    */
   test_log_write_range(log, gen, hid, key_size, 0, 1);
   log_durable_ticket clean_ticket;
   platform_assert_status_ok(log_make_durable_begin(log, &clean_ticket));

   /*
    * Force ordinary data pages out of the next group before poisoning it. The
    * missing terminator must make recovery discard every one of those pages.
    */
   test_log_write_range(log, gen, hid, key_size, 1, 256);

   blob invalid_blob = {
      .length   = 0,
      .checksum = {0},
      .format   = BLOB_FORMAT + 1,
   };
   message invalid_msg =
      message_create(MESSAGE_TYPE_INSERT,
                     cacheh,
                     slice_create(sizeof(invalid_blob), &invalid_blob));
   DECLARE_AUTO_KEY_BUFFER(keybuffer, hid);
   key invalid_key = test_key(&keybuffer, TEST_RANDOM, 257, 0, 0, key_size, 0);
   int append_rc   = log_write(log, invalid_key, invalid_msg, 257, 0);
   platform_assert(append_rc == STATUS_INVALID_STATE.r);

   log_durable_ticket poisoned_ticket;
   platform_assert_status_ok(log_make_durable_begin(log, &poisoned_ticket));
   platform_assert(poisoned_ticket > clean_ticket);

   /* A raw later append may stage, but it must never cross the poison on disk.
    */
   test_log_write_range(log, gen, hid, key_size, 258, 1);

   platform_status rc = log_make_durable_wait(log, poisoned_ticket);
   platform_assert(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
   rc = log_make_durable_wait(log, clean_ticket);
   platform_assert_status_ok(rc);

   rc = log_seal(log);
   platform_assert(STATUS_IS_EQ(rc, STATUS_INVALID_STATE));
   log_deinit(log);

   rc = cache_writeback_dirty(cacheh);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier(cacheh);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(cc,
                        cache_cfg,
                        io,
                        al,
                        "append-failure-poison",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);

   platform_assert_status_ok(
      shard_log_iterator_create((cache *)cc, cfg, hid, segment, 0, &itor));
   platform_assert(!log_iterator_stream_complete(itor));
   platform_assert(log_iterator_can_next(itor));
   uint64 memtable_generation;
   uint64 leaf_generation;
   log_iterator_curr_generations(itor, &memtable_generation, &leaf_generation);
   platform_assert(memtable_generation == 0);
   platform_assert(leaf_generation == 0);
   platform_assert_status_ok(log_iterator_next(itor));
   platform_assert(!log_iterator_can_next(itor));

   log_iterator_deinit(itor);
   shard_log_dec_ref((cache *)cc, &segment);
   return 0;
}

/*
 * A begin ticket pins both the handle and the stream's mini allocator.  The
 * owner may retire the handle and release its on-disk head before the waiter
 * runs; the ticket must keep graduation safe and perform the final cleanup.
 */
static int
test_log_ticket_lifetime(cache                  *cc,
                         shard_log_config       *cfg,
                         platform_heap_id        hid,
                         test_message_generator *gen,
                         uint64                  key_size)
{
   log_handle *log;
   platform_assert_status_ok(shard_log_create(cc, cfg, hid, &log));
   log_head head = log_get_head(log);
   test_log_write_range(log, gen, hid, key_size, 0, 1);

   log_durable_ticket ticket;
   platform_assert_status_ok(log_make_durable_begin(log, &ticket));
   platform_assert(ticket != 0);

   log_deinit(log);
   shard_log_dec_ref(cc, &head);
   platform_assert_status_ok(log_make_durable_wait(log, ticket));
   return 0;
}

/*
 * Sealing a stream and creating a fresh one must yield two distinct,
 * independently replayable segments. Reinitializing the cache after each forced
 * physical persistence cut makes this test exercise only persisted pages for
 * both identities; it does not model logical durable-tail publication.
 */
static int
test_log_two_segments(clockcache             *cc,
                      clockcache_config      *cache_cfg,
                      io_handle              *io,
                      allocator              *al,
                      shard_log_config       *cfg,
                      platform_heap_id        hid,
                      test_message_generator *gen,
                      uint64                  key_size)
{
   const uint64 old_first = 1000, old_count = 16;
   const uint64 new_first = 2000, new_count = 16;
   log_head     sealed, fresh;

   log_handle *log;
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   sealed = log_get_head(log); // identity is fixed at creation
   test_log_write_range(log, gen, hid, key_size, old_first, old_count);

   platform_status rc = log_seal(log);
   platform_assert_status_ok(rc);
   log_deinit(log);
   platform_assert(sealed.addr != 0);
   platform_assert(sealed.meta_addr != 0);

   rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(
      cc, cache_cfg, io, al, "sealed-old", hid, platform_get_module_id());
   platform_assert_status_ok(rc);
   test_log_verify_segment(
      (cache *)cc, cfg, &sealed, gen, hid, key_size, old_first, old_count);

   // A fresh stream is a distinct segment: new mini allocator and new nonce.
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   fresh = log_get_head(log);
   test_log_write_range(log, gen, hid, key_size, new_first, new_count);
   rc = log_seal(log);
   platform_assert_status_ok(rc);
   log_deinit(log);
   platform_assert(fresh.addr != 0);
   platform_assert(fresh.meta_addr != 0);
   platform_assert(sealed.meta_addr != fresh.meta_addr);
   platform_assert(!log_nonce_is_equal(sealed.nonce, fresh.nonce));

   rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(
      cc, cache_cfg, io, al, "sealed-new", hid, platform_get_module_id());
   platform_assert_status_ok(rc);
   test_log_verify_segment(
      (cache *)cc, cfg, &sealed, gen, hid, key_size, old_first, old_count);
   test_log_verify_segment(
      (cache *)cc, cfg, &fresh, gen, hid, key_size, new_first, new_count);

   shard_log_dec_ref((cache *)cc, &sealed);
   shard_log_dec_ref((cache *)cc, &fresh);
   return 0;
}

static int
test_log_large_message(cache *cc, shard_log_config *cfg, platform_heap_id hid)
{
   platform_status   rc;
   log_head          sealed;
   log_iterator     *itor;
   merge_accumulator msg;
   key               returned_key;
   message           returned_message;
   char              key_data[] = "large-log-key";
   key               skey = key_create(FALSE, sizeof(key_data) - 1, key_data);
   /* Exercise blob_writeback's whole-extent path and its partial tail. */
   uint64 value_len = cache_extent_size(cc) + 3 * cache_page_size(cc) + 123;

   log_handle *logh;
   platform_assert_status_ok(shard_log_create(cc, cfg, hid, &logh));
   sealed = log_get_head(logh); // identity is fixed at creation

   merge_accumulator_init(&msg, hid);
   bool32 success = merge_accumulator_resize(&msg, value_len);
   platform_assert(success);
   merge_accumulator_set_class(&msg, MESSAGE_TYPE_INSERT);
   memset(merge_accumulator_data(&msg), 'L', value_len);

   int log_rc = log_write(logh, skey, merge_accumulator_to_message(&msg), 0, 0);
   platform_assert(log_rc == 0);

   merge_accumulator filler;
   merge_accumulator_init(&filler, hid);
   success = merge_accumulator_resize(&filler, cache_page_size(cc) / 4);
   platform_assert(success);
   merge_accumulator_set_class(&filler, MESSAGE_TYPE_INSERT);
   memset(
      merge_accumulator_data(&filler), 'f', merge_accumulator_length(&filler));
   for (uint64 i = 1; i < 16; i++) {
      log_rc = log_write(
         logh, skey, merge_accumulator_to_message(&filler), i / 4, i % 4);
      platform_assert(log_rc == 0);
   }
   merge_accumulator_deinit(&filler);

   rc = log_seal(logh); // identity captured above
   platform_assert_status_ok(rc);
   log_deinit(logh);
   rc = cache_writeback_dirty(cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier(cc);
   platform_assert_status_ok(rc);

   platform_assert_status_ok(
      shard_log_iterator_create(cc, cfg, hid, sealed, 0, &itor));
   platform_assert(log_iterator_stream_complete(itor));
   platform_assert(log_iterator_can_next(itor));

   log_iterator_curr(itor, &returned_key, &returned_message);
   platform_assert(data_key_compare(cfg->data_cfg, skey, returned_key) == 0);
   platform_assert(
      message_lex_cmp(merge_accumulator_to_message(&msg), returned_message)
      == 0);

   log_iterator_deinit(itor);
   merge_accumulator_deinit(&msg);
   shard_log_dec_ref(cc, &sealed);
   return 0;
}

/*
 * A checksum failure in one blob invalidates its whole log group, not merely
 * that record.  Earlier durable groups remain a replayable prefix.
 */
static int
test_log_blob_checksum_prefix(clockcache        *cc,
                              clockcache_config *cache_cfg,
                              io_handle         *io,
                              allocator         *al,
                              shard_log_config  *cfg,
                              platform_heap_id   hid)
{
   cache            *cacheh = (cache *)cc;
   platform_status   rc;
   log_handle       *log;
   char              key_data[] = "blob-checksum-prefix";
   key               skey = key_create(FALSE, sizeof(key_data) - 1, key_data);
   merge_accumulator msg;
   merge_accumulator_init(&msg, hid);

   platform_assert_status_ok(shard_log_create(cacheh, cfg, hid, &log));
   log_head sealed = log_get_head(log);

   bool32 success = merge_accumulator_resize(&msg, 32);
   platform_assert(success);
   merge_accumulator_set_class(&msg, MESSAGE_TYPE_INSERT);
   memset(merge_accumulator_data(&msg), 'A', merge_accumulator_length(&msg));
   platform_assert(
      log_write(log, skey, merge_accumulator_to_message(&msg), 0, 0) == 0);
   platform_assert_status_ok(log_make_durable(log));

   success = merge_accumulator_resize(&msg, cache_page_size(cacheh) + 123);
   platform_assert(success);
   merge_accumulator_set_class(&msg, MESSAGE_TYPE_INSERT);
   memset(merge_accumulator_data(&msg), 'B', merge_accumulator_length(&msg));
   platform_assert(
      log_write(log, skey, merge_accumulator_to_message(&msg), 1, 0) == 0);
   platform_assert_status_ok(log_seal(log));
   log_deinit(log);

   rc = cache_writeback_dirty(cacheh);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier(cacheh);
   platform_assert_status_ok(rc);

   /* Locate a byte in the blob referenced by the second group. */
   log_iterator *itor;
   platform_assert_status_ok(
      shard_log_iterator_create(cacheh, cfg, hid, sealed, 0, &itor));
   platform_assert(log_iterator_stream_complete(itor));
   platform_assert(log_iterator_can_next(itor));
   platform_assert_status_ok(log_iterator_next(itor));
   platform_assert(log_iterator_can_next(itor));

   key     returned_key;
   message returned_message;
   log_iterator_curr(itor, &returned_key, &returned_message);
   platform_assert(message_is_blob(returned_message));

   slice              sblob = message_slice(returned_message);
   blob_page_iterator blob_itor;
   rc = blob_page_iterator_init(cacheh,
                                &blob_itor,
                                sblob,
                                blob_length(sblob) - 1,
                                BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH);
   platform_assert_status_ok(rc);
   uint64 ignored_offset;
   slice  ignored_data;
   rc = blob_page_iterator_get_curr(&blob_itor, &ignored_offset, &ignored_data);
   platform_assert_status_ok(rc);
   uint64 corrupt_page_addr   = blob_itor.fragment.addr;
   uint64 corrupt_page_offset = blob_itor.fragment.offset;
   blob_page_iterator_deinit(&blob_itor);
   log_iterator_deinit(itor);

   page_handle *page =
      cache_get(cacheh, corrupt_page_addr, TRUE, PAGE_TYPE_BLOB);
   while (!cache_try_claim(cacheh, page)) {
      cache_unget(cacheh, page);
      page = cache_get(cacheh, corrupt_page_addr, TRUE, PAGE_TYPE_BLOB);
   }
   cache_lock(cacheh, page);
   page->data[corrupt_page_offset] ^= 0x80;
   cache_unlock(cacheh, page);
   cache_unclaim(cacheh, page);
   cache_unget(cacheh, page);

   rc = cache_writeback_dirty(cacheh);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier(cacheh);
   platform_assert_status_ok(rc);

   /* Force replay to consult only the corrupted durable image. */
   clockcache_deinit(cc);
   rc = clockcache_init(cc,
                        cache_cfg,
                        io,
                        al,
                        "blob-checksum-prefix",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);

   platform_assert_status_ok(
      shard_log_iterator_create((cache *)cc, cfg, hid, sealed, 0, &itor));
   platform_assert(!log_iterator_stream_complete(itor));
   platform_assert(log_iterator_can_next(itor));
   uint64 memtable_generation;
   uint64 leaf_generation;
   log_iterator_curr_generations(itor, &memtable_generation, &leaf_generation);
   platform_assert(memtable_generation == 0);
   platform_assert(leaf_generation == 0);
   platform_assert_status_ok(log_iterator_next(itor));
   platform_assert(!log_iterator_can_next(itor));

   log_iterator_deinit(itor);
   merge_accumulator_deinit(&msg);
   shard_log_dec_ref((cache *)cc, &sealed);
   return 0;
}

/*
 * Recovery must treat pages beyond a regular file's EOF as absent rather than
 * relaxing all reads.  Cover both shapes that motivated the range query: a
 * fresh stream whose initial data extent has no page at all, and an extent
 * whose last page write was torn at EOF.
 */
static int
test_log_recovery_at_eof(clockcache        *cc,
                         clockcache_config *cache_cfg,
                         io_handle         *io,
                         allocator         *al,
                         shard_log_config  *cfg,
                         platform_heap_id   hid,
                         const char        *filename)
{
   platform_status rc;

   log_handle *log;
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   log_head empty = log_get_head(log);

   /* No log page has been written: EOF is exactly the initial extent base. */
   io_wait_all(io);
   int sys_rc = truncate(filename, empty.addr);
   platform_assert(
      sys_rc == 0, "truncate(%s) failed with errno %d", filename, errno);

   log_iterator *itor;
   platform_assert_status_ok(
      shard_log_iterator_create((cache *)cc, cfg, hid, empty, 0, &itor));
   platform_assert(!log_iterator_can_next(itor));
   platform_assert(!log_iterator_stream_complete(itor));
   log_iterator_deinit(itor);

   log_deinit(log);
   shard_log_dec_ref((cache *)cc, &empty);

   /*
    * Four half-page values force at least four distinct log pages.  Keep two
    * complete pages and half of the next one; because the group's terminator
    * was on a later page, replay must discard the group rather than return a
    * prefix of it.
    */
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   log_head partial = log_get_head(log);

   char              key_data[] = "partial-log-extent";
   key               skey = key_create(FALSE, sizeof(key_data) - 1, key_data);
   merge_accumulator msg;
   merge_accumulator_init(&msg, hid);
   bool32 success =
      merge_accumulator_resize(&msg, cache_page_size((cache *)cc) / 2);
   platform_assert(success);
   merge_accumulator_set_class(&msg, MESSAGE_TYPE_INSERT);
   memset(merge_accumulator_data(&msg), 'P', merge_accumulator_length(&msg));

   for (uint64 i = 0; i < 4; i++) {
      int log_rc =
         log_write(log, skey, merge_accumulator_to_message(&msg), i, 0);
      platform_assert(log_rc == 0);
   }
   rc = log_seal(log);
   platform_assert_status_ok(rc);
   log_deinit(log);
   merge_accumulator_deinit(&msg);

   rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);
   uint64 page_size = cache_page_size((cache *)cc);
   clockcache_deinit(cc);

   uint64 partial_eof = partial.addr + 2 * page_size + page_size / 2;
   sys_rc             = truncate(filename, partial_eof);
   platform_assert(
      sys_rc == 0, "truncate(%s) failed with errno %d", filename, errno);

   rc = clockcache_init(
      cc, cache_cfg, io, al, "partial-log-eof", hid, platform_get_module_id());
   platform_assert_status_ok(rc);

   platform_assert_status_ok(
      shard_log_iterator_create((cache *)cc, cfg, hid, partial, 0, &itor));
   platform_assert(!log_iterator_can_next(itor));
   platform_assert(!log_iterator_stream_complete(itor));
   log_iterator_deinit(itor);
   shard_log_dec_ref((cache *)cc, &partial);

   return 0;
}

typedef struct test_log_thread_params {
   log_handle             *logh;
   platform_thread         thread;
   int                     thread_id;
   test_message_generator *gen;
   uint64                  key_size;
   uint64                  num_entries;
} test_log_thread_params;

void
test_log_thread(void *arg)
{
   platform_heap_id        hid         = platform_get_heap_id();
   test_log_thread_params *params      = (test_log_thread_params *)arg;
   log_handle             *logh        = params->logh;
   int                     thread_id   = params->thread_id;
   uint64                  num_entries = params->num_entries;
   test_message_generator *gen         = params->gen;
   uint64                  key_size    = params->key_size;
   uint64                  i;
   merge_accumulator       msg;
   DECLARE_AUTO_KEY_BUFFER(keybuf, hid);

   merge_accumulator_init(&msg, hid);

   for (i = thread_id * num_entries; i < (thread_id + 1) * num_entries; i++) {
      key skey = test_key(&keybuf, TEST_RANDOM, i, 0, 0, key_size, 0);
      generate_test_message(gen, i, &msg);
      int log_rc = log_write(
         logh, skey, merge_accumulator_to_message(&msg), i / 1024, i % 1024);
      platform_assert(log_rc == 0);
   }

   merge_accumulator_deinit(&msg);
}

typedef struct test_log_pipeline_writer_params {
   log_handle             *log;
   platform_thread         thread;
   test_message_generator *gen;
   platform_heap_id        hid;
   uint64                  key_size;
   uint64                  first;
   uint64                  count;
   volatile bool32        *start;
} test_log_pipeline_writer_params;

static void
test_log_pipeline_writer(void *arg)
{
   test_log_pipeline_writer_params *params = arg;
   while (!__atomic_load_n(params->start, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(100);
   }
   test_log_write_range(params->log,
                        params->gen,
                        params->hid,
                        params->key_size,
                        params->first,
                        params->count);
}

typedef struct test_log_pipeline_waiter_params {
   log_handle      *log;
   platform_thread  thread;
   uint64           cuts;
   volatile bool32 *start;
   platform_status  status;
} test_log_pipeline_waiter_params;

static void
test_log_pipeline_waiter(void *arg)
{
   test_log_pipeline_waiter_params *params = arg;
   while (!__atomic_load_n(params->start, __ATOMIC_ACQUIRE)) {
      platform_sleep_ns(100);
   }

   params->status = STATUS_OK;
   for (uint64 i = 0; i < params->cuts; i++) {
      log_durable_ticket ticket;
      params->status = log_make_durable_begin(params->log, &ticket);
      if (!SUCCESS(params->status)) {
         return;
      }
      /* Let other callers cut/stage later groups before this waiter drives. */
      platform_sleep_ns(1000);
      params->status = log_make_durable_wait(params->log, ticket);
      if (!SUCCESS(params->status)) {
         return;
      }
   }
}

/* Concurrent writers and split-phase waiters exercise reservation drains. */
static int
test_log_concurrent_durability(clockcache             *cc,
                               clockcache_config      *cache_cfg,
                               io_handle              *io,
                               allocator              *al,
                               shard_log_config       *cfg,
                               platform_heap_id        hid,
                               test_message_generator *gen,
                               uint64                  key_size)
{
   enum {
      NUM_WRITERS = 4,
      NUM_WAITERS = 2,
   };
   const uint64    entries_per_writer = 1024;
   const uint64    cuts_per_waiter    = 16;
   volatile bool32 start              = FALSE;

   log_handle *log;
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &log));
   log_head segment = log_get_head(log);

   test_log_pipeline_writer_params writers[NUM_WRITERS];
   test_log_pipeline_waiter_params waiters[NUM_WAITERS];
   for (uint64 i = 0; i < NUM_WRITERS; i++) {
      writers[i] = (test_log_pipeline_writer_params){
         .log      = log,
         .gen      = gen,
         .hid      = hid,
         .key_size = key_size,
         .first    = i * entries_per_writer,
         .count    = entries_per_writer,
         .start    = &start,
      };
      platform_assert_status_ok(platform_thread_create(&writers[i].thread,
                                                       FALSE,
                                                       test_log_pipeline_writer,
                                                       &writers[i],
                                                       hid));
   }
   for (uint64 i = 0; i < NUM_WAITERS; i++) {
      waiters[i] = (test_log_pipeline_waiter_params){
         .log = log, .cuts = cuts_per_waiter, .start = &start};
      platform_assert_status_ok(platform_thread_create(&waiters[i].thread,
                                                       FALSE,
                                                       test_log_pipeline_waiter,
                                                       &waiters[i],
                                                       hid));
   }
   __atomic_store_n(&start, TRUE, __ATOMIC_RELEASE);

   for (uint64 i = 0; i < NUM_WRITERS; i++) {
      platform_thread_join(&writers[i].thread);
   }
   for (uint64 i = 0; i < NUM_WAITERS; i++) {
      platform_thread_join(&waiters[i].thread);
      platform_assert_status_ok(waiters[i].status);
   }

   platform_assert_status_ok(log_seal(log));
   log_deinit(log);
   platform_status rc = cache_writeback_dirty((cache *)cc);
   platform_assert_status_ok(rc);
   rc = cache_durable_barrier((cache *)cc);
   platform_assert_status_ok(rc);

   clockcache_deinit(cc);
   rc = clockcache_init(cc,
                        cache_cfg,
                        io,
                        al,
                        "concurrent-log-durability",
                        hid,
                        platform_get_module_id());
   platform_assert_status_ok(rc);
   test_log_verify_segment((cache *)cc,
                           cfg,
                           &segment,
                           gen,
                           hid,
                           key_size,
                           0,
                           NUM_WRITERS * entries_per_writer);
   shard_log_dec_ref((cache *)cc, &segment);
   return 0;
}

platform_status
test_log_perf(cache                  *cc,
              shard_log_config       *cfg,
              uint64                  num_entries,
              test_message_generator *gen,
              uint64                  key_size,
              uint64                  num_threads,
              task_system            *ts,
              platform_heap_id        hid)

{
   test_log_thread_params *params =
      TYPED_ARRAY_MALLOC(hid, params, num_threads);
   platform_assert(params);
   uint64          start_time;
   platform_status ret;

   log_handle *logh;
   platform_assert_status_ok(shard_log_create((cache *)cc, cfg, hid, &logh));
   log_head sealed = log_get_head(logh);

   for (uint64 i = 0; i < num_threads; i++) {
      params[i].logh        = logh;
      params[i].thread_id   = i;
      params[i].gen         = gen;
      params[i].key_size    = key_size;
      params[i].num_entries = num_entries / num_threads;
   }

   start_time = platform_get_timestamp();
   for (uint64 i = 0; i < num_threads; i++) {
      ret = platform_thread_create(
         &params[i].thread, FALSE, test_log_thread, &params[i], hid);
      if (!SUCCESS(ret)) {
         // Wait for existing threads to quit
         for (uint64 j = 0; j < i; j++) {
            platform_thread_join(&params[j].thread);
         }
         goto cleanup;
      }
   }
   for (uint64 i = 0; i < num_threads; i++) {
      platform_thread_join(&params[i].thread);
   }

   platform_default_log("log insertion rate: %luM insertions/second\n",
                        SEC_TO_MSEC(num_entries)
                           / platform_timestamp_elapsed(start_time));

cleanup:
   // Finish the stream, free the handle, and release the segment's extents.
   platform_assert_status_ok(log_seal(logh));
   log_deinit(logh);
   shard_log_dec_ref((cache *)cc, &sealed);
   platform_free(hid, params);

   return ret;
}


static void
usage(const char *argv0)
{
   platform_error_log("Usage:\n"
                      "\t%s\n"
                      "\t%s --perf\n"
                      "\t%s --crash\n",
                      argv0,
                      argv0,
                      argv0);
   config_usage();
}

int
log_test(int argc, char *argv[])
{
   platform_status        status;
   system_config          system_cfg;
   rc_allocator           al;
   platform_status        ret;
   int                    config_argc;
   char                 **config_argv;
   bool32                 run_perf_test;
   bool32                 run_crash_test;
   int                    rc;
   uint64                 seed;
   task_system            ts;
   test_message_generator gen;

   platform_register_thread();

   if (argc > 1 && strncmp(argv[1], "--perf", sizeof("--perf")) == 0) {
      run_perf_test  = TRUE;
      run_crash_test = FALSE;
      config_argc    = argc - 2;
      config_argv    = argv + 2;
   } else if (argc > 1 && strncmp(argv[1], "--crash", sizeof("--crash")) == 0) {
      run_perf_test  = FALSE;
      run_crash_test = TRUE;
      config_argc    = argc - 2;
      config_argv    = argv + 2;
   } else {
      run_perf_test  = FALSE;
      run_crash_test = FALSE;
      config_argc    = argc - 1;
      config_argv    = argv + 1;
   }

   bool use_shmem = config_parse_use_shmem(config_argc, config_argv);
   platform_default_log("\nStarted log_test%s!!\n",
                        (use_shmem ? " using shared memory" : ""));

   // Create a heap for io, allocator, cache and splinter
   platform_heap_id hid = NULL;
   status               = platform_heap_create(
      platform_get_module_id(), 512 * MiB, use_shmem, &hid);
   platform_assert_status_ok(status);

   core_config         *cfg                            = TYPED_MALLOC(hid, cfg);
   uint64               num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads
   test_workload_config workload_cfg;

   status = test_parse_args(&system_cfg,
                            &workload_cfg,
                            &seed,
                            &gen,
                            &num_bg_threads[TASK_TYPE_MEMTABLE],
                            &num_bg_threads[TASK_TYPE_NORMAL],
                            config_argc,
                            config_argv);
   if (!SUCCESS(status)) {
      platform_error_log("log_test: failed to parse config: %s\n",
                         platform_status_to_string(status));
      /*
       * Provided arguments but set things up incorrectly.
       * Print usage so client can fix commandline.
       */
      usage(argv[0]);
      rc = -1;
      goto cleanup;
   }

   io_handle *io = io_handle_create(&system_cfg.io_cfg, hid);
   if (io == NULL) {
      platform_error_log("Failed to create IO handle\n");
      rc = -1;
      goto cleanup;
   }

   status = test_init_task_system(&ts, hid, &system_cfg.task_cfg);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(status));
      rc = -1;
      goto destroy_iohandle;
   }

   status = rc_allocator_init(
      &al, &system_cfg.allocator_cfg, io, hid, platform_get_module_id());
   platform_assert_status_ok(status);

   clockcache *cc = TYPED_MALLOC(hid, cc);
   platform_assert(cc != NULL);
   status = clockcache_init(cc,
                            &system_cfg.cache_cfg,
                            io,
                            (allocator *)&al,
                            "test",
                            hid,
                            platform_get_module_id());
   platform_assert_status_ok(status);

   rc = test_log_recovery_at_eof(cc,
                                 &system_cfg.cache_cfg,
                                 io,
                                 (allocator *)&al,
                                 &system_cfg.log_cfg,
                                 hid,
                                 system_cfg.io_cfg.filename);
   platform_assert(rc == 0);

   rc = test_log_large_message((cache *)cc, &system_cfg.log_cfg, hid);
   platform_assert(rc == 0);

   rc = test_log_blob_checksum_prefix(cc,
                                      &system_cfg.cache_cfg,
                                      io,
                                      (allocator *)&al,
                                      &system_cfg.log_cfg,
                                      hid);
   platform_assert(rc == 0);

   rc = test_log_multiple_groups(cc,
                                 &system_cfg.cache_cfg,
                                 io,
                                 (allocator *)&al,
                                 &system_cfg.log_cfg,
                                 hid,
                                 &gen,
                                 workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_pipelined_groups(cc,
                                  &system_cfg.cache_cfg,
                                  io,
                                  (allocator *)&al,
                                  &system_cfg.log_cfg,
                                  hid,
                                  &gen,
                                  workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_concurrent_begin_handoff(cc,
                                          &system_cfg.cache_cfg,
                                          io,
                                          (allocator *)&al,
                                          &system_cfg.log_cfg,
                                          hid,
                                          &gen,
                                          workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_begin_claim_precedes_seal(cc,
                                           &system_cfg.cache_cfg,
                                           io,
                                           (allocator *)&al,
                                           &system_cfg.log_cfg,
                                           hid,
                                           &gen,
                                           workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_seal_claim_precedes_begin(cc,
                                           &system_cfg.cache_cfg,
                                           io,
                                           (allocator *)&al,
                                           &system_cfg.log_cfg,
                                           hid,
                                           &gen,
                                           workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_append_failure_poison(cc,
                                       &system_cfg.cache_cfg,
                                       io,
                                       (allocator *)&al,
                                       &system_cfg.log_cfg,
                                       hid,
                                       &gen,
                                       workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_ticket_lifetime(
      (cache *)cc, &system_cfg.log_cfg, hid, &gen, workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_concurrent_durability(cc,
                                       &system_cfg.cache_cfg,
                                       io,
                                       (allocator *)&al,
                                       &system_cfg.log_cfg,
                                       hid,
                                       &gen,
                                       workload_cfg.key_size);
   platform_assert(rc == 0);

   rc = test_log_two_segments(cc,
                              &system_cfg.cache_cfg,
                              io,
                              (allocator *)&al,
                              &system_cfg.log_cfg,
                              hid,
                              &gen,
                              workload_cfg.key_size);
   platform_assert(rc == 0);

   if (run_perf_test) {
      ret = test_log_perf((cache *)cc,
                          &system_cfg.log_cfg,
                          200000000,
                          &gen,
                          workload_cfg.key_size,
                          16,
                          &ts,
                          hid);
      platform_assert_status_ok(ret);
      rc = 0;
   } else if (run_crash_test) {
      rc = test_log_crash(cc,
                          &system_cfg.cache_cfg,
                          io,
                          (allocator *)&al,
                          &system_cfg.log_cfg,
                          &ts,
                          hid,
                          &gen,
                          workload_cfg.key_size,
                          500000,
                          TRUE /* crash */);
      platform_assert(rc == 0);
   } else {
      rc = test_log_crash(cc,
                          &system_cfg.cache_cfg,
                          io,
                          (allocator *)&al,
                          &system_cfg.log_cfg,
                          &ts,
                          hid,
                          &gen,
                          workload_cfg.key_size,
                          500000,
                          FALSE /* don't crash */);
      platform_assert(rc == 0);
   }

   io_wait_all(io);
   clockcache_deinit(cc);
   platform_free(hid, cc);
   rc_allocator_deinit(&al);
   test_deinit_task_system(&ts);
destroy_iohandle:
   io_handle_destroy(io);
cleanup:
   platform_free(hid, cfg);
   platform_heap_destroy(&hid);
   platform_deregister_thread();
   return rc == 0 ? 0 : -1;
}
