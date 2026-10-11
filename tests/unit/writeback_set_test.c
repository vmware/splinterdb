// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * writeback_set_test.c --
 *
 *     Unit tests for the writeback set (src/writeback_set.c).
 *
 *     These drive a bare clockcache: allocate pages, dirty them, and check that
 *     a set issues their writes, waits for completion, and makes them durable.
 *     The observable that matters is cache_count_dirty() -- if
 *     writeback_set_wait() returned before the writes landed, pages would still
 *     be dirty afterwards.
 */

#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.

#include "functional/test.h"
#include "splinterdb/data.h"
#include "../config.h"
#include "platform_io.h"
#include "platform_units.h"
#include "platform_sleep.h"
#include "rc_allocator.h"
#include "clockcache.h"
// For the init_*_config_from_master_config() helpers.
#include "btree_test_common.h"
#include "writeback_set.h"
#include "poison.h"

CTEST_DATA(writeback_set)
{
   master_config     master_cfg;
   data_config      *data_cfg;
   io_config         io_cfg;
   allocator_config  allocator_cfg;
   clockcache_config cache_cfg;

   platform_heap_id hid;
   io_handle       *io;
   rc_allocator     al;
   clockcache       cc;
};

CTEST_SETUP(writeback_set)
{
   platform_register_thread();
   config_set_defaults(&data->master_cfg);
   data->master_cfg.cache_capacity = MiB_TO_B(64);
   data->data_cfg                  = test_data_config;

   if (!SUCCESS(
          config_parse(&data->master_cfg, 1, Ctest_argc, (char **)Ctest_argv))
       || !init_data_config_from_master_config(data->data_cfg,
                                               &data->master_cfg)
       || !init_io_config_from_master_config(&data->io_cfg, &data->master_cfg)
       || !init_rc_allocator_config_from_master_config(
          &data->allocator_cfg, &data->master_cfg, &data->io_cfg)
       || !init_clockcache_config_from_master_config(
          &data->cache_cfg, &data->master_cfg, &data->io_cfg))
   {
      ASSERT_TRUE(FALSE, "Failed to parse args\n");
   }

   if (!SUCCESS(platform_heap_create(platform_get_module_id(),
                                     512 * MiB,
                                     data->master_cfg.use_shmem,
                                     &data->hid)))
   {
      ASSERT_TRUE(FALSE, "Failed to init heap\n");
   }

   data->io = io_handle_create(&data->io_cfg, data->hid);
   ASSERT_NOT_NULL(data->io);

   ASSERT_TRUE(SUCCESS(rc_allocator_init(&data->al,
                                         &data->allocator_cfg,
                                         data->io,
                                         data->hid,
                                         platform_get_module_id())));
   ASSERT_TRUE(SUCCESS(clockcache_init(&data->cc,
                                       &data->cache_cfg,
                                       data->io,
                                       (allocator *)&data->al,
                                       "test",
                                       data->hid,
                                       platform_get_module_id())));
}

CTEST_TEARDOWN(writeback_set)
{
   clockcache_deinit(&data->cc);
   rc_allocator_deinit(&data->al);
   io_handle_destroy(data->io);
   platform_heap_destroy(&data->hid);
   platform_deregister_thread();
}

/*
 * One extent, deliberately. Issuing many writes before waiting is *less*
 * sensitive, not more: each submission to the io layer tends to reap earlier
 * completions, so a large set cleans itself during the issue phase and a
 * writeback_set_wait() that never waited would still look correct. Keeping the
 * issue phase short leaves the completions genuinely outstanding when wait() is
 * called.
 */

/*
 * Allocate one extent's worth of pages and dirty every one of them. Returns the
 * extent's base address; page i is at base + i * page_size.
 */
static uint64
alloc_and_dirty_extent(clockcache *cc, rc_allocator *al)
{
   uint64          base_addr;
   platform_status rc =
      allocator_alloc((allocator *)al, &base_addr, PAGE_TYPE_MISC);
   ASSERT_TRUE(SUCCESS(rc));

   cache *ccp       = (cache *)cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 pages_per_extent =
      cache_config_pages_per_extent(cache_get_config(ccp));

   for (uint64 i = 0; i < pages_per_extent; i++) {
      uint64       addr = base_addr + i * page_size;
      page_handle  storage;
      page_handle *page = cache_alloc(ccp, addr, PAGE_TYPE_MISC, &storage);
      ASSERT_NOT_NULL(page);
      // cache_alloc hands back a dirty, write-locked page.
      memset(page_handle_data(page), (int)(i & 0xff), page_size);
      cache_unlock(ccp, page);
      cache_unclaim(ccp, page);
      cache_unget(ccp, page);
   }
   return base_addr;
}

/* Reject submissions synchronously, as io_submit() does on a hard error.
 * Preserve the real backend's iovec bookkeeping and state cleanup. */
static const io_ops      *submission_saved_ops;
static io_ops             submission_io_ops;
static io_async_state_ops submission_state_ops;
static uint64             submission_rejections;

static async_status
reject_submission(io_async_state *state)
{
   submission_rejections++;
   return ASYNC_STATUS_DONE;
}

static platform_status
rejected_submission_result(io_async_state *state)
{
   return STATUS_IO_ERROR;
}

static platform_status
reject_write_init(io_async_state   *state,
                  io_handle        *io,
                  io_async_cmd      cmd,
                  uint64            addr,
                  async_callback_fn callback,
                  void             *callback_arg)
{
   platform_status rc = submission_saved_ops->async_state_init(
      state, io, cmd, addr, callback, callback_arg);
   if (SUCCESS(rc) && cmd == io_async_pwritev) {
      submission_state_ops            = *state->ops;
      submission_state_ops.run        = reject_submission;
      submission_state_ops.get_result = rejected_submission_result;
      state->ops                      = &submission_state_ops;
   }
   return rc;
}

static void
reject_write_submissions(io_handle *io)
{
   io_wait_all(io);
   submission_saved_ops               = io->ops;
   submission_io_ops                  = *io->ops;
   submission_io_ops.async_state_init = reject_write_init;
   submission_rejections              = 0;
   io->ops                            = &submission_io_ops;
}

static void
assert_failed_request(writeback_set *set)
{
   ASSERT_EQUAL(1, writeback_set_num_requests(set));
   writeback_set_entry *entry = vector_get_ptr(&set->entries, 0);
   /* Check before waiting so a regression fails instead of hanging. */
   ASSERT_EQUAL(CACHE_WRITEBACK_FAILED,
                cache_writeback_get_status(set->cc, &entry->request));
   ASSERT_TRUE(STATUS_IS_EQ(writeback_set_wait(set), STATUS_IO_ERROR));
}

CTEST2(writeback_set, test_rejected_page_submission)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));
   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);
   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);
   page_handle acquired_page =
      test_cache_get_page(ccp, base_addr, TRUE, PAGE_TYPE_MISC);
   page_handle *page = &acquired_page;

   reject_write_submissions(data->io);
   ASSERT_TRUE(SUCCESS(writeback_set_add_page(&set, page, PAGE_TYPE_MISC)));
   data->io->ops = submission_saved_ops;
   cache_unget(ccp, page);
   ASSERT_EQUAL(1, submission_rejections);
   assert_failed_request(&set);
   ASSERT_EQUAL(num_pages, cache_count_dirty(ccp));

   ASSERT_TRUE(SUCCESS(writeback_set_retry_incomplete(&set)));
   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_EQUAL(num_pages - 1, cache_count_dirty(ccp));
   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

CTEST2(writeback_set, test_rejected_extent_submissions)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));
   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);
   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);

   /* A clean middle page splits the extent into two separate submissions. */
   page_handle acquired_page = test_cache_get_page(
      ccp, base_addr + (num_pages / 2) * page_size, TRUE, PAGE_TYPE_MISC);
   page_handle *page = &acquired_page;
   ASSERT_TRUE(SUCCESS(writeback_set_add_page(&set, page, PAGE_TYPE_MISC)));
   cache_unget(ccp, page);
   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   writeback_set_reset(&set);

   reject_write_submissions(data->io);
   ASSERT_TRUE(
      SUCCESS(writeback_set_add_extent(&set, base_addr, PAGE_TYPE_MISC)));
   data->io->ops = submission_saved_ops;
   ASSERT_EQUAL(2, submission_rejections);
   assert_failed_request(&set);
   ASSERT_EQUAL(num_pages - 1, cache_count_dirty(ccp));

   ASSERT_TRUE(SUCCESS(writeback_set_retry_incomplete(&set)));
   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_EQUAL(0, cache_count_dirty(ccp));
   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

CTEST2(writeback_set, test_rejected_cleaner_submission)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));
   alloc_and_dirty_extent(&data->cc, &data->al);

   reject_write_submissions(data->io);
   platform_status rc = cache_writeback_dirty(ccp);
   data->io->ops      = submission_saved_ops;
   ASSERT_NOT_EQUAL(0, submission_rejections);
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_IO_ERROR));
   ASSERT_EQUAL(num_pages, cache_count_dirty(ccp));

   ASSERT_TRUE(SUCCESS(cache_writeback_dirty(ccp)));
   ASSERT_EQUAL(0, cache_count_dirty(ccp));
   ASSERT_TRUE(SUCCESS(cache_durable_barrier(ccp)));
}

/*
 * A set of individually added pages is issued, waited for, and made durable,
 * and the pages really are clean afterwards.
 */
CTEST2(writeback_set, test_wait_makes_pages_clean)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);
   ASSERT_EQUAL(num_pages, cache_count_dirty(ccp));

   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);

   for (uint64 i = 0; i < num_pages; i++) {
      uint64       addr = base_addr + i * page_size;
      page_handle  acquired_page =
         test_cache_get_page(ccp, addr, TRUE, PAGE_TYPE_MISC);
      page_handle *page = &acquired_page;
      ASSERT_NOT_NULL(page);
      ASSERT_TRUE(SUCCESS(writeback_set_add_page(&set, page, PAGE_TYPE_MISC)));
      cache_unget(ccp, page);
   }
   ASSERT_EQUAL(num_pages, writeback_set_num_requests(&set));

   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   /*
    * Checked before make_durable(), deliberately: wait() is what establishes
    * completion, and letting the barrier run first would give the I/O extra
    * time to land and mask a wait() that did not actually wait.
    */
   ASSERT_EQUAL(0, cache_count_dirty(ccp));

   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

/* The same, but adding the extent in one call rather than page by page. */
CTEST2(writeback_set, test_add_extent)
{
   cache *ccp = (cache *)&data->cc;

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);
   ASSERT_NOT_EQUAL(0, cache_count_dirty(ccp));

   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);

   ASSERT_TRUE(
      SUCCESS(writeback_set_add_extent(&set, base_addr, PAGE_TYPE_MISC)));
   // One request covers the whole extent, however many pages it holds.
   ASSERT_EQUAL(1, writeback_set_num_requests(&set));

   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_EQUAL(0, cache_count_dirty(ccp)); // before the barrier; see above

   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

/* Waiting on an empty set is legal and does nothing. */
CTEST2(writeback_set, test_empty_set)
{
   cache        *ccp = (cache *)&data->cc;
   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);

   ASSERT_EQUAL(0, writeback_set_num_requests(&set));
   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));

   writeback_set_deinit(&set);
}

/*
 * Adding an already-clean page is a no-op the set still accounts for: the
 * request carries gen == 0, and waiting on it completes immediately.
 */
CTEST2(writeback_set, test_add_clean_page)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);

   // Clean everything first, so the pages below are already written back.
   writeback_set flush;
   writeback_set_init(&flush, ccp, data->hid);
   ASSERT_TRUE(
      SUCCESS(writeback_set_add_extent(&flush, base_addr, PAGE_TYPE_MISC)));
   ASSERT_TRUE(SUCCESS(writeback_set_wait(&flush)));
   writeback_set_deinit(&flush);
   ASSERT_EQUAL(0, cache_count_dirty(ccp));

   writeback_set set;
   writeback_set_init(&set, ccp, data->hid);
   for (uint64 i = 0; i < num_pages; i++) {
      uint64       addr = base_addr + i * page_size;
      page_handle  acquired_page =
         test_cache_get_page(ccp, addr, TRUE, PAGE_TYPE_MISC);
      page_handle *page = &acquired_page;
      ASSERT_NOT_NULL(page);
      ASSERT_TRUE(SUCCESS(writeback_set_add_page(&set, page, PAGE_TYPE_MISC)));
      cache_unget(ccp, page);
   }

   ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
   ASSERT_EQUAL(0, cache_count_dirty(ccp)); // before the barrier; see above

   ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
   writeback_set_deinit(&set);
}

/*
 * A set may be reused across rounds: dirty, flush, dirty again, flush again.
 * Catches a wait() that trusted stale receipts from an earlier round.
 */
CTEST2(writeback_set, test_repeated_rounds)
{
   cache *ccp       = (cache *)&data->cc;
   uint64 page_size = cache_config_page_size(cache_get_config(ccp));
   uint64 num_pages = cache_config_pages_per_extent(cache_get_config(ccp));

   uint64 base_addr = alloc_and_dirty_extent(&data->cc, &data->al);

   for (uint64 round = 0; round < 4; round++) {
      if (round > 0) {
         // Re-dirty every page.
         for (uint64 i = 0; i < num_pages; i++) {
            uint64       addr = base_addr + i * page_size;
            page_handle  acquired_page =
               test_cache_get_page(ccp, addr, TRUE, PAGE_TYPE_MISC);
            page_handle *page = &acquired_page;
            ASSERT_NOT_NULL(page);
            while (!cache_try_claim(ccp, page)) {
               cache_unget(ccp, page);
               *page = test_cache_get_page(ccp, addr, TRUE, PAGE_TYPE_MISC);
            }
            cache_lock(ccp, page);
            memset(page_handle_data(page), (int)(round & 0xff), page_size);
            cache_unlock(ccp, page);
            cache_unclaim(ccp, page);
            cache_unget(ccp, page);
         }
         ASSERT_NOT_EQUAL(0, cache_count_dirty(ccp));
      }

      writeback_set set;
      writeback_set_init(&set, ccp, data->hid);
      ASSERT_TRUE(
         SUCCESS(writeback_set_add_extent(&set, base_addr, PAGE_TYPE_MISC)));
      ASSERT_TRUE(SUCCESS(writeback_set_wait(&set)));
      ASSERT_EQUAL(0, cache_count_dirty(ccp)); // before the barrier; see above
      ASSERT_TRUE(SUCCESS(writeback_set_make_durable(&set)));
      writeback_set_deinit(&set);
   }
}

/* Exercise cache read failures below the cache API, including shared loading
 * entries. A timeout (rather than EIO) verifies exact status propagation. */
static const io_ops      *read_saved_ops;
static io_ops             read_fault_ops;
static io_async_state_ops read_fault_state_ops;
static async_callback_fn  read_fault_callback;
static void              *read_fault_callback_arg;
static bool32             read_fault_complete;
static bool32             read_fault_reject_init;
static bool32             read_fault_reject_append;

static platform_status
reject_read(io_handle *io, void *buf, uint64 bytes, uint64 addr)
{
   return STATUS_TIMEDOUT;
}

static async_status
read_fault_run(io_async_state *state)
{
   return read_fault_complete ? ASYNC_STATUS_DONE : ASYNC_STATUS_RUNNING;
}

static platform_status
read_fault_result(io_async_state *state)
{
   return STATUS_TIMEDOUT;
}

static platform_status
read_fault_append(io_async_state *state, void *buf)
{
   return STATUS_NO_MEMORY;
}

static platform_status
read_fault_init(io_async_state   *state,
                io_handle        *io,
                io_async_cmd      cmd,
                uint64            addr,
                async_callback_fn callback,
                void             *callback_arg)
{
   if (cmd == io_async_preadv && read_fault_reject_init) {
      return STATUS_NO_MEMORY;
   }
   platform_status rc = read_saved_ops->async_state_init(
      state, io, cmd, addr, callback, callback_arg);
   if (SUCCESS(rc) && cmd == io_async_preadv) {
      read_fault_state_ops            = *state->ops;
      read_fault_state_ops.run        = read_fault_run;
      read_fault_state_ops.get_result = read_fault_result;
      if (read_fault_reject_append) {
         read_fault_state_ops.append_page = read_fault_append;
      }
      state->ops              = &read_fault_state_ops;
      read_fault_callback     = callback;
      read_fault_callback_arg = callback_arg;
   }
   return rc;
}

static void
read_fault_install(io_handle *io)
{
   io_wait_all(io);
   read_saved_ops                  = io->ops;
   read_fault_ops                  = *io->ops;
   read_fault_ops.read             = reject_read;
   read_fault_ops.async_state_init = read_fault_init;
   read_fault_complete             = FALSE;
   read_fault_reject_init          = FALSE;
   read_fault_reject_append        = FALSE;
   io->ops                         = &read_fault_ops;
}

static void
read_ready(void *arg)
{
   bool32 *ready = arg;
   *ready        = TRUE;
}

CTEST2(writeback_set, test_cache_sync_read_error_and_retry)
{
   cache *cc   = (cache *)&data->cc;
   uint64 addr = alloc_and_dirty_extent(&data->cc, &data->al);
   cache_flush(cc);
   ASSERT_TRUE(SUCCESS(cache_evict(cc, FALSE)));
   page_handle page = {.page = (cache_page *)1};
   ASSERT_TRUE(STATUS_IS_EQ(cache_get(cc, addr, FALSE, PAGE_TYPE_MISC, &page),
                            STATUS_BUSY));
   ASSERT_TRUE(page_handle_is_null(&page));
   read_fault_install(data->io);
   platform_status rc = cache_get(cc, addr, TRUE, PAGE_TYPE_MISC, &page);
   data->io->ops      = read_saved_ops;
   ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_TIMEDOUT));
   ASSERT_TRUE(page_handle_is_null(&page));
   cache_assert_free(cc);
   ASSERT_TRUE(SUCCESS(cache_get(cc, addr, TRUE, PAGE_TYPE_MISC, &page)));
   ASSERT_EQUAL(addr, page_handle_addr(&page));
   ASSERT_EQUAL(0, (unsigned char)page_handle_data(&page)[0]);
   cache_unget(cc, &page);
}

CTEST2(writeback_set, test_cache_async_read_setup_failures)
{
   cache *cc   = (cache *)&data->cc;
   uint64 addr = alloc_and_dirty_extent(&data->cc, &data->al);
   cache_flush(cc);
   ASSERT_TRUE(SUCCESS(cache_evict(cc, FALSE)));
   for (unsigned fail_step = 0; fail_step < 3; fail_step++) {
      read_fault_install(data->io);
      read_fault_reject_init   = fail_step == 0;
      read_fault_reject_append = fail_step == 1;
      read_fault_complete      = fail_step == 2;
      page_get_async_state_buffer state;
      bool32                      ready = FALSE;
      cache_get_async_state_init(
         &state, cc, addr, PAGE_TYPE_MISC, read_ready, &ready);
      ASSERT_EQUAL(ASYNC_STATUS_DONE, cache_get_async(&state));
      page_handle     page;
      platform_status rc = cache_get_async_state_result(&state, &page);
      data->io->ops      = read_saved_ops;
      ASSERT_TRUE(
         STATUS_IS_EQ(rc, fail_step == 2 ? STATUS_TIMEDOUT : STATUS_NO_MEMORY));
      ASSERT_TRUE(page_handle_is_null(&page));
      cache_assert_free(cc);
   }
   page_handle page;
   ASSERT_TRUE(SUCCESS(cache_get(cc, addr, TRUE, PAGE_TYPE_MISC, &page)));
   cache_unget(cc, &page);
}

CTEST2(writeback_set, test_cache_failed_load_wakes_waiters)
{
   cache *cc   = (cache *)&data->cc;
   uint64 addr = alloc_and_dirty_extent(&data->cc, &data->al);
   cache_flush(cc);
   ASSERT_TRUE(SUCCESS(cache_evict(cc, FALSE)));
   read_fault_install(data->io);
   page_get_async_state_buffer states[3];
   bool32                      ready[3] = {FALSE};
   for (unsigned i = 0; i < ARRAY_SIZE(states); i++) {
      cache_get_async_state_init(
         &states[i], cc, addr, PAGE_TYPE_MISC, read_ready, &ready[i]);
      ASSERT_EQUAL(ASYNC_STATUS_RUNNING, cache_get_async(&states[i]));
   }
   /* The first operation owns I/O; the others wait on its cache entry. */
   read_fault_complete = TRUE;
   read_fault_callback(read_fault_callback_arg);
   data->io->ops = read_saved_ops;
   for (unsigned i = 0; i < ARRAY_SIZE(states); i++) {
      ASSERT_TRUE(ready[i]);
      while (cache_get_async(&states[i]) != ASYNC_STATUS_DONE) {
         cache_cleanup(cc);
      }
      page_handle     page;
      platform_status rc = cache_get_async_state_result(&states[i], &page);
      if (i == 0) {
         ASSERT_TRUE(STATUS_IS_EQ(rc, STATUS_TIMEDOUT));
         ASSERT_TRUE(page_handle_is_null(&page));
      } else {
         /* Queued readers retry identity/readiness after a wakeup. */
         ASSERT_TRUE(SUCCESS(rc));
         ASSERT_EQUAL(addr, page_handle_addr(&page));
         ASSERT_EQUAL(0, (unsigned char)page_handle_data(&page)[0]);
         cache_unget(cc, &page);
      }
   }
   cache_assert_free(cc);
}

static const cache_ops *reclaim_saved_ops;
static cache_ops        reclaim_fault_ops;
static uint64           reclaim_failed_addr;
static uint64           reclaim_reads;

static platform_status
reclaim_fail_second_read(cache       *cc,
                         uint64       addr,
                         bool32       blocking,
                         page_type    type,
                         page_handle *page)
{
   if (addr == reclaim_failed_addr && ++reclaim_reads == 2) {
      *page = (page_handle){0};
      return STATUS_TIMEDOUT;
   }
   return reclaim_saved_ops->page_get(cc, addr, blocking, type, page);
}

CTEST2(writeback_set, test_partial_reclamation_requires_recovery)
{
   cache     *cc = (cache *)&data->cc;
   allocator *al = (allocator *)&data->al;
   uint64     meta_head;
   ASSERT_TRUE(SUCCESS(allocator_alloc(al, &meta_head, PAGE_TYPE_MISC)));
   mini_allocator mini;
   ASSERT_TRUE(
      SUCCESS(mini_init(&mini, cc, meta_head, 0, 1, PAGE_TYPE_MISC, NULL)));
   uint64 first_extent = 0;
   uint64 last_extent;
   do {
      ASSERT_TRUE(SUCCESS(mini_alloc_extent(&mini, 0, &last_extent, NULL)));
      if (first_extent == 0) {
         first_extent = last_extent;
      }
   } while (mini.meta_tail == meta_head);
   mini_release(&mini);
   reclaim_saved_ops          = cc->ops;
   reclaim_fault_ops          = *cc->ops;
   reclaim_fault_ops.page_get = reclaim_fail_second_read;
   reclaim_failed_addr        = mini.meta_tail;
   reclaim_reads              = 0;
   cc->ops                    = &reclaim_fault_ops;
   /* The validation pass succeeds. The release pass frees the first page's
    * extents and then fails, making a restart from the head unsafe. */
   ASSERT_EQUAL(0, mini_dec_ref(cc, meta_head, PAGE_TYPE_MISC));
   cc->ops = reclaim_saved_ops;
   ASSERT_EQUAL(2, reclaim_reads);
   ASSERT_EQUAL(AL_FREE, allocator_get_refcount(al, first_extent));
   ASSERT_EQUAL(AL_ONE_REF, allocator_get_refcount(al, last_extent));
   ASSERT_TRUE(STATUS_IS_EQ(allocator_reclamation_status(al), STATUS_TIMEDOUT));
   uint64 state_addr = UINT64_MAX;
   ASSERT_TRUE(
      STATUS_IS_EQ(allocator_persist(al, &state_addr), STATUS_TIMEDOUT));
   ASSERT_EQUAL(0, state_addr);
   cache_assert_free(cc);

   cache_flush(cc);
   ASSERT_TRUE(SUCCESS(cache_evict(cc, FALSE)));
   /* An empty reachable-root set does not visit the abandoned metadata. */
   ASSERT_TRUE(SUCCESS(allocator_recovery_begin(al)));
   allocator_recovery_finish(al);
   ASSERT_EQUAL(AL_FREE, allocator_get_refcount(al, meta_head));
   ASSERT_EQUAL(AL_FREE, allocator_get_refcount(al, last_extent));
   ASSERT_TRUE(SUCCESS(allocator_reclamation_status(al)));
}

CTEST2(writeback_set, test_failed_prefetch_leaves_demand_read_retryable)
{
   cache *cc   = (cache *)&data->cc;
   uint64 addr = alloc_and_dirty_extent(&data->cc, &data->al);
   cache_flush(cc);
   ASSERT_TRUE(SUCCESS(cache_evict(cc, FALSE)));
   for (unsigned fail_step = 0; fail_step < 3; fail_step++) {
      read_fault_install(data->io);
      read_fault_reject_init   = fail_step == 0;
      read_fault_reject_append = fail_step == 1;
      read_fault_complete      = TRUE;
      cache_prefetch(cc, addr, PAGE_TYPE_MISC);
      data->io->ops = read_saved_ops;
      cache_assert_free(cc);
      page_handle page;
      ASSERT_TRUE(SUCCESS(cache_get(cc, addr, TRUE, PAGE_TYPE_MISC, &page)));
      ASSERT_EQUAL(0, (unsigned char)page_handle_data(&page)[0]);
      cache_unget(cc, &page);
      ASSERT_TRUE(SUCCESS(cache_evict(cc, FALSE)));
   }
}

typedef struct sync_read_waiter {
   cache          *cc;
   uint64          addr;
   platform_status rc;
   page_handle     page;
} sync_read_waiter;

static void
sync_read_wait_for_load(void *arg)
{
   sync_read_waiter *waiter = arg;
   waiter->rc =
      cache_get(waiter->cc, waiter->addr, TRUE, PAGE_TYPE_MISC, &waiter->page);
   if (SUCCESS(waiter->rc)) {
      cache_unget(waiter->cc, &waiter->page);
   }
}

CTEST2(writeback_set, test_failed_load_releases_synchronous_waiter)
{
   cache *cc   = (cache *)&data->cc;
   uint64 addr = alloc_and_dirty_extent(&data->cc, &data->al);
   cache_flush(cc);
   ASSERT_TRUE(SUCCESS(cache_evict(cc, FALSE)));
   read_fault_install(data->io);
   page_get_async_state_buffer state;
   bool32                      ready = FALSE;
   cache_get_async_state_init(
      &state, cc, addr, PAGE_TYPE_MISC, read_ready, &ready);
   ASSERT_EQUAL(ASYNC_STATUS_RUNNING, cache_get_async(&state));
   /* The async owner holds this entry stable. Wait until the synchronous
    * reader owns its reference before publishing the failed completion. */
   uint32           entry   = data->cc.lookup[addr / cache_page_size(cc)];
   page_handle      loading = {.page = &data->cc.entry[entry].page};
   sync_read_waiter waiter  = {.cc = cc, .addr = addr};
   platform_thread  thread;
   ASSERT_TRUE(SUCCESS(platform_thread_create(
      &thread, FALSE, sync_read_wait_for_load, &waiter, data->hid)));
   timestamp start = platform_get_timestamp();
   while (cache_get_read_ref(cc, &loading) < 2
          && platform_timestamp_elapsed(start) < SEC_TO_NSEC(5))
   {
      platform_sleep_ns(USEC_TO_NSEC(50));
   }
   bool32 waiting      = cache_get_read_ref(cc, &loading) >= 2;
   read_fault_complete = TRUE;
   read_fault_callback(read_fault_callback_arg);
   ASSERT_TRUE(SUCCESS(platform_thread_join(&thread)));
   ASSERT_EQUAL(ASYNC_STATUS_DONE, cache_get_async(&state));
   page_handle     page;
   platform_status owner_rc = cache_get_async_state_result(&state, &page);
   data->io->ops            = read_saved_ops;
   ASSERT_TRUE(waiting);
   ASSERT_TRUE(STATUS_IS_EQ(owner_rc, STATUS_TIMEDOUT));
   ASSERT_TRUE(STATUS_IS_EQ(waiter.rc, STATUS_TIMEDOUT));
   ASSERT_TRUE(page_handle_is_null(&waiter.page));
   cache_assert_free(cc);
}
