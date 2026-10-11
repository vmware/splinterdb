// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * Exercise an async trunk lookup across a real stack lifetime boundary.  The
 * mock cache forces the pivot-page get to yield; a different thread then
 * resumes the same async state.
 */

#include "trunk.h"
#include "lookup_result.h"
#include "platform_threads.h"
#include "ctest.h"

#define TRUNK_ASYNC_TEST_PAGE_SIZE   4096
#define TRUNK_ASYNC_TEST_EXTENT_SIZE (32 * TRUNK_ASYNC_TEST_PAGE_SIZE)
#define TRUNK_ASYNC_TEST_HEADER_ADDR TRUNK_ASYNC_TEST_EXTENT_SIZE

typedef struct ONDISK trunk_async_test_node {
   uint16 height;
   uint16 num_pivots;
   uint16 num_inflight_bundles;
   uint32 inflight_bundles_offset;
   uint32 pivot_offsets[];
} trunk_async_test_node;

typedef struct ONDISK trunk_async_test_pivot_stats {
   int64 num_kv_bytes;
   int64 num_tuples;
} trunk_async_test_pivot_stats;

typedef struct ONDISK trunk_async_test_pivot {
   trunk_async_test_pivot_stats stats;
   uint64                       child_addr;
   uint64                       num_live_inflight_bundles;
   ondisk_key                   key;
} trunk_async_test_pivot;

typedef struct trunk_async_test_cache {
   cache        super;
   page_handle *pivot_page;
   uint64       async_calls;
   platform_status result;
} trunk_async_test_cache;

typedef struct trunk_async_test_get_state {
   trunk_async_test_cache *cc;
   uint64                  calls;
} trunk_async_test_get_state;

_Static_assert(sizeof(trunk_async_test_get_state)
                  <= PAGE_GET_ASYNC_STATE_BUFFER_SIZE,
               "mock async state exceeds cache payload");

static uint64
trunk_async_test_page_size(const cache_config *cfg)
{
   return TRUNK_ASYNC_TEST_PAGE_SIZE;
}

static uint64
trunk_async_test_extent_size(const cache_config *cfg)
{
   return TRUNK_ASYNC_TEST_EXTENT_SIZE;
}

static const cache_config_ops trunk_async_test_config_ops = {
   .page_size   = trunk_async_test_page_size,
   .extent_size = trunk_async_test_extent_size,
};

static cache_config trunk_async_test_config = {
   .ops = &trunk_async_test_config_ops,
};

static cache_config *
trunk_async_test_get_config(const cache *cc)
{
   return &trunk_async_test_config;
}

static void
trunk_async_test_unget(cache *cc, page_handle *page)
{
}

static void
trunk_async_test_get_init(void             *payload,
                          cache            *cc,
                          uint64            addr,
                          page_type         type,
                          async_callback_fn callback,
                          void             *callback_arg)
{
   trunk_async_test_cache *test_cc = (trunk_async_test_cache *)cc;
   platform_assert(addr == test_cc->pivot_page->disk_addr);
   platform_assert(type == PAGE_TYPE_TRUNK);
   trunk_async_test_get_state *state = payload;
   state->cc                         = test_cc;
   state->calls                      = 0;
}

static async_status
trunk_async_test_get_async(void *payload)
{
   trunk_async_test_get_state *state = payload;
   state->cc->async_calls++;
   return state->calls++ == 0 ? ASYNC_STATUS_RUNNING : ASYNC_STATUS_DONE;
}

static platform_status
trunk_async_test_get_result(void *payload, page_handle *page)
{
   trunk_async_test_get_state *state = payload;
   if (SUCCESS(state->cc->result)) {
      *page = *state->cc->pivot_page;
   }
   return state->cc->result;
}

static const cache_ops trunk_async_test_cache_ops = {
   .page_get_async_state_init = trunk_async_test_get_init,
   .page_get_async            = trunk_async_test_get_async,
   .page_get_async_result     = trunk_async_test_get_result,
   .page_unget                = trunk_async_test_unget,
   .get_config                = trunk_async_test_get_config,
};

typedef struct trunk_async_test_resume {
   trunk_merge_lookup_async_state *state;
   async_status                    status;
} trunk_async_test_resume;

static void
trunk_async_test_resume_on_fresh_stack(void *arg)
{
   trunk_async_test_resume *resume = arg;

   /* Keep this call on a fresh stack so an automatic local from the suspended
    * invocation cannot appear to survive through accidental stack reuse. */
   volatile uintptr_t stack_clobber[64] = {0};
   resume->status = trunk_merge_lookup_async(resume->state);
   platform_assert(stack_clobber[0] == 0);
}

static void
trunk_async_test_pivot_lookup(bool32 fail)
{
   platform_register_thread();

   uint64 header_words[TRUNK_ASYNC_TEST_PAGE_SIZE / sizeof(uint64)] = {0};
   uint64 pivot_words[TRUNK_ASYNC_TEST_PAGE_SIZE / sizeof(uint64)]  = {0};

   page_handle header_page = {
      .data      = (char *)header_words,
      .disk_addr = TRUNK_ASYNC_TEST_HEADER_ADDR,
   };
   page_handle pivot_page = {
      .data      = (char *)pivot_words,
      .disk_addr = TRUNK_ASYNC_TEST_HEADER_ADDR + TRUNK_ASYNC_TEST_PAGE_SIZE,
   };
   trunk_async_test_cache test_cc = {
      .super      = {.ops = &trunk_async_test_cache_ops},
      .pivot_page = &pivot_page,
      .result     = fail ? STATUS_TIMEDOUT : STATUS_OK,
   };

   trunk_async_test_node *header = (trunk_async_test_node *)header_page.data;
   header->height                = 0;
   header->num_pivots            = 2;
   header->pivot_offsets[0]      = TRUNK_ASYNC_TEST_PAGE_SIZE;
   header->pivot_offsets[1]      = 2 * TRUNK_ASYNC_TEST_PAGE_SIZE;

   trunk_async_test_pivot *pivot = (trunk_async_test_pivot *)pivot_page.data;
   pivot->key.flags              = ONDISK_KEY_DEFAULT_FLAGS;
   pivot->key.length             = 0;

   trunk_ondisk_node_handle handle = {
      .cc          = &test_cc.super,
      .page_size   = TRUNK_ASYNC_TEST_PAGE_SIZE,
      .extent_size = TRUNK_ASYNC_TEST_EXTENT_SIZE,
      .header_page = header_page,
   };
   trunk_context context = {
      .cc = &test_cc.super,
   };
   lookup_result result;
   lookup_result_init(&result, NULL, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   trunk_merge_lookup_async_state state;
   trunk_merge_lookup_async_state_init(&state,
                                       &context,
                                       &handle,
                                       NEGATIVE_INFINITY_KEY,
                                       &result,
                                       NULL,
                                       NULL,
                                       NULL);

   ASSERT_EQUAL(ASYNC_STATUS_RUNNING, trunk_merge_lookup_async(&state));

   trunk_async_test_resume resume = {
      .state  = &state,
      .status = ASYNC_STATUS_RUNNING,
   };
   platform_thread thread;
   platform_status rc =
      platform_thread_create(&thread,
                             FALSE,
                             trunk_async_test_resume_on_fresh_stack,
                             &resume,
                             platform_get_heap_id());
   ASSERT_TRUE(SUCCESS(rc));
   rc = platform_thread_join(&thread);
   ASSERT_TRUE(SUCCESS(rc));

   ASSERT_EQUAL(ASYNC_STATUS_DONE, resume.status);
   ASSERT_TRUE(STATUS_IS_EQ(async_result(&state), test_cc.result));
   ASSERT_EQUAL(2, test_cc.async_calls);

   trunk_ondisk_node_handle_deinit(&handle);
   lookup_result_deinit(&result);
   platform_deregister_thread();
}

CTEST(trunk_async, pivot_header_survives_cross_thread_yield)
{
   trunk_async_test_pivot_lookup(FALSE);
}

CTEST(trunk_async, pivot_read_error_survives_cross_thread_yield)
{
   trunk_async_test_pivot_lookup(TRUE);
}
