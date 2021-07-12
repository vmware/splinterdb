// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * test_async.c --
 *
 *     This file contains interfaces for a toy per-thread ctxt manager
 *     used by tests. The ctxt manager can get new contexts, allow
 *     them to be put in ready queue, and unget the contexts. It also
 *     allows contexts to be restarted.
 */

#include "platform.h"

#include "test_async.h"

#include "poison.h"

/*
 * Callback called when an IO completes on behalf of a context used by
 * splinter_lookup_async(). This is the producer end of ready_q and
 * enqueues the ctxt into it. This is called from IO completion
 * context.
 */
static void
test_async_callback(splinter_async_ctxt *spl_ctxt)
{
   test_async_ctxt *ctxt = container_of(spl_ctxt, test_async_ctxt, ctxt);

   platform_assert(spl_ctxt->cache_ctxt.page);
   pcq_enqueue(ctxt->ready_q, ctxt);
}

/*
 * Get a new context from the avail_q, for use in a splinter_lookup_async()
 * Returns NULL if no contexts are avaiable (max_async_inflight reached).
 */
test_async_ctxt *
async_ctxt_get(test_async_lookup *async_lookup)
{
   test_async_ctxt *ctxt;
   platform_status rc;

   rc = pcq_dequeue(async_lookup->avail_q, (void **)&ctxt);
   if (!SUCCESS(rc)) {
      return NULL;
   }
   splinter_async_ctxt_init(&ctxt->ctxt, test_async_callback);

   return ctxt;
}

/*
 * Ungets a context after splinter_lookup_async() returns success. The
 * context should not be in-flight. It's returned back to avail_q.
 */
void
async_ctxt_unget(test_async_lookup *async_lookup,
                 test_async_ctxt   *ctxt)
{
   pcq_enqueue(async_lookup->avail_q, ctxt);
}

/*
 * Initialize the async ctxt manager.
 */
void
async_ctxt_init(platform_heap_id     hid,                  // IN
                uint32               max_async_inflight,   // IN
                uint64               data_size,            // IN
                test_async_lookup  **out)                  // OUT
{
   char *data;
   test_async_lookup *async_lookup;

   // max_async_inflight can be zero
   platform_assert(max_async_inflight <= TEST_MAX_ASYNC_INFLIGHT);
   async_lookup = TYPED_FLEXIBLE_STRUCT_MALLOC(hid, async_lookup,
                                               ctxt, max_async_inflight);
   platform_assert(async_lookup);
   async_lookup->max_async_inflight = max_async_inflight;
   async_lookup->avail_q = pcq_alloc(hid, max_async_inflight);
   platform_assert(async_lookup->avail_q);
   async_lookup->ready_q = pcq_alloc(hid, max_async_inflight);
   platform_assert(async_lookup->ready_q);
   if (max_async_inflight > 0) {
      data = TYPED_ARRAY_MALLOC(hid, data, max_async_inflight * data_size);
      platform_assert(data);
   }
   for (uint64 i=0; i<max_async_inflight; i++) {
      async_lookup->ctxt[i].data = data;
      data += data_size;
      async_lookup->ctxt[i].ready_q = async_lookup->ready_q;
      // All ctxts start out as available
      pcq_enqueue(async_lookup->avail_q, &async_lookup->ctxt[i]);
   }
   *out = async_lookup;
}

/*
 * Deinitialize the async ctxt manager.
 */
void
async_ctxt_deinit(platform_heap_id   hid,
                  test_async_lookup *async_lookup)
{
   platform_assert(pcq_is_full(async_lookup->avail_q));
   pcq_free(hid, async_lookup->avail_q);
   platform_assert(pcq_is_empty(async_lookup->ready_q));
   pcq_free(hid, async_lookup->ready_q);
   if (async_lookup->max_async_inflight > 0) {
      platform_free(hid, async_lookup->ctxt[0].data);
   }
   platform_free(hid, async_lookup);
}


/*
 * Process a single async ctxt by first doing an async lookup
 * and if successful, run process_cb on it.
 */
void
async_ctxt_process_one(splinter_handle       *spl,
                       test_async_lookup     *async_lookup,
                       test_async_ctxt       *ctxt,
                       timestamp             *latency_max,
                       async_ctxt_process_cb  process_cb,
                       void                  *process_arg)
{
   bool found;
   cache_async_result res;
   timestamp ts;

   ts = platform_get_timestamp();
   res = splinter_lookup_async(spl, ctxt->key, ctxt->data, &found,
                               &ctxt->ctxt);
   ts = platform_timestamp_elapsed(ts);
   if (latency_max != NULL && *latency_max < ts) {
      *latency_max = ts;
   }

   switch (res) {
   case async_locked:
   case async_no_reqs:
      pcq_enqueue(async_lookup->ready_q, ctxt);
      break;
   case async_io_started:
      break;
   case async_success:
      process_cb(spl, ctxt, found, process_arg);
      async_ctxt_unget(async_lookup, ctxt);
      break;
   default:
      platform_assert(0);
   }
}

/*
 * Process all async ctxts on the ready queue. This is the
 * consumer end of the ready queue.
 *
 * Returns: TRUE if no context at all are used.
 */
bool
async_ctxt_process_ready(splinter_handle       *spl,
                         test_async_lookup     *async_lookup,
                         timestamp             *latency_max,
                         async_ctxt_process_cb  process_cb,
                         void                  *process_arg)
{
   uint32 count = pcq_count(async_lookup->avail_q);

   if (count == async_lookup->max_async_inflight) {
      return FALSE;
   }
   count = pcq_count(async_lookup->ready_q);
   while (count-- > 0) {
      test_async_ctxt *ctxt;
      platform_status  rc;

      rc = pcq_dequeue(async_lookup->ready_q, (void **)&ctxt);
      if (!SUCCESS(rc)) {
         // Something is ready, just can't be dequeued yet.
         break;
      }
      async_ctxt_process_one(spl, async_lookup, ctxt, latency_max,
                             process_cb, process_arg);
   }

   return TRUE;
}
