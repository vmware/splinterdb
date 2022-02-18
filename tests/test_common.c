// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * test_common.c --
 *
 * Module contains functions shared between functional/ and unit/ test sources.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/platform_public.h"
#include "trunk.h"
#include "functional/test.h"
#include "functional/test_async.h"
#include "test_common.h"

// Function prototypes


/*
 * Tuple verification routine.
 */
void
verify_tuple(trunk_handle *spl,
             uint64        lookup_num,
             char         *key,
             slice         data,
             uint64        data_size,
             bool          expected_found)
{
   if (slice_is_null(data) != !expected_found) {
      char key_str[128];
      trunk_key_to_string(spl, key, key_str);
      platform_handle_log(stderr,
                          "(%2lu) key %lu (%s): found %d (expected:%d)\n",
                          platform_get_tid(),
                          lookup_num,
                          key_str,
                          !slice_is_null(data),
                          expected_found);
      trunk_print_lookup(spl, key);
      platform_assert(FALSE);
   }

   if (!slice_is_null(data) && expected_found) {
      char expected_data[MAX_MESSAGE_SIZE];
      char data_str[128];
      test_insert_data((data_handle *)expected_data,
                       1,
                       (char *)&lookup_num,
                       sizeof(lookup_num),
                       data_size,
                       MESSAGE_TYPE_INSERT);
      if (slice_length(data) != data_size
          || memcmp(expected_data, slice_data(data), data_size) != 0)
      {
         trunk_message_to_string(spl, data, data_str);
         platform_handle_log(stderr, "key found with data: %s\n", data_str);
         platform_assert(FALSE);
      }
   }
}

/*
 * Wait-for in-flight lookup to complete
 */
void
test_wait_for_inflight(trunk_handle      *spl,
                       test_async_lookup *async_lookup,
                       verify_tuple_arg  *vtarg)
{
   const timestamp ts          = platform_get_timestamp();
   uint64         *latency_max = NULL;
   if (vtarg->stats != NULL) {
      latency_max = &vtarg->stats->latency_max;
   }

   // Rough detection of stuck contexts
   while (async_ctxt_process_ready(
      spl, async_lookup, latency_max, verify_tuple_callback, vtarg))
   {
      cache_cleanup(spl->cc);
      platform_assert(platform_timestamp_elapsed(ts) < TEST_STUCK_IO_TIMEOUT);
   }
}

/*
 * Callback function for async tuple verification.
 */
void
verify_tuple_callback(trunk_handle *spl, test_async_ctxt *ctxt, void *arg)
{
   verify_tuple_arg *vta   = arg;
   bool              found = trunk_lookup_found(&ctxt->data);

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
}

test_async_ctxt *
test_async_ctxt_get(trunk_handle      *spl,
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
