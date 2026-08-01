// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * writeback_set.c --
 *
 *     Implementation of the writeback set. See writeback_set.h.
 */

#include "platform.h"
#include "writeback_set.h"
#include "poison.h"

void
writeback_set_init(writeback_set *set, cache *cc, platform_heap_id hid)
{
   set->cc = cc;
   vector_init(&set->requests, hid);
}

void
writeback_set_deinit(writeback_set *set)
{
   vector_deinit(&set->requests);
   set->cc = NULL;
}

uint64
writeback_set_num_requests(const writeback_set *set)
{
   return vector_length(&set->requests);
}

/*
 * Reserve room for one more member before issuing its write.
 *
 * The order matters: if we issued first and then failed to grow the vector, the
 * write would be in flight with nothing recording it, so writeback_set_wait()
 * would return without covering it and the caller would believe a page was
 * durable when it had not even been waited for. Growing first means the
 * subsequent append cannot fail.
 */
static platform_status
writeback_set_reserve_one(writeback_set *set)
{
   return vector_ensure_capacity(&set->requests,
                                 vector_length(&set->requests) + 1);
}

platform_status
writeback_set_add_page(writeback_set *set, page_handle *page, page_type type)
{
   platform_status rc = writeback_set_reserve_one(set);
   if (!SUCCESS(rc)) {
      return rc;
   }

   cache_writeback_request req;
   rc = cache_writeback_page(set->cc, page, type, &req);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = vector_append(&set->requests, req);
   platform_assert_status_ok(rc); // reserved above
   return STATUS_OK;
}

platform_status
writeback_set_add_extent(writeback_set *set, uint64 addr, page_type type)
{
   platform_status rc = writeback_set_reserve_one(set);
   if (!SUCCESS(rc)) {
      return rc;
   }

   cache_writeback_request req;
   rc = cache_writeback_extent(set->cc, addr, type, &req);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = vector_append(&set->requests, req);
   platform_assert_status_ok(rc); // reserved above
   return STATUS_OK;
}

platform_status
writeback_set_wait(writeback_set *set)
{
   platform_status result = STATUS_OK;

   for (uint64 i = 0; i < vector_length(&set->requests); i++) {
      const cache_writeback_request *req = vector_get_ptr(&set->requests, i);

      while (TRUE) {
         cache_writeback_status status =
            cache_writeback_get_status(set->cc, req);

         if (status == CACHE_WRITEBACK_PENDING) {
            /*
             * cache_cleanup() reaps completions on this thread, which is what
             * makes this loop progress rather than spin waiting for another
             * thread to do it.
             */
            cache_cleanup(set->cc);
            continue;
         }

         if (status == CACHE_WRITEBACK_FAILED) {
            /*
             * Record it but keep going, so that when we return no write
             * belonging to this set is still outstanding and the caller can
             * safely act on the failure.
             */
            platform_error_log("writeback_set_wait: writeback of addr %lu "
                               "failed\n",
                               req->addr);
            result = STATUS_IO_ERROR;
         } else if (status == CACHE_WRITEBACK_REDIRTIED) {
            /*
             * The contents we asked to be written did reach the device, so the
             * request is satisfied -- but somebody dirtied the page again while
             * we were writing it, which for a caller that owns these pages is a
             * bug in its own locking rather than something the cache did.
             */
            debug_assert(FALSE,
                         "page %lu was re-dirtied during a writeback set",
                         req->addr);
         }
         break;
      }
   }

   return result;
}

platform_status
writeback_set_make_durable(writeback_set *set)
{
   return cache_durable_barrier(set->cc);
}
