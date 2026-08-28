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
   vector_init(&set->entries, hid);
}

void
writeback_set_deinit(writeback_set *set)
{
   vector_deinit(&set->entries);
   set->cc = NULL;
}

void
writeback_set_reset(writeback_set *set)
{
   vector_truncate(&set->entries, 0);
}

uint64
writeback_set_num_requests(const writeback_set *set)
{
   return vector_length(&set->entries);
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
   return vector_ensure_capacity(&set->entries,
                                 vector_length(&set->entries) + 1);
}

platform_status
writeback_set_add_page(writeback_set *set, page_handle *page, page_type type)
{
   platform_status rc = writeback_set_reserve_one(set);
   if (!SUCCESS(rc)) {
      return rc;
   }

   writeback_set_entry entry = {.type = type};
   rc = cache_writeback_page(set->cc, page, type, &entry.request);
   entry.needs_retry = !SUCCESS(rc);

   rc = vector_append(&set->entries, entry);
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

   writeback_set_entry entry = {.type = type};
   rc = cache_writeback_extent(set->cc, addr, type, &entry.request);
   entry.needs_retry = !SUCCESS(rc);

   rc = vector_append(&set->entries, entry);
   platform_assert_status_ok(rc); // reserved above
   return STATUS_OK;
}

platform_status
writeback_set_wait(writeback_set *set)
{
   platform_status result = STATUS_OK;

   for (uint64 i = 0; i < vector_length(&set->entries); i++) {
      writeback_set_entry           *entry = vector_get_ptr(&set->entries, i);
      const cache_writeback_request *req   = &entry->request;

      if (entry->needs_retry && SUCCESS(result)) {
         /* Drain anything partially issued, but do not report the set ready. */
         result = STATUS_BUSY;
      }

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
            entry->needs_retry = TRUE;
            result             = STATUS_IO_ERROR;
         } else if (status == CACHE_WRITEBACK_REDIRTIED) {
            /*
             * The contents we asked to be written did reach the device, so the
             * request is satisfied -- but somebody dirtied the page again while
             * we were writing it, which for a caller that owns these pages is a
             * bug in its own locking rather than something the cache did.
             */
            platform_error_log("writeback_set_wait: addr %lu was re-dirtied "
                               "during writeback\n",
                               req->addr);
            entry->needs_retry = TRUE;
            if (SUCCESS(result)) {
               result = STATUS_BUSY;
            }
         }
         break;
      }
   }

   return result;
}

platform_status
writeback_set_retry_incomplete(writeback_set *set)
{
   platform_status result = STATUS_OK;

   for (uint64 i = 0; i < vector_length(&set->entries); i++) {
      writeback_set_entry   *entry = vector_get_ptr(&set->entries, i);
      cache_writeback_status status =
         cache_writeback_get_status(set->cc, &entry->request);
      if (!entry->needs_retry && status != CACHE_WRITEBACK_FAILED
          && status != CACHE_WRITEBACK_REDIRTIED)
      {
         continue;
      }

      cache_writeback_request retry_request = entry->request;
      platform_status         rc;
      if (entry->request.is_extent) {
         rc = cache_writeback_extent(
            set->cc, entry->request.addr, entry->type, &retry_request);
      } else {
         page_handle *page =
            cache_get(set->cc, entry->request.addr, TRUE, entry->type);
         if (page == NULL) {
            rc = STATUS_IO_ERROR;
         } else {
            rc =
               cache_writeback_page(set->cc, page, entry->type, &retry_request);
            cache_unget(set->cc, page);
         }
      }
      /*
       * Even a failed extent retry may have issued a subset of its pages.  The
       * new receipt is therefore the one wait() must drain; needs_retry keeps
       * the missing subset from being forgotten on the next attempt.
       */
      entry->request     = retry_request;
      entry->needs_retry = !SUCCESS(rc);
      if (!SUCCESS(rc)) {
         platform_error_log(
            "writeback_set_retry_incomplete: retry of addr %lu failed: %s\n",
            entry->request.addr,
            platform_status_to_string(rc));
         result = rc;
      }
   }

   return result;
}

platform_status
writeback_set_make_durable(writeback_set *set)
{
   return cache_durable_barrier(set->cc);
}
