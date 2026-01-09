// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * histogram.c --
 *
 *     This file contains the implementation for histogram data collection.
 */

#include "histogram.h"
#include "platform_typed_alloc.h"
#include "platform_log.h"
#include "poison.h"

platform_status
histogram_create(platform_heap_id   heap_id,
                 uint32             num_buckets,
                 const int64 *const bucket_limits,
                 histogram_handle  *histo)
{
   histogram_handle hh;
   hh = TYPED_MANUAL_MALLOC(heap_id,
                            hh,
                            sizeof(hh) // NOLINT(bugprone-sizeof-expression)
                               + num_buckets * sizeof(hh->count[0]));
   if (!hh) {
      return STATUS_NO_MEMORY;
   }
   hh->num_buckets   = num_buckets;
   hh->bucket_limits = bucket_limits;
   hh->total         = 0;
   hh->min           = INT64_MAX;
   hh->max           = INT64_MIN;
   hh->num           = 0;
   memset(hh->count, 0, hh->num_buckets * sizeof(hh->count[0]));

   *histo = hh;
   return STATUS_OK;
}

void
histogram_destroy(platform_heap_id heap_id, histogram_handle *histo_out)
{
   platform_assert(histo_out);
   histogram_handle histo = *histo_out;
   platform_free(heap_id, histo);
   *histo_out = NULL;
}

void
histogram_print(histogram_handle     histo,
                const char          *name,
                platform_log_handle *log_handle)
{
   if (histo->num == 0) {
      return;
   }

   platform_log(log_handle, "%s\n", name);
   platform_log(log_handle, "min: %ld\n", histo->min);
   platform_log(log_handle, "max: %ld\n", histo->max);
   platform_log(log_handle,
                "mean: %ld\n",
                histo->num == 0 ? 0 : histo->total / histo->num);
   platform_log(log_handle, "count: %ld\n", histo->num);
   for (uint32 i = 0; i < histo->num_buckets; i++) {
      if (i == histo->num_buckets - 1) {
         platform_log(log_handle,
                      "%-12ld  > %12ld\n",
                      histo->count[i],
                      histo->bucket_limits[i - 1]);
      } else {
         platform_log(log_handle,
                      "%-12ld <= %12ld\n",
                      histo->count[i],
                      histo->bucket_limits[i]);
      }
   }
   platform_log(log_handle, "\n");
}
