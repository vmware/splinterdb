// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * histogram.h --
 *
 *     This file contains the interface for histogram data collection.
 */


#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_heap.h"
#include "platform_assert.h"
#include "platform_status.h"

typedef struct histogram {
   unsigned int  num_buckets;
   const long   *bucket_limits;
   long          min, max, total;
   unsigned long num; // no. of elements
   unsigned long count[];
} *histogram_handle;

platform_status
histogram_create(platform_heap_id   heap_id,
                 uint32             num_buckets,
                 const int64 *const bucket_limits,
                 histogram_handle  *histo);

void
histogram_destroy(platform_heap_id heap_id, histogram_handle *histo);

void
histogram_print(histogram_handle     histo,
                const char          *name,
                platform_log_handle *log_handle);

static inline void
histogram_insert(histogram_handle histo, int64 datum)
{
   int lo = 0, hi = histo->num_buckets - 1;

   while (hi > lo) {
      int mid = lo + (hi - lo) / 2;

      if (datum > histo->bucket_limits[mid]) {
         lo = mid + 1;
      } else {
         hi = mid - 1;
      }
   }
   platform_assert(lo < histo->num_buckets);
   histo->count[lo]++;
   if (histo->num == 0) {
      histo->min = histo->max = datum;
   } else {
      histo->max = MAX(histo->max, datum);
      histo->min = MIN(histo->min, datum);
   }
   histo->total += datum;
   histo->num++;
}

static inline void
histogram_merge_in(histogram_handle dest_histo, histogram_handle src_histo)
{
   uint32 i;
   if (src_histo->num == 0) {
      return;
   }

   platform_assert(dest_histo->num_buckets == src_histo->num_buckets);
   for (i = 0; i < dest_histo->num_buckets - 1; i++) {
      platform_assert(dest_histo->bucket_limits[i]
                      == src_histo->bucket_limits[i]);
   }
   if (src_histo->min < dest_histo->min || dest_histo->num == 0) {
      dest_histo->min = src_histo->min;
   }
   if (src_histo->max > dest_histo->max || dest_histo->num == 0) {
      dest_histo->max = src_histo->max;
   }
   dest_histo->total += src_histo->total;
   dest_histo->num += src_histo->num;

   for (i = 0; i < dest_histo->num_buckets; i++) {
      dest_histo->count[i] += src_histo->count[i];
   }
}
