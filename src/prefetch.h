// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * prefetch.h --
 *
 *     Shared helpers for read-ahead policy.
 */

#pragma once

#include "platform_assert.h"

/*
 * Minimum deep-prefetch depth for one eligible stream. Keeping at least this
 * many extents in flight is what makes deep prefetch worthwhile compared to the
 * legacy single-extent-ahead path.
 */
#define PREFETCH_MIN_EXTENT_LOOKAHEAD (2)

/*
 * Convert a soft byte budget into a per-stream extent lookahead. The budget is
 * divided across the streams, but each active stream gets at least
 * PREFETCH_MIN_EXTENT_LOOKAHEAD extents. With many streams, that minimum can
 * intentionally exceed the byte budget; the budget is a read-ahead target, not
 * a hard cap.
 */
static inline uint32
prefetch_budget_to_extent_lookahead(uint64 extent_size,
                                    uint64 prefetch_budget,
                                    uint64 num_streams)
{
   platform_assert(extent_size != 0);

   if (prefetch_budget == 0 || num_streams == 0) {
      return 0;
   }

   uint64 budget_extents = prefetch_budget / extent_size;
   uint64 per_stream     = budget_extents / num_streams;
   if (per_stream < PREFETCH_MIN_EXTENT_LOOKAHEAD) {
      per_stream = PREFETCH_MIN_EXTENT_LOOKAHEAD;
   }
   return (uint32)per_stream;
}
