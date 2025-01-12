// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * routing_filter.h --
 *
 *     This file contains the routing_filter interface.
 */

#pragma once

#include "cache.h"
#include "iterator.h"
#include "splinterdb/data.h"
#include "util.h"
#include "platform.h"

/*
 * In Splinter, there is a strict max number of compacted tuples in a node, so
 * we used routing filters designed for that many keys. This allows us to not
 * worry about how many bits to store etc. MAX_FILTERS is an artificial limit
 * on the values stored in the filters. There is a hard limit at 64 because
 * lookups return a 64 bit bit-vector.
 */
#define MAX_FILTERS       32
#define ROUTING_NOT_FOUND (UINT16_MAX)


/*
 * Routing Filters Configuration structure - used to setup routing filters.
 */
typedef struct routing_config {
   cache_config *cache_cfg;
   data_config  *data_cfg;
   uint32        fingerprint_size;
   uint32        index_size;
   uint32        log_index_size;

   hash_fn      hash;
   unsigned int seed;
} routing_config;

/*
 * -----------------------------------------------------------------------------
 * Routing Filter: Disk-resident structure, on pages of type PAGE_TYPE_TRUNK.
 * Stored in trunk nodes, and is a pointer to a routing filter.
 * -----------------------------------------------------------------------------
 */
typedef struct ONDISK routing_filter {
   uint64 addr;
   uint64 meta_head;
   uint32 num_fingerprints;
   uint32 num_unique;
   uint32 value_size;
} routing_filter;

#define NULL_ROUTING_FILTER ((routing_filter){0})

typedef struct ONDISK routing_hdr routing_hdr;

platform_status
routing_filter_add(cache                *cc,
                   const routing_config *cfg,
                   routing_filter       *old_filter,
                   routing_filter       *filter,
                   uint32               *new_fp_arr,
                   uint64                num_new_fingerprints,
                   uint16                value);

platform_status
routing_filter_lookup(cache                *cc,
                      const routing_config *cfg,
                      routing_filter       *filter,
                      key                   target,
                      uint64               *found_values);

static inline uint16
routing_filter_get_next_value(uint64 found_values, uint16 last_value)
{
   if (last_value != ROUTING_NOT_FOUND) {
      uint64 mask = (1 << last_value) - 1;
      found_values &= mask;
   }
   if (found_values == 0) {
      return ROUTING_NOT_FOUND;
   }
   return 63 - __builtin_clzll(found_values);
}

static inline bool32
routing_filter_is_value_found(uint64 found_values, uint16 value)
{
   return ((found_values & (1 << value)) != 0);
}


static inline bool32
routing_filters_equal(const routing_filter *f1, const routing_filter *f2)
{
   return (f1->addr == f2->addr);
}

// clang-format off
DEFINE_ASYNC_STATE(routing_filter_lookup_async_state, 2,
   param, cache *,                      cc,
   param, const routing_config *,       cfg,
   param, routing_filter,               filter,
   param, key,                          target,
   param, uint64 *,                     found_values,
   param, async_callback_fn,            callback,
   param, void *,                       callback_arg,
   local, platform_status,              __async_result,
   local, uint32,                       fp,
   local, uint32,                       remainder_size,
   local, uint32,                       bucket,
   local, uint32,                       index,
   local, routing_hdr *,                hdr,
   local, page_handle *,                filter_page,
   local, uint64,                       page_size,
   local, uint64,                       addrs_per_page,
   local, uint64,                       index_addr,
   local, uint64,                       hdr_raw_addr,
   local, uint64,                       header_addr,
   local, page_handle *,                index_page,
   local, page_get_async_state_buffer, cache_get_state)
// clang-format on

async_status
routing_filter_lookup_async(routing_filter_lookup_async_state *state);

void
routing_filter_dec_ref(cache *cc, routing_filter *filter);

void
routing_filter_inc_ref(cache *cc, routing_filter *filter);

uint32
routing_filter_estimate_unique_keys_from_count(const routing_config *cfg,
                                               uint64 num_unique);

uint32
routing_filter_estimate_unique_keys(routing_filter *filter,
                                    routing_config *cfg);

uint32
routing_filter_estimate_unique_fp(cache                *cc,
                                  const routing_config *cfg,
                                  platform_heap_id      hid,
                                  routing_filter       *filter,
                                  uint64                num_filters);

// Debug functions

void
routing_filter_verify(cache          *cc,
                      routing_config *cfg,
                      routing_filter *filter,
                      uint16          value,
                      iterator       *itor);

void
routing_filter_print(cache *cc, routing_config *cfg, routing_filter *filter);
