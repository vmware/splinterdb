// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * mini_allocator.h --
 *
 *     This file contains the abstract interface for an allocator which
 *     allocates individual pages from extents.
 *
 *     The purpose of the mini allocator is to allocate pages from extents
 *     and to maintain a list of allocated extents for future bulk operations,
 *     such as reference counting and deallocation. Keyed mini allocators
 *     further associate a key range to each extent, so that these bulk
 *     operations can be restricted to given key ranges.
 */

#pragma once

#include "platform.h"
#include "allocator.h"
#include "cache.h"
#include "data_internal.h"

/*
 * Mini-allocator breaks extents into pages. The pages are fed out of separate
 * batches, so that pages from each batch are contiguous within extents. This
 * facilitates, for example, packing successive BTree leaves contiguously into
 * extents. This batch-size is somewhat of an artificial limit to manage this
 * contiguity.
 */
#define MINI_MAX_BATCHES 16

/*
 * mini_allocator: Mini-allocator context.
 */
typedef struct mini_allocator {
   allocator   *al;
   cache       *cc;
   data_config *data_cfg;
   bool         keyed;
   uint64       num_batches;
   page_type    meta_type;
   page_type    types[MINI_MAX_BATCHES];

   uint64          num_extents;
   uint64          meta_head;
   volatile uint64 meta_tail;
   volatile uint64 next_addr[MINI_MAX_BATCHES];
   uint64          saved_next_addr[MINI_MAX_BATCHES];
   uint64          next_extent[MINI_MAX_BATCHES];
} mini_allocator;

platform_status
mini_init(mini_allocator *mini,
          cache          *cc,
          data_config    *cfg,
          uint64          meta_head,
          uint64          meta_tail,
          uint64          num_batches,
          page_type       meta_type,
          const page_type types[], // num_batches
          bool            keyed);
void
mini_release(mini_allocator *mini, key end_key);

/*
 * NOTE: Can only be called on a mini_allocator which has made no allocations.
 */
void
mini_destroy_unused(mini_allocator *mini);

/*
 *-----------------------------------------------------------------------------
 * mini_alloc_bytes --
 *
 *      Allocate num_bytes (<= extent_size) in at most two contiguous
 *      chunks at disk addresses of the given alignment.
 *      If boundary >= num_bytes, then the allocation
 *      will be in one chunk that does not cross an address that is a
 *      multiple of boundary.  Note that you must have
 *         - alignment divides extent_size
 *      and if num_bytes <= boundary, then you must have
 *         - alignment divides boundary
 *         - boundary divides extent_size
 *
 *      Example use of boundary: When allocating less than a
 *      page-worth of bytes and you don't care about their alignment
 *      but you do care that they are all allocated on a single page.
 *
 *      If the allocator is keyed, then the extent(s) from which the allocation
 *      is made will include the given key. NOTE: This requires keys provided be
 *      monotonically increasing.
 *
 *      The starting addresses of the chunks are returned in addrs[].
 *      If the bytes are allocated in 1 chunk, then addrs[1] == 0.
 *
 * Results:
 *      platform_status indicating success or error.
 *
 * Side effects:
 *      - Disk allocation
 *      - Locks the batch (see mini_allocator_finish)
 *      - Standard cache side effects
 *-----------------------------------------------------------------------------
 */
platform_status
mini_alloc_bytes(mini_allocator *mini,
                 uint64          batch,
                 uint64          num_bytes,
                 uint64          alignment,
                 uint64          boundary,
                 key             alloc_key,
                 uint64          addrs[2],
                 uint64         *next_extent);

/*
 * Users _must_ call this function after a successful call to
 * mini_alloc_bytes.  Until this function is called, no more
 * allocations can occur on the given batch.
 *
 * The purpose is to enable mutliple threads to coordinate calls to
 * cache_alloc vs. cache_get on returned pages.  Specifically, when
 * threads use mini_alloc_bytes to allocate less than an entire page,
 * then multiple threads may be handed allocations on the same page.
 * In this case, exactly one of the threads must call
 * cache_alloc---all other threads must call cache_get---and the call
 * to cache_alloc must precede the calls to cache_get.
 *
 * If mini_alloc_bytes gives you a whole page, then none of this is an
 * issue, since no other thread will attempt to access the page.  In
 * this case, the user given the page can call mini_alloc_bytes_finish
 * immediately after mini_alloc_bytes, and then call cache_alloc at
 * its leisure.
 *
 * The idiomatic way to use this functionality is: whenever
 * mini_alloc_bytes gives you an allocation that includes the
 * beginning of a page but not the entire page, then you call
 * cache_alloc and then call mini_alloc_bytes_finish.  If your
 * allocation doesn't start at the beginning of a page, then you call
 * mini_alloc_bytes_finish and then call cache_get.
 */
void
mini_alloc_bytes_finish(mini_allocator *mini, uint64 batch);

uint64
mini_alloc_page(mini_allocator *mini,
                uint64          batch,
                key             alloc_key,
                uint64         *next_extent);

uint64
mini_alloc_extent(mini_allocator *mini,
                  uint64          batch,
                  key             alloc_key,
                  uint64         *next_extent);

/*
 * NOTE: If other threads perform allocations on this batch after you
 * call this function, then the next_addr returned by this function
 * may not be the next address you see allocated by _your_ thread.  This
 * function is really useful only when your thread has sole access to the mini
 * allocator (or at least to the specified batch).
 */
uint64
mini_next_addr(mini_allocator *mini, uint64 batch);

/*
 * Increment the refcount of the given extent and record it in the
 * mini_allocator as if it had been allocated by this mini_allocator.
 */
platform_status
mini_attach_extent(mini_allocator *mini,
                   uint64          batch,
                   key             alloc_key,
                   uint64          addr);

uint8
mini_unkeyed_inc_ref(cache *cc, uint64 meta_head);
uint8
mini_unkeyed_dec_ref(cache *cc, uint64 meta_head, page_type type);

void
mini_keyed_inc_ref(cache       *cc,
                   data_config *data_cfg,
                   page_type    type,
                   uint64       meta_head,
                   key          start_key,
                   key          end_key);
bool
mini_keyed_dec_ref(cache       *cc,
                   data_config *data_cfg,
                   page_type    type,
                   uint64       meta_head,
                   key          start_key,
                   key          end_key);

void
mini_block_dec_ref(cache *cc, uint64 meta_head);

void
mini_unblock_dec_ref(cache *cc, uint64 meta_head);

uint64
mini_keyed_extent_count(cache       *cc,
                        data_config *data_cfg,
                        page_type    type,
                        uint64       meta_head,
                        key          start_key,
                        key          end_key);
void
mini_unkeyed_prefetch(cache *cc, page_type type, uint64 meta_head);

void
mini_unkeyed_print(cache *cc, uint64 meta_head, page_type type);
void
mini_keyed_print(cache       *cc,
                 data_config *data_cfg,
                 uint64       meta_head,
                 page_type    type);

static inline uint64
mini_meta_tail(mini_allocator *mini)
{
   return mini->meta_tail;
}

static inline uint64
mini_num_extents(mini_allocator *mini)
{
   return mini->num_extents;
}
