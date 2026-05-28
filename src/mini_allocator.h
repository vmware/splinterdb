// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * mini_allocator.h --
 *
 *     This file contains the abstract interface for an allocator which
 *     allocates individual pages from extents.
 *
 *     The purpose of the mini allocator is to allocate pages from extents
 *     and to maintain a list of allocated extents for future bulk operations,
 *     such as reference counting and deallocation. A single mini allocator can
 *     manage batches with different page types; the metadata records each
 *     extent's page type so bulk operations can deallocate mixed trees/blobs.
 */

#pragma once

#include "allocator.h"
#include "cache.h"

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
   allocator      *al;
   cache          *cc;
   bool32          pinned;
   uint64          meta_head;
   volatile uint64 meta_tail;
   page_type       meta_type;
   page_type       types[MINI_MAX_BATCHES];

   uint64          num_extents;
   uint64          num_batches;
   volatile uint64 next_addr[MINI_MAX_BATCHES];
   uint64          saved_next_addr[MINI_MAX_BATCHES];
   uint64          next_extent[MINI_MAX_BATCHES];
} mini_allocator;

uint64
mini_init(mini_allocator *mini,
          cache          *cc,
          uint64          meta_head,
          uint64          meta_tail,
          uint64          num_batches,
          page_type       type);

uint64
mini_init_with_types(mini_allocator  *mini,
                     cache           *cc,
                     uint64           meta_head,
                     uint64           meta_tail,
                     uint64           num_batches,
                     page_type        meta_type,
                     const page_type *types);
void
mini_release(mini_allocator *mini);

uint64
mini_alloc(mini_allocator *mini, uint64 batch, uint64 *next_extent);

platform_status
mini_alloc_bytes(mini_allocator *mini,
                 uint64          batch,
                 uint64          num_bytes,
                 uint64          alignment,
                 uint64          boundary,
                 uint64          addrs[2],
                 uint64         *next_extent);

void
mini_alloc_bytes_finish(mini_allocator *mini, uint64 batch);

uint64
mini_alloc_page(mini_allocator *mini, uint64 batch, uint64 *next_extent);

uint64
mini_alloc_extent(mini_allocator *mini, uint64 batch, uint64 *next_extent);

platform_status
mini_attach_extent(mini_allocator *mini, uint64 batch, uint64 addr);

uint64
mini_next_addr(mini_allocator *mini, uint64 batch);

refcount
mini_inc_ref(cache *cc, uint64 meta_head);
refcount
mini_dec_ref(cache *cc, uint64 meta_head, page_type type);

void
mini_block_dec_ref(cache *cc, uint64 meta_head);

void
mini_unblock_dec_ref(cache *cc, uint64 meta_head);

void
mini_prefetch(cache *cc, page_type type, uint64 meta_head);

/* Return total bytes allocated by the mini_allocator, including space used by
 * the mini_allocator itself.*/
uint64
mini_space_use_bytes(cache *cc, uint64 meta_head, page_type type);

void
mini_print(cache *cc, uint64 meta_head, page_type type);

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
