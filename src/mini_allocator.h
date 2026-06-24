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
   // For each batch, the meta page that holds the entry for the extent the
   // batch is currently allocating from. Lets a caller (e.g. the btree) record,
   // in each page it allocates, where that page's extent is listed in the meta
   // stream, so a prefetch cursor can start there without scanning from
   // meta_head. See mini_current_extent_meta_page().
   uint64          cur_extent_meta_page[MINI_MAX_BATCHES];
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

/*
 * mini_meta_cursor: a forward cursor over the extent entries of a finalized
 * mini_allocator, in allocation order. Entries from all batches are
 * interleaved in the stream; the caller filters by batch as needed (each entry
 * reports its batch). The btree iterator uses this to read extent addresses
 * ahead of itself for prefetching.
 *
 * The cursor holds a read reference on the meta page it is currently reading;
 * call mini_meta_cursor_deinit() to release it. The cursor reads meta pages
 * with blocking cache_get(); meta pages are tiny and become hot quickly, so a
 * miss is rare, but callers on async paths should be aware it can block.
 */
typedef struct mini_meta_cursor {
   cache       *cc;
   page_type    meta_type;
   page_handle *meta_page;   // currently held meta page, or NULL
   uint64       meta_addr;   // addr of meta_page, or the next page to load
   uint64       entry_idx;   // index of the next entry to read on meta_page
   uint64       num_entries; // number of entries on meta_page
} mini_meta_cursor;

void
mini_meta_cursor_init(mini_meta_cursor *cursor,
                      cache            *cc,
                      page_type         meta_type,
                      uint64            meta_addr);

void
mini_meta_cursor_deinit(mini_meta_cursor *cursor);

// Emit the next extent entry (its extent address and originating batch) in
// allocation order. Returns FALSE once the stream is exhausted.
bool32
mini_meta_cursor_next(mini_meta_cursor *cursor,
                      uint64           *extent_addr,
                      uint64           *batch);

// Advance the cursor until it emits the entry for target_extent_addr, leaving
// the cursor positioned just after it. Returns FALSE if not found before the
// stream ends.
bool32
mini_meta_cursor_seek_extent(mini_meta_cursor *cursor,
                             uint64            target_extent_addr);

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

/*
 * Address of the meta page holding the extent entry for the extent that batch
 * is currently allocating from. Valid immediately after an allocation from
 * batch (e.g. mini_alloc_page), for the thread that performed it.
 */
static inline uint64
mini_current_extent_meta_page(mini_allocator *mini, uint64 batch)
{
   return mini->cur_extent_meta_page[batch];
}


static inline uint64
mini_num_extents(mini_allocator *mini)
{
   return mini->num_extents;
}
