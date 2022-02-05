// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * mini_allocator.h --
 *
 *     This file contains the abstract interface for an allocator which
 *     allocates individual pages from extents.
 */

#ifndef __MINI_ALLOCATOR_H
#define __MINI_ALLOCATOR_H

#include "platform.h"
#include "allocator.h"
#include "cache.h"
#include "data_internal.h"

/*
 * Mini-allocator breaks extents into pages, so they feed out (allocates) pages
 * to consumers. Batch ensures that it feeds it out of contiguous sets of pages,
 * to facilitate packing back to extents / contiguous pages. This batch-size
 * is somewhat of an artificial limit to manage this contiguity.
 */
#define MINI_MAX_BATCHES 8

extern const char * const page_type_str[];

/*
 * mini_allocator: Mini-allocator context structure. To allocate individual
 *  pages from an allocated extent.
 */
typedef struct mini_allocator {
   allocator      *al;
   cache          *cc;
   data_config    *data_cfg;
   bool            keyed;
   bool            pinned;
   uint64          meta_head;
   volatile uint64 meta_tail;
   page_type       type;

   uint64          num_batches;
   volatile uint64 next_addr[MINI_MAX_BATCHES];
   uint64          next_extent[MINI_MAX_BATCHES];
} mini_allocator;

// Provide two pass-thru caller interface for readability
#define mini_init_unkeyed(mini, cc, meta_head, meta_tail, num_batches, type)   \
   mini_init((mini),                                                           \
             (cc),                                                             \
             (data_config *)NULL,                                              \
             (meta_head),                                                      \
             (meta_tail),                                                      \
             (num_batches),                                                    \
             (type),                                                           \
             FALSE)

#define mini_init_keyed(                                                       \
   mini, cc, data_cfg, meta_head, meta_tail, num_batches, type)                \
   mini_init((mini),                                                           \
             (cc),                                                             \
             (data_cfg),                                                       \
             (meta_head),                                                      \
             (meta_tail),                                                      \
             (num_batches),                                                    \
             (type),                                                           \
             TRUE)

uint64
mini_init(mini_allocator *mini,
          cache          *cc,
          data_config    *cfg,
          uint64          meta_head,
          uint64          meta_tail,
          uint64          num_batches,
          page_type       type,
          bool            keyed);
void
mini_release(mini_allocator *mini, const slice key);

uint64
mini_alloc(mini_allocator *mini,
           uint64          batch,
           const slice     key,
           uint64         *next_extent);


uint8
mini_unkeyed_inc_ref(cache *cc, uint64 meta_head);
uint8
mini_unkeyed_dec_ref(cache *cc, uint64 meta_head, page_type type, bool pinned);

void
mini_keyed_inc_ref(cache       *cc,
                   data_config *data_cfg,
                   page_type    type,
                   uint64       meta_head,
                   const slice  start_key,
                   const slice  end_key);
bool
mini_keyed_dec_ref(cache       *cc,
                   data_config *data_cfg,
                   page_type    type,
                   uint64       meta_head,
                   const slice  start_key,
                   const slice  end_key);

void
mini_block_dec_ref(cache *cc, uint64 meta_head);

void
mini_unblock_dec_ref(cache *cc, uint64 meta_head);

uint64
mini_keyed_extent_count(cache       *cc,
                        data_config *data_cfg,
                        page_type    type,
                        uint64       meta_head,
                        const slice  start_key,
                        const slice  end_key);
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

#endif // __MINI_ALLOCATOR_H
