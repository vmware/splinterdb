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
#include "data.h"

#define MINI_MAX_BATCHES 8

typedef struct mini_allocator {
   allocator *     al;
   cache *         cc;
   data_config *   data_cfg;
   uint64          meta_head;
   volatile uint64 meta_tail;
   uint64          num_batches;
   volatile uint64 next_addr[MINI_MAX_BATCHES];
   uint64          next_extent[MINI_MAX_BATCHES];
   uint64          last_meta_addr[MINI_MAX_BATCHES];
   uint64          last_meta_pos[MINI_MAX_BATCHES];
   page_type       type;
} mini_allocator;

typedef struct mini_allocator_sync_arg {
   uint64 *pages_outstanding;
   uint64  page_size;
} mini_allocator_sync_arg;

uint64
mini_allocator_init(mini_allocator *mini,
                    cache          *cc,
                    data_config    *cfg,
                    uint64          meta_head,
                    uint64          meta_tail,
                    uint64          num_batches,
                    page_type       type);

uint64
mini_allocator_alloc(mini_allocator *mini,
                     uint64          batch,
                     char           *key,
                     uint64         *next_extent);

void
mini_allocator_release(mini_allocator *mini,
                       char           *key);

bool
mini_allocator_zap(cache       *cc,
                   data_config *data_cfg,
                   uint64       meta_head,
                   const char  *start_key,
                   const char  *end_key,
                   page_type    type);

void
mini_allocator_sync(cache     *cc,
                    page_type  type,
                    uint64     meta_head,
                    uint64    *pages_outstanding);

void
mini_allocator_inc_range(cache       *cc,
                         data_config *data_cfg,
                         page_type    type,
                         uint64       meta_head,
                         const char  *start_key,
                         const char  *end_key);

page_handle *
mini_allocator_blind_inc(cache *cc,
                         uint64 meta_head);

void
mini_allocator_blind_zap(cache       *cc,
                         page_type    type,
                         page_handle *meta_page);

uint64
mini_allocator_extent_count(cache     *cc,
                            page_type  type,
                            uint64     meta_head);

uint64
mini_allocator_count_extents_in_range(cache       *cc,
                                      data_config *data_cfg,
                                      page_type    type,
                                      uint64       meta_head,
                                      const char  *start_key,
                                      const char  *end_key);

void
mini_allocator_prefetch(cache     *cc,
                        page_type  type,
                        uint64     meta_head);

static inline uint64
mini_allocator_meta_tail(mini_allocator *mini)
{
   return mini->meta_tail;
}

#endif // __MINI_ALLOCATOR_H
