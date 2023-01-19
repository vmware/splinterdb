// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * allocator.c --
 *
 *     This file implements the abstract base behavior shared by allocators.
 */

#include "allocator.h"
#include "poison.h"

void
allocator_config_init(allocator_config *allocator_cfg,
                      io_config        *io_cfg,
                      uint64            capacity)
{
   ZERO_CONTENTS(allocator_cfg);

   allocator_cfg->io_cfg   = io_cfg;
   allocator_cfg->capacity = capacity;

   allocator_cfg->page_capacity   = capacity / io_cfg->page_size;
   allocator_cfg->extent_capacity = capacity / io_cfg->extent_size;
   uint64 log_extent_size         = 63 - __builtin_clzll(io_cfg->extent_size);
   allocator_cfg->extent_mask     = ~((1ULL << log_extent_size) - 1);
}

/*
 * Return page number for the page at 'addr', in terms of page-size.
 * This routine assume that input 'addr' is a valid page address.
 */
uint64
allocator_page_number(allocator *al, uint64 page_addr)
{
   allocator_config *allocator_cfg = allocator_get_config(al);
   debug_assert(allocator_valid_page_addr(al, page_addr));
   return ((page_addr / allocator_cfg->io_cfg->page_size));
}

/*
 * Return page offset for the page at 'addr', in terms of page-size,
 * as an index into the extent holding the page.
 * This routine assume that input 'addr' is a valid page address.
 *
 * Returns index from [0 .. ( <#-of-pages-in-an-extent> - 1) ]
 */
uint64
allocator_page_offset(allocator *al, uint64 page_addr)
{
   allocator_config *allocator_cfg = allocator_get_config(al);
   debug_assert(allocator_valid_page_addr(al, page_addr));
   uint64 npages_in_extent =
      (allocator_cfg->io_cfg->extent_size / allocator_cfg->io_cfg->page_size);
   return (allocator_page_number(al, page_addr) % npages_in_extent);
}
