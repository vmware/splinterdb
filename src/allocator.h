// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * allocator.h --
 *
 *     This file contains the abstract interface for an allocator.
 */

#ifndef __ALLOCATOR_H
#define __ALLOCATOR_H

#include "platform.h"
#include "io.h"

typedef uint64 allocator_root_id;
#define INVALID_ALLOCATOR_ROOT_ID (0)

typedef struct allocator allocator;

typedef platform_status
               (*alloc_extent_fn)    (allocator *al, uint64 *addr_arr);

typedef uint8  (*inc_refcount_fn)    (allocator *al, uint64 addr);
typedef uint8  (*dec_refcount_fn)    (allocator *al, uint64 addr);
typedef uint8  (*get_refcount_fn)    (allocator *al, uint64 addr);

typedef uint64 (*get_capacity_fn)    (allocator *al);
typedef platform_status (*get_super_addr_fn)  (allocator *al,
                                               allocator_root_id spl_id,
                                               uint64 *addr);
typedef platform_status (*alloc_super_addr_fn) (allocator *al,
                                                allocator_root_id spl_id,
                                                uint64 *addr);
typedef void (*remove_super_addr_fn) (allocator *al,
                                      allocator_root_id spl_id);
typedef uint64 (*get_size_fn)        (allocator *al);

typedef void   (*print_allocated_fn) (allocator *al);
typedef uint64 (*max_allocated_fn)   (allocator *al);
typedef uint64 (*in_use_fn)          (allocator *al);

typedef struct allocator_ops {
   alloc_extent_fn      alloc_extent;

   inc_refcount_fn      inc_refcount;
   dec_refcount_fn      dec_refcount;
   get_refcount_fn      get_refcount;

   get_capacity_fn      get_capacity;
   get_super_addr_fn    get_super_addr;
   alloc_super_addr_fn  alloc_super_addr;
   remove_super_addr_fn remove_super_addr;
   in_use_fn            in_use;
   max_allocated_fn     max_allocated;
   get_size_fn          get_extent_size;
   get_size_fn          get_page_size;

   print_allocated_fn   print_allocated;
} allocator_ops;

// To sub-class cache, make a cache your first field;
struct allocator {
   const allocator_ops *ops;
};

static inline platform_status
allocator_alloc_extent(allocator *al, uint64 *addr)
{
   return al->ops->alloc_extent(al, addr);
}

static inline uint8
allocator_inc_refcount(allocator *al, uint64 addr)
{
   return al->ops->inc_refcount(al, addr);
}

static inline uint8
allocator_dec_refcount(allocator *al, uint64 addr)
{
   return al->ops->dec_refcount(al, addr);
}

static inline uint8
allocator_get_refcount(allocator *al, uint64 addr)
{
   return al->ops->get_refcount(al, addr);
}

static inline uint64
allocator_get_capacity(allocator *al)
{
   return al->ops->get_capacity(al);
}

static inline platform_status
allocator_get_super_addr(allocator *al, allocator_root_id spl_id, uint64 *addr)
{
   return al->ops->get_super_addr(al, spl_id, addr);
}

static inline platform_status
allocator_alloc_super_addr(allocator *al, allocator_root_id spl_id, uint64 *addr)
{
   return al->ops->alloc_super_addr(al, spl_id, addr);
}

static inline uint64
allocator_in_use(allocator *al)
{
   return al->ops->in_use(al);
}

static inline uint64
allocator_max_allocated(allocator *al)
{
   return al->ops->max_allocated(al);
}

static inline uint64
allocator_extent_size(allocator *al)
{
   return al->ops->get_extent_size(al);
}

static inline uint64
allocator_page_size(allocator *al)
{
   return al->ops->get_page_size(al);
}

static inline void
allocator_print_allocated(allocator *al)
{
   return al->ops->print_allocated(al);
}

static inline void
allocator_remove_super_addr(allocator *al, allocator_root_id spl_id)
{
   return al->ops->remove_super_addr(al, spl_id);
}
#endif // __ALLOCATOR_H
