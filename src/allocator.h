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

#define AL_ONE_REF 2
#define AL_NO_REFS 1
#define AL_FREE    0

/*
 * ----------------------------------------------------------------------------
 * Different types of pages managed by SplinterDB:
 * This is currently not a Disk-resident value, but different modules that
 * access a type of a page "know" to expect a page of a specific type / format.
 *
 * Brief overview of the data structures involved in different page types:
 *
 * - PAGE_TYPE_TRUNK      : struct trunk_hdr{} + computed offsets for
 *                          pivots / branches depending on key_size
 *                          Following the trunk_hdr{}, we have an array of
 *                          [<key>, struct trunk_pivot_data{} ]
 *
 * - PAGE_TYPE_BRANCH, PAGE_TYPE_MEMTABLE
 *                        : struct btree_hdr{} + computed items for offsets.
 *
 * - PAGE_TYPE_FILTER     : Freeform, because they consist of very concise
 *                          and heavily optimized data structures
 *
 * - PAGE_TYPE_LOG        : struct shard_log_hdr{} + computed offsets
 *
 * - PAGE_TYPE_SUPERBLOCK : struct trunk_super_block{}
 * ----------------------------------------------------------------------------
 */
typedef enum page_type {
   PAGE_TYPE_INVALID = 0,
   PAGE_TYPE_FIRST   = 1,
   PAGE_TYPE_TRUNK   = PAGE_TYPE_FIRST,
   PAGE_TYPE_BRANCH,
   PAGE_TYPE_MEMTABLE,
   PAGE_TYPE_FILTER,
   PAGE_TYPE_LOG,
   PAGE_TYPE_SUPERBLOCK,
   PAGE_TYPE_MISC, // Used mainly as a testing hook, for cache access testing.
   PAGE_TYPE_LOCK_NO_DATA,
   NUM_PAGE_TYPES,
} page_type;

static const char *const page_type_str[] = {"invalid",
                                            "trunk",
                                            "branch",
                                            "memtable",
                                            "filter",
                                            "log",
                                            "superblock",
                                            "misc",
                                            "lock"};

// Ensure that the page-type lookup array is adequately sized.
_Static_assert(
   ARRAY_SIZE(page_type_str) == NUM_PAGE_TYPES,
   "Lookup array page_type_str[] is incorrectly sized for NUM_PAGE_TYPES");

typedef struct allocator allocator;

typedef platform_status (*alloc_fn)(allocator *al,
                                    uint64    *addr,
                                    page_type  type);

typedef uint8 (*dec_ref_fn)(allocator *al, uint64 addr, page_type type);
typedef uint8 (*generic_ref_fn)(allocator *al, uint64 addr);

typedef platform_status (*get_super_addr_fn)(allocator        *al,
                                             allocator_root_id spl_id,
                                             uint64           *addr);
typedef platform_status (*alloc_super_addr_fn)(allocator        *al,
                                               allocator_root_id spl_id,
                                               uint64           *addr);
typedef void (*remove_super_addr_fn)(allocator *al, allocator_root_id spl_id);
typedef uint64 (*get_size_fn)(allocator *al);

typedef void (*print_fn)(allocator *al);
typedef void (*assert_fn)(allocator *al);

/*
 * Define an abstract allocator interface, holding different allocation-related
 * function pointers.
 */
typedef struct allocator_ops {
   alloc_fn alloc;

   generic_ref_fn inc_ref;
   dec_ref_fn     dec_ref;
   generic_ref_fn get_ref;

   alloc_super_addr_fn  alloc_super_addr;
   get_super_addr_fn    get_super_addr;
   remove_super_addr_fn remove_super_addr;

   get_size_fn in_use;

   get_size_fn get_capacity;

   assert_fn assert_noleaks;

   print_fn print_stats;
   print_fn print_allocated;
} allocator_ops;

// To sub-class cache, make a cache your first field;
struct allocator {
   const allocator_ops *ops;
};

static inline platform_status
allocator_alloc(allocator *al, uint64 *addr, page_type type)
{
   return al->ops->alloc(al, addr, type);
}

static inline uint8
allocator_inc_ref(allocator *al, uint64 addr)
{
   return al->ops->inc_ref(al, addr);
}

static inline uint8
allocator_dec_ref(allocator *al, uint64 addr, page_type type)
{
   return al->ops->dec_ref(al, addr, type);
}

static inline uint8
allocator_get_ref(allocator *al, uint64 addr)
{
   return al->ops->get_ref(al, addr);
}

static inline platform_status
allocator_get_super_addr(allocator *al, allocator_root_id spl_id, uint64 *addr)
{
   return al->ops->get_super_addr(al, spl_id, addr);
}

static inline platform_status
allocator_alloc_super_addr(allocator        *al,
                           allocator_root_id spl_id,
                           uint64           *addr)
{
   return al->ops->alloc_super_addr(al, spl_id, addr);
}

static inline void
allocator_remove_super_addr(allocator *al, allocator_root_id spl_id)
{
   return al->ops->remove_super_addr(al, spl_id);
}

static inline uint64
allocator_in_use(allocator *al)
{
   return al->ops->in_use(al);
}


static inline uint64
allocator_get_capacity(allocator *al)
{
   return al->ops->get_capacity(al);
}

static inline void
allocator_assert_noleaks(allocator *al)
{
   return al->ops->assert_noleaks(al);
}

static inline void
allocator_print_stats(allocator *al)
{
   return al->ops->print_stats(al);
}

static inline void
allocator_print_allocated(allocator *al)
{
   return al->ops->print_allocated(al);
}

#endif // __ALLOCATOR_H
