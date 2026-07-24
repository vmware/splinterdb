// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * allocator.h --
 *
 *     This file contains the abstract interface for an allocator.
 */

#pragma once

#include "platform_log.h"
#include "platform_io.h"

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
 * - PAGE_TYPE_BLOB       : raw user data (see blob.c)
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
   PAGE_TYPE_BLOB,
   PAGE_TYPE_SUPERBLOCK,
   PAGE_TYPE_MISC, // Used mainly as a testing hook, for cache access testing.
   NUM_PAGE_TYPES,
} page_type;

static const char *const page_type_str[] = {"invalid",
                                            "trunk",
                                            "branch",
                                            "memtable",
                                            "filter",
                                            "log",
                                            "blob",
                                            "superblock",
                                            "misc"};

// Ensure that the page-type lookup array is adequately sized.
_Static_assert(
   ARRAY_SIZE(page_type_str) == NUM_PAGE_TYPES,
   "Lookup array page_type_str[] is incorrectly sized for NUM_PAGE_TYPES");

/*
 * Configuration structure to set up the Allocation sub-system.
 */
typedef struct allocator_config {
   io_config *io_cfg;
   uint64     capacity;

   // computed
   uint64 page_capacity;
   uint64 extent_capacity;
   uint64 extent_mask;
} allocator_config;

typedef struct disk_geometry {
   uint64 disk_size;
   uint64 page_size;
   uint64 extent_size;
} disk_geometry;

/*
 *-----------------------------------------------------------------------------
 * allocator_config_init
 *
 *      Initialize allocator config values
 *-----------------------------------------------------------------------------
 */
void
allocator_config_init(allocator_config *allocator_cfg,
                      io_config        *io_cfg,
                      uint64            capacity);

static inline uint64
allocator_config_extent_base_addr(allocator_config *allocator_cfg, uint64 addr)
{
   return addr & allocator_cfg->extent_mask;
}

static inline uint64
allocator_config_pages_share_extent(allocator_config *allocator_cfg,
                                    uint64            addr1,
                                    uint64            addr2)
{
   return allocator_config_extent_base_addr(allocator_cfg, addr1)
          == allocator_config_extent_base_addr(allocator_cfg, addr2);
}

// ----------------------------------------------------------------------
// Type declarations for allocator ops

typedef uint32 refcount;

typedef struct allocator allocator;

typedef allocator_config *(*allocator_get_config_fn)(allocator *al);

typedef platform_status (*alloc_fn)(allocator *al,
                                    uint64    *addr,
                                    page_type  type);

typedef refcount (*dec_ref_fn)(allocator *al, uint64 addr, page_type type);
typedef refcount (*generic_ref_fn)(allocator *al, uint64 addr);

/*
 * Record one logical reference to addr while rebuilding a recovery map (see
 * allocator_load_refcounts() with rebuild == TRUE). The first reference to a
 * given extent establishes its nonzero allocation floor; later references
 * increment it normally. addr must be the base address of a non-reserved
 * extent.
 */
typedef platform_status (*recovery_record_reference_fn)(allocator *al,
                                                         uint64     addr,
                                                         page_type  type);

/*
 * Populate the in-memory refcount map of an allocator that has been attached
 * (mounted) but whose map is not yet loaded.  rebuild == FALSE loads the
 * trusted persisted map from its fixed durable location; rebuild == TRUE
 * initializes an empty map that reserves only the allocator's own fixed extents
 * and enters recovery mode (the caller then rebuilds via
 * recovery_record_reference()).  Must be called exactly once, after the attach
 * mount and before any allocation.  The caller learns which mode to request
 * from durable metadata it owns (the superblock's allocation-state validity),
 * so the allocator never reads the persisted map when it would be discarded.
 */
typedef platform_status (*load_refcounts_fn)(allocator *al, bool32 rebuild);

/*
 * Write the refcount map to its durable location and make it durable.  Returns
 * the base address of the persisted map in *state_addr; the caller records that
 * as the superblock's allocation_state_addr only after this returns, so the
 * "map is trustworthy" flag never becomes durable before the map itself.  Not
 * valid while a recovery rebuild is in progress.
 */
typedef platform_status (*persist_refcounts_fn)(allocator *al,
                                                uint64    *state_addr);

typedef uint64 (*get_size_fn)(allocator *al);
typedef uint64 (*base_addr_fn)(const allocator *al, uint64 addr);

typedef void (*print_fn)(allocator *al);
typedef void (*assert_fn)(allocator *al);

/*
 * Define an abstract allocator interface, holding different allocation-related
 * function pointers.
 */
typedef struct allocator_ops {
   allocator_get_config_fn get_config;
   alloc_fn                alloc;

   generic_ref_fn                inc_ref;
   dec_ref_fn                    dec_ref;
   generic_ref_fn                get_ref;
   recovery_record_reference_fn  recovery_record_reference;

   load_refcounts_fn    load_refcounts;
   persist_refcounts_fn persist;

   get_size_fn in_use;

   get_size_fn  get_capacity;
   base_addr_fn extent_base_addr;

   assert_fn assert_noleaks;

   print_fn print_stats;
   print_fn print_allocated;
} allocator_ops;

// To sub-class cache, make a cache your first field;
struct allocator {
   const allocator_ops *ops;
};

static inline allocator_config *
allocator_get_config(allocator *al)
{
   return al->ops->get_config(al);
}

static inline platform_status
allocator_alloc(allocator *al, uint64 *addr, page_type type)
{
   return al->ops->alloc(al, addr, type);
}

static inline refcount
allocator_inc_ref(allocator *al, uint64 addr)
{
   return al->ops->inc_ref(al, addr);
}

static inline refcount
allocator_dec_ref(allocator *al, uint64 addr, page_type type)
{
   return al->ops->dec_ref(al, addr, type);
}

static inline refcount
allocator_get_refcount(allocator *al, uint64 addr)
{
   return al->ops->get_ref(al, addr);
}

static inline platform_status
allocator_recovery_record_reference(allocator *al, uint64 addr, page_type type)
{
   return al->ops->recovery_record_reference(al, addr, type);
}

static inline platform_status
allocator_load_refcounts(allocator *al, bool32 rebuild)
{
   return al->ops->load_refcounts(al, rebuild);
}

static inline platform_status
allocator_persist(allocator *al, uint64 *state_addr)
{
   return al->ops->persist(al, state_addr);
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

static inline bool32
allocator_page_valid(allocator *al, uint64 addr)
{
   allocator_config *allocator_cfg = allocator_get_config(al);

   if ((addr % allocator_cfg->io_cfg->page_size) != 0) {
      platform_error_log("%s():%d: Specified addr=%lu is not divisible by"
                         " configured page size=%lu\n",
                         __func__,
                         __LINE__,
                         addr,
                         allocator_cfg->io_cfg->page_size);
      return FALSE;
   }

   uint64 base_addr = allocator_config_extent_base_addr(allocator_cfg, addr);
   if ((base_addr != 0) && (addr < allocator_cfg->capacity)) {
      refcount rfc = allocator_get_refcount(al, base_addr);
      if (rfc == 0) {
         platform_error_log(
            "%s():%d: Trying to access an unreferenced extent."
            " base_addr=%lu, addr=%lu, allocator_get_refcount()=%d\n",
            __func__,
            __LINE__,
            base_addr,
            addr,
            rfc);
      }
      return (rfc != 0);
   } else {
      platform_error_log("%s():%d: Extent out of allocator capacity range."
                         " base_addr=%lu, addr=%lu"
                         ", allocator_get_capacity()=%lu\n",
                         __func__,
                         __LINE__,
                         base_addr,
                         addr,
                         allocator_get_capacity(al));
      return FALSE;
   }
}
