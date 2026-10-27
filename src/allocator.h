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
 * These numeric values are persisted in mini-allocator extent entries. Changing
 * their meaning requires a mini-allocator format version change. Modules that
 * access a page also know which structure format to expect.
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

typedef uint64 refcount;

typedef struct allocator allocator;

typedef allocator_config *(*allocator_get_config_fn)(allocator *al);

typedef platform_status (*alloc_fn)(allocator *al,
                                    uint64    *addr,
                                    page_type  type);

typedef refcount (*dec_ref_fn)(allocator *al, uint64 addr, page_type type);
typedef refcount (*generic_ref_fn)(allocator *al, uint64 addr);
typedef platform_status (*inc_ref_fn)(allocator *al,
                                      uint64     addr,
                                      refcount  *new_count);

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
 *
 */
typedef platform_status (*generic_status_allocator_fn)(allocator *al);

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

typedef void (*generic_void_allocator_fn)(allocator *al);

/*
 * Define an abstract allocator interface, holding different allocation-related
 * function pointers.
 */
typedef struct allocator_ops {
   allocator_get_config_fn get_config;

   alloc_fn       alloc;
   inc_ref_fn     inc_ref;
   dec_ref_fn     dec_ref;
   generic_ref_fn get_ref;
   /* Packed maps can answer allocation-state checks without a hash lookup. */
   bool32 (*is_allocated)(allocator *al, uint64 addr);

   // After a mount, you must do either a recovery or a load

   generic_status_allocator_fn  recovery_begin;
   recovery_record_reference_fn recovery_record_reference;
   generic_void_allocator_fn    recovery_finish;

   generic_status_allocator_fn load_refcounts;

   persist_refcounts_fn persist;

   /* Optional: reclaim storage retained for a loaded or persisted allocation
    * snapshot. The caller must first durably invalidate the snapshot's
    * publication and keep the allocator quiescent throughout this call.
    * Loading a snapshot alone does not authorize releasing its storage. */
   generic_status_allocator_fn release_snapshot;

   get_size_fn in_use;

   get_size_fn  get_capacity;
   base_addr_fn extent_base_addr;

   generic_void_allocator_fn assert_noleaks;

   generic_void_allocator_fn print_stats;
   generic_void_allocator_fn print_allocated;
} allocator_ops;

// To sub-class allocator, make an allocator your first field;
struct allocator {
   const allocator_ops *ops;
   /* First abandoned reclamation error. Extents may remain allocated until a
    * reachability rebuild; never persist this map as a clean allocation state.
    */
   platform_status reclamation_status;
};

static inline void
allocator_record_reclamation_failure(allocator *al, platform_status status)
{
   platform_assert(!SUCCESS(status));
   internal_platform_status expected = 0;
   __atomic_compare_exchange_n(&al->reclamation_status.r,
                               &expected,
                               status.r,
                               FALSE,
                               __ATOMIC_RELEASE,
                               __ATOMIC_RELAXED);
}

static inline platform_status
allocator_reclamation_status(const allocator *al)
{
   return CONST_STATUS(
      __atomic_load_n(&al->reclamation_status.r, __ATOMIC_ACQUIRE));
}


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

/* Acquire one reference to an allocated extent. On failure the count and
 * optional output remain unchanged. A concurrent owner may still change the
 * count. Returns INVALID_STATE for a free extent, LIMIT_EXCEEDED at UINT64_MAX,
 * or NO_MEMORY if the packed overflow table cannot grow. */
__attribute__((warn_unused_result)) static inline platform_status
allocator_inc_ref(allocator *al, uint64 addr, refcount *new_count)
{
   return al->ops->inc_ref(al, addr, new_count);
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

static inline bool32
allocator_is_allocated(allocator *al, uint64 addr)
{
   if (al->ops->is_allocated != NULL) {
      return al->ops->is_allocated(al, addr);
   }
   return allocator_get_refcount(al, addr) != AL_FREE;
}

/*
 * Discard whatever the refcount map holds and start a rebuild: afterwards only
 * the reserved extents are referenced, and
 * allocator_recovery_record_reference() supplies the rest until
 * allocator_recovery_finish() declares the map usable.
 *
 * Repeatable, deliberately.  Crash recovery rebuilds twice: once counting the
 * logs, so that replay is never handed an extent a record it has not reached
 * yet depends on, and again from the durable root alone once replay has
 * finished and been folded in.  The second rebuild is what frees the logs --
 * their extents are simply absent from it -- which is why nothing has to
 * enumerate them a second time in order to release them.
 */
static inline platform_status
allocator_recovery_begin(allocator *al)
{
   return al->ops->recovery_begin(al);
}

static inline platform_status
allocator_recovery_record_reference(allocator *al, uint64 addr, page_type type)
{
   return al->ops->recovery_record_reference(al, addr, type);
}

static inline void
allocator_recovery_finish(allocator *al)
{
   al->ops->recovery_finish(al);
   __atomic_store_n(&al->reclamation_status.r, 0, __ATOMIC_RELEASE);
}


static inline platform_status
allocator_load_refcounts(allocator *al)
{
   return al->ops->load_refcounts(al);
}

static inline platform_status
allocator_persist(allocator *al, uint64 *state_addr)
{
   platform_status rc = allocator_reclamation_status(al);
   if (!SUCCESS(rc)) {
      if (state_addr != NULL) {
         *state_addr = 0;
      }
      return rc;
   }
   return al->ops->persist(al, state_addr);
}

/* Only after durable invalidation of the allocation snapshot, with no
 * concurrent allocator users. Backends without snapshot storage need no op. */
static inline platform_status
allocator_release_snapshot(allocator *al)
{
   return al->ops->release_snapshot == NULL ? STATUS_OK
                                            : al->ops->release_snapshot(al);
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
      bool32 rfc = allocator_is_allocated(al, base_addr);
      if (rfc == 0) {
         platform_error_log(
            "%s():%d: Trying to access an unreferenced extent."
            " base_addr=%lu, addr=%lu, allocator_is_allocated()=%d\n",
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
