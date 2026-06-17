// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * rc_allocator.h --
 *
 * This file contains the interface for the ref count allocator.
 */

#pragma once

#include "platform_hash.h"
#include "platform_buffer.h"
#include "platform_mutex.h"
#include "allocator.h"
#include "util.h"

/*
 * In the current system, every Splinter instance has a superblock, one
 * for each table that is mapped to the Splinter instance. This limit
 * is the max number of superblocks (special pages) that can be accessed.
 * All of these superblocks are required to be on the 1st extent.
 */
#define RC_ALLOCATOR_MAX_ROOT_IDS (30)

#define RC_ALLOCATOR_SUPER_BLOCK_MAGIC   (0x53504c5444425342ULL)
#define RC_ALLOCATOR_SUPER_BLOCK_VERSION (1)

/*
 *----------------------------------------------------------------------
 * rc_allocator_super_block -- Disk-resident bootstrap superblock.
 *
 * This is the first field in the allocator meta page, which is persisted at
 * offset 0.  Keep it independent of the configured page size so open can read
 * the database geometry before mounting the rest of SplinterDB.
 *----------------------------------------------------------------------
 */
typedef struct ONDISK rc_allocator_super_block {
   uint64      magic;
   uint64      format_version;
   uint64      feature_flags;
   uint64      incompatible_feature_flags;
   uint64      disk_size;
   uint64      page_size;
   uint64      extent_size;
   checksum128 checksum;
} rc_allocator_super_block;

typedef struct rc_allocator_super_block_config {
   uint64 format_version;
   uint64 feature_flags;
   uint64 incompatible_feature_flags;
   uint64 disk_size;
   uint64 page_size;
   uint64 extent_size;
} rc_allocator_super_block_config;

/*
 *----------------------------------------------------------------------
 * rc_allocator_meta_page -- Disk-resident structure.
 *
 * An on disk structure to hold the bootstrap superblock and the super block
 * addresses for all Splinter tables using this allocator.
 *----------------------------------------------------------------------
 */
typedef struct ONDISK rc_allocator_meta_page {
   rc_allocator_super_block super;
   allocator_root_id        splinters[RC_ALLOCATOR_MAX_ROOT_IDS];
   checksum128              checksum;
} rc_allocator_meta_page;

_Static_assert(offsetof(rc_allocator_meta_page, super) == 0,
               "super block should be first field in meta_page struct");
_Static_assert(sizeof(rc_allocator_meta_page) <= IO_DEFAULT_PAGE_SIZE,
               "allocator meta page must fit in the default page size");

/*
 *----------------------------------------------------------------------
 * rc_allocator_stats --
 *----------------------------------------------------------------------
 */
typedef struct rc_allocator_stats {
   int64 curr_allocated; // # of extents allocated
   int64 max_allocated;  // # of extents allocated high-water mark
   int64 extent_allocs[NUM_PAGE_TYPES];
   int64 extent_deallocs[NUM_PAGE_TYPES];
} rc_allocator_stats;

/*
 *----------------------------------------------------------------------
 * rc_allocator -- Ref Count allocator context structure.
 *----------------------------------------------------------------------
 */
typedef struct rc_allocator {
   allocator               super;
   allocator_config       *cfg;
   buffer_handle           bh;
   refcount               *ref_count;
   uint64                  hand;
   io_handle              *io;
   rc_allocator_meta_page *meta_page;

   /*
    * mutex to synchronize updates to super block addresses of the splinter
    * tables in the meta page.
    */
   platform_mutex   lock;
   platform_heap_id heap_id;

   // Stats -- not distributed for now
   rc_allocator_stats stats;
} rc_allocator;

platform_status
rc_allocator_init(rc_allocator      *al,
                  allocator_config  *cfg,
                  io_handle         *io,
                  platform_heap_id   hid,
                  platform_module_id mid);

void
rc_allocator_deinit(rc_allocator *al);

platform_status
rc_allocator_mount(rc_allocator      *al,
                   allocator_config  *cfg,
                   io_handle         *io,
                   platform_heap_id   hid,
                   platform_module_id mid);

platform_status
rc_allocator_read_super_block(const char                      *filename,
                              platform_heap_id                 hid,
                              rc_allocator_super_block_config *super_cfg);

platform_status
rc_allocator_super_block_matches_config(
   const rc_allocator_super_block_config *super_cfg,
   const allocator_config                *cfg);

void
rc_allocator_unmount(rc_allocator *al);
