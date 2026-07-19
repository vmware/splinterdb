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

/*
 *----------------------------------------------------------------------
 * rc_allocator_meta_page -- Disk-resident structure.
 *
 * An on disk structure to hold the bootstrap disk geometry and the super block
 * addresses for all Splinter tables using this allocator. The geometry lives at
 * offset 0 so open can read it before mounting the rest of SplinterDB.
 *----------------------------------------------------------------------
 */
typedef struct ONDISK rc_allocator_meta_page {
   disk_geometry geometry;
   /*
    * Identifies the immutable bootstrap layout.  In particular, it proves
    * that the two fixed clean-state extents after the refcount map are owned
    * by this allocator rather than by an older on-disk format.
    */
   uint64            format_magic;
   uint64            format_version;
   allocator_root_id splinters[RC_ALLOCATOR_MAX_ROOT_IDS];
   checksum128       checksum;
} rc_allocator_meta_page;

_Static_assert(offsetof(rc_allocator_meta_page, geometry) == 0,
               "disk geometry should be first field in meta_page struct");
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

   /*
    * True between rc_allocator_mount_recovery() and either
    * rc_allocator_rebuild_finish() or rc_allocator_abort_recovery().  An
    * incomplete rebuilt map must never be written back to disk.
    */
   bool32 recovery_in_progress;

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

/*
 * Normal mount accepts only a durable clean-state record and publishes an
 * unclean state record before returning.  Those records occupy two fixed,
 * alternating extents after the persisted refcount map, so normal lifecycle
 * operations never rewrite the sole allocator bootstrap page.
 * STATUS_INVALID_STATE means callers must use the recovery-rebuild path
 * instead of trusting the persisted refcount map.
 */
platform_status
rc_allocator_mount(rc_allocator      *al,
                   allocator_config  *cfg,
                   io_handle         *io,
                   platform_heap_id   hid,
                   platform_module_id mid);

/*
 * Mount the allocator for crash recovery.  This validates and retains the
 * allocator metadata page, but deliberately ignores the persisted refcount
 * table.  The caller must rebuild the in-memory table from durable objects,
 * then call rc_allocator_rebuild_finish() before using normal allocator
 * lifecycle operations.
 */
platform_status
rc_allocator_mount_recovery(rc_allocator      *al,
                            allocator_config  *cfg,
                            io_handle         *io,
                            platform_heap_id   hid,
                            platform_module_id mid);

/*
 * Add one logical ownership reference for an extent while rebuilding a
 * recovery map.  The first reference establishes the allocator's nonzero
 * allocation floor (AL_ONE_REF) and records it in stats.extent_allocs[type];
 * later references increment the refcount normally.  extent_addr must be the
 * base address of a non-reserved allocator extent.
 */
platform_status
rc_allocator_rebuild_acquire_extent(rc_allocator *al,
                                    uint64        extent_addr,
                                    page_type     type);

/*
 * Complete a successful rebuild without performing I/O.  A later normal
 * rc_allocator_unmount() may persist the rebuilt table on clean shutdown.
 */
void
rc_allocator_rebuild_finish(rc_allocator *al);

/*
 * Discard a partially rebuilt recovery map.  Unlike rc_allocator_unmount(),
 * this never writes allocator state to disk.
 */
void
rc_allocator_abort_recovery(rc_allocator *al);

platform_status
rc_allocator_read_disk_geometry(const char *filename, disk_geometry *geometry);

platform_status
rc_allocator_disk_geometry_matches_config(const disk_geometry    *geometry,
                                          const allocator_config *cfg);

void
rc_allocator_unmount(rc_allocator *al);
