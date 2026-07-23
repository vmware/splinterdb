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
   allocator         super;
   allocator_config *cfg;
   buffer_handle     bh;
   refcount         *ref_count;
   uint64            hand;
   io_handle        *io;

   /* Serializes refcount-map mutations that must be atomic (e.g. stats). */
   platform_mutex   lock;
   platform_heap_id heap_id;

   /*
    * True between rc_allocator_open_refcounts(al, rebuild=TRUE) and either
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
 * Attach to an existing device: initialize the in-memory structures and the
 * (zeroed) refcount buffer, but do not read the persisted map.  The caller
 * then populates the map exactly once via allocator_open_refcounts() (see
 * allocator.h): open_refcounts(rebuild=FALSE) loads the trusted persisted map;
 * open_refcounts(rebuild=TRUE) initializes an empty map for a rebuild.  The
 * caller chooses the mode from the superblock's allocation-state validity,
 * which it has already read, so a rebuild never pays for a discarded map read.
 * Before any allocation mutates the loaded map, the caller publishes an
 * invalidated superblock so a crash forces a rebuild.
 */
platform_status
rc_allocator_mount(rc_allocator      *al,
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
 * Complete a successful rebuild without performing I/O.  A later
 * rc_allocator_persist() may persist the rebuilt table on clean shutdown.
 */
void
rc_allocator_rebuild_finish(rc_allocator *al);

/*
 * Discard a partially rebuilt recovery map.  Never writes allocator state to
 * disk; the caller follows with rc_allocator_deinit().
 */
void
rc_allocator_abort_recovery(rc_allocator *al);
