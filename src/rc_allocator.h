// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * rc_allocator.h --
 *
 * This file contains the interface for the ref count allocator.
 */

#ifndef __RC_ALLOCATOR_H
#define __RC_ALLOCATOR_H

#include "platform.h"
#include "allocator.h"
#include "util.h"

#define RC_ALLOCATOR_MAX_ALLOCATOR_ROOT_IDS (30)

typedef struct rc_allocator_config {
   uint64 capacity;
   uint64 page_capacity;
   uint64 extent_capacity;
   uint64 page_size;
   uint64 extent_size;
} rc_allocator_config;

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_meta_page --
 *
 *  An on disk structure to hold the super block information about all the
 *  splinter tables using this allocator. This is persisted at
 *  offset 0 of the device. 
 *
 *----------------------------------------------------------------------
 */

typedef struct PACKED rc_allocator_meta_page {
   allocator_root_id         splinters[RC_ALLOCATOR_MAX_ALLOCATOR_ROOT_IDS];
   checksum128         checksum;
} rc_allocator_meta_page;

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator --
 *
 *----------------------------------------------------------------------
 */

typedef struct rc_allocator {
   allocator               super;
   rc_allocator_config    *cfg;
   int64                   allocated;
   int64                   max_allocated;
   buffer_handle          *bh;
   uint8                  *ref_count;
   uint64                  hand;
   io_handle              *io;
   rc_allocator_meta_page *meta_page;
   /*
    * mutex to synchronize updates to super block addresses of the splinter
    * tables in the meta page.
    */
   platform_mutex          lock;
   platform_heap_handle    heap_handle;
   platform_heap_id        heap_id;
} rc_allocator;


void
rc_allocator_config_init(rc_allocator_config *allocator_cfg,
                         uint64               page_size,
                         uint64               extent_size,
                         uint64               capacity);

platform_status  rc_allocator_init     (rc_allocator *al,
                                        rc_allocator_config *cfg,
                                        io_handle *io,
                                        platform_heap_handle hh,
                                        platform_heap_id hid,
                                        platform_module_id mid);
void             rc_allocator_deinit   (rc_allocator *al);
platform_status  rc_allocator_mount    (rc_allocator *al,
                                        rc_allocator_config *cfg,
                                        io_handle *io,
                                        platform_heap_handle hh,
                                        platform_heap_id hid,
                                        platform_module_id mid);
void             rc_allocator_dismount (rc_allocator *al);

#endif
