// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * clockcache.h --
 *
 *     This file contains interface for a concurrent clock cache.
 */

#ifndef __STUBCACHE_H
#define __STUBCACHE_H

#include "allocator.h"
#include "cache.h"
#include "io.h"
#include "task.h"

//#define ADDR_TRACING
#define TRACE_ADDR  (UINT64_MAX - 1)
#define TRACE_ENTRY (UINT32_MAX - 1)

typedef struct stubcache_config {
   cache_config super;

   uint64_t extent_size;
   uint64_t extent_mask;
   uint64_t           disk_capacity_bytes;
   uint64_t           page_size;
   uint64_t           disk_capacity_pages;
} stubcache_config;

void stubcache_config_init(
  stubcache_config* cfg,  // OUT
  uint64_t page_size, // IN
  uint64_t extent_size, // IN
  allocator *al);         // IN -- to learn capacity of "disk"

typedef struct stubcache       stubcache;
struct stubcache {
   cache              super;
   stubcache_config *cfg;

   // stash an alloctaor to satisfy vestigial ifc
   // TODO mini_init should take an allocator in its init.
   allocator           *al;

   page_handle        *page_handles;
   char*           the_disk;
};



platform_status
stubcache_init(stubcache          *cc,  // OUT
               stubcache_config *cfg, // IN
                allocator           *al   // IN -- to learn capacity of "disk"
              );

void
stubcache_deinit(stubcache *cc); // IN

#endif // __STUBCACHE_H
