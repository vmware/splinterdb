// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * stubcache.c --
 *
 *     This file contains the implementation for a stub in-memory cache.
 *-----------------------------------------------------------------------------
 */
#include "platform.h"

#include "allocator.h"
#include "stubcache.h"
#include "io.h"
#include "task.h"

#include <stddef.h>
#include "util.h"

//#include "poison.h" // TODO how do you guys debug?

//////////////////////////////////////////////////////////////////////////////
// stubcache_config
//
// jonh got lazy and rolled the implementation together with the
// type-correcting trampoline

uint64
stubcache_config_page_size_virtual(const cache_config *cfg)
{
   stubcache_config *ccfg = (stubcache_config *)cfg;
   return ccfg->page_size;
}

uint64
stubcache_config_extent_size_virtual(const cache_config *cfg)
{
   stubcache_config *ccfg = (stubcache_config *)cfg;
   return ccfg->extent_size;
}

uint64
stubcache_config_extent_base_addr_virtual(const cache_config *cfg, uint64 addr)
{
   stubcache_config *ccfg = (stubcache_config *)cfg;
   return addr & ccfg->extent_mask;
}


cache_config_ops stubcache_config_ops = {
   .page_size        = stubcache_config_page_size_virtual,
   .extent_size      = stubcache_config_extent_size_virtual,
   .extent_base_addr = stubcache_config_extent_base_addr_virtual,
};

void stubcache_config_init(
  stubcache_config* cfg,  // OUT
  uint64_t page_size, // IN
  uint64_t extent_size, // IN
  allocator *al)   // IN -- to learn capacity of "disk"
{
   cfg->super.ops = &stubcache_config_ops;
   cfg->disk_capacity_bytes = allocator_get_capacity(al);
   cfg->page_size = page_size;
   assert(cfg->page_size == 4096);
   cfg->disk_capacity_pages = cfg->disk_capacity_bytes / cfg->page_size;

   cfg->extent_size = extent_size;
   uint64 log_extent_size   = 63 - __builtin_clzll(extent_size);
   cfg->extent_mask   = ~((1ULL << log_extent_size) - 1);
}

//////////////////////////////////////////////////////////////////////////////
// stubcache

// runtime fail placeholder for things I haven't stubbed properly yet
void stubcache_unimpl() {
  assert(FALSE);
}

void stubcache_noop() {
}

//////////////////////////////////////////////////////////////////////////////
// fwd decls

page_handle *
stubcache_page_alloc(stubcache *sc, uint64 addr, page_type type);

allocator *
stubcache_allocator(const stubcache *sc);

//////////////////////////////////////////////////////////////////////////////
// type-fixing "virtual" trampolines

page_handle *
stubcache_page_alloc_virtual(cache *c, uint64 addr, page_type type) {
   stubcache *cc = (stubcache *)c;
   return stubcache_page_alloc(cc, addr, type);
}

page_handle *
stubcache_get_virtual(cache *c, uint64 addr, bool blocking, page_type type)
{
   stubcache *cc = (stubcache *)c;
   return stubcache_page_alloc(cc, addr, type);
}

allocator *
stubcache_allocator_virtual(const cache *c)
{
   stubcache *cc = (stubcache *)c;
   return stubcache_allocator(cc);
}

cache_config *
stubcache_get_config_virtual(const cache *c)
{
   stubcache *sc = (stubcache *)c;
   return &sc->cfg->super;
}

bool
stubcache_page_valid_virtual(cache *c, uint64 addr)
{
   //stubcache *cc = (stubcache *)c;
   return TRUE;
}

void
stubcache_io_stats_virtual(cache *c, uint64 *read_bytes, uint64 *write_bytes)
{
   *read_bytes = 0;
   *write_bytes = 0;
}

//////////////////////////////////////////////////////////////////////////////

static cache_ops stubcache_ops = {
    // do
   .page_alloc        = stubcache_page_alloc_virtual,
   .extent_hard_evict = (void*) stubcache_noop, // TODO rename discard

   // ignore
   .page_get_ref      = (void*) stubcache_unimpl,

   // do
   .page_get          = stubcache_get_virtual,

   // ignore
   .page_get_async    = (void*) stubcache_unimpl, // probably irrelevant; turn off with test configs
   .page_async_done   = (void*) stubcache_unimpl, // probably irrelevant

   // do
   .page_unget        = (void*) stubcache_noop,

   // noop
   .page_claim        = (void*) stubcache_noop,
   .page_unclaim      = (void*) stubcache_noop,
   .page_lock         = (void*) stubcache_noop,
   .page_unlock       = (void*) stubcache_noop,
   .page_prefetch     = (void*) stubcache_noop,
   .page_mark_dirty   = (void*) stubcache_noop,
   .page_pin          = (void*) stubcache_noop,
   .page_unpin        = (void*) stubcache_noop,
   .page_sync         = (void*) stubcache_noop,

   // ignore
   .extent_sync       = (void*) stubcache_unimpl,
   .flush             = (void*) stubcache_noop,
   .evict             = (void*) stubcache_noop,
   .cleanup           = (void*) stubcache_noop,
   .assert_ungot      = (void*) stubcache_noop,
   .assert_free       = (void*) stubcache_noop,
   .print             = (void*) stubcache_unimpl,
   .print_stats       = (void*) stubcache_noop,
   .io_stats          = stubcache_io_stats_virtual,
   .reset_stats       = (void*) stubcache_noop,
   .page_valid        = stubcache_page_valid_virtual,
   .validate_page     = (void*) stubcache_noop,
   .count_dirty       = (void*) stubcache_unimpl,
   .page_get_read_ref = (void*) stubcache_unimpl, // alex doesn't know what it does
   .cache_present     = (void*) stubcache_unimpl,
   .enable_sync_get   = (void*) stubcache_unimpl,
   .cache_allocator   = stubcache_allocator_virtual, // TODO vestigial, but half a dozen call sites incl. mini_allocator
   .get_config        = stubcache_get_config_virtual,
};

//////////////////////////////////////////////////////////////////////////////
// type-specific init, deinit

platform_status
stubcache_init(stubcache          *sc,
               stubcache_config *cfg,
                allocator           *al)
{
   sc->super.ops = &stubcache_ops;
   sc->cfg = cfg;
   sc->al = al;

   platform_heap_id hid_0 = 0;
   sc->page_handles = TYPED_ARRAY_MALLOC(hid_0, sc->page_handles, sc->cfg->disk_capacity_pages);
   sc->the_disk = platform_aligned_malloc(hid_0, 1, sc->cfg->disk_capacity_bytes);

   return STATUS_OK;
}

void
stubcache_deinit(stubcache *sc)
{
}

//////////////////////////////////////////////////////////////////////////////
// cache_ops

page_handle *
stubcache_page_alloc(stubcache *sc, uint64 addr, page_type type) {
    assert(addr < sc->cfg->disk_capacity_bytes);
    // platform_default_log
    // platform_error_log
    //printf("stubcache_page_alloc addr %lx\n", addr);
    uint64_t page = addr/sc->cfg->page_size;
    assert(page * sc->cfg->page_size == addr);
    page_handle* ph = &(sc->page_handles[page]);
    ph->data = &sc->the_disk[addr];
    ph->disk_addr = addr;
    return ph;
}

allocator *
stubcache_allocator(const stubcache *sc)
{
    return sc->al;
}
