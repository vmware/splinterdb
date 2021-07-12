// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * rc_allocator.c --
 *
 *     This file contains the implementation of the ref count allocator.
 */

#include "platform.h"

#include "rc_allocator.h"
#include "io.h"

#include "poison.h"

#define RC_ALLOCATOR_META_PAGE_CSUM_SEED (2718281828)

/*
 * Base offset from where the allocator starts. Currently hard coded to 0.
 */

#define RC_ALLOCATOR_BASE_OFFSET (0)
/*
 *------------------------------------------------------------------------------
 *
 * function declarations
 *
 *------------------------------------------------------------------------------
 */

// allocator.h functions
platform_status      rc_allocator_alloc_extent         (rc_allocator *al, uint64 *addr_arr);
uint8                rc_allocator_inc_ref              (rc_allocator *al, uint64 addr);
uint8                rc_allocator_dec_ref              (rc_allocator *al, uint64 addr);
uint8                rc_allocator_get_ref              (rc_allocator *al, uint64 addr);
uint64               rc_allocator_get_capacity         (rc_allocator *al);
platform_status      rc_allocator_get_super_addr       (rc_allocator *al,
                                                        allocator_root_id spl_id,
                                                        uint64 *addr);
platform_status      rc_allocator_alloc_super_addr     (rc_allocator *al,
                                                        allocator_root_id spl_id,
                                                        uint64 *addr);
void                 rc_allocator_remove_super_addr    (rc_allocator *al,
                                                        allocator_root_id spl_id);
uint64               rc_allocator_extent_size          (rc_allocator *al);
uint64               rc_allocator_page_size            (rc_allocator *al);
void                 rc_allocator_print_allocated      (rc_allocator *al);
uint64               rc_allocator_max_allocated        (rc_allocator *al);
uint64               rc_allocator_in_use               (rc_allocator *al);

// other functions

const static allocator_ops rc_allocator_ops = {
   .alloc_extent      = (alloc_extent_fn)      rc_allocator_alloc_extent,
   .inc_refcount      = (inc_refcount_fn)      rc_allocator_inc_ref,
   .dec_refcount      = (dec_refcount_fn)      rc_allocator_dec_ref,
   .get_refcount      = (get_refcount_fn)      rc_allocator_get_ref,
   .get_capacity      = (get_capacity_fn)      rc_allocator_get_capacity,
   .get_super_addr    = (get_super_addr_fn)    rc_allocator_get_super_addr,
   .alloc_super_addr  = (alloc_super_addr_fn)  rc_allocator_alloc_super_addr,
   .remove_super_addr = (remove_super_addr_fn) rc_allocator_remove_super_addr,
   .in_use            = (in_use_fn)            rc_allocator_in_use,
   .get_extent_size   = (get_size_fn)          rc_allocator_extent_size,
   .get_page_size     = (get_size_fn)          rc_allocator_page_size,
   .print_allocated   = (print_allocated_fn)   rc_allocator_print_allocated,
   .max_allocated     = (max_allocated_fn)     rc_allocator_max_allocated,
};


static platform_status
rc_allocator_init_meta_page(rc_allocator *al)
{
   _Static_assert(offsetof(rc_allocator_meta_page, splinters) == 0,
                  "splinter array should be first field in meta_page struct");
   /*
    * To make it easier to do aligned i/o's we allocate the meta page to
    * always be page size. In the future we can use the remaining space
    * for some other reserved information we may want to persist as part
    * of the meta page.
    */
   platform_assert(sizeof(rc_allocator_meta_page) <= al->cfg->page_size);
   /*
    * Ensure that the meta page and  all the super blocks will fit in one
    * extent.
    */
   platform_assert((1 + RC_ALLOCATOR_MAX_ALLOCATOR_ROOT_IDS) * al->cfg->page_size
                    <= al->cfg->extent_size);

   al->meta_page =
      platform_aligned_malloc(al->heap_id, al->cfg->page_size,
                              al->cfg->page_size);
   if (al->meta_page == NULL) {
      return STATUS_NO_MEMORY;
   }

   memset(al->meta_page, 0, al->cfg->page_size);
   memset(al->meta_page->splinters, INVALID_ALLOCATOR_ROOT_ID,
          sizeof(al->meta_page->splinters));

   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 *
 * rc_allocator_config_init --
 *
 *      Initialize rc_allocator config values
 *
 *-----------------------------------------------------------------------------
 */

void
rc_allocator_config_init(rc_allocator_config *allocator_cfg,
                         uint64              page_size,
                         uint64              extent_size,
                         uint64              capacity)
{
   ZERO_CONTENTS(allocator_cfg);

   allocator_cfg->page_size   = page_size;
   allocator_cfg->extent_size = extent_size;
   allocator_cfg->capacity    = capacity;
   allocator_cfg->page_capacity = capacity / page_size;
   allocator_cfg->extent_capacity = capacity / extent_size;
}

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_[de]init --
 *
 *      [de]initialize an allocator
 *
 *----------------------------------------------------------------------
 */

platform_status
rc_allocator_init(rc_allocator         *al,
                  rc_allocator_config  *cfg,
                  io_handle            *io,
                  platform_heap_handle  hh,
                  platform_heap_id      hid,
                  platform_module_id    mid)
{
   uint64 rc_extent_count;
   uint64 addr;
   platform_status rc;
   platform_assert(al != NULL);
   ZERO_CONTENTS(al);
   al->super.ops = &rc_allocator_ops;
   al->cfg = cfg;
   al->io = io;
   al->heap_handle = hh;
   al->heap_id = hid;

   platform_assert(cfg->page_size % 4096 == 0);
   platform_assert(cfg->capacity == cfg->extent_size * cfg->extent_capacity);
   platform_assert(cfg->capacity == cfg->page_size * cfg->page_capacity);

   rc = platform_mutex_init(&al->lock, mid, al->heap_id);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init mutex for the allocator\n");
      return rc;
   }
   rc = rc_allocator_init_meta_page(al);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init meta page for rc allocator\n");
      platform_mutex_destroy(&al->lock);
      return rc;
   }
   // To ensure alignment always allocate in multiples of page size.
   uint32 buffer_size = cfg->extent_capacity * sizeof(uint8);
   buffer_size = ROUNDUP(buffer_size, cfg->page_size);
   al->bh = platform_buffer_create(buffer_size, al->heap_handle, mid);
   if (al->bh == NULL) {
      platform_error_log("Failed to create buffer for ref counts\n");
      platform_mutex_destroy(&al->lock);
      platform_free(al->heap_id, al->meta_page);
      return STATUS_NO_MEMORY;
   }

   al->ref_count = platform_buffer_getaddr(al->bh);
   memset(al->ref_count, 0, buffer_size);

   // allocate the super block
   allocator_alloc_extent(&al->super, &addr);
   // super block extent should always start from address 0.
   platform_assert(addr == RC_ALLOCATOR_BASE_OFFSET);

   /*
    * Allocate room for the ref counts, use same rounded up size used in buffer
    * creation.
    */
   rc_extent_count = (buffer_size + al->cfg->extent_size - 1)
      / al->cfg->extent_size;
   for (uint64 i = 0; i < rc_extent_count; i++) {
      allocator_alloc_extent(&al->super, &addr);
      platform_assert(addr == cfg->extent_size * (i + 1));
   }

   return STATUS_OK;
}

void rc_allocator_deinit(rc_allocator *al)
{
   platform_buffer_destroy(al->bh);
   al->ref_count = NULL;
   platform_mutex_destroy(&al->lock);
   platform_free(al->heap_id, al->meta_page);
}

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_[dis]mount --
 *
 *      Loads the file system from disk
 *      Write the file system to disk
 *
 *----------------------------------------------------------------------
 */

platform_status
rc_allocator_mount(rc_allocator         *al,
                   rc_allocator_config  *cfg,
                   io_handle            *io,
                   platform_heap_handle  hh,
                   platform_heap_id      hid,
                   platform_module_id    mid)
{
   platform_status status;

   platform_assert(al != NULL);
   ZERO_CONTENTS(al);
   al->super.ops = &rc_allocator_ops;
   al->cfg = cfg;
   al->io = io;
   al->heap_handle = hh;
   al->heap_id = hid;

   status = platform_mutex_init(&al->lock, mid, al->heap_id);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init mutex for the allocator\n");
      return status;
   }

   status = rc_allocator_init_meta_page(al);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init meta page for rc allocator\n");
      platform_mutex_destroy(&al->lock);
      return status;
   }

   platform_assert(cfg->page_size % 4096 == 0);
   platform_assert(cfg->capacity == cfg->extent_size * cfg->extent_capacity);
   platform_assert(cfg->capacity == cfg->page_size * cfg->page_capacity);

   uint32 buffer_size = cfg->extent_capacity * sizeof(uint8);
   buffer_size = ROUNDUP(buffer_size, cfg->page_size);
   al->bh = platform_buffer_create(buffer_size, al->heap_handle, mid);
   platform_assert(al->bh != NULL);
   al->ref_count = platform_buffer_getaddr(al->bh);

   // load the meta page from disk.
   status = io_read(io, al->meta_page, al->cfg->page_size,
                    RC_ALLOCATOR_BASE_OFFSET);
   platform_assert_status_ok(status);
   // validate the checksum of the meta page.
   checksum128 currChecksum =
      platform_checksum128(al->meta_page, sizeof(al->meta_page->splinters),
                           RC_ALLOCATOR_META_PAGE_CSUM_SEED);
   if (!platform_checksum_is_equal(al->meta_page->checksum, currChecksum)) {
      platform_assert(0 == "Corrupt Meta Page upon mount");
   }

   // load the ref counts from disk.
   uint32 io_size = ROUNDUP(al->cfg->extent_capacity, al->cfg->page_size);
   status = io_read(io, al->ref_count, io_size, cfg->extent_size);
   platform_assert_status_ok(status);
   for (uint64 i = 0; i < al->cfg->extent_capacity; i++) {
      if (al->ref_count[i] != 0) {
         al->allocated++;
      }
   }
   platform_log("Allocated at mount: %lu MiB\n",
         B_TO_MiB(al->allocated * cfg->extent_size));
   return STATUS_OK;
}


void
rc_allocator_dismount(rc_allocator *al)
{
   platform_status status;

   platform_log("Allocated at dismount: %lu MiB\n",
         B_TO_MiB(al->allocated * al->cfg->extent_size));
   // persist the ref counts upon dismount.
   uint32 io_size = ROUNDUP(al->cfg->extent_capacity, al->cfg->page_size);
   status = io_write(al->io, al->ref_count, io_size, al->cfg->extent_size);
   platform_assert_status_ok(status);
   rc_allocator_deinit(al);
}


/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_inc_ref --
 *
 *      Increments the ref count of the given address.
 *
 *----------------------------------------------------------------------
 */

uint8
rc_allocator_inc_ref(rc_allocator *al,
                     uint64 addr)
{
   uint64 extent_no;
   uint8  ref_count;

   debug_assert(addr % al->cfg->extent_size == 0);
   extent_no = addr / al->cfg->extent_size;
   debug_assert(extent_no < al->cfg->extent_capacity);
   ref_count = __sync_fetch_and_add(&al->ref_count[extent_no], 1);
   platform_assert(ref_count != 0 && ref_count != UINT8_MAX);
   return ref_count;
}

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_get_ref --
 *
 *      Returns the ref count of the given address.
 *
 *----------------------------------------------------------------------
 */

uint8
rc_allocator_get_ref(rc_allocator *al,
                     uint64       addr)
{
   uint64 extent_no;

   debug_assert(addr % al->cfg->extent_size == 0);
   extent_no = addr / al->cfg->extent_size;
   debug_assert(extent_no < al->cfg->extent_capacity);
   return al->ref_count[extent_no];
}

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_dec_ref --
 *
 *      Decrements the ref count and returns the new one. If the ref_count
 *      is 0, then the extent is free.
 *
 *----------------------------------------------------------------------
 */

uint8
rc_allocator_dec_ref(rc_allocator *al,
                     uint64        addr)
{
   uint64 extent_no;
   uint8  ref_count;

   debug_assert(addr % al->cfg->extent_size == 0);
   extent_no = addr / al->cfg->extent_size;
   debug_assert(extent_no < al->cfg->extent_capacity);
   ref_count = __sync_fetch_and_sub(&al->ref_count[extent_no], 1);
   if (ref_count == 0) {
      platform_log("allocator ref count < 0: %lu\n", addr);
   }
   platform_assert(ref_count != 0);
   if (ref_count == 1) {
      __sync_fetch_and_sub(&al->allocated, 1);
   }
   return ref_count;
}


/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_get_[capacity,super_addr] --
 *
 *      returns the struct/parameter
 *
 *----------------------------------------------------------------------
 */

uint64
rc_allocator_get_capacity(rc_allocator *al)
{
   return al->cfg->capacity;
}


platform_status
rc_allocator_get_super_addr(rc_allocator *al,
                            allocator_root_id  allocator_root_id,
                            uint64       *addr)
{
   platform_status status = STATUS_NOT_FOUND;

   platform_mutex_lock(&al->lock);
   for (uint8 idx = 0; idx < RC_ALLOCATOR_MAX_ALLOCATOR_ROOT_IDS; idx++) {
      if (al->meta_page->splinters[idx] == allocator_root_id) {
         // have already seen this table before, return existing addr.
         *addr =  (1 + idx) * al->cfg->page_size;
         status =  STATUS_OK;
         break;
      }
   }

   platform_mutex_unlock(&al->lock);
   return status;

}

uint64 rc_allocator_in_use(rc_allocator *al)
{
   return al->allocated * al->cfg->extent_size;
}

uint64 rc_allocator_max_allocated(rc_allocator *al)
{
   return al->max_allocated * al->cfg->extent_size;
}

platform_status
rc_allocator_alloc_super_addr(rc_allocator *al,
                              allocator_root_id allocator_root_id,
                              uint64 *addr)
{
   platform_status status = STATUS_NOT_FOUND;

   platform_mutex_lock(&al->lock);
   for (uint8 idx = 0; idx < RC_ALLOCATOR_MAX_ALLOCATOR_ROOT_IDS; idx++) {
      if (al->meta_page->splinters[idx] == INVALID_ALLOCATOR_ROOT_ID) {
         // assign the first available slot and update the on disk metadata.
         al->meta_page->splinters[idx] = allocator_root_id;
         *addr = (1 + idx) * al->cfg->page_size;
         al->meta_page->checksum =
            platform_checksum128(al->meta_page,
                                 sizeof(al->meta_page->splinters),
                                 RC_ALLOCATOR_META_PAGE_CSUM_SEED);
         platform_status io_status =
            io_write(al->io, al->meta_page, al->cfg->page_size,
                     RC_ALLOCATOR_BASE_OFFSET);
         platform_assert_status_ok(io_status);
         status = STATUS_OK;
         break;
      }
   }

   platform_mutex_unlock(&al->lock);
   return status;
}


void
rc_allocator_remove_super_addr(rc_allocator *al,
                               allocator_root_id allocator_root_id)
{
   platform_mutex_lock(&al->lock);

   for (uint8 idx = 0; idx < RC_ALLOCATOR_MAX_ALLOCATOR_ROOT_IDS; idx++) {
      /*
       * clear out the mapping for this splinter table and update on disk
       * metadata.
       */
      if (al->meta_page->splinters[idx] == allocator_root_id) {
         al->meta_page->splinters[idx] = INVALID_ALLOCATOR_ROOT_ID;
         al->meta_page->checksum =
            platform_checksum128(al->meta_page,
                                 sizeof(al->meta_page->splinters),
                                 RC_ALLOCATOR_META_PAGE_CSUM_SEED);
         platform_status status =
            io_write(al->io, al->meta_page, al->cfg->page_size,
                     RC_ALLOCATOR_BASE_OFFSET);
         platform_assert_status_ok(status);
         platform_mutex_unlock(&al->lock);
         return;
      }
   }

   platform_mutex_unlock(&al->lock);
   // Couldnt find the splinter id in the meta page.
   platform_assert(0 == "Couldnt find existing splinter table in meta page");

}


uint64
rc_allocator_extent_size(rc_allocator *al)
{
   return al->cfg->extent_size;
}

uint64
rc_allocator_page_size(rc_allocator *al)
{
   return al->cfg->page_size;
}

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_alloc_extent --
 *
 *      Allocate an extent
 *
 *----------------------------------------------------------------------
 */

platform_status
rc_allocator_alloc_extent(rc_allocator *al,         /* IN  */
                          uint64       *addr)       /* OUT */
{
   uint64 first_hand = al->hand % al->cfg->extent_capacity;
   uint64 hand;
   bool extent_is_free = FALSE;

   do {
      hand = __sync_fetch_and_add(&al->hand, 1) % al->cfg->extent_capacity;
      if (al->ref_count[hand] == 0)
         extent_is_free = __sync_bool_compare_and_swap(&al->ref_count[hand], 0, 2);
   } while (!extent_is_free && (hand + 1) % al->cfg->extent_capacity != first_hand);
   if (!extent_is_free) {
      platform_log ("Out of Space: allocated %lu out of %lu addrs.\n",
            al->allocated, al->cfg->extent_capacity);
      return STATUS_NO_SPACE;
   }
   int64 curr_allocated =__sync_add_and_fetch(&al->allocated, 1);
   int64 max_allocated = al->max_allocated;
   while (curr_allocated > max_allocated) {
      __sync_bool_compare_and_swap(&al->max_allocated, max_allocated,
                                   curr_allocated);
      max_allocated = al->max_allocated;
   }
   *addr = hand * al->cfg->extent_size;

   return STATUS_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * rc_allocator_print_allocated --
 *
 *      Prints the base addrs of all allocated extents.
 *
 *----------------------------------------------------------------------
 */

void
rc_allocator_print_allocated(rc_allocator *al)
{
   uint64 i;
   uint8  ref;
   platform_log("Allocated: %lu\n", al->allocated);
   for (i = 0; i < al->cfg->extent_capacity; i++) {
      ref = al->ref_count[i];
      if (ref != 0)
         platform_log("%lu -- %u\n", i * al->cfg->extent_size, ref);
   }
}
