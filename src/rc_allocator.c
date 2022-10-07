// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *------------------------------------------------------------------------------
 * rc_allocator.c --
 *
 *     This file contains the implementation of the ref count allocator.
 *------------------------------------------------------------------------------
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

/* A predicate defining whether to trace allocations/ref-count changes
 * on a given address.
 *
 * Examples:
 * #define SHOULD_TRACE(addr) (1) // trace all addresses
 * #define SHOULD_TRACE(addr) ((addr) / (4096 * 32) == 339ULL) // trace extent
 * 339
 */
#define SHOULD_TRACE(addr) (0) // Do not trace anything

/*
 *------------------------------------------------------------------------------
 * Function declarations
 *------------------------------------------------------------------------------
 */

// allocator.h functions
platform_status
rc_allocator_alloc(rc_allocator *al, uint64 *addr, page_type type);

platform_status
rc_allocator_alloc_virtual(allocator *a, uint64 *addr, page_type type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_alloc(al, addr, type);
}

uint8
rc_allocator_inc_ref(rc_allocator *al, uint64 addr);

uint8
rc_allocator_inc_ref_virtual(allocator *a, uint64 addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_inc_ref(al, addr);
}

uint8
rc_allocator_dec_ref(rc_allocator *al, uint64 addr, page_type type);

uint8
rc_allocator_dec_ref_virtual(allocator *a, uint64 addr, page_type type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_dec_ref(al, addr, type);
}

uint8
rc_allocator_get_ref(rc_allocator *al, uint64 addr);

uint8
rc_allocator_get_ref_virtual(allocator *a, uint64 addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_get_ref(al, addr);
}

platform_status
rc_allocator_get_super_addr(rc_allocator     *al,
                            allocator_root_id spl_id,
                            uint64           *addr);

platform_status
rc_allocator_get_super_addr_virtual(allocator        *a,
                                    allocator_root_id spl_id,
                                    uint64           *addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_get_super_addr(al, spl_id, addr);
}

platform_status
rc_allocator_alloc_super_addr(rc_allocator     *al,
                              allocator_root_id spl_id,
                              uint64           *addr);

platform_status
rc_allocator_alloc_super_addr_virtual(allocator        *a,
                                      allocator_root_id spl_id,
                                      uint64           *addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_alloc_super_addr(al, spl_id, addr);
}

void
rc_allocator_remove_super_addr(rc_allocator *al, allocator_root_id spl_id);

void
rc_allocator_remove_super_addr_virtual(allocator *a, allocator_root_id spl_id)
{
   rc_allocator *al = (rc_allocator *)a;
   rc_allocator_remove_super_addr(al, spl_id);
}

uint64
rc_allocator_in_use(rc_allocator *al);

uint64
rc_allocator_in_use_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_in_use(al);
}

uint64
rc_allocator_get_capacity(rc_allocator *al);

uint64
rc_allocator_get_capacity_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_get_capacity(al);
}

void
rc_allocator_assert_noleaks(rc_allocator *al);

void
rc_allocator_assert_noleaks_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   rc_allocator_assert_noleaks(al);
}

void
rc_allocator_print_stats(rc_allocator *al);

void
rc_allocator_print_stats_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   rc_allocator_print_stats(al);
}

void
rc_allocator_print_allocated(rc_allocator *al);

void
rc_allocator_print_allocated_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   rc_allocator_print_allocated(al);
}

const static allocator_ops rc_allocator_ops = {
   .alloc             = rc_allocator_alloc_virtual,
   .inc_ref           = rc_allocator_inc_ref_virtual,
   .dec_ref           = rc_allocator_dec_ref_virtual,
   .get_ref           = rc_allocator_get_ref_virtual,
   .get_super_addr    = rc_allocator_get_super_addr_virtual,
   .alloc_super_addr  = rc_allocator_alloc_super_addr_virtual,
   .remove_super_addr = rc_allocator_remove_super_addr_virtual,
   .get_capacity      = rc_allocator_get_capacity_virtual,
   .assert_noleaks    = rc_allocator_assert_noleaks_virtual,
   .print_stats       = rc_allocator_print_stats_virtual,
   .print_allocated   = rc_allocator_print_allocated_virtual,
};

/*
 * Helper methods
 */
/*
 * Is page address 'base_addr' a valid extent address? I.e. it is the address
 * of the 1st page in an extent.
 */
__attribute__((unused)) static inline bool
rc_allocator_valid_extent_addr(rc_allocator *al, uint64 base_addr)
{
   return ((base_addr % al->cfg->io_cfg->extent_size) == 0);
}

/*
 * Convert page-address to the extent number of extent containing this page.
 * Returns the index into the allocated extents reference count array.
 * This function can be used on any page-address to map it to the holding
 * extent's number. 'addr' need not be just the base_addr; i.e. the address
 * of the 1st page in an extent.
 */
static inline uint64
rc_allocator_extent_number(rc_allocator *al, uint64 addr)
{
   return (addr / al->cfg->io_cfg->extent_size);
}

static platform_status
rc_allocator_init_meta_page(rc_allocator *al)
{
   /*
    * To make it easier to do aligned i/o's we allocate the meta page to
    * always be page size. In the future we can use the remaining space
    * for some other reserved information we may want to persist as part
    * of the meta page.
    */
   platform_assert(sizeof(rc_allocator_meta_page)
                   <= al->cfg->io_cfg->page_size);
   /*
    * Ensure that the meta page and  all the super blocks will fit in one
    * extent.
    */
   platform_assert((1 + RC_ALLOCATOR_MAX_ROOT_IDS) * al->cfg->io_cfg->page_size
                   <= al->cfg->io_cfg->extent_size);

   al->meta_page = TYPED_ALIGNED_ZALLOC(al->heap_id,
                                        al->cfg->io_cfg->page_size,
                                        al->meta_page,
                                        al->cfg->io_cfg->page_size);
   if (al->meta_page == NULL) {
      return STATUS_NO_MEMORY;
   }

   memset(al->meta_page->splinters,
          INVALID_ALLOCATOR_ROOT_ID,
          sizeof(al->meta_page->splinters));

   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * rc_allocator_config_init --
 *
 *      Initialize rc_allocator config values
 *-----------------------------------------------------------------------------
 */
void
rc_allocator_config_init(rc_allocator_config *allocator_cfg,
                         io_config           *io_cfg,
                         uint64               capacity)
{
   ZERO_CONTENTS(allocator_cfg);

   allocator_cfg->io_cfg          = io_cfg;
   allocator_cfg->capacity        = capacity;
   allocator_cfg->page_capacity   = capacity / io_cfg->page_size;
   allocator_cfg->extent_capacity = capacity / io_cfg->extent_size;
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_valid_config() --
 *
 * Do minimal validation of RC-allocator cofiguration.
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_valid_config(rc_allocator_config *cfg)
{
   platform_status rc = STATUS_OK;
   rc                 = laio_config_valid(cfg->io_cfg);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (cfg->capacity == 0) {
      platform_error_log("Configured disk size %lu bytes is invalid.\n",
                         cfg->capacity);
      return STATUS_BAD_PARAM;
   }
   if (cfg->extent_capacity == 0) {
      platform_error_log("Configured extent capacity %lu bytes is invalid.\n",
                         cfg->extent_capacity);
      return STATUS_BAD_PARAM;
   }

   // Assert: Disk size == (page-size * #-of-pages)
   if (cfg->capacity != (cfg->io_cfg->page_size * cfg->page_capacity)) {
      platform_error_log("Configured disk size, %lu bytes, is not an integral"
                         " multiple of page capacity, %lu pages"
                         ", for page size of %lu bytes.\n",
                         cfg->capacity,
                         cfg->page_capacity,
                         cfg->io_cfg->page_size);
      return STATUS_BAD_PARAM;
   }

   // Assert: Disk size == (extent-size * #-of-extents)
   if (cfg->capacity != (cfg->io_cfg->extent_size * cfg->extent_capacity)) {
      platform_error_log("Configured disk size, %lu bytes, is not an integral"
                         " multiple of extent capacity, %lu extents"
                         ", for extent size of %lu bytes.\n",
                         cfg->capacity,
                         cfg->extent_capacity,
                         cfg->io_cfg->extent_size);
      return STATUS_BAD_PARAM;
   }
   return rc;
}


/*
 *----------------------------------------------------------------------
 * rc_allocator_[de]init --
 *
 *      [de]initialize an allocator
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_init(rc_allocator        *al,
                  rc_allocator_config *cfg,
                  io_handle           *io,
                  platform_heap_handle hh,
                  platform_heap_id     hid,
                  platform_module_id   mid)
{
   uint64          rc_extent_count;
   uint64          addr;
   platform_status rc;
   platform_assert(al != NULL);
   ZERO_CONTENTS(al);
   al->super.ops   = &rc_allocator_ops;
   al->cfg         = cfg;
   al->io          = io;
   al->heap_handle = hh;
   al->heap_id     = hid;

   rc = rc_allocator_valid_config(cfg);
   if (!SUCCESS(rc)) {
      return rc;
   }

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
   buffer_size        = ROUNDUP(buffer_size, cfg->io_cfg->page_size);
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
   allocator_alloc(&al->super, &addr, PAGE_TYPE_SUPERBLOCK);
   // super block extent should always start from address 0.
   platform_assert(addr == RC_ALLOCATOR_BASE_OFFSET);

   /*
    * Allocate room for the ref counts, use same rounded up size used in buffer
    * creation.
    */
   rc_extent_count = (buffer_size + al->cfg->io_cfg->extent_size - 1)
                     / al->cfg->io_cfg->extent_size;
   for (uint64 i = 0; i < rc_extent_count; i++) {
      allocator_alloc(&al->super, &addr, PAGE_TYPE_SUPERBLOCK);
      platform_assert(addr == cfg->io_cfg->extent_size * (i + 1));
   }

   return STATUS_OK;
}

void
rc_allocator_deinit(rc_allocator *al)
{
   platform_buffer_destroy(al->bh);
   al->ref_count = NULL;
   platform_mutex_destroy(&al->lock);
   platform_free(al->heap_id, al->meta_page);
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_{mount,unmount} --
 *
 *      Loads the file system from disk
 *      Write the file system to disk
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_mount(rc_allocator        *al,
                   rc_allocator_config *cfg,
                   io_handle           *io,
                   platform_heap_handle hh,
                   platform_heap_id     hid,
                   platform_module_id   mid)
{
   platform_status status;

   platform_assert(al != NULL);
   ZERO_CONTENTS(al);
   al->super.ops   = &rc_allocator_ops;
   al->cfg         = cfg;
   al->io          = io;
   al->heap_handle = hh;
   al->heap_id     = hid;

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

   platform_assert(cfg->io_cfg->page_size % 4096 == 0);
   platform_assert(cfg->capacity
                   == cfg->io_cfg->extent_size * cfg->extent_capacity);
   platform_assert(cfg->capacity
                   == cfg->io_cfg->page_size * cfg->page_capacity);

   uint32 buffer_size = cfg->extent_capacity * sizeof(uint8);
   buffer_size        = ROUNDUP(buffer_size, cfg->io_cfg->page_size);
   al->bh = platform_buffer_create(buffer_size, al->heap_handle, mid);
   platform_assert(al->bh != NULL);
   al->ref_count = platform_buffer_getaddr(al->bh);

   // load the meta page from disk.
   status = io_read(
      io, al->meta_page, al->cfg->io_cfg->page_size, RC_ALLOCATOR_BASE_OFFSET);
   platform_assert_status_ok(status);
   // validate the checksum of the meta page.
   checksum128 currChecksum =
      platform_checksum128(al->meta_page,
                           sizeof(al->meta_page->splinters),
                           RC_ALLOCATOR_META_PAGE_CSUM_SEED);
   if (!platform_checksum_is_equal(al->meta_page->checksum, currChecksum)) {
      platform_assert(0, "Corrupt Meta Page upon mount");
   }

   // load the ref counts from disk.
   uint32 io_size =
      ROUNDUP(al->cfg->extent_capacity, al->cfg->io_cfg->page_size);
   status = io_read(io, al->ref_count, io_size, cfg->io_cfg->extent_size);
   platform_assert(SUCCESS(status),
                   "io_read() to load ref counts from disk failed:"
                   " ref_count=%p, expected io_size=%u, extent_size=%lu."
                   " Read %d bytes.\n",
                   al->ref_count,
                   io_size,
                   cfg->io_cfg->extent_size,
                   io->nbytes_rw);

   for (uint64 i = 0; i < al->cfg->extent_capacity; i++) {
      if (al->ref_count[i] != 0) {
         al->stats.curr_allocated++;
      }
   }
   platform_default_log(
      "Allocated %lu extents at mount: (%lu MiB)\n",
      al->stats.curr_allocated,
      B_TO_MiB(al->stats.curr_allocated * cfg->io_cfg->extent_size));
   return STATUS_OK;
}


void
rc_allocator_unmount(rc_allocator *al)
{
   platform_status status;

   platform_default_log(
      "Allocated at unmount: %lu MiB\n",
      B_TO_MiB(al->stats.curr_allocated * al->cfg->io_cfg->extent_size));

   // persist the ref counts upon unmount.
   uint32 io_size =
      ROUNDUP(al->cfg->extent_capacity, al->cfg->io_cfg->page_size);
   status =
      io_write(al->io, al->ref_count, io_size, al->cfg->io_cfg->extent_size);
   platform_assert_status_ok(status);
   rc_allocator_deinit(al);
}


/*
 *----------------------------------------------------------------------
 * rc_allocator_[inc,dec,get]_ref --
 *
 *      Increments/decrements/fetches the ref count of the given address and
 *      returns the new one. If the ref_count goes to 0, then the extent is
 *      freed.
 *----------------------------------------------------------------------
 */
uint8
rc_allocator_inc_ref(rc_allocator *al, uint64 addr)
{
   debug_assert(rc_allocator_valid_extent_addr(al, addr));

   uint64 extent_no = addr / al->cfg->io_cfg->extent_size;
   debug_assert(extent_no < al->cfg->extent_capacity);

   uint8 ref_count = __sync_add_and_fetch(&al->ref_count[extent_no], 1);
   platform_assert(ref_count != 1 && ref_count != 0);
   if (SHOULD_TRACE(addr)) {
      platform_default_log("rc_allocator_inc_ref(%lu): %d -> %d\n",
                           addr,
                           ref_count,
                           ref_count + 1);
   }
   return ref_count;
}

uint8
rc_allocator_dec_ref(rc_allocator *al, uint64 addr, page_type type)
{
   debug_assert(rc_allocator_valid_extent_addr(al, addr));

   uint64 extent_no = addr / al->cfg->io_cfg->extent_size;
   debug_assert(extent_no < al->cfg->extent_capacity);

   uint8 ref_count = __sync_sub_and_fetch(&al->ref_count[extent_no], 1);
   platform_assert(ref_count != UINT8_MAX);
   if (ref_count == 0) {
      platform_assert(type != PAGE_TYPE_INVALID);
      __sync_sub_and_fetch(&al->stats.curr_allocated, 1);
      __sync_add_and_fetch(&al->stats.extent_deallocs[type], 1);
   }
   if (SHOULD_TRACE(addr)) {
      platform_default_log("rc_allocator_dec_ref(%lu): %d -> %d\n",
                           addr,
                           ref_count,
                           ref_count - 1);
   }
   return ref_count;
}

uint8
rc_allocator_get_ref(rc_allocator *al, uint64 addr)
{
   uint64 extent_no;

   debug_assert(rc_allocator_valid_extent_addr(al, addr));
   extent_no = rc_allocator_extent_number(al, addr);
   debug_assert(extent_no < al->cfg->extent_capacity);
   return al->ref_count[extent_no];
}


/*
 *----------------------------------------------------------------------
 * rc_allocator_get_[capacity,super_addr] --
 *
 *      returns the struct/parameter
 *----------------------------------------------------------------------
 */
uint64
rc_allocator_get_capacity(rc_allocator *al)
{
   return al->cfg->capacity;
}

platform_status
rc_allocator_get_super_addr(rc_allocator     *al,
                            allocator_root_id allocator_root_id,
                            uint64           *addr)
{
   platform_status status = STATUS_NOT_FOUND;

   platform_mutex_lock(&al->lock);
   for (uint8 idx = 0; idx < RC_ALLOCATOR_MAX_ROOT_IDS; idx++) {
      if (al->meta_page->splinters[idx] == allocator_root_id) {
         // have already seen this table before, return existing addr.
         *addr  = (1 + idx) * al->cfg->io_cfg->page_size;
         status = STATUS_OK;
         break;
      }
   }

   platform_mutex_unlock(&al->lock);
   return status;
}

platform_status
rc_allocator_alloc_super_addr(rc_allocator     *al,
                              allocator_root_id allocator_root_id,
                              uint64           *addr)
{
   platform_status status = STATUS_NOT_FOUND;

   platform_mutex_lock(&al->lock);
   for (uint8 idx = 0; idx < RC_ALLOCATOR_MAX_ROOT_IDS; idx++) {
      if (al->meta_page->splinters[idx] == INVALID_ALLOCATOR_ROOT_ID) {
         // assign the first available slot and update the on disk metadata.
         al->meta_page->splinters[idx] = allocator_root_id;
         *addr                         = (1 + idx) * al->cfg->io_cfg->page_size;
         al->meta_page->checksum =
            platform_checksum128(al->meta_page,
                                 sizeof(al->meta_page->splinters),
                                 RC_ALLOCATOR_META_PAGE_CSUM_SEED);
         platform_status io_status = io_write(al->io,
                                              al->meta_page,
                                              al->cfg->io_cfg->page_size,
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
rc_allocator_remove_super_addr(rc_allocator     *al,
                               allocator_root_id allocator_root_id)
{
   platform_mutex_lock(&al->lock);

   for (uint8 idx = 0; idx < RC_ALLOCATOR_MAX_ROOT_IDS; idx++) {
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
         platform_status status = io_write(al->io,
                                           al->meta_page,
                                           al->cfg->io_cfg->page_size,
                                           RC_ALLOCATOR_BASE_OFFSET);
         platform_assert_status_ok(status);
         platform_mutex_unlock(&al->lock);
         return;
      }
   }

   platform_mutex_unlock(&al->lock);
   // Couldn't find the splinter id in the meta page.
   platform_assert(0, "Couldn't find existing splinter table in meta page");
}

uint64
rc_allocator_extent_size(rc_allocator *al)
{
   return al->cfg->io_cfg->extent_size;
}

uint64
rc_allocator_page_size(rc_allocator *al)
{
   return al->cfg->io_cfg->page_size;
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_alloc--
 *
 *      Allocate an extent
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_alloc(rc_allocator *al,   // IN
                   uint64       *addr, // OUT
                   page_type     type)     // IN
{
   uint64 first_hand = al->hand % al->cfg->extent_capacity;
   uint64 hand;
   bool   extent_is_free = FALSE;

   do {
      hand = __sync_fetch_and_add(&al->hand, 1) % al->cfg->extent_capacity;
      if (al->ref_count[hand] == 0)
         extent_is_free =
            __sync_bool_compare_and_swap(&al->ref_count[hand], 0, 2);
   } while (!extent_is_free
            && (hand + 1) % al->cfg->extent_capacity != first_hand);
   if (!extent_is_free) {
      platform_default_log(
         "Out of Space, while allocating an extent of type=%d (%s):"
         " allocated %lu out of %lu extents.\n",
         type,
         page_type_str[type],
         al->stats.curr_allocated,
         al->cfg->extent_capacity);
      return STATUS_NO_SPACE;
   }
   int64 curr_allocated = __sync_add_and_fetch(&al->stats.curr_allocated, 1);
   int64 max_allocated  = al->stats.max_allocated;
   while (curr_allocated > max_allocated) {
      __sync_bool_compare_and_swap(
         &al->stats.max_allocated, max_allocated, curr_allocated);
      max_allocated = al->stats.max_allocated;
   }
   __sync_add_and_fetch(&al->stats.extent_allocs[type], 1);
   *addr = hand * al->cfg->io_cfg->extent_size;
   if (SHOULD_TRACE(*addr)) {
      platform_default_log(
         "rc_allocator_alloc_extent %12lu (%s)\n", *addr, page_type_str[type]);
   }

   return STATUS_OK;
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_in_use --
 *
 *      Returns the number of extents currently allocated
 *----------------------------------------------------------------------
 */
uint64
rc_allocator_in_use(rc_allocator *al)
{
   return al->stats.curr_allocated;
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_assert_noleaks --
 *
 *      Asserts that the allocations of each type are completely matched by
 *      deallocations by operations that do something like create / destroy
 *      of objects. Primitive function to do some basic cross-checking of
 *      these operations.
 *----------------------------------------------------------------------
 */
void
rc_allocator_assert_noleaks(rc_allocator *al)
{
   for (page_type type = PAGE_TYPE_FIRST; type < NUM_PAGE_TYPES; type++) {
      // Log pages and super-block page are never deallocated.
      if ((type == PAGE_TYPE_LOG) || (type == PAGE_TYPE_SUPERBLOCK)) {
         continue;
      }
      if (al->stats.extent_allocs[type] != al->stats.extent_deallocs[type]) {
         platform_default_log("assert_noleaks: leak found\n");
         platform_default_log("\n");
         rc_allocator_print_stats(al);
         rc_allocator_print_allocated(al);
         platform_assert(0);
      }
   }
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_print_stats() --
 *
 *      Prints basic statistics about the allocator state.
 *
 *      Max allocations, and page type stats are since last mount.
 *----------------------------------------------------------------------
 */
void
rc_allocator_print_stats(rc_allocator *al)
{
   int64 divider = GiB / al->cfg->io_cfg->extent_size;
   platform_default_log(
      "----------------------------------------------------------------\n");
   platform_default_log(
      "| Allocator Stats                                              |\n");
   platform_default_log(
      "|--------------------------------------------------------------|\n");
   uint64 curr_gib = al->stats.curr_allocated / divider;
   platform_default_log(
      "| Currently Allocated: %12lu extents (%4luGiB)          |\n",
      al->stats.curr_allocated,
      curr_gib);
   uint64 max_gib = al->stats.max_allocated / divider;
   platform_default_log(
      "| Max Allocated:       %12lu extents (%4luGiB)          |\n",
      al->stats.max_allocated,
      max_gib);
   platform_default_log(
      "|--------------------------------------------------------------|\n");
   platform_default_log(
      "| Page Type | Allocations | Deallocations | Footprint (bytes)  |\n");
   platform_default_log(
      "|--------------------------------------------------------------|\n");
   int64 exp_allocated_count = 0;
   for (page_type type = PAGE_TYPE_FIRST; type < NUM_PAGE_TYPES; type++) {
      const char *str           = page_type_str[type];
      int64       allocs        = al->stats.extent_allocs[type];
      int64       deallocs      = al->stats.extent_deallocs[type];
      int64       footprint     = allocs - deallocs;
      int64       footprint_gib = footprint / divider;

      exp_allocated_count += footprint;

      platform_default_log("| %-10s | %11ld | %13ld | %8ld (%4ldGiB) |\n",
                           str,
                           allocs,
                           deallocs,
                           footprint,
                           footprint_gib);
   }
   platform_default_log(
      "----------------------------------------------------------------\n");
   platform_default_log("Expected allocation count from footprint = %ld\n",
                        exp_allocated_count);
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_print_allocated() --
 *
 *      Prints the base addresses of all allocated extents to the default
 *      log handle.
 *----------------------------------------------------------------------
 */
void
rc_allocator_print_allocated(rc_allocator *al)
{
   uint64 i;
   uint8  ref;
   uint64 nallocated = al->stats.curr_allocated;

   // For more than a few allocated extents, print enclosing { } tags.
   bool print_curly = (nallocated > 20);

   platform_default_log(
      "Allocated extents: %lu\n%s", nallocated, (print_curly ? "{\n" : ""));
   platform_default_log("   Index  ExtentAddr  Count\n");

   // # of extents with non-zero referenced page-count found
   uint64 found = 0;

   for (i = 0; i < al->cfg->extent_capacity; i++) {
      ref = al->ref_count[i];
      if (ref != 0) {
         found++;
         uint64 ext_addr = (i * al->cfg->io_cfg->extent_size);
         platform_default_log("%8lu %12lu     %u\n", i, ext_addr, ref);
      }
   }
   platform_default_log("%sFound %lu extents with allocated pages.\n",
                        (print_curly ? "}\n" : ""),
                        found);
}
