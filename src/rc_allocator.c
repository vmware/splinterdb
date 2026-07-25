// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *------------------------------------------------------------------------------
 * rc_allocator.c --
 *
 *     This file contains the implementation of the ref count allocator.
 *------------------------------------------------------------------------------
 */

#include "rc_allocator.h"
#include "platform_io.h"
#include "platform_hash.h"
#include "platform_buffer.h"
#include "platform_mutex.h"
#include "platform_typed_alloc.h"
#include "poison.h"

/*
 * The refcount map is the allocator's only durable structure.  It lives in a
 * fixed run of extents starting at extent 1 (extent 0 is reserved for the
 * superblock).  Whether the persisted map is trustworthy is recorded by the
 * superblock (allocation_state_addr), which the caller consults to choose a
 * clean vs recovery mount -- the allocator itself no longer keeps a
 * clean-state record or a bootstrap meta page.
 */

/* Extent 0 holds the superblock; the refcount map begins at extent 1. */
#define RC_ALLOCATOR_REFCOUNT_MAP_EXTENT (1)

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
 * Helper methods
 */
/*
 * Is page address 'base_addr' a valid extent address? I.e. it is the address
 * of the 1st page in an extent.
 */
debug_only static inline bool32
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

static uint64
rc_allocator_refcount_buffer_size(const allocator_config *cfg)
{
   uint64 buffer_size = cfg->extent_capacity * sizeof(refcount);
   return ROUNDUP(buffer_size, cfg->io_cfg->page_size);
}

static uint64
rc_allocator_refcount_extent_count(const allocator_config *cfg)
{
   uint64 buffer_size = rc_allocator_refcount_buffer_size(cfg);
   return (buffer_size + cfg->io_cfg->extent_size - 1)
          / cfg->io_cfg->extent_size;
}

static uint64
rc_allocator_reserved_extent_count(const allocator_config *cfg)
{
   /* Extent 0 (superblock) + the refcount-map extents.  No clean-state
    * extents: superblock allocation_state_addr now records map validity. */
   return 1 + rc_allocator_refcount_extent_count(cfg);
}

static void
rc_allocator_record_allocated_extent(rc_allocator *al)
{
   int64 curr_allocated = __sync_add_and_fetch(&al->stats.curr_allocated, 1);
   int64 max_allocated  = al->stats.max_allocated;
   while (curr_allocated > max_allocated) {
      __sync_bool_compare_and_swap(
         &al->stats.max_allocated, max_allocated, curr_allocated);
      max_allocated = al->stats.max_allocated;
   }
}

static platform_status
rc_allocator_recovery_initialize_refcounts(rc_allocator *al)
{
   uint64 reserved_extent_count = rc_allocator_reserved_extent_count(al->cfg);

   if (reserved_extent_count > al->cfg->extent_capacity) {
      platform_error_log("Allocator needs %lu reserved extents, but its "
                         "configured capacity is only %lu extents.\n",
                         reserved_extent_count,
                         al->cfg->extent_capacity);
      return STATUS_BAD_PARAM;
   }

   memset(al->ref_count, 0, rc_allocator_refcount_buffer_size(al->cfg));

   /*
    * Extent 0 holds the superblock; the refcount map begins at extent 1.
    * Reserve both so recovery never hands them out.
    */
   for (uint64 extent_no = 0; extent_no < reserved_extent_count; extent_no++) {
      platform_assert(al->ref_count[extent_no] == AL_FREE);
      al->ref_count[extent_no] = AL_ONE_REF;
      rc_allocator_record_allocated_extent(al);
   }

   /* Match a regular mount: the first post-recovery allocation scans at 0. */
   al->hand = 0;
   return STATUS_OK;
}

static bool32
rc_allocator_recovery_extent_is_reserved(const rc_allocator *al,
                                         uint64              extent_no)
{
   return extent_no < rc_allocator_reserved_extent_count(al->cfg);
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_valid_config() --
 *
 * Do minimal validation of RC-allocator cofiguration.
 *
 * TODO(robj): Now that config is in generic allocator.h, this validator
 * should probably move into allocator.c.
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_valid_config(allocator_config *cfg)
{
   platform_status rc = STATUS_OK;
   rc                 = io_config_valid(cfg->io_cfg);
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

   if (rc_allocator_reserved_extent_count(cfg) > cfg->extent_capacity) {
      platform_error_log("Configured allocator has %lu extents, but needs %lu "
                         "reserved extents for metadata, refcounts, and clean "
                         "state.\n",
                         cfg->extent_capacity,
                         rc_allocator_reserved_extent_count(cfg));
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
 * rc_allocator_load_refcounts --
 *
 *      Populate the refcount map of an attached allocator (see
 *      allocator_load_refcounts()).  rebuild == FALSE loads the trusted
 *      persisted map from its fixed reserved location; rebuild == TRUE
 *      initializes an empty map that reserves only the fixed extents and enters
 *      recovery mode.  Geometry and clean-vs-rebuild validity live in the
 *      superblock, which the caller has already read; the allocator only
 *      touches its own refcount map here.
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_load_refcounts(rc_allocator *al, bool32 rebuild)
{
   platform_assert(al != NULL);
   platform_assert(al->ref_count != NULL);

   if (rebuild) {
      platform_status status = rc_allocator_recovery_initialize_refcounts(al);
      if (!SUCCESS(status)) {
         return status;
      }
      return STATUS_OK;
   }

   // Load the trusted refcount map from its fixed reserved location.
   uint64          buffer_size = rc_allocator_refcount_buffer_size(al->cfg);
   platform_status status =
      io_read(al->io,
              al->ref_count,
              buffer_size,
              RC_ALLOCATOR_REFCOUNT_MAP_EXTENT * al->cfg->io_cfg->extent_size);
   if (!SUCCESS(status)) {
      return status;
   }

   // Compute curr_allocated authoritatively from the loaded map (set, not
   // accumulate), so this is correct even if the allocator's stats were not
   // freshly zeroed.
   al->stats.curr_allocated = 0;
   for (uint64 i = 0; i < al->cfg->extent_capacity; i++) {
      if (al->ref_count[i] != 0) {
         al->stats.curr_allocated++;
      }
   }
   al->map_is_valid = TRUE;
   return STATUS_OK;
}

platform_status
rc_allocator_recovery_record_reference(rc_allocator *al,
                                       uint64        extent_addr,
                                       page_type     type)
{
   if (al->map_is_valid) {
      platform_error_log(
         "Cannot acquire allocator extent once the refcount map is valid.\n");
      return STATUS_INVALID_STATE;
   }

   uint64 extent_size = al->cfg->io_cfg->extent_size;
   if (extent_addr >= al->cfg->capacity || extent_addr % extent_size != 0) {
      platform_error_log("Invalid allocator recovery extent address %lu.\n",
                         extent_addr);
      return STATUS_BAD_PARAM;
   }

   uint64 extent_no = rc_allocator_extent_number(al, extent_addr);
   platform_assert(extent_no < al->cfg->extent_capacity);
   if (rc_allocator_recovery_extent_is_reserved(al, extent_no)) {
      platform_error_log("Cannot rebuild ownership of reserved allocator "
                         "extent %lu.\n",
                         extent_no);
      return STATUS_BAD_PARAM;
   }


   refcount new_refcount = __sync_add_and_fetch(&al->ref_count[extent_no], 1);
   if (new_refcount == 0) {
      platform_error_log("Allocator recovery refcount overflow for extent "
                         "%lu.\n",
                         extent_no);
      return STATUS_LIMIT_EXCEEDED;
   } else if (new_refcount == AL_NO_REFS) {
      new_refcount = __sync_add_and_fetch(&al->ref_count[extent_no], 1);
      if (new_refcount == 0) {
         platform_error_log("Allocator recovery refcount overflow for extent "
                            "%lu.\n",
                            extent_no);
         return STATUS_LIMIT_EXCEEDED;
      }
      platform_assert(type != PAGE_TYPE_INVALID);
      rc_allocator_record_allocated_extent(al);
      __sync_add_and_fetch(&al->stats.extent_allocs[type], 1);
   }

   return STATUS_OK;
}

void
rc_allocator_rebuild_finish(rc_allocator *al)
{
   platform_assert(al != NULL);
   platform_assert(!al->map_is_valid);

   /*
    * Intentionally no I/O here.  A rebuilt map is durable only after a later
    * rc_allocator_persist(); another crash before then simply rebuilds it
    * again.
    */
   al->map_is_valid = TRUE;
}


/*
 *----------------------------------------------------------------------
 * rc_allocator_persist --
 *
 *      Write the refcount map to its fixed reserved location and make it
 *      durable.  Returns the base address of the persisted map in *state_addr,
 *      which the caller records as the superblock's allocation_state_addr only
 *      after this returns -- so the "map is trustworthy" flag never becomes
 *      durable before the map itself.  Does not tear down the allocator; the
 *      caller invokes rc_allocator_deinit() separately.
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_persist(rc_allocator *al, uint64 *state_addr)
{
   platform_assert(al->map_is_valid);

   uint64 map_addr =
      RC_ALLOCATOR_REFCOUNT_MAP_EXTENT * al->cfg->io_cfg->extent_size;
   uint64 buffer_size = rc_allocator_refcount_buffer_size(al->cfg);
   uint32 io_size     = ROUNDUP(buffer_size, al->cfg->io_cfg->page_size);

   platform_status status = io_write(al->io, al->ref_count, io_size, map_addr);
   if (!SUCCESS(status)) {
      return status;
   }
   status = io_durable_barrier(al->io);
   if (!SUCCESS(status)) {
      return status;
   }
   if (state_addr != NULL) {
      *state_addr = map_addr;
   }
   return STATUS_OK;
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
refcount
rc_allocator_inc_ref(rc_allocator *al, uint64 addr)
{
   debug_assert(rc_allocator_valid_extent_addr(al, addr));

   uint64 extent_no = addr / al->cfg->io_cfg->extent_size;
   debug_assert(extent_no < al->cfg->extent_capacity);

   refcount ref_count = __sync_add_and_fetch(&al->ref_count[extent_no], 1);
   platform_assert(ref_count != 1 && ref_count != 0);
   if (SHOULD_TRACE(addr)) {
      platform_default_log("rc_allocator_inc_ref(%lu): %d -> %d\n",
                           addr,
                           ref_count,
                           ref_count + 1);
   }
   return ref_count;
}

refcount
rc_allocator_dec_ref(rc_allocator *al, uint64 addr, page_type type)
{
   debug_assert(rc_allocator_valid_extent_addr(al, addr));

   uint64 extent_no = addr / al->cfg->io_cfg->extent_size;
   debug_assert(extent_no < al->cfg->extent_capacity);

   refcount ref_count = __sync_sub_and_fetch(&al->ref_count[extent_no], 1);

   // We should have decremented the ref-count. If it rolls-over and
   // goes back to this value, it means the original value was 0. That
   // indicates that we are decrementing a ref-count for an extent where
   // no page was previously allocated.
   platform_assert(ref_count != (refcount)(-1),
                   "extent_no=%lu, ref_count=%d (0x%x)\n",
                   extent_no,
                   ref_count,
                   ref_count);

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

refcount
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
 * rc_allocator_get_config--
 *
 *      Retrieve the allocator configuration.
 *----------------------------------------------------------------------
 */
allocator_config *
rc_allocator_get_config(rc_allocator *al)
{
   return al->cfg;
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
   bool32 extent_is_free = FALSE;

   do {
      hand = __sync_fetch_and_add(&al->hand, 1) % al->cfg->extent_capacity;
      if (al->ref_count[hand] == 0) {
         extent_is_free =
            __sync_bool_compare_and_swap(&al->ref_count[hand], 0, 2);
      }
   } while (!extent_is_free
            && (hand + 1) % al->cfg->extent_capacity != first_hand);

   // Error out if no extent is free; allocation fails.
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
   // clang-format off
   const char *dashes = "-------------------------------------------------------------------";
   platform_default_log("|%s|\n", dashes);
   platform_default_log("| Allocator Stats                                                   |\n");
   platform_default_log("|%s|\n", dashes);
   // clang-format on

   uint64 extent_size = al->cfg->io_cfg->extent_size; // bytes
   platform_default_log(
      "| Currently Allocated: %12lu extents %-14s          |\n",
      al->stats.curr_allocated,
      size_fmtstr("(%s)", (al->stats.curr_allocated * extent_size)));

   platform_default_log(
      "| Max Allocated:       %12lu extents %-14s          |\n",
      al->stats.max_allocated,
      size_fmtstr("(%s)", (al->stats.max_allocated * extent_size)));

   // clang-format off
   platform_default_log("|%s|\n", dashes);
   platform_default_log("| Page Type  | Allocations | Deallocations |      Footprint         |\n");
   platform_default_log("|            |      (Number of extents)    | # extents  (bytes)     |\n");
   platform_default_log("|%s|\n", dashes);
   // clang-format on

   int64 exp_allocated_count = 0;
   for (page_type type = PAGE_TYPE_FIRST; type < NUM_PAGE_TYPES; type++) {
      const char *str       = page_type_str[type];
      int64       allocs    = al->stats.extent_allocs[type];
      int64       deallocs  = al->stats.extent_deallocs[type];
      int64       footprint = allocs - deallocs;

      exp_allocated_count += footprint;

      platform_default_log("| %-10s | %11ld | %13ld | %8ld %14s|\n",
                           str,
                           allocs,
                           deallocs,
                           footprint,
                           size_fmtstr("(%s)", (footprint * extent_size)));
   }
   platform_default_log("|%s|\n", dashes);
   platform_default_log(
      "Expected count of extents in-use from footprint = %ld extents (%s)\n",
      exp_allocated_count,
      size_str(exp_allocated_count * extent_size));
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
   uint64   i;
   refcount ref;
   uint64   nallocated = al->stats.curr_allocated;

   // For more than a few allocated extents, print enclosing { } tags.
   bool32 print_curly = (nallocated > 20);

   platform_default_log(
      "Allocated extents: %lu\n%s", nallocated, (print_curly ? "{\n" : ""));
   platform_default_log("   Index  ExtentAddr  Count\n");

   // # of extents with non-zero referenced page-count found
   uint64 nextents_found = 0;
   uint64 extent_size    = al->cfg->io_cfg->extent_size;

   for (i = 0; i < al->cfg->extent_capacity; i++) {
      ref = al->ref_count[i];
      if (ref != 0) {
         nextents_found++;
         uint64 ext_addr = (i * extent_size);
         platform_default_log("%8lu %12lu     %u\n", i, ext_addr, ref);
      }
   }

   platform_default_log("%sFound %lu extents (%s) with allocated pages.\n",
                        (print_curly ? "}\n" : ""),
                        nextents_found,
                        size_str(nextents_found * extent_size));
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

// allocator.h functions

allocator_config *
rc_allocator_get_config_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_get_config(al);
}

platform_status
rc_allocator_alloc_virtual(allocator *a, uint64 *addr, page_type type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_alloc(al, addr, type);
}

refcount
rc_allocator_inc_ref_virtual(allocator *a, uint64 addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_inc_ref(al, addr);
}

refcount
rc_allocator_dec_ref_virtual(allocator *a, uint64 addr, page_type type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_dec_ref(al, addr, type);
}

refcount
rc_allocator_get_ref_virtual(allocator *a, uint64 addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_get_ref(al, addr);
}

platform_status
rc_allocator_recovery_record_reference_virtual(allocator *a,
                                               uint64     addr,
                                               page_type  type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_recovery_record_reference(al, addr, type);
}

platform_status
rc_allocator_load_refcounts_virtual(allocator *a, bool32 rebuild)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_load_refcounts(al, rebuild);
}

platform_status
rc_allocator_persist_virtual(allocator *a, uint64 *state_addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_persist(al, state_addr);
}


uint64
rc_allocator_in_use_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_in_use(al);
}

uint64
rc_allocator_get_capacity_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_get_capacity(al);
}

void
rc_allocator_assert_noleaks_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   rc_allocator_assert_noleaks(al);
}

void
rc_allocator_print_stats_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   rc_allocator_print_stats(al);
}

void
rc_allocator_print_allocated_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   rc_allocator_print_allocated(al);
}

const static allocator_ops rc_allocator_ops = {
   .get_config                = rc_allocator_get_config_virtual,
   .alloc                     = rc_allocator_alloc_virtual,
   .inc_ref                   = rc_allocator_inc_ref_virtual,
   .dec_ref                   = rc_allocator_dec_ref_virtual,
   .get_ref                   = rc_allocator_get_ref_virtual,
   .recovery_record_reference = rc_allocator_recovery_record_reference_virtual,
   .load_refcounts            = rc_allocator_load_refcounts_virtual,
   .persist                   = rc_allocator_persist_virtual,
   .in_use                    = rc_allocator_in_use_virtual,
   .get_capacity              = rc_allocator_get_capacity_virtual,
   .assert_noleaks            = rc_allocator_assert_noleaks_virtual,
   .print_stats               = rc_allocator_print_stats_virtual,
   .print_allocated           = rc_allocator_print_allocated_virtual,
};

/*
 *----------------------------------------------------------------------
 * rc_allocator_[de]init --
 *
 *      [de]initialize an allocator
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_init(rc_allocator      *al,
                  allocator_config  *cfg,
                  io_handle         *io,
                  platform_heap_id   hid,
                  platform_module_id mid)
{
   uint64          rc_extent_count;
   uint64          addr;
   platform_status rc;
   platform_assert(al != NULL);
   ZERO_CONTENTS(al);
   al->super.ops = &rc_allocator_ops;
   al->cfg       = cfg;
   al->io        = io;
   al->heap_id   = hid;

   rc = rc_allocator_valid_config(cfg);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = platform_mutex_init(&al->lock, mid, al->heap_id);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init mutex for the allocator\n");
      return rc;
   }
   // To ensure alignment always allocate in multiples of page size.
   uint64 buffer_size = rc_allocator_refcount_buffer_size(cfg);
   rc                 = platform_buffer_init(&al->bh, buffer_size);
   if (!SUCCESS(rc)) {
      platform_mutex_destroy(&al->lock);
      platform_error_log("Failed to create buffer for ref counts\n");
      return STATUS_NO_MEMORY;
   }
   al->ref_count = platform_buffer_getaddr(&al->bh);
   memset(al->ref_count, 0, buffer_size);

   // Reserve extent 0 for the superblock; the superblock module owns its
   // pages 0/1.
   allocator_alloc(&al->super, &addr, PAGE_TYPE_SUPERBLOCK);
   platform_assert(addr == 0);

   // Reserve the refcount-map extents (extent 1 .. rc_extent_count).
   rc_extent_count = rc_allocator_refcount_extent_count(cfg);
   for (uint64 i = 0; i < rc_extent_count; i++) {
      allocator_alloc(&al->super, &addr, PAGE_TYPE_SUPERBLOCK);
      platform_assert(addr == cfg->io_cfg->extent_size * (i + 1));
   }

   /*
    * A fresh map is synthesized correctly in memory -- there is nothing to
    * load, so it's trustworthy immediately, like a map that just finished a
    * rebuild.  It is not persisted here, though: the superblock that mkfs
    * writes records allocation_state_addr == 0, so a crash before a later
    * clean close enters rebuild recovery rather than trusting this in-memory
    * map.  The map is persisted only by rc_allocator_persist().
    */
   al->map_is_valid = TRUE;
   return STATUS_OK;
}

void
rc_allocator_deinit(rc_allocator *al)
{
   platform_buffer_deinit(&al->bh);
   al->ref_count = NULL;
   platform_mutex_destroy(&al->lock);
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_mount --
 *
 *      Attach an allocator to an existing device: initialize the in-memory
 *      structures and allocate the (zeroed) refcount buffer, but do NOT read
 *      the persisted map.  The caller populates the map exactly once via
 *      allocator_load_refcounts() -- loading the trusted map or initializing a
 *      rebuild -- so a rebuild never pays for a map read it would discard.
 *----------------------------------------------------------------------
 */
platform_status
rc_allocator_mount(rc_allocator      *al,
                   allocator_config  *cfg,
                   io_handle         *io,
                   platform_heap_id   hid,
                   platform_module_id mid)
{
   platform_status status;

   platform_assert(al != NULL);
   ZERO_CONTENTS(al);
   al->super.ops = &rc_allocator_ops;
   al->cfg       = cfg;
   al->io        = io;
   al->heap_id   = hid;

   status = platform_mutex_init(&al->lock, mid, al->heap_id);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to init mutex for the allocator\n");
      return status;
   }

   platform_assert(cfg->io_cfg->page_size % 4096 == 0);
   platform_assert(cfg->capacity
                   == cfg->io_cfg->extent_size * cfg->extent_capacity);
   platform_assert(cfg->capacity
                   == cfg->io_cfg->page_size * cfg->page_capacity);

   uint64 buffer_size = rc_allocator_refcount_buffer_size(cfg);
   status             = platform_buffer_init(&al->bh, buffer_size);
   if (!SUCCESS(status)) {
      platform_mutex_destroy(&al->lock);
      platform_error_log("Failed to create buffer to load ref counts\n");
      return STATUS_NO_MEMORY;
   }
   al->ref_count = platform_buffer_getaddr(&al->bh);
   memset(al->ref_count, 0, buffer_size);
   return STATUS_OK;
}
