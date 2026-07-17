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

#define RC_ALLOCATOR_META_PAGE_CSUM_SEED   (2718281828)
#define RC_ALLOCATOR_CLEAN_STATE_CSUM_SEED (2718281829)

#define RC_ALLOCATOR_FORMAT_MAGIC   (0x534442414C4C4F43ULL) // SDBALLOC
#define RC_ALLOCATOR_FORMAT_VERSION (1)

#define RC_ALLOCATOR_CLEAN_STATE_MAGIC   (0x534442434C45414EULL) // SDBCLEAN
#define RC_ALLOCATOR_CLEAN_STATE_VERSION (1)
#define RC_ALLOCATOR_CLEAN_STATE_SLOTS   (2)

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
 * A/B clean-state records live in two fixed extents after the refcount map.
 * They are deliberately separate from the sole allocator bootstrap page: a
 * torn state update must leave an older valid state record available for the
 * next mount or rebuild.
 */
typedef struct ONDISK rc_allocator_clean_state {
   uint64      magic;
   uint64      format_version;
   uint64      sequence;
   bool32      clean_shutdown;
   checksum128 checksum;
} rc_allocator_clean_state;

typedef struct rc_allocator_clean_states {
   rc_allocator_clean_state state[RC_ALLOCATOR_CLEAN_STATE_SLOTS];
   bool32                   valid[RC_ALLOCATOR_CLEAN_STATE_SLOTS];
   bool32                   have_newest;
   uint64                   newest_slot;
   bool32                   duplicate_sequence;
} rc_allocator_clean_states;

/*
 *------------------------------------------------------------------------------
 * Function declarations and virtual trampolines
 *------------------------------------------------------------------------------
 */

// allocator.h functions

allocator_config *
rc_allocator_get_config(rc_allocator *al);

allocator_config *
rc_allocator_get_config_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_get_config(al);
}

platform_status
rc_allocator_alloc(rc_allocator *al, uint64 *addr, page_type type);

platform_status
rc_allocator_alloc_virtual(allocator *a, uint64 *addr, page_type type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_alloc(al, addr, type);
}

refcount
rc_allocator_inc_ref(rc_allocator *al, uint64 addr);

refcount
rc_allocator_inc_ref_virtual(allocator *a, uint64 addr)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_inc_ref(al, addr);
}

refcount
rc_allocator_dec_ref(rc_allocator *al, uint64 addr, page_type type);

refcount
rc_allocator_dec_ref_virtual(allocator *a, uint64 addr, page_type type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_dec_ref(al, addr, type);
}

refcount
rc_allocator_get_ref(rc_allocator *al, uint64 addr);

refcount
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
   .get_config        = rc_allocator_get_config_virtual,
   .alloc             = rc_allocator_alloc_virtual,
   .inc_ref           = rc_allocator_inc_ref_virtual,
   .dec_ref           = rc_allocator_dec_ref_virtual,
   .get_ref           = rc_allocator_get_ref_virtual,
   .get_super_addr    = rc_allocator_get_super_addr_virtual,
   .alloc_super_addr  = rc_allocator_alloc_super_addr_virtual,
   .remove_super_addr = rc_allocator_remove_super_addr_virtual,
   .in_use            = rc_allocator_in_use_virtual,
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

static checksum128
rc_allocator_meta_page_checksum(const rc_allocator_meta_page *meta_page)
{
   return platform_checksum128(meta_page,
                               offsetof(rc_allocator_meta_page, checksum),
                               RC_ALLOCATOR_META_PAGE_CSUM_SEED);
}

static platform_status
rc_allocator_write_meta_page(rc_allocator *al)
{
   al->meta_page->checksum = rc_allocator_meta_page_checksum(al->meta_page);
   return io_write(al->io,
                   al->meta_page,
                   al->cfg->io_cfg->page_size,
                   RC_ALLOCATOR_BASE_OFFSET);
}

static disk_geometry
rc_allocator_config_get_disk_geometry(allocator_config *cfg)
{
   return (disk_geometry){
      .disk_size   = cfg->capacity,
      .page_size   = cfg->io_cfg->page_size,
      .extent_size = cfg->io_cfg->extent_size,
   };
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
rc_allocator_clean_state_extent_no(const allocator_config *cfg, uint64 slot)
{
   platform_assert(slot < RC_ALLOCATOR_CLEAN_STATE_SLOTS);
   return 1 + rc_allocator_refcount_extent_count(cfg) + slot;
}

static uint64
rc_allocator_clean_state_addr(const allocator_config *cfg, uint64 slot)
{
   return rc_allocator_clean_state_extent_no(cfg, slot)
          * cfg->io_cfg->extent_size;
}

static uint64
rc_allocator_reserved_extent_count(const allocator_config *cfg)
{
   return 1 + rc_allocator_refcount_extent_count(cfg)
          + RC_ALLOCATOR_CLEAN_STATE_SLOTS;
}

static checksum128
rc_allocator_clean_state_checksum(const rc_allocator_clean_state *state)
{
   return platform_checksum128(state,
                               offsetof(rc_allocator_clean_state, checksum),
                               RC_ALLOCATOR_CLEAN_STATE_CSUM_SEED);
}

static bool32
rc_allocator_clean_state_is_valid(const rc_allocator_clean_state *state)
{
   return state->magic == RC_ALLOCATOR_CLEAN_STATE_MAGIC
          && state->format_version == RC_ALLOCATOR_CLEAN_STATE_VERSION
          && state->sequence != 0
          && (state->clean_shutdown == FALSE || state->clean_shutdown == TRUE)
          && platform_checksum_is_equal(
             state->checksum, rc_allocator_clean_state_checksum(state));
}

static platform_status
rc_allocator_read_clean_state(rc_allocator             *al,
                              uint64                    slot,
                              rc_allocator_clean_state *state,
                              bool32                   *valid)
{
   platform_assert(slot < RC_ALLOCATOR_CLEAN_STATE_SLOTS);
   platform_assert(sizeof(*state) <= al->cfg->io_cfg->page_size);

   buffer_handle   buffer;
   platform_status rc =
      platform_buffer_init(&buffer, al->cfg->io_cfg->page_size);
   if (!SUCCESS(rc)) {
      return rc;
   }

   void *page = platform_buffer_getaddr(&buffer);
   rc = io_read(al->io,
                page,
                al->cfg->io_cfg->page_size,
                rc_allocator_clean_state_addr(al->cfg, slot));
   if (SUCCESS(rc)) {
      memcpy(state, page, sizeof(*state));
      *valid = rc_allocator_clean_state_is_valid(state);
   }

   platform_status deinit_rc = platform_buffer_deinit(&buffer);
   if (SUCCESS(rc) && !SUCCESS(deinit_rc)) {
      rc = deinit_rc;
   }
   return rc;
}

static platform_status
rc_allocator_write_clean_state(rc_allocator                   *al,
                               uint64                          slot,
                               const rc_allocator_clean_state *state,
                               bool32                          durable)
{
   platform_assert(slot < RC_ALLOCATOR_CLEAN_STATE_SLOTS);
   platform_assert(sizeof(*state) <= al->cfg->io_cfg->page_size);

   buffer_handle   buffer;
   platform_status rc =
      platform_buffer_init(&buffer, al->cfg->io_cfg->page_size);
   if (!SUCCESS(rc)) {
      return rc;
   }

   void *page = platform_buffer_getaddr(&buffer);
   memset(page, 0, al->cfg->io_cfg->page_size);
   memcpy(page, state, sizeof(*state));
   rc = io_write(al->io,
                 page,
                 al->cfg->io_cfg->page_size,
                 rc_allocator_clean_state_addr(al->cfg, slot));
   if (SUCCESS(rc) && durable) {
      rc = io_durable_barrier(al->io);
   }

   platform_status deinit_rc = platform_buffer_deinit(&buffer);
   if (SUCCESS(rc) && !SUCCESS(deinit_rc)) {
      rc = deinit_rc;
   }
   return rc;
}

static platform_status
rc_allocator_load_clean_states(rc_allocator              *al,
                               rc_allocator_clean_states *states)
{
   ZERO_CONTENTS(states);
   for (uint64 slot = 0; slot < RC_ALLOCATOR_CLEAN_STATE_SLOTS; slot++) {
      platform_status rc = rc_allocator_read_clean_state(
         al, slot, &states->state[slot], &states->valid[slot]);
      if (!SUCCESS(rc)) {
         return rc;
      }

      if (!states->valid[slot]) {
         continue;
      }
      if (!states->have_newest
          || states->state[states->newest_slot].sequence
                < states->state[slot].sequence)
      {
         states->have_newest = TRUE;
         states->newest_slot = slot;
      } else if (states->state[states->newest_slot].sequence
                 == states->state[slot].sequence)
      {
         states->duplicate_sequence = TRUE;
      }
   }
   return STATUS_OK;
}

static platform_status
rc_allocator_publish_clean_state(rc_allocator *al, bool32 clean_shutdown)
{
   rc_allocator_clean_states states;
   platform_status rc = rc_allocator_load_clean_states(al, &states);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (states.have_newest
       && states.state[states.newest_slot].sequence == UINT64_MAX)
   {
      return STATUS_LIMIT_EXCEEDED;
   }

   uint64 target_slot = states.have_newest ? states.newest_slot ^ 1 : 0;
   rc_allocator_clean_state state;
   ZERO_CONTENTS(&state);
   state.magic          = RC_ALLOCATOR_CLEAN_STATE_MAGIC;
   state.format_version = RC_ALLOCATOR_CLEAN_STATE_VERSION;
   state.sequence = states.have_newest
                       ? states.state[states.newest_slot].sequence + 1
                       : 1;
   state.clean_shutdown = clean_shutdown;
   state.checksum       = rc_allocator_clean_state_checksum(&state);
   return rc_allocator_write_clean_state(al, target_slot, &state, TRUE);
}

static platform_status
rc_allocator_initialize_clean_states(rc_allocator *al)
{
   for (uint64 slot = 0; slot < RC_ALLOCATOR_CLEAN_STATE_SLOTS; slot++) {
      rc_allocator_clean_state state;
      ZERO_CONTENTS(&state);
      state.magic          = RC_ALLOCATOR_CLEAN_STATE_MAGIC;
      state.format_version = RC_ALLOCATOR_CLEAN_STATE_VERSION;
      state.sequence       = slot + 1;
      state.clean_shutdown = FALSE;
      state.checksum       = rc_allocator_clean_state_checksum(&state);

      platform_status rc =
         rc_allocator_write_clean_state(al, slot, &state, FALSE);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return io_durable_barrier(al->io);
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

   memset(al->ref_count,
          0,
          rc_allocator_refcount_buffer_size(al->cfg));

   /*
    * Extent 0 contains both the allocator meta page and every fixed table
    * superblock.  The refcount table begins at extent 1; the two extents
    * after it hold alternating clean-state records.
    */
   for (uint64 extent_no = 0; extent_no < reserved_extent_count; extent_no++)
   {
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

static platform_status
rc_allocator_validate_disk_geometry(rc_allocator *al)
{
   disk_geometry geometry = al->meta_page->geometry;

   return rc_allocator_disk_geometry_matches_config(&geometry, al->cfg);
}

platform_status
rc_allocator_disk_geometry_matches_config(const disk_geometry    *geometry,
                                          const allocator_config *cfg)
{
   if (geometry->disk_size != cfg->capacity
       || geometry->page_size != cfg->io_cfg->page_size
       || geometry->extent_size != cfg->io_cfg->extent_size)
   {
      platform_error_log(
         "SplinterDB disk geometry does not match configuration: "
         "disk=(disk_size=%lu, page_size=%lu, extent_size=%lu), "
         "config=(disk_size=%lu, page_size=%lu, extent_size=%lu)\n",
         geometry->disk_size,
         geometry->page_size,
         geometry->extent_size,
         cfg->capacity,
         cfg->io_cfg->page_size,
         cfg->io_cfg->extent_size);
      return STATUS_BAD_PARAM;
   }

   return STATUS_OK;
}

platform_status
rc_allocator_read_disk_geometry(const char *filename, disk_geometry *geometry)
{
   return io_read_bootstrap(
      filename, geometry, sizeof(*geometry), RC_ALLOCATOR_BASE_OFFSET);
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
   al->meta_page->geometry       = rc_allocator_config_get_disk_geometry(al->cfg);
   al->meta_page->format_magic   = RC_ALLOCATOR_FORMAT_MAGIC;
   al->meta_page->format_version = RC_ALLOCATOR_FORMAT_VERSION;

   return STATUS_OK;
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
   rc = rc_allocator_init_meta_page(al);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init meta page for rc allocator\n");
      platform_mutex_destroy(&al->lock);
      return rc;
   }
   // To ensure alignment always allocate in multiples of page size.
   uint64 buffer_size = rc_allocator_refcount_buffer_size(cfg);
   rc                 = platform_buffer_init(&al->bh, buffer_size);
   if (!SUCCESS(rc)) {
      platform_mutex_destroy(&al->lock);
      platform_free(al->heap_id, al->meta_page);
      platform_error_log("Failed to create buffer for ref counts\n");
      return STATUS_NO_MEMORY;
   }
   al->ref_count = platform_buffer_getaddr(&al->bh);
   memset(al->ref_count, 0, buffer_size);

   // allocate the super block
   allocator_alloc(&al->super, &addr, PAGE_TYPE_SUPERBLOCK);
   // super block extent should always start from address 0.
   platform_assert(addr == RC_ALLOCATOR_BASE_OFFSET);

   /*
    * Allocate room for the ref counts, use same rounded up size used in buffer
    * creation.
    */
   rc_extent_count = rc_allocator_refcount_extent_count(cfg);
   for (uint64 i = 0; i < rc_extent_count; i++) {
      allocator_alloc(&al->super, &addr, PAGE_TYPE_SUPERBLOCK);
      platform_assert(addr == cfg->io_cfg->extent_size * (i + 1));
   }

   for (uint64 slot = 0; slot < RC_ALLOCATOR_CLEAN_STATE_SLOTS; slot++) {
      allocator_alloc(&al->super, &addr, PAGE_TYPE_SUPERBLOCK);
      platform_assert(addr == rc_allocator_clean_state_addr(cfg, slot));
   }

   /*
    * Persist the immutable bootstrap layout and both initial false state
    * records before returning a newly created allocator. A crash before a
    * later clean close therefore enters rebuild recovery rather than trusting
    * this freshly initialized refcount map.
    */
   rc = rc_allocator_initialize_clean_states(al);
   if (!SUCCESS(rc)) {
      goto deinit_allocator;
   }
   rc = rc_allocator_write_meta_page(al);
   if (!SUCCESS(rc)) {
      goto deinit_allocator;
   }
   rc = io_durable_barrier(al->io);
   if (!SUCCESS(rc)) {
      goto deinit_allocator;
   }

   return STATUS_OK;

deinit_allocator:
   rc_allocator_deinit(al);
   ZERO_CONTENTS(al);
   return rc;
}

void
rc_allocator_deinit(rc_allocator *al)
{
   platform_buffer_deinit(&al->bh);
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
static platform_status
rc_allocator_mount_internal(rc_allocator      *al,
                            allocator_config  *cfg,
                            io_handle         *io,
                            platform_heap_id   hid,
                            platform_module_id mid,
                            bool32             rebuild_refcounts)
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

   uint64 buffer_size = rc_allocator_refcount_buffer_size(cfg);
   status             = platform_buffer_init(&al->bh, buffer_size);
   if (!SUCCESS(status)) {
      platform_free(al->heap_id, al->meta_page);
      platform_mutex_destroy(&al->lock);
      platform_error_log("Failed to create buffer to load ref counts\n");
      return STATUS_NO_MEMORY;
   }
   al->ref_count = platform_buffer_getaddr(&al->bh);

   // load the meta page from disk.
   status = io_read(
      io, al->meta_page, al->cfg->io_cfg->page_size, RC_ALLOCATOR_BASE_OFFSET);
   if (!SUCCESS(status)) {
      goto deinit_buffer;
   }

   status = rc_allocator_validate_disk_geometry(al);
   if (!SUCCESS(status)) {
      goto deinit_buffer;
   }

   // validate the checksum of the meta page.
   checksum128 currChecksum = rc_allocator_meta_page_checksum(al->meta_page);
   if (!platform_checksum_is_equal(al->meta_page->checksum, currChecksum)) {
      platform_error_log("Corrupt SplinterDB allocator meta page on mount\n");
      status = STATUS_BAD_PARAM;
      goto deinit_buffer;
   }

   if (al->meta_page->format_magic != RC_ALLOCATOR_FORMAT_MAGIC
       || al->meta_page->format_version != RC_ALLOCATOR_FORMAT_VERSION)
   {
      platform_error_log("Unsupported SplinterDB allocator bootstrap format "
                         "on mount.\n");
      status = STATUS_BAD_PARAM;
      goto deinit_buffer;
   }

   if (!rebuild_refcounts) {
      rc_allocator_clean_states states;
      status = rc_allocator_load_clean_states(al, &states);
      if (!SUCCESS(status)) {
         goto deinit_buffer;
      }
      if (!states.have_newest || states.duplicate_sequence
          || !states.state[states.newest_slot].clean_shutdown)
      {
         platform_error_log("Allocator was not cleanly shut down; recovery "
                            "rebuild is required.\n");
         status = STATUS_INVALID_STATE;
         goto deinit_buffer;
      }
   }

   if (rebuild_refcounts) {
      /* Do not leave a stale clean state if recovery itself is interrupted. */
      status = rc_allocator_publish_clean_state(al, FALSE);
      if (!SUCCESS(status)) {
         goto deinit_buffer;
      }
      status = rc_allocator_recovery_initialize_refcounts(al);
      if (!SUCCESS(status)) {
         goto deinit_buffer;
      }
      al->recovery_in_progress = TRUE;
   } else {
      // Load the ref counts from disk during a normal, clean mount.
      status = io_read(io, al->ref_count, buffer_size, cfg->io_cfg->extent_size);
      if (!SUCCESS(status)) {
         goto deinit_buffer;
      }

      for (uint64 i = 0; i < al->cfg->extent_capacity; i++) {
         if (al->ref_count[i] != 0) {
            al->stats.curr_allocated++;
         }
      }

      /* Mark dirty before handing the trusted map to any mutating caller. */
      status = rc_allocator_publish_clean_state(al, FALSE);
      if (!SUCCESS(status)) {
         goto deinit_buffer;
      }
   }
   return STATUS_OK;

deinit_buffer:
   platform_buffer_deinit(&al->bh);
   al->ref_count = NULL;
   platform_free(al->heap_id, al->meta_page);
   al->meta_page = NULL;
   platform_mutex_destroy(&al->lock);
   return status;
}

platform_status
rc_allocator_mount(rc_allocator      *al,
                   allocator_config  *cfg,
                   io_handle         *io,
                   platform_heap_id   hid,
                   platform_module_id mid)
{
   return rc_allocator_mount_internal(al, cfg, io, hid, mid, FALSE);
}

platform_status
rc_allocator_mount_recovery(rc_allocator      *al,
                            allocator_config  *cfg,
                            io_handle         *io,
                            platform_heap_id   hid,
                            platform_module_id mid)
{
   return rc_allocator_mount_internal(al, cfg, io, hid, mid, TRUE);
}

platform_status
rc_allocator_rebuild_acquire_extent(rc_allocator *al, uint64 extent_addr)
{
   if (!al->recovery_in_progress) {
      platform_error_log("Cannot acquire allocator extent while recovery is "
                         "not in progress.\n");
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

   while (TRUE) {
      refcount old_ref = __atomic_load_n(&al->ref_count[extent_no],
                                         __ATOMIC_RELAXED);
      if (old_ref == (refcount)-1) {
         platform_error_log("Allocator recovery refcount overflow for extent "
                            "%lu.\n",
                            extent_no);
         return STATUS_LIMIT_EXCEEDED;
      }

      refcount new_ref =
         old_ref == AL_FREE ? AL_ONE_REF : old_ref + 1;
      if (!__sync_bool_compare_and_swap(
             &al->ref_count[extent_no], old_ref, new_ref))
      {
         continue;
      }

      if (old_ref == AL_FREE) {
         rc_allocator_record_allocated_extent(al);
      }
      return STATUS_OK;
   }
}

void
rc_allocator_rebuild_finish(rc_allocator *al)
{
   platform_assert(al != NULL);
   platform_assert(al->recovery_in_progress);

   /*
    * Intentionally no I/O here.  A rebuilt map is durable only after a later
    * clean unmount; another crash before then simply rebuilds it again.
    */
   al->recovery_in_progress = FALSE;
}

void
rc_allocator_abort_recovery(rc_allocator *al)
{
   platform_assert(al != NULL);
   platform_assert(al->recovery_in_progress);

   rc_allocator_deinit(al);
   ZERO_CONTENTS(al);
}


void
rc_allocator_unmount(rc_allocator *al)
{
   platform_status status;

   if (al->recovery_in_progress) {
      platform_error_log("Discarding incomplete allocator recovery instead of "
                         "persisting its partial refcount map.\n");
      rc_allocator_abort_recovery(al);
      return;
   }

   // persist the ref counts upon unmount.
   uint64 buffer_size = rc_allocator_refcount_buffer_size(al->cfg);
   uint32 io_size     = ROUNDUP(buffer_size, al->cfg->io_cfg->page_size);
   status =
      io_write(al->io, al->ref_count, io_size, al->cfg->io_cfg->extent_size);
   platform_assert_status_ok(status);

   /*
    * This is the sole normal persistence point for the allocator map. The
    * checkpoint record was already made durable by core_unmount(); do not
    * advertise a clean allocator snapshot until this write has crossed the
    * device durability boundary as well.
    */
   status = io_durable_barrier(al->io);
   platform_assert_status_ok(status);

   /* Publish clean permission in the alternate durable state record. */
   status = rc_allocator_publish_clean_state(al, TRUE);
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
         platform_status io_status = rc_allocator_write_meta_page(al);
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
         platform_status status = rc_allocator_write_meta_page(al);
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
