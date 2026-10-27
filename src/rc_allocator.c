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

platform_status
rc_allocator_validate_format_version(uint16 format_version)
{
   if (format_version != RC_ALLOCATOR_FORMAT_VERSION) {
      platform_error_log("Unsupported allocator format version %u; "
                         "this build requires version %u.\n",
                         format_version,
                         RC_ALLOCATOR_FORMAT_VERSION);
      return STATUS_INVALID_STATE;
   }
   return STATUS_OK;
}

/*
 * The durable refcount snapshot has a fixed four-bit map starting at extent 1
 * (extent 0 holds the superblock), plus a temporary linked chain of overflow
 * extents. Whether the persisted snapshot is trustworthy is recorded by the
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

/* The compact representation is deliberately opt-in while the public retain
 * callbacks are infallible. The container itself has a failure-safe try_inc. */
static inline bool32
rc_allocator_has_refcounts(rc_allocator *al)
{
#if SPLINTER_RC_COMPACT
   return al->compact.words != NULL;
#else
   return al->ref_count != NULL;
#endif
}

static inline refcount
rc_allocator_count_load(rc_allocator *al, uint64 extent)
{
#if SPLINTER_RC_COMPACT
   return rc_refcount_get(&al->compact, extent);
#else
   return __atomic_load_n(&al->ref_count[extent], __ATOMIC_RELAXED);
#endif
}

static inline bool32
rc_allocator_count_allocated(rc_allocator *al, uint64 extent)
{
#if SPLINTER_RC_COMPACT
   return rc_refcount_is_allocated(&al->compact, extent);
#else
   return rc_allocator_count_load(al, extent) != AL_FREE;
#endif
}

static inline bool32
rc_allocator_count_claim(rc_allocator *al, uint64 extent)
{
#if SPLINTER_RC_COMPACT
   return rc_refcount_try_claim(&al->compact, extent);
#else
   return __sync_bool_compare_and_swap(
      &al->ref_count[extent], AL_FREE, AL_ONE_REF);
#endif
}

/*
 * Helper methods
 */
/*
 * Is page address 'base_addr' a valid extent address? I.e. it is the address
 * of the 1st page in an extent and lies within this allocator's device.
 */
static inline bool32
rc_allocator_valid_extent_addr(rc_allocator *al, uint64 base_addr)
{
   return base_addr < al->cfg->capacity
          && (base_addr % al->cfg->io_cfg->extent_size) == 0;
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

/*
 * Refcount operations are a security boundary: their addresses can originate
 * in an untrusted disk image. Keep this validation in production so a corrupt
 * extent address cannot index outside ref_count[].
 */
static inline uint64
rc_allocator_checked_extent_number(rc_allocator *al, uint64 addr)
{
   platform_assert(al != NULL);
   platform_assert(al->cfg != NULL);
   platform_assert(rc_allocator_has_refcounts(al));
   platform_assert(rc_allocator_valid_extent_addr(al, addr),
                   "Invalid allocator extent address %lu (capacity %lu, "
                   "extent size %lu)",
                   addr,
                   al->cfg->capacity,
                   al->cfg->io_cfg->extent_size);

   uint64 extent_no = rc_allocator_extent_number(al, addr);
   platform_assert(extent_no < al->cfg->extent_capacity);
   return extent_no;
}

static inline void
rc_allocator_assert_valid_page_type(page_type type)
{
   platform_assert(type >= PAGE_TYPE_FIRST && type < NUM_PAGE_TYPES,
                   "Invalid allocator page type %d",
                   type);
}

#if !SPLINTER_RC_COMPACT
static uint64
rc_allocator_refcount_buffer_size(const allocator_config *cfg)
{
   uint64 buffer_size = cfg->extent_capacity * sizeof(refcount);
   return ROUNDUP(buffer_size, cfg->io_cfg->page_size);
}
#endif

static disk_geometry
rc_allocator_geometry(const allocator_config *cfg)
{
   return (disk_geometry){.disk_size   = cfg->capacity,
                          .page_size   = cfg->io_cfg->page_size,
                          .extent_size = cfg->io_cfg->extent_size};
}

static uint64
rc_allocator_refcount_extent_count(const allocator_config *cfg)
{
   /* The disk encoding is always four bits, independent of the RAM backend. */
   uint64 tag_bytes = cfg->extent_capacity / 2 + cfg->extent_capacity % 2;
   uint64 bytes =
      cfg->io_cfg->page_size + ROUNDUP(tag_bytes, cfg->io_cfg->page_size);
   return (bytes + cfg->io_cfg->extent_size - 1) / cfg->io_cfg->extent_size;
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
   int64 max_allocated =
      __atomic_load_n(&al->stats.max_allocated, __ATOMIC_RELAXED);
   while (curr_allocated > max_allocated) {
      __sync_bool_compare_and_swap(
         &al->stats.max_allocated, max_allocated, curr_allocated);
      max_allocated =
         __atomic_load_n(&al->stats.max_allocated, __ATOMIC_RELAXED);
   }
}

/*
 * Allocate the (zeroed) in-memory refcount buffer, if it is not already
 * allocated.  mkfs (rc_allocator_init), a clean load (rc_allocator_load_
 * refcounts), and a recovery rebuild (rc_allocator_recovery_begin) each ensure
 * the buffer exists before populating it.  Attach (rc_allocator_mount)
 * deliberately does NOT: it leaves ref_count NULL so that using the allocator
 * before a load/recovery faults immediately.  Idempotent so it tolerates an
 * allocator reused across mount cycles without re-attaching.
 */
static platform_status
rc_allocator_alloc_refcount_buffer(rc_allocator *al)
{
   if (rc_allocator_has_refcounts(al)) {
      return STATUS_OK;
   }
#if SPLINTER_RC_COMPACT
   return rc_refcount_init(&al->compact,
                           al->cfg->extent_capacity,
                           al->heap_id,
                           platform_get_module_id());
#else
   uint64          buffer_size = rc_allocator_refcount_buffer_size(al->cfg);
   platform_status status      = platform_buffer_init(&al->bh, buffer_size);
   if (!SUCCESS(status)) {
      platform_error_log("Failed to create buffer for ref counts\n");
      return status;
   }
   al->ref_count = platform_buffer_getaddr(&al->bh);
   memset(al->ref_count, 0, buffer_size);
   return STATUS_OK;
#endif
}

/*
 * Choose a useful starting point after loading or rebuilding the allocation
 * map. These paths run before regular allocation is enabled, so no allocator
 * can race with this scan.
 */
static void
rc_allocator_reset_hand(rc_allocator *al)
{
   uint64 hand = 0;
   while (hand < al->cfg->extent_capacity
          && rc_allocator_count_allocated(al, hand))
   {
      hand++;
   }
   if (hand == al->cfg->extent_capacity) {
      hand = 0;
   }
   __atomic_store_n(&al->hand, hand, __ATOMIC_RELAXED);
}

static platform_status
rc_allocator_init_refcounts(rc_allocator *al)
{
   platform_status status = rc_allocator_alloc_refcount_buffer(al);
   if (!SUCCESS(status)) {
      return status;
   }

   uint64 reserved_extent_count = rc_allocator_reserved_extent_count(al->cfg);

   if (reserved_extent_count > al->cfg->extent_capacity) {
      platform_error_log("Allocator needs %lu reserved extents, but its "
                         "configured capacity is only %lu extents.\n",
                         reserved_extent_count,
                         al->cfg->extent_capacity);
      return STATUS_BAD_PARAM;
   }

#if SPLINTER_RC_COMPACT
   rc_refcount_reset(&al->compact);
#else
   memset(al->ref_count, 0, rc_allocator_refcount_buffer_size(al->cfg));
#endif
   if (al->snapshot_extents != NULL) {
      platform_free(al->heap_id, al->snapshot_extents);
   }
   al->snapshot_extents      = NULL;
   al->snapshot_extent_count = al->snapshot_extent_capacity = 0;
   /*
    * Set, not accumulated, for the same reason rc_allocator_load_refcounts()
    * does it: this establishes a map rather than adding to one, and a rebuild
    * may run against an allocator whose stats are not freshly zeroed.  The
    * reserved extents counted just below are then the whole of it.
    *
    * The per-type histograms go with it.  A rebuild re-derives the map from
    * what is on disk, so the allocations it records are the same ones an
    * earlier round recorded, not new ones; leaving them to accumulate would
    * make rc_allocator_assert_noleaks() report every rebuilt extent as leaked,
    * since nothing will ever deallocate it twice.
    */
   al->stats.curr_allocated = 0;
   memset(al->stats.extent_allocs, 0, sizeof(al->stats.extent_allocs));
   memset(al->stats.extent_deallocs, 0, sizeof(al->stats.extent_deallocs));

   /*
    * Extent 0 holds the superblock; the refcount map begins at extent 1.
    * Reserve both so recovery never hands them out.
    */
   for (uint64 extent_no = 0; extent_no < reserved_extent_count; extent_no++) {
      bool32 claimed = rc_allocator_count_claim(al, extent_no);
      platform_assert(claimed);
      rc_allocator_record_allocated_extent(al);
   }

   /* Everything after the fixed reserved prefix is free in a fresh map. */
   __atomic_store_n(&al->hand, reserved_extent_count, __ATOMIC_RELAXED);
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

   disk_geometry            disk = rc_allocator_geometry(cfg);
   rc_allocator_disk_layout layout;
   rc = rc_allocator_disk_compute_layout(&disk, &layout);
   if (!SUCCESS(rc)) {
      return rc;
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

platform_status
rc_allocator_recovery_begin(rc_allocator *al)
{
   /*
    * Cleared before the map is rebuilt, which is what makes a second rebuild
    * possible on an allocator that already finished one: while it is set,
    * rc_allocator_recovery_record_reference() refuses and
    * rc_allocator_recovery_finish() asserts.  Recovery rebuilds twice -- once
    * including the logs so replay can run without being handed their space, and
    * again from the root alone once replay is done, which is what releases that
    * space.
    */
   al->map_is_valid = FALSE;
   return rc_allocator_init_refcounts(al);
}

platform_status
rc_allocator_recovery_record_reference(rc_allocator *al,
                                       uint64        extent_addr,
                                       page_type     type)
{
   if (type < PAGE_TYPE_FIRST || type >= NUM_PAGE_TYPES) {
      platform_error_log("Invalid allocator recovery page type %d.\n", type);
      return STATUS_BAD_PARAM;
   }

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


#if SPLINTER_RC_COMPACT
   refcount previous = rc_allocator_count_load(al, extent_no);
   if (previous == UINT64_MAX) {
      return STATUS_LIMIT_EXCEEDED;
   }
   /* Recovery runs before ordinary allocator use. A first reference also
    * installs the allocation floor, without a transient count of one. */
   platform_status status = rc_refcount_set(
      &al->compact, extent_no, previous == AL_FREE ? AL_ONE_REF : previous + 1);
   if (!SUCCESS(status)) {
      return status;
   }
   if (previous == AL_FREE) {
      rc_allocator_record_allocated_extent(al);
      __sync_add_and_fetch(&al->stats.extent_allocs[type], 1);
   }
#else
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

#endif
   return STATUS_OK;
}

void
rc_allocator_recovery_finish(rc_allocator *al)
{
   platform_assert(al != NULL);
   platform_assert(!al->map_is_valid);

   /*
    * Intentionally no I/O here.  A rebuilt map is durable only after a later
    * rc_allocator_persist(); another crash before then simply rebuilds it
    * again.
    */
   rc_allocator_reset_hand(al);
   al->map_is_valid = TRUE;
}


/* The snapshot codec is shared by all RAM backends and the offline inspector.
 */
static platform_status
rc_allocator_snapshot_read(void *context, void *buf, uint64 bytes, uint64 addr)
{
   rc_allocator *al = context;
   return io_read(al->io, buf, bytes, addr);
}

static platform_status
rc_allocator_snapshot_count(void *context, uint64 extent, refcount count)
{
   rc_allocator *al = context;
#if SPLINTER_RC_COMPACT
   return rc_refcount_set(&al->compact, extent, count);
#else
   al->ref_count[extent] = count;
   return STATUS_OK;
#endif
}

static platform_status
rc_allocator_snapshot_chain(void *context, uint64 addr)
{
   rc_allocator *al = context;
   if (al->snapshot_extent_count == al->snapshot_extent_capacity) {
      if (al->snapshot_extent_capacity > SIZE_MAX / (2 * sizeof(uint64))) {
         return STATUS_NO_MEMORY;
      }
      uint64 capacity = al->snapshot_extent_capacity == 0
                           ? 16
                           : 2 * al->snapshot_extent_capacity;
      if (capacity > SIZE_MAX / sizeof(uint64)) {
         return STATUS_NO_MEMORY;
      }
      uint64 *extents = TYPED_ARRAY_MALLOC(al->heap_id, extents, capacity);
      if (extents == NULL) {
         return STATUS_NO_MEMORY;
      }
      if (al->snapshot_extent_count != 0) {
         memcpy(extents,
                al->snapshot_extents,
                al->snapshot_extent_count * sizeof(*extents));
      }
      if (al->snapshot_extents != NULL) {
         platform_free(al->heap_id, al->snapshot_extents);
      }
      al->snapshot_extents         = extents;
      al->snapshot_extent_capacity = capacity;
   }
   al->snapshot_extents[al->snapshot_extent_count++] = addr;
   return STATUS_OK;
}

platform_status
rc_allocator_load_refcounts(rc_allocator *al)
{
   platform_status status = rc_allocator_alloc_refcount_buffer(al);
   if (!SUCCESS(status)) {
      return status;
   }
   al->map_is_valid = FALSE;
   if (al->snapshot_extents != NULL) {
      platform_free(al->heap_id, al->snapshot_extents);
   }
   al->snapshot_extents      = NULL;
   al->snapshot_extent_count = al->snapshot_extent_capacity = 0;
#if SPLINTER_RC_COMPACT
   rc_refcount_reset(&al->compact);
#else
   memset(al->ref_count, 0, rc_allocator_refcount_buffer_size(al->cfg));
#endif
   disk_geometry             geometry = rc_allocator_geometry(al->cfg);
   rc_allocator_disk_summary summary;
   status = rc_allocator_disk_read(&geometry,
                                   al->heap_id,
                                   rc_allocator_snapshot_read,
                                   rc_allocator_snapshot_count,
                                   rc_allocator_snapshot_chain,
                                   al,
                                   &summary);
   if (!SUCCESS(status)) {
      /* Partial import is deliberately unusable; recovery may reset it. */
      if (al->snapshot_extents != NULL) {
         platform_free(al->heap_id, al->snapshot_extents);
      }
      al->snapshot_extents      = NULL;
      al->snapshot_extent_count = al->snapshot_extent_capacity = 0;
      return status;
   }
   al->stats.curr_allocated = summary.allocated;
   al->stats.max_allocated  = summary.allocated;
   /* Other page-type totals cannot be recovered from this map. */
   al->stats.extent_allocs[PAGE_TYPE_MISC] += al->snapshot_extent_count;
   rc_allocator_reset_hand(al);
   al->map_is_valid = TRUE;
   return STATUS_OK;
}

platform_status
rc_allocator_release_snapshot(rc_allocator *al)
{
   platform_assert(al->map_is_valid);
   /* Check the complete chain before changing any ownership. */
   for (uint64 i = 0; i < al->snapshot_extent_count; i++) {
      if (rc_allocator_count_load(
             al, al->snapshot_extents[i] / al->cfg->io_cfg->extent_size)
          != AL_ONE_REF)
      {
         return STATUS_INVALID_STATE;
      }
   }
   for (uint64 i = 0; i < al->snapshot_extent_count; i++) {
      uint64   addr  = al->snapshot_extents[i];
      refcount count = allocator_dec_ref(&al->super, addr, PAGE_TYPE_MISC);
      platform_assert(count == AL_NO_REFS);
      count = allocator_dec_ref(&al->super, addr, PAGE_TYPE_MISC);
      platform_assert(count == AL_FREE);
   }
   if (al->snapshot_extents != NULL) {
      platform_free(al->heap_id, al->snapshot_extents);
   }
   al->snapshot_extents      = NULL;
   al->snapshot_extent_count = al->snapshot_extent_capacity = 0;
   return STATUS_OK;
}

/* Quiescent clean shutdown only. The caller must have durably invalidated any
 * previous snapshot before this can overwrite the fixed map. No descriptor is
 * published here: on success the caller publishes only after our barrier.
 * Failed preparation leaves *state_addr unchanged and may require recovery. */
platform_status
rc_allocator_persist(rc_allocator *al, uint64 *state_addr)
{
   platform_assert(al->map_is_valid);
   if (al->snapshot_extent_count != 0) {
      /* A loaded/published snapshot must first be invalidated and released. */
      return STATUS_BUSY;
   }
   disk_geometry            geometry = rc_allocator_geometry(al->cfg);
   rc_allocator_disk_layout layout;
   platform_status          status =
      rc_allocator_disk_compute_layout(&geometry, &layout);
   if (!SUCCESS(status)) {
      return status;
   }
   uint64 records = 0;
   for (uint64 extent = 0; extent < al->cfg->extent_capacity; extent++) {
      records +=
         rc_allocator_count_load(al, extent) >= RC_ALLOCATOR_DISK_TAG_ESCAPE;
   }
   uint64 nodes = records / layout.records_per_node
                  + (records % layout.records_per_node != 0);
   uint64 chunk_bytes =
      MIN(layout.tag_disk_bytes, RC_ALLOCATOR_DISK_CHUNK_BYTES);
   uint8 *tags =
      TYPED_ALIGNED_MALLOC(al->heap_id, geometry.page_size, tags, chunk_bytes);
   uint8 *node = TYPED_ALIGNED_MALLOC(
      al->heap_id, geometry.page_size, node, geometry.extent_size);
   if (tags == NULL || node == NULL) {
      status = STATUS_NO_MEMORY;
      goto out;
   }
   if (nodes != 0) {
      al->snapshot_extents =
         TYPED_ARRAY_MALLOC(al->heap_id, al->snapshot_extents, nodes);
      if (al->snapshot_extents == NULL) {
         status = STATUS_NO_MEMORY;
         goto out;
      }
      al->snapshot_extent_capacity = nodes;
   }
   /* These allocations are raw2, so they do not change the overflow population.
    * Finish them before serializing tags, which must own the chain itself. */
   for (uint64 i = 0; i < nodes; i++) {
      uint64 addr;
      status = allocator_alloc(&al->super, &addr, PAGE_TYPE_MISC);
      if (!SUCCESS(status)) {
         goto out;
      }
      al->snapshot_extents[al->snapshot_extent_count++] = addr;
   }
   XXH3_state_t chain_state;
   XXH3_128bits_reset(&chain_state);
   uint64 extent    = 0;
   uint64 remaining = records;
   for (uint64 i = 0; i < nodes; i++) {
      uint64 count = MIN(remaining, layout.records_per_node);
      uint64 next  = i + 1 < nodes ? al->snapshot_extents[i + 1] : 0;
      rc_allocator_disk_init_node(node, &geometry, i, next, count);
      for (uint64 record = 0; record < count; record++) {
         while (extent < al->cfg->extent_capacity
                && rc_allocator_count_load(al, extent)
                      < RC_ALLOCATOR_DISK_TAG_ESCAPE)
         {
            extent++;
         }
         platform_assert(extent < al->cfg->extent_capacity);
         uint8 *pair = node + RC_ALLOCATOR_DISK_NODE_HEADER_BYTES
                       + record * RC_ALLOCATOR_DISK_RECORD_BYTES;
         rc_allocator_disk_put_u64(pair, extent);
         rc_allocator_disk_put_u64(pair + 8,
                                   rc_allocator_count_load(al, extent));
         extent++;
      }
      rc_allocator_disk_seal_block(node, geometry.extent_size);
      XXH3_128bits_update(&chain_state, node, geometry.extent_size);
      status =
         io_write(al->io, node, geometry.extent_size, al->snapshot_extents[i]);
      if (!SUCCESS(status)) {
         goto out;
      }
      remaining -= count;
   }
   XXH128_hash_t chain_hash = XXH3_128bits_digest(&chain_state);
   XXH3_state_t  tag_state;
   XXH3_128bits_reset(&tag_state);
   for (uint64 offset = 0; offset < layout.tag_disk_bytes;
        offset += chunk_bytes)
   {
      uint64 transfer = MIN(chunk_bytes, layout.tag_disk_bytes - offset);
      uint64 valid    = MIN(transfer, layout.tag_bytes - offset);
      memset(tags, 0, transfer);
      for (uint64 byte = 0; byte < valid; byte++) {
         uint64   first = 2 * (offset + byte);
         refcount low   = rc_allocator_count_load(al, first);
         refcount high  = first + 1 < al->cfg->extent_capacity
                             ? rc_allocator_count_load(al, first + 1)
                             : 0;
         tags[byte]     = MIN(low, RC_ALLOCATOR_DISK_TAG_ESCAPE)
                      | (MIN(high, RC_ALLOCATOR_DISK_TAG_ESCAPE) << 4);
      }
      XXH3_128bits_update(&tag_state, tags, valid);
      status = io_write(al->io,
                        tags,
                        transfer,
                        geometry.extent_size + geometry.page_size + offset);
      if (!SUCCESS(status)) {
         goto out;
      }
   }
   XXH128_hash_t tag_hash = XXH3_128bits_digest(&tag_state);
   rc_allocator_disk_init_header(node,
                                 &geometry,
                                 records,
                                 nodes,
                                 nodes == 0 ? 0 : al->snapshot_extents[0],
                                 tag_hash,
                                 chain_hash);
   status = io_write(al->io, node, geometry.page_size, geometry.extent_size);
   if (SUCCESS(status)) {
      status = io_durable_barrier(al->io);
   }
   if (SUCCESS(status) && state_addr != NULL) {
      *state_addr = geometry.extent_size;
   }
out:
   if (tags != NULL) {
      platform_free(al->heap_id, tags);
   }
   if (node != NULL) {
      platform_free(al->heap_id, node);
   }
   if (!SUCCESS(status)) {
      /* No caller may publish a failed persist. The old descriptor was already
       * invalidated, so these private allocations may safely be rolled back. */
      platform_status release_status = rc_allocator_release_snapshot(al);
      platform_assert_status_ok(release_status);
   }
   return status;
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
   platform_assert(al->map_is_valid);
   uint64 extent_no = rc_allocator_checked_extent_number(al, addr);

#if SPLINTER_RC_COMPACT
   refcount        ref_count;
   platform_status status =
      rc_refcount_try_inc(&al->compact, extent_no, &ref_count);
   /* Experimental adapter only: legacy ownership callbacks cannot propagate
    * OOM. Do not return a fabricated count or continue after a failed retain.
    * Keep BUILD_RC_COMPACT disabled in production until these callers can
    * roll back a fallible retain. The container has no partial-count change. */
   platform_assert(
      SUCCESS(status),
      "Experimental compact allocator retain failed at extent %lu: %s. "
      "Fallible ownership callbacks are required before production use.",
      extent_no,
      platform_status_to_string(status));
#else
   refcount ref_count = __sync_add_and_fetch(&al->ref_count[extent_no], 1);
#endif
   platform_assert(ref_count != 1 && ref_count != 0);
   if (SHOULD_TRACE(addr)) {
      platform_default_log("rc_allocator_inc_ref(%lu): %lu -> %lu\n",
                           addr,
                           ref_count,
                           ref_count + 1);
   }
   return ref_count;
}

refcount
rc_allocator_dec_ref(rc_allocator *al, uint64 addr, page_type type)
{
   platform_assert(al->map_is_valid);
   rc_allocator_assert_valid_page_type(type);
   uint64 extent_no = rc_allocator_checked_extent_number(al, addr);

#if SPLINTER_RC_COMPACT
   refcount ref_count = rc_refcount_dec(&al->compact, extent_no);
#else
   refcount ref_count = __sync_sub_and_fetch(&al->ref_count[extent_no], 1);
#endif

   // We should have decremented the ref-count. If it rolls-over and
   // goes back to this value, it means the original value was 0. That
   // indicates that we are decrementing a ref-count for an extent where
   // no page was previously allocated.
   platform_assert(ref_count != (refcount)(-1),
                   "extent_no=%lu, ref_count=%lu (0x%lx)\n",
                   extent_no,
                   ref_count,
                   ref_count);

   if (ref_count == 0) {
      __sync_sub_and_fetch(&al->stats.curr_allocated, 1);
      __sync_add_and_fetch(&al->stats.extent_deallocs[type], 1);
   }
   if (SHOULD_TRACE(addr)) {
      platform_default_log("rc_allocator_dec_ref(%lu): %lu -> %lu\n",
                           addr,
                           ref_count,
                           ref_count - 1);
   }
   return ref_count;
}

refcount
rc_allocator_get_ref(rc_allocator *al, uint64 addr)
{
   /* Recovery reads references it has already recorded before the whole map is
    * declared valid, so get_ref() requires a live buffer but not a finished
    * rebuild. rc_allocator_checked_extent_number() enforces the former. */
   uint64 extent_no = rc_allocator_checked_extent_number(al, addr);
   return rc_allocator_count_load(al, extent_no);
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
 * Publish progress made by a private allocation scan. hand is an unbounded
 * ticket, reduced modulo extent_capacity only when indexing ref_count. A scan
 * that finishes late must not move it behind progress already published by
 * another allocator.
 *
 * The serial-number comparison remains unambiguous because one scan advances
 * by at most extent_capacity, which is far below half the uint64 range for the
 * supported I/O geometry.
 */
static void
rc_allocator_advance_hand(rc_allocator *al, uint64 new_hand)
{
   uint64 old_hand = __atomic_load_n(&al->hand, __ATOMIC_RELAXED);
   while (new_hand != old_hand && new_hand - old_hand < (UINT64_C(1) << 63)) {
      if (__atomic_compare_exchange_n(&al->hand,
                                      &old_hand,
                                      new_hand,
                                      FALSE,
                                      __ATOMIC_RELAXED,
                                      __ATOMIC_RELAXED))
      {
         return;
      }
   }
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
   platform_assert(al->map_is_valid);
   rc_allocator_assert_valid_page_type(type);
   uint64 first_ticket   = __atomic_fetch_add(&al->hand, 1, __ATOMIC_RELAXED);
   uint64 first_hand     = first_ticket % al->cfg->extent_capacity;
   uint64 hand           = first_hand;
   bool32 extent_is_free = FALSE;

   for (uint64 offset = 0; offset < al->cfg->extent_capacity; offset++) {
      hand = (first_hand + offset) % al->cfg->extent_capacity;
      if (!rc_allocator_count_allocated(al, hand)) {
         extent_is_free = rc_allocator_count_claim(al, hand);
         if (extent_is_free) {
            break;
         }
      }
   }

   // Error out if no extent is free; allocation fails.
   if (!extent_is_free) {
      platform_default_log(
         "Out of Space, while allocating an extent of type=%d (%s):"
         " allocated %lu out of %lu extents.\n",
         type,
         page_type_str[type],
         __atomic_load_n(&al->stats.curr_allocated, __ATOMIC_RELAXED),
         al->cfg->extent_capacity);
      return STATUS_NO_SPACE;
   }
   uint64 scan_distance = hand >= first_hand
                             ? hand - first_hand
                             : al->cfg->extent_capacity - first_hand + hand;
   rc_allocator_advance_hand(al, first_ticket + scan_distance + 1);
   int64 curr_allocated = __sync_add_and_fetch(&al->stats.curr_allocated, 1);
   int64 max_allocated =
      __atomic_load_n(&al->stats.max_allocated, __ATOMIC_RELAXED);
   while (curr_allocated > max_allocated) {
      __sync_bool_compare_and_swap(
         &al->stats.max_allocated, max_allocated, curr_allocated);
      max_allocated =
         __atomic_load_n(&al->stats.max_allocated, __ATOMIC_RELAXED);
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
   return __atomic_load_n(&al->stats.curr_allocated, __ATOMIC_RELAXED);
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

   uint64   histogram[5] = {0};
   refcount maximum      = 0;
   for (uint64 extent = 0; extent < al->cfg->extent_capacity; extent++) {
      refcount count = rc_allocator_count_load(al, extent);
      uint64   bin   = count < 3 ? count : (count < 255 ? 3 : 4);
      histogram[bin]++;
      maximum = MAX(maximum, count);
   }
   platform_default_log(
      "Refcount census (non-atomic): capacity=%lu free=%lu "
      "retiring=%lu one=%lu raw3_254=%lu raw255plus=%lu max=%lu\n",
      al->cfg->extent_capacity,
      histogram[0],
      histogram[1],
      histogram[2],
      histogram[3],
      histogram[4],
      maximum);
#if SPLINTER_RC_COMPACT
   rc_refcount_usage usage;
   rc_refcount_get_usage(&al->compact, &usage);
   platform_default_log("Refcount storage: packed=%lu overflow=%lu "
                        "entries=%lu slots=%lu container=%lu\n",
                        usage.packed_bytes,
                        usage.overflow_bytes,
                        usage.overflow_entries,
                        usage.overflow_slots,
                        usage.container_bytes);
#else
   platform_default_log("Refcount storage: dense=%lu container=%lu\n",
                        rc_allocator_refcount_buffer_size(al->cfg),
                        sizeof(*al));
#endif

   uint64 extent_size = al->cfg->io_cfg->extent_size; // bytes
   int64  curr_allocated =
      __atomic_load_n(&al->stats.curr_allocated, __ATOMIC_RELAXED);
   int64 max_allocated =
      __atomic_load_n(&al->stats.max_allocated, __ATOMIC_RELAXED);
   platform_default_log(
      "| Currently Allocated: %12lu extents %-14s          |\n",
      curr_allocated,
      size_fmtstr("(%s)", (curr_allocated * extent_size)));

   platform_default_log(
      "| Max Allocated:       %12lu extents %-14s          |\n",
      max_allocated,
      size_fmtstr("(%s)", (max_allocated * extent_size)));

   // clang-format off
   platform_default_log("|%s|\n", dashes);
   platform_default_log("| Page Type  | Allocations | Deallocations |      Footprint         |\n");
   platform_default_log("|            |      (Number of extents)    | # extents  (bytes)     |\n");
   platform_default_log("|%s|\n", dashes);
   // clang-format on

   int64 exp_allocated_count = 0;
   for (page_type type = PAGE_TYPE_FIRST; type < NUM_PAGE_TYPES; type++) {
      const char *str = page_type_str[type];
      int64       allocs =
         __atomic_load_n(&al->stats.extent_allocs[type], __ATOMIC_RELAXED);
      int64 deallocs =
         __atomic_load_n(&al->stats.extent_deallocs[type], __ATOMIC_RELAXED);
      int64 footprint = allocs - deallocs;

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
   uint64   nallocated =
      __atomic_load_n(&al->stats.curr_allocated, __ATOMIC_RELAXED);

   // For more than a few allocated extents, print enclosing { } tags.
   bool32 print_curly = (nallocated > 20);

   platform_default_log(
      "Allocated extents: %lu\n%s", nallocated, (print_curly ? "{\n" : ""));
   platform_default_log("   Index  ExtentAddr  Count\n");

   // # of extents with non-zero referenced page-count found
   uint64 nextents_found = 0;
   uint64 extent_size    = al->cfg->io_cfg->extent_size;

   for (i = 0; i < al->cfg->extent_capacity; i++) {
      ref = rc_allocator_count_load(al, i);
      if (ref != 0) {
         nextents_found++;
         uint64 ext_addr = (i * extent_size);
         platform_default_log("%8lu %12lu     %lu\n", i, ext_addr, ref);
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

static bool32
rc_allocator_is_allocated_virtual(allocator *a, uint64 addr)
{
   rc_allocator *al     = (rc_allocator *)a;
   uint64        extent = rc_allocator_checked_extent_number(al, addr);
   return rc_allocator_count_allocated(al, extent);
}

platform_status
rc_allocator_recovery_begin_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_recovery_begin(al);
}

platform_status
rc_allocator_recovery_record_reference_virtual(allocator *a,
                                               uint64     addr,
                                               page_type  type)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_recovery_record_reference(al, addr, type);
}

void
rc_allocator_recovery_finish_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_recovery_finish(al);
}


platform_status
rc_allocator_load_refcounts_virtual(allocator *a)
{
   rc_allocator *al = (rc_allocator *)a;
   return rc_allocator_load_refcounts(al);
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

static platform_status
rc_allocator_release_snapshot_virtual(allocator *a)
{
   return rc_allocator_release_snapshot((rc_allocator *)a);
}

const static allocator_ops rc_allocator_ops = {
   .get_config                = rc_allocator_get_config_virtual,
   .alloc                     = rc_allocator_alloc_virtual,
   .inc_ref                   = rc_allocator_inc_ref_virtual,
   .dec_ref                   = rc_allocator_dec_ref_virtual,
   .get_ref                   = rc_allocator_get_ref_virtual,
   .is_allocated              = rc_allocator_is_allocated_virtual,
   .recovery_begin            = rc_allocator_recovery_begin_virtual,
   .recovery_record_reference = rc_allocator_recovery_record_reference_virtual,
   .recovery_finish           = rc_allocator_recovery_finish_virtual,
   .load_refcounts            = rc_allocator_load_refcounts_virtual,
   .persist                   = rc_allocator_persist_virtual,
   .release_snapshot          = rc_allocator_release_snapshot_virtual,
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

   rc = rc_allocator_init_refcounts(al);
   if (!SUCCESS(rc)) {
      platform_mutex_destroy(&al->lock);
      return rc;
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
   if (al->snapshot_extents != NULL) {
      platform_free(al->heap_id, al->snapshot_extents);
   }
   al->snapshot_extents      = NULL;
   al->snapshot_extent_count = al->snapshot_extent_capacity = 0;
#if SPLINTER_RC_COMPACT
   rc_refcount_deinit(&al->compact);
#else
   if (al->ref_count) {
      platform_buffer_deinit(&al->bh);
      al->ref_count = NULL;
   }
#endif
   platform_mutex_destroy(&al->lock);
}

/*
 *----------------------------------------------------------------------
 * rc_allocator_mount --
 *
 *      Attach an allocator to an existing device: initialize the in-memory
 *      structures only.  The refcount buffer is deliberately left NULL so that
 *      using the allocator before its map is populated faults immediately; the
 *      caller MUST next call rc_allocator_load_refcounts() (clean mount) or
 *      rc_allocator_recovery_begin() (rebuild), each of which allocates and
 *      populates the buffer.  A rebuild therefore never reads a map it would
 *      discard.
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

   // Attach only: ref_count is left NULL (and map_is_valid FALSE) until
   // load_refcounts / recovery_begin populates the map, so a premature use of
   // the allocator faults at once.
   return STATUS_OK;
}
