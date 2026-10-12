// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * superblock.c --
 *
 *     Implementation of the SplinterDB superblock (see superblock.h).
 */

#include "superblock.h"
#include "poison.h"

#define SUPERBLOCK_CSUM_SEED (0x53555045524231ULL) // "SUPERB1"

static uint64
superblock_slot_addr(const superblock_context *ctx, uint64 slot)
{
   platform_assert(slot < SUPERBLOCK_NUM_SLOTS);
   return slot * ctx->page_size;
}

static checksum128
superblock_checksum(const superblock *sb)
{
   return platform_checksum128(
      sb, offsetof(superblock, checksum), SUPERBLOCK_CSUM_SEED);
}

static bool32
superblock_geometry_matches(disk_geometry geometry, const allocator_config *cfg)
{
   return geometry.disk_size == cfg->capacity
          && geometry.page_size == cfg->io_cfg->page_size
          && geometry.extent_size == cfg->io_cfg->extent_size;
}

/*
 * Tree roots and log heads name allocator extents.  Validate these addresses
 * before any mount path passes them to the allocator or cache.  Zero is
 * reserved as the absent-root/log sentinel and is handled by the callers.
 */
static bool32
superblock_extent_addr_is_valid(uint64 addr, disk_geometry geometry)
{
   return geometry.extent_size <= geometry.disk_size && addr != 0
          && addr % geometry.extent_size == 0
          && addr <= geometry.disk_size - geometry.extent_size;
}

static bool32
superblock_log_head_is_valid(superblock_log_head log, disk_geometry geometry)
{
   if (SUPERBLOCK_NO_LOG(log)) {
      return log.head.addr == 0 && log.head.nonce.high == 0
             && log.head.nonce.low == 0 && log.start_generation == 0
             && log.head.format_version == 0 && log.head.checksum_seed == 0;
   }

   return superblock_extent_addr_is_valid(log.head.addr, geometry)
          && superblock_extent_addr_is_valid(log.head.meta_addr, geometry);
}

static bool32
superblock_metadata_is_valid(const superblock *sb)
{
   /* Copy packed members by value before inspecting them. */
   disk_geometry          geometry = sb->geometry;
   superblock_tree_record tree     = sb->tree;

   if ((tree.root_addr != 0
        && !superblock_extent_addr_is_valid(tree.root_addr, geometry))
       || !superblock_log_head_is_valid(tree.live_log, geometry)
       || !superblock_log_head_is_valid(tree.sealed_log, geometry)
       || (!SUPERBLOCK_NO_LOG(tree.sealed_log)
           && SUPERBLOCK_NO_LOG(tree.live_log)))
   {
      return FALSE;
   }

   if (sb->allocation_state_addr == 0) {
      return TRUE;
   }

   /*
    * The refcount map is a fixed-format structure beginning at extent 1.
    * A valid map describes a clean shutdown, whose durable tree record no
    * longer needs either recovery log.
    */
   return sb->allocation_state_addr == geometry.extent_size
          && SUPERBLOCK_NO_LOG(tree.live_log)
          && SUPERBLOCK_NO_LOG(tree.sealed_log);
}

static bool32
superblock_has_valid_bootstrap(const superblock *sb)
{
   disk_geometry geometry = sb->geometry;
   return sb->format_magic == SUPERBLOCK_FORMAT_MAGIC
          && geometry.page_size == IO_DEFAULT_PAGE_SIZE
          && geometry.extent_size == IO_DEFAULT_EXTENT_SIZE
          && geometry.disk_size >= SUPERBLOCK_NUM_SLOTS * geometry.page_size
          && geometry.disk_size % geometry.page_size == 0
          && geometry.disk_size % geometry.extent_size == 0
          && platform_checksum_is_equal(sb->checksum, superblock_checksum(sb));
}

static bool32
superblock_is_valid(const superblock *sb, const allocator_config *cfg)
{
   /* Copy the packed geometry member by value (avoids an unaligned address). */
   disk_geometry geometry = sb->geometry;
   return superblock_has_valid_bootstrap(sb)
          && superblock_geometry_matches(geometry, cfg)
          && (sb->format_version != SUPERBLOCK_FORMAT_VERSION
              || superblock_metadata_is_valid(sb));
}

static platform_status
superblock_unsupported_version(uint64 version);

/* Check supported readers only after choosing the newest valid record. An
 * unknown component version must never roll the database back to an older
 * checkpoint whose versions happen to be understood. */
static platform_status
superblock_check_versions(const superblock *sb)
{
   /* An unknown version with a checksum valid under this layout is a
    * recognizable record, not a torn write. Reject it after generation
    * selection instead of silently returning an older checkpoint. */
   if (sb->format_version != SUPERBLOCK_FORMAT_VERSION) {
      return superblock_unsupported_version(sb->format_version);
   }
   if (sb->trunk_format_version != TRUNK_FORMAT_VERSION
       || sb->allocator_format_version != RC_ALLOCATOR_FORMAT_VERSION
       || (!SUPERBLOCK_NO_LOG(sb->tree.live_log)
           && sb->tree.live_log.head.format_version != SHARD_LOG_FORMAT_VERSION)
       || (!SUPERBLOCK_NO_LOG(sb->tree.sealed_log)
           && sb->tree.sealed_log.head.format_version
                 != SHARD_LOG_FORMAT_VERSION))
   {
      platform_error_log("Unsupported on-disk structure version: "
                         "trunk=%u allocator=%u live-log=%u sealed-log=%u\n",
                         sb->trunk_format_version,
                         sb->allocator_format_version,
                         sb->tree.live_log.head.format_version,
                         sb->tree.sealed_log.head.format_version);
      return STATUS_INVALID_STATE;
   }
   return STATUS_OK;
}

static platform_status
superblock_unsupported_version(uint64 version)
{
   platform_error_log("Unsupported superblock format version %lu; "
                      "this build requires version %u.\n",
                      version,
                      SUPERBLOCK_FORMAT_VERSION);
   return STATUS_INVALID_STATE;
}

/*
 * Compare wrapping 64-bit serial numbers. Published A/B generations are
 * adjacent, and therefore always less than half the serial-number space apart.
 * A separation of exactly 2^63 would be ambiguous and is outside that
 * invariant.
 */
static bool32
superblock_generation_is_newer(uint64 candidate, uint64 current)
{
   uint64 distance = candidate - current;
   return distance != 0 && distance < (UINT64_C(1) << 63);
}

platform_status
superblock_read_bootstrap(const char *filename, superblock *image)
{
   if (filename == NULL || image == NULL) {
      return STATUS_BAD_PARAM;
   }

   bool32     have_valid          = FALSE;
   superblock newest              = {0};
   bool32     have_unsupported    = FALSE;
   uint64     unsupported_version = 0;
   for (uint64 slot = 0; slot < SUPERBLOCK_NUM_SLOTS; slot++) {
      superblock      candidate;
      platform_status rc = io_read_bootstrap(
         filename, &candidate, sizeof(candidate), slot * IO_DEFAULT_PAGE_SIZE);
      if (!SUCCESS(rc)) {
         continue;
      }
      if (candidate.format_magic == SUPERBLOCK_FORMAT_MAGIC
          && candidate.format_version != SUPERBLOCK_FORMAT_VERSION)
      {
         have_unsupported    = TRUE;
         unsupported_version = candidate.format_version;
      }
      if (superblock_has_valid_bootstrap(&candidate)
          && (!have_valid
              || superblock_generation_is_newer(candidate.generation,
                                                newest.generation)))
      {
         newest     = candidate;
         have_valid = TRUE;
      }
   }
   if (!have_valid) {
      if (have_unsupported) {
         return superblock_unsupported_version(unsupported_version);
      }
      platform_error_log(
         "superblock_read_bootstrap: no valid superblock found\n");
      return STATUS_NOT_FOUND;
   }
   platform_status rc = superblock_check_versions(&newest);
   if (SUCCESS(rc)) {
      *image = newest;
   }
   return rc;
}

platform_status
superblock_read_geometry(const char *filename, disk_geometry *geometry)
{
   if (geometry == NULL) {
      return STATUS_BAD_PARAM;
   }
   superblock      image;
   platform_status rc = superblock_read_bootstrap(filename, &image);
   if (SUCCESS(rc)) {
      *geometry = image.geometry;
   }
   return rc;
}

platform_status
superblock_context_init(superblock_context     *ctx,
                        io_handle              *io,
                        const allocator_config *cfg,
                        platform_heap_id        hid)
{
   ZERO_CONTENTS(ctx);
   ctx->io        = io;
   ctx->heap_id   = hid;
   ctx->page_size = cfg->io_cfg->page_size;

   platform_assert(sizeof(superblock) <= ctx->page_size);

   platform_status rc =
      platform_buffer_init(&ctx->image_buffer, ctx->page_size);
   if (!SUCCESS(rc)) {
      return rc;
   }
   ctx->image = platform_buffer_getaddr(&ctx->image_buffer);
   memset(ctx->image, 0, ctx->page_size);
   ctx->current_slot = 0;
   return STATUS_OK;
}

void
superblock_context_deinit(superblock_context *ctx)
{
   if (ctx->image != NULL) {
      platform_buffer_deinit(&ctx->image_buffer);
      ctx->image = NULL;
   }
}

platform_status
superblock_mount(superblock_context *ctx, const allocator_config *cfg)
{
   platform_assert(ctx->image != NULL);

   buffer_handle   slot_buffer;
   platform_status rc = platform_buffer_init(&slot_buffer, ctx->page_size);
   if (!SUCCESS(rc)) {
      return rc;
   }
   superblock *slot = platform_buffer_getaddr(&slot_buffer);

   bool32          have_valid          = FALSE;
   platform_status first_read_error    = STATUS_OK;
   bool32          have_unsupported    = FALSE;
   uint64          unsupported_version = 0;
   for (uint64 s = 0; s < SUPERBLOCK_NUM_SLOTS; s++) {
      rc = io_read(ctx->io, slot, ctx->page_size, superblock_slot_addr(ctx, s));
      if (!SUCCESS(rc)) {
         if (SUCCESS(first_read_error)) {
            first_read_error = rc;
         }
         continue;
      }
      if (slot->format_magic == SUPERBLOCK_FORMAT_MAGIC
          && slot->format_version != SUPERBLOCK_FORMAT_VERSION)
      {
         have_unsupported    = TRUE;
         unsupported_version = slot->format_version;
      }
      if (!superblock_is_valid(slot, cfg)) {
         continue;
      }
      if (!have_valid
          || superblock_generation_is_newer(slot->generation,
                                            ctx->image->generation))
      {
         memcpy(ctx->image, slot, ctx->page_size);
         ctx->current_slot = s;
         have_valid        = TRUE;
      }
   }

   if (!have_valid) {
      if (have_unsupported) {
         rc = superblock_unsupported_version(unsupported_version);
      } else if (!SUCCESS(first_read_error)) {
         platform_error_log("superblock_mount: no readable valid superblock "
                            "found: %s\n",
                            platform_status_to_string(first_read_error));
         rc = first_read_error;
      } else {
         platform_error_log("superblock_mount: no valid superblock found\n");
         rc = STATUS_NOT_FOUND;
      }
      goto out;
   }
   rc = superblock_check_versions(ctx->image);

out:
{
   platform_status deinit_rc = platform_buffer_deinit(&slot_buffer);
   if (SUCCESS(rc) && !SUCCESS(deinit_rc)) {
      rc = deinit_rc;
   }
}
   return rc;
}

/*
 * Checksum the in-memory image and write it to a physical slot.  Does not make
 * it durable; superblock_make_durable() issues the barrier.
 */
static platform_status
superblock_write_slot(superblock_context *ctx, uint64 slot)
{
   ctx->image->checksum = superblock_checksum(ctx->image);
   return io_write(
      ctx->io, ctx->image, ctx->page_size, superblock_slot_addr(ctx, slot));
}

platform_status
superblock_make_durable(superblock_context *ctx)
{
   platform_assert(ctx->image != NULL);

   uint64 target = ctx->current_slot ^ 1;
   ctx->image->generation += 1;

   platform_status rc = superblock_write_slot(ctx, target);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = io_durable_barrier(ctx->io);
   if (!SUCCESS(rc)) {
      return rc;
   }
   ctx->current_slot = target;
   return STATUS_OK;
}

platform_status
superblock_format(superblock_context *ctx, const allocator_config *cfg)
{
   platform_assert(ctx->image != NULL);

   memset(ctx->image, 0, ctx->page_size);
   ctx->image->geometry.disk_size       = cfg->capacity;
   ctx->image->geometry.page_size       = cfg->io_cfg->page_size;
   ctx->image->geometry.extent_size     = cfg->io_cfg->extent_size;
   ctx->image->format_magic             = SUPERBLOCK_FORMAT_MAGIC;
   ctx->image->format_version           = SUPERBLOCK_FORMAT_VERSION;
   ctx->image->trunk_format_version     = TRUNK_FORMAT_VERSION;
   ctx->image->allocator_format_version = RC_ALLOCATOR_FORMAT_VERSION;
   ctx->image->generation               = 0;
   ctx->image->allocation_state_addr    = 0; // fresh DB: rebuild on crash
   // A fresh, empty tree: no root, and nothing incorporated yet -- replay from
   // generation 0.
   ctx->image->tree.first_unincorporated_generation = 0;

   /*
    * Write both physical copies so torn-write protection is in force from the
    * outset.  The first publish targets slot 0 (generation 1) because the
    * initial current_slot is 1; the second targets slot 1 (generation 2),
    * leaving slot 1 newest.
    */
   ctx->current_slot  = 1;
   platform_status rc = superblock_make_durable(ctx);
   if (!SUCCESS(rc)) {
      return rc;
   }
   return superblock_make_durable(ctx);
}

bool32
superblock_allocation_state_valid(const superblock_context *ctx)
{
   return ctx->image->allocation_state_addr != 0;
}

uint64
superblock_allocation_state_addr(const superblock_context *ctx)
{
   return ctx->image->allocation_state_addr;
}

void
superblock_get_tree_record(const superblock_context *ctx,
                           superblock_tree_record   *out)
{
   *out = ctx->image->tree;
}

void
superblock_log_cut(superblock_context *ctx, superblock_log_head new_live)
{
   // The retiring live log becomes the sealed log: its entries are still being
   // folded into the next root.  new_live receives subsequent inserts.  The
   // persisted allocation map no longer matches the (log) state, so invalidate
   // it.
   ctx->image->tree.sealed_log       = ctx->image->tree.live_log;
   ctx->image->tree.live_log         = new_live;
   ctx->image->allocation_state_addr = 0;
}

void
superblock_snapshot_tree(superblock_context *ctx,
                         uint64              root_addr,
                         uint64              first_unincorporated_generation)
{
   // Advancing the root diverges the persisted allocation map, so invalidate
   // it.  The live log is left alone; see the header.
   ctx->image->tree.root_addr = root_addr;
   ctx->image->tree.first_unincorporated_generation =
      first_unincorporated_generation;
   ctx->image->allocation_state_addr = 0;

   /*
    * Drop the sealed log only once the new root covers everything it holds.  It
    * covers generations up to live_log.start_generation - 1, so it is fully
    * incorporated exactly when first_unincorporated_generation reaches
    * live_log.start_generation.  Otherwise keep it: recovery still needs those
    * entries.
    */
   if (first_unincorporated_generation
       >= ctx->image->tree.live_log.start_generation)
   {
      ctx->image->tree.sealed_log = (superblock_log_head){0};
   }
}

void
superblock_discard_logs(superblock_context *ctx)
{
   ctx->image->tree.sealed_log       = (superblock_log_head){0};
   ctx->image->tree.live_log         = (superblock_log_head){0};
   ctx->image->allocation_state_addr = 0;
}

void
superblock_snapshot_allocator(superblock_context *ctx, uint64 map_addr)
{
   // The only operation that validates the allocation state; every tree/log
   // transition invalidates it.  The caller must have made the map itself
   // durable first, then make the superblock durable after.
   platform_assert(map_addr != 0);
   ctx->image->allocation_state_addr = map_addr;
}
