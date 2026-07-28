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

static bool32
superblock_is_valid(const superblock *sb, const allocator_config *cfg)
{
   // Copy the packed geometry member by value (avoids an unaligned address).
   disk_geometry geometry = sb->geometry;
   return sb->format_magic == SUPERBLOCK_FORMAT_MAGIC
          && sb->format_version == SUPERBLOCK_FORMAT_VERSION
          && superblock_geometry_matches(geometry, cfg)
          && platform_checksum_is_equal(sb->checksum, superblock_checksum(sb));
}

platform_status
superblock_read_geometry(const char *filename, disk_geometry *geometry)
{
   /*
    * Geometry is the first field of the superblock at page 0, and it is
    * write-invariant, so a raw read of it is safe even against a torn write of
    * the page's mutable fields.
    */
   return io_read_bootstrap(filename, geometry, sizeof(*geometry), 0);
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

   bool32 have_valid = FALSE;
   for (uint64 s = 0; s < SUPERBLOCK_NUM_SLOTS; s++) {
      rc = io_read(ctx->io, slot, ctx->page_size, superblock_slot_addr(ctx, s));
      if (!SUCCESS(rc)) {
         goto out;
      }
      if (!superblock_is_valid(slot, cfg)) {
         continue;
      }
      if (!have_valid || slot->generation > ctx->image->generation) {
         memcpy(ctx->image, slot, ctx->page_size);
         ctx->current_slot = s;
         have_valid        = TRUE;
      }
   }

   if (!have_valid) {
      platform_error_log("superblock_mount: no valid superblock found\n");
      rc = STATUS_NOT_FOUND;
      goto out;
   }
   rc = STATUS_OK;

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
   platform_assert(ctx->image->generation != 0); // generation wraparound

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
   ctx->image->geometry.disk_size    = cfg->capacity;
   ctx->image->geometry.page_size    = cfg->io_cfg->page_size;
   ctx->image->geometry.extent_size  = cfg->io_cfg->extent_size;
   ctx->image->format_magic          = SUPERBLOCK_FORMAT_MAGIC;
   ctx->image->format_version        = SUPERBLOCK_FORMAT_VERSION;
   ctx->image->generation            = 0;
   ctx->image->allocation_state_addr = 0; // fresh DB: rebuild on crash
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
   // The current live log becomes the sealed log (its entries are being folded
   // into the next root); new_live receives subsequent inserts.  The persisted
   // allocation map no longer matches the (log) state, so invalidate it.
   ctx->image->tree.sealed_log       = ctx->image->tree.live_log;
   ctx->image->tree.live_log         = new_live;
   ctx->image->allocation_state_addr = 0;
}

void
superblock_snapshot_tree(superblock_context *ctx,
                         uint64              root_addr,
                         uint64              first_unincorporated_generation,
                         superblock_log_head new_live)
{
   // The durable tree now includes everything folded into root_addr, so the
   // sealed log (if any) is done with; new_live is the log carried forward
   // (empty at a clean shutdown).  Advancing the root diverges the persisted
   // allocation map, so invalidate it.
   ctx->image->tree.root_addr = root_addr;
   ctx->image->tree.first_unincorporated_generation =
      first_unincorporated_generation;
   ctx->image->tree.sealed_log       = (superblock_log_head){0};
   ctx->image->tree.live_log         = new_live;
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
