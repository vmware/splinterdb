// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * superblock.h --
 *
 *     The SplinterDB superblock: the single, atomically-updated root of all
 *     durable instance metadata.  It unifies what used to be three unrelated
 *     mechanisms (the allocator meta page, the allocator clean-state A/B
 *     records, and core's per-table checkpoint directory + record A/B pairs)
 *     into one structure, so that a checkpoint advances a tree's root and
 *     invalidates the persisted allocation state in a single atomic write.
 *
 *     Two physical copies live at pages 0 and 1 of the device.  Writes
 *     alternate between them and carry a monotonic generation number, so a
 *     torn write always leaves the previous generation intact in the other
 *     page.  Mount picks the newest copy that passes magic/version/checksum/
 *     geometry validation.
 *
 *     The disk geometry is the first field (offset 0) so it can be read via a
 *     raw bootstrap read before any page size is known.  Geometry is
 *     write-invariant -- every generation writes identical geometry bytes --
 *     so a torn write to page 0 can only damage the checksummed mutable fields
 *     (recovered from the other page), never the geometry the bootstrap read
 *     depends on.
 *
 *     The instance holds a single tree today: its per-tree record is embedded
 *     directly rather than as an array.  Supporting multiple trees would be an
 *     on-disk format change (version bump + reformat), not just a machinery
 *     change.
 */

#pragma once

#include "platform_hash.h"
#include "platform_buffer.h"
#include "allocator.h"
#include "platform_io.h"
#include "util.h"

#define SUPERBLOCK_FORMAT_MAGIC   (0x5344425355504552ULL) // SDBSUPER
#define SUPERBLOCK_FORMAT_VERSION (1)

/* The two physical superblock copies live at pages 0 and 1. */
#define SUPERBLOCK_NUM_SLOTS (2)

/*
 * A log's on-disk identity.  Mirrors log_segment_info's layout; the superblock
 * stores it opaquely and does not depend on the log module.  meta_addr == 0
 * means "no log present".
 */
typedef struct ONDISK superblock_log_info {
   uint64 addr;
   uint64 meta_addr;
   uint64 magic;
} superblock_log_info;

/* An empty (absent) log slot: meta_addr == 0. */
#define SUPERBLOCK_NO_LOG(info) ((info).meta_addr == 0)

/*
 * The durable per-tree record.  The instance always has exactly one tree (from
 * mkfs onward), so the record carries no id or occupancy marker.
 *
 * The two log pointers implement the two-log checkpoint protocol: live_log is
 * the stream currently receiving inserts; sealed_log is set only while a
 * checkpoint is in progress -- the just-sealed stream whose entries are being
 * folded into the new root.  At rest (between checkpoints, and after a clean
 * unmount) sealed_log is empty.  On crash recovery the sealed log (if present)
 * then the live log are replayed onto root_addr, skipping entries at or below
 * incorporated_generation.
 *
 * There is intentionally no clean/dirty ("unmounted") flag: replaying a clean
 * root is a no-op because every entry is at or below incorporated_generation,
 * so the superblock's allocation_state_addr validity is the single at-rest
 * signal.
 */
typedef struct ONDISK superblock_tree_record {
   uint64 root_addr;
   /*
    * Highest memtable generation folded into root_addr; UINT64_MAX means none
    * has been incorporated yet (distinct from generation 0, which is a real,
    * live generation).  Drives memtable-generation resume at mount and the
    * replay-skip boundary.
    */
   uint64              incorporated_generation;
   superblock_log_info live_log;
   superblock_log_info sealed_log;
} superblock_tree_record;

/* Sentinel for superblock_tree_record.incorporated_generation. */
#define SUPERBLOCK_NO_INCORPORATED_GENERATION (UINT64_MAX)

typedef struct ONDISK superblock {
   disk_geometry geometry; // MUST be first; see the bootstrap-read note above.
   uint64        format_magic;
   uint64        format_version;
   uint64        generation; // monotonic; newest valid copy wins at mount
   /*
    * Base address of the persisted allocator refcount map when it is
    * trustworthy, or 0 to force a rebuild-by-walking on the next mount.  A
    * checkpoint writes this as 0 in the same atomic update that advances a
    * root; a clean unmount writes it nonzero after the map is durable.
    */
   uint64                 allocation_state_addr;
   superblock_tree_record tree;
   checksum128            checksum;
} superblock;

_Static_assert(offsetof(superblock, geometry) == 0,
               "disk geometry must be the first superblock field");
_Static_assert(sizeof(superblock) <= IO_DEFAULT_PAGE_SIZE,
               "superblock must fit in the default page size");

/*
 * In-memory handle over the on-disk superblock.  Holds a page-aligned image of
 * the current superblock and tracks which physical slot (0 or 1) it came from,
 * so a publish can target the other slot.
 */
typedef struct superblock_context {
   io_handle       *io;
   platform_heap_id heap_id;
   uint64           page_size;
   buffer_handle    image_buffer;
   superblock      *image; // page-aligned, page-sized
   uint64 current_slot;    // slot the in-memory image was last read/written
} superblock_context;

/*
 * Read the raw disk geometry via a bootstrap read of page 0, before any
 * subsystem is configured.  Safe against torn writes because geometry is
 * write-invariant.  Does not validate the checksum (the full mount does).
 */
platform_status
superblock_read_geometry(const char *filename, disk_geometry *geometry);

/* Allocate the in-memory image.  Does no I/O. */
platform_status
superblock_context_init(superblock_context     *ctx,
                        io_handle              *io,
                        const allocator_config *cfg,
                        platform_heap_id        hid);

/* Release in-memory resources.  Does no I/O.  Specifically, does _not_ make the
 * current in-memory superblock durable. */
void
superblock_context_deinit(superblock_context *ctx);

/*
 * Read both physical copies, validate each (magic, version, checksum, and
 * geometry against cfg), and load the newest valid one into the in-memory
 * image.  Returns STATUS_NOT_FOUND if neither copy is a valid superblock.
 *
 * Must call superblock_init() first.
 */
platform_status
superblock_mount(superblock_context *ctx, const allocator_config *cfg);

/*
 * Initialize a fresh superblock in the in-memory image (empty tree table,
 * allocation state invalid) and write both physical copies durably.  Used by
 * mkfs.
 *
 * Must call superblock_init() first.
 */
platform_status
superblock_format(superblock_context *ctx, const allocator_config *cfg);

/*
 * ---- Durable-state transitions ----
 *
 * Each transition mutates the in-memory image only; nothing reaches disk until
 * superblock_make_durable().  A checkpoint or clean unmount is a sequence of
 * these transitions followed by one superblock_make_durable(), so the
 * durability boundary is always explicit at the call site.
 *
 * The tree/log transitions encode the crash-safety invariant that the persisted
 * allocation map is trustworthy only while it matches the durable tree:
 * superblock_log_cut() and superblock_snapshot_tree() both invalidate it, and
 * superblock_snapshot_allocator() is the only operation that re-validates it.
 */

/*
 * Rotate the log: the current live log becomes the sealed log (its entries are
 * being folded into the next root) and new_live receives subsequent inserts.
 * Invalidates the allocation state.  Used at a checkpoint's begin, and to
 * install a fresh live log at mkfs/mount (where there is no prior live log, so
 * the sealed slot stays empty).
 */
void
superblock_log_cut(superblock_context *ctx, superblock_log_info new_live);

/*
 * Advance the durable tree to root_addr (having incorporated up to
 * incorporated_generation) and clear the sealed log -- a snapshot is taken only
 * after the sealed log has been folded into the root.  new_live is the log
 * carried forward (empty at a clean shutdown).  Invalidates the allocation
 * state.  Used at a checkpoint's completion and at a clean unmount.
 */
void
superblock_snapshot_tree(superblock_context *ctx,
                         uint64              root_addr,
                         uint64              incorporated_generation,
                         superblock_log_info new_live);

/*
 * Record the persisted allocator refcount map at map_addr as trustworthy.  This
 * is the only operation that validates the allocation state; the caller must
 * have made the map itself durable first.  Used as the final step of a clean
 * unmount.
 */
void
superblock_snapshot_allocator(superblock_context *ctx, uint64 map_addr);

/*
 * Make the current in-memory image durable: bump the generation, write it to
 * the physical slot not currently newest, and issue a durable barrier.  On
 * success that slot becomes newest; a torn write leaves the previous generation
 * intact in the other slot.  This is the single durability boundary for the
 * transitions above.
 */
platform_status
superblock_make_durable(superblock_context *ctx);

/* ---- Read-only accessors on the in-memory image ---- */

bool32
superblock_allocation_state_valid(const superblock_context *ctx);

uint64
superblock_allocation_state_addr(const superblock_context *ctx);

/* Copy the (always-present) tree record into *out. */
void
superblock_get_tree_record(const superblock_context *ctx,
                           superblock_tree_record   *out);
