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
 *     This is intentionally a new on-disk format; older databases must be
 *     reformatted.
 */

#pragma once

#include "platform_hash.h"
#include "platform_buffer.h"
#include "allocator.h"
#include "platform_io.h"
#include "util.h"

/*
 * Format headroom for multiple trees.  The current system asserts at most one
 * occupied record (see the caller's claim path); the array is sized so that
 * multi-tree support becomes a machinery change, not a format change.
 */
#define SUPERBLOCK_MAX_TREES (30)

#define SUPERBLOCK_FORMAT_MAGIC   (0x5344425355504552ULL) // SDBSUPER
#define SUPERBLOCK_FORMAT_VERSION (1)

/* The two physical superblock copies live at pages 0 and 1. */
#define SUPERBLOCK_NUM_SLOTS (2)

/*
 * A per-tree durable record.  table_id == INVALID_ALLOCATOR_ROOT_ID marks an
 * empty slot.  log_meta_head is format headroom for the per-tree segmented log
 * (the oldest live segment, from whose start recovery replays); it is 0 until
 * the log is wired.  No intra-segment replay offset is stored: recovery replays
 * from a segment boundary and skips records whose generation is at or below
 * incorporated_generation.
 */
typedef struct ONDISK superblock_tree_record {
   uint64 table_id;
   uint64 root_addr;
   uint64 log_meta_head; // oldest live log segment = replay start (0 until wired)
   /*
    * Highest memtable generation folded into root_addr; UINT64_MAX means none
    * has been incorporated yet (distinct from generation 0, which is a real,
    * live generation).  Drives memtable-generation resume at mount and, once
    * the log is wired, the replay-skip boundary.
    */
   uint64 incorporated_generation;
   bool32 unmounted; // TRUE iff root_addr is a clean-unmount root
   uint32 pad;       // explicit: keep trailing on-disk bytes deterministic
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
   superblock_tree_record trees[SUPERBLOCK_MAX_TREES];
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
   superblock      *image;        // page-aligned, page-sized
   uint64           current_slot; // slot the in-memory image was last read/written
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
superblock_context_init(superblock_context *ctx,
                        io_handle          *io,
                        const allocator_config *cfg,
                        platform_heap_id    hid);

void
superblock_context_deinit(superblock_context *ctx);

/*
 * Read both physical copies, validate each (magic, version, checksum, and
 * geometry against cfg), and load the newest valid one into the in-memory
 * image.  Returns STATUS_NOT_FOUND if neither copy is a valid superblock.
 */
platform_status
superblock_mount(superblock_context *ctx, const allocator_config *cfg);

/*
 * Initialize a fresh superblock in the in-memory image (empty tree table,
 * allocation state invalid) and write both physical copies durably.  Used by
 * mkfs.
 */
platform_status
superblock_format(superblock_context *ctx, const allocator_config *cfg);

/*
 * Publish the current in-memory image: bump the generation, write it to the
 * physical slot not currently newest, and make it durable.  This is the single
 * atomic commit for a checkpoint or a clean-unmount transition; mutate the
 * image first via the setters below (tree records, allocation state), then
 * publish.
 */
platform_status
superblock_publish(superblock_context *ctx);

/* ---- Accessors on the in-memory image ---- */

bool32
superblock_allocation_state_valid(const superblock_context *ctx);

uint64
superblock_allocation_state_addr(const superblock_context *ctx);

void
superblock_set_allocation_state_addr(superblock_context *ctx, uint64 addr);

/*
 * Copy the tree record for table_id into *out.  Returns STATUS_NOT_FOUND if
 * table_id has no record.
 */
platform_status
superblock_get_tree_record(const superblock_context *ctx,
                           allocator_root_id         table_id,
                           superblock_tree_record   *out);

/*
 * Upsert rec into the in-memory image, matched by rec->table_id.  A new
 * table_id claims a free slot; STATUS_NO_SPACE if none remain.  In-memory
 * only; not durable until superblock_publish().
 */
platform_status
superblock_set_tree_record(superblock_context           *ctx,
                           const superblock_tree_record *rec);

/*
 * Remove the tree record for table_id from the in-memory image, freeing its
 * slot.  Returns STATUS_NOT_FOUND if table_id has no record.  In-memory only;
 * not durable until superblock_publish().
 */
platform_status
superblock_remove_tree_record(superblock_context *ctx,
                              allocator_root_id   table_id);

/* Number of occupied tree records in the in-memory image. */
uint64
superblock_num_trees(const superblock_context *ctx);
