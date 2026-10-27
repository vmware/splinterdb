// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "allocator.h"
#include "platform_hash.h"
#include "platform_heap.h"

/* Durable allocation maps always use four-bit tags, independent of RAM width.
 * All integer fields below are explicit little-endian uint64 values. */
#define RC_ALLOCATOR_DISK_VERSION           UINT64_C(3)
#define RC_ALLOCATOR_DISK_MAGIC             UINT64_C(0x3350414d434f4c41)
#define RC_ALLOCATOR_DISK_NODE_MAGIC        UINT64_C(0x33444f4e434f4c41)
#define RC_ALLOCATOR_DISK_CHECKSUM_SEED     UINT64_C(0x1e96a12f7bb05d43)
#define RC_ALLOCATOR_DISK_TAG_BITS          4
#define RC_ALLOCATOR_DISK_TAG_ESCAPE        15
#define RC_ALLOCATOR_DISK_HEADER_BYTES      120
#define RC_ALLOCATOR_DISK_NODE_HEADER_BYTES 64
#define RC_ALLOCATOR_DISK_RECORD_BYTES      16
#define RC_ALLOCATOR_DISK_CHUNK_BYTES       (UINT64_C(1) << 20)

/* The fixed map starts at extent 1: one header page and the page-padded tags.
 * reserved_extents includes extent 0 and all fixed map extents, but not nodes.
 */
typedef struct rc_allocator_disk_layout {
   uint64 extent_count;
   uint64 tag_bytes;
   uint64 tag_disk_bytes;
   uint64 fixed_bytes;
   uint64 reserved_extents;
   uint64 records_per_node;
} rc_allocator_disk_layout;

typedef struct rc_allocator_disk_summary {
   uint64 allocated;
   uint64 reserved;
   uint64 overflow_records;
   uint64 chain_extents;
   uint64 chain_head;
} rc_allocator_disk_summary;

platform_status
rc_allocator_disk_compute_layout(const disk_geometry      *geometry,
                                 rc_allocator_disk_layout *layout);

uint64
rc_allocator_disk_get_u64(const void *field);

void
rc_allocator_disk_put_u64(void *field, uint64 value);

/* Block checksum occupies bytes 0..15 and covers every byte from 16 to end.
 * Tag and chain aggregate checksums instead use unseeded XXH3_128bits streams.
 */
void
rc_allocator_disk_seal_block(void *block, uint64 bytes);

bool32
rc_allocator_disk_verify_block(const void *block, uint64 bytes);

/* Geometry and counts must already be validated by the writer. Header encoding
 * clears all page padding and seals the resulting header. */
void
rc_allocator_disk_init_header(void                *page,
                              const disk_geometry *geometry,
                              uint64               overflow_records,
                              uint64               chain_extents,
                              uint64               chain_head,
                              checksum128          tag_hash,
                              checksum128          chain_hash);

/* Clears the complete extent and writes its header. The writer next encodes
 * records at byte 64 and seals the entire block. */
void
rc_allocator_disk_init_node(void                *block,
                            const disk_geometry *geometry,
                            uint64               index,
                            uint64               next_addr,
                            uint64               records);

typedef platform_status (*rc_allocator_disk_read_fn)(void  *context,
                                                     void  *buffer,
                                                     uint64 bytes,
                                                     uint64 addr);
typedef platform_status (*rc_allocator_disk_count_fn)(void    *context,
                                                      uint64   extent,
                                                      refcount count);
typedef platform_status (*rc_allocator_disk_chain_fn)(void  *context,
                                                      uint64 addr);

/* Read a quiescent version-3 map at extent 1. Every I/O buffer/address/length
 * is page-aligned. Allocations use heap_id and are bounded by a 1 MiB tag
 * window, one extent block and one page. No full-map or chain-address set is
 * allocated. Count and chain visitors are optional. A failure can follow
 * partial visitor delivery: callers must discard all partial map/chain results.
 * summary is only assigned on success. Checksums are corruption checks, not
 * authentication. */
platform_status
rc_allocator_disk_read(const disk_geometry       *geometry,
                       platform_heap_id           heap_id,
                       rc_allocator_disk_read_fn  read_fn,
                       rc_allocator_disk_count_fn count_fn,
                       rc_allocator_disk_chain_fn chain_fn,
                       void                      *context,
                       rc_allocator_disk_summary *summary);
