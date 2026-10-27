// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"
#include "rc_allocator_disk.h"

static bool32
is_power_of_two(uint64 value)
{
   return value != 0 && (value & (value - 1)) == 0;
}

platform_status
rc_allocator_disk_compute_layout(const disk_geometry      *geometry,
                                 rc_allocator_disk_layout *layout)
{
   if (geometry == NULL || layout == NULL
       || !is_power_of_two(geometry->page_size)
       || geometry->page_size < RC_ALLOCATOR_DISK_HEADER_BYTES
       || geometry->page_size > RC_ALLOCATOR_DISK_CHUNK_BYTES
       || !is_power_of_two(geometry->extent_size)
       || geometry->extent_size < geometry->page_size
       || geometry->disk_size == 0 || geometry->disk_size > INT64_MAX
       || geometry->disk_size % geometry->extent_size != 0)
   {
      return STATUS_BAD_PARAM;
   }
   rc_allocator_disk_layout result = {0};
   result.extent_count = geometry->disk_size / geometry->extent_size;
   result.tag_bytes    = result.extent_count / 2 + result.extent_count % 2;
   /* disk_size <= INT64_MAX and extent_size >= 128 bound this rounding. */
   result.tag_disk_bytes = ROUNDUP(result.tag_bytes, geometry->page_size);
   result.fixed_bytes    = geometry->page_size + result.tag_disk_bytes;
   result.reserved_extents =
      1 + result.fixed_bytes / geometry->extent_size
      + (result.fixed_bytes % geometry->extent_size != 0);
   result.records_per_node =
      (geometry->extent_size - RC_ALLOCATOR_DISK_NODE_HEADER_BYTES)
      / RC_ALLOCATOR_DISK_RECORD_BYTES;
   if (result.reserved_extents > result.extent_count) {
      return STATUS_BAD_PARAM;
   }
   *layout = result;
   return STATUS_OK;
}

uint64
rc_allocator_disk_get_u64(const void *field)
{
   const uint8 *bytes = field;
   uint64       value = 0;
   for (unsigned i = 0; i < 8; i++) {
      value |= (uint64)bytes[i] << (8 * i);
   }
   return value;
}

void
rc_allocator_disk_put_u64(void *field, uint64 value)
{
   uint8 *bytes = field;
   for (unsigned i = 0; i < 8; i++) {
      bytes[i] = (uint8)(value >> (8 * i));
   }
}

void
rc_allocator_disk_seal_block(void *block, uint64 bytes)
{
   platform_assert(bytes >= 16);
   checksum128 sum = XXH3_128bits_withSeed(
      (uint8 *)block + 16, bytes - 16, RC_ALLOCATOR_DISK_CHECKSUM_SEED);
   rc_allocator_disk_put_u64(block, sum.low64);
   rc_allocator_disk_put_u64((uint8 *)block + 8, sum.high64);
}

bool32
rc_allocator_disk_verify_block(const void *block, uint64 bytes)
{
   if (bytes < 16) {
      return FALSE;
   }
   checksum128 actual = XXH3_128bits_withSeed(
      (const uint8 *)block + 16, bytes - 16, RC_ALLOCATOR_DISK_CHECKSUM_SEED);
   return actual.low64 == rc_allocator_disk_get_u64(block)
          && actual.high64
                == rc_allocator_disk_get_u64((const uint8 *)block + 8);
}

void
rc_allocator_disk_init_header(void                *page,
                              const disk_geometry *geometry,
                              uint64               overflow_records,
                              uint64               chain_extents,
                              uint64               chain_head,
                              checksum128          tag_hash,
                              checksum128          chain_hash)
{
   rc_allocator_disk_layout layout;
   platform_status status = rc_allocator_disk_compute_layout(geometry, &layout);
   platform_assert(SUCCESS(status));
   uint8 *p = page;
   memset(p, 0, geometry->page_size);
   rc_allocator_disk_put_u64(p + 16, RC_ALLOCATOR_DISK_MAGIC);
   rc_allocator_disk_put_u64(p + 24, RC_ALLOCATOR_DISK_VERSION);
   rc_allocator_disk_put_u64(p + 32, layout.extent_count);
   rc_allocator_disk_put_u64(p + 40, geometry->page_size);
   rc_allocator_disk_put_u64(p + 48, geometry->extent_size);
   rc_allocator_disk_put_u64(p + 56, layout.tag_bytes);
   rc_allocator_disk_put_u64(p + 64, overflow_records);
   rc_allocator_disk_put_u64(p + 72, chain_extents);
   rc_allocator_disk_put_u64(p + 80, chain_head);
   rc_allocator_disk_put_u64(p + 88, tag_hash.low64);
   rc_allocator_disk_put_u64(p + 96, tag_hash.high64);
   rc_allocator_disk_put_u64(p + 104, chain_hash.low64);
   rc_allocator_disk_put_u64(p + 112, chain_hash.high64);
   rc_allocator_disk_seal_block(p, geometry->page_size);
}

void
rc_allocator_disk_init_node(void                *block,
                            const disk_geometry *geometry,
                            uint64               index,
                            uint64               next_addr,
                            uint64               records)
{
   uint8 *p = block;
   memset(p, 0, geometry->extent_size);
   rc_allocator_disk_put_u64(p + 16, RC_ALLOCATOR_DISK_NODE_MAGIC);
   rc_allocator_disk_put_u64(p + 24, index);
   rc_allocator_disk_put_u64(p + 32, next_addr);
   rc_allocator_disk_put_u64(p + 40, records);
}

static bool32
all_zero(const uint8 *bytes, uint64 length)
{
   for (uint64 i = 0; i < length; i++) {
      if (bytes[i] != 0) {
         return FALSE;
      }
   }
   return TRUE;
}

typedef struct disk_reader {
   const disk_geometry       *geometry;
   rc_allocator_disk_layout   layout;
   rc_allocator_disk_summary  summary;
   rc_allocator_disk_read_fn  read_fn;
   rc_allocator_disk_chain_fn chain_fn;
   void                      *context;
   uint8                     *node;
   uint8                     *page;
   XXH3_state_t               chain_hash;
   uint64                     next_node;
   uint64                     nodes_read;
   uint64                     node_records;
   uint64                     record_index;
   uint64                     records_read;
   uint64                     previous_extent;
} disk_reader;

static bool32
valid_node_addr(const disk_reader *reader, uint64 addr)
{
   const disk_geometry *geometry = reader->geometry;
   return addr % geometry->extent_size == 0 && addr < geometry->disk_size
          && addr / geometry->extent_size >= reader->layout.reserved_extents;
}

/* Read just the containing tag page; the stream checksum is checked separately
 * when the main ordered tag scan reaches this page. */
static platform_status
check_node_tag(disk_reader *reader, uint64 addr)
{
   const disk_geometry *geometry = reader->geometry;
   uint64               extent   = addr / geometry->extent_size;
   uint64               byte     = extent / 2;
   uint64               offset   = byte - byte % geometry->page_size;
   uint64 tag_addr = geometry->extent_size + geometry->page_size + offset;
   platform_status status = reader->read_fn(
      reader->context, reader->page, geometry->page_size, tag_addr);
   if (!SUCCESS(status)) {
      return status;
   }
   uint8 tag = (reader->page[byte % geometry->page_size]
                >> ((extent % 2) * RC_ALLOCATOR_DISK_TAG_BITS))
               & RC_ALLOCATOR_DISK_TAG_ESCAPE;
   return tag == AL_ONE_REF ? STATUS_OK : STATUS_INVALID_STATE;
}

static platform_status
read_node(disk_reader *reader)
{
   const disk_geometry *geometry = reader->geometry;
   if (reader->nodes_read >= reader->summary.chain_extents
       || !valid_node_addr(reader, reader->next_node))
   {
      return STATUS_INVALID_STATE;
   }
   uint64          addr   = reader->next_node;
   platform_status status = reader->read_fn(
      reader->context, reader->node, geometry->extent_size, addr);
   if (!SUCCESS(status)) {
      return status;
   }
   uint8 *node  = reader->node;
   uint64 index = reader->nodes_read;
   uint64 count = MIN(reader->layout.records_per_node,
                      reader->summary.overflow_records - reader->records_read);
   uint64 next  = rc_allocator_disk_get_u64(node + 32);
   bool32 last  = index + 1 == reader->summary.chain_extents;
   if (!rc_allocator_disk_verify_block(node, geometry->extent_size)
       || rc_allocator_disk_get_u64(node + 16) != RC_ALLOCATOR_DISK_NODE_MAGIC
       || rc_allocator_disk_get_u64(node + 24) != index
       || rc_allocator_disk_get_u64(node + 40) != count || count == 0
       || !all_zero(node + 48, 16)
       || !all_zero(node + RC_ALLOCATOR_DISK_NODE_HEADER_BYTES
                       + count * RC_ALLOCATOR_DISK_RECORD_BYTES,
                    geometry->extent_size - RC_ALLOCATOR_DISK_NODE_HEADER_BYTES
                       - count * RC_ALLOCATOR_DISK_RECORD_BYTES)
       || (last ? next != 0 : !valid_node_addr(reader, next)))
   {
      return STATUS_INVALID_STATE;
   }
   status = check_node_tag(reader, addr);
   if (!SUCCESS(status)) {
      return status;
   }
   if (XXH3_128bits_update(&reader->chain_hash, node, geometry->extent_size)
       != XXH_OK)
   {
      return STATUS_INVALID_STATE;
   }
   if (reader->chain_fn != NULL) {
      status = reader->chain_fn(reader->context, addr);
      if (!SUCCESS(status)) {
         return status;
      }
   }
   reader->nodes_read++;
   reader->node_records = count;
   reader->record_index = 0;
   reader->next_node    = next;
   return STATUS_OK;
}

static platform_status
next_record(disk_reader *reader, uint64 expected_extent, refcount *value)
{
   if (reader->records_read == reader->summary.overflow_records) {
      return STATUS_INVALID_STATE;
   }
   if (reader->record_index == reader->node_records) {
      platform_status status = read_node(reader);
      if (!SUCCESS(status)) {
         return status;
      }
   }
   uint8 *record = reader->node + RC_ALLOCATOR_DISK_NODE_HEADER_BYTES
                   + reader->record_index * RC_ALLOCATOR_DISK_RECORD_BYTES;
   uint64 extent = rc_allocator_disk_get_u64(record);
   uint64 count  = rc_allocator_disk_get_u64(record + 8);
   if (extent != expected_extent || extent >= reader->layout.extent_count
       || extent < reader->layout.reserved_extents
       || (reader->records_read != 0 && extent <= reader->previous_extent)
       || count < RC_ALLOCATOR_DISK_TAG_ESCAPE)
   {
      return STATUS_INVALID_STATE;
   }
   reader->previous_extent = extent;
   reader->record_index++;
   reader->records_read++;
   *value = count;
   return STATUS_OK;
}

platform_status
rc_allocator_disk_read(const disk_geometry       *geometry,
                       platform_heap_id           heap_id,
                       rc_allocator_disk_read_fn  read_fn,
                       rc_allocator_disk_count_fn count_fn,
                       rc_allocator_disk_chain_fn chain_fn,
                       void                      *context,
                       rc_allocator_disk_summary *summary)
{
   disk_reader     reader = {.geometry = geometry,
                             .read_fn  = read_fn,
                             .chain_fn = chain_fn,
                             .context  = context};
   platform_status status =
      rc_allocator_disk_compute_layout(geometry, &reader.layout);
   if (!SUCCESS(status) || read_fn == NULL || summary == NULL) {
      return STATUS_BAD_PARAM;
   }
   uint8 *tags = NULL;
   reader.page = TYPED_ALIGNED_MALLOC(
      heap_id, geometry->page_size, reader.page, geometry->page_size);
   if (reader.page == NULL) {
      return STATUS_NO_MEMORY;
   }
   status =
      read_fn(context, reader.page, geometry->page_size, geometry->extent_size);
   if (!SUCCESS(status)) {
      goto cleanup;
   }
   uint8 *header = reader.page;
   if (!rc_allocator_disk_verify_block(header, geometry->page_size)
       || rc_allocator_disk_get_u64(header + 16) != RC_ALLOCATOR_DISK_MAGIC
       || rc_allocator_disk_get_u64(header + 24) != RC_ALLOCATOR_DISK_VERSION
       || rc_allocator_disk_get_u64(header + 32) != reader.layout.extent_count
       || rc_allocator_disk_get_u64(header + 40) != geometry->page_size
       || rc_allocator_disk_get_u64(header + 48) != geometry->extent_size
       || rc_allocator_disk_get_u64(header + 56) != reader.layout.tag_bytes
       || !all_zero(header + RC_ALLOCATOR_DISK_HEADER_BYTES,
                    geometry->page_size - RC_ALLOCATOR_DISK_HEADER_BYTES))
   {
      status = STATUS_INVALID_STATE;
      goto cleanup;
   }
   reader.summary.reserved         = reader.layout.reserved_extents;
   reader.summary.overflow_records = rc_allocator_disk_get_u64(header + 64);
   reader.summary.chain_extents    = rc_allocator_disk_get_u64(header + 72);
   reader.summary.chain_head       = rc_allocator_disk_get_u64(header + 80);
   reader.next_node                = reader.summary.chain_head;
   checksum128 expected_tags = {.low64 = rc_allocator_disk_get_u64(header + 88),
                                .high64 =
                                   rc_allocator_disk_get_u64(header + 96)};
   checksum128 expected_chain = {
      .low64  = rc_allocator_disk_get_u64(header + 104),
      .high64 = rc_allocator_disk_get_u64(header + 112)};
   uint64 records = reader.summary.overflow_records;
   uint64 nodes   = records / reader.layout.records_per_node
                  + (records % reader.layout.records_per_node != 0);
   uint64 available =
      reader.layout.extent_count - reader.layout.reserved_extents;
   if (reader.summary.chain_extents != nodes || nodes > available
       || records > available - nodes
       || (nodes == 0 ? reader.next_node != 0
                      : !valid_node_addr(&reader, reader.next_node)))
   {
      status = STATUS_INVALID_STATE;
      goto cleanup;
   }
   uint64 tag_buffer_bytes =
      MIN(reader.layout.tag_disk_bytes, RC_ALLOCATOR_DISK_CHUNK_BYTES);
   tags = TYPED_ALIGNED_MALLOC(
      heap_id, geometry->page_size, tags, tag_buffer_bytes);
   if (tags == NULL) {
      status = STATUS_NO_MEMORY;
      goto cleanup;
   }
   if (nodes != 0) {
      reader.node = TYPED_ALIGNED_MALLOC(
         heap_id, geometry->page_size, reader.node, geometry->extent_size);
      if (reader.node == NULL) {
         status = STATUS_NO_MEMORY;
         goto cleanup;
      }
   }
   XXH3_state_t tag_hash;
   if (XXH3_128bits_reset(&tag_hash) != XXH_OK
       || XXH3_128bits_reset(&reader.chain_hash) != XXH_OK)
   {
      status = STATUS_INVALID_STATE;
      goto cleanup;
   }
   for (uint64 offset = 0; offset < reader.layout.tag_disk_bytes;
        offset += tag_buffer_bytes)
   {
      uint64 bytes =
         MIN(tag_buffer_bytes, reader.layout.tag_disk_bytes - offset);
      uint64 useful = MIN(bytes, reader.layout.tag_bytes - offset);
      status        = read_fn(context,
                       tags,
                       bytes,
                       geometry->extent_size + geometry->page_size + offset);
      if (!SUCCESS(status)) {
         goto cleanup;
      }
      if (!all_zero(tags + useful, bytes - useful)
          || XXH3_128bits_update(&tag_hash, tags, useful) != XXH_OK)
      {
         status = STATUS_INVALID_STATE;
         goto cleanup;
      }
      for (uint64 i = 0; i < useful; i++) {
         for (unsigned half = 0; half < 2; half++) {
            uint64   extent = 2 * (offset + i) + half;
            refcount count  = (tags[i] >> (half * RC_ALLOCATOR_DISK_TAG_BITS))
                             & RC_ALLOCATOR_DISK_TAG_ESCAPE;
            if (extent == reader.layout.extent_count) {
               if (count != AL_FREE) {
                  status = STATUS_INVALID_STATE;
                  goto cleanup;
               }
               continue;
            }
            if (extent < reader.layout.reserved_extents && count != AL_ONE_REF)
            {
               status = STATUS_INVALID_STATE;
               goto cleanup;
            }
            if (count == RC_ALLOCATOR_DISK_TAG_ESCAPE) {
               status = next_record(&reader, extent, &count);
               if (!SUCCESS(status)) {
                  goto cleanup;
               }
            }
            reader.summary.allocated += count != AL_FREE;
            if (count_fn != NULL) {
               status = count_fn(context, extent, count);
               if (!SUCCESS(status)) {
                  goto cleanup;
               }
            }
         }
      }
   }
   if (reader.records_read != records || reader.nodes_read != nodes
       || reader.record_index != reader.node_records || reader.next_node != 0
       || !XXH128_isEqual(XXH3_128bits_digest(&tag_hash), expected_tags)
       || !XXH128_isEqual(XXH3_128bits_digest(&reader.chain_hash),
                          expected_chain))
   {
      status = STATUS_INVALID_STATE;
      goto cleanup;
   }
   *summary = reader.summary;
   status   = STATUS_OK;
cleanup:
   if (reader.node != NULL) {
      platform_free(heap_id, reader.node);
   }
   if (tags != NULL) {
      platform_free(heap_id, tags);
   }
   platform_free(heap_id, reader.page);
   return status;
}
