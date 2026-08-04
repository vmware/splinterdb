// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "platform_hash.h"
#include "cache.h"
#include "util.h"
#include "writeback_set.h"

typedef struct ONDISK blob {
   uint64 length;
   uint64 addrs[];
} blob;

/*
 * The checksum trailer follows the last address in a checksummed blob
 * descriptor.  Keeping it at the end preserves the layout of length and
 * addrs[], so descriptors written before checksums were introduced remain
 * readable and old readers can ignore the trailer.
 */
#define BLOB_CHECKSUM_FORMAT (UINT64_C(0x424C4F4243530001))
#define BLOB_CHECKSUM_SEED   (UINT64_C(0x424C4F424353554D))

typedef struct ONDISK blob_checksum_trailer {
   uint64      format;
   checksum128 checksum;
} blob_checksum_trailer;

_Static_assert(sizeof(blob_checksum_trailer) == 24,
               "blob checksum trailer layout changed");

typedef struct parsed_blob_entry {
   uint64 addr;
   uint64 length;
} parsed_blob_entry;

typedef struct parsed_blob {
   const blob       *base;
   uint64            num_extents;
   parsed_blob_entry leftovers[3];
} parsed_blob;

typedef struct page_fragment {
   uint64 addr;
   uint64 offset;
   uint64 length;
} page_fragment;

typedef enum blob_page_iterator_mode {
   BLOB_PAGE_ITERATOR_MODE_PREFETCH,
   BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH,
   BLOB_PAGE_ITERATOR_MODE_ALLOC,
} blob_page_iterator_mode;

typedef struct blob_page_iterator {
   cache                  *cc;
   blob_page_iterator_mode mode;
   uint64                  extent_size;
   uint64                  page_size;
   parsed_blob             pblob;

   uint64        offset;
   page_fragment fragment;
   page_handle  *page;
} blob_page_iterator;

bool
can_round_up(uint64 rounded_size, uint64 length);

void
parse_blob(uint64       extent_size,
           uint64       page_size,
           const blob  *blobby,
           parsed_blob *pblobby);

uint64
blob_length(slice sblob);

/*
 * Return the checksum stored in sblob's trailer.  Legacy blob descriptors do
 * not have a trailer and return STATUS_NOT_FOUND; callers must not interpret
 * that as successful validation.
 */
platform_status
blob_get_checksum(slice sblob, checksum128 *checksum);

/*
 * Read and checksum all logical bytes referenced by sblob.  Every backing page
 * must be readable from the I/O address space; this is intended for recovery,
 * after the blob's writeback has completed.  Returns STATUS_NOT_FOUND for a
 * legacy/unchecksummed descriptor and STATUS_IO_ERROR when a page is absent or
 * the stored checksum does not match.
 */
platform_status
blob_validate(cache *cc, slice sblob);

platform_status
blob_page_iterator_init(cache                  *cc,
                        blob_page_iterator     *iter,
                        slice                   sblobby,
                        uint64                  offset,
                        blob_page_iterator_mode mode);

void
blob_page_iterator_deinit(blob_page_iterator *iter);

platform_status
blob_page_iterator_get_curr(blob_page_iterator *iter,
                            uint64             *offset,
                            slice              *result);

bool
blob_page_iterator_at_end(blob_page_iterator *iter);

void
blob_page_iterator_advance_bytes(blob_page_iterator *iter, uint64 num_bytes);

void
blob_page_iterator_advance_page(blob_page_iterator *iter);

platform_status
blob_materialize(cache           *cc,
                 slice            sblob,
                 uint64           start,
                 uint64           end,
                 writable_buffer *result);

static inline platform_status
blob_materialize_full(cache *cc, slice sblob, writable_buffer *result)
{
   return blob_materialize(cc, sblob, 0, blob_length(sblob), result);
}

/*
 * Issue writeback of every page of the blob, recording each in `set` so the
 * caller can later wait for them.  Does not wait and does not make anything
 * durable; `set` may be NULL to issue and forget.
 *
 * A caller that treats a blob as part of some larger durable unit must pass a
 * set: the blob holds the record's value, so a unit declared durable without
 * it would replay a record whose value never reached the device.
 */
platform_status
blob_writeback(cache *cc, slice sblob, writeback_set *set);

/*
 * Record the allocator references this blob's storage holds, for a crash-
 * recovery rebuild.  Call between allocator_recovery_begin() and
 * allocator_recovery_finish().
 *
 * Reads nothing.  A blob carries the addresses of its own storage inline (see
 * struct blob), so the extents can be named without touching a page -- which is
 * what makes this usable during a rebuild, when reading a page whose extent is
 * not yet marked allocated is exactly what is forbidden.
 *
 * Records at most one reference per extent, skipping any that already has one.
 * That is the correct count and not merely deduplication: an extent gets a
 * single reference when its mini allocator hands it out, and blobs share
 * extents -- one holds the tails of many -- so counting per blob would leave
 * extents referenced several times over and never freed.
 */
platform_status
blob_recover_allocations(cache *cc, slice sblob);
