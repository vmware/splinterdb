// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stddef.h>

#include "platform_hash.h"
#include "cache.h"
#include "util.h"
#include "writeback_set.h"

#define BLOB_FORMAT        UINT16_C(2)
#define BLOB_CHECKSUM_SEED UINT64_C(0x424C4F424353554D)
#define BLOB_MAX_HEIGHT    16

typedef struct ONDISK blob {
   uint64      length;
   checksum128 checksum;
   uint16      format;
   uint64      addrs[];
} blob;

_Static_assert(offsetof(blob, length) == 0, "blob length layout changed");
_Static_assert(offsetof(blob, checksum) == 8, "blob checksum layout changed");
_Static_assert(offsetof(blob, format) == 24, "blob format layout changed");
_Static_assert(sizeof(((blob *)0)->format) == sizeof(uint16),
               "blob format must be uint16");
_Static_assert(offsetof(blob, addrs) == 26, "blob address layout changed");
_Static_assert(sizeof(blob) == 26, "blob header layout changed");

typedef struct parsed_blob_entry {
   uint64 addr;
   uint64 length;
} parsed_blob_entry;

typedef struct parsed_blob {
   const blob *base;
   /* Height one is the legacy flat layout. num_extents counts root entries;
    * each covers at most extent_span data extents. Neither depends on the
    * encoder's descriptor-size limit. */
   uint64            height;
   uint64            extent_span;
   uint64            num_extents;
   parsed_blob_entry leftovers[3];
} parsed_blob;

/* In-memory ownership reference to an immutable whole-extent subtree.
 * Height zero is data; higher extents contain child pointers terminated by
 * zero, unless completely full. Packed tails belong to their container.
 * Keep the owning container alive, or clone into another container. */
typedef struct blob_ref {
   uint64 addr;
   uint64 height;
} blob_ref;

blob_ref
blob_ref_create(uint64 addr, uint64 height);

blob_ref
blob_get_ref(cache *cc, const parsed_blob *parsed, uint64 index);

platform_status
blob_ref_validate(cache *cc, blob_ref ref);

/* Whole extents charged to this descriptor root, including pointer extents.
 * Counts shared storage again and uses descriptor geometry without I/O. */
uint64
blob_root_num_extents(cache *cc, const parsed_blob *parsed, uint64 index);

/* Consumes the reference, including on failure. Failed reclamation is recorded
 * with the allocator and must not be retried. Never touches packed tails. */
platform_status
blob_ref_release(cache *cc, blob_ref ref);

/* Count this incoming edge, and discover outgoing edges only on first visit.
 * Only valid during an allocator recovery rebuild. */
platform_status
blob_ref_recover(cache *cc, blob_ref ref);

/* Release the whole-extent references of a successfully built, unadopted
 * descriptor. Tail allocations remain owned by the destination container. */
platform_status
blob_release_refs(cache *cc, slice descriptor);

typedef struct page_fragment {
   uint64 addr;
   uint64 offset;
   uint64 length;
} page_fragment;

typedef enum blob_page_iterator_mode {
   BLOB_PAGE_ITERATOR_MODE_PREFETCH,
   BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH,
   BLOB_PAGE_ITERATOR_MODE_ALLOC,
   BLOB_PAGE_ITERATOR_MODE_VERIFY,
} blob_page_iterator_mode;

typedef struct blob_page_iterator {
   cache                  *cc;
   blob_page_iterator_mode mode;
   uint64                  extent_size;
   uint64                  page_size;
   parsed_blob             pblob;

   uint64          offset;
   page_fragment   fragment;
   page_handle     page;
   platform_status status;
   uint64          data_extent;
   uint64          data_extent_addr;
   page_handle     index_pages[BLOB_MAX_HEIGHT];
} blob_page_iterator;

bool
can_round_up(uint64 rounded_size, uint64 length);

/*
 * Validate the inline descriptor without I/O. Infer the minimum height whose
 * address count matches its size. Pointer pages are checked when traversed.
 * Legacy packed tails are always height one; redundant tree levels are illegal.
 */
platform_status
blob_parse_descriptor(cache *cc, slice sblob, parsed_blob *pblobby);

uint64
blob_length(slice sblob);

/*
 * Read and checksum all logical bytes referenced by sblob.  Every backing page
 * must be readable from the I/O address space; this is intended for recovery,
 * after the blob's writeback has completed.  Returns STATUS_INVALID_STATE for
 * an invalid descriptor format and STATUS_IO_ERROR when a page is absent or the
 * stored checksum does not match.
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

platform_status
blob_materialize_full(cache *cc, slice sblob, writable_buffer *result);

/*
 * Issue writeback of all data and pointer pages, recording them in `set` so the
 * caller can later wait for them.  Does not wait and does not make anything
 * durable; `set` may be NULL to issue and forget.
 *
 * A caller that treats a blob as part of some larger durable unit must pass a
 * set: the blob holds the record's value, so a unit declared durable without
 * it would replay a record whose value never reached the device.
 */
platform_status
blob_writeback(cache *cc, slice sblob, writeback_set *set);

/* Preorder walk of whole extents, including intermediate pointer extents.
 * The callback runs before an extent is read, permitting recovery to mark it.
 * Packed tails are excluded. disk_only checks pointer-page readability before
 * strict reads. Descriptor geometry bounds traversal; each partial pointer
 * extent must terminate at the expected zero slot.
 */
typedef platform_status (*blob_extent_callback)(cache *cc,
                                                uint64 addr,
                                                void  *arg);

/* A NULL visitor validates pointer topology and terminators only. */
platform_status
blob_walk_extents(cache               *cc,
                  slice                sblob,
                  bool32               disk_only,
                  blob_extent_callback visit,
                  void                *arg);

/* Recover one container-owned descriptor from a validated log record. Whole
 * extent roots count incoming edges; pointer descendants are enumerated once.
 * Tail extents are reserved once because packed tails share container storage.
 * Call only during an allocator recovery rebuild. */
platform_status
blob_recover_allocations(cache *cc, slice sblob);
