// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#ifndef __BLOB_H
#define __BLOB_H

#include "util.h"
#include "cache.h"

/* We break the value into parts as follows:
 * - extent-sized segments
 * - sub-extent-sized multi-page segments (at most 2)
 * - less-than-page-sized segments (at most 1 -- page fragments are not split
 *   across extents)
 */
typedef struct ONDISK blob {
   uint64 length; // length of the byte sequence represented by this blob
   uint64 addrs[];
} blob;

/* This is used internally to avoid recomputing the number of extent entries,
 * etc. */
typedef struct parsed_blob_entry {
   uint64 addr;
   uint64 length;
} parsed_blob_entry;

typedef struct parsed_blob {
   uint64            length;
   uint64            num_extents;  // == number of extent entries
   const uint64     *extents;      // extent entries in original blob
   parsed_blob_entry leftovers[3]; // multi-page and sub-page entries
} parsed_blob;

typedef struct blob_page_iterator {
   cache       *cc;
   page_type    type;
   bool         alloc;
   bool         do_prefetch;
   uint64       extent_size;
   uint64       page_size;
   parsed_blob  pblob;
   uint64       offset; // logical byte offset into entire sequence
   uint64       page_addr;
   uint64       page_offset;
   uint64       length;
   page_handle *page; // the page with the data in it.
} blob_page_iterator;

/* If the data is large enough (or close enough to a whole number of
 * rounded_size pieces), then we just put it entirely into
 * rounded_size pieces, since this won't waste too much space.
 */
bool
can_round_up(uint64 rounded_size, uint64 length);

void
parse_blob(uint64       extent_size,
           uint64       page_size,
           const blob  *blobby,
           parsed_blob *pblobby);

/*
 * The length of the sequence of bytes represented by this
 * blob.
 */
uint64
blob_length(slice sindy);

platform_status
blob_page_iterator_init(cache              *cc,
                        blob_page_iterator *iter,
                        slice               sblobby,
                        uint64              offset,
                        page_type           type,
                        bool                alloc,
                        bool                do_prefetch);

void
blob_page_iterator_deinit(blob_page_iterator *iter);

platform_status
blob_page_iterator_get_curr(blob_page_iterator *iter,
                            uint64             *offset,
                            slice              *result);

bool
blob_page_iterator_at_end(blob_page_iterator *iter);

void
blob_page_iterator_advance(blob_page_iterator *iter);

platform_status
blob_materialize(cache           *cc,
                 slice            sblob,
                 uint64           start,
                 uint64           end,
                 page_type        type,
                 writable_buffer *result);

platform_status
blob_sync(cache *cc, slice sblob, page_type type);

#endif /* __BLOB_H */
