// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "cache.h"
#include "util.h"

typedef struct ONDISK blob {
   uint64 length;
   uint64 addrs[];
} blob;

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

platform_status
blob_sync(cache *cc, slice sblob);
