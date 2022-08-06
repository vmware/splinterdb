// Copyright 2018-2021 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#include "util.h"
#include "cache.h"
#include "poison.h"

/* Describes a sequence of bytes stored contiguously on disk.  The
   bytes may span pages, but not extents. Does not necessarily start
   or end at page boundaries.  */
typedef struct ONDISK indirect_entry {
   uint64 addr;
   uint64 offset; // offset of this subsequence in the overall sequence
} indirect_entry;


typedef struct ONDISK indirection {
   uint64 length; // length of the byte sequence represented by this indirection
   uint16 num_entries;
   indirect_entry entries[];
   /* Following the entries there may be a tail of direct bytes that
      reside at the _beginning_ of the logical value. */
} indirection;

typedef struct indirection_page_iterator {
   cache       *cc;
   indirection *indy;
   uint64       direct_size;
   page_type    type;
   uint64       page_size;
   uint64       offset; // logical byte offset
   uint64       entry;  // the entry we are current iterating through
   page_handle *page;   // the page with the data in it.
} indirection_page_iterator;

/* Returns indy->num_entries to indicate that offset is in the direct portion.
 */
static uint64
entry_for_offset(indirection_page_iterator *iter, uint64 offset)
{
   indirection *indy = iter->indy;
   debug_assert(offset < indy->length);

   if (offset < iter->direct_size) {
      return indy->num_entries; // offset is in the direct part
   }

   /* [robj]: replace with binary search. */
   uint64 i = 0;
   while (i < indy->num_entries - 1 && indy->entries[i + 1].offset <= offset) {
      debug_assert(iter->direct_size <= indy->entries[i].offset);
      debug_assert(indy->entries[i].offset < indy->length);
      debug_assert(indy->entries[i].offset < indy->entries[i + 1].offset);
      i++;
   }

   debug_assert(iter->direct_size <= indy->entries[i].offset);
   debug_assert(indy->entries[i].offset < indy->length);

   return i;
}

static uint64
byte_address(indirection_page_iterator *iter)
{
   debug_assert(iter->entry == entry_for_offset(iter, iter->offset));
   debug_assert(iter->entry < iter->indy->num_entries);

   indirect_entry    *entry   = &iter->indy->entries[iter->entry];
   return entry->addr + (iter->offset - entry->offset);
}

static uint64
page_address(indirection_page_iterator *iter)
{
   uint64 addr = byte_address(iter);
   return addr - (addr % iter->page_size);
}

static uint64
page_offset(indirection_page_iterator *iter)
{
   uint64 address = byte_address(iter);
   return address % iter->page_size;
}

static uint64
entry_size(indirection_page_iterator *iter)
{
   indirection *indy = iter->indy;
   if (iter->entry == indy->num_entries - 1) {
      return indy->length - indy->entries[iter->entry].offset;
   }
   return indy->entries[iter->entry + 1].offset
          - indy->entries[iter->entry].offset;
}

static uint64
page_length(indirection_page_iterator *iter)
{
   uint64 entry_sz  = entry_size(iter);
   uint64 entry_off = iter->offset - iter->indy->entries[iter->entry].offset;
   uint64 entry_remainder = entry_sz - entry_off;
   uint64 pg_offset       = page_offset(iter);
   uint64 page_remainder  = iter->page_size - pg_offset;
   return entry_remainder < page_remainder ? entry_remainder : page_remainder;
}

platform_status
indirection_page_iterator_init(cache                     *cc,
                               indirection_page_iterator *iter,
                               slice                      sindy,
                               uint64                     offset,
                               page_type                  type)
{
   iter->cc        = cc;
   iter->indy      = slice_data(sindy);
   iter->type      = type;
   iter->page_size = cache_page_size(cc);
   iter->offset    = offset;

   uint64 slength = slice_length(sindy);
   if (slength < sizeof(*iter->indy)) {
      return STATUS_BAD_PARAM;
   }
   uint64 minslength =
      sizeof(*iter->indy)
      + iter->indy->num_entries * sizeof(iter->indy->entries[0]);
   if (slength < minslength) {
      return STATUS_BAD_PARAM;
   }
   if (iter->indy->length < offset) {
      return STATUS_BAD_PARAM;
   }

   iter->direct_size =
      slength - (sizeof(*indy) + indy->num_entries * sizeof(indy->entries[0]));
   iter->entry = entry_for_offset(iter->indy, iter->offset);
   if (iter->entry < iter->indy->num_entries) {
      iter->page = cache_get(iter->cc, page_address(iter), FALSE, iter->type);
   }

   return STATUS_OK;
}

platform_status
indirection_page_iterator_curr(indirection_iterator *iter, slice *result)
{
   indirection *indy = iter->indy;

   if (iter->entry < indy->num_entries && iter->page == NULL) {
      iter->page = cache_get(iter->cc, page_address(iter), TRUE, iter->type);

   } else {
      *result =
         slice_create(iter->direct_size - offset,
                      ((char *)&indy->entries[indy->num_entries]) + offset);
   }
}

platform_status
indirection_materialize(cache           *cc,
                        slice            sindy,
                        uint64           start,
                        uint64           end,
                        materialization *result)
{
   uint64       slength = slice_length(sindy);
   indirection *indy    = slice_data(sindy);

   if (slength < sizeof(indirection)) {
      return STATUS_BAD_PARAM;
   }
   if (slength < sizeof(*indy) + indy->num_entries * sizeof(indy->entries[0])) {
      return STATUS_BAD_PARAM;
   }
   if (end < start || indy->length < end) {
      return STATUS_BAD_PARAM;
   }

   uint64 tail_length =
      slength - sizeof(*indy) + indy->num_entries * sizeof(indy->entries[0]);

   platform_status rc = writable_buffer_resize(result, indy->length);
   if (!SUCCESS(rc)) {
      return rc;
   }
   void *data = writable_buffer_data(result);

   uint64 page_size = cache_page_size(cc);

   uint64 offset = 0;
   for (uint64 i = 0; i < indy->num_entries; i++) {
   }
}
