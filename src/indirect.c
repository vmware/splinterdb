// Copyright 2022 VMware, Inc. All rights reserved. -- VMware Confidential
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
   /* Following the entries there may be a tail of inline bytes that
      reside at the _beginning_ of the logical value. */
} indirection;

#define INLINE_ENTRY ((uint64)(-1))

typedef struct indirection_page_iterator {
   cache             *cc;
   const indirection *indy;
   page_type          type;
   uint64             page_size;
   uint64             offset; // logical byte offset into entire sequence
   uint64             entry;  // the entry we are current iterating through.
   page_handle       *page;   // the page with the data in it.
} indirection_page_iterator;

/* Returns INLINE_ENTRY to indicate that offset is in the inline portion. */
static uint64
entry_for_offset(const indirection *indy, uint64 offset)
{
   debug_assert(offset < indy->length);
   debug_assert(0 < indy->num_entries);

   if (offset < indy->entries[0].offset) {
      return INLINE_ENTRY; // offset is in the inline part
   }

   /* [robj]: replace with binary search. */
   uint64 i = 0;
   while (i < indy->num_entries - 1 && indy->entries[i + 1].offset <= offset) {
      debug_assert(indy->entries[i].offset < indy->length);
      debug_assert(indy->entries[i].offset < indy->entries[i + 1].offset);
      i++;
   }

   debug_assert(indy->entries[i].offset < indy->length);

   return i;
}

platform_status
indirection_page_iterator_init(cache                     *cc,
                               indirection_page_iterator *iter,
                               slice                      sindy,
                               uint64                     offset,
                               page_type                  type)
{
   const indirection *indy = slice_data(sindy);
   iter->cc                = cc;
   iter->indy              = indy;
   iter->type              = type;
   iter->page_size         = cache_page_size(cc);
   iter->offset            = offset;

   uint64 slength = slice_length(sindy);
   if (slength < sizeof(*indy)) {
      return STATUS_BAD_PARAM;
   }
   uint64 minslength = sizeof(*indy)
                       + indy->num_entries * sizeof(indy->entries[0])
                       + indy->entries[0].offset;
   if (slength < minslength) {
      return STATUS_BAD_PARAM;
   }
   if (indy->length < offset) {
      return STATUS_BAD_PARAM;
   }

   iter->entry = entry_for_offset(indy, offset);
   return STATUS_OK;
}

void
indirection_page_iterator_deinit(indirection_page_iterator *iter)
{
   if (iter->page) {
      cache_unget(iter->cc, iter->page);
      iter->page = NULL;
   }
}

static uint64
byte_address(const indirection_page_iterator *iter)
{
   debug_assert(iter->entry == entry_for_offset(iter, iter->offset));
   debug_assert(iter->entry < iter->indy->num_entries);

   const indirect_entry *entry = &iter->indy->entries[iter->entry];
   return entry->addr + (iter->offset - entry->offset);
}

static uint64
page_address(const indirection_page_iterator *iter)
{
   uint64 addr = byte_address(iter);
   return addr - (addr % iter->page_size);
}

static uint64
page_offset(const indirection_page_iterator *iter)
{
   uint64 address = byte_address(iter);
   return address % iter->page_size;
}

static uint64
entry_size(const indirection_page_iterator *iter)
{
   const indirection *indy = iter->indy;
   if (iter->entry == indy->num_entries - 1) {
      return indy->length - indy->entries[iter->entry].offset;
   }
   return indy->entries[iter->entry + 1].offset
          - indy->entries[iter->entry].offset;
}

static uint64
page_length(const indirection_page_iterator *iter)
{
   const indirect_entry *entry           = &iter->indy->entries[iter->entry];
   uint64                entry_off       = iter->offset - entry->offset;
   uint64                entry_remainder = entry_size(iter) - entry_off;
   uint64                pg_offset       = page_offset(iter);
   uint64                page_remainder  = iter->page_size - pg_offset;
   return entry_remainder < page_remainder ? entry_remainder : page_remainder;
}

static slice
page_data(indirection_page_iterator *iter)
{
   return slice_create(page_length(iter), iter->page->data + page_offset(iter));
}

platform_status
indirection_page_iterator_get_curr(indirection_page_iterator *iter,
                                   uint64                    *offset,
                                   slice                     *result)
{
   const indirection *indy = iter->indy;
   *offset                 = iter->offset;

   if (iter->entry == INLINE_ENTRY) {
      *result = slice_create(indy->entries[0].offset - iter->offset,
                             ((char *)&indy->entries[indy->num_entries])
                                + iter->offset);
   } else {

      if (iter->page == NULL) {
         iter->page =
            cache_get(iter->cc, page_address(iter), FALSE, iter->type);
      }
      *result = page_data(iter);
   }
   return STATUS_OK;
}

bool
indirection_page_iterator_at_end(indirection_page_iterator *iter)
{
   return iter->indy->length <= iter->offset;
}

void
indirection_page_iterator_advance(indirection_page_iterator *iter)
{
   if (iter->entry == INLINE_ENTRY) {
      iter->entry  = 0;
      iter->offset = iter->indy->entries[0].offset;
   } else {
      if (iter->page) {
         cache_unget(iter->cc, iter->page);
         iter->page = NULL;
      }
      iter->offset += page_length(iter);
      if (iter->offset < iter->indy->length
          && iter->indy->entries[iter->entry + 1].offset <= iter->offset)
      {
         iter->entry++;
      }
   }
}

platform_status
indirection_materialize(cache           *cc,
                        slice            sindy,
                        uint64           start,
                        uint64           end,
                        page_type        type,
                        writable_buffer *result)
{
   const indirection *indy = slice_data(sindy);

   if (end < start || indy->length < end) {
      return STATUS_BAD_PARAM;
   }

   platform_status rc = writable_buffer_resize(result, end - start);
   if (!SUCCESS(rc)) {
      return rc;
   }

   indirection_page_iterator iter;
   rc = indirection_page_iterator_init(cc, &iter, sindy, start, type);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint64 offset;
   slice  data;
   rc = indirection_page_iterator_get_curr(&iter, &offset, &data);
   if (!SUCCESS(rc)) {
      goto out;
   }

   void *dst = writable_buffer_data(result);
   while (offset < end) {
      uint64 slen   = slice_length(data);
      uint64 length = end - offset < slen ? end - offset : slen;
      memcpy(dst + (offset - start), slice_data(data), length);

      indirection_page_iterator_advance(&iter);
      if (indirection_page_iterator_at_end(&iter)) {
         goto out;
      }

      rc = indirection_page_iterator_get_curr(&iter, &offset, &data);
      if (!SUCCESS(rc)) {
         goto out;
      }
   }

out:
   indirection_page_iterator_deinit(&iter);
   return rc;
}
