// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "blob_key.h"
#include "splinterdb/default_data_config.h"

#define BLOB_KEY_FORMAT 1

typedef struct ONDISK blob_key_header {
   uint64 length;
   uint8  prefix_bytes;
   uint8  format;
} blob_key_header;

typedef struct blob_key_view {
   uint64 length;
   slice  prefix;
   slice  descriptor;
   bool32 is_blob;
} blob_key_view;

/* Bounds checks on every comparison, including a prefix-only comparison. */
static platform_status
blob_key_view_init(blob_key_config *cfg, user_key input, blob_key_view *view)
{
   ZERO_CONTENTS(view);
   if (!cfg->blob_keys || input.is_query_key) {
      view->length = slice_length(input.key);
      view->prefix = input.key;
      return STATUS_OK;
   }

   uint64 size = slice_length(input.key);
   if (size < sizeof(blob_key_header)) {
      return STATUS_INVALID_STATE;
   }
   const blob_key_header *hdr    = slice_data(input.key);
   uint64                 prefix = hdr->prefix_bytes;
   if (hdr->format != BLOB_KEY_FORMAT || prefix > BLOB_KEY_MAX_PREFIX_BYTES
       || prefix > hdr->length || prefix > size - sizeof(*hdr)
       || size - sizeof(*hdr) - prefix < sizeof(blob)
       || size - sizeof(*hdr) - prefix > BLOB_KEY_DESCRIPTOR_BYTES)
   {
      return STATUS_INVALID_STATE;
   }
   const char *bytes = slice_data(input.key);
   view->length      = hdr->length;
   view->prefix      = slice_create(prefix, bytes + sizeof(*hdr));
   view->descriptor =
      slice_create(size - sizeof(*hdr) - prefix, bytes + sizeof(*hdr) + prefix);
   const blob *descriptor = slice_data(view->descriptor);
   if (descriptor->length != hdr->length || descriptor->format != BLOB_FORMAT
       || (slice_length(view->descriptor) - sizeof(blob)) % sizeof(uint64) != 0)
   {
      return STATUS_INVALID_STATE;
   }
   view->is_blob = TRUE;
   return STATUS_OK;
}

/*
 * Existing data_config callbacks cannot return an I/O status. This harness
 * therefore stops on malformed keys or read failures. Never synthesize an
 * ordering after an error: that could direct a mutation into the wrong leaf.
 */
static void
blob_key_callback_check(platform_status rc)
{
   platform_assert(SUCCESS(rc),
                   "blob-key prototype callback failed: %s; production "
                   "integration requires fallible comparison and hashing",
                   platform_status_to_string(rc));
}

platform_status
blob_key_validate(blob_key_config *cfg, slice encoded)
{
   blob_key_view   view;
   platform_status rc = blob_key_view_init(
      cfg, (user_key){.key = encoded, .is_query_key = FALSE}, &view);
   if (!SUCCESS(rc) || !view.is_blob) {
      return rc;
   }
   parsed_blob parsed;
   return blob_parse_descriptor(cfg->cc, view.descriptor, &parsed);
}

typedef struct blob_key_cursor {
   blob_page_iterator   iter;
   const blob_key_view *view;
   uint64               offset;
   bool32               initialized;
} blob_key_cursor;

static platform_status
blob_key_cursor_init(blob_key_config     *cfg,
                     blob_key_cursor     *cursor,
                     const blob_key_view *view,
                     uint64               offset)
{
   cursor->view        = view;
   cursor->offset      = offset;
   cursor->initialized = FALSE;
   if (!view->is_blob || offset == view->length) {
      return STATUS_OK;
   }
   platform_status rc =
      blob_page_iterator_init(cfg->cc,
                              &cursor->iter,
                              view->descriptor,
                              offset,
                              BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH);
   cursor->initialized = SUCCESS(rc);
   return rc;
}

static void
blob_key_cursor_deinit(blob_key_cursor *cursor)
{
   if (cursor->initialized) {
      blob_page_iterator_deinit(&cursor->iter);
   }
}

static platform_status
blob_key_cursor_get(blob_key_config *cfg, blob_key_cursor *cursor, slice *part)
{
   if (!cursor->view->is_blob) {
      *part = slice_create(cursor->view->length - cursor->offset,
                           (const char *)slice_data(cursor->view->prefix)
                              + cursor->offset);
      return STATUS_OK;
   }
   uint64          offset;
   platform_status rc =
      blob_page_iterator_get_curr(&cursor->iter, &offset, part);
   if (SUCCESS(rc)) {
      cfg->counters.blob_fragments++;
      platform_assert(offset == cursor->offset);
   }
   return rc;
}

static void
blob_key_cursor_advance(blob_key_cursor *cursor, uint64 bytes)
{
   cursor->offset += bytes;
   if (cursor->initialized) {
      blob_page_iterator_advance_bytes(&cursor->iter, bytes);
   }
}

static int
blob_key_compare_lengths(uint64 a, uint64 b)
{
   return (a > b) - (a < b);
}

/* Keep two large blob iterator frames off the inline-prefix fast path. */
static __attribute__((noinline)) int
blob_key_compare_suffix(blob_key_config     *cfg,
                        const blob_key_view *a,
                        const blob_key_view *b,
                        uint64               offset)
{
   cfg->counters.blob_fallbacks++;
   blob_key_cursor left;
   blob_key_cursor right;
   blob_key_callback_check(blob_key_cursor_init(cfg, &left, a, offset));
   blob_key_callback_check(blob_key_cursor_init(cfg, &right, b, offset));
   uint64 end    = MIN(a->length, b->length);
   int    result = 0;
   while (offset < end) {
      slice lpart;
      slice rpart;
      blob_key_callback_check(blob_key_cursor_get(cfg, &left, &lpart));
      blob_key_callback_check(blob_key_cursor_get(cfg, &right, &rpart));
      uint64 bytes = MIN(slice_length(lpart), slice_length(rpart));
      bytes        = MIN(bytes, end - offset);
      platform_assert(bytes != 0);
      cfg->counters.blob_bytes += bytes * (a->is_blob + b->is_blob);
      result = memcmp(slice_data(lpart), slice_data(rpart), bytes);
      if (result != 0) {
         break;
      }
      offset += bytes;
      if (offset < end) {
         blob_key_cursor_advance(&left, bytes);
         blob_key_cursor_advance(&right, bytes);
      }
   }
   blob_key_cursor_deinit(&right);
   blob_key_cursor_deinit(&left);
   return result == 0 ? blob_key_compare_lengths(a->length, b->length) : result;
}

static int
blob_key_compare(const data_config *data_cfg, user_key left, user_key right)
{
   blob_key_config *cfg = (blob_key_config *)data_cfg;
   cfg->counters.comparisons++;
   if (!cfg->blob_keys) {
      return slice_lex_cmp(left.key, right.key);
   }
   blob_key_view a;
   blob_key_view b;
   blob_key_callback_check(blob_key_view_init(cfg, left, &a));
   blob_key_callback_check(blob_key_view_init(cfg, right, &b));
   uint64 prefix = MIN(slice_length(a.prefix), slice_length(b.prefix));
   int    result =
      prefix ? memcmp(slice_data(a.prefix), slice_data(b.prefix), prefix) : 0;
   if (result != 0 || prefix == MIN(a.length, b.length)) {
      cfg->counters.prefix_resolved++;
      return result == 0 ? blob_key_compare_lengths(a.length, b.length)
                         : result;
   }
   return blob_key_compare_suffix(cfg, &a, &b, prefix);
}

/* FNV-1a is deliberately simple and independent of fragment boundaries. Both
 * inline and blob benchmark variants use the same nonproduction hash choice. */
static uint32
blob_key_hash_bytes(uint32 hash, slice bytes)
{
   const uint8 *data = slice_data(bytes);
   for (uint64 i = 0; i < slice_length(bytes); i++) {
      hash = (hash ^ data[i]) * UINT32_C(16777619);
   }
   return hash;
}

static __attribute__((noinline)) uint32
blob_key_hash_blob(blob_key_config *cfg, const blob_key_view *view, uint32 hash)
{
   blob_key_cursor cursor;
   blob_key_callback_check(blob_key_cursor_init(cfg, &cursor, view, 0));
   while (cursor.offset < view->length) {
      slice part;
      blob_key_callback_check(blob_key_cursor_get(cfg, &cursor, &part));
      cfg->counters.blob_bytes += slice_length(part);
      hash = blob_key_hash_bytes(hash, part);
      blob_key_cursor_advance(&cursor, slice_length(part));
   }
   blob_key_cursor_deinit(&cursor);
   return hash;
}

static uint32
blob_key_hash(const data_config *data_cfg, user_key input, uint32 seed)
{
   blob_key_config *cfg = (blob_key_config *)data_cfg;
   cfg->counters.hash_calls++;
   blob_key_view view;
   blob_key_callback_check(blob_key_view_init(cfg, input, &view));
   uint32 hash = UINT32_C(2166136261) ^ seed;
   return view.is_blob ? blob_key_hash_blob(cfg, &view, hash)
                       : blob_key_hash_bytes(hash, view.prefix);
}

static void
blob_key_to_string(const data_config *data_cfg,
                   user_key           input,
                   char              *str,
                   uint64             max_len)
{
   blob_key_config *cfg = (blob_key_config *)data_cfg;
   blob_key_view    view;
   platform_status  rc = blob_key_view_init(cfg, input, &view);
   if (!SUCCESS(rc)) {
      if (max_len != 0) {
         str[0] = '\0';
      }
      return;
   }
   debug_hex_encode(
      str, max_len, slice_data(view.prefix), slice_length(view.prefix));
}

platform_status
blob_key_config_init(blob_key_config *cfg,
                     cache           *cc,
                     bool32           blob_keys,
                     uint64           prefix_bytes)
{
   if (cfg == NULL || prefix_bytes > BLOB_KEY_MAX_PREFIX_BYTES
       || (blob_keys && cc == NULL))
   {
      return STATUS_BAD_PARAM;
   }
   ZERO_CONTENTS(cfg);
   default_data_config_init(&cfg->data_cfg);
   cfg->cc                     = cc;
   cfg->blob_keys              = blob_keys;
   cfg->prefix_bytes           = prefix_bytes;
   cfg->data_cfg.key_compare   = blob_key_compare;
   cfg->data_cfg.key_hash      = blob_key_hash;
   cfg->data_cfg.key_to_string = blob_key_to_string;
   /* Bytewise prefix compression is valid only for the raw inline variant. */
   cfg->data_cfg.key_compare_is_lexical = !blob_keys;
   return STATUS_OK;
}

void
blob_key_reset_counters(blob_key_config *cfg)
{
   ZERO_CONTENTS(&cfg->counters);
}

static platform_status
blob_key_assemble(uint64           length,
                  slice            prefix,
                  slice            descriptor,
                  writable_buffer *encoded)
{
   if (slice_length(descriptor) > BLOB_KEY_DESCRIPTOR_BYTES) {
      return STATUS_LIMIT_EXCEEDED;
   }
   uint64 bytes =
      sizeof(blob_key_header) + slice_length(prefix) + slice_length(descriptor);
   platform_status rc = writable_buffer_resize(encoded, bytes);
   if (!SUCCESS(rc)) {
      return rc;
   }
   blob_key_header *header = writable_buffer_data(encoded);
   *header                 = (blob_key_header){
                      .length       = length,
                      .prefix_bytes = slice_length(prefix),
                      .format       = BLOB_KEY_FORMAT,
   };
   char *dst = (char *)header + sizeof(*header);
   if (slice_length(prefix) != 0) {
      memcpy(dst, slice_data(prefix), slice_length(prefix));
   }
   memcpy(dst + slice_length(prefix),
          slice_data(descriptor),
          slice_length(descriptor));
   return STATUS_OK;
}

platform_status
blob_key_encode(blob_key_config         *cfg,
                const blob_build_config *build_cfg,
                mini_allocator          *mini,
                slice                    logical,
                writable_buffer         *encoded)
{
   if (!cfg->blob_keys) {
      return writable_buffer_copy_slice(encoded, logical);
   }
   blob_build_config compact        = *build_cfg;
   compact.blob_max_descriptor_size = BLOB_KEY_DESCRIPTOR_BYTES;
   auto_writable_buffer descriptor;
   writable_buffer_init(&descriptor, encoded->heap_id);
   platform_status rc =
      blob_build(&compact, cfg->cc, mini, logical, &descriptor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   slice prefix = slice_create(MIN(cfg->prefix_bytes, slice_length(logical)),
                               slice_data(logical));
   return blob_key_assemble(slice_length(logical),
                            prefix,
                            writable_buffer_to_slice(&descriptor),
                            encoded);
}

platform_status
blob_key_clone(blob_key_config         *cfg,
               const blob_build_config *build_cfg,
               mini_allocator          *mini,
               slice                    encoded,
               writable_buffer         *cloned)
{
   if (!cfg->blob_keys) {
      return writable_buffer_copy_slice(cloned, encoded);
   }
   blob_key_view   view;
   platform_status rc = blob_key_view_init(
      cfg, (user_key){.key = encoded, .is_query_key = FALSE}, &view);
   if (!SUCCESS(rc)) {
      return rc;
   }
   blob_build_config compact        = *build_cfg;
   compact.blob_max_descriptor_size = BLOB_KEY_DESCRIPTOR_BYTES;
   auto_writable_buffer descriptor;
   writable_buffer_init(&descriptor, cloned->heap_id);
   rc = blob_clone(&compact, cfg->cc, mini, view.descriptor, &descriptor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   return blob_key_assemble(
      view.length, view.prefix, writable_buffer_to_slice(&descriptor), cloned);
}

platform_status
blob_key_decode(blob_key_config *cfg, user_key input, writable_buffer *logical)
{
   blob_key_view   view;
   platform_status rc = blob_key_view_init(cfg, input, &view);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (!view.is_blob) {
      return writable_buffer_copy_slice(logical, input.key);
   }
   /* blob_materialize_full validates descriptor shape before page reads. */
   rc = blob_key_validate(cfg, input.key);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = blob_materialize_full(cfg->cc, view.descriptor, logical);
   if (SUCCESS(rc)) {
      cfg->counters.blob_bytes += view.length;
      if (slice_length(view.prefix) != 0
          && memcmp(writable_buffer_data(logical),
                    slice_data(view.prefix),
                    slice_length(view.prefix))
                != 0)
      {
         return STATUS_INVALID_STATE;
      }
   }
   return rc;
}
