// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "btree.h"

/* These views describe logical keys; descriptor bytes never participate in
 * ordering. P comes from the owning tree, not from an individual entry. */
typedef struct {
   slice  prefix;
   slice  descriptor;
   uint64 length;
   bool32 blob;
} btree_key_view;

static platform_status
btree_key_view_init(const btree_config *cfg, key k, btree_key_view *view)
{
   *view = (btree_key_view){
      .prefix = key_slice(k), .length = key_length(k), .blob = k.is_blob};
   if (!k.is_blob) {
      return STATUS_OK;
   }
   uint64 p = cfg->blob_key_prefix_size;
   if (cfg->cc == NULL || p > key_length(k) || key_length(k) - p < sizeof(blob))
   {
      return STATUS_INVALID_STATE;
   }
   view->prefix = slice_create(p, key_data(k));
   view->descriptor =
      slice_create(key_length(k) - p, (const char *)key_data(k) + p);
   parsed_blob     parsed;
   platform_status rc =
      blob_parse_descriptor(cfg->cc, view->descriptor, &parsed);
   if (!SUCCESS(rc)) {
      return rc;
   }
   view->length = blob_length(view->descriptor);
   if (view->length <= MAX_INLINE_KEY_SIZE(cfg->page_size) || view->length <= p)
   {
      return STATUS_INVALID_STATE;
   }
   return STATUS_OK;
}

/* Keep descriptor parsing and its scratch frame out of inline-key searches,
 * including when link-time optimization can see this definition. */
__attribute__((noinline)) platform_status
btree_key_validate(const btree_config *cfg, key k)
{
   if (!key_is_user_key(k)) {
      return k.is_blob ? STATUS_INVALID_STATE : STATUS_OK;
   }
   if (key_length(k) > MAX_INLINE_KEY_SIZE(cfg->page_size)) {
      return STATUS_INVALID_STATE;
   }
   btree_key_view view;
   return btree_key_view_init(cfg, k, &view);
}

platform_status
btree_key_materialize(const btree_config *cfg, key stored, key_buffer *result)
{
   if (!stored.is_blob) {
      return key_buffer_copy_key(result, stored);
   }
   btree_key_view  view;
   platform_status rc = btree_key_view_init(cfg, stored, &view);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = blob_materialize_full(cfg->cc, view.descriptor, &result->wb);
   if (SUCCESS(rc)) {
      result->kind         = USER_KEY;
      result->is_query_key = stored.is_query_key;
      result->is_blob      = FALSE;
      if (slice_length(view.prefix)
          && memcmp(key_buffer_data(result),
                    slice_data(view.prefix),
                    slice_length(view.prefix))
                != 0)
      {
         return STATUS_INVALID_STATE;
      }
   }
   return rc;
}

platform_status
btree_key_encode(const btree_config *cfg,
                 mini_allocator     *mini,
                 key                 logical,
                 key_buffer         *result)
{
   platform_assert(!logical.is_blob);
   if (key_length(logical) <= MAX_INLINE_KEY_SIZE(cfg->page_size)) {
      return key_buffer_copy_key(result, logical);
   }
   auto_writable_buffer descriptor;
   writable_buffer_init(&descriptor, result->wb.heap_id);
   platform_status rc = mini_blob_build(
      &cfg->blob_cfg, cfg->cc, mini, key_slice(logical), &descriptor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   uint64 p = cfg->blob_key_prefix_size;
   uint64 d = writable_buffer_length(&descriptor);
   if (p > MAX_INLINE_KEY_SIZE(cfg->page_size)
       || d > MAX_INLINE_KEY_SIZE(cfg->page_size) - p)
   {
      return STATUS_LIMIT_EXCEEDED;
   }
   rc = key_buffer_resize(result, p + d);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (p) {
      memcpy(key_buffer_data(result), key_data(logical), p);
   }
   memcpy((char *)key_buffer_data(result) + p,
          writable_buffer_data(&descriptor),
          d);
   result->is_blob      = TRUE;
   result->is_query_key = FALSE;
   return STATUS_OK;
}

typedef struct {
   const btree_key_view *view;
   blob_page_iterator    iter;
   uint64                offset;
   bool32                live;
} btree_key_cursor;

static platform_status
btree_key_cursor_init(const btree_config   *cfg,
                      btree_key_cursor     *c,
                      const btree_key_view *view,
                      uint64                offset)
{
   *c = (btree_key_cursor){.view = view, .offset = offset};
   if (!view->blob || offset == view->length) {
      return STATUS_OK;
   }
   platform_status rc =
      blob_page_iterator_init(cfg->cc,
                              &c->iter,
                              view->descriptor,
                              offset,
                              BLOB_PAGE_ITERATOR_MODE_NO_PREFETCH);
   c->live = SUCCESS(rc);
   return rc;
}

static void
btree_key_cursor_deinit(btree_key_cursor *c)
{
   if (c->live) {
      blob_page_iterator_deinit(&c->iter);
   }
}

static platform_status
btree_key_cursor_get(btree_key_cursor *c, slice *part)
{
   if (!c->view->blob) {
      *part =
         slice_create(c->view->length - c->offset,
                      (const char *)slice_data(c->view->prefix) + c->offset);
      return STATUS_OK;
   }
   uint64 offset;
   return blob_page_iterator_get_curr(&c->iter, &offset, part);
}

static void
btree_key_cursor_advance(btree_key_cursor *c, uint64 bytes)
{
   c->offset += bytes;
   if (c->live) {
      blob_page_iterator_advance_bytes(&c->iter, bytes);
   }
}

static platform_status
btree_key_compare_streamed(const btree_config   *cfg,
                           const btree_key_view *a,
                           const btree_key_view *b,
                           uint64                offset,
                           int                  *cmp)
{
   btree_key_cursor left = {0}, right = {0};
   platform_status  rc = btree_key_cursor_init(cfg, &left, a, offset);
   if (!SUCCESS(rc)) {
      goto out;
   }
   rc = btree_key_cursor_init(cfg, &right, b, offset);
   if (!SUCCESS(rc)) {
      goto out;
   }
   uint64 end = MIN(a->length, b->length);
   *cmp       = 0;
   while (offset < end) {
      slice lpart, rpart;
      rc = btree_key_cursor_get(&left, &lpart);
      if (!SUCCESS(rc)) {
         goto out;
      }
      rc = btree_key_cursor_get(&right, &rpart);
      if (!SUCCESS(rc)) {
         goto out;
      }
      uint64 bytes =
         MIN(MIN(slice_length(lpart), slice_length(rpart)), end - offset);
      if (bytes == 0) {
         rc = STATUS_INVALID_STATE;
         goto out;
      }
      *cmp = memcmp(slice_data(lpart), slice_data(rpart), bytes);
      if (*cmp) {
         goto out;
      }
      offset += bytes;
      if (offset < end) {
         btree_key_cursor_advance(&left, bytes);
         btree_key_cursor_advance(&right, bytes);
      }
   }
   *cmp = (a->length > b->length) - (a->length < b->length);
out:
   btree_key_cursor_deinit(&right);
   btree_key_cursor_deinit(&left);
   return rc;
}

/* The inline comparison is in btree_key_compare_checked(). Do not pull the
 * blob cursor and materialization paths into every ordinary pivot search. */
__attribute__((noinline)) platform_status
btree_blob_key_compare(const btree_config *cfg, key a, key b, int *cmp)
{
   if (!key_is_user_key(a) || !key_is_user_key(b) || (!a.is_blob && !b.is_blob))
   {
      *cmp = data_key_compare(cfg->data_cfg, a, b);
      return STATUS_OK;
   }
   btree_key_view  av, bv;
   platform_status rc = btree_key_view_init(cfg, a, &av);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = btree_key_view_init(cfg, b, &bv);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (cfg->data_cfg->key_compare_is_lexical) {
      uint64 common = MIN(slice_length(av.prefix), slice_length(bv.prefix));
      *cmp          = common
                         ? memcmp(slice_data(av.prefix), slice_data(bv.prefix), common)
                         : 0;
      if (*cmp || common == MIN(av.length, bv.length)) {
         if (!*cmp) {
            *cmp = (av.length > bv.length) - (av.length < bv.length);
         }
         return STATUS_OK;
      }
      return btree_key_compare_streamed(cfg, &av, &bv, common, cmp);
   }
   DECLARE_AUTO_KEY_BUFFER(left, PROCESS_PRIVATE_HEAP_ID);
   DECLARE_AUTO_KEY_BUFFER(right, PROCESS_PRIVATE_HEAP_ID);
   rc = btree_key_materialize(cfg, a, &left);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = btree_key_materialize(cfg, b, &right);
   if (SUCCESS(rc)) {
      *cmp = data_key_compare(
         cfg->data_cfg, key_buffer_key(&left), key_buffer_key(&right));
   }
   return rc;
}
