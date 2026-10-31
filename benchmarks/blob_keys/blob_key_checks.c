// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "blob_key_checks.h"

#define CHECK(expr)                                                            \
   do {                                                                        \
      if (!(expr)) {                                                           \
         platform_error_log("blob-key check failed at %s:%d: %s\n",            \
                            __FILE__,                                          \
                            __LINE__,                                          \
                            #expr);                                            \
         return STATUS_TEST_FAILED;                                            \
      }                                                                        \
   } while (0)
#define CHECK_OK(expr) CHECK(SUCCESS(expr))

static int
sign_of(int value)
{
   return (value > 0) - (value < 0);
}

static user_key
stored_key(writable_buffer *buffer)
{
   return (user_key){.key          = writable_buffer_to_slice(buffer),
                     .is_query_key = FALSE};
}

static platform_status
check_length(blob_key_config         *cfg,
             const blob_build_config *build_cfg,
             mini_allocator          *mini,
             platform_heap_id         hid,
             uint64                   length)
{
   auto_writable_buffer logical;
   auto_writable_buffer first;
   auto_writable_buffer second;
   auto_writable_buffer decoded;
   writable_buffer_init(&logical, hid);
   writable_buffer_init(&first, hid);
   writable_buffer_init(&second, hid);
   writable_buffer_init(&decoded, hid);
   /* Keep a non-NULL source pointer even for the empty logical key. */
   CHECK_OK(writable_buffer_resize(&logical, length + 1));
   uint8 *bytes = writable_buffer_data(&logical);
   for (uint64 i = 0; i < length; i++) {
      bytes[i] = (uint8)((i * UINT64_C(157) + (i >> 8)) & 255);
   }
   user_key query = {.key = slice_create(length, bytes), .is_query_key = TRUE};
   CHECK_OK(blob_key_encode(cfg, build_cfg, mini, query.key, &first));
   CHECK_OK(blob_key_encode(cfg, build_cfg, mini, query.key, &second));
   CHECK_OK(blob_key_validate(cfg, writable_buffer_to_slice(&first)));
   CHECK_OK(blob_key_decode(cfg, stored_key(&first), &decoded));
   CHECK(writable_buffer_length(&decoded) == length);
   CHECK(length == 0
         || memcmp(writable_buffer_data(&decoded), bytes, length) == 0);

   data_config *dc = &cfg->data_cfg;
   CHECK(dc->key_compare(dc, stored_key(&first), stored_key(&second)) == 0);
   CHECK(dc->key_compare(dc, stored_key(&first), query) == 0);
   CHECK(dc->key_compare(dc, query, stored_key(&first)) == 0);
   CHECK(dc->key_compare(dc, query, query) == 0);
   blob_key_config inline_cfg;
   CHECK_OK(blob_key_config_init(&inline_cfg, cfg->cc, FALSE, 0));
   const uint32 seeds[] = {0, 1, UINT32_MAX};
   for (uint64 i = 0; i < ARRAY_SIZE(seeds); i++) {
      uint32 expected =
         inline_cfg.data_cfg.key_hash(&inline_cfg.data_cfg, query, seeds[i]);
      CHECK(dc->key_hash(dc, query, seeds[i]) == expected);
      CHECK(dc->key_hash(dc, stored_key(&first), seeds[i]) == expected);
      CHECK(dc->key_hash(dc, stored_key(&second), seeds[i]) == expected);
   }
   CHECK_OK(blob_key_decode(cfg, query, &decoded));
   CHECK(writable_buffer_length(&decoded) == slice_length(query.key));
   CHECK(slice_length(query.key) == 0
         || memcmp(writable_buffer_data(&decoded),
                   slice_data(query.key),
                   slice_length(query.key))
               == 0);

   if (length != 0) {
      /* A key that is a proper prefix sorts first, including an empty prefix.
       */
      user_key shorter = {.key          = slice_create(length / 2, bytes),
                          .is_query_key = TRUE};
      CHECK_OK(blob_key_encode(cfg, build_cfg, mini, shorter.key, &second));
      CHECK(dc->key_compare(dc, stored_key(&second), stored_key(&first)) < 0);
      CHECK(dc->key_compare(dc, stored_key(&first), stored_key(&second)) > 0);
      CHECK(dc->key_compare(dc, shorter, stored_key(&first)) < 0);
      CHECK(dc->key_compare(dc, stored_key(&first), shorter) > 0);

      /* Different first/last bytes exercise prefix and streamed suffix paths.
       */
      const uint64 positions[] = {0, length - 1};
      for (uint64 p = 0; p < ARRAY_SIZE(positions); p++) {
         uint64 at     = positions[p];
         uint8  before = bytes[at];
         bytes[at] ^= 0x80;
         int expected = (before > bytes[at]) - (before < bytes[at]);
         CHECK_OK(blob_key_encode(cfg, build_cfg, mini, query.key, &second));
         CHECK(sign_of(
                  dc->key_compare(dc, stored_key(&first), stored_key(&second)))
               == expected);
         CHECK(sign_of(
                  dc->key_compare(dc, stored_key(&second), stored_key(&first)))
               == -expected);
         CHECK(sign_of(dc->key_compare(dc, stored_key(&first), query))
               == expected);
         CHECK(sign_of(dc->key_compare(dc, query, stored_key(&first)))
               == -expected);
         bytes[at] = before;
      }
   }
   return STATUS_OK;
}

static platform_status
check_malformed(blob_key_config         *cfg,
                const blob_build_config *build_cfg,
                mini_allocator          *mini,
                platform_heap_id         hid)
{
   uint8 logical[65];
   for (uint64 i = 0; i < sizeof(logical); i++) {
      logical[i] = (uint8)i;
   }
   auto_writable_buffer original;
   auto_writable_buffer changed;
   auto_writable_buffer decoded;
   writable_buffer_init(&original, hid);
   writable_buffer_init(&changed, hid);
   writable_buffer_init(&decoded, hid);
   CHECK_OK(blob_key_encode(
      cfg, build_cfg, mini, slice_create(sizeof(logical), logical), &original));
   slice valid = writable_buffer_to_slice(&original);
   /* Codec v1 envelope: uint64 length, uint8 prefix length, uint8 format. */
   const uint64 header_bytes = 10;
   for (uint64 length = 0; length < header_bytes; length++) {
      slice short_key = slice_create(length, slice_data(valid));
      CHECK(!SUCCESS(blob_key_validate(cfg, short_key)));
      CHECK(!SUCCESS(
         blob_key_decode(cfg, (user_key){.key = short_key}, &decoded)));
   }
   for (uint64 variant = 0; variant < 6; variant++) {
      CHECK_OK(writable_buffer_copy_slice(&changed, valid));
      uint8 *bytes      = writable_buffer_data(&changed);
      uint64 descriptor = header_bytes + bytes[8];
      switch (variant) {
         case 0:
            bytes[8] = BLOB_KEY_MAX_PREFIX_BYTES + 1;
            break;
         case 1:
            bytes[9] = 0;
            break;
         case 2:
         {
            uint64 wrong_length = sizeof(logical) + 1;
            memcpy(bytes, &wrong_length, sizeof(wrong_length));
            break;
         }
         case 3:
         {
            uint16 wrong_format = 0;
            memcpy(bytes + descriptor + offsetof(blob, format),
                   &wrong_format,
                   sizeof(wrong_format));
            break;
         }
         case 4:
            CHECK_OK(writable_buffer_resize(
               &changed, writable_buffer_length(&changed) - 1));
            break;
         case 5:
         {
            uint64 invalid_addr = 0;
            memcpy(bytes + descriptor + sizeof(blob),
                   &invalid_addr,
                   sizeof(invalid_addr));
            break;
         }
      }
      CHECK(
         !SUCCESS(blob_key_validate(cfg, writable_buffer_to_slice(&changed))));
      CHECK(!SUCCESS(blob_key_decode(cfg, stored_key(&changed), &decoded)));
   }
   if (cfg->prefix_bytes != 0) {
      CHECK_OK(writable_buffer_copy_slice(&changed, valid));
      ((uint8 *)writable_buffer_data(&changed))[header_bytes] ^= 0x80;
      /* Shape-only validation cannot detect this; materialization must. */
      CHECK_OK(blob_key_validate(cfg, writable_buffer_to_slice(&changed)));
      CHECK(!SUCCESS(blob_key_decode(cfg, stored_key(&changed), &decoded)));
   }
   return STATUS_OK;
}

platform_status
blob_key_checks(blob_key_config         *cfg,
                const blob_build_config *build_cfg,
                mini_allocator          *mini,
                platform_heap_id         hid)
{
   blob_key_config invalid;
   CHECK(!SUCCESS(blob_key_config_init(
      &invalid, cfg->cc, TRUE, BLOB_KEY_MAX_PREFIX_BYTES + 1)));
   CHECK(!SUCCESS(blob_key_config_init(&invalid, NULL, TRUE, 0)));
   uint64       page      = cache_page_size(cfg->cc);
   uint64       extent    = cache_extent_size(cfg->cc);
   const uint64 lengths[] = {
      0,
      1,
      7,
      16,
      63,
      64,
      65,
      page - 1,
      page,
      page + 1,
      extent - 1,
      extent,
      extent + 1,
      /* With 128 descriptor bytes, thirteen extents require an indirect tree.
       */
      13 * extent,
   };
   const uint64 prefixes[] = {0, 16, BLOB_KEY_MAX_PREFIX_BYTES};
   for (uint64 p = 0; p < ARRAY_SIZE(prefixes); p++) {
      blob_key_config check_cfg;
      CHECK_OK(blob_key_config_init(&check_cfg, cfg->cc, TRUE, prefixes[p]));
      for (uint64 i = 0; i < ARRAY_SIZE(lengths); i++) {
         CHECK_OK(check_length(&check_cfg, build_cfg, mini, hid, lengths[i]));
      }
      CHECK_OK(check_malformed(&check_cfg, build_cfg, mini, hid));
   }
   return STATUS_OK;
}
