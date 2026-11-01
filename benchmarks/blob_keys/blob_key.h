// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "blob_build.h"
#include "data_internal.h"

/*
 * Experimental codec for the real-cache/B-tree benchmark. Not a public API
 * or persistent database format. Counters and callbacks are single-threaded.
 * This codec implements unsigned bytewise lexical ordering only. Its prefix
 * shortcut belongs inside that comparator; arbitrary application comparators
 * must receive logical bytes and cannot use this shortcut without a contract.
 */
#define BLOB_KEY_MAX_PREFIX_BYTES 64
#define BLOB_KEY_DESCRIPTOR_BYTES 128

typedef struct blob_key_counters {
   uint64 comparisons;
   uint64 prefix_resolved;
   uint64 blob_fallbacks;
   /* Comparator/hash fragment visits; decode uses blob_materialize_full. */
   uint64 blob_fragments;
   /* Logical bytes passed to memcmp/hash/copy from blob fragments, not I/O. */
   uint64 blob_bytes;
   uint64 hash_calls;
} blob_key_counters;

typedef struct blob_key_config {
   data_config       data_cfg; /* Must remain first: callback downcast. */
   cache            *cc;
   bool32            blob_keys;
   uint64            prefix_bytes;
   blob_key_counters counters;
} blob_key_config;

platform_status
blob_key_config_init(blob_key_config *cfg,
                     cache           *cc,
                     bool32           blob_keys,
                     uint64           prefix_bytes);

void
blob_key_reset_counters(blob_key_config *cfg);

/* Output buffers must not alias input storage. Blob batches belong to mini. */
platform_status
blob_key_encode(blob_key_config         *cfg,
                const blob_build_config *build_cfg,
                mini_allocator          *mini,
                slice                    logical,
                writable_buffer         *encoded);

platform_status
blob_key_clone(blob_key_config         *cfg,
               const blob_build_config *build_cfg,
               mini_allocator          *mini,
               slice                    encoded,
               writable_buffer         *cloned);

platform_status
blob_key_decode(blob_key_config *cfg,
                user_key         encoded_or_query,
                writable_buffer *logical);

/* Validate the encoded envelope and descriptor without reading blob pages. */
platform_status
blob_key_validate(blob_key_config *cfg, slice encoded);
