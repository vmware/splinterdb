// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * ondisk_ref.h --
 *
 *     In-memory ownership references for allocator-backed on-disk objects.
 */

#pragma once

#include "allocator.h"
#include "cache.h"

/*
 * ondisk_ref is an in-memory owner for an allocator-backed on-disk object.
 * Callers provide object-specific inc/dec callbacks so the ref can protect
 * roots of compound structures such as trunk nodes or btree mini-allocators.
 *
 * Lifecycle mirrors RAII:
 *    init       acquires a new reference
 *    init_adopt records an already-owned reference
 *    deinit     releases the held reference
 *    copy       initializes dst with a newly acquired copy of src
 *    replace    releases dst after acquiring src
 */
typedef struct ondisk_ref {
   cache                   *cc;
   uint64                   addr;
   page_type                type;
   const void              *arg;
   const struct ondisk_ref *self;
   void (*inc)(const struct ondisk_ref *ref);
   void (*dec)(const struct ondisk_ref *ref);
   const uint8 no_copy;
} ondisk_ref;

#define ONDISK_REF_NULL ((ondisk_ref){0})

bool32
ondisk_ref_is_null(const ondisk_ref *ref);

void
ondisk_ref_init_null(ondisk_ref *ref);

void
ondisk_ref_init(ondisk_ref *ref,
                cache      *cc,
                uint64      addr,
                page_type   type,
                const void *arg,
                void (*inc)(const ondisk_ref *ref),
                void (*dec)(const ondisk_ref *ref));

void
ondisk_ref_init_adopt(ondisk_ref *ref,
                      cache      *cc,
                      uint64      addr,
                      page_type   type,
                      const void *arg,
                      void (*inc)(const ondisk_ref *ref),
                      void (*dec)(const ondisk_ref *ref));

void
ondisk_ref_deinit(ondisk_ref *ref);

void
ondisk_ref_copy(ondisk_ref *dst, const ondisk_ref *src);

void
ondisk_ref_replace(ondisk_ref *dst, const ondisk_ref *src);
