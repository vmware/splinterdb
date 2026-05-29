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
 */
typedef struct ondisk_ref {
   cache      *cc;
   uint64      addr;
   page_type   type;
   const void *arg;
   void (*inc)(const struct ondisk_ref *ref);
   void (*dec)(const struct ondisk_ref *ref);
} ondisk_ref;

#define ONDISK_REF_NULL ((ondisk_ref){0})

static inline bool32
ondisk_ref_is_null(const ondisk_ref *ref)
{
   return ref == NULL || ref->dec == NULL;
}

static inline void
ondisk_ref_inc(const ondisk_ref *ref)
{
   if (!ondisk_ref_is_null(ref)) {
      debug_assert(ref->inc != NULL);
      ref->inc(ref);
   }
}

static inline void
ondisk_ref_dec(ondisk_ref *ref)
{
   if (!ondisk_ref_is_null(ref)) {
      ondisk_ref ref_copy = *ref;
      *ref                = ONDISK_REF_NULL;
      ref_copy.dec(&ref_copy);
   }
}
