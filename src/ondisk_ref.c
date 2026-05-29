// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "ondisk_ref.h"
#include "poison.h"

bool32
ondisk_ref_is_null(const ondisk_ref *ref)
{
   return ref == NULL || ref->dec == NULL;
}

static void
ondisk_ref_assert_valid(const ondisk_ref *ref)
{
   debug_assert(!ondisk_ref_is_null(ref));
   debug_assert(ref->self == ref);
   debug_assert(ref->inc != NULL);
   debug_assert(ref->dec != NULL);
}

static void
ondisk_ref_clear(ondisk_ref *ref)
{
   ref->cc   = NULL;
   ref->addr = 0;
   ref->type = PAGE_TYPE_INVALID;
   ref->arg  = NULL;
   ref->self = NULL;
   ref->inc  = NULL;
   ref->dec  = NULL;
}

static void
ondisk_ref_init_internal(ondisk_ref *ref,
                         cache      *cc,
                         uint64      addr,
                         page_type   type,
                         const void *arg,
                         void (*inc)(const ondisk_ref *ref),
                         void (*dec)(const ondisk_ref *ref))
{
   ref->cc   = cc;
   ref->addr = addr;
   ref->type = type;
   ref->arg  = arg;
   ref->self = ref;
   ref->inc  = inc;
   ref->dec  = dec;
}

static void
ondisk_ref_inc(const ondisk_ref *ref)
{
   if (!ondisk_ref_is_null(ref)) {
      ondisk_ref_assert_valid(ref);
      ref->inc(ref);
   }
}

static void
ondisk_ref_copy_fields(ondisk_ref *dst, const ondisk_ref *src)
{
   dst->cc   = src->cc;
   dst->addr = src->addr;
   dst->type = src->type;
   dst->arg  = src->arg;
   dst->self = dst;
   dst->inc  = src->inc;
   dst->dec  = src->dec;
}

void
ondisk_ref_init_null(ondisk_ref *ref)
{
   ondisk_ref_clear(ref);
}

void
ondisk_ref_init(ondisk_ref *ref,
                cache      *cc,
                uint64      addr,
                page_type   type,
                const void *arg,
                void (*inc)(const ondisk_ref *ref),
                void (*dec)(const ondisk_ref *ref))
{
   ondisk_ref_init_internal(ref, cc, addr, type, arg, inc, dec);
   ondisk_ref_inc(ref);
}

void
ondisk_ref_init_adopt(ondisk_ref *ref,
                      cache      *cc,
                      uint64      addr,
                      page_type   type,
                      const void *arg,
                      void (*inc)(const ondisk_ref *ref),
                      void (*dec)(const ondisk_ref *ref))
{
   ondisk_ref_init_internal(ref, cc, addr, type, arg, inc, dec);
}

void
ondisk_ref_deinit(ondisk_ref *ref)
{
   if (!ondisk_ref_is_null(ref)) {
      ondisk_ref_assert_valid(ref);
      ondisk_ref ref_copy = ONDISK_REF_NULL;
      ondisk_ref_copy_fields(&ref_copy, ref);
      ondisk_ref_clear(ref);
      ref_copy.dec(&ref_copy);
      ondisk_ref_clear(&ref_copy);
   }
}

void
ondisk_ref_copy(ondisk_ref *dst, const ondisk_ref *src)
{
   debug_assert(dst != src);
   if (ondisk_ref_is_null(src)) {
      ondisk_ref_init_null(dst);
      return;
   }
   ondisk_ref_inc(src);
   ondisk_ref_copy_fields(dst, src);
}

void
ondisk_ref_replace(ondisk_ref *dst, const ondisk_ref *src)
{
   if (dst == src) {
      return;
   }

   bool32 has_src = !ondisk_ref_is_null(src);
   if (has_src) {
      ondisk_ref_inc(src);
   }

   ondisk_ref_deinit(dst);
   if (has_src) {
      ondisk_ref_copy_fields(dst, src);
   } else {
      ondisk_ref_clear(dst);
   }
}
