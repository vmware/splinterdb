// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_assert.h"
#include "platform_util.h"
#include "platform_heap.h"

/*
 * -----------------------------------------------------------------------------
 * TYPED_MANUAL_MALLOC(), TYPED_MANUAL_ZALLOC() -
 * TYPED_ARRAY_MALLOC(),  TYPED_ARRAY_ZALLOC() -
 *
 * Utility macros to avoid common memory allocation / initialization mistakes.
 * NOTE: ZALLOC variants will also memset allocated memory chunk to 0.
 *
 * Call-flow is:
 *  TYPED_MALLOC()
 *   -> TYPED_ARRAY_MALLOC()
 *        -> TYPED_MANUAL_MALLOC() -> platform_aligned_malloc()
 *
 *  TYPED_ZALLOC()
 *   -> TYPED_ARRAY_ZALLOC()
 *        -> TYPED_MANUAL_ZALLOC() -> platform_aligned_zalloc()
 *
 * -----------------------------------------------------------------------------
 * Common mistake to make is:
 *    TYPE *foo = platform_malloc(sizeof(WRONG_TYPE));
 * or
 *    TYPE *foo;
 *    ...
 *    foo = platform_malloc(sizeof(WRONG_TYPE));
 * WRONG_TYPE may have been the correct type and the code changed, or you may
 * have the wrong number of '*'s, or you may have copied and pasted and forgot
 * to change the type/size.
 *
 * A useful pattern is:
 *    TYPE *foo = platform_malloc(sizeof(*foo))
 * but you can still cause a mistake by not typing the variable name correctly
 * twice.
 *
 * We _could_ make a macro MALLOC_AND_SET for the above pattern, e.g.
 *    define MALLOC_AND_SET(x) (x) = platform_malloc(sizeof(*x))
 * but that is hard to read.
 *
 * The boilerplate of extra typing (e.g. remember to type x twice) isn't a big
 * problem, it's just that you can get the types wrong and the compiler won't
 * notice.
 * We can keep the easy-to-read pattern of `x = function(x)` with typesafety by
 * including a cast inside the macro.
 *
 * These macros let you avoid the avoid the mistakes by typing malloc.
 *
 *    struct foo *foo_pointer = TYPED_MALLOC(foo_pointer);
 *    struct foo *foo_pointer2;
 *    foo_pointer2 = TYPED_MALLOC(foo_pointer2);
 *
 * To replace dynamic/vla arrays, use TYPED_ARRAY_MALLOC, e.g.
 *    struct foo array[X()];
 * becomes
 *    struct foo *array = TYPED_ARRAY_MALLOC(array, X());
 * ZALLOC versions will also memset to 0.
 *
 * All mallocs here are cache aligned.
 * Consider the following types:
 * struct unaligned_foo {
 *   cache_aligned_type bar;
 * };
 * If you (unaligned) malloc a 'unaligned_foo', the compile is still allowed to
 * assume that bar is properly cache aligned.  It may do unsafe optimizations.
 * One known unsafe optimization is turning memset(...,0,...) into avx
 * instructions that crash if something is not aligned.
 *
 * The simplest solution is to simply align ALL mallocs.
 * We do not do a sufficient number of mallocs for this to have any major
 * problems.  The slight additional memory usage (for tiny mallocs) should not
 * matter.  The potential minor perf hit should also not matter due to us
 * slowly coalescing all mallocs anyway into either initialization or amortized
 * situations.
 *
 * Alternative solutions are to be careful with mallocs, and/or make ALL structs
 * be aligned.
 *
 * Another common use case is if you have a struct with a flexible array member.
 * In that case you should use TYPED_FLEXIBLE_STRUCT_(M|Z)ALLOC
 *
 * If you are doing memory size calculation manually (e.g. if you're avoiding
 * multiple mallocs by doing one larger malloc and setting pointers manually,
 * or the data type has a something[] or something[0] at the end) you should
 * instead use the TYPED_*ALLOC_MANUAL macros that allow you to provide the
 * exact size.  These macros currently assume (and in debug mode assert) that
 * you will never malloc LESS than the struct/type size.
 *
 * DO NOT USE these macros to assign to a void*.  The debug asserts will cause
 * a compile error when debug is on.  Assigning to a void* should be done by
 * calling aligned_alloc manually (or create a separate macro)
 *
 * Parameters:
 *	hid - Platform heap-ID to allocate memory from.
 *	v   - Structure to allocate memory for.
 *	n   - Number of bytes of memory to allocate.
 * -----------------------------------------------------------------------------
 */
#define TYPED_MANUAL_MALLOC(hid, v, n)                                         \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_malloc(hid,                                  \
                                         PLATFORM_CACHELINE_SIZE,              \
                                         (n),                                  \
                                         STRINGIFY(v),                         \
                                         __func__,                             \
                                         __FILE__,                             \
                                         __LINE__);                            \
   })
#define TYPED_MANUAL_ZALLOC(hid, v, n)                                         \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_zalloc(hid,                                  \
                                         PLATFORM_CACHELINE_SIZE,              \
                                         (n),                                  \
                                         STRINGIFY(v),                         \
                                         __func__,                             \
                                         __FILE__,                             \
                                         __LINE__);                            \
   })

/*
 * TYPED_ALIGNED_MALLOC(), TYPED_ALIGNED_ZALLOC()
 *
 * Allocate memory for a typed structure at caller-specified alignment.
 * These are similar to TYPED_MANUAL_MALLOC() & TYPED_MANUAL_ZALLOC() but with
 * the difference that the alignment is caller-specified.
 *
 * Parameters:
 *	hid - Platform heap-ID to allocate memory from.
 *	a   - Alignment needed for allocated memory.
 *	v   - Structure to allocate memory for.
 *	n   - Number of bytes of memory to allocate.
 */
#define TYPED_ALIGNED_MALLOC(hid, a, v, n)                                     \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_malloc(                                      \
         hid, (a), (n), STRINGIFY(v), __func__, __FILE__, __LINE__);           \
   })
#define TYPED_ALIGNED_ZALLOC(hid, a, v, n)                                     \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_zalloc(                                      \
         hid, (a), (n), STRINGIFY(v), __func__, __FILE__, __LINE__);           \
   })

/*
 * FLEXIBLE_STRUCT_SIZE(): Compute the size of a structure 'v' with a nested
 * flexible array member, array_field_name, with 'n' members.
 *
 * Flexible array members don't necessarily start after sizeof(v)
 * They can start within the padding at the end, so the correct size
 * needed to allocate a struct with a flexible array member is the
 * larger of sizeof(struct v) or (offset of flexible array +
 * n*sizeof(arraymember))
 *
 * The only reasonable static assert we can do is check that the flexible array
 * member is actually an array.  We cannot check size==0 (compile error), and
 * since it doesn't necessarily start at the end we also cannot check
 * offset==sizeof.
 *
 * Parameters:
 *  v                   - Structure to allocate memory for.
 *  array_field_name    - Name of flexible array field nested in 'v'
 *  n                   - Number of members in array_field_name[].
 */
#define FLEXIBLE_STRUCT_SIZE(v, array_field_name, n)                           \
   ({                                                                          \
      _Static_assert(IS_ARRAY((v)->array_field_name),                          \
                     "flexible array members must be arrays");                 \
      max_size_t(sizeof(*(v)),                                                 \
                 (n) * sizeof((v)->array_field_name[0])                        \
                    + offsetof(typeof(*(v)), array_field_name));               \
   })

/*
 * -----------------------------------------------------------------------------
 * TYPED_FLEXIBLE_STRUCT_MALLOC(), TYPED_FLEXIBLE_STRUCT_ZALLOC() -
 *    Allocate memory for a structure with a nested flexible array member.
 *
 * Parameters:
 *  hid                 - Platform heap-ID to allocate memory from.
 *  v                   - Structure to allocate memory for.
 *  array_field_name    - Name of flexible array field nested in 'v'
 *  n                   - Number of members in array_field_name[].
 * -----------------------------------------------------------------------------
 */
#define TYPED_FLEXIBLE_STRUCT_MALLOC(hid, v, array_field_name, n)              \
   TYPED_MANUAL_MALLOC(                                                        \
      hid, (v), FLEXIBLE_STRUCT_SIZE((v), array_field_name, (n)))

#define TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, v, array_field_name, n)              \
   TYPED_MANUAL_ZALLOC(                                                        \
      hid, (v), FLEXIBLE_STRUCT_SIZE((v), array_field_name, (n)))

/*
 * TYPED_ARRAY_MALLOC(), TYPED_ARRAY_ZALLOC()
 * Allocate memory for an array of 'n' elements of structure 'v'.
 *
 * Parameters:
 *  hid - Platform heap-ID to allocate memory from.
 *  v   - Structure to allocate memory for.
 *  n   - Number of members of type 'v' in array.
 */
#define TYPED_ARRAY_MALLOC(hid, v, n)                                          \
   TYPED_MANUAL_MALLOC(hid, (v), (n) * sizeof(*(v)))
#define TYPED_ARRAY_ZALLOC(hid, v, n)                                          \
   TYPED_MANUAL_ZALLOC(hid, (v), (n) * sizeof(*(v)))

/*
 * TYPED_ARRAY_MALLOC(), TYPED_ARRAY_ZALLOC()
 * Allocate memory for one element of structure 'v'.
 *
 * Parameters:
 *  hid - Platform heap-ID to allocate memory from.
 *  v   - Structure to allocate memory for.
 */
#define TYPED_MALLOC(hid, v) TYPED_ARRAY_MALLOC(hid, v, 1)
#define TYPED_ZALLOC(hid, v) TYPED_ARRAY_ZALLOC(hid, v, 1)
