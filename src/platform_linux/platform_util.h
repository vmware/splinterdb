// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * This file could likely be moved into a platform_common directory that could
 * serve as a common library for all platforms.
 */

#pragma once
#include "splinterdb/platform_linux/public_platform.h"
#include <stddef.h>

#define IS_POWER_OF_2(n) ((n) > 0 && ((n) & ((n) - 1)) == 0)

#ifndef MAX
#   define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

#ifndef MIN
#   define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

static inline size_t
max_size_t(size_t a, size_t b)
{
   return a > b ? a : b;
}

/* calculate difference between two pointers */
static inline ptrdiff_t
diff_ptr(const void *base, const void *limit)
{
   _Static_assert(sizeof(char) == 1, "Assumption violated");
   return (char *)limit - (char *)base;
}

/*
 * Helper macro that takes a pointer, type of the container, and the
 * name of the member the pointer refers to. The macro expands to a
 * new address pointing to the container which accomodates the
 * specified  member.
 */
#ifndef container_of
#   define container_of(ptr, type, memb)                                       \
      ((type *)((char *)(ptr) - offsetof(type, memb)))
#endif

/*
 * C11 and higher already supports native _Static_assert which has good
 * compiler output on errors
 * Since we are lower than C11, we need something that works.
 */
#if !defined(__STDC_VERSION__) || __STDC_VERSION__ < 201112l
#   define _Static_assert(expr, str)                                           \
      do {                                                                     \
         typedef char oc_assert_fail[((expr) ? 1 : -1)]                        \
            __attribute__((__unused__));                                       \
      } while (0)
#endif


// Helper function so that ARRAYSIZE can be used inside of _Static_assert
#define ASSERT_EXPR(condition, return_value)                                   \
   (sizeof(char[(condition) ? 1 : -1]) ? (return_value) : (return_value))

/*
 * Utility macro to test if an indexable expression is an array.
 * Gives an error at compile time if the expression is not indexable.
 * Only pointers and arrays are indexable.
 * Expression value is 0 for pointer and 1 for array
 */
#define IS_ARRAY(x)                                                            \
   __builtin_choose_expr(                                                      \
      __builtin_types_compatible_p(typeof((x)[0])[], typeof(x)), 1, 0)

/*
 * Errors at compile time if you use ARRAY_SIZE() on a pointer.
 * ARRAY_SIZE can be used inside of _Static_assert.
 */
#define ARRAY_SIZE(x) ASSERT_EXPR(IS_ARRAY(x), (sizeof(x) / sizeof((x)[0])))

/*
 * -----------------------------------------------------------------------------
 * Utility macros to clear memory
 * They have similar usage/prevent similar mistakes to the TYPED_MALLOC
 * kind of macros.
 * They are not appropriate when you have a malloced array or when you've
 * done non-obvious sized mallocs.
 *
 * Array/Pointer/Struct have different implementations cause the size
 * and pointer calculations are different.
 * Passing any one type to a clearing function of another type is likely
 * to be a bug, so if the calling isn't perfect it will give a compile-time
 * error.
 * -----------------------------------------------------------------------------
 */
/*
 * Zero an array.
 * Cause compile-time error if used on pointer or non-indexible variable
 */
#define ZERO_ARRAY(v)                                                          \
   do {                                                                        \
      _Static_assert(IS_ARRAY(v), "Use of ZERO_ARRAY on non-array object");    \
      memset((v), 0, sizeof(v));                                               \
   } while (0)

/*
 * Zero a manual array (e.g. we malloced an array).
 * Cause compile-time error if used on an array or non-indexible variable
 */
#define ZERO_CONTENTS_N(v, n)                                                  \
   do {                                                                        \
      _Static_assert(!IS_ARRAY(v), "Use of ZERO_CONTENTS on array");           \
      memset((v), 0, (n) * sizeof(*(v)));                                      \
   } while (0)

/*
 * Zero a non-array pointer (clears what the pointer points to).
 * Cause compile-time error if used on an array or non-indexible variable
 *
 * Should not be used to zero out structs. Use ZERO_STRUCT instead.
 * It is difficult to add compile errors when you pass structs here, but
 * a debug compile is likely to compile error on the debug_assert.
 */
#define ZERO_CONTENTS(v) ZERO_CONTENTS_N((v), 1)

/*
 * Zero a struct.
 * We want to give a compile-time error if v is not a struct, so we cannot
 * use something like memset(&v, 0, sizeof(v)); Even C11 is not rich enough
 * to determine if a variable is a struct, so we use syntax errors to catch
 * it instead.
 *
 * Unfortunately C11/gnu11 syntax is only rich enough to cause errors on
 * pointers and primitives, but not arrays.
 *
 * Note: We could take advantage of the syntax and still use memset like this:
 *    __builtin_choose_expr(
 *       0,
 *       (typeof(v)) {},
 *       memset(&(v), 0, sizeof(v)))
 * however while the above still does prevent pointers and primitives, it will
 * incorrectly initialize arrays (read: corrupt memory) if they happen to get
 * passed in.
 *    Struct assignment properly initializes both arrays and structs:
 *       v = (typeof(v)) {}
 *    Memset for structs:
 *       memset(&v, 0, sizeof(v))
 *    Memset for arrays:
 *       memset(v, 0, sizeof(v))
 *
 * memset performance is affected by compiler and headers
 * struct assignment performance is just affected by compiler
 * It's not obvious that one has a performance advantage and since memset is
 * often a compiler intrinsic it's likely to have the same performance.
 *
 * Note; This version would only work during declaration:
 *    (v) = {}
 * Note; This version could work during declaration and a regular statement,
 * but we force it as a statement to match the usage of ZERO_ARRAY/ZERO_POINTER
 *    (v) = (typeof(v)) {}
 *
 * This macro intentionally CANNOT be used during declaration
 * (see ZERO_STRUCT_AT_DECL).
 */
#define ZERO_STRUCT(v)                                                         \
   do {                                                                        \
      (v) = (typeof(v)){};                                                     \
   } while (0)

/*
 * Zero a struct at declaration time.
 * Equivalent to doing:
 *    struct foo s;
 *    ZERO_STRUCT(s);
 * Usage example:
 *    struct foo ZERO_STRUCT_AT_DECL(s);
 * See documentation for ZERO_STRUCT.
 * Note:
 *    You can actually use ZERO_STRUCT_AT_DECL as a regular statement,
 *    but it is slightly less safe because the first v cannot be wrapped
 *    in parenthesis.
 */
#define ZERO_STRUCT_AT_DECL(v)                                                 \
   v = (typeof(v)) {}

#define UNUSED_PARAM(_parm) _parm __attribute__((__unused__))
#define UNUSED_TYPE(_parm)  UNUSED_PARAM(_parm)

/*
 *   Helper macro that causes branch prediction to favour the likely
 *   side of a jump instruction. If the prediction is correct,
 *   the jump instruction takes zero cycles. If it's wrong, the
 *   processor pipeline needs to be flushed and it can cost
 *   several cycles.
 */
#define LIKELY(_exp)   __builtin_expect(!!(_exp), 1)
#define UNLIKELY(_exp) __builtin_expect(!!(_exp), 0)

#define STRINGIFY(x)       #x
#define STRINGIFY_VALUE(s) STRINGIFY(s)

#define ROUNDUP(x, y)   (((x) + (y) - 1) / (y) * (y))
#define ROUNDDOWN(x, y) ((x) / (y) * (y))

typedef struct fraction {
   uint64 numerator;
   uint64 denominator;
} fraction;

static inline fraction
init_fraction(uint64 numerator, uint64 denominator)
{
   return (fraction){
      .numerator   = numerator,
      .denominator = denominator,
   };
}

#define zero_fraction                                                          \
   ((fraction){                                                                \
      .numerator   = 0,                                                        \
      .denominator = 1,                                                        \
   })

static inline fraction
fraction_init_or_zero(uint64 num, uint64 den)
{
   return den ? init_fraction(num, den) : zero_fraction;
}

// Length of output buffer to snprintf()-into size as string w/ unit specifier
#define SIZE_TO_STR_LEN 20

// Format a size value with unit-specifiers, in an output buffer.
char *
size_to_str(char *outbuf, size_t outbuflen, size_t size);

char *
size_to_fmtstr(char *outbuf, size_t outbuflen, const char *fmtstr, size_t size);

/*
 * Convenience caller macros to convert 'sz' bytes to return a string,
 * formatting the input size as human-readable value with unit-specifiers.
 */
// char *size_str(size_t sz)
#define size_str(sz)                                                           \
   (({                                                                         \
       struct {                                                                \
          char buffer[SIZE_TO_STR_LEN];                                        \
       } onstack_chartmp;                                                      \
       size_to_str(                                                            \
          onstack_chartmp.buffer, sizeof(onstack_chartmp.buffer), sz);         \
       onstack_chartmp;                                                        \
    }).buffer)

// char *size_fmtstr(const char *fmtstr, size_t sz)
#define size_fmtstr(fmtstr, sz)                                                \
   (({                                                                         \
       struct {                                                                \
          char buffer[SIZE_TO_STR_LEN];                                        \
       } onstack_chartmp;                                                      \
       size_to_fmtstr(                                                         \
          onstack_chartmp.buffer, sizeof(onstack_chartmp.buffer), fmtstr, sz); \
       onstack_chartmp;                                                        \
    }).buffer)
