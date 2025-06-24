/*
 * Type-safe vectors.  Implementation is entirely macros.
 *
 * Macros in lower_case behave like functions (i.e. they evaluate
 * their parameters exactly once).
 *
 * Macros in UPPER_CASE may evaluate any of their parameters any number of
 * times, so use them accordingly.
 */

#pragma once


#include "util.h"

#define VECTOR(elt_type)                                                       \
   struct {                                                                    \
      writable_buffer wb;                                                      \
      elt_type        vector_element_type_handle[0];                           \
   }

// These macros don't evaluate their parameters, so we can use them even in
// function-like macros below.
#define vector_elt_type(v)     typeof((v)->vector_element_type_handle[0])
#define vector_elt_size(v)     sizeof((v)->vector_element_type_handle[0])
#define vector_elt_ptr_type(v) typeof(&((v)->vector_element_type_handle[0]))

#define vector_init(v, hid) writable_buffer_init(&((v)->wb), hid)
#define vector_deinit(v)    writable_buffer_deinit(&((v)->wb))

#define vector_data(v)                                                         \
   ((vector_elt_ptr_type(v))writable_buffer_data(&((v)->wb)))

#define vector_capacity(v)                                                     \
   (writable_buffer_capacity(&((v)->wb)) / vector_elt_size(v))

// |v|
#define vector_length(v)                                                       \
   (writable_buffer_length(&((v)->wb)) / vector_elt_size(v))

// v[i]
#define vector_get(v, i)                                                       \
   ({                                                                          \
      typeof(v) __v = (v);                                                     \
      typeof(i) __i = (i);                                                     \
      debug_assert(__i < vector_length(__v));                                  \
      vector_data(__v)[__i];                                                   \
   })

// &v[i]
#define vector_get_ptr(v, i)                                                   \
   ({                                                                          \
      typeof(v) __v = (v);                                                     \
      typeof(i) __i = (i);                                                     \
      debug_assert(__i < vector_length(__v));                                  \
      vector_data(__v) + __i;                                                  \
   })

// This is used to access reserved space that is not yet part of the logical
// vector, e.g. to initialize new elements at the end of the vector.
// It still asserts that accesses are within the space allocated for the vector,
// so it's not totally unsafe...
#define vector_get_ptr_unsafe(v, i)                                            \
   ({                                                                          \
      typeof(v) __v = (v);                                                     \
      typeof(i) __i = (i);                                                     \
      debug_assert(__i < vector_capacity(__v));                                \
      vector_data(__v) + __i;                                                  \
   })

// v[i] = val
#define vector_set(v, i, val)                                                  \
   ({                                                                          \
      typeof(v)   __v   = (v);                                                 \
      typeof(i)   __i   = (i);                                                 \
      typeof(val) __val = (val);                                               \
      debug_assert(__i < vector_length(__v));                                  \
      vector_data(__v)[__i] = __val;                                           \
   })

// This is used to access reserved space that is not yet part of the logical
// vector, e.g. to initialize new elements at the end of the vector.
// It still asserts that accesses are within the space allocated for the vector,
// so it's not totally unsafe...
#define vector_set_unsafe(v, i, val)                                           \
   ({                                                                          \
      typeof(v)   __v   = (v);                                                 \
      typeof(i)   __i   = (i);                                                 \
      typeof(val) __val = (val);                                               \
      debug_assert(__i < vector_capacity(__v));                                \
      vector_data(__v)[__i] = __val;                                           \
   })

// v = v + [ val ]
#define vector_append(v, val)                                                  \
   ({                                                                          \
      vector_elt_type(v) __val = (val);                                        \
      writable_buffer_append(&(v)->wb, sizeof(__val), &(__val));               \
   })

static inline platform_status
__vector_replace(writable_buffer       *dst,
                 uint64                 eltsize,
                 uint64                 dstoff,
                 uint64                 dstlen,
                 const writable_buffer *src,
                 uint64                 srcoff,
                 uint64                 srclen)
{
   platform_status   rc           = STATUS_OK;
   uint64            old_dst_size = writable_buffer_length(dst);
   debug_only uint64 src_size     = writable_buffer_length(src);

   debug_assert((dstoff + dstlen) * eltsize <= old_dst_size);
   debug_assert((srcoff + srclen) * eltsize <= src_size);

   if (dstlen < srclen) {
      rc = writable_buffer_resize(dst,
                                  old_dst_size + (srclen - dstlen) * eltsize);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   uint8 *dstdata = writable_buffer_data(dst);
   uint8 *srcdata = writable_buffer_data(src);
   memmove(dstdata + (dstoff + srclen) * eltsize,
           dstdata + (dstoff + dstlen) * eltsize,
           old_dst_size - (dstoff + dstlen) * eltsize);
   memmove(
      dstdata + dstoff * eltsize, srcdata + srcoff * eltsize, srclen * eltsize);

   if (srclen < dstlen) {
      rc = writable_buffer_resize(dst,
                                  old_dst_size - (dstlen - srclen) * eltsize);
      platform_assert_status_ok(rc);
   }
   return rc;
}

#define vector_replace(dst, dstoff, dstlen, src, srcoff, srclen)               \
   ({                                                                          \
      _Static_assert(__builtin_types_compatible_p(vector_elt_type(dst),        \
                                                  vector_elt_type(src)),       \
                     "vector_replace must be called with vectors of "          \
                     "the same element type.");                                \
      _Static_assert(vector_elt_size(dst) == vector_elt_size(src),             \
                     "vector_replace must be called with vectors of "          \
                     "elements of same size.");                                \
      __vector_replace(&((dst)->wb),                                           \
                       vector_elt_size(dst),                                   \
                       dstoff,                                                 \
                       dstlen,                                                 \
                       &((src)->wb),                                           \
                       srcoff,                                                 \
                       srclen);                                                \
   })

#define vector_append_subvector(dst, src, start, end)                          \
   ({                                                                          \
      _Static_assert(__builtin_types_compatible_p(vector_elt_type(dst),        \
                                                  vector_elt_type(src)),       \
                     "vector_append_vector must be called with vectors of "    \
                     "the same element type.");                                \
      _Static_assert(vector_elt_size(dst) == vector_elt_size(src),             \
                     "vector_append_subvector must be called with vectors of " \
                     "elements of same size.");                                \
      uint64 __start                     = (start);                            \
      vector_elt_ptr_type(src) __srcdata = vector_data(src);                   \
      writable_buffer_append(&(dst)->wb,                                       \
                             ((end)-__start) * vector_elt_size(src),           \
                             __srcdata + __start);                             \
   })

#define vector_append_vector(dst, src)                                         \
   vector_append_subvector(dst, src, 0, vector_length(src))

#define vector_truncate(v, new_length)                                         \
   ({                                                                          \
      typeof(v)          __v          = (v);                                   \
      typeof(new_length) __new_length = (new_length);                          \
      debug_assert(__new_length <= vector_length(__v));                        \
      platform_status __rc =                                                   \
         writable_buffer_resize(&__v->wb, __new_length * vector_elt_size(v));  \
      platform_assert_status_ok(__rc);                                         \
   })

#define vector_ensure_capacity(v, capacity)                                    \
   (writable_buffer_ensure_space(&(v)->wb, (capacity)*vector_elt_size(v)))

#define vector_copy(v, src)                                                    \
   ({                                                                          \
      _Static_assert(__builtin_types_compatible_p(vector_elt_type(v),          \
                                                  vector_elt_type(src)),       \
                     "Incompatible vector types");                             \
      writable_buffer_copy_slice(&(v)->wb,                                     \
                                 writable_buffer_to_slice(&(src)->wb));        \
   })

// forall i: func(v, i, ...)
// func can be a function or a macro.
// In either case, f(v, i, ...) must have type void.
#define VECTOR_APPLY_GENERIC(v, func, ...)                                     \
   ({                                                                          \
      uint64 __idx;                                                            \
      _Static_assert(                                                          \
         __builtin_types_compatible_p(                                         \
            void, typeof(func((v), __idx __VA_OPT__(, __VA_ARGS__)))),         \
         "vector_apply_generic can be used only with void functions");         \
      for (__idx = 0; __idx < vector_length(v); __idx++) {                     \
         func(v, __idx __VA_OPT__(, __VA_ARGS__));                             \
      }                                                                        \
   })

// Adapters to define vector_apply_to_elements and
// vector_apply_to_ptrs. You probably don't need to use
// these directly.
#define vector_apply_to_elt(v, i, func, ...)                                   \
   func(vector_get(v, i) __VA_OPT__(, __VA_ARGS__))
#define vector_apply_to_ptr(v, i, func, ...)                                   \
   func(vector_get_ptr(v, i) __VA_OPT__(, __VA_ARGS__))

#define vector_apply_to_ptr_unsafe(v, i, func, ...)                            \
   func(vector_get_ptr_unsafe(v, i) __VA_OPT__(, __VA_ARGS__))

// forall i: f(v[i], ...)
// f can be a function or a macro.
// In either case, f(v[i], ...) must have type void.
#define VECTOR_APPLY_TO_ELTS(v, func, ...)                                     \
   VECTOR_APPLY_GENERIC(v, vector_apply_to_elt, func __VA_OPT__(, __VA_ARGS__))

// forall i: f(&v[i], ...)
// f can be a function or a macro.
// In either case, f(&v[i], ...) must have type void.
#define VECTOR_APPLY_TO_PTRS(v, func, ...)                                     \
   VECTOR_APPLY_GENERIC(v, vector_apply_to_ptr, func __VA_OPT__(, __VA_ARGS__))

// forall i: dst[i] = f(src, i, ...)
// f can be a function or a macro.
#define VECTOR_MAP_GENERIC(dst, func, src, ...)                                \
   ({                                                                          \
      platform_status __rc;                                                    \
      uint64          __len  = vector_length(src);                             \
      uint64          __size = __len * vector_elt_size(dst);                   \
      __rc                   = writable_buffer_resize(&(dst)->wb, __size);     \
      if (SUCCESS(__rc)) {                                                     \
         for (uint64 __idx = 0; __idx < __len; __idx++) {                      \
            vector_elt_type(dst) __result =                                    \
               func(src, __idx __VA_OPT__(, __VA_ARGS__));                     \
            vector_set(dst, __idx, __result);                                  \
         }                                                                     \
      }                                                                        \
      __rc;                                                                    \
   })

// forall i: dst[i] = f(src[i], ...)
// f can be a function or a macro.
#define VECTOR_MAP_ELTS(dst, func, src, ...)                                   \
   VECTOR_MAP_GENERIC(                                                         \
      dst, vector_apply_to_elt, src, func __VA_OPT__(, __VA_ARGS__))

// forall i: dst[i] = f(src[i], ...)
// f can be a function or a macro.
#define VECTOR_MAP_PTRS(dst, func, src, ...)                                   \
   VECTOR_MAP_GENERIC(                                                         \
      dst, vector_apply_to_ptr, src, func __VA_OPT__(, __VA_ARGS__))

/*
 * Convenience function so you can use vector_apply_to_elements to
 * free all the elements of a vector of pointers.
 */
static inline void
vector_apply_platform_free(void *ptr, platform_heap_id hid)
{
   if (ptr) {
      platform_free(hid, ptr);
   }
}

// acc = zero
// for i = 0 to |v| - 1:
//   acc = add(acc, v, i, ...)
#define VECTOR_FOLD_LEFT_GENERIC(v, add, zero, ...)                            \
   ({                                                                          \
      typeof(zero) __acc = zero;                                               \
      for (uint64 __idx = 0; __idx < vector_length(v); __idx++) {              \
         __acc = add(__acc, v, __idx __VA_OPT__(, __VA_ARGS__));               \
      }                                                                        \
      __acc;                                                                   \
   })

// acc = zero
// for i = |v|-1 down to 0:
//   acc = add(acc, v, i, ...)
#define VECTOR_FOLD_RIGHT_GENERIC(v, add, zero, ...)                           \
   ({                                                                          \
      typeof(zero) __acc = zero;                                               \
      for (int64 __idx = vector_length(v) - 1; 0 <= __idx; __idx--) {          \
         __acc = add(__acc, v, __idx __VA_OPT__(, __VA_ARGS__));               \
      }                                                                        \
      __acc;                                                                   \
   })

// Adapters used to define
//   fold_{left,right}_acc_{elt,ptr}
// and
//   fold_{left,right}_{elt,ptr}_acc
#define vector_fold_acc_elt(acc, v, i, add, ...)                               \
   add(acc, vector_get(v, i) __VA_OPT__(, __VA_ARGS__))
#define vector_fold_elt_acc(acc, v, i, add, ...)                               \
   add(vector_get(v, i), acc __VA_OPT__(, __VA_ARGS__))
#define vector_fold_acc_ptr(acc, v, i, add, ...)                               \
   add(acc, vector_get_ptr(v, i) __VA_OPT__(, __VA_ARGS__))
#define vector_fold_ptr_acc(acc, v, i, add, ...)                               \
   add(vector_get_ptr(v, i), acc __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = 0 to |v| - 1:
//   acc = add(acc, v[i], ...)
#define VECTOR_FOLD_LEFT_ACC_ELT(v, add, zero, ...)                            \
   VECTOR_FOLD_LEFT_GENERIC(                                                   \
      v, vector_fold_acc_elt, zero, add __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = 0 to |v| - 1:
//   acc = add(acc, &v[i], ...)
#define VECTOR_FOLD_LEFT_ACC_PTR(v, add, zero, ...)                            \
   VECTOR_FOLD_LEFT_GENERIC(                                                   \
      v, vector_fold_acc_ptr, zero, add __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = 0 to |v| - 1:
//   acc = add(v[i], acc, ...)
#define VECTOR_FOLD_LEFT_ELT_ACC(v, add, zero, ...)                            \
   VECTOR_FOLD_LEFT_GENERIC(                                                   \
      v, vector_fold_elt_acc, zero, add __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = 0 to |v| - 1:
//   acc = add(&v[i], acc, ...)
#define VECTOR_FOLD_LEFT_PTR_ACC(v, add, zero, ...)                            \
   VECTOR_FOLD_LEFT_GENERIC(                                                   \
      v, vector_fold_ptr_acc, zero, add __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = |v| - 1 down to 0:
//   acc = add(acc, v[i], ...)
#define VECTOR_FOLD_RIGHT_ACC_ELT(v, add, zero, ...)                           \
   VECTOR_FOLD_RIGHT_GENERIC(                                                  \
      v, vector_fold_acc_elt, zero, add __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = |v| - 1 down to 0:
//   acc = add(acc, &v[i], ...)
#define VECTOR_FOLD_RIGHT_ACC_PTR(v, add, zero, ...)                           \
   VECTOR_FOLD_RIGHT_GENERIC(                                                  \
      v, vector_fold_acc_ptr, zero, add __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = |v| - 1 down to 0:
//   acc = add(v[i], acc, ...)
#define VECTOR_FOLD_RIGHT_ELT_ACC(v, add, zero, ...)                           \
   VECTOR_FOLD_RIGHT_GENERIC(                                                  \
      v, vector_fold_elt_acc, zero, add __VA_OPT__(, __VA_ARGS__))

// acc = zero
// for i = |v| - 1 down to 0:
//   acc = add(&v[i], acc, ...)
#define VECTOR_FOLD_RIGHT_PTR_ACC(v, add, zero, ...)                           \
   VECTOR_FOLD_RIGHT_GENERIC(                                                  \
      v, vector_fold_ptr_acc, zero, add __VA_OPT__(, __VA_ARGS__))


#define VECTOR_FOLD2_GENERIC(v1, v2, combiner, folder, init, ...)              \
   ({                                                                          \
      debug_assert(vector_length(v1) == vector_length(v2));                    \
      __auto_type __acc = init;                                                \
      for (uint64 __idx = 0; __idx < vector_length(v1); __idx++) {             \
         __acc =                                                               \
            folder(__acc, combiner(v1, v2, __idx __VA_OPT__(, __VA_ARGS__)));  \
      }                                                                        \
      __acc;                                                                   \
   })

#define vector_apply_to_elts2(v1, v2, idx, combiner, ...)                      \
   combiner(vector_get(v1, idx), vector_get(v2, idx) __VA_OPT__(, __VA_ARGS__))
#define vector_apply_to_ptrs2(v1, v2, idx, combiner, ...)                      \
   combiner(vector_get_ptr(v1, idx),                                           \
            vector_get_ptr(v2, idx) __VA_OPT__(, __VA_ARGS__))

#define VECTOR_FOLD2_ELTS(v1, v2, combiner, folder, init, ...)                 \
   VECTOR_FOLD2_GENERIC(                                                       \
      v1, v2, vector_apply_to_elts2, folder, init, combiner, __VA_ARGS__)

#define VECTOR_FOLD2_PTRS(v1, v2, combiner, folder, init, ...)                 \
   VECTOR_FOLD2_GENERIC(                                                       \
      v1, v2, vector_apply_to_ptrs2, folder, init, combiner, __VA_ARGS__)

#define VECTOR_AND(a, b) ((a) && (b))

#define VECTOR_ELTS_EQUAL(v1, v2, comparator)                                  \
   (vector_length(v1) == vector_length(v2)                                     \
    && VECTOR_FOLD2_ELTS(v1, v2, comparator, VECTOR_AND, TRUE))

#define VECTOR_ELTS_EQUAL_BY_PTR(v1, v2, comparator)                           \
   (vector_length(v1) == vector_length(v2)                                     \
    && VECTOR_FOLD2_PTRS(v1, v2, comparator, VECTOR_AND, TRUE))

_Static_assert(__builtin_types_compatible_p(void, void), "Uhoh");
_Static_assert(__builtin_types_compatible_p(platform_status, platform_status),
               "Uhoh");
_Static_assert(!__builtin_types_compatible_p(void, platform_status), "Uhoh");
_Static_assert(!__builtin_types_compatible_p(platform_status, void), "Uhoh");

// func(...)
// func may be void or return a platform_status
//
// The purpose of this macro is to transform void function
// calls into expressions that return platform_status, so
// we can deal with void and failable functions uniformly
// in the macros that follow.
#define VECTOR_CALL_FAILABLE(func, ...)                                        \
   ({                                                                          \
      _Static_assert(                                                          \
         __builtin_types_compatible_p(platform_status,                         \
                                      typeof(func(__VA_ARGS__)))               \
            || __builtin_types_compatible_p(void, typeof(func(__VA_ARGS__))),  \
         "vector_call_failable can be called only with "                       \
         "functions that return platform_status or void.");                    \
      __builtin_choose_expr(                                                   \
         __builtin_types_compatible_p(void, typeof(func(__VA_ARGS__))),        \
         ({                                                                    \
            func(__VA_ARGS__);                                                 \
            STATUS_OK;                                                         \
         }),                                                                   \
         ({ func(__VA_ARGS__); }));                                            \
   })

#define VECTOR_FAILABLE_FOR_LOOP_GENERIC(v, start, end, func, ...)             \
   ({                                                                          \
      platform_status   __rc     = STATUS_OK;                                  \
      debug_only uint64 __length = vector_length(v);                           \
      uint64            __end    = (end);                                      \
      debug_assert(__end <= __length);                                         \
      for (uint64 __idx = (start); __idx < __end; __idx++) {                   \
         __rc =                                                                \
            VECTOR_CALL_FAILABLE(func, v, __idx __VA_OPT__(, __VA_ARGS__));    \
         if (!SUCCESS(__rc)) {                                                 \
            break;                                                             \
         }                                                                     \
      }                                                                        \
      __rc;                                                                    \
   })

#define VECTOR_FAILABLE_FOR_LOOP_ELTS(v, start, end, func, ...)                \
   VECTOR_FAILABLE_FOR_LOOP_GENERIC(                                           \
      v, start, end, vector_apply_to_elt, func __VA_OPT__(, __VA_ARGS__))

#define VECTOR_FAILABLE_FOR_LOOP_PTRS(v, start, end, func, ...)                \
   VECTOR_FAILABLE_FOR_LOOP_GENERIC(                                           \
      v, start, end, vector_apply_to_ptr, func __VA_OPT__(, __VA_ARGS__))


// allocates space for one more element, then calls
//   init(v, |v|, ...)
// init may be void or return a platform_status
// if init succeeds, then the length of v is increased
// by 1. returns platform_status to indicate success
#define VECTOR_EMPLACE_APPEND_GENERIC(v, init, ...)                            \
   ({                                                                          \
      uint64          __old_length = vector_length(v);                         \
      uint64          __old_size   = __old_length * vector_elt_size(v);        \
      uint64          __new_size   = __old_size + vector_elt_size(v);          \
      platform_status __rc;                                                    \
      __rc = writable_buffer_resize(&(v)->wb, __new_size);                     \
      if (SUCCESS(__rc)) {                                                     \
         __rc = VECTOR_CALL_FAILABLE(                                          \
            init, (v), __old_length __VA_OPT__(, __VA_ARGS__));                \
      }                                                                        \
      if (!SUCCESS(__rc)) {                                                    \
         __rc = writable_buffer_resize(&(v)->wb, __old_size);                  \
         platform_assert_status_ok(__rc);                                      \
      }                                                                        \
      __rc;                                                                    \
   })

// allocates space for one more element, then calls
//   init(&v[|v|], ...)
// init may be void or return a platform_status
// if init succeeds, then the length of v is increased
// by 1. returns platform_status to indicate success
#define VECTOR_EMPLACE_APPEND(v, init, ...)                                    \
   VECTOR_EMPLACE_APPEND_GENERIC(                                              \
      v, vector_apply_to_ptr_unsafe, init __VA_OPT__(, __VA_ARGS__))

// for i = 0 to |src|: func(&dst[i], src, i, ...)
// Stops after first failed call to func.
// Leaves dst length equal to the number of successful
// calls. returns platform_status indicating
// success/failure.
#define VECTOR_EMPLACE_MAP_GENERIC(dst, func, src, ...)                        \
   ({                                                                          \
      uint64          __len  = vector_length(src);                             \
      uint64          __size = __len * vector_elt_size(dst);                   \
      platform_status __rc   = writable_buffer_resize(&(dst)->wb, __size);     \
      if (SUCCESS(__rc)) {                                                     \
         uint64 __idx;                                                         \
         for (__idx = 0; __idx < __len; __idx++) {                             \
            __rc = VECTOR_CALL_FAILABLE(func,                                  \
                                        vector_get_ptr_unsafe(dst, __idx),     \
                                        src,                                   \
                                        __idx __VA_OPT__(, __VA_ARGS__));      \
            if (!SUCCESS(__rc)) {                                              \
               break;                                                          \
            }                                                                  \
         }                                                                     \
         writable_buffer_resize(&(dst)->wb, __idx *vector_elt_size(dst));      \
      }                                                                        \
      __rc;                                                                    \
   })

#define vector_emplace_map_elt(tgt, src, idx, func, ...)                       \
   func(tgt, vector_get(src, idx) __VA_OPT__(, __VA_ARGS__))

#define vector_emplace_map_ptr(tgt, src, idx, func, ...)                       \
   func(tgt, vector_get_ptr(src, idx) __VA_OPT__(, __VA_ARGS__))

#define VECTOR_EMPLACE_MAP_ELTS(dst, func, src, ...)                           \
   VECTOR_EMPLACE_MAP_GENERIC(                                                 \
      dst, vector_emplace_map_elt, src, func __VA_OPT__(, __VA_ARGS__))

#define VECTOR_EMPLACE_MAP_PTRS(dst, func, src, ...)                           \
   VECTOR_EMPLACE_MAP_GENERIC(                                                 \
      dst, vector_emplace_map_ptr, src, func __VA_OPT__(, __VA_ARGS__))

static inline void
__vector_reverse(void *arr, uint64 nelts, uint64 eltsize, void *tmp)
{
   for (uint64 i = 0; i < nelts / 2; i++) {
      memcpy(tmp, arr + i * eltsize, eltsize);
      memcpy(arr + i * eltsize, arr + (nelts - i - 1) * eltsize, eltsize);
      memcpy(arr + (nelts - i - 1) * eltsize, tmp, eltsize);
   }
}

#define vector_reverse(v)                                                      \
   {                                                                           \
      vector_elt_type(v) __tmp;                                                \
      __vector_reverse(                                                        \
         vector_data(v), vector_length(v), vector_elt_size(v), &__tmp);        \
   }
