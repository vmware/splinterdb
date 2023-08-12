/*
 * Type-safe vectors.  Implementation is entirely macros.
 *
 * Macros in lower_case behave like functions (i.e. they evaluate
 * their parameters at most once).
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

#define vector_elt_type(v)     typeof((v)->vector_element_type_handle[0])
#define vector_elt_size(v)     sizeof((v)->vector_element_type_handle[0])
#define vector_elt_ptr_type(v) typeof(&((v)->vector_element_type_handle[0]))
#define vector_data(v)                                                         \
   ((vector_elt_ptr_type(v))writable_buffer_data(&((v)->wb)))

#define vector_init(v, hid) writable_buffer_init(&((v)->wb), hid)
#define vector_deinit(v)    writable_buffer_deinit(&((v)->wb))

// |v|
#define vector_length(v)                                                       \
   (writable_buffer_length(&((v)->wb)) / vector_elt_size(v))

#define vector_capacity(v)                                                     \
   (writable_buffer_capacity(&((v)->wb)) / vector_elt_size(v))

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
   (writable_buffer_ensure_space(&(v)->wv,                                     \
                                 capacity * vector_element_size(capacity)))

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

// Adapters to define vector_apply_to_elements and vector_apply_to_ptrs.
// You probably don't need to use these directly.
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

// func(...)
// func may be void or return a platform_status
//
// The purpose of this macro is to transform void function calls into
// expressions that return platform_status, so we can deal with void and
// failable functions uniformly in the macros that follow.
#define VECTOR_CALL_FAILABLE(func, ...)                                        \
   ({                                                                          \
      _Static_assert(                                                          \
         __builtin_types_compatible_p(platform_status,                         \
                                      typeof(func(__VA_ARGS__)))               \
            || __builtin_types_compatible_p(void, typeof(func(__VA_ARGS__))),  \
         "vector_call_failable_at can be called only with "                    \
         "functions that return platform_status or void.");                    \
      platform_status __rc;                                                    \
      if (__builtin_types_compatible_p(platform_status,                        \
                                       typeof(func(__VA_ARGS__)))) {           \
         __rc = func(__VA_ARGS__);                                             \
      } else if (__builtin_types_compatible_p(void,                            \
                                              typeof(func(__VA_ARGS__)))) {    \
         func(__VA_ARGS__);                                                    \
         __rc = STATUS_OK;                                                     \
      } else {                                                                 \
         platform_assert(0);                                                   \
      }                                                                        \
      __rc;                                                                    \
   })

// allocates space for one more element, then calls
//   init(v, |v|, ...)
// init may be void or return a platform_status
// if init succeeds, then the length of v is increased by 1.
// returns platform_status to indicate success
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
// if init succeeds, then the length of v is increased by 1.
// returns platform_status to indicate success
#define VECTOR_EMPLACE_APPEND(v, init, ...)                                    \
   VECTOR_EMPLACE_APPEND_GENERIC(                                              \
      v, vector_apply_to_ptr_unsafe, init __VA_OPT__(, __VA_ARGS__))

// for i = 0 to |src|: func(&dst[i], src, i, ...)
// Stops after first failed call to func.
// Leaves dst length equal to the number of successful calls.
// returns platform_status indicating success/failure.
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
