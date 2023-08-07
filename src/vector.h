#pragma once


#include "util.h"

#define VECTOR_DEFINE(name, elt_type)                                          \
   typedef struct name {                                                       \
      writable_buffer wb;                                                      \
      elt_type        vector_element_type_handle[0];                           \
   } name;

#define vector_elt_type(v)     typeof((v)->vector_element_type_handle[0])
#define vector_elt_size(v)     sizeof((v)->vector_element_type_handle[0])
#define vector_elt_ptr_type(v) typeof(&((v)->vector_element_type_handle[0]))
#define vector_data(v)                                                         \
   ((vector_elt_ptr_type(v))writable_buffer_data(&((v)->wb)))

#define vector_init(v, hid) writable_buffer_init(&((v)->wb), hid)
#define vector_deinit(v)    writable_buffer_deinit(&((v)->wb))

#define vector_length(v)                                                       \
   (writable_buffer_length(&((v)->wb)) / sizeof(vector_elt_type(v)))

#define vector_get(v, i)                                                       \
   ({                                                                          \
      uint64          vector_tmp_idx = (i);                                    \
      const typeof(v) vector_tmp     = (v);                                    \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      vector_data(vector_tmp)[vector_tmp_idx];                                 \
   })

#define vector_get_ptr(v, i)                                                   \
   ({                                                                          \
      uint64          vector_tmp_idx = (i);                                    \
      const typeof(v) vector_tmp     = (v);                                    \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      vector_data(vector_tmp) + vector_tmp_idx;                                \
   })

#define vector_set(v, i, val)                                                  \
   ({                                                                          \
      uint64            vector_tmp_idx = (i);                                  \
      const typeof(v)   vector_tmp     = (v);                                  \
      const typeof(val) val_tmp        = (val);                                \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      vector_data(vector_tmp)[vector_tmp_idx] = val_tmp;                       \
   })

#define vector_append(v, val)                                                  \
   ({                                                                          \
      const typeof(v) vector_tmp       = (v);                                  \
      const vector_elt_type(v) val_tmp = (val);                                \
      writable_buffer_append(&vector_tmp->wb, sizeof(val_tmp), &val_tmp);      \
      STATUS_OK;                                                               \
   })

#define vector_emplace(v, init, args...)                                       \
   ({                                                                          \
      const typeof(v) vector_emplace_tmp = (v);                                \
      uint64          vector_emplace_old_size =                                \
         writable_buffer_length(&vector_emplace_tmp->wb);                      \
      platform_status vector_rc =                                              \
         writable_buffer_resize(&vector_emplace_tmp->wb,                       \
                                vector_emplace_old_size + vector_elt_size(v)); \
      if (SUCCESS(vector_rc)) {                                                \
         vector_elt_ptr_type(v) vector_elt_ptr_tmp = vector_get_ptr(           \
            vector_emplace_tmp, vector_length(vector_emplace_tmp) - 1);        \
         vector_rc = init(vector_elt_ptr_tmp, args);                           \
         if (!SUCCESS(vector_rc)) {                                            \
            platform_status vector_resize_rc = writable_buffer_resize(         \
               &vector_emplace_tmp->wb, vector_emplace_old_size);              \
            platform_assert_status_ok(vector_resize_rc);                       \
         }                                                                     \
      }                                                                        \
      vector_rc;                                                               \
   })

#define vector_apply(v, func, ...)                                             \
   ({                                                                          \
      const typeof(v) vector_apply_tmp = (v);                                  \
      for (uint64 vector_apply_tmp_idx = 0;                                    \
           vector_apply_tmp_idx < vector_length(v);                            \
           vector_apply_tmp_idx++)                                             \
      {                                                                        \
         func(vector_get(vector_apply_tmp, vector_apply_tmp_idx)               \
                 __VA_OPT__(, ) __VA_ARGS__);                                  \
      }                                                                        \
   })

/*
 * Convenience function so you can use vector_apply to free all the
 * elements of a vector.
 */
static inline void
vector_apply_platform_free(void *ptr, platform_heap_id hid)
{
   platform_free(hid, ptr);
}

#define vector_apply_ptr(v, func, ...)                                         \
   ({                                                                          \
      const typeof(v) vector_apply_tmp = (v);                                  \
      for (uint64 vector_apply_tmp_idx = 0;                                    \
           vector_apply_tmp_idx < vector_length(v);                            \
           vector_apply_tmp_idx++)                                             \
      {                                                                        \
         func(vector_get_ptr(vector_apply_tmp, vector_apply_tmp_idx)           \
                 __VA_OPT__(, ) __VA_ARGS__);                                  \
      }                                                                        \
   })

#define vector_truncate(v, new_length)                                         \
   ({                                                                          \
      const typeof(v) vector_truncate_tmp = (v);                               \
      debug_assert(new_length <= vector_length(vector_truncate_tmp));          \
      platform_status vector_truncate_rc = writable_buffer_resize(             \
         &vector_truncate_tmp->wb, new_length * vector_elt_size(v));           \
      platform_assert_status_ok(vector_truncate_rc);                           \
   })
