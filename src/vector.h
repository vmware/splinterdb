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
      uint64    vector_tmp_idx = (i);                                          \
      typeof(v) vector_tmp     = (v);                                          \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      vector_data(vector_tmp)[vector_tmp_idx];                                 \
   })

#define vector_get_ptr(v, i)                                                   \
   ({                                                                          \
      uint64    vector_tmp_idx = (i);                                          \
      typeof(v) vector_tmp     = (v);                                          \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      vector_data(vector_tmp) + vector_tmp_idx;                                \
   })

#define vector_set(v, i, val)                                                  \
   ({                                                                          \
      uint64      vector_tmp_idx = (i);                                        \
      typeof(v)   vector_tmp     = (v);                                        \
      typeof(val) val_tmp        = (val);                                      \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      vector_data(vector_tmp)[vector_tmp_idx] = val_tmp;                       \
   })

#define vector_append(v, val)                                                  \
   ({                                                                          \
      typeof(v) vector_tmp       = (v);                                        \
      vector_elt_type(v) val_tmp = (val);                                      \
      writable_buffer_append(&vector_tmp->wb, sizeof(val_tmp), &val_tmp);      \
      STATUS_OK;                                                               \
   })

#define vector_emplace(v, init, args...)                                       \
   ({                                                                          \
      typeof(v)       vector_tmp = (v);                                        \
      platform_status vector_rc  = writable_buffer_resize(                     \
         &vector_tmp->wb,                                                     \
         writable_buffer_length(&vector_tmp->wb) + vector_elt_size(v));       \
      if (!SUCCESS(vector_rc)) {                                               \
         return vector_rc;                                                     \
      }                                                                        \
      vector_elt_ptr_type(v) vector_elt_ptr_tmp =                              \
         vector_get_ptr(vector_tmp, vector_length(vector_tmp) - 1);            \
      init(vector_elt_ptr_tmp, args);                                          \
   })
