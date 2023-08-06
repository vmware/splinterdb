#pragma once

#include "util.h"

#define VECTOR_DEFINE(name, elt_type)                                          \
   typedef struct name {                                                       \
      writable_buffer wb;                                                      \
      elt_type        vector_element_type_handle[0];                           \
   } name;

#define vector_length(v)                                                       \
   (writable_buffer_length(&((v)->wb))                                         \
    / sizeof((v)->vector_element_type_handle[0]))

#define vector_get(v, i)                                                       \
   ({                                                                          \
      uint64    vector_tmp_idx = (i);                                          \
      typeof(v) vector_tmp     = (v);                                          \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      ((typeof(&(vector_tmp)->vector_element_type_handle[0]))                  \
          writable_buffer_data(&((vector_tmp)->wb)))[(vector_tmp_idx)];        \
   })

#define vector_set(v, i, val)                                                  \
   ({                                                                          \
      uint64      vector_tmp_idx = (i);                                        \
      typeof(v)   vector_tmp     = (v);                                        \
      typeof(val) val_tmp        = (val);                                      \
      debug_assert((vector_tmp_idx) < vector_length(vector_tmp));              \
      ((typeof(&(vector_tmp)->vector_element_type_handle[0]))                  \
          writable_buffer_data(&((vector_tmp)->wb)))[(vector_tmp_idx)] =       \
         val_tmp;                                                              \
   })

#define vector_append(v, val)                                                  \
   ({                                                                          \
      typeof(v)                                         vector_tmp = (v);      \
      typeof(vector_tmp->vector_element_type_handle[0]) val_tmp    = (val);    \
      writable_buffer_append(&vector_tmp->wb, sizeof(val_tmp), &val_tmp);      \
      STATUS_OK;                                                               \
   })
