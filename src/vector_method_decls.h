/*
 * This file is part of the vector subsystem.  This
 * header simply defines a type-specific dynamic-array type.  This is
 * useful in header files where you want to define a typed dynamic
 * array, but not its methods.  (If you just want to declare a typed
 * dynamic array in your header, you can just do
 *
 * typedef struct <VECTOR_NAME> <VECTOR_NAME>;
 *
 * Before including this header, you must define the following
 * preprocessor tokens:
 *
 * #define VECTOR_NAME
 * #define VECTOR_ELEMENT_TYPE
 * #define VECTOR_STORAGE
 *
 * e.g.
 *
 * #define VECTOR_NAME pivot_array
 * #define VECTOR_ELEMENT_TYPE pivot *
 * #define VECTOR_STORAGE static
 *
 */

#include "platform.h"
#include "util.h"
#include "vector_decl.h"

#define CONCAT_(prefix, suffix)  prefix##_##suffix
#define CONCAT(prefix, suffix)   CONCAT_(prefix, suffix)
#define VECTOR_FUNC_NAME(suffix) CONCAT(VECTOR_NAME, suffix)

// clang-format off
VECTOR_STORAGE
void
VECTOR_FUNC_NAME(init)(platform_heap_id          hid,
                       VECTOR_NAME *array)
   __attribute__((unused));

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(init_from_c_array)(platform_heap_id                  hid,
                                                 VECTOR_NAME         *array,
                                                 uint64                            num_elts,
                                                 VECTOR_ELEMENT_TYPE *elts)
   __attribute__((unused));

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(init_from_slice)(platform_heap_id          hid,
                                               VECTOR_NAME *array,
                                               slice                     elts)
   __attribute__((unused));

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(init_from_array)(platform_heap_id          hid,
                                               VECTOR_NAME *array,
                                               VECTOR_NAME *src)
   __attribute__((unused));

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(deinit)(VECTOR_NAME *array)
   __attribute__((unused));

#ifndef vector_length
#define vector_length(v) (writable_buffer_length(&((v)->wb)) / sizeof((v)->vector_element_type_handle[0]))
#endif

#ifndef vector_get
#define vector_get(v, i) \
  ({\
    uint64 vector_tmp_idx = (i);                       \
    typeof(v) vector_tmp = (v); \
    debug_assert((vector_tmp_idx) < vector_length(vector_tmp)); \
    ((typeof(&(vector_tmp)->vector_element_type_handle[0]))writable_buffer_data(&((vector_tmp)->wb)))[(vector_tmp_idx)];\
  })
#endif

VECTOR_STORAGE
slice
VECTOR_FUNC_NAME(slice)(const VECTOR_NAME *array)
  __attribute__((unused));

#ifndef vector_set
#define vector_set(v, i, val)                       \
  ({\
    uint64 vector_tmp_idx = (i);                       \
    typeof(v) vector_tmp = (v); \
    typeof(val) val_tmp = (val); \
    debug_assert((vector_tmp_idx) < vector_length(vector_tmp)); \
    ((typeof(&(vector_tmp)->vector_element_type_handle[0]))writable_buffer_data(&((vector_tmp)->wb)))[(vector_tmp_idx)] = val_tmp;\
  })
#endif

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(set_c_array)(
   VECTOR_NAME         *array,
   uint64                            idx,
   uint64                            num_elts,
   VECTOR_ELEMENT_TYPE *elts)
  __attribute__((unused));

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(set_array)(VECTOR_NAME *array,
                                         uint64                    idx,
                                         uint64 num_elts,
                                         VECTOR_NAME *src,
                                         uint64                    offset)
  __attribute__((unused));

#ifndef vector_append
#define vector_append(v, val) \
  ({ \
    typeof(v) vector_tmp = (v);                                         \
    typeof(vector_tmp->vector_element_type_handle[0]) val_tmp = (val); \
    writable_buffer_append(&vector_tmp->wb, sizeof(val_tmp), &val_tmp); \
    STATUS_OK; \
  })
#endif

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(insert)(VECTOR_NAME        *array,
                                      uint64                           idx,
                                      VECTOR_ELEMENT_TYPE elt)
  __attribute__((unused));

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(insert_c_array)(
   VECTOR_NAME         *array,
   uint64                            idx,
   uint64                            num_elts,
   VECTOR_ELEMENT_TYPE *elts)
  __attribute__((unused));


VECTOR_STORAGE
void
VECTOR_FUNC_NAME(delete)(VECTOR_NAME *array,
                                      uint64                    from,
                                      uint64                    num_elts)
  __attribute__((unused));


// clang-format on
