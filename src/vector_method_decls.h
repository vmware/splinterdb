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

VECTOR_STORAGE
uint64
VECTOR_FUNC_NAME(length)(VECTOR_NAME *array)
   __attribute__((unused));

VECTOR_STORAGE
VECTOR_ELEMENT_TYPE
VECTOR_FUNC_NAME(get)(VECTOR_NAME *array, uint64 idx)
  __attribute__((unused));

VECTOR_STORAGE
slice
VECTOR_FUNC_NAME(slice)(VECTOR_NAME *array)
  __attribute__((unused));

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(set)(VECTOR_NAME        *array,
                                   uint64                           idx,
                                   VECTOR_ELEMENT_TYPE elt)
  __attribute__((unused));

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

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(append)(VECTOR_NAME        *array,
                                      VECTOR_ELEMENT_TYPE elt)
  __attribute__((unused));

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
