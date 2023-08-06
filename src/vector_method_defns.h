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
#include "vector_method_decls.h"

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(init)(platform_heap_id hid, VECTOR_NAME *array)
{
   writable_buffer_init(hid, &array->wb);
}

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(init_from_c_array)(platform_heap_id     hid,
                                    VECTOR_NAME         *array,
                                    uint64               num_elts,
                                    VECTOR_ELEMENT_TYPE *elts)
{
   slice src = slice_create(num_elts, elts);
   return writable_buffer_init_from_slice(hid, &array->wb, src);
}

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(init_from_slice)(platform_heap_id hid,
                                  VECTOR_NAME     *array,
                                  slice            elts)
{
   return writable_buffer_init_from_slice(hid, &array->wb, elts);
}

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(init_from_array)(platform_heap_id hid,
                                  VECTOR_NAME     *array,
                                  VECTOR_NAME     *src)
{
   return writable_buffer_init_from_slice(
      hid, &array->wb, writable_buffer_to_slice(&src->wb));
}

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(deinit)(VECTOR_NAME *array)
{
   writable_buffer_deinit(&array->wb);
}

VECTOR_STORAGE
slice
VECTOR_FUNC_NAME(slice)(const VECTOR_NAME *array)
{
   return writable_buffer_to_slice(&array->wb);
}

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(set_c_array)(VECTOR_NAME         *array,
                              uint64               idx,
                              uint64               num_elts,
                              VECTOR_ELEMENT_TYPE *elts)
{
   debug_assert(idx + num_elts < vector_length(array));
   VECTOR_ELEMENT_TYPE *data =
      (VECTOR_ELEMENT_TYPE *)writable_buffer_data(&array->wb);
   memcpy(&data[idx], elts, num_elts * sizeof(*elts));
}

VECTOR_STORAGE
void
VECTOR_FUNC_NAME(set_array)(VECTOR_NAME *array,
                            uint64       idx,
                            uint64       num_elts,
                            VECTOR_NAME *src,
                            uint64       offset)
{
   debug_assert(idx + num_elts < vector_length(array));
   debug_assert(offset + num_elts < vector_length(src));

   VECTOR_ELEMENT_TYPE *dest =
      (VECTOR_ELEMENT_TYPE *)writable_buffer_data(&array->wb);
   VECTOR_ELEMENT_TYPE *source =
      (VECTOR_ELEMENT_TYPE *)writable_buffer_data(&array->wb);
   memcpy(&dest[idx], &source[offset], num_elts);
}

VECTOR_STORAGE platform_status
VECTOR_FUNC_NAME(insert)(VECTOR_NAME        *array,
                         uint64              idx,
                         VECTOR_ELEMENT_TYPE elt)
{
   uint64 length = vector_length(array);
   debug_assert(idx <= length);
   platform_status rc =
      writable_buffer_resize(&array->wb, (length + 1) * sizeof(elt));
   if (!SUCCESS(rc)) {
      return rc;
   }
   VECTOR_ELEMENT_TYPE *data =
      (VECTOR_ELEMENT_TYPE *)writable_buffer_data(&array->wb);
   memmove(&data[idx + 1], &data[idx], (length - idx) * sizeof(elt));
   data[idx] = elt;
   return rc;
}

VECTOR_STORAGE
platform_status
VECTOR_FUNC_NAME(insert_c_array)(VECTOR_NAME         *array,
                                 uint64               idx,
                                 uint64               num_elts,
                                 VECTOR_ELEMENT_TYPE *elts)
{
   uint64 length = vector_length(array);
   debug_assert(idx <= length);
   platform_status rc =
      writable_buffer_resize(&array->wb, (length + num_elts) * sizeof(*elts));
   if (!SUCCESS(rc)) {
      return rc;
   }
   VECTOR_ELEMENT_TYPE *data =
      (VECTOR_ELEMENT_TYPE *)writable_buffer_data(&array->wb);
   memmove(&data[idx + num_elts], &data[idx], (length - idx) * sizeof(*elts));
   memcpy(&data[idx], elts, num_elts * sizeof(*elts));
   return rc;
}

VECTOR_STORAGE
void VECTOR_FUNC_NAME(delete)(VECTOR_NAME *array, uint64 idx, uint64 num_elts)
{
   uint64 length = vector_length(array);
   debug_assert(idx <= length);
   debug_assert(idx + num_elts <= length);
   VECTOR_ELEMENT_TYPE *data =
      (VECTOR_ELEMENT_TYPE *)writable_buffer_data(&array->wb);
   memmove(&data[idx],
           &data[idx + num_elts],
           num_elts * sizeof(VECTOR_ELEMENT_TYPE));
   platform_status rc = writable_buffer_resize(
      &array->wb, (length - num_elts) * sizeof(VECTOR_ELEMENT_TYPE));
   platform_assert_status_ok(rc);
}
