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
 *
 * e.g.
 *
 * #define VECTOR_NAME pivot_array
 * #define VECTOR_ELEMENT_TYPE pivot *
 *
 */

#include "util.h"

typedef struct VECTOR_NAME {
   writable_buffer wb;
} VECTOR_NAME;
