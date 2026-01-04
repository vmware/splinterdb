// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * vector_test.c --
 *
 *  Test the type-safe vector code.
 * -----------------------------------------------------------------------------
 */
#include "vector.h"
#include "ctest.h"

typedef VECTOR(uint64) uint64_vector;

CTEST_DATA(vector)
{
   uint64_vector empty;
   uint64_vector one;
   uint64_vector ten;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(vector)
{
   platform_register_thread();
   platform_heap_id hid = platform_get_heap_id();
   vector_init(&data->empty, hid);
   vector_init(&data->one, hid);
   vector_init(&data->ten, hid);

   platform_status rc = vector_append(&data->one, 0);
   platform_assert_status_ok(rc);
   for (uint64 i = 0; i < 10; i++) {
      rc = vector_append(&data->ten, i);
      platform_assert_status_ok(rc);
   }
}

CTEST_TEARDOWN(vector)
{
   vector_deinit(&data->empty);
   vector_deinit(&data->one);
   vector_deinit(&data->ten);
   platform_deregister_thread();
}

CTEST2(vector, length)
{
   ASSERT_EQUAL(0, vector_length(&data->empty));
   ASSERT_EQUAL(1, vector_length(&data->one));
   ASSERT_EQUAL(10, vector_length(&data->ten));
}

CTEST2(vector, get)
{
   ASSERT_EQUAL(0, vector_get(&data->one, 0));
   for (int i = 0; i < vector_length(&data->ten); i++) {
      ASSERT_EQUAL(i, vector_get(&data->ten, i));
   }
}

CTEST2(vector, get_ptr)
{
   ASSERT_EQUAL(0, vector_get(&data->one, 0));
   for (int i = 0; i < vector_length(&data->ten); i++) {
      ASSERT_EQUAL(i, *vector_get_ptr(&data->ten, i));
   }
}

CTEST2(vector, set)
{
   for (int i = 0; i < vector_length(&data->ten); i++) {
      vector_set(&data->ten, i, 2 * i);
   }
   for (int i = 0; i < vector_length(&data->ten); i++) {
      ASSERT_EQUAL(2 * i, vector_get(&data->ten, i));
   }
}

CTEST2(vector, truncate)
{
   vector_truncate(&data->ten, 5);
   ASSERT_EQUAL(5, vector_length(&data->ten));

   for (int i = 0; i < vector_length(&data->ten); i++) {
      ASSERT_EQUAL(i, vector_get(&data->ten, i));
   }
}

CTEST2(vector, copy)
{
   vector_copy(&data->one, &data->ten);

   ASSERT_EQUAL(10, vector_length(&data->one));

   for (int i = 0; i < vector_length(&data->one); i++) {
      ASSERT_EQUAL(i, vector_get(&data->one, i));
   }
}

void
sumvi(uint64_vector *v, uint64 idx, uint64 *acc)
{
   *acc += vector_get(v, idx);
}

CTEST2(vector, apply_generic_function)
{
   uint64 acc = 0;
   VECTOR_APPLY_GENERIC(&data->ten, sumvi, &acc);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

#define summacro(v, i, a) sumvi(v, i, &a)

CTEST2(vector, apply_generic_macro)
{
   uint64 acc = 0;
   VECTOR_APPLY_GENERIC(&data->ten, summacro, acc);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

void
sumv(uint64 elt, uint64 *acc)
{
   *acc += elt;
}

CTEST2(vector, apply_to_elts)
{
   uint64 acc = 0;
   VECTOR_APPLY_TO_ELTS(&data->ten, sumv, &acc);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

void
sumaddrv(uint64 *elt, uint64 *acc)
{
   *acc += *elt;
}

CTEST2(vector, apply_to_ptrs)
{
   uint64 acc = 0;
   VECTOR_APPLY_TO_PTRS(&data->ten, sumaddrv, &acc);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

uint64
square(uint64 x)
{
   return x * x;
}

CTEST2(vector, map_elts)
{
   VECTOR_MAP_ELTS(&data->empty, square, &data->ten);

   ASSERT_EQUAL(10, vector_length(&data->empty));
   for (int i = 0; i < vector_length(&data->ten); i++) {
      ASSERT_EQUAL(i * i, vector_get(&data->empty, i));
   }
}

uint64
squarep(uint64 *x)
{
   return *x * *x;
}

CTEST2(vector, map_ptrs)
{
   VECTOR_MAP_PTRS(&data->empty, squarep, &data->ten);

   ASSERT_EQUAL(10, vector_length(&data->empty));
   for (int i = 0; i < vector_length(&data->ten); i++) {
      ASSERT_EQUAL(i * i, vector_get(&data->empty, i));
   }
}

uint64
add(uint64 acc, uint64_vector *v, uint64 idx)
{
   return acc + vector_get(v, idx);
}

CTEST2(vector, fold_left_generic_function)
{
   uint64 acc = VECTOR_FOLD_LEFT_GENERIC(&data->ten, add, 0);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

#define addmacro(a, v, i) a + vector_get(v, i)

CTEST2(vector, fold_left_generic_macro)
{
   uint64 acc = VECTOR_FOLD_LEFT_GENERIC(&data->ten, addmacro, 0);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

CTEST2(vector, fold_right_generic_function)
{
   uint64 acc = VECTOR_FOLD_RIGHT_GENERIC(&data->ten, add, 0);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

#define addmacro(a, v, i) a + vector_get(v, i)

CTEST2(vector, fold_right_generic_macro)
{
   uint64 acc = VECTOR_FOLD_RIGHT_GENERIC(&data->ten, addmacro, 0);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

uint64
addee(uint64 a, uint64 b)
{
   return a + b;
}

CTEST2(vector, fold_left_acc_elt)
{
   uint64 acc = VECTOR_FOLD_LEFT_ACC_ELT(&data->ten, addee, 0);
   for (int i = 0; i < vector_length(&data->ten); i++) {
      acc -= vector_get(&data->ten, i);
   }
   ASSERT_EQUAL(0, acc);
}

platform_status
assignvi(uint64_vector *v, uint64 i, uint64 val)
{
   vector_set(v, i, val);
   return STATUS_OK;
}

CTEST2(vector, emplace_append_generic)
{
   uint64          val = vector_length(&data->ten);
   platform_status rc =
      VECTOR_EMPLACE_APPEND_GENERIC(&data->ten, assignvi, val);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(11, vector_length(&data->ten));
   ASSERT_EQUAL(10, vector_get(&data->ten, 10));
}

platform_status
assignelt(uint64 *v, uint64 val)
{
   *v = val;
   return STATUS_OK;
}

CTEST2(vector, emplace_append)
{
   platform_status rc = VECTOR_EMPLACE_APPEND(&data->ten, assignelt, 32);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_EQUAL(11, vector_length(&data->ten));
   ASSERT_EQUAL(32, vector_get(&data->ten, 10));
}

platform_status
emplacevi_fail_after_5(uint64 *v, uint64_vector *src, uint64 i)
{
   if (i < 5) {
      *v = vector_get(src, i);
      return STATUS_OK;
   } else {
      return STATUS_NO_MEMORY;
   }
}

CTEST2(vector, emplace_map_generic)
{
   platform_status rc = VECTOR_EMPLACE_MAP_GENERIC(
      &data->empty, emplacevi_fail_after_5, &data->ten);
   ASSERT_FALSE(SUCCESS(rc));
   ASSERT_EQUAL(5, vector_length(&data->empty));
   for (int i = 0; i < vector_length(&data->empty); i++) {
      ASSERT_EQUAL(i, vector_get(&data->empty, i));
   }
}

platform_status
emplaceelt_fail_after_5(uint64 *v, uint64 src)
{
   if (src < 5) {
      *v = src;
      return STATUS_OK;
   } else {
      return STATUS_NO_MEMORY;
   }
}

CTEST2(vector, emplace_map_elts)
{
   platform_status rc = VECTOR_EMPLACE_MAP_ELTS(
      &data->empty, emplaceelt_fail_after_5, &data->ten);
   ASSERT_FALSE(SUCCESS(rc));
   ASSERT_EQUAL(5, vector_length(&data->empty));
   for (int i = 0; i < vector_length(&data->empty); i++) {
      ASSERT_EQUAL(i, vector_get(&data->empty, i));
   }
}

platform_status
emplaceptr_fail_after_5(uint64 *v, uint64 *src)
{
   if (*src < 5) {
      *v = *src;
      return STATUS_OK;
   } else {
      return STATUS_NO_MEMORY;
   }
}

CTEST2(vector, emplace_map_ptrs)
{
   platform_status rc = VECTOR_EMPLACE_MAP_PTRS(
      &data->empty, emplaceptr_fail_after_5, &data->ten);
   ASSERT_FALSE(SUCCESS(rc));
   ASSERT_EQUAL(5, vector_length(&data->empty));
   for (int i = 0; i < vector_length(&data->empty); i++) {
      ASSERT_EQUAL(i, vector_get(&data->empty, i));
   }
}
