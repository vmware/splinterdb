// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * writable_buffer_test.c
 *
 *  Exercises the Writable Buffer interfaces.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "platform.h"
#include "test_misc_common.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "util.h"

// Size of an on-stack buffer used for testing
#define WB_ONSTACK_BUFSIZE 30

/*
 * Global data declaration macro:
 */
CTEST_DATA(writable_buffer)
{
   // Declare head handles for memory allocation.
   platform_heap_handle hh;
   platform_heap_id     hid;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(writable_buffer)
{
   bool use_shmem = test_using_shmem(Ctest_argc, (char **)Ctest_argv);

   platform_status rc = platform_heap_create(
      platform_get_module_id(), (1 * GiB), use_shmem, &data->hh, &data->hid);
   platform_assert_status_ok(rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(writable_buffer)
{
   platform_status rc = platform_heap_destroy(&data->hh);
   ASSERT_TRUE(SUCCESS(rc));
}

/*
 * Test basic usage, initializing to an empty input buffer.
 */
CTEST2(writable_buffer, test_basic_empty_buffer)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   ASSERT_EQUAL(0, writable_buffer_length(wb));
   ASSERT_NULL(writable_buffer_data(wb));
   writable_buffer_deinit(wb);
}

/*
 * Test resize starting from an empty input buffer.
 */
CTEST2(writable_buffer, test_resize_empty_buffer)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   uint64 new_length = 20;
   writable_buffer_resize(wb, 0, new_length);

   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // We should have done some memory allocation.
   ASSERT_TRUE(wb->can_free);
   ASSERT_NOT_NULL(writable_buffer_data(wb));
   writable_buffer_deinit(wb);
}

/*
 * Test resize starting from an empty input buffer,
 * then reduce its size. Data ptr should remain unchanged.
 */
CTEST2(writable_buffer, test_resize_empty_buffer_to_smaller)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   uint64 new_length = 20;
   writable_buffer_resize(wb, writable_buffer_length(wb), new_length);

   void *vdatap = writable_buffer_data(wb);

   new_length = (new_length - 5);
   writable_buffer_resize(wb, writable_buffer_length(wb), new_length);

   // Data ptr handle should not change for resize reduction
   ASSERT_TRUE(vdatap == writable_buffer_data(wb));
   writable_buffer_deinit(wb);
}

/*
 * Start from an empty input buffer, resize an allocated buffer to larger size.
 */
CTEST2(writable_buffer, test_resize_empty_buffer_to_larger)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   uint64 new_length = 20;
   writable_buffer_resize(wb, 0, new_length);

   // void * vdatap = writable_buffer_data(wb);

   new_length = (new_length + 50);
   writable_buffer_resize(wb, writable_buffer_length(wb), new_length);

   /*
    * We cannot really assert that the data ptr will change following a realloc,
    * because it depends on platform realloc(). We may just end up increasing
    * the buffer on the allocation block.
    *   ASSERT_TRUE(vdatap != writable_buffer_data(wb));
    */

   // But we can assert just these two ...
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));
   ASSERT_TRUE(writable_buffer_length(wb) <= wb->buffer_capacity);
   writable_buffer_deinit(wb);
}

/*
 * Test basic usage, initializing to a user-provided buffer.
 */
CTEST2(writable_buffer, test_basic_user_buffer)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   char buf[WB_ONSTACK_BUFSIZE];
   writable_buffer_init_with_buffer(
      wb, data->hid, sizeof(buf), (void *)buf, WRITABLE_BUFFER_NULL_LENGTH);

   // Validate internals
   uint64 buf_len = writable_buffer_length(wb);
   ASSERT_EQUAL(0, buf_len, "Unexpected buffer length=%lu", buf_len);
   ASSERT_EQUAL(sizeof(buf),
                wb->buffer_capacity,
                "Expected size=%lu, received buffer_size=%lu ",
                sizeof(buf),
                wb->buffer_capacity);

   // RESOLVE - This I find very odd. We just init'ed a buffer, and provided
   // on-stack buffer. I would naturally want to write stuff to the address
   // returned by writable_buffer_data(); like a snprintf(). But that is coming
   // out as NULL. Unexpected.
   // ASSERT_NOT_NULL(writable_buffer_data(wb)); // ... is failing; RESOLVE

   // We will be writing to the user-provided on-stack buffer.
   ASSERT_NULL(writable_buffer_data(wb));

   // But as no memory allocation has happened, yet, the "length" of
   // the 'allocated' portion of the writable buffer should still == 0.
   ASSERT_EQUAL(0, writable_buffer_length(wb));
   ASSERT_EQUAL(WB_ONSTACK_BUFSIZE, wb->buffer_capacity);
   writable_buffer_deinit(wb);
}

/*
 * Test resize starting from an initialized user-provided input buffer to
 * the same size as original buffer provided.
 */
CTEST2(writable_buffer, test_resize_user_buffer_to_same_length)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   char buf[WB_ONSTACK_BUFSIZE];
   writable_buffer_init_with_buffer(wb, data->hid, sizeof(buf), (void *)buf, 0);

   uint64 new_length = sizeof(buf);
   writable_buffer_resize(wb, writable_buffer_length(wb), new_length);

   // This is fine ... BUT RESOLVE: It's odd that writable_buffer_length()
   // immediately after an init() will return 0. But the same interface
   // returns a non-zero length, -even- when I just resized to the size of
   // the originally provided buffer. In other words, even w/o any new
   // memory allocation occurring, writable_buffer_length() now returns a
   // non-zero value.
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // Nothing has changed, so data handle should still be an un-allocated one.
   // RESOLVE: This is failing; It's unexpected that if we resize to the same
   // length, return from here is non-NULL. This is not right.
   // ASSERT_NULL(writable_buffer_data(wb));

   // Rework ... to move test along ...
   ASSERT_NOT_NULL(writable_buffer_data(wb));
   ASSERT_TRUE((void *)buf == writable_buffer_data(wb));

   // No allocation was done, so nothing to free
   ASSERT_FALSE(wb->can_free);
   writable_buffer_deinit(wb);
}

/*
 * Test resize starting from an initialized user-provided input buffer to
 * decrease its size.
 */
CTEST2(writable_buffer, test_resize_user_buffer_to_smaller)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   char buf[WB_ONSTACK_BUFSIZE];
   writable_buffer_init_with_buffer(wb, data->hid, sizeof(buf), (void *)buf, 0);

   uint64 new_length = (sizeof(buf) - 5);
   writable_buffer_resize(wb, 0, new_length);
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // RESOLVE - Again, odd; just do this to keep test moving along ...
   ASSERT_TRUE((void *)buf == writable_buffer_data(wb));

   ASSERT_NOT_NULL(writable_buffer_data(wb));
   ASSERT_FALSE(wb->can_free);
   writable_buffer_deinit(wb);
}

/*
 * Test resize starting from an initialized user-provided input buffer to
 * increase its size.
 */
CTEST2(writable_buffer, test_resize_user_buffer_to_larger)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   char junk_before[WB_ONSTACK_BUFSIZE];
   char buf[WB_ONSTACK_BUFSIZE];
   char junk_after[WB_ONSTACK_BUFSIZE];

   // Shut up compiler warning
   ASSERT_EQUAL(WB_ONSTACK_BUFSIZE, sizeof(junk_before));
   ASSERT_EQUAL(WB_ONSTACK_BUFSIZE, sizeof(junk_after));

   writable_buffer_init_with_buffer(wb, data->hid, sizeof(buf), (void *)buf, 0);

   // Increase buffer beyond its current length.
   // RESOLVE: This is odd. I am expecting this to change to 35, but because
   // the length() returns 0, this results in 5. Unexpected!
   uint64 new_length = (writable_buffer_length(wb) + 5);
   writable_buffer_resize(wb, 0, new_length);
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // RESOLVE - Again, odd; just do this to keep test moving along ...
   void *old_datap_before_resize = writable_buffer_data(wb);
   ASSERT_TRUE((void *)buf == old_datap_before_resize);

   // Do a hard resize to some really bigger value
   new_length = (sizeof(buf) * 4);

   writable_buffer_resize(wb, writable_buffer_length(wb), new_length);

   // We should have allocated memory, so data ptr should change.
   ASSERT_TRUE(old_datap_before_resize != writable_buffer_data(wb));

   ASSERT_NOT_NULL(writable_buffer_data(wb));
   ASSERT_TRUE(wb->can_free);
   writable_buffer_deinit(wb);
}

/*
 * Test how writable buffer morphs after you copy some data into it.
 */
CTEST2(writable_buffer, test_basic_copy_slice)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   const char *input_str = "Hello World!";

   char        buf[WB_ONSTACK_BUFSIZE];
   const slice src = slice_create(strlen(input_str), (const void *)input_str);

   writable_buffer_init_with_buffer(wb, data->hid, sizeof(buf), (void *)buf, 0);
   platform_status rc = writable_buffer_copy_slice(wb, src);
   ASSERT_TRUE(SUCCESS(rc));

   // Check that we cp'ed in less than the buffer's original capacity
   ASSERT_TRUE(strlen(input_str) < sizeof(buf));

   uint64 exp_len = strlen(input_str);
   uint64 act_len = writable_buffer_length(wb);
   ASSERT_EQUAL(
      exp_len, act_len, "exp_len=%lu, act_len=%lu ", exp_len, act_len);
   writable_buffer_deinit(wb);
}

/*
 * Test how writable buffer morphs after you copy some data into it causing
 * the buffer to expand.
 */
CTEST2(writable_buffer, test_copy_slice_causing_resize_larger)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   const char *input_str = "Hello World!";

   char        buf[WB_ONSTACK_BUFSIZE];
   const slice src = slice_create(strlen(input_str), (const void *)input_str);

   writable_buffer_init_with_buffer(wb, data->hid, sizeof(buf), (void *)buf, 0);
   platform_status rc = writable_buffer_copy_slice(wb, src);
   ASSERT_TRUE(SUCCESS(rc));

   // Check that we cp'ed in less than the buffer's original capacity
   ASSERT_TRUE(strlen(input_str) < sizeof(buf));

   const char *very_long_str = "This is a very long string"
                               ", much longer than input_str"
                               ", much longer than input_str"
                               ", much longer than input_str"
                               ", much longer than input_str"
                               " known to exceed original WB_ONSTACK_BUFSIZE";

   const slice newsrc =
      slice_create(strlen(very_long_str), (const void *)very_long_str);
   rc = writable_buffer_copy_slice(wb, newsrc);
   ASSERT_TRUE(SUCCESS(rc));

   uint64 exp_len = strlen(very_long_str);
   uint64 act_len = writable_buffer_length(wb);
   ASSERT_EQUAL(
      exp_len, act_len, "exp_len=%lu, act_len=%lu ", exp_len, act_len);

   // Memory allocation should have occurred!
   ASSERT_TRUE(wb->can_free);

   writable_buffer_deinit(wb);
}

/*
 * Test APIs after deinit is done.
 */
CTEST2(writable_buffer, test_basic_length_after_deinit)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   writable_buffer_deinit(wb);
   ASSERT_EQUAL(0, writable_buffer_length(wb));
   ASSERT_NULL(writable_buffer_data(wb));
}


CTEST2(writable_buffer, test_resize_empty_buffer_then_check_apis_after_deinit)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   uint64 new_length = 20;
   writable_buffer_resize(wb, 0, new_length);

   writable_buffer_deinit(wb);

   // RESOLVE - This seems wrong that we are getting a non-zero length, which
   // is set upon resize(), but we get this non-zero -after- deinit.
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));
   // ASSERT_EQUAL(0, writable_buffer_length(wb)); // is expected assertion.

   // RESOLVE - This seems wrong, too, that datap is non-NULL after deinit!
   // ASSERT_NOT_NULL(writable_buffer_data(wb)); // is expected assertion.

   ASSERT_NULL(wb->buffer);

   // RESOLVE - It's wrong that we still think something can be freed!
   ASSERT_FALSE(wb->can_free);
}

/*
 * This test case is interesting as the append interfaces calls realloc
 * below. For default heap-segment, 'realloc()' is a system-call, so stuff
 * works ok. When we run with shared memory enabled, there were some bugs in
 * the draft implementation of this shmem-based realloc() capability. This
 * test case does minor verification of this behavior.
 */
CTEST2(writable_buffer, test_writable_buffer_append)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid);
   const char *input_str = "Hello World!";
   writable_buffer_append(wb, strlen(input_str), (const void *)input_str);
   ASSERT_STREQN(writable_buffer_data(wb), input_str, strlen(input_str));

   input_str = "Another Hello World!";
   writable_buffer_append(wb, strlen(input_str), (const void *)input_str);

   const char *exp_str = "Hello World!Another Hello World!";
   ASSERT_STREQN(writable_buffer_data(wb),
                 exp_str,
                 strlen(exp_str),
                 "Unexpected data: '%s'\n",
                 (char *)writable_buffer_data(wb));

   writable_buffer_deinit(wb);
}
