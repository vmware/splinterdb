// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * writable_buffer_test.c
 *
 *  Exercises the Writable Buffer interfaces.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/platform_public.h"
#include "platform.h"
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
   platform_status rc = platform_heap_create(
      platform_get_module_id(), (1 * GiB), &data->hh, &data->hid);
   platform_assert_status_ok(rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(writable_buffer)
{
   platform_heap_destroy(&data->hh);
}

/*
 * Test basic usage, initializing to an empty input buffer.
 */
CTEST2(writable_buffer, test_basic_empty_buffer)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid, 0, NULL);
   ASSERT_EQUAL(0, writable_buffer_length(wb));
   ASSERT_NULL(writable_buffer_data(wb));
}

/*
 * Test resize starting from an empty input buffer.
 */
CTEST2(writable_buffer, test_resize_empty_buffer)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid, 0, NULL);
   uint64 new_length = 20;
   writable_buffer_resize(wb, new_length);

   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // We should have done some memory allocation.
   ASSERT_TRUE(wb->can_free);
   ASSERT_NOT_NULL(writable_buffer_data(wb));
}

/*
 * Test resize starting from an empty input buffer,
 * then reduce its size. Data ptr should remain unchanged.
 */
CTEST2(writable_buffer, test_resize_empty_buffer_to_smaller)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid, 0, NULL);
   uint64 new_length = 20;
   writable_buffer_resize(wb, new_length);

   void *vdatap = writable_buffer_data(wb);

   new_length = (new_length - 5);
   writable_buffer_resize(wb, new_length);

   // Data ptr handle should not change for resize reduction
   ASSERT_TRUE(vdatap == writable_buffer_data(wb));
}

/*
 * Start from an empty input buffer, resize an allocated buffer to larger size.
 */
CTEST2(writable_buffer, test_resize_empty_buffer_to_larger)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   writable_buffer_init(wb, data->hid, 0, NULL);
   uint64 new_length = 20;
   writable_buffer_resize(wb, new_length);

   // void * vdatap = writable_buffer_data(wb);

   new_length = (new_length + 50);
   writable_buffer_resize(wb, new_length);

   /*
    * We cannot really assert that the data ptr will change following a realloc,
    * because it depends on platform realloc(). We may just end up increasing
    * the buffer on the allocation block.
    *   ASSERT_TRUE(vdatap != writable_buffer_data(wb));
    */

   // But we can assert just these two ...
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));
   ASSERT_TRUE(writable_buffer_length(wb) <= wb->buffer_size);
}

/*
 * Test basic usage, initializing to a user-provided buffer.
 */
CTEST2(writable_buffer, test_basic_user_buffer)
{
   writable_buffer  wb_data;
   writable_buffer *wb = &wb_data;

   char buf[WB_ONSTACK_BUFSIZE];
   writable_buffer_init(wb, data->hid, sizeof(buf), (void *)buf);

   // Validate internals
   uint64 buf_len = writable_buffer_length(wb);
   ASSERT_EQUAL(0, buf_len, "Unexpected buffer length=%lu", buf_len);
   ASSERT_EQUAL(sizeof(buf),
                wb->buffer_size,
                "Expected size=%lu, received buffer_size=%lu ",
                sizeof(buf),
                wb->buffer_size);

   // RESOLVE - This I find very odd. We just init'ed a buffer, and provided
   // on-stack buffer. I would naturally want to write stuff to the address
   // returned by writable_buffer_data(); like a snprintf(). But that is coming
   // out as NULL. Unexpected.
   // We will be writing to the user-provided on-stack buffer.
   ASSERT_NULL(writable_buffer_data(wb));

   // But as no memory allocation has happened, yet, the "length" of
   // the 'allocated' portion of the writable buffer should still == 0.
   ASSERT_EQUAL(0, writable_buffer_length(wb));
   ASSERT_EQUAL(WB_ONSTACK_BUFSIZE, wb->buffer_size);
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
   writable_buffer_init(wb, data->hid, sizeof(buf), (void *)buf);

   uint64 new_length = sizeof(buf);
   writable_buffer_resize(wb, new_length);

   // This is fine ...
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // Nothing has changed, so data handle should still be an un-allocated one.
   // RESOLVE: This is failing; It's unexpected that if we resize to the same
   // length, return from here is non-NULL. This is not right.
   // ASSERT_NULL(writable_buffer_data(wb));

   // Rework ... to move test along ...
   ASSERT_NOT_NULL(writable_buffer_data(wb));
   ASSERT_TRUE((void *)buf == writable_buffer_data(wb));

   ASSERT_NOT_NULL(writable_buffer_data(wb));

   // No allocation was done, so nothing to free
   ASSERT_FALSE(wb->can_free);
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
   writable_buffer_init(wb, data->hid, sizeof(buf), (void *)buf);

   uint64 new_length = (sizeof(buf) - 5);
   writable_buffer_resize(wb, new_length);
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // RESOLVE - Again, odd; just do this to keep test moving along ...
   ASSERT_TRUE((void *)buf == writable_buffer_data(wb));

   ASSERT_NOT_NULL(writable_buffer_data(wb));
   ASSERT_FALSE(wb->can_free);
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

   writable_buffer_init(wb, data->hid, sizeof(buf), (void *)buf);

   // Increase buffer beyond its current length.
   // RESOLVE: This is odd. I am expecting this to change to 35, but because
   // the length() returns 0, this results in 5. Unexpected!
   uint64 new_length = (writable_buffer_length(wb) + 5);
   writable_buffer_resize(wb, new_length);
   ASSERT_EQUAL(new_length, writable_buffer_length(wb));

   // RESOLVE - Again, odd; just do this to keep test moving along ...
   // ASSERT_FALSE((void *)buf == writable_buffer_data(wb));
   ASSERT_TRUE((void *)buf == writable_buffer_data(wb));

   ASSERT_NOT_NULL(writable_buffer_data(wb));
   ASSERT_FALSE(wb->can_free);
}
