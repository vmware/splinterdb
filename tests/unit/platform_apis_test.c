// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * platform_apis_test.c
 *
 *  Exercises some of the interfaces in platform.c
 * -----------------------------------------------------------------------------
 */
#include "ctest.h" // This is required for all test-case files.
#include "platform.h"

/*
 * Global data declaration macro:
 */
CTEST_DATA(platform_api)
{
   // Declare heap handles for platform heap memory.
   platform_heap_handle hh;
   platform_heap_id     hid;
   platform_module_id   mid;
};

CTEST_SETUP(platform_api)
{
   platform_status rc = STATUS_OK;

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.
   data->mid            = platform_get_module_id();
   rc = platform_heap_create(data->mid, heap_capacity, &data->hh, &data->hid);
   platform_assert_status_ok(rc);
}

CTEST_TEARDOWN(platform_api)
{
   platform_heap_destroy(&data->hh);
}

/*
 * Test platform_buffer_create() and platform_buffer_destroy().
 */
CTEST2(platform_api, test_platform_buffer_create)
{
   buffer_handle *bh = platform_buffer_create(KiB, data->hid, data->mid);
   ASSERT_TRUE(bh != NULL);

   ASSERT_TRUE(bh->addr == platform_buffer_getaddr(bh));

   platform_status rc = platform_buffer_destroy(data->hid, &bh);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(bh == NULL);
}
