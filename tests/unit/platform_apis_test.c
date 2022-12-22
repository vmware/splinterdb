// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * platform_apis_test.c
 *
 *  Exercise some of the interfaces in platform.c
 * -----------------------------------------------------------------------------
 */
#include <sys/mman.h>

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
   // This test exercises error cases, so even when everything succeeds
   // it generates lots of "error" messages.
   // By default, that would go to stderr, which would pollute test output.
   // Here we ensure those expected error messages are only printed
   // when the caller sets the VERBOSE env var to opt-in.
   if (Ctest_verbose) {
      platform_set_log_streams(stdout, stderr);
      CTEST_LOG_INFO("\nVerbose mode on.  This test exercises an error case, "
                     "so on sucess it "
                     "will print a message that appears to be an error.\n");
   } else {
      FILE *dev_null = fopen("/dev/null", "w");
      ASSERT_NOT_NULL(dev_null);
      platform_set_log_streams(dev_null, dev_null);
   }

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
 * Test platform_buffer_init() and platform_buffer_deinit().
 */
CTEST2(platform_api, test_platform_buffer_init)
{
   platform_status rc = STATUS_NO_MEMORY;

   buffer_handle bh;
   ZERO_CONTENTS(&bh);

   rc = platform_buffer_init(&bh, KiB);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(bh.addr != NULL);
   ASSERT_TRUE(bh.addr == platform_buffer_getaddr(&bh));
   ASSERT_TRUE(bh.length == KiB);

   rc = platform_buffer_deinit(&bh);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_TRUE(bh.addr == NULL);
   ASSERT_TRUE(bh.length == 0);
}

/*
 * Test failure to mmap() a very large buffer_init().
 */
CTEST2(platform_api, test_platform_buffer_init_fails_for_very_large_length)
{
   platform_status rc = STATUS_NO_MEMORY;

   buffer_handle bh;
   ZERO_CONTENTS(&bh);

   size_t length = (1024 * KiB * GiB);

   // On most test machines we use, this is expected to fail as mmap() under
   // here will fail for very large lengths. (If this test case ever fails,
   // check the 'length' here and the machine's configuration to see why
   // mmap() unexpectedly succeeded.)
   rc = platform_buffer_init(&bh, length);
   ASSERT_FALSE(SUCCESS(rc));
   ASSERT_TRUE(bh.addr == NULL);
   ASSERT_TRUE(bh.length == 0);

   // deinit() would fail horribly when nothing was successfully mmap()'ed
   rc = platform_buffer_deinit(&bh);
   ASSERT_FALSE(SUCCESS(rc));
}
