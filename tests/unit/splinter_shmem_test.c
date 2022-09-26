// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_shmem_test.c --
 *
 *  Exercises the interfaces in SplinterDB shared memory mgmt module.
 *  This is just a template file. Fill it out for your specific test suite.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "shmem.h"

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinter_shmem){};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(splinter_shmem) {}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinter_shmem) {}

/*
 * Basic test case. This goes through the basic create / destroy
 * interfaces to setup a shared memory segment. While at it, run through
 * few lookup interfaces to validate sizes.
 */
CTEST2(splinter_shmem, test_create_destroy_shmem)
{
   platform_heap_handle hh            = NULL;
   platform_heap_id     hid           = NULL;
   size_t               requested     = (512 * MiB); // bytes
   size_t               heap_capacity = requested;
   platform_status      rc            = platform_heap_create(
      platform_get_module_id(), heap_capacity, TRUE, &hh, &hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Total size of shared segment must be what requested for.
   ASSERT_EQUAL(platform_shmsize(hh), requested);

   // A small chunk at the head is used for shmem_info{} tracking struct
   ASSERT_EQUAL(platform_shmfree(hh),
                (requested - platform_shm_ctrlblock_size()));

   // Destroy shared memory and release memory.
   platform_shmdestroy(hh);
}
