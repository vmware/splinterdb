// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * variable_length_btree_test.c --
 *
 * Exercises the new BTree module that supports variable-length key and value
 * sizes.
 * -----------------------------------------------------------------------------
 */
#include <stdlib.h> // Needed for system calls; e.g. free
#include <stdio.h>
#include <unistd.h>
#include "ctest.h" // This is required for all test-case files.

#include "platform.h"
#include "config.h"
#include "variable_length_btree.h"
#include "test_data.h"

#define Mega (1024UL * 1024UL)

#define TEST_DB_NAME "ctestsdb"

// Function Prototypes

/*
 * Global data declaration macro:
 */
CTEST_DATA(btree)
{
   master_config                 master_cfg;
   data_config                   data_cfg;
   io_config                     io_cfg;
   rc_allocator_config           allocator_cfg;
   clockcache_config             cache_cfg;
   variable_length_btree_scratch test_scratch;
   variable_length_btree_config  dbtree_cfg;

   /*
   config_set_defaults(&master_cfg);
   data_cfg = test_data_config;
   */
};

// Setup function for suite, called before every test in suite
CTEST_SETUP(btree)
{
   config_set_defaults(&data->master_cfg);
   data->data_cfg = test_data_config;
    /*
   if (!SUCCESS(config_parse(&master_cfg, 1, argc - 1, argv + 1)) ||
       !init_data_config_from_master_config(&data_cfg, &master_cfg) ||
       !init_io_config_from_master_config(&io_cfg, &master_cfg) ||
       !init_rc_allocator_config_from_master_config(&allocator_cfg,
                                                    &master_cfg) ||
       !init_clockcache_config_from_master_config(&cache_cfg, &master_cfg) ||
       !init_variable_length_btree_config_from_master_config(
          &dbtree_cfg, &master_cfg, &data_cfg)) {
      platform_log("Failed to parse args\n");
      return -1;
   }

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;
   if (!SUCCESS(
          platform_heap_create(platform_get_module_id(), 1 * GiB, &hh, &hid))) {
      platform_log("Failed to init heap\n");
      return -3;
   }

   platform_io_handle io;
   uint8              num_bg_threads[NUM_TASK_TYPES] = {0};
   task_system *      ts;
   rc_allocator       al;
   clockcache         cc;

   if (!SUCCESS(io_handle_init(&io, &io_cfg, hh, hid)) ||
       !SUCCESS(task_system_create(hid,
                                   &io,
                                   &ts,
                                   master_cfg.use_stats,
                                   FALSE,
                                   num_bg_threads,
                                   sizeof(variable_length_btree_scratch))) ||
       !SUCCESS(rc_allocator_init(&al,
                                  &allocator_cfg,
                                  (io_handle *)&io,
                                  hh,
                                  hid,
                                  platform_get_module_id())) ||
       !SUCCESS(clockcache_init(&cc,
                                &cache_cfg,
                                (io_handle *)&io,
                                (allocator *)&al,
                                "test",
                                ts,
                                hh,
                                hid,
                                platform_get_module_id()))) {
      platform_log(
         "Failed to init io or task system or rc_allocator or clockcache\n");
      return -2;
   }
   */

}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(btree)
{
}

/*
 * ---------------------------------------------------------------------------
 * Test case that exercises inserts of large volume of data, single-threaded.
 * We exercise these Splinter APIs:
 *  - kvstore_basic_insert()
 *  - kvstore_basic_lookup() and
 *  - kvstore_basic_delete()
 */
CTEST2(btree, test_basic)
{
}
