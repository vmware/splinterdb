// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

#include "splinterdb/splinterdb.h"
#include "test.h"

#include "poison.h"

static void
usage(void)
{
   platform_error_log("List of tests:\n");
   platform_error_log("\tbtree_test\n");
   platform_error_log("\tfilter_test\n");
   platform_error_log("\tsplinter_test\n");
   platform_error_log("\tlog_test\n");
   platform_error_log("\tcache_test\n");
   platform_error_log("\tio_apis_test\n");
#ifdef PLATFORM_LINUX
   platform_error_log("\tycsb_test\n");
#endif
}


int
test_dispatcher(int argc, char *argv[])
{
   platform_set_log_streams(stdout, stderr);

   platform_default_log("%s: %s\n", argv[0], BUILD_VERSION);
   // check first arg and call the appropriate test
   if (argc > 1) {
      // check test name and dispatch
      char *test_name = argv[1];
      platform_default_log("Dispatch test %s\n", test_name);

      if (STRING_EQUALS_LITERAL(test_name, "btree_test")) {
         return btree_test(argc - 1, &argv[1]);
      } else if (STRING_EQUALS_LITERAL(test_name, "filter_test")) {
         return filter_test(argc - 1, &argv[1]);
      } else if (STRING_EQUALS_LITERAL(test_name, "splinter_test")) {
         return splinter_test(argc - 1, &argv[1]);
      } else if (STRING_EQUALS_LITERAL(test_name, "log_test")) {
         return log_test(argc - 1, &argv[1]);
      } else if (STRING_EQUALS_LITERAL(test_name, "cache_test")) {
         return cache_test(argc - 1, &argv[1]);
      } else if (STRING_EQUALS_LITERAL(test_name, "io_apis_test")) {
         return splinter_io_apis_test(argc - 1, &argv[1]);
#ifdef PLATFORM_LINUX
      } else if (STRING_EQUALS_LITERAL(test_name, "ycsb_test")) {
         return ycsb_test(argc - 1, &argv[1]);
#endif
      } else {
         // error test not found
         platform_error_log("invalid test\n");
         usage();
         return -1;
      }
   } else {
      platform_error_log("invalid arg\n");
      usage();
      return -1;
   }
}
