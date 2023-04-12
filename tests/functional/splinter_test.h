// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinter_test.h --
 *
 *    This file contains constants and functions the pertain to
 *    splinter tests.
 */

#ifndef _SPLINTER_TEST_H_
#define _SPLINTER_TEST_H_

#include "../config.h"
#include "test.h"

typedef enum test_type {
   basic,
   perf,
   delete,
   seq_perf,
   semiseq_perf,
   parallel_perf,
   periodic,
   functionality,
} test_type;

typedef struct test_config {
   uint64        tree_size;
   test_key_type key_type;
   uint64        semiseq_freq; // random key every this many keys
   uint64        period;       // if TEST_PERIODIC then repeat sequence after
                               // this many keys
   uint64 num_periods;         // if TEST_PERIODIC then repeat this many times
   test_message_generator *gen;
   test_exec_config       *test_exec_cfg; // Describes test's exec parameters
} test_config;

static inline void
test_config_set_defaults(test_type test, test_config *cfg)
{
   cfg->tree_size = GiB_TO_B(40);
   switch (test) {
      case perf:
         cfg->key_type     = TEST_RANDOM;
         cfg->semiseq_freq = 0;
         break;
      case delete:
         cfg->key_type     = TEST_RANDOM;
         cfg->semiseq_freq = 0;
         break;
      case seq_perf:
         cfg->key_type     = TEST_SEQ;
         cfg->semiseq_freq = 0;
         break;
      case semiseq_perf:
         cfg->key_type     = TEST_SEMISEQ;
         cfg->semiseq_freq = 10;
         break;
      case parallel_perf:
         cfg->key_type     = TEST_RANDOM;
         cfg->semiseq_freq = 0;
         break;
      case periodic:
         cfg->key_type     = TEST_RANDOM;
         cfg->semiseq_freq = 0;
      default:
         break;
   }
}

static inline platform_status
test_config_parse(test_config *cfg,
                  const uint8  num_config,
                  int         *argc_p,
                  char       **argv_p[])
{
   platform_status rc = STATUS_OK;
   uint64          i;
   int             argc = *argc_p;
   char          **argv = *argv_p;
   // temp config for parsing key-type
   typedef struct temp_config {
      char key_type[MAX_STRING_LENGTH];
   } temp_config;

   platform_memfrag  memfrag_temp_cfg;
   platform_memfrag *mf = &memfrag_temp_cfg;
   temp_config      *temp_cfg =
      TYPED_ARRAY_MALLOC(platform_get_heap_id(), temp_cfg, num_config);

   for (i = 0; i < argc; i++) {
      // Don't be mislead; this is not dead-code. See the config macro expansion
      if (0) {
         config_set_mib("tree-size", cfg, tree_size)
         {
            *argc_p -= 2;
            *argv_p += 2;
         }
         config_set_gib("tree-size", cfg, tree_size)
         {
            *argc_p -= 2;
            *argv_p += 2;
         }
         config_set_string("key-type", temp_cfg, key_type)
         {
            for (uint8 idx = 0; idx < num_config; idx++) {
               if (STRING_EQUALS_LITERAL(temp_cfg[idx].key_type, "rand"))
                  cfg[idx].key_type = TEST_RANDOM;
               else if (STRING_EQUALS_LITERAL(temp_cfg[idx].key_type, "seq"))
                  cfg[idx].key_type = TEST_SEQ;
               else if (STRING_EQUALS_LITERAL(temp_cfg[idx].key_type,
                                              "semiseq"))
                  cfg[idx].key_type = TEST_SEMISEQ;
               else {
                  platform_error_log("config: failed to parse key-type\n");
                  rc = STATUS_BAD_PARAM;
                  goto out;
               }
            }
            *argc_p -= 2;
            *argv_p += 2;
         }
         config_set_uint64("semiseq-freq", cfg, semiseq_freq)
         {
            *argc_p -= 2;
            *argv_p += 2;
         }
         config_set_else
         {
            goto out;
         }
      }
   out:
      platform_free(platform_get_heap_id(), mf);
      return rc;
   }

   static inline void test_config_usage(void)
   {
      platform_error_log("\nTest Configuration:\n");
      platform_error_log("\t--tree-size-gib\n");
      platform_error_log("\t--tree-size-mib\n");
      platform_error_log("\t--key-type (rand, seq, semiseq)\n");
      platform_error_log("\t--semiseq-freq\n");
   }

#endif /* _SPLINTER_TEST_H_ */
