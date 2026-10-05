// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "merge.h"
#include "splinterdb/default_data_config.h"
#include "platform_threads.h"
#include "ctest.h"

typedef struct merge_test_iterator {
   iterator    super;
   const char *keys;
   int         pos;
   bool32      fail_step;
   bool32      failed;
   bool32      updates_at_edges;
   uint64      steps;
} merge_test_iterator;

static void
merge_test_curr(iterator *base, key *k, message *msg)
{
   merge_test_iterator *itor = (merge_test_iterator *)base;
   platform_assert(!itor->failed && 0 <= itor->pos && itor->pos < 3);
   *k = key_create_from_slice(FALSE, slice_create(1, itor->keys + itor->pos));
   message_type type = itor->updates_at_edges && itor->pos != 1
                          ? MESSAGE_TYPE_UPDATE
                          : MESSAGE_TYPE_INSERT;
   *msg              = message_create(type, NULL, slice_create(1, "v"));
}

static bool32
merge_test_can_next(iterator *base)
{
   merge_test_iterator *itor = (merge_test_iterator *)base;
   return !itor->failed && itor->pos < 3;
}

static bool32
merge_test_can_prev(iterator *base)
{
   merge_test_iterator *itor = (merge_test_iterator *)base;
   return !itor->failed && itor->pos >= 0;
}

static platform_status
merge_test_step(iterator *base, int direction)
{
   merge_test_iterator *itor = (merge_test_iterator *)base;
   itor->steps++;
   if (itor->fail_step) {
      itor->failed = TRUE;
      return STATUS_IO_ERROR;
   }
   itor->pos += direction;
   return STATUS_OK;
}

static platform_status
merge_test_next(iterator *base)
{
   return merge_test_step(base, 1);
}

static platform_status
merge_test_prev(iterator *base)
{
   return merge_test_step(base, -1);
}

static const iterator_ops merge_test_ops = {
   .curr     = merge_test_curr,
   .can_next = merge_test_can_next,
   .can_prev = merge_test_can_prev,
   .next     = merge_test_next,
   .prev     = merge_test_prev,
};

static void
check_direction_change(bool32 forwards, int failing_child)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   merge_test_iterator children[] = {
      {.super = {.ops = &merge_test_ops}, .keys = "adg", .pos = 1},
      {.super = {.ops = &merge_test_ops}, .keys = "beh", .pos = 1},
      {.super = {.ops = &merge_test_ops}, .keys = "cfi", .pos = 1},
   };
   iterator *inputs[ARRAY_SIZE(children)];
   for (uint64 i = 0; i < ARRAY_SIZE(children); i++) {
      inputs[i]             = &children[i].super;
      children[i].fail_step = (int)i == failing_child;
   }
   merge_iterator *merge = NULL;
   platform_status rc    = merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                                              &cfg,
                                              ARRAY_SIZE(inputs),
                                              inputs,
                                              MERGE_FULL,
                                              !forwards,
                                              &merge);
   ASSERT_TRUE(SUCCESS(rc));
   rc = forwards ? iterator_next(&merge->super) : iterator_prev(&merge->super);
   if (failing_child >= 0) {
      ASSERT_EQUAL(STATUS_IO_ERROR.r, rc.r);
      ASSERT_TRUE(children[failing_child].failed);
      ASSERT_EQUAL(1, children[failing_child].steps);
   } else {
      ASSERT_TRUE(SUCCESS(rc));
      ASSERT_TRUE(iterator_can_curr(&merge->super));
      key     k;
      message msg;
      iterator_curr(&merge->super, &k, &msg);
      ASSERT_EQUAL(
         0,
         data_key_compare(&cfg, k, key_create(FALSE, 1, forwards ? "g" : "c")));
      for (uint64 i = 0; i < ARRAY_SIZE(children); i++) {
         ASSERT_EQUAL(1, children[i].steps);
      }
   }
   ASSERT_TRUE(
      SUCCESS(merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &merge)));
   platform_deregister_thread();
}

CTEST(merge, direction_change_propagates_child_errors)
{
   for (int child = 0; child < 3; child++) {
      check_direction_change(TRUE, child);
      check_direction_change(FALSE, child);
   }
}

CTEST(merge, direction_change_succeeds)
{
   check_direction_change(TRUE, -1);
   check_direction_change(FALSE, -1);
}

static int
merge_test_fail_finalize(const data_config *cfg,
                         slice              k,
                         merge_accumulator *msg)
{
   return STATUS_NO_MEMORY.r;
}

CTEST(merge, failed_direction_change_preserves_iterator_ownership)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   cfg.merge_tuples_final = merge_test_fail_finalize;
   for (int forwards = 0; forwards < 2; forwards++) {
      merge_test_iterator child = {
         .super            = {.ops = &merge_test_ops},
         .keys             = "abc",
         .pos              = 1,
         .updates_at_edges = TRUE,
      };
      iterator       *inputs[] = {&child.super};
      merge_iterator *merge    = NULL;
      ASSERT_TRUE(SUCCESS(merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                                                &cfg,
                                                1,
                                                inputs,
                                                MERGE_FULL,
                                                !forwards,
                                                &merge)));
      platform_status rc =
         forwards ? iterator_next(&merge->super) : iterator_prev(&merge->super);
      ASSERT_EQUAL(STATUS_NO_MEMORY.r, rc.r);
      ASSERT_TRUE(
         SUCCESS(merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &merge)));
   }
   platform_deregister_thread();
}

CTEST(merge, failed_creation_cleans_up)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   cfg.merge_tuples_final    = merge_test_fail_finalize;
   merge_test_iterator child = {
      .super            = {.ops = &merge_test_ops},
      .keys             = "abc",
      .pos              = 0,
      .updates_at_edges = TRUE,
   };
   iterator       *inputs[] = {&child.super};
   merge_iterator *merge    = NULL;
   platform_status rc       = merge_iterator_create(
      PROCESS_PRIVATE_HEAP_ID, &cfg, 1, inputs, MERGE_FULL, TRUE, &merge);
   ASSERT_EQUAL(STATUS_NO_MEMORY.r, rc.r);
   ASSERT_NULL(merge);
   platform_deregister_thread();
}
