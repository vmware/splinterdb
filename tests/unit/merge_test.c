// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "merge.h"
#include "splinterdb/default_data_config.h"
#include "platform_threads.h"
#include "ctest.h"

// Each child holds one single-character key per character of keys.
typedef struct merge_test_iterator {
   iterator    super;
   const char *keys;
   const char *vals; // one value character per key; "v" for every key if NULL
   int         pos;
   bool32      fail_step;
   bool32      failed;
   bool32      updates_at_edges;
   bool32      updates; // every message is an update
   uint64      steps;
} merge_test_iterator;

static int
merge_test_num_keys(const merge_test_iterator *itor)
{
   return strlen(itor->keys);
}

static void
merge_test_curr(iterator *base, key *k, message *msg)
{
   merge_test_iterator *itor = (merge_test_iterator *)base;
   platform_assert(!itor->failed && 0 <= itor->pos
                   && itor->pos < merge_test_num_keys(itor));
   *k = key_create_from_slice(FALSE, slice_create(1, itor->keys + itor->pos));
   message_type type =
      itor->updates || (itor->updates_at_edges && itor->pos != 1)
         ? MESSAGE_TYPE_UPDATE
         : MESSAGE_TYPE_INSERT;
   const char *val = itor->vals ? itor->vals + itor->pos : "v";
   *msg            = message_create(type, NULL, slice_create(1, val));
}

static bool32
merge_test_can_next(iterator *base)
{
   merge_test_iterator *itor = (merge_test_iterator *)base;
   return !itor->failed && itor->pos < merge_test_num_keys(itor);
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
   // The merge iterator must never step a child past either end.
   platform_assert(direction > 0 ? itor->pos < merge_test_num_keys(itor)
                                 : itor->pos >= 0);
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

CTEST(merge, maximum_arity_is_accepted)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   merge_test_iterator empty = {
      .super = {.ops = &merge_test_ops},
      .keys  = "",
      .pos   = 0,
   };
   iterator *inputs[MAX_MERGE_ARITY];
   for (uint64 i = 0; i < ARRAY_SIZE(inputs); i++) {
      inputs[i] = &empty.super;
   }

   merge_iterator *merge = NULL;
   platform_status rc    = merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                                              &cfg,
                                              MAX_MERGE_ARITY,
                                              inputs,
                                              MERGE_RAW,
                                              TRUE,
                                              &merge);
   ASSERT_TRUE(SUCCESS(rc));
   ASSERT_NOT_NULL(merge);
   ASSERT_TRUE(
      SUCCESS(merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &merge)));

   rc = merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                              &cfg,
                              MAX_MERGE_ARITY + 1,
                              inputs,
                              MERGE_RAW,
                              TRUE,
                              &merge);
   ASSERT_EQUAL(STATUS_BAD_PARAM.r, rc.r);
   ASSERT_NULL(merge);
   platform_deregister_thread();
}

/*
 * Merge updates by concatenating their values, oldest first, so a merged
 * message shows exactly which children contributed to it.
 */
static int
merge_test_concat_updates(const data_config *cfg,
                          slice              k,
                          message            old_message,
                          merge_accumulator *new_message)
{
   uint64 old_len = message_length(old_message);
   uint64 new_len = merge_accumulator_length(new_message);
   if (!merge_accumulator_resize(new_message, old_len + new_len)) {
      return -1;
   }
   char *data = merge_accumulator_data(new_message);
   memmove(data + old_len, data, new_len);
   memcpy(data, message_data(old_message), old_len);
   merge_accumulator_set_class(new_message, message_class(old_message));
   return 0;
}

static int
merge_test_finalize_updates(const data_config *cfg,
                            slice              k,
                            merge_accumulator *oldest_message)
{
   merge_accumulator_set_class(oldest_message, MESSAGE_TYPE_INSERT);
   return 0;
}

// An expected_key of 0 means the iterator is past one of its ends.
static void
merge_test_check_curr(merge_iterator *merge,
                      char            expected_key,
                      const char     *expected_val)
{
   if (expected_key == 0) {
      ASSERT_FALSE(iterator_can_curr(&merge->super));
      return;
   }
   ASSERT_TRUE(iterator_can_curr(&merge->super));
   key     k;
   message msg;
   iterator_curr(&merge->super, &k, &msg);
   ASSERT_EQUAL(1, key_length(k));
   ASSERT_EQUAL(expected_key, *(const char *)key_data(k));
   if (expected_val != NULL) {
      ASSERT_EQUAL(strlen(expected_val), message_length(msg));
      ASSERT_EQUAL(
         0, memcmp(expected_val, message_data(msg), message_length(msg)));
   }
}

/*
 * A direction change re-sorts the children. The child that sorts last must not
 * keep a next_key_equal flag from its old position, or once the other children
 * are exhausted it is treated as one of several copies of its key.
 */
CTEST(merge, direction_change_clears_stale_equal_key_flag)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   // Moving forward to "c" leaves the third child (at "d") flagged as equal to
   // the second. Moving back to "b" sorts the third child (now at "a") last.
   merge_test_iterator children[] = {
      {.super = {.ops = &merge_test_ops}, .keys = "c"},
      {.super = {.ops = &merge_test_ops}, .keys = "bd"},
      {.super = {.ops = &merge_test_ops}, .keys = "ad"},
   };
   iterator *inputs[ARRAY_SIZE(children)];
   for (uint64 i = 0; i < ARRAY_SIZE(children); i++) {
      inputs[i] = &children[i].super;
   }
   merge_iterator *merge = NULL;
   ASSERT_TRUE(SUCCESS(merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                                             &cfg,
                                             ARRAY_SIZE(inputs),
                                             inputs,
                                             MERGE_FULL,
                                             TRUE,
                                             &merge)));
   merge_test_check_curr(merge, 'a', NULL);
   ASSERT_TRUE(SUCCESS(iterator_next(&merge->super)));
   merge_test_check_curr(merge, 'b', NULL);
   ASSERT_TRUE(SUCCESS(iterator_next(&merge->super)));
   merge_test_check_curr(merge, 'c', NULL);
   ASSERT_TRUE(SUCCESS(iterator_prev(&merge->super)));
   merge_test_check_curr(merge, 'b', NULL);
   ASSERT_TRUE(SUCCESS(iterator_prev(&merge->super)));
   merge_test_check_curr(merge, 'a', NULL);
   ASSERT_TRUE(SUCCESS(iterator_prev(&merge->super)));
   merge_test_check_curr(merge, 0, NULL);
   ASSERT_TRUE(
      SUCCESS(merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &merge)));
   platform_deregister_thread();
}

/*
 * Merging the copies of the current key steps every child but the one with the
 * oldest copy past that key. A direction change must not land those children
 * back on it, or the key is returned again, merged from only some copies.
 */
CTEST(merge, direction_change_at_duplicate_key_moves_to_adjacent_key)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   cfg.merge_tuples               = merge_test_concat_updates;
   cfg.merge_tuples_final         = merge_test_finalize_updates;
   merge_test_iterator children[] = {
      {.super   = {.ops = &merge_test_ops},
       .keys    = "ab",
       .vals    = "01",
       .updates = TRUE},
      {.super   = {.ops = &merge_test_ops},
       .keys    = "bc",
       .vals    = "23",
       .updates = TRUE},
   };
   iterator *inputs[ARRAY_SIZE(children)];
   for (uint64 i = 0; i < ARRAY_SIZE(children); i++) {
      inputs[i] = &children[i].super;
   }
   merge_iterator *merge = NULL;
   ASSERT_TRUE(SUCCESS(merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                                             &cfg,
                                             ARRAY_SIZE(inputs),
                                             inputs,
                                             MERGE_FULL,
                                             TRUE,
                                             &merge)));
   merge_test_check_curr(merge, 'a', "0");
   ASSERT_TRUE(SUCCESS(iterator_next(&merge->super)));
   merge_test_check_curr(merge, 'b', "12");
   ASSERT_TRUE(SUCCESS(iterator_prev(&merge->super)));
   merge_test_check_curr(merge, 'a', "0");
   ASSERT_TRUE(SUCCESS(iterator_next(&merge->super)));
   merge_test_check_curr(merge, 'b', "12");
   ASSERT_TRUE(SUCCESS(iterator_next(&merge->super)));
   merge_test_check_curr(merge, 'c', "3");
   ASSERT_TRUE(SUCCESS(iterator_prev(&merge->super)));
   merge_test_check_curr(merge, 'b', "12");
   ASSERT_TRUE(SUCCESS(iterator_next(&merge->super)));
   merge_test_check_curr(merge, 'c', "3");
   ASSERT_TRUE(
      SUCCESS(merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &merge)));
   platform_deregister_thread();
}

/*
 * Step merge iterators over overlapping children back and forth at random and
 * check every position against the merged sequence computed from the children.
 */
CTEST(merge, random_direction_changes_match_merged_sequence)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   cfg.merge_tuples       = merge_test_concat_updates;
   cfg.merge_tuples_final = merge_test_finalize_updates;
   // Each child has its own key storage: the merge iterator asserts that
   // children never share key memory, which merged string literals would.
   struct {
      uint64 num_children;
      char   keys[4][5];
   } forests[] = {
      {3, {"c", "bd", "ad"}},
      {2, {"ab", "bc"}},
      {4, {"abcd", "bd", "abd", "d"}},
      {4, {"aceg", "bdfh", "cg", "ah"}},
      {3, {"a", "a", "a"}},
      {3, {"", "ab", "b"}},
   };
   // Each child's values are its own index, so a merged value lists the
   // children that hold the key, oldest first.
   const char *child_vals[] = {"0000", "1111", "2222", "3333"};
   uint64      seed         = 1;
   for (uint64 f = 0; f < ARRAY_SIZE(forests); f++) {
      uint64              num_children = forests[f].num_children;
      merge_test_iterator children[ARRAY_SIZE(forests[f].keys)];
      iterator           *inputs[ARRAY_SIZE(forests[f].keys)];
      for (uint64 i = 0; i < num_children; i++) {
         children[i] = (merge_test_iterator){
            .super   = {.ops = &merge_test_ops},
            .keys    = forests[f].keys[i],
            .vals    = child_vals[i],
            .updates = TRUE,
         };
         inputs[i] = &children[i].super;
      }

      char expected_keys[26];
      char expected_vals[26][ARRAY_SIZE(forests[f].keys) + 1];
      int  num_expected = 0;
      for (char c = 'a'; c <= 'z'; c++) {
         int len = 0;
         for (uint64 i = 0; i < num_children; i++) {
            if (strchr(children[i].keys, c) != NULL) {
               expected_vals[num_expected][len++] = '0' + i;
            }
         }
         if (len != 0) {
            expected_vals[num_expected][len] = '\0';
            expected_keys[num_expected++]    = c;
         }
      }

      merge_iterator *merge = NULL;
      ASSERT_TRUE(SUCCESS(merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                                                &cfg,
                                                num_children,
                                                inputs,
                                                MERGE_FULL,
                                                TRUE,
                                                &merge)));
      // -1 and num_expected are the positions past either end.
      int pos = 0;
      for (uint64 step = 0; step < 200; step++) {
         if (pos == -1 || pos == num_expected) {
            merge_test_check_curr(merge, 0, NULL);
         } else {
            merge_test_check_curr(
               merge, expected_keys[pos], expected_vals[pos]);
         }
         ASSERT_EQUAL(pos != -1, iterator_can_prev(&merge->super));
         ASSERT_EQUAL(pos != num_expected, iterator_can_next(&merge->super));

         seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
         bool32 forwards =
            pos == -1 || (pos != num_expected && (seed >> 33) % 2 == 0);
         platform_status rc = forwards ? iterator_next(&merge->super)
                                       : iterator_prev(&merge->super);
         ASSERT_TRUE(SUCCESS(rc), "forest %lu step %lu", f, step);
         pos += forwards ? 1 : -1;
      }
      ASSERT_TRUE(
         SUCCESS(merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &merge)));
   }
   platform_deregister_thread();
}

static bool32
merge_test_keep_one_key(void *context, key tuple_key)
{
   return *(const char *)key_data(tuple_key) == *(const char *)context;
}

CTEST(merge, filtering_does_not_eagerly_drain_nonmatching_sources)
{
   platform_register_thread();
   data_config cfg;
   default_data_config_init(&cfg);
   // Every update belongs to a rejected key. No finalizer may see it.
   cfg.merge_tuples_final = merge_test_fail_finalize;
   for (bool32 forwards = FALSE; forwards <= TRUE; forwards++) {
      char                selected   = forwards ? 'b' : 'y';
      merge_test_iterator children[] = {
         {.super = {.ops = &merge_test_ops}, .keys = forwards ? "ab" : "yz"},
         {.super   = {.ops = &merge_test_ops},
          .keys    = "acdefghijklmnopqrstuvwxz",
          .updates = TRUE},
      };
      iterator *inputs[] = {&children[0].super, &children[1].super};
      for (uint64 i = 0; i < ARRAY_SIZE(children); i++) {
         children[i].pos = forwards ? 0 : merge_test_num_keys(&children[i]) - 1;
      }
      merge_iterator *merge = NULL;
      ASSERT_TRUE(
         SUCCESS(merge_iterator_create_with_filter(PROCESS_PRIVATE_HEAP_ID,
                                                   &cfg,
                                                   ARRAY_SIZE(inputs),
                                                   inputs,
                                                   MERGE_FULL,
                                                   forwards,
                                                   merge_test_keep_one_key,
                                                   &selected,
                                                   &merge)));
      merge_test_check_curr(merge, selected, "v");
      ASSERT_EQUAL(1, children[1].steps);
      // Cross each bound and reverse; rejected update histories stay hidden.
      ASSERT_TRUE(SUCCESS(forwards ? iterator_prev(&merge->super)
                                   : iterator_next(&merge->super)));
      merge_test_check_curr(merge, 0, NULL);
      ASSERT_TRUE(SUCCESS(forwards ? iterator_next(&merge->super)
                                   : iterator_prev(&merge->super)));
      merge_test_check_curr(merge, selected, "v");
      ASSERT_TRUE(SUCCESS(forwards ? iterator_next(&merge->super)
                                   : iterator_prev(&merge->super)));
      merge_test_check_curr(merge, 0, NULL);
      ASSERT_TRUE(SUCCESS(forwards ? iterator_prev(&merge->super)
                                   : iterator_next(&merge->super)));
      merge_test_check_curr(merge, selected, "v");
      ASSERT_TRUE(
         SUCCESS(merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &merge)));
   }
   platform_deregister_thread();
}
