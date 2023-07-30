// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

#include "merge.h"
#include "iterator.h"
#include "util.h"

#include "poison.h"

/* This struct is just used to define type-safe flags. */
struct merge_behavior {
   // struct must have non-zero size or compilers will place all
   // instances of the struct at a single address.
   char dummy;
} merge_raw, merge_intermediate, merge_full;

/* Function declarations and iterator_ops */
void
merge_curr(iterator *itor, key *curr_key, message *data);

bool32
merge_can_prev(iterator *itor);

bool32
merge_can_next(iterator *itor);

platform_status
merge_next(iterator *itor);

platform_status
merge_prev(iterator *itor);

static iterator_ops merge_ops = {
   .curr     = merge_curr,
   .can_prev = merge_can_prev,
   .can_next = merge_can_next,
   .next     = merge_next,
   .prev     = merge_prev,
};

/*
 * bsearch which returns insertion position
 *
 * *(prev|next)_equal is set to TRUE if we compared to previous(next) index and
 * first attempt comparison matched
 **/

/* Comparison function for bsearch of the min ritor array */
static inline int
bsearch_comp(const ordered_iterator *itor_one,
             const ordered_iterator *itor_two,
             const bool32            forwards,
             const data_config      *cfg,
             bool32                 *keys_equal)
{
   int cmp     = data_key_compare(cfg, itor_one->curr_key, itor_two->curr_key);
   cmp         = forwards ? cmp : -cmp;
   *keys_equal = (cmp == 0);
   if (cmp == 0) {
      cmp = itor_two->seq - itor_one->seq;
   }
   // Various optimizations require us not to have duplicate keys in a single
   // iterator so final cmp must not be 0
   debug_assert(cmp != 0);
   return cmp;
}

struct merge_ctxt {
   bool32       forwards;
   data_config *cfg;
};

/* Comparison function for sort of the min ritor array */
static int
merge_comp(const void *one, const void *two, void *ctxt)
{
   struct merge_ctxt      *m_ctxt   = (struct merge_ctxt *)ctxt;
   const ordered_iterator *itor_one = *(ordered_iterator **)one;
   const ordered_iterator *itor_two = *(ordered_iterator **)two;
   bool32                  forwards = m_ctxt->forwards;
   data_config            *cfg      = m_ctxt->cfg;
   bool32                  ignore_keys_equal;
   return bsearch_comp(itor_one, itor_two, forwards, cfg, &ignore_keys_equal);
}

// Returns index (from base0) where key belongs
static inline int
bsearch_insert(register const ordered_iterator *key,
               ordered_iterator               **base0,
               const size_t                     nmemb,
               const data_config               *cfg,
               bool32                           forwards,
               bool32                          *prev_equal_out,
               bool32                          *next_equal_out)
{
   register ordered_iterator **base = base0;
   register int                lim, cmp;
   register ordered_iterator **p;
   bool32                      prev_equal = FALSE;
   bool32                      next_equal = FALSE;


   for (lim = nmemb; lim != 0; lim >>= 1) {
      p = base + (lim >> 1);
      bool32 keys_equal;
      cmp = bsearch_comp(key, *p, forwards, cfg, &keys_equal);
      debug_assert(cmp != 0);

      if (cmp > 0) { /* key > p: move right */
         base = p + 1;
         lim--;
         // We can skip the redundant cmp > 0 check here:
         prev_equal |= keys_equal;
      } /* else move left */
      next_equal |= (cmp < 0) & keys_equal;
   }
   p -= (cmp < 0);
   *prev_equal_out = prev_equal;
   *next_equal_out = next_equal;
   return p - base0;
}

static inline void
set_curr_ordered_iterator(const data_config *cfg, ordered_iterator *itor)
{
   iterator_curr(itor->itor, &itor->curr_key, &itor->curr_data);
   debug_assert(key_is_user_key(itor->curr_key));
}

static inline void
debug_assert_message_type_valid(debug_only merge_iterator *merge_itor)
{
#if SPLINTER_DEBUG
   message_type type = message_class(merge_itor->curr_data);
   debug_assert(
      IMPLIES(!merge_itor->emit_deletes, type != MESSAGE_TYPE_DELETE));
   debug_assert(IMPLIES(merge_itor->finalize_updates,
                        message_is_definitive(merge_itor->curr_data)));
#endif
}

static void
debug_verify_sorted(debug_only merge_iterator *merge_itor,
                    debug_only const int       index)
{
#if SPLINTER_DEBUG
   if (index < 0 || index + 1 >= merge_itor->num_remaining) {
      return;
   }
   const int cmp =
      data_key_compare(merge_itor->cfg,
                       merge_itor->ordered_iterators[index]->curr_key,
                       merge_itor->ordered_iterators[index + 1]->curr_key);
   if (merge_itor->ordered_iterators[index]->next_key_equal) {
      debug_assert(cmp == 0);
   } else {
      if (merge_itor->forwards)
         debug_assert(cmp < 0);
      else
         debug_assert(cmp > 0);
   }
#endif
}

static inline platform_status
advance_and_resort_min_ritor(merge_iterator *merge_itor)
{
   platform_status rc;

   debug_assert(!key_equals(merge_itor->curr_key,
                            merge_itor->ordered_iterators[0]->curr_key));

   merge_itor->ordered_iterators[0]->next_key_equal = FALSE;
   merge_itor->ordered_iterators[0]->curr_key       = NULL_KEY;
   merge_itor->ordered_iterators[0]->curr_data      = NULL_MESSAGE;
   if (merge_itor->forwards) {
      rc = iterator_next(merge_itor->ordered_iterators[0]->itor);
   } else {
      rc = iterator_prev(merge_itor->ordered_iterators[0]->itor);
   }

   if (!SUCCESS(rc)) {
      return rc;
   }

   // if it's exhausted, kill it and move the ritors up the queue.
   if (UNLIKELY(!iterator_can_curr(merge_itor->ordered_iterators[0]->itor))) {
      merge_itor->num_remaining--;
      ordered_iterator *tmp = merge_itor->ordered_iterators[0];
      for (int i = 0; i < merge_itor->num_remaining; ++i) {
         merge_itor->ordered_iterators[i] =
            merge_itor->ordered_iterators[i + 1];
      }
      merge_itor->ordered_iterators[merge_itor->num_remaining] = tmp;
      goto out;
   }

   // Pull out key and data (now that we know we aren't at end)
   set_curr_ordered_iterator(merge_itor->cfg, merge_itor->ordered_iterators[0]);
   if (merge_itor->num_remaining == 1) {
      goto out;
   }

   bool32 prev_equal;
   bool32 next_equal;
   // otherwise, find its position in the array
   // Add 1 to return value since it gives offset from [1]
   int index = 1
               + bsearch_insert(*merge_itor->ordered_iterators,
                                merge_itor->ordered_iterators + 1,
                                merge_itor->num_remaining - 1,
                                merge_itor->cfg,
                                merge_itor->forwards,
                                &prev_equal,
                                &next_equal);
   debug_assert(index >= 0);
   debug_assert(index < merge_itor->num_remaining);

   if (index != 0) {
      ordered_iterator *old_min_itor = merge_itor->ordered_iterators[0];
      memmove(&merge_itor->ordered_iterators[0],
              &merge_itor->ordered_iterators[1],
              index * sizeof(ordered_iterator *));
      // move the other pointers to make room
      // for (int i = 0; i < index; ++i) {
      //   merge_itor->ordered_iterators[i] =
      //      merge_itor->ordered_iterators[i + 1];
      //}
      merge_itor->ordered_iterators[index] = old_min_itor;
   }
   // This may access index -1 but that's fine.  See
   // ordered_iterator_stored_pad and ordered_iterators_pad
   merge_itor->ordered_iterators[index - 1]->next_key_equal = prev_equal;
   merge_itor->ordered_iterators[index]->next_key_equal     = next_equal;

   debug_verify_sorted(merge_itor, index - 1);
   debug_verify_sorted(merge_itor, index);

out:
   return STATUS_OK;
}

/*
 * In the case where the two minimum iterators of the merge iterator have equal
 * keys, resolve_equal_keys will merge the data as necessary
 */
static platform_status
merge_resolve_equal_keys(merge_iterator *merge_itor)
{
   debug_assert(merge_itor->ordered_iterators[0]->next_key_equal);
   debug_assert(message_data(merge_itor->curr_data)
                != merge_accumulator_data(&merge_itor->merge_buffer));
   debug_assert(key_equals(merge_itor->curr_key,
                           merge_itor->ordered_iterators[0]->curr_key));

   data_config *cfg = merge_itor->cfg;

#if SPLINTER_DEBUG
   ordered_iterator *expected_itor = merge_itor->ordered_iterators[1];
#endif

   // there is more than one copy of the current key
   bool32 success = merge_accumulator_copy_message(&merge_itor->merge_buffer,
                                                   merge_itor->curr_data);
   if (!success) {
      return STATUS_NO_MEMORY;
   }

   do {
      // Verify we don't fall off the end
      debug_assert(merge_itor->num_remaining >= 2);
      // Verify keys match
      debug_assert(
         !data_key_compare(cfg,
                           merge_itor->curr_key,
                           merge_itor->ordered_iterators[1]->curr_key));

      if (data_merge_tuples(cfg,
                            merge_itor->curr_key,
                            merge_itor->ordered_iterators[1]->curr_data,
                            &merge_itor->merge_buffer))
      {
         return STATUS_NO_MEMORY;
      }

      /*
       * Need to maintain invariant that merge_itor->curr_key points to a valid
       * page; this means that this pointer must be updated before the 0th
       * iterator is advanced
       */
      merge_itor->curr_key = merge_itor->ordered_iterators[1]->curr_key;
      debug_assert(key_is_user_key(merge_itor->curr_key));
      platform_status rc = advance_and_resort_min_ritor(merge_itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
#if SPLINTER_DEBUG
      debug_assert(expected_itor == merge_itor->ordered_iterators[0]);
      expected_itor = merge_itor->ordered_iterators[1];
#endif
   } while (merge_itor->ordered_iterators[0]->next_key_equal);

   merge_itor->curr_data =
      merge_accumulator_to_message(&merge_itor->merge_buffer);

   // Dealt with all duplicates, now pointing to last copy.
   debug_assert(!merge_itor->ordered_iterators[0]->next_key_equal);

   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 * if merge_itor->finalize_updates:
 *    resolves MESSAGE_TYPE_UPDATE messages into
 *       MESSAGE_TYPE_INSERT or MESSAGE_TYPE_DELETE messages
 * if merge_itor->delete_mode == dont_emit_deletes:
 *    discards MESSAGE_TYPE_DELETE messages
 * return True if it discarded a MESSAGE_TYPE_DELETE message
 *-----------------------------------------------------------------------------
 */
static inline platform_status
merge_finalize_updates_and_discard_deletes(merge_iterator *merge_itor,
                                           bool32         *discarded)
{
   data_config *cfg   = merge_itor->cfg;
   message_type class = message_class(merge_itor->curr_data);
   if (class != MESSAGE_TYPE_INSERT && merge_itor->finalize_updates) {
      if (message_data(merge_itor->curr_data)
          != merge_accumulator_data(&merge_itor->merge_buffer))
      {
         bool32 success = merge_accumulator_copy_message(
            &merge_itor->merge_buffer, merge_itor->curr_data);
         if (!success) {
            return STATUS_NO_MEMORY;
         }
      }
      if (data_merge_tuples_final(
             cfg, merge_itor->curr_key, &merge_itor->merge_buffer))
      {
         return STATUS_NO_MEMORY;
      }
      merge_itor->curr_data =
         merge_accumulator_to_message(&merge_itor->merge_buffer);
      class = message_class(merge_itor->curr_data);
   }
   if (class == MESSAGE_TYPE_DELETE && !merge_itor->emit_deletes) {
      merge_itor->discarded_deletes++;
      *discarded = TRUE;
   } else {
      *discarded = FALSE;
   }
   return STATUS_OK;
}

static platform_status
advance_one_loop(merge_iterator *merge_itor, bool32 *retry)
{
   *retry = FALSE;
   // Determine whether we're no longer in range.
   if (merge_itor->num_remaining == 0) {
      if (merge_itor->forwards) {
         merge_itor->can_next = FALSE;
      } else {
         merge_itor->can_prev = FALSE;
      }
      return STATUS_OK;
   }

   // set the next key/data from the min ritor
   merge_itor->curr_key = merge_itor->ordered_iterators[0]->curr_key;
   debug_assert(key_is_user_key(merge_itor->curr_key));
   merge_itor->curr_data = merge_itor->ordered_iterators[0]->curr_data;
   if (!merge_itor->merge_messages) {
      /*
       * We only have keys.  We COULD still merge (skip duplicates) the keys
       * but it would break callers.  Right now, rough estimates rely on the
       * duplicate keys outputted to improve the estimates.
       */
      return STATUS_OK;
   }

   platform_status rc;
   if (merge_itor->ordered_iterators[0]->next_key_equal) {
      rc = merge_resolve_equal_keys(merge_itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   bool32 discarded;
   rc = merge_finalize_updates_and_discard_deletes(merge_itor, &discarded);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (discarded) {
      *retry = TRUE;
      return STATUS_OK;
   }
   debug_assert_message_type_valid(merge_itor);
   return STATUS_OK;
}

/*
 * Given a merge_iterator with initialized ordered_iterators restore the
 * following invariants:
 *    1. Each iterator holds the appropriate key and value
 *    2. Dead iterators come after live iterators.
 *    3. Live iterators are in sorted order. (depends on merge_itor->forwards)
 *    4. Live iterators that have equal keys indicate so.
 *    5. The first key in the sorted order has all its iterators merged.
 */
static platform_status
setup_ordered_iterators(merge_iterator *merge_itor)
{
   platform_status   rc = STATUS_OK;
   ordered_iterator *temp;
   merge_itor->can_prev = FALSE;
   merge_itor->can_next = FALSE;

   // Move all the dead iterators to the end and count how many are still alive.
   merge_itor->num_remaining = merge_itor->num_trees;
   int i                     = 0;
   while (i < merge_itor->num_remaining) {
      // determine if the merge itor can go prev/next based upon ordered_itors
      if (iterator_can_prev(merge_itor->ordered_iterators[i]->itor)) {
         merge_itor->can_prev = TRUE;
      }
      if (iterator_can_next(merge_itor->ordered_iterators[i]->itor)) {
         merge_itor->can_next = TRUE;
      }

      if (!iterator_can_curr(merge_itor->ordered_iterators[i]->itor)) {
         ordered_iterator *tmp =
            merge_itor->ordered_iterators[merge_itor->num_remaining - 1];
         merge_itor->ordered_iterators[merge_itor->num_remaining - 1] =
            merge_itor->ordered_iterators[i];
         merge_itor->ordered_iterators[i] = tmp;
         merge_itor->num_remaining--;
      } else {
         set_curr_ordered_iterator(merge_itor->cfg,
                                   merge_itor->ordered_iterators[i]);
         i++;
      }
   }
   struct merge_ctxt merge_args;
   merge_args.forwards = merge_itor->forwards;
   merge_args.cfg      = merge_itor->cfg;
   platform_sort_slow(merge_itor->ordered_iterators,
                      merge_itor->num_remaining,
                      sizeof(*merge_itor->ordered_iterators),
                      merge_comp,
                      &merge_args,
                      &temp);
   // Generate initial value for next_key_equal bits
   for (int i = 0; i + 1 < merge_itor->num_remaining; ++i) {
      int cmp =
         data_key_compare(merge_itor->cfg,
                          merge_itor->ordered_iterators[i]->curr_key,
                          merge_itor->ordered_iterators[i + 1]->curr_key);
      if (merge_itor->forwards) {
         debug_assert(cmp <= 0);
      } else {
         debug_assert(cmp >= 0);
      }
      merge_itor->ordered_iterators[i]->next_key_equal = (cmp == 0);
   }

   bool32 retry;
   rc = advance_one_loop(merge_itor, &retry);

   if (retry && SUCCESS(rc)) {
      if (merge_itor->forwards) {
         rc = merge_next((iterator *)merge_itor);
      } else {
         rc = merge_prev((iterator *)merge_itor);
      }
   }

   if (!SUCCESS(rc)) {
      // destroy the iterator
      platform_status merge_iterator_rc =
         merge_iterator_destroy(platform_get_heap_id(), &merge_itor);
      if (!SUCCESS(merge_iterator_rc)) {
         platform_error_log(
            "setup_ordered_iterators: exception while releasing\n");
         if (SUCCESS(rc)) {
            platform_error_log("setup_ordered_iterators: clobbering rc\n");
            rc = merge_iterator_rc;
         }
      }
      platform_error_log("setup_ordered_iterators: exception: %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   if (!key_is_null(merge_itor->curr_key)) {
      debug_assert_message_type_valid(merge_itor);
   }

   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * merge_iterator_create --
 *
 *      Initialize a merge iterator for a forest of B-trees.
 *
 *      Prerequisite:
 *         All input iterators must be homogeneous for data_type
 *
 * Results:
 *      0 if successful, error otherwise
 *-----------------------------------------------------------------------------
 */
platform_status
merge_iterator_create(platform_heap_id hid,
                      data_config     *cfg,
                      int              num_trees,
                      iterator       **itor_arr,
                      merge_behavior   merge_mode,
                      merge_iterator **out_itor)
{
   int             i;
   platform_status rc = STATUS_OK;
   merge_iterator *merge_itor;

   if (!out_itor || !itor_arr || !cfg || num_trees < 0
       || num_trees >= ARRAY_SIZE(merge_itor->ordered_iterator_stored))
   {
      platform_error_log("merge_iterator_create: bad parameter merge_itor %p"
                         " num_trees %d itor_arr %p cfg %p\n",
                         out_itor,
                         num_trees,
                         itor_arr,
                         cfg);
      return STATUS_BAD_PARAM;
   }

   _Static_assert(ARRAY_SIZE(merge_itor->ordered_iterator_stored)
                     == ARRAY_SIZE(merge_itor->ordered_iterators),
                  "size mismatch");

   merge_itor = TYPED_ZALLOC(hid, merge_itor);
   if (merge_itor == NULL) {
      return STATUS_NO_MEMORY;
   }
   merge_accumulator_init(&merge_itor->merge_buffer, hid);

   merge_itor->super.ops = &merge_ops;
   merge_itor->num_trees = num_trees;

   debug_assert(merge_mode == MERGE_RAW || merge_mode == MERGE_INTERMEDIATE
                || merge_mode == MERGE_FULL);
   merge_itor->merge_messages   = merge_mode != MERGE_RAW;
   merge_itor->finalize_updates = merge_mode == MERGE_FULL;
   merge_itor->emit_deletes     = merge_mode != MERGE_FULL;

   merge_itor->cfg      = cfg;
   merge_itor->curr_key = NULL_KEY;
   merge_itor->forwards = TRUE;

   // index -1 initializes the pad variable
   for (i = -1; i < num_trees; i++) {
      merge_itor->ordered_iterator_stored[i] = (ordered_iterator){
         .seq            = i,
         .itor           = i == -1 ? NULL : itor_arr[i],
         .curr_key       = NULL_KEY,
         .curr_data      = NULL_MESSAGE,
         .next_key_equal = FALSE,
      };
      merge_itor->ordered_iterators[i] =
         &merge_itor->ordered_iterator_stored[i];
   }

   rc = setup_ordered_iterators(merge_itor);
   if (!SUCCESS(rc)) {
      return rc;
   }

   *out_itor = merge_itor;
   return rc;
}


/*
 *-----------------------------------------------------------------------------
 * merge_iterator_destroy --
 *
 *      Destroys a merge iterator.
 *-----------------------------------------------------------------------------
 */
platform_status
merge_iterator_destroy(platform_heap_id hid, merge_iterator **merge_itor)
{
   merge_accumulator_deinit(&(*merge_itor)->merge_buffer);
   platform_free(hid, *merge_itor);
   *merge_itor = NULL;

   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * merge_iterator_set_direction --
 *
 *      Prepares the merge iterator for traveling either forwards or backwards
 *      by calling next or prev respectively on each iterator and then
 *      resorting.
 *
 * Results:
 *      0 if successful, error otherwise
 *-----------------------------------------------------------------------------
 */
static platform_status
merge_iterator_set_direction(merge_iterator *merge_itor, bool32 forwards)
{
   debug_assert(merge_itor->forwards != forwards);
   platform_status rc;
   merge_itor->forwards = forwards;

   // Step every iterator, both alive and dead, in the appropriate direction.
   for (int i = 0; i < merge_itor->num_trees; i++) {
      if (forwards && iterator_can_next(merge_itor->ordered_iterators[i]->itor))
      {
         iterator_next(merge_itor->ordered_iterators[i]->itor);
      }
      if (!forwards
          && iterator_can_prev(merge_itor->ordered_iterators[i]->itor)) {
         iterator_prev(merge_itor->ordered_iterators[i]->itor);
      }
   }

   // restore iterator invariants
   rc = setup_ordered_iterators(merge_itor);

   return rc;
}


/*
 *-----------------------------------------------------------------------------
 * merge_can_prev and merge_can_next --
 *
 *      Checks if the iterator is able to move prev or next.
 *      The half open range [start_key, end_key) defines the iterator's bounds.
 *
 * Results:
 *      Returns TRUE if the iterator can move as requested, FALSE otherwise.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
bool32
merge_can_prev(iterator *itor)
{
   merge_iterator *merge_itor = (merge_iterator *)itor;
   return merge_itor->can_prev;
}

bool32
merge_can_next(iterator *itor)
{
   merge_iterator *merge_itor = (merge_iterator *)itor;
   return merge_itor->can_next;
}

/*
 *-----------------------------------------------------------------------------
 * merge_get_curr --
 *
 *      Returns current key and data from the merge itor.
 *
 * Results:
 *      Current key and data.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
void
merge_curr(iterator *itor, key *curr_key, message *data)
{
   debug_assert(iterator_can_curr(itor));
   merge_iterator *merge_itor = (merge_iterator *)itor;
   *curr_key                  = merge_itor->curr_key;
   *data                      = merge_itor->curr_data;
}

static inline platform_status
merge_advance_helper(merge_iterator *merge_itor)
{
   platform_status rc = STATUS_OK;
   bool32          retry;
   do {
      merge_itor->curr_key  = NULL_KEY;
      merge_itor->curr_data = NULL_MESSAGE;
      // Advance one iterator
      rc = advance_and_resort_min_ritor(merge_itor);
      if (!SUCCESS(rc)) {
         return rc;
      }

      rc = advance_one_loop(merge_itor, &retry);
      if (!SUCCESS(rc)) {
         return rc;
      }
   } while (retry);

   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * merge_next --
 *
 *      Merges the next key from the array of input iterators.
 *
 * Results:
 *      0 if successful, error otherwise
 *-----------------------------------------------------------------------------
 */
platform_status
merge_next(iterator *itor)
{
   merge_iterator *merge_itor = (merge_iterator *)itor;

   if (!merge_itor->forwards) {
      return merge_iterator_set_direction(merge_itor, TRUE);
   }
   return merge_advance_helper(merge_itor);
}

/*
 *-----------------------------------------------------------------------------
 * merge_prev --
 *
 *      Merges the prev key from the array of input iterators.
 *
 * Results:
 *      0 if successful, error otherwise
 *-----------------------------------------------------------------------------
 */
platform_status
merge_prev(iterator *itor)
{
   merge_iterator *merge_itor = (merge_iterator *)itor;

   if (merge_itor->forwards) {
      return merge_iterator_set_direction(merge_itor, FALSE);
   }
   return merge_advance_helper(merge_itor);
}

void
merge_iterator_print(merge_iterator *merge_itor)
{
   uint64       i;
   key          curr_key;
   message      data;
   data_config *data_cfg = merge_itor->cfg;
   iterator_curr(&merge_itor->super, &curr_key, &data);

   platform_default_log("****************************************\n");
   platform_default_log("** merge iterator\n");
   platform_default_log("**  - trees: %u remaining: %u\n",
                        merge_itor->num_trees,
                        merge_itor->num_remaining);
   platform_default_log("** curr: %s\n", key_string(data_cfg, curr_key));
   platform_default_log("----------------------------------------\n");
   for (i = 0; i < merge_itor->num_trees; i++) {
      platform_default_log("%u: ", merge_itor->ordered_iterators[i]->seq);
      if (!iterator_can_curr(merge_itor->ordered_iterators[i]->itor)) {
         platform_default_log("# : ");
      } else {
         platform_default_log("_ : ");
      }
      if (i < merge_itor->num_remaining) {
         iterator_curr(
            merge_itor->ordered_iterators[i]->itor, &curr_key, &data);
         platform_default_log("%s\n", key_string(data_cfg, curr_key));
      } else {
         platform_default_log("\n");
      }
   }
   platform_default_log("\n");
}
