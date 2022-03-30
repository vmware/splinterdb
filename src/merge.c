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
merge_get_curr(iterator *itor, slice *key, message *data);

platform_status
merge_at_end(iterator *itor, bool *at_end);

platform_status
merge_advance(iterator *itor);

static iterator_ops merge_ops = {
   .get_curr = merge_get_curr,
   .at_end   = merge_at_end,
   .advance  = merge_advance,
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
             const data_config      *cfg,
             bool                   *keys_equal)
{
   int cmp     = data_key_compare(cfg, itor_one->key, itor_two->key);
   *keys_equal = (cmp == 0);
   if (cmp == 0) {
      cmp = itor_two->seq - itor_one->seq;
   }
   // Various optimizations require us not to have duplicate keys in a single
   // iterator so final cmp must not be 0
   debug_assert(cmp != 0);
   return cmp;
}

/* Comparison function for sort of the min ritor array */
static int
merge_comp(const void *one, const void *two, void *ctxt)
{
   const ordered_iterator *itor_one = *(ordered_iterator **)one;
   const ordered_iterator *itor_two = *(ordered_iterator **)two;
   data_config            *cfg      = (data_config *)ctxt;
   bool                    ignore_keys_equal;
   return bsearch_comp(itor_one, itor_two, cfg, &ignore_keys_equal);
}

// Returns index (from base0) where key belongs
static inline int
bsearch_insert(register const ordered_iterator *key,
               ordered_iterator               **base0,
               const size_t                     nmemb,
               const data_config               *cfg,
               bool                            *prev_equal_out,
               bool                            *next_equal_out)
{
   register ordered_iterator **base = base0;
   register int                lim, cmp;
   register ordered_iterator **p;
   bool                        prev_equal = FALSE;
   bool                        next_equal = FALSE;


   for (lim = nmemb; lim != 0; lim >>= 1) {
      p = base + (lim >> 1);
      bool keys_equal;
      cmp = bsearch_comp(key, *p, cfg, &keys_equal);
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
   iterator_get_curr(itor->itor, &itor->key, &itor->data);
}

static inline void
debug_assert_message_type_valid(debug_only merge_iterator *merge_itor)
{
#if SPLINTER_DEBUG
   message_type type = message_class(merge_itor->data);
   debug_assert(
      IMPLIES(!merge_itor->emit_deletes, type != MESSAGE_TYPE_DELETE));
   debug_assert(IMPLIES(merge_itor->finalize_updates,
                        message_is_definitive(merge_itor->data)));
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
                       merge_itor->ordered_iterators[index]->key,
                       merge_itor->ordered_iterators[index + 1]->key);
   if (merge_itor->ordered_iterators[index]->next_key_equal) {
      debug_assert(cmp == 0);
   } else {
      debug_assert(cmp < 0);
   }
#endif
}

static inline platform_status
advance_and_resort_min_ritor(merge_iterator *merge_itor)
{
   platform_status rc;

   debug_assert(
      !slices_equal(merge_itor->key, merge_itor->ordered_iterators[0]->key));

   merge_itor->ordered_iterators[0]->next_key_equal = FALSE;
   merge_itor->ordered_iterators[0]->key            = NULL_SLICE;
   merge_itor->ordered_iterators[0]->data           = NULL_MESSAGE;
   rc = iterator_advance(merge_itor->ordered_iterators[0]->itor);
   if (!SUCCESS(rc)) {
      return rc;
   }

   bool at_end;
   // if it's exhausted, kill it and move the ritors up the queue.
   rc = iterator_at_end(merge_itor->ordered_iterators[0]->itor, &at_end);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (UNLIKELY(at_end)) {
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

   bool prev_equal;
   bool next_equal;
   // otherwise, find its position in the array
   // Add 1 to return value since it gives offset from [1]
   int index = 1
               + bsearch_insert(*merge_itor->ordered_iterators,
                                merge_itor->ordered_iterators + 1,
                                merge_itor->num_remaining - 1,
                                merge_itor->cfg,
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
   debug_assert(message_data(merge_itor->data)
                != merge_accumulator_data(&merge_itor->merge_buffer));
   debug_assert(
      slices_equal(merge_itor->key, merge_itor->ordered_iterators[0]->key));

   data_config *cfg = merge_itor->cfg;

#if SPLINTER_DEBUG
   ordered_iterator *expected_itor = merge_itor->ordered_iterators[1];
#endif

   // there is more than one copy of the current key
   bool success = merge_accumulator_copy_message(&merge_itor->merge_buffer,
                                                 merge_itor->data);
   if (!success) {
      return STATUS_NO_MEMORY;
   }

   do {
      // Verify we don't fall off the end
      debug_assert(merge_itor->num_remaining >= 2);
      // Verify keys match
      debug_assert(!data_key_compare(
         cfg, merge_itor->key, merge_itor->ordered_iterators[1]->key));

      if (data_merge_tuples(cfg,
                            merge_itor->key,
                            merge_itor->ordered_iterators[1]->data,
                            &merge_itor->merge_buffer))
      {
         return STATUS_NO_MEMORY;
      }

      /*
       * Need to maintain invariant that merge_itor->key points to a valid
       * page; this means that this pointer must be updated before the 0th
       * iterator is advanced
       */
      merge_itor->key    = merge_itor->ordered_iterators[1]->key;
      platform_status rc = advance_and_resort_min_ritor(merge_itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
#if SPLINTER_DEBUG
      debug_assert(expected_itor == merge_itor->ordered_iterators[0]);
      expected_itor = merge_itor->ordered_iterators[1];
#endif
   } while (merge_itor->ordered_iterators[0]->next_key_equal);

   merge_itor->data = merge_accumulator_to_message(&merge_itor->merge_buffer);

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
                                           bool           *discarded)
{
   data_config *cfg   = merge_itor->cfg;
   message_type class = message_class(merge_itor->data);
   if (class != MESSAGE_TYPE_INSERT && merge_itor->finalize_updates) {
      if (message_data(merge_itor->data)
          != merge_accumulator_data(&merge_itor->merge_buffer))
      {
         bool success = merge_accumulator_copy_message(
            &merge_itor->merge_buffer, merge_itor->data);
         if (!success) {
            return STATUS_NO_MEMORY;
         }
      }
      if (data_merge_tuples_final(
             cfg, merge_itor->key, &merge_itor->merge_buffer)) {
         return STATUS_NO_MEMORY;
      }
      merge_itor->data =
         merge_accumulator_to_message(&merge_itor->merge_buffer);
      class = message_class(merge_itor->data);
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
advance_one_loop(merge_iterator *merge_itor, bool *retry)
{
   *retry = FALSE;
   // Determine whether we're at the end.
   if (merge_itor->num_remaining == 0) {
      merge_itor->at_end = TRUE;
      return STATUS_OK;
   }

   // set the next key/data from the min ritor
   merge_itor->key  = merge_itor->ordered_iterators[0]->key;
   merge_itor->data = merge_itor->ordered_iterators[0]->data;
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

   bool discarded;
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
   int               i;
   platform_status   rc = STATUS_OK, merge_iterator_rc;
   merge_iterator   *merge_itor;
   ordered_iterator *temp;

   if (!out_itor || !itor_arr || !cfg || num_trees < 0
       || num_trees >= ARRAY_SIZE(merge_itor->ordered_iterator_stored))
   {
      platform_default_log("merge_iterator_create: bad parameter merge_itor %p"
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

   merge_itor->at_end = FALSE;
   merge_itor->cfg    = cfg;

   // index -1 initializes the pad variable
   for (i = -1; i < num_trees; i++) {
      merge_itor->ordered_iterator_stored[i] = (ordered_iterator){
         .seq            = i,
         .itor           = i == -1 ? NULL : itor_arr[i],
         .key            = NULL_SLICE,
         .data           = NULL_MESSAGE,
         .next_key_equal = FALSE,
      };
      merge_itor->ordered_iterators[i] =
         &merge_itor->ordered_iterator_stored[i];
   }

   // Move all the dead iterators to the end and count how many are still alive.
   merge_itor->num_remaining = num_trees;
   i                         = 0;
   while (i < merge_itor->num_remaining) {
      bool at_end;
      rc = iterator_at_end(merge_itor->ordered_iterators[i]->itor, &at_end);
      if (!SUCCESS(rc)) {
         goto destroy;
      }
      if (at_end) {
         ordered_iterator *tmp =
            merge_itor->ordered_iterators[merge_itor->num_remaining - 1];
         merge_itor->ordered_iterators[merge_itor->num_remaining - 1] =
            merge_itor->ordered_iterators[i];
         merge_itor->ordered_iterators[i] = tmp;
         merge_itor->num_remaining--;
      } else {
         set_curr_ordered_iterator(cfg, merge_itor->ordered_iterators[i]);
         i++;
      }
   }
   platform_sort_slow(merge_itor->ordered_iterators,
                      merge_itor->num_remaining,
                      sizeof(*merge_itor->ordered_iterators),
                      merge_comp,
                      merge_itor->cfg,
                      &temp);
   // Generate initial value for next_key_equal bits
   for (i = 0; i + 1 < merge_itor->num_remaining; ++i) {
      int cmp = data_key_compare(merge_itor->cfg,
                                 merge_itor->ordered_iterators[i]->key,
                                 merge_itor->ordered_iterators[i + 1]->key);
      debug_assert(cmp <= 0);
      merge_itor->ordered_iterators[i]->next_key_equal = (cmp == 0);
   }

   bool retry;
   rc = advance_one_loop(merge_itor, &retry);
   if (!SUCCESS(rc)) {
      goto out;
   }

   if (retry) {
      rc = merge_advance((iterator *)merge_itor);
   }

   goto out;

destroy:
   merge_iterator_rc = merge_iterator_destroy(hid, &merge_itor);
   if (!SUCCESS(merge_iterator_rc)) {
      platform_error_log("merge_iterator_create: exception while releasing\n");
      if (SUCCESS(rc)) {
         platform_error_log("merge_iterator_create: clobbering rc\n");
         rc = merge_iterator_rc;
      }
   }

out:
   if (!SUCCESS(rc)) {
      platform_error_log("merge_iterator_create: exception: %s\n",
                         platform_status_to_string(rc));
   } else {
      *out_itor = merge_itor;
      if (!merge_itor->at_end) {
         debug_assert_message_type_valid(merge_itor);
      }
   }
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
 * merge_at_end --
 *
 *      Checks if more values are left in the merge itor.
 *
 * Results:
 *      Returns TRUE if the itor is at end, FALSE otherwise.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
platform_status
merge_at_end(iterator *itor, // IN
             bool     *at_end)   // OUT
{
   merge_iterator *merge_itor = (merge_iterator *)itor;
   *at_end                    = merge_itor->at_end;
   debug_assert(*at_end == slice_is_null(merge_itor->key));

   return STATUS_OK;
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
merge_get_curr(iterator *itor, slice *key, message *data)
{
   merge_iterator *merge_itor = (merge_iterator *)itor;
   debug_assert(!merge_itor->at_end);
   *key  = merge_itor->key;
   *data = merge_itor->data;
}

/*
 *-----------------------------------------------------------------------------
 * merge_advance --
 *
 *      Merges the next key from the array of input iterators.
 *
 * Results:
 *      0 if successful, error otherwise
 *-----------------------------------------------------------------------------
 */
platform_status
merge_advance(iterator *itor)
{
   platform_status rc         = STATUS_OK;
   merge_iterator *merge_itor = (merge_iterator *)itor;

   bool retry;
   do {
      merge_itor->key  = NULL_SLICE;
      merge_itor->data = NULL_MESSAGE;
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

   return STATUS_OK;
}

void
merge_iterator_print(merge_iterator *merge_itor)
{
   uint64       i;
   slice        key;
   message      data;
   data_config *data_cfg = merge_itor->cfg;
   iterator_get_curr(&merge_itor->super, &key, &data);

   platform_default_log("****************************************\n");
   platform_default_log("** merge iterator\n");
   platform_default_log("**  - trees: %u remaining: %u\n",
                        merge_itor->num_trees,
                        merge_itor->num_remaining);
   platform_default_log("** curr: %s\n", key_string(data_cfg, key));
   platform_default_log("----------------------------------------\n");
   for (i = 0; i < merge_itor->num_trees; i++) {
      bool at_end;
      iterator_at_end(merge_itor->ordered_iterators[i]->itor, &at_end);
      platform_default_log("%u: ", merge_itor->ordered_iterators[i]->seq);
      if (at_end) {
         platform_default_log("# : ");
      } else {
         platform_default_log("_ : ");
      }
      if (i < merge_itor->num_remaining) {
         iterator_get_curr(merge_itor->ordered_iterators[i]->itor, &key, &data);
         platform_default_log("%s\n", key_string(data_cfg, key));
      } else {
         platform_default_log("\n");
      }
   }
   platform_default_log("\n");
}
