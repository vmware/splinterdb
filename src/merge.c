// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

#include "merge.h"
#include "btree.h"
#include "iterator.h"
#include "util.h"

#include "poison.h"

/* Function declarations and iterator_ops */
void            merge_get_curr (iterator *itor, char **key, char **data,
                                data_type *type);
platform_status merge_at_end   (iterator *itor, bool *at_end);
platform_status merge_advance  (iterator *itor);

// FIXME: [nsarmicanic 2020-08-19]
// Make these exisiting functions static, change API to take merge_iterator*
// Add wrapper fucntions for external API that will take iterator* which will
//    cast to merge_iterator* and call the internal functions
static iterator_ops merge_ops = {
   .get_curr = merge_get_curr,
   .at_end   = merge_at_end,
   .advance  = merge_advance,
};


// Range Stack Accessors and Mutators

// Caller must zero this struct before init
static void
range_stack_init(range_stack *stack, int num_trees)
{
   // if stack is empty, index 0 will be -1
   stack->seq[0] = -1;
   stack->end_seq = -1;
   stack->num_sequences = num_trees;
}

static inline bool
range_stack_makes_point_redundant(range_stack *stack, message_type class)
{
   return stack->size > 0 && class == MESSAGE_TYPE_DELETE;
}

static inline bool
range_stack_clobbers_point(range_stack *stack, int point_seq)
{
   debug_assert(point_seq >= 0);

   /*
    * merge_iterator takes homogenous input iterators so anything on the stack
    * cannot share a sequence with a point.
    * Also if stack is empty, index 0 will be -1
    */
   debug_assert(point_seq != stack->seq[0]);

   return point_seq < stack->seq[0];
}

static inline char*
range_stack_get_start_key(range_stack *stack)
{
   return stack->start_key;
}

// end_key is the data of range delete (key, data)
static inline char*
range_stack_get_end_key(range_stack *stack)
{
   debug_assert(stack->end_seq >= 0);
   debug_assert(stack->end_seq < stack->num_sequences);
   return stack->limits[stack->end_seq];
}

static int
find_predecessor(range_stack *stack, int seq)
{
   /*
    * FIXME: [yfogel 2020-06-11] maybe do binary search?  maybe do binary search
    * iff stack size is big enough?
    * You can do a binary search for highest index where stack[i].seq >= seq
    * (note == should assert(FALSE))
    * use linear scan for first run.. we can consider binary search later
    * (including algo that decides on linear or binary depending on size)
    * It's PROBABLY never worth it to do binary search
    */

   // find highest index i s.t. stack.seq[i] > seq
   for (int i = stack->size - 1; i >= 0; --i) {
      if (stack->seq[i] > seq) {
         return i;
      }
   }
   return -1;
}


/*
 *-----------------------------------------------------------------------------
 *
 * find_useful_successor --
 *
 *    This function looks for the lowest index i such that limit will be
 *    less than all limits in stack for starting index insert_position.
 *
 *    Precondition:
 *       seq > stack->seq[i] for all insert_position <= i < stack->size
 *
 *    Return found index i, otherwise return stack size
 *
 *-----------------------------------------------------------------------------
 */
static int
find_useful_successor(range_stack *stack,
                      const char  *limit,
                      int          seq,
                      int          insert_position,
                      data_config *cfg)
{
   /*
    * While this COULD be done with binary search, it would actually be SLOWER
    * in common cases than a linear scan!!!
    * this particular function (in both callers) is amortized O(1) if you do a
    * linear scan starting at the right position, so there's no point to do the
    * more complicated binary search
    */

   /*
    * Search for the lowest index i such that
    * limits[stack.seq[i]] > limit
    *    Note: don't search indexes lower than insert_position
    */

   for (int i = insert_position; i < stack->size; ++i) {
      /*
       * (seq[i] == seq) would mean range deletes from one sequence
       *    overlap (which is illegal)
       * (seq[i] > seq) would imply the precondition was violated
       */
      debug_assert(stack->seq[i] < seq);
      // note that it's > and NOT >=
      if (fixed_size_data_key_compare(cfg, stack->limits[stack->seq[i]], limit) > 0) {
         // This index (and everything later) is useful to keep
         return i;
      }
   }

   return stack->size;
}


// FIXME: [nsarmicanic 2020-08-17] Right now there's no validating types of
// config inside of merge_iterator. Move data_type from btree_config
// to data_config so we can validate type
static inline void
debug_range_stack_check_invariants(debug_only range_stack *stack,
                                   debug_only data_config *cfg)
{
#if SPLINTER_DEBUG
    debug_assert(stack->size <= ARRAY_SIZE(stack->seq));
    debug_assert(stack->num_sequences <= ARRAY_SIZE(stack->seq));

    // has_start_key should only be set if there is something in the stack
    debug_assert(stack->has_start_key == (stack->size > 0));

    debug_assert(IMPLIES((stack->end_seq >= 0), (stack->size == 0)));
    if (stack->end_seq >= 0) {
       debug_assert(stack->end_seq < stack->num_sequences);
    } else {
       debug_assert(stack->end_seq == -1);
    }

    // Verify that sequence strictly decreases as index increases
    for (int i = 1; i < stack->size; i++) {
       debug_assert(stack->seq[i - 1] > stack->seq[i]);
    }

    if (stack->size > 0) {
       // Verify all sequences are valid
       for (int i = 0; i < stack->size; i++) {
          debug_assert(stack->seq[i] >= 0);
          debug_assert(stack->seq[i] < stack->num_sequences);
       }
       // Verify first limit is strictly greater than start_key
       int seq = stack->seq[0];
       debug_assert(fixed_size_data_key_compare(cfg, stack->start_key,
                                     stack->limits[seq]) < 0);
    } else {
       debug_assert(stack->seq[0] == -1);
    }

    // Verify that end key is strictly increasing as index increases
    for (int i = 1; i < stack->size; i++) {
       int seq_lo = stack->seq[i - 1];
       int seq_hi = stack->seq[i];
       debug_assert(fixed_size_data_key_compare(cfg, stack->limits[seq_lo],
                                     stack->limits[seq_hi]) < 0);
    }
#endif
}


static void
add_new_range_delete_to_stack(range_stack *stack,
                              const char  *startkey,
                              const char  *limit,
                              int          seq,
                              data_config *cfg)
{
   debug_range_stack_check_invariants(stack, cfg);

   if (stack->size == 0) {
      debug_assert(!stack->has_start_key);
      stack->has_start_key = TRUE;
      memmove(stack->start_key, startkey, cfg->key_size);
      stack->end_seq = -1;
      stack->size = 1;
      memmove(stack->limits[seq], limit, cfg->key_size);
      stack->seq[0] = seq;
      goto done;
   }

   /*
    * Note: find_predecessor is NOT amortized O(1) here
    *     It's actually possible to do find_useful_successor before
    *     find_predecessor but it's not obvious that it would ever help
    *     to do that.
    *     Doing so would make find_useful_successor stop being amortized O(1)
    *     and binary search on comparison functions is somewhat expensive
    */
   int predecessor = find_predecessor(stack, seq);
   debug_assert(predecessor == -1 || stack->seq[predecessor] > seq);
   if (predecessor >= 0 &&
       fixed_size_data_key_compare(cfg, stack->limits[stack->seq[predecessor]],
                        limit) >= 0) {
      // The input range delete is redunant; throw it away.
      goto done;
   }

   // We need to put the new range delete immediately after the predecessor
   int insert_position = predecessor + 1;

   // We are keeping the range delete; clone the limit key.
   memmove(stack->limits[seq], limit, cfg->key_size);

   int successor = find_useful_successor(stack, limit, seq,
                                         insert_position, cfg);

   int num_deleting = successor - insert_position;

   /*
    * limits are NOT stored in the array but instead per-sequence.
    * This avoids an expensive memmove of the keys when we really only
    * need (at most) one per sequence and thus don't care
    *
    * shift things
    *    if nothing is being deleted this is shifting everything right by 1,
    *    if 1 is being deleted it's a no-op
    *    if more than 1 is being deleted it's a left shift
    *    if we're deleting everything the memmove moves 0 bytes
    */
   int *dst = &stack->seq[insert_position + 1];
   const int *src = &stack->seq[successor];
   const int *end = &stack->seq[stack->size];
   size_t bytes_to_move = (end - src) * sizeof(*src);
   memmove(dst, src, bytes_to_move);

   stack->size -= num_deleting;
   // A new range is added to the stack, increment stack size
   stack->size++;
   stack->seq[insert_position] = seq;

done:
   debug_range_stack_check_invariants(stack, cfg);
   return;
}

/*
 *-----------------------------------------------------------------------------
 *
 * maybe_remove_range_deletes --
 *
 *  Inform range stack that we have started processing a new key. Modifies
 *  the stack and removes any range deletes no longer necessary.
 *
 *  returns:
 *    TRUE if a range delete was generated to be outputted by the merge_iterator
 *    FALSE otherwise
 *
 *-----------------------------------------------------------------------------
 */
static bool
maybe_remove_range_deletes(range_stack *stack,
                           char *next_key,
                           data_config *cfg)
{
   bool r;
   debug_range_stack_check_invariants(stack, cfg);
   if (stack->size == 0) {
      r = FALSE;
      goto done;
   }

   // Find the first relevant/useful range_delete
   int num_deleting =
      find_useful_successor(stack, next_key,
                            //passing a number bigger than anything on the stack
                            stack->seq[0] + 1,
                            0, cfg);
   debug_assert(num_deleting >= 0);
   debug_assert(num_deleting <= stack->size);

   if (num_deleting == stack->size) {
      // we are deleting EVERYTHING
      debug_assert(stack->has_start_key);
      stack->end_seq = stack->seq[stack->size - 1];
      stack->has_start_key = FALSE;
      stack->size = 0;
      // if stack is empty, index 0 should be -1
      stack->seq[0] = -1;
      r = TRUE;
      goto done;
   }

   /*
    * we're shifting the ones we're keeping to left,
    * and we don't care about clearing out the remainder
    */
   int *dest = &stack->seq[0];
   const int *src = &stack->seq[num_deleting];
   const int *end = &stack->seq[stack->size];
   size_t bytes_to_move = (end - src) * sizeof(*src);
   memmove(dest, src, bytes_to_move);

   stack->size -= num_deleting;
   r = FALSE;

done:
   debug_range_stack_check_invariants(stack, cfg);
   return r;
}

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
             const data_config *cfg,
             bool *keys_equal)
{
   int cmp = fixed_size_data_key_compare(cfg,
                              itor_one->key,
                              itor_two->key);
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
merge_comp(const void *one,
           const void *two,
           void *ctxt)
{
   const ordered_iterator *itor_one = *(ordered_iterator **)one;
   const ordered_iterator *itor_two = *(ordered_iterator **)two;
   data_config *cfg = (data_config *)ctxt;
   bool ignore_keys_equal;
   return bsearch_comp(itor_one, itor_two, cfg, &ignore_keys_equal);
}

// Returns index (from base0) where key belongs
static inline int
bsearch_insert(register const ordered_iterator *key,
               ordered_iterator **base0,
               const size_t nmemb,
               const data_config *cfg,
               bool *prev_equal_out,
               bool *next_equal_out)
{
   register ordered_iterator **base = base0;
   register int lim, cmp;
   register ordered_iterator **p;
   bool prev_equal = FALSE;
   bool next_equal = FALSE;


   for (lim = nmemb; lim != 0; lim >>= 1) {
      p = base + (lim >> 1);
      bool keys_equal;
      cmp = bsearch_comp(key, *p, cfg, &keys_equal);
      debug_assert(cmp != 0);

      if (cmp > 0) {	/* key > p: move right */
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
set_curr_ordered_iterator(ordered_iterator *itor)
{
   iterator_get_curr(itor->itor, &itor->key, &itor->data, &itor->type);
}

static inline void
assert_data_config_valid(data_config *point_cfg,
                         data_config *range_cfg)
{
   // FIXME: [nsarmicanic 2020-07-31] Should we add type to data_config?
   //debug_assert(point_cfg->type = data_type_point);
   //debug_assert(range_cfg->type = data_type_range);

   debug_assert(point_cfg->key_size <= point_cfg->message_size);
   debug_assert(range_cfg->key_size == range_cfg->message_size);
   debug_assert(point_cfg->key_size == range_cfg->key_size);
   debug_assert(point_cfg->key_size <= MAX_KEY_SIZE);
}

static inline void
debug_assert_message_type_valid(debug_only merge_iterator *merge_itor)
{
#if SPLINTER_DEBUG
   if (!merge_itor->at_end) {
      debug_assert(merge_itor->type == data_type_point);
   } else {
      debug_assert(merge_itor->type == data_type_invalid);
   }
   debug_code(char *data = merge_itor->data);
   debug_code(data_config *cfg = merge_itor->cfg);
   message_type type =
      data == NULL ? MESSAGE_TYPE_INVALID : fixed_size_data_message_class(cfg, data);
   debug_assert(!merge_itor->has_data ||
                !merge_itor->discard_deletes ||
                data == NULL ||
                type != MESSAGE_TYPE_DELETE);
   debug_assert(!merge_itor->has_data ||
                !merge_itor->resolve_updates ||
                data == NULL ||
                type != MESSAGE_TYPE_UPDATE);
#endif
}

static void
debug_verify_sorted(debug_only merge_iterator *merge_itor,
              debug_only const int index)
{
#if SPLINTER_DEBUG
   if (index < 0 || index + 1 >= merge_itor->num_remaining) {
      return;
   }
   const int cmp =
      fixed_size_data_key_compare(merge_itor->cfg,
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

   debug_assert(merge_itor->key != merge_itor->ordered_iterators[0]->key);
   if (merge_itor->has_data) {
      debug_assert(merge_itor->data != merge_itor->ordered_iterators[0]->data);
   }

   merge_itor->ordered_iterators[0]->next_key_equal = FALSE;
   merge_itor->ordered_iterators[0]->key = NULL;
   merge_itor->ordered_iterators[0]->data = NULL;
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
   set_curr_ordered_iterator(merge_itor->ordered_iterators[0]);
   if (merge_itor->num_remaining == 1) {
      goto out;
   }

   bool prev_equal;
   bool next_equal;
   // otherwise, find its position in the array
   // Add 1 to return value since it gives offset from [1]
   int index = 1 + bsearch_insert(*merge_itor->ordered_iterators,
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
      //for (int i = 0; i < index; ++i) {
      //   merge_itor->ordered_iterators[i] =
      //      merge_itor->ordered_iterators[i + 1];
      //}
      merge_itor->ordered_iterators[index] = old_min_itor;
   }
   // This may access index -1 but that's fine.  See
   // ordered_iterator_stored_pad and ordered_iterators_pad
   merge_itor->ordered_iterators[index - 1]->next_key_equal = prev_equal;
   merge_itor->ordered_iterators[index]->next_key_equal = next_equal;

   debug_verify_sorted(merge_itor, index - 1);
   debug_verify_sorted(merge_itor, index);

out:
   return STATUS_OK;
}

static void
process_one_range_delete_or_clobber_one_point(merge_iterator *merge_itor)
{
   ordered_iterator *ordered_itor = merge_itor->ordered_iterators[0];
   data_config *range_cfg = merge_itor->range_cfg;

   switch (ordered_itor->type) {
      case data_type_point:
         // FIXME: [nsarmicanic 2020-07-31] remove if guard when this function
         //        is not NULL
         if (range_cfg->clobber_message_with_range_delete) {
            fixed_size_data_clobber_message_with_range_delete(range_cfg, ordered_itor->key,
                                                   ordered_itor->data);
         }
         break;
      case data_type_range:
         add_new_range_delete_to_stack(&merge_itor->stack,
                                       ordered_itor->key,
                                       ordered_itor->data,
                                       ordered_itor->seq,
                                       range_cfg);
         break;
      default:
         debug_assert(FALSE);
         break;
   }
}

static platform_status
process_remaining_range_deletes_and_clobber_points(merge_iterator *merge_itor)
{
#if SPLINTER_DEBUG
   ordered_iterator *expected_itor = merge_itor->ordered_iterators[1];
#endif
   debug_assert(merge_itor->ordered_iterators[0]->next_key_equal);
   do {
      /*
       * Need to maintain invariant that merge_itor->key points to a valid
       * page; this means that this pointer must be updated before the 0th
       * iterator is advanced
       */
      merge_itor->key = merge_itor->ordered_iterators[1]->key;
      debug_assert(merge_itor->data != merge_itor->ordered_iterators[0]->data);
      platform_status rc = advance_and_resort_min_ritor(merge_itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
#if SPLINTER_DEBUG
      debug_assert(expected_itor == merge_itor->ordered_iterators[0]);
      expected_itor = merge_itor->ordered_iterators[1];
#endif
      process_one_range_delete_or_clobber_one_point(merge_itor);
   } while (merge_itor->ordered_iterators[0]->next_key_equal);

   return STATUS_OK;
}

/*
 * In the case where the two minimum iterators of the merge iterator have equal
 * keys, resolve_equal_keys will merge the data as necessary
 */

static platform_status
merge_resolve_equal_keys(merge_iterator *merge_itor, bool *retry) {
   debug_assert(merge_itor->ordered_iterators[0]->next_key_equal);
   debug_assert(merge_itor->data != merge_itor->merge_buffer);
   debug_assert(merge_itor->key == merge_itor->ordered_iterators[0]->key);

   data_config *cfg = merge_itor->cfg;

#if SPLINTER_DEBUG
   ordered_iterator *expected_itor = merge_itor->ordered_iterators[1];
#endif

   if (1
       && merge_itor->ordered_iterators[0]->type == data_type_point
       && !range_stack_clobbers_point(&merge_itor->stack,
                                      merge_itor->ordered_iterators[0]->seq))
   {
      /*
       * There is more than one copy of the current key, and the newest copy is
       * a point that is not clobbered by range deletes.
       */

      memmove(merge_itor->merge_buffer, merge_itor->data, cfg->message_size);
      merge_itor->data = merge_itor->merge_buffer;
      do {
         if (1
             && merge_itor->ordered_iterators[1]->type == data_type_point
             && !range_stack_clobbers_point(
                                 &merge_itor->stack,
                                 merge_itor->ordered_iterators[1]->seq))
         {
            /*
             * There is at least one more point that is not clobbered by
             * range deletes.
             */

            // Verify we don't fall off the end
            debug_assert(merge_itor->num_remaining >= 2);
            // Verify keys match
            debug_assert(
                  !fixed_size_data_key_compare(cfg,
                     merge_itor->key,
                     merge_itor->ordered_iterators[1]->key));
            debug_assert(merge_itor->data == merge_itor->merge_buffer);

            fixed_size_data_merge_tuples(cfg, merge_itor->key,
                  merge_itor->ordered_iterators[1]->data,
                  merge_itor->data);
            // FIXME: [yfogel 2020-01-11] handle class==MESSAGE_TYPE_INVALID
            //    We should crash or cancel the entire compaction

            /*
             * Need to maintain invariant that merge_itor->key points to a valid
             * page; this means that this pointer must be updated before the 0th
             * iterator is advanced
             */
            merge_itor->key = merge_itor->ordered_iterators[1]->key;
            debug_assert(merge_itor->data == merge_itor->merge_buffer);
            platform_status rc = advance_and_resort_min_ritor(merge_itor);
            if (!SUCCESS(rc)) {
               return rc;
            }
#if SPLINTER_DEBUG
            debug_assert(expected_itor == merge_itor->ordered_iterators[0]);
            expected_itor = merge_itor->ordered_iterators[1];
#endif

         } else {
            /* Anything that's left over is either range deletes or points that
             * are being clobbered.
             * This deals with all duplicates so we break out of the loop once
             * we are done.
             */
            debug_assert(merge_itor->data == merge_itor->merge_buffer);
            process_remaining_range_deletes_and_clobber_points(merge_itor);
            debug_assert(merge_itor->stack.size > 0);
            break;
         }
      } while (merge_itor->ordered_iterators[0]->next_key_equal);

      // Dealt with all duplicates, now pointing to last copy.
      debug_assert(!merge_itor->ordered_iterators[0]->next_key_equal);

      message_type class = fixed_size_data_message_class(cfg, merge_itor->data);
      /*
       * If retry is TRUE that means we threw away a point delete
       * because we didn't need it because of range delete
       *
       * Point deletes on top of range deletes are redundant.
       */
      *retry = range_stack_makes_point_redundant(&merge_itor->stack, class);
   } else {
      merge_itor->data = NULL;
      process_one_range_delete_or_clobber_one_point(merge_itor);
      process_remaining_range_deletes_and_clobber_points(merge_itor);
      *retry = TRUE;
   }

   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 *
 * discard_range_deletes--
 *
 * if merge_itor->discard_deletes:
 *    discards MESSAGE_TYPE_DELETE messages
 * return True if discarded a range delete
 *
 *-----------------------------------------------------------------------------
 */
static inline bool
discard_range_deletes(merge_iterator *merge_itor)
{
   debug_assert(merge_itor->type == data_type_range);
   debug_assert(merge_itor->discard_deletes == !!merge_itor->discard_deletes);

   // Increment stats iff discarded_deletes is true
   merge_itor->discarded_deletes += merge_itor->discard_deletes;
   return merge_itor->discard_deletes;
}


/*
 *-----------------------------------------------------------------------------
 *
 * if merge_itor->resolve_deletes:
 *    resolves MESSAGE_TYPE_UPDATE messages into
 *       MESSAGE_TYPE_INSERT or MESSAGE_TYPE_DELETE messages
 * if merge_itor->discard_deletes:
 *    discards MESSAGE_TYPE_DELETE messages
 * return True if it discarded a MESSAGE_TYPE_DELETE message
 *
 *-----------------------------------------------------------------------------
 */
static inline bool
merge_resolve_updates_and_discard_deletes(merge_iterator *merge_itor)
{
   debug_assert(merge_itor->type == data_type_point);
   data_config *cfg = merge_itor->cfg;
   message_type class = fixed_size_data_message_class(cfg, merge_itor->data);
   // FIXME: [yfogel 2020-01-11] handle class==MESSAGE_TYPE_INVALID
   //    We should crash or cancel the entire compaction
   if (class != MESSAGE_TYPE_INSERT && merge_itor->resolve_updates) {
      if (merge_itor->data != merge_itor->merge_buffer) {
         // We might already be in merge_buffer if we did some merging.
         memmove(merge_itor->merge_buffer, merge_itor->data, cfg->message_size);
         merge_itor->data = merge_itor->merge_buffer;
      }
      fixed_size_data_merge_tuples_final(cfg, merge_itor->key,
                              merge_itor->data);
      class = fixed_size_data_message_class(cfg, merge_itor->data);
      // FIXME: [yfogel 2020-01-11] handle class==MESSAGE_TYPE_INVALID
      //    We should crash or cancel the entire compaction
   }
   if (class == MESSAGE_TYPE_DELETE && merge_itor->discard_deletes) {
      merge_itor->discarded_deletes++;
      return TRUE;
   }
   return FALSE;
}

static platform_status
advance_one_loop(merge_iterator *merge_itor, bool *retry)
{
   *retry= FALSE;
   // Determine whether we're at the end.
   if (merge_itor->num_remaining == 0) {
      if (maybe_remove_range_deletes(&merge_itor->stack,
                                     merge_itor->range_cfg->max_key,
                                     merge_itor->range_cfg))
      {
         // We're at the end (modulo range in the stack)
         merge_itor->type = data_type_range;
         merge_itor->key = range_stack_get_start_key(&merge_itor->stack);
         merge_itor->data = range_stack_get_end_key(&merge_itor->stack);
         *retry = discard_range_deletes(merge_itor);
         debug_assert(merge_itor->stack.size == 0);
      } else {
         merge_itor->type = data_type_invalid;
         merge_itor->at_end = TRUE;
      }
      return STATUS_OK;
   }

   // set the next key/data/type from the min ritor
   merge_itor->key = merge_itor->ordered_iterators[0]->key;
   data_type type = merge_itor->ordered_iterators[0]->type;
   debug_assert(type == data_type_point || type == data_type_range);
   if (!merge_itor->has_data) {
      /*
       * We only have keys.  We COULD still merge (skip duplicates) the keys
       * but it would break callers.  Right now, rough estimates rely on the
       * duplicate keys outputted to improve the estimates.
       */
      merge_itor->type = type;
      return STATUS_OK;
   }
   merge_itor->data = merge_itor->ordered_iterators[0]->data;

   if (maybe_remove_range_deletes(&merge_itor->stack, merge_itor->key,
                                  merge_itor->range_cfg))
   {
      merge_itor->type = data_type_range;
      merge_itor->key = range_stack_get_start_key(&merge_itor->stack);
      merge_itor->data = range_stack_get_end_key(&merge_itor->stack);
      *retry = discard_range_deletes(merge_itor);
      return STATUS_OK;
   }

   if (merge_itor->ordered_iterators[0]->next_key_equal) {
      platform_status rc = merge_resolve_equal_keys(merge_itor, retry);
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (*retry) {
         return STATUS_OK;
      }
   } else if (merge_itor->ordered_iterators[0]->type == data_type_range ||
         range_stack_clobbers_point(&merge_itor->stack,
                                    merge_itor->ordered_iterators[0]->seq))
   {
      /*
       * We have no duplicates and have either
       * - a point that gets clobbered or
       * - one range delete.
       */
      process_one_range_delete_or_clobber_one_point(merge_itor);
      *retry = TRUE;
      return STATUS_OK;
   }

   merge_itor->type = type;
   debug_assert(merge_itor->type == data_type_point);

   if (merge_resolve_updates_and_discard_deletes(merge_itor)) {
      *retry = TRUE;
      return STATUS_OK;
   }
   debug_assert_message_type_valid(merge_itor);
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 *
 * merge_iterator_create --
 *
 *      Initialize a merge iterator for a forest of B-trees.
 *
 *      Prerequisite:
 *         All input iterators must be homogeneous for data_type
 *
 * Results:
 *      0 if successful, error otherwise
 *
 *-----------------------------------------------------------------------------
 */

// FIXME: [yfogel 2020-07-17] rename cfg to point_cfg
// FIXME: [yfogel 2020-07-17] ADD data_type type; to data_config (dont' remove
//       from btree_config (yet? maybe later)
// FIXME: [yfogel 2020-08-24] Verify all input iterators are homogeneous for data_type
platform_status
merge_iterator_create(platform_heap_id  hid,
                      data_config      *cfg,
                      data_config      *range_cfg,
                      int               num_trees,
                      iterator        **itor_arr,
                      bool              discard_deletes,
                      bool              resolve_updates,
                      bool              has_data,
                      merge_iterator  **out_itor)
{
   int i;
   platform_status rc = STATUS_OK, merge_iterator_rc;
   merge_iterator *merge_itor;
   ordered_iterator *temp;
   // FIXME: [yfogel 2020-07-17] Add and call sanity check functions
   //        (also sanity functions WHEN FIRST NEEDED for splinterconfig and
   //         btree_config)

   // FIXME: [nsarmicanic 2020-07-31] remove if guard once range_cfg is never passed as NULL
   if (range_cfg) {
      assert_data_config_valid(cfg, range_cfg);
   }

   // FIXME: [yfogel 2020-07-01] need to also err on missing range_cfg
   //      but callers are currently passing NULL until callers
   //      are fixed
   if (0
       || !out_itor
       || !itor_arr
       || !cfg
       || num_trees < 0
       || num_trees >= ARRAY_SIZE(merge_itor->ordered_iterator_stored))
   {
      platform_log("merge_iterator_create: bad parameter merge_itor %p"
            " num_trees %d itor_arr %p cfg %p range_cfg %p\n",
            out_itor, num_trees, itor_arr, cfg, range_cfg);
      return STATUS_BAD_PARAM;
   }

   _Static_assert(ARRAY_SIZE(merge_itor->ordered_iterator_stored) ==
                  ARRAY_SIZE(merge_itor->ordered_iterators),
                  "size mismatch");

   merge_itor = TYPED_ZALLOC(hid, merge_itor);
   if (merge_itor == NULL) {
      return STATUS_NO_MEMORY;
   }

   merge_itor->super.ops = &merge_ops;
   merge_itor->num_trees = num_trees;
   // clamp bool to 0-1
   merge_itor->discard_deletes = !!discard_deletes;
   merge_itor->resolve_updates = !!resolve_updates;
   merge_itor->has_data = has_data;
   merge_itor->at_end = FALSE;
   // FIXME: [yfogel 2020-07-17] (YONI) optimization to figure out comparison
   //                            function and call it DIRECTLY
   //                            (won't work for sort, but works
   //                            everywhere else)
   //                            maybe also copy data config locally?
   merge_itor->cfg = cfg;
   merge_itor->range_cfg = range_cfg;

   // Initialize range stack
   range_stack_init(&merge_itor->stack, num_trees);

   // index -1 initializes the pad variable
   for (i = -1; i < num_trees; i++) {
     merge_itor->ordered_iterator_stored[i] = (ordered_iterator) {
        .seq = i,
        .itor = i == -1 ? NULL : itor_arr[i],
        .key = NULL,
        .data = NULL,
        .next_key_equal = FALSE,
     };
     merge_itor->ordered_iterators[i] =
        &merge_itor->ordered_iterator_stored[i];
   }

   // Move all the dead iterators to the end and count how many are still alive.
   merge_itor->num_remaining = num_trees;
   i = 0;
   while (i < merge_itor->num_remaining) {
      bool at_end;
      rc = iterator_at_end(merge_itor->ordered_iterators[i]->itor, &at_end);
      if (!SUCCESS(rc)) {
         goto destroy;
      }
      if (at_end) {
         ordered_iterator *tmp
            = merge_itor->ordered_iterators[merge_itor->num_remaining - 1];
         merge_itor->ordered_iterators[merge_itor->num_remaining - 1]
            = merge_itor->ordered_iterators[i];
         merge_itor->ordered_iterators[i] = tmp;
         merge_itor->num_remaining--;
      } else {
         set_curr_ordered_iterator(merge_itor->ordered_iterators[i]);
         i++;
      }
   }
   platform_sort_slow(merge_itor->ordered_iterators, merge_itor->num_remaining,
                      sizeof(*merge_itor->ordered_iterators), merge_comp,
                      merge_itor->cfg, &temp);
   // Generate initial value for next_key_equal bits
   for (i = 0; i + 1 < merge_itor->num_remaining; ++i) {
      int cmp = fixed_size_data_key_compare(merge_itor->cfg,
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
      rc = merge_advance((iterator*) merge_itor);
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
      debug_assert_message_type_valid(merge_itor);
   }
   return rc;
}


/*
 *-----------------------------------------------------------------------------
 *
 * merge_iterator_destroy --
 *
 *      Destroys a merge iterator.
 *
 *-----------------------------------------------------------------------------
 */

platform_status
merge_iterator_destroy(platform_heap_id hid, merge_iterator **merge_itor)
{
   platform_free(hid, *merge_itor);
   *merge_itor = NULL;

   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 *
 * merge_at_end --
 *
 *      Checks if more values are left in the merge itor.
 *
 * Results:
 *      Returns TRUE if the itor is at end, FALSE otherwise.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

platform_status
merge_at_end(iterator *itor,   // IN
             bool     *at_end) // OUT
{
   merge_iterator *merge_itor = (merge_iterator *)itor;
   *at_end = merge_itor->at_end;
   debug_assert(*at_end == (merge_itor->key == NULL));

   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 *
 * merge_get_curr --
 *
 *      Returns current key and data from the merge itor.
 *
 * Results:
 *      Current key and data.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

// FIXME: [tjiaheng 2020-07-17] should support data_type_range
void
merge_get_curr(iterator   *itor,
               char      **key,
               char      **data,
               data_type  *type)
{
   merge_iterator *merge_itor = (merge_iterator *)itor;
   debug_assert(!merge_itor->at_end);
   debug_assert(merge_itor->type != data_type_invalid);
   *key = merge_itor->key;
   *data = merge_itor->data;
   *type = merge_itor->type;
}

/*
 *-----------------------------------------------------------------------------
 *
 * merge_advance --
 *
 *      Merges the next key from the array of input iterators.
 *
 * Results:
 *      0 if successful, error otherwise
 *
 *-----------------------------------------------------------------------------
 */
// FIXME: [nsarmicanic 2020-08-06] merge_advance should take a merge_iterator*
platform_status
merge_advance(iterator *itor)
{
   platform_status rc = STATUS_OK;
   merge_iterator *merge_itor = (merge_iterator *)itor;

   debug_assert(!merge_itor->has_data || merge_itor->data);
   bool retry;
   do {
      merge_itor->key = NULL;
      merge_itor->data = NULL;
      if (merge_itor->type != data_type_range) {
         // Advance one iterator
         rc = advance_and_resort_min_ritor(merge_itor);
         if (!SUCCESS(rc)) {
            return rc;
         }
      }
      merge_itor->type = data_type_invalid;

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
   uint64 i;
   char *key, *data, key_str[MAX_KEY_SIZE];
   data_type type;
   data_config *data_cfg = merge_itor->cfg;
   iterator_get_curr(&merge_itor->super, &key, &data, &type);
   fixed_size_data_key_to_string(data_cfg, key, key_str, 32);

   platform_log("****************************************\n");
   platform_log("** merge iterator\n");
   platform_log("**  - trees: %u remaining: %u\n", merge_itor->num_trees, merge_itor->num_remaining);
   platform_log("** curr: %s\n", key_str);
   platform_log("----------------------------------------\n");
   for (i = 0; i < merge_itor->num_trees; i++) {
      bool at_end;
      iterator_at_end(merge_itor->ordered_iterators[i]->itor, &at_end);
      platform_log("%u: ", merge_itor->ordered_iterators[i]->seq);
      if (at_end)
         platform_log("# : ");
      else
         platform_log("_ : ");
      if (i < merge_itor->num_remaining) {
         iterator_get_curr(merge_itor->ordered_iterators[i]->itor, &key, &data, &type);
         fixed_size_data_key_to_string(data_cfg, key, key_str, 32);
         platform_log("%s\n", key_str);
      } else {
         platform_log("\n");
      }
   }
   platform_log("\n");

}
