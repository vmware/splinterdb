/* **********************************************************
 * Copyright 2018-2020 VMware, Inc.  All rights reserved. -- VMware Confidential
 * **********************************************************/

#include "variable_length_btree_private.h"
#include "poison.h"

/******************************************************************
 * Structure of a node:
 *                                 hdr->next_entry
 *                                               |
 *   0                                           v     page_size
 *   -----------------------------------------------------------
 *   | header | offsets table ---> | empty space | <--- entries|
 *   -----------------------------------------------------------
 *
 * The arrows indicate that the offsets table grows to the left
 * and the entries grow to the right.
 *
 * Entries are not physically sorted in a node.  The offsets table
 * gives the offset of each entry, in key order.
 *
 * Offsets are from byte 0 of the node.
 *
 * New entries are placed in the empty space.
 *
 * When an entry is replaced with a physically smaller entry, the
 * replacement is performed in place.  When an entry is replaced with
 * a physically larger entry, then the new entry is stored in the
 * empty space.

 * A node may have free space fragmentation after some entries have
 * been replaced.  Defragmenting the node rebuilds it with no
 * free-space fragmentation.  When a node runs out of free space, we
 * measure its dead space.  If it below a threshold, we split the
 * node.  If it is above the threshhold, then we defragment the node
 * instead of splitting it.
 *
 *******************************************************************/

/* Threshold for splitting instead of defragmenting. */
#define VARIABLE_LENGTH_BTREE_SPLIT_THRESHOLD(page_size) ((page_size) / 2)

/* After a split, the free space in the left node may be fragmented.
   If there's less than this much contiguous free space, then we also
   defrag the left node. */
#define VARIABLE_LENGTH_BTREE_DEFRAGMENT_THRESHOLD(page_size) ((page_size) / 4)

char  positive_infinity_buffer;
slice positive_infinity = {0, &positive_infinity_buffer};

/*
 * Branches keep track of the number of keys and the total size of
 * all keys and messages in their subtrees.  But memtables do not
 * (because it is difficult to maintain this information during
 * insertion).  However, the current implementation uses the same
 * data structure for both memtables and branches.  So memtables
 * store VARIABLE_LENGTH_BTREE_UNKNOWN for these counters.
 */
#define VARIABLE_LENGTH_BTREE_UNKNOWN (0x7fffffffUL)


static inline uint8
variable_length_btree_height(const variable_length_btree_hdr *hdr)
{
   return hdr->height;
}

static inline table_entry
variable_length_btree_get_table_entry(variable_length_btree_hdr *hdr, int i)
{
   debug_assert(i < hdr->num_entries);
   return hdr->offsets[i];
}

static inline table_index
variable_length_btree_num_entries(const variable_length_btree_hdr *hdr)
{
   return hdr->num_entries;
}

static inline void
variable_length_btree_increment_height(variable_length_btree_hdr *hdr)
{
   hdr->height++;
}

static inline void
variable_length_btree_reset_node_entries(
   const variable_length_btree_config *cfg,
   variable_length_btree_hdr *         hdr)
{
   hdr->num_entries = 0;
   hdr->next_entry  = variable_length_btree_page_size(cfg);
}


static inline uint64
index_entry_size(const slice key)
{
   return sizeof(index_entry) + slice_length(key);
}

static inline uint64
leaf_entry_size(const slice key, const slice message)
{
   return sizeof(leaf_entry) + slice_length(key) + slice_length(message);
}

static inline uint64
leaf_entry_key_size(const leaf_entry *entry)
{
   return entry->key_size;
}


static inline uint64
leaf_entry_message_size(const leaf_entry *entry)
{
   return entry->message_size;
}


/**************************************
 * Basic get/set on index nodes
 **************************************/

static inline void
variable_length_btree_fill_index_entry(const variable_length_btree_config *cfg,
                                       variable_length_btree_hdr *         hdr,
                                       index_entry *entry,
                                       slice        new_pivot_key,
                                       uint64       new_addr,
                                       uint32       kv_pairs,
                                       uint32       key_bytes,
                                       uint32       message_bytes)
{
   debug_assert((void *)hdr <= (void *)entry);
   debug_assert(diff_ptr(hdr, entry) + index_entry_size(new_pivot_key) <=
                cfg->page_size);
   memcpy(entry->key, slice_data(new_pivot_key), slice_length(new_pivot_key));
   entry->key_size                         = slice_length(new_pivot_key);
   entry->pivot_data.child_addr            = new_addr;
   entry->pivot_data.num_kvs_in_tree       = kv_pairs;
   entry->pivot_data.key_bytes_in_tree     = key_bytes;
   entry->pivot_data.message_bytes_in_tree = message_bytes;
}

bool
variable_length_btree_set_index_entry(const variable_length_btree_config *cfg,
                                      variable_length_btree_hdr *         hdr,
                                      table_index                         k,
                                      slice  new_pivot_key,
                                      uint64 new_addr,
                                      int64  kv_pairs,
                                      int64  key_bytes,
                                      int64  message_bytes)
{
   platform_assert(k <= hdr->num_entries);
   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k + 1;

   if (k < hdr->num_entries) {
      index_entry *old_entry =
         variable_length_btree_get_index_entry(cfg, hdr, k);
      if (hdr->next_entry == diff_ptr(hdr, old_entry)
          && (diff_ptr(hdr, &hdr->offsets[new_num_entries])
                 + index_entry_size(new_pivot_key)
              <= hdr->next_entry + sizeof_index_entry(old_entry)))
      {
         /* special case to avoid creating fragmentation:
          * the old entry is the physically first entry in the node
          * and the new entry will fit in the space avaiable from the old
          * entry plus the free space preceding the old_entry.
          * In this case, just reset next_entry so we can insert the new entry.
          */
         hdr->next_entry += sizeof_index_entry(old_entry);
         /* Fall through */
      } else if (index_entry_size(new_pivot_key)
                 <= sizeof_index_entry(old_entry)) {
         /* old_entry is not the physically first in the node,
          * but new entry will fit inside it.
          */
         variable_length_btree_fill_index_entry(cfg,
                                                hdr,
                                                old_entry,
                                                new_pivot_key,
                                                new_addr,
                                                kv_pairs,
                                                key_bytes,
                                                message_bytes);
         return TRUE;
      }
      /* Fall through */
   }

   if (hdr->next_entry < diff_ptr(hdr, &hdr->offsets[new_num_entries]) +
                            index_entry_size(new_pivot_key)) {
      return FALSE;
   }

   index_entry *new_entry = pointer_byte_offset(
      hdr, hdr->next_entry - index_entry_size(new_pivot_key));
   variable_length_btree_fill_index_entry(cfg,
                                          hdr,
                                          new_entry,
                                          new_pivot_key,
                                          new_addr,
                                          kv_pairs,
                                          key_bytes,
                                          message_bytes);

   hdr->offsets[k]  = diff_ptr(hdr, new_entry);
   hdr->num_entries = new_num_entries;
   hdr->next_entry  = diff_ptr(hdr, new_entry);
   return TRUE;
}

static inline bool
variable_length_btree_insert_index_entry(
   const variable_length_btree_config *cfg,
   variable_length_btree_hdr *         hdr,
   uint32                              k,
   slice                               new_pivot_key,
   uint64                              new_addr,
   int64                               kv_pairs,
   int64                               key_bytes,
   int64                               message_bytes)
{
   bool succeeded = variable_length_btree_set_index_entry(cfg,
                                                          hdr,
                                                          hdr->num_entries,
                                                          new_pivot_key,
                                                          new_addr,
                                                          kv_pairs,
                                                          key_bytes,
                                                          message_bytes);
   if (succeeded) {
      node_offset this_entry_offset = hdr->offsets[hdr->num_entries - 1];
      memmove(&hdr->offsets[k + 1],
              &hdr->offsets[k],
              (hdr->num_entries - k - 1) * sizeof(hdr->offsets[0]));
      hdr->offsets[k] = this_entry_offset;
   }
   return succeeded;
}


/**************************************
 * Basic get/set on leaf nodes
 **************************************/

static inline void
variable_length_btree_fill_leaf_entry(const variable_length_btree_config *cfg,
                                      variable_length_btree_hdr *         hdr,
                                      leaf_entry *                        entry,
                                      slice                               key,
                                      slice message)
{
   debug_assert(pointer_byte_offset(entry, leaf_entry_size(key, message)) <=
                pointer_byte_offset(hdr, cfg->page_size));
   memcpy(entry->key_and_message, slice_data(key), slice_length(key));
   memcpy(entry->key_and_message + slice_length(key),
          slice_data(message),
          slice_length(message));
   entry->key_size     = slice_length(key);
   entry->message_size = slice_length(message);
}

static inline bool
variable_length_btree_can_set_leaf_entry(
   const variable_length_btree_config *cfg,
   const variable_length_btree_hdr *   hdr,
   table_index                         k,
   slice                               new_key,
   slice                               new_message)
{
   if (hdr->num_entries < k)
      return FALSE;

   if (k < hdr->num_entries) {
      leaf_entry *old_entry = variable_length_btree_get_leaf_entry(cfg, hdr, k);
      if (leaf_entry_size(new_key, new_message) <=
          sizeof_leaf_entry(old_entry)) {
         return TRUE;
      }
      /* Fall through */
   }

   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k + 1;
   if (hdr->next_entry < diff_ptr(hdr, &hdr->offsets[new_num_entries]) +
                            leaf_entry_size(new_key, new_message)) {
      return FALSE;
   }

   return TRUE;
}

bool
variable_length_btree_set_leaf_entry(const variable_length_btree_config *cfg,
                                     variable_length_btree_hdr *         hdr,
                                     table_index                         k,
                                     slice new_key,
                                     slice new_message)
{
   if (k < hdr->num_entries) {
      leaf_entry *old_entry = variable_length_btree_get_leaf_entry(cfg, hdr, k);
      if (leaf_entry_size(new_key, new_message) <=
          sizeof_leaf_entry(old_entry)) {
         variable_length_btree_fill_leaf_entry(
            cfg, hdr, old_entry, new_key, new_message);
         return TRUE;
      }
      /* Fall through */
   }

   platform_assert(k <= hdr->num_entries);
   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k + 1;
   if (hdr->next_entry < diff_ptr(hdr, &hdr->offsets[new_num_entries]) +
                            leaf_entry_size(new_key, new_message)) {
      return FALSE;
   }

   leaf_entry *new_entry = pointer_byte_offset(
      hdr, hdr->next_entry - leaf_entry_size(new_key, new_message));
   platform_assert((void *)&hdr->offsets[new_num_entries] <= (void *)new_entry);
   variable_length_btree_fill_leaf_entry(
      cfg, hdr, new_entry, new_key, new_message);

   hdr->offsets[k]  = diff_ptr(hdr, new_entry);
   hdr->num_entries = new_num_entries;
   hdr->next_entry  = diff_ptr(hdr, new_entry);
   platform_assert(0 < hdr->num_entries);

   return TRUE;
}

static inline bool
variable_length_btree_insert_leaf_entry(const variable_length_btree_config *cfg,
                                        variable_length_btree_hdr *         hdr,
                                        table_index                         k,
                                        slice new_key,
                                        slice new_message)
{
   debug_assert(k <= hdr->num_entries);
   bool succeeded = variable_length_btree_set_leaf_entry(
      cfg, hdr, hdr->num_entries, new_key, new_message);
   if (succeeded) {
      node_offset this_entry_offset = hdr->offsets[hdr->num_entries - 1];
      debug_assert(k + 1 <= hdr->num_entries);
      memmove(&hdr->offsets[k + 1],
              &hdr->offsets[k],
              (hdr->num_entries - k - 1) * sizeof(hdr->offsets[0]));
      hdr->offsets[k] = this_entry_offset;
   }
   return succeeded;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_find_pivot --
 *
 *      Returns idx such that
 *          - -1 <= idx < num_entries
 *          - forall i | 0 <= i <= idx         :: key_i <= key
 *          - forall i | idx < i < num_entries :: key   <  key_i
 *      Also
 *          - *found == 0 || *found == 1
 *          - *found == 1 <==> (0 <= idx && key_idx == key)
 *-----------------------------------------------------------------------------
 */
/*
 * The C code below is a translation of the following verified dafny
implementation.

method bsearch(s: seq<int>, k: int) returns (idx: int, f: bool)
  requires forall i, j | 0 <= i < j < |s| :: s[i] < s[j]
  ensures -1 <= idx < |s|
  ensures forall i | 0 <= i <= idx :: s[i] <= k
  ensures forall i | idx < i < |s| :: k < s[i]
  ensures f <==> (0 <= idx && s[idx] == k)
{
  var lo := 0;
  var hi := |s|;

  f := false;

  while lo < hi
    invariant 0 <= lo <= hi <= |s|
    invariant forall i | 0 <= i < lo :: s[i] <= k
    invariant forall i | hi <= i < |s| :: k < s[i]
    invariant f <==> (0 < lo && s[lo-1] == k)
  {
    var mid := (lo + hi) / 2;
    if s[mid] <= k {
      lo := mid + 1;
      f := s[mid] == k;
    } else {
      hi := mid;
    }
  }

  idx := lo - 1;
}

*/
int64
variable_length_btree_find_pivot(const variable_length_btree_config *cfg,
                                 const variable_length_btree_hdr *   hdr,
                                 slice                               key,
                                 bool *                              found)
{
   int64 lo = 0, hi = variable_length_btree_num_entries(hdr);

   if (slice_is_null(key)) {
      return -1;
   }

   *found = 0;

   while (lo < hi) {
      int64 mid = (lo + hi) / 2;
      int   cmp = variable_length_btree_key_compare(
         cfg, variable_length_btree_get_pivot(cfg, hdr, mid), key);
      if (cmp == 0) {
         *found = 1;
         return mid;
      } else if (cmp < 0) {
         lo     = mid + 1;
      } else {
         hi = mid;
      }
   }

   return lo - 1;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_find_tuple --
 *
 *      Returns idx such that
 *          - -1 <= idx < num_entries
 *          - forall i | 0 <= i <= idx         :: key_i <= key
 *          - forall i | idx < i < num_entries :: key   <  key_i
 *      Also
 *          - *found == 0 || *found == 1
 *          - *found == 1 <==> (0 <= idx && key_idx == key)
 *-----------------------------------------------------------------------------
 */
/*
 * The C code below is a translation of the same dafny implementation as above.
 */
static inline int64
variable_length_btree_find_tuple(const variable_length_btree_config *cfg,
                                 const variable_length_btree_hdr *   hdr,
                                 slice                               key,
                                 bool *                              found)
{
   int64 lo = 0, hi = variable_length_btree_num_entries(hdr);

   *found = 0;

   while (lo < hi) {
      int64 mid = (lo + hi) / 2;
      int   cmp = variable_length_btree_key_compare(
         cfg, variable_length_btree_get_tuple_key(cfg, hdr, mid), key);
      if (cmp == 0) {
         *found = 1;
         return mid;
      } else if (cmp < 0) {
         lo     = mid + 1;
      } else {
         hi = mid;
      }
   }

   return lo - 1;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_leaf_incorporate_tuple
 *
 *   Adds the given key and value to node (must be a leaf).
 *
 * This is broken into several pieces to avoid repeated work during
 * exceptional cases.
 *
 * - create_incorporate_spec() computes everything needed to update
 *   the leaf, i.e. the index of the key, whether it is replacing an
 *   existing entry, and the merged message if it is.
 *
 * - can_perform_incorporate_spec says whether the leaf has enough
 *   room to actually perform the incorporation.
 *
 * - perform_incorporate_spec() does what it says.
 *
 * - incorporate_tuple() is a convenience wrapper.
 *-----------------------------------------------------------------------------
 */
static inline int
variable_length_btree_merge_tuples(const variable_length_btree_config *cfg,
                                   slice                               key,
                                   slice                               old_data,
                                   writable_buffer *                   new_data)
{
   return data_merge_tuples(cfg->data_cfg, key, old_data, new_data);
}

static slice
spec_message(const leaf_incorporate_spec *spec)
{
   if (spec->was_found) {
      return writable_buffer_to_slice(&spec->msg.merged_message);
   } else {
      return spec->msg.new_message;
   }
}

platform_status
variable_length_btree_create_leaf_incorporate_spec(
   const variable_length_btree_config *cfg,
   platform_heap_id                    heap_id,
   variable_length_btree_hdr *         hdr,
   slice                               key,
   slice                               message,
   leaf_incorporate_spec *             spec)
{
   spec->key = key;
   spec->idx =
      variable_length_btree_find_tuple(cfg, hdr, key, &spec->was_found);
   if (!spec->was_found) {
      spec->msg.new_message = message;
      spec->idx++;
      return STATUS_OK;
   } else {
      leaf_entry *entry =
         variable_length_btree_get_leaf_entry(cfg, hdr, spec->idx);
      slice           oldmessage = leaf_entry_message_slice(entry);
      platform_status rc;
      rc = writable_buffer_init_from_slice(
         &spec->msg.merged_message, heap_id, message);
      if (!SUCCESS(rc)) {
         return STATUS_NO_MEMORY;
      }
      if (variable_length_btree_merge_tuples(
             cfg, key, oldmessage, &spec->msg.merged_message))
      {
         writable_buffer_reinit(&spec->msg.merged_message);
         return STATUS_NO_MEMORY;
      } else {
         return STATUS_OK;
      }
   }
}

static void
destroy_leaf_incorporate_spec(leaf_incorporate_spec *spec)
{
   if (spec->was_found) {
      writable_buffer_reinit(&spec->msg.merged_message);
   }
}

static inline bool
variable_length_btree_can_perform_leaf_incorporate_spec(
   const variable_length_btree_config *cfg,
   variable_length_btree_hdr *         hdr,
   const leaf_incorporate_spec *       spec)
{
   if (!spec->was_found) {
      return variable_length_btree_can_set_leaf_entry(
         cfg,
         hdr,
         variable_length_btree_num_entries(hdr),
         spec->key,
         spec->msg.new_message);
   } else {
      slice merged = writable_buffer_to_slice(&spec->msg.merged_message);
      return variable_length_btree_can_set_leaf_entry(
         cfg, hdr, spec->idx, spec->key, merged);
   }
}

bool
variable_length_btree_try_perform_leaf_incorporate_spec(
   const variable_length_btree_config *cfg,
   variable_length_btree_hdr *         hdr,
   const leaf_incorporate_spec *       spec,
   uint64 *                            generation)
{
   bool success;
   if (!spec->was_found) {
      success = variable_length_btree_insert_leaf_entry(
         cfg, hdr, spec->idx, spec->key, spec->msg.new_message);
   } else {
      slice merged = writable_buffer_to_slice(&spec->msg.merged_message);
      success      = variable_length_btree_set_leaf_entry(
         cfg, hdr, spec->idx, spec->key, merged);
   }
   if (success) {
      *generation = hdr->generation++;
   }
   return success;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_defragment_leaf --
 *
 *      Defragment a node
 *-----------------------------------------------------------------------------
 */
void
variable_length_btree_defragment_leaf(
   const variable_length_btree_config *cfg, // IN
   variable_length_btree_scratch *     scratch,
   variable_length_btree_hdr *         hdr,
   int64                               omit_idx) // IN
{
   variable_length_btree_hdr *scratch_hdr =
      (variable_length_btree_hdr *)scratch->defragment_node.scratch_node;
   memcpy(scratch_hdr, hdr, variable_length_btree_page_size(cfg));
   variable_length_btree_reset_node_entries(cfg, hdr);
   uint64 dst_idx = 0;
   for (int64 i = 0; i < variable_length_btree_num_entries(scratch_hdr); i++) {
      if (i != omit_idx) {
         leaf_entry *entry =
            variable_length_btree_get_leaf_entry(cfg, scratch_hdr, i);
         debug_only bool success = variable_length_btree_set_leaf_entry(
            cfg,
            hdr,
            dst_idx++,
            leaf_entry_key_slice(entry),
            leaf_entry_message_slice(entry));
         debug_assert(success);
      }
   }
}

static inline void
variable_length_btree_truncate_leaf(
   const variable_length_btree_config *cfg, // IN
   variable_length_btree_hdr *         hdr, // IN
   uint64                              target_entries)                   // IN
{
   uint64 new_next_entry = variable_length_btree_page_size(cfg);

   for (uint64 i = 0; i < target_entries; i++) {
      if (hdr->offsets[i] < new_next_entry)
         new_next_entry = hdr->offsets[i];
   }

   hdr->num_entries = target_entries;
   hdr->next_entry  = new_next_entry;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_split_leaf --
 *
 *      Splits the node at left_addr into a new node at right_addr.
 *
 *      Assumes write lock on both nodes.
 *-----------------------------------------------------------------------------
 */

static leaf_splitting_plan initial_plan = {0, FALSE};


static bool
most_of_entry_is_on_left_side(uint64 total_bytes,
                              uint64 left_bytes,
                              uint64 entry_size)
{
   return left_bytes + sizeof(table_entry) + entry_size <
          (total_bytes + sizeof(table_entry) + entry_size) / 2;
}

/* Figure out how many entries we can put on the left side.
 * Basically, we split the node as evenly as possible by bytes.
 * The old node had total_bytes of entries (and table entries).
 * The new nodes will have as close as possible to total_bytes / 2 bytes.
 * We iterate over each entry and, if most of its bytes fall on
 * left side of total_bytes / 2, then we can put it on the left side.
 *
 * Note that the loop is split into two (see build_leaf_splitting_plan)
 * so we can handle the entry for the key being inserted specially.
 * Specifically, if the key being inserted replaces an existing key,
 * then we need to skip over the entry for the existing key.
 */
static uint64
plan_move_more_entries_to_left(const variable_length_btree_config *cfg,
                               const variable_length_btree_hdr *   hdr,
                               uint64                              max_entries,
                               uint64                              total_bytes,
                               uint64                              left_bytes,
                               leaf_splitting_plan *plan) // IN/OUT
{
   leaf_entry *entry;
   while (plan->split_idx < max_entries &&
          (entry = variable_length_btree_get_leaf_entry(
              cfg, hdr, plan->split_idx)) &&
          most_of_entry_is_on_left_side(
             total_bytes, left_bytes, sizeof_leaf_entry(entry))) {
      left_bytes += sizeof(table_entry) + sizeof_leaf_entry(entry);
      plan->split_idx++;
   }
   return left_bytes;
}

/*
 * Choose a splitting point so that we are guaranteed to be able to
 * insert the given key-message pair into the correct node after the
 * split. Assumes all leaf entries are at most half the total free
 * space in an empty leaf.
 */
leaf_splitting_plan
variable_length_btree_build_leaf_splitting_plan(
   const variable_length_btree_config *cfg, // IN
   const variable_length_btree_hdr *   hdr,
   const leaf_incorporate_spec *       spec) // IN
{
   /* Split the content by bytes -- roughly half the bytes go to the
      right node.  So count the bytes, including the new entry to be
      inserted. */
   uint64 num_entries = variable_length_btree_num_entries(hdr);
   uint64 entry_size  = leaf_entry_size(spec->key, spec_message(spec));
   uint64 total_bytes = entry_size;

   for (uint64 i = 0; i < num_entries; i++) {
      if (i != spec->idx || !spec->was_found) {
         leaf_entry *entry = variable_length_btree_get_leaf_entry(cfg, hdr, i);
         total_bytes += sizeof_leaf_entry(entry);
      }
   }
   total_bytes += (num_entries + !spec->was_found) * sizeof(table_entry);

   /* Now figure out the number of entries to move, and figure out how
      much free space will be created in the left_hdr by the split. */
   uint64              left_bytes = 0;
   leaf_splitting_plan plan       = initial_plan;

   /* Figure out how many of the items to the left of spec.idx can be
      put into the left node. */
   left_bytes = plan_move_more_entries_to_left(
      cfg, hdr, spec->idx, total_bytes, left_bytes, &plan);

   /* Figure out whether our new entry can go into the left node.  If it
      can't, then no subsequent entries can, either, so we're done. */
   if (plan.split_idx == spec->idx
       && most_of_entry_is_on_left_side(total_bytes, left_bytes, entry_size))
   {
      left_bytes += sizeof(table_entry) + entry_size;
      plan.insertion_goes_left = TRUE;
   } else {
      return plan;
   }
   if (spec->was_found) {
      /* If our new entry is replacing an existing entry, then skip
         that entry in our planning. */
      plan.split_idx++;
   }

   /* Figure out how many more entries after spec.idx can go into the
      left node. */
   plan_move_more_entries_to_left(
      cfg, hdr, num_entries, total_bytes, left_bytes, &plan);

   return plan;
}

static inline slice
variable_length_btree_splitting_pivot(
   const variable_length_btree_config *cfg, // IN
   const variable_length_btree_hdr *   hdr,
   const leaf_incorporate_spec *       spec,
   leaf_splitting_plan                 plan)
{
   if (plan.split_idx == spec->idx && !spec->was_found
       && !plan.insertion_goes_left)
   {
      return spec->key;
   } else {
      return variable_length_btree_get_tuple_key(cfg, hdr, plan.split_idx);
   }
}

static inline void
variable_length_btree_split_leaf_build_right_node(
   const variable_length_btree_config *cfg,      // IN
   const variable_length_btree_hdr *   left_hdr, // IN
   const leaf_incorporate_spec *       spec,     // IN
   leaf_splitting_plan                 plan,     // IN
   variable_length_btree_hdr *         right_hdr)         // IN/OUT
{
   /* Build the right node. */
   memmove(right_hdr, left_hdr, sizeof(*right_hdr));
   right_hdr->generation++;
   variable_length_btree_reset_node_entries(cfg, right_hdr);
   uint64 num_left_entries = variable_length_btree_num_entries(left_hdr);
   uint64 dst_idx          = 0;
   for (uint64 i = plan.split_idx; i < num_left_entries; i++) {
      if (i != spec->idx || !spec->was_found) {
         leaf_entry *entry =
            variable_length_btree_get_leaf_entry(cfg, left_hdr, i);
         variable_length_btree_set_leaf_entry(cfg,
                                              right_hdr,
                                              dst_idx,
                                              leaf_entry_key_slice(entry),
                                              leaf_entry_message_slice(entry));
         dst_idx++;
      }
   }
}

static inline void
variable_length_btree_split_leaf_cleanup_left_node(
   const variable_length_btree_config *cfg, // IN
   variable_length_btree_scratch *     scratch,
   variable_length_btree_hdr *         left_hdr, // IN
   const leaf_incorporate_spec *       spec,     // IN
   leaf_splitting_plan                 plan,
   uint64                              right_addr) // IN
{
   left_hdr->next_addr = right_addr;
   variable_length_btree_truncate_leaf(cfg, left_hdr, plan.split_idx);
   left_hdr->generation++;
   if (plan.insertion_goes_left
       && !variable_length_btree_can_perform_leaf_incorporate_spec(
          cfg, left_hdr, spec))
   {
      variable_length_btree_defragment_leaf(
         cfg, scratch, left_hdr, spec->was_found ? spec->idx : -1);
   }
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_split_index --
 *
 *      Splits the node at left_addr into a new node at right_addr.
 *
 *      Assumes write lock on both nodes.
 *-----------------------------------------------------------------------------
 */
static inline bool
variable_length_btree_index_is_full(
   const variable_length_btree_config *cfg, // IN
   const variable_length_btree_hdr *   hdr)    // IN
{
   return hdr->next_entry < diff_ptr(hdr, &hdr->offsets[hdr->num_entries + 2]) +
                               sizeof(index_entry) + MAX_INLINE_KEY_SIZE;
}

static inline uint64
variable_length_btree_choose_index_split(
   const variable_length_btree_config *cfg, // IN
   const variable_length_btree_hdr *   hdr)    // IN
{
   /* Split the content by bytes -- roughly half the bytes go to the
      right node.  So count the bytes. */
   uint64 total_entry_bytes = 0;
   for (uint64 i = 0; i < variable_length_btree_num_entries(hdr); i++) {
      index_entry *entry = variable_length_btree_get_index_entry(cfg, hdr, i);
      total_entry_bytes += sizeof_index_entry(entry);
   }

   /* Now figure out the number of entries to move, and figure out how
      much free space will be created in the left_hdr by the split. */
   uint64 target_left_entries  = 0;
   uint64 new_left_entry_bytes = 0;
   while (new_left_entry_bytes < total_entry_bytes / 2) {
      index_entry *entry =
         variable_length_btree_get_index_entry(cfg, hdr, target_left_entries);
      new_left_entry_bytes += sizeof_index_entry(entry);
      target_left_entries++;
   }
   return target_left_entries;
}

static inline void
variable_length_btree_split_index_build_right_node(
   const variable_length_btree_config *cfg,                 // IN
   const variable_length_btree_hdr *   left_hdr,            // IN
   uint64                              target_left_entries, // IN
   variable_length_btree_hdr *         right_hdr)                    // IN/OUT
{
   uint64 target_right_entries =
      variable_length_btree_num_entries(left_hdr) - target_left_entries;

   /* Build the right node. */
   memmove(right_hdr, left_hdr, sizeof(*right_hdr));
   right_hdr->generation++;
   variable_length_btree_reset_node_entries(cfg, right_hdr);
   for (uint64 i = 0; i < target_right_entries; i++) {
      index_entry *entry = variable_length_btree_get_index_entry(
         cfg, left_hdr, target_left_entries + i);
      bool succeeded = variable_length_btree_set_index_entry(
         cfg,
         right_hdr,
         i,
         index_entry_key_slice(entry),
         index_entry_child_addr(entry),
         entry->pivot_data.num_kvs_in_tree,
         entry->pivot_data.key_bytes_in_tree,
         entry->pivot_data.message_bytes_in_tree);
      platform_assert(succeeded);
   }
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_defragment_index --
 *
 *      Defragment a node
 *-----------------------------------------------------------------------------
 */
void
variable_length_btree_defragment_index(
   const variable_length_btree_config *cfg, // IN
   variable_length_btree_scratch *     scratch,
   variable_length_btree_hdr *         hdr) // IN
{
   variable_length_btree_hdr *scratch_hdr =
      (variable_length_btree_hdr *)scratch->defragment_node.scratch_node;
   memcpy(scratch_hdr, hdr, variable_length_btree_page_size(cfg));
   variable_length_btree_reset_node_entries(cfg, hdr);
   for (uint64 i = 0; i < variable_length_btree_num_entries(scratch_hdr); i++) {
      index_entry *entry =
         variable_length_btree_get_index_entry(cfg, scratch_hdr, i);
      bool succeeded = variable_length_btree_set_index_entry(
         cfg,
         hdr,
         i,
         index_entry_key_slice(entry),
         index_entry_child_addr(entry),
         entry->pivot_data.num_kvs_in_tree,
         entry->pivot_data.key_bytes_in_tree,
         entry->pivot_data.message_bytes_in_tree);
      platform_assert(succeeded);
   }
}

static inline void
variable_length_btree_truncate_index(
   const variable_length_btree_config *cfg, // IN
   variable_length_btree_scratch *     scratch,
   variable_length_btree_hdr *         hdr, // IN
   uint64                              target_entries)                   // IN
{
   uint64 new_next_entry = variable_length_btree_page_size(cfg);
   for (uint64 i = 0; i < target_entries; i++) {
      if (hdr->offsets[i] < new_next_entry) {
         new_next_entry = hdr->offsets[i];
      }
   }

   hdr->num_entries = target_entries;
   hdr->next_entry  = new_next_entry;
   hdr->generation++;

   if (new_next_entry < VARIABLE_LENGTH_BTREE_DEFRAGMENT_THRESHOLD(
                           variable_length_btree_page_size(cfg))) {
      variable_length_btree_defragment_index(cfg, scratch, hdr);
   }
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_alloc --
 *
 *      Allocates a node from the preallocator. Will refill it if there are no
 *      more nodes available for the given height.
 *-----------------------------------------------------------------------------
 */
bool
variable_length_btree_alloc(cache *                     cc,
                            mini_allocator *            mini,
                            uint64                      height,
                            slice                       key,
                            uint64 *                    next_extent,
                            page_type                   type,
                            variable_length_btree_node *node)
{
   node->addr = mini_alloc(mini, height, key, next_extent);
   debug_assert(node->addr != 0);
   node->page = cache_alloc(cc, node->addr, type);
   // If this btree is for a memetable
   // then pin all pages belong to it
   if (type == PAGE_TYPE_MEMTABLE) {
      cache_pin(cc, node->page);
   }
   node->hdr  = (variable_length_btree_hdr *)(node->page->data);
   return TRUE;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_node_[get,release] --
 *
 *      Gets the node with appropriate lock or releases the lock.
 *-----------------------------------------------------------------------------
 */
static inline void
variable_length_btree_node_get(cache *                             cc,
                               const variable_length_btree_config *cfg,
                               variable_length_btree_node *        node,
                               page_type                           type)
{
   debug_assert(node->addr != 0);

   node->page = cache_get(cc, node->addr, TRUE, type);
   node->hdr  = (variable_length_btree_hdr *)(node->page->data);
}

static inline bool
variable_length_btree_node_claim(cache *                             cc,  // IN
                                 const variable_length_btree_config *cfg, // IN
                                 variable_length_btree_node *        node)        // IN
{
   return cache_claim(cc, node->page);
}

static inline void
variable_length_btree_node_lock(cache *                             cc,  // IN
                                const variable_length_btree_config *cfg, // IN
                                variable_length_btree_node *        node)        // IN
{
   cache_lock(cc, node->page);
   cache_mark_dirty(cc, node->page);
}

static inline void
variable_length_btree_node_unlock(cache *                             cc,  // IN
                                  const variable_length_btree_config *cfg, // IN
                                  variable_length_btree_node *node)        // IN
{
   cache_unlock(cc, node->page);
}

static inline void
variable_length_btree_node_unclaim(
   cache *                             cc,  // IN
   const variable_length_btree_config *cfg, // IN
   variable_length_btree_node *        node)        // IN
{
   cache_unclaim(cc, node->page);
}

void
variable_length_btree_node_unget(cache *                             cc,  // IN
                                 const variable_length_btree_config *cfg, // IN
                                 variable_length_btree_node *        node)        // IN
{
   cache_unget(cc, node->page);
   node->page = NULL;
   node->hdr  = NULL;
}

static inline void
variable_length_btree_node_full_unlock(
   cache *                             cc,  // IN
   const variable_length_btree_config *cfg, // IN
   variable_length_btree_node *        node)        // IN
{
   variable_length_btree_node_unlock(cc, cfg, node);
   variable_length_btree_node_unclaim(cc, cfg, node);
   variable_length_btree_node_unget(cc, cfg, node);
}

static inline void
variable_length_btree_node_get_from_cache_ctxt(
   const variable_length_btree_config *cfg,  // IN
   cache_async_ctxt *                  ctxt, // IN
   variable_length_btree_node *        node)         // OUT
{
   node->addr = ctxt->page->disk_addr;
   node->page = ctxt->page;
   node->hdr  = (variable_length_btree_hdr *)node->page->data;
}


static inline bool
variable_length_btree_addrs_share_extent(
   const variable_length_btree_config *cfg,
   uint64                              left_addr,
   uint64                              right_addr)
{
   return (right_addr / cfg->extent_size) == (left_addr / cfg->extent_size);
}

static inline uint64
variable_length_btree_get_extent_base_addr(
   const variable_length_btree_config *cfg,
   variable_length_btree_node *        node)
{
   return (node->addr / cfg->extent_size) * cfg->extent_size;
}

static inline uint64
variable_length_btree_root_to_meta_addr(const variable_length_btree_config *cfg,
                                        uint64 root_addr,
                                        uint64 meta_page_no)
{
   return root_addr + (meta_page_no + 1) * cfg->page_size;
}


/*----------------------------------------------------------
 * Creating and destroying B-trees.
 *----------------------------------------------------------
 */


uint64
variable_length_btree_create(cache *                             cc,
                             const variable_length_btree_config *cfg,
                             mini_allocator *                    mini,
                             page_type                           type)
{
   // get a free node for the root
   // we don't use the next_addr arr for this, since the root doesn't
   // maintain constant height
   allocator *     al = cache_allocator(cc);
   uint64          base_addr;
   platform_status rc = allocator_alloc(al, &base_addr, type);
   platform_assert_status_ok(rc);
   page_handle *root_page = cache_alloc(cc, base_addr, type);
   bool         pinned    = (type == PAGE_TYPE_MEMTABLE);

   // set up the root
   variable_length_btree_node root;
   root.page = root_page;
   root.addr = base_addr;
   root.hdr  = (variable_length_btree_hdr *)root_page->data;

   variable_length_btree_init_hdr(cfg, root.hdr);

   cache_mark_dirty(cc, root.page);

   // If this btree is for a memetable
   // then pin all pages belong to it
   if (pinned) {
      cache_pin(cc, root.page);
   }

   // release root
   cache_unlock(cc, root_page);
   cache_unclaim(cc, root_page);
   cache_unget(cc, root_page);

   // set up the mini allocator
   mini_init(mini,
             cc,
             cfg->data_cfg,
             root.addr + cfg->page_size,
             0,
             VARIABLE_LENGTH_BTREE_MAX_HEIGHT,
             type,
             type == PAGE_TYPE_BRANCH);

   return root.addr;
}

void
variable_length_btree_inc_ref_range(cache *                             cc,
                                    const variable_length_btree_config *cfg,
                                    uint64      root_addr,
                                    const slice start_key,
                                    const slice end_key)
{
   uint64 meta_page_addr =
      variable_length_btree_root_to_meta_addr(cfg, root_addr, 0);
   if (!slice_is_null(start_key) && !slice_is_null(end_key)) {
      debug_assert(variable_length_btree_key_compare(cfg, start_key, end_key) <
                   0);
   }
   mini_keyed_inc_ref(
      cc, cfg->data_cfg, PAGE_TYPE_BRANCH, meta_page_addr, start_key, end_key);
}

bool
variable_length_btree_dec_ref_range(cache *                             cc,
                                    const variable_length_btree_config *cfg,
                                    uint64      root_addr,
                                    const slice start_key,
                                    const slice end_key,
                                    page_type   type)
{
   debug_assert(type == PAGE_TYPE_BRANCH);

   if (!slice_is_null(start_key) && !slice_is_null(end_key)) {
      platform_assert(
         variable_length_btree_key_compare(cfg, start_key, end_key) < 0);
   }

   uint64 meta_page_addr =
      variable_length_btree_root_to_meta_addr(cfg, root_addr, 0);
   return mini_keyed_dec_ref(
      cc, cfg->data_cfg, PAGE_TYPE_BRANCH, meta_page_addr, start_key, end_key);
}

bool
variable_length_btree_dec_ref(cache *                             cc,
                              const variable_length_btree_config *cfg,
                              uint64                              root_addr,
                              page_type                           type)
{
   platform_assert(type == PAGE_TYPE_MEMTABLE);
   uint64 meta_head =
      variable_length_btree_root_to_meta_addr(cfg, root_addr, 0);
   uint8 ref = mini_unkeyed_dec_ref(cc, meta_head, type, TRUE);
   return ref == 0;
}

void
variable_length_btree_block_dec_ref(cache *                       cc,
                                    variable_length_btree_config *cfg,
                                    uint64                        root_addr)
{
   uint64 meta_head =
      variable_length_btree_root_to_meta_addr(cfg, root_addr, 0);
   mini_block_dec_ref(cc, meta_head);
}

void
variable_length_btree_unblock_dec_ref(cache *                       cc,
                                      variable_length_btree_config *cfg,
                                      uint64                        root_addr)
{
   uint64 meta_head =
      variable_length_btree_root_to_meta_addr(cfg, root_addr, 0);
   mini_unblock_dec_ref(cc, meta_head);
}

/**********************************************************************
 * The process of splitting a child leaf is divided into four steps in
 * order to minimize the amount of time that we hold write-locks on
 * the parent and child:
 *
 * 0. Start with claims on parent and child.
 *
 * 1. Allocate a node for the right child.  Hold a write lock on the
 *    new node.
 *
 * 2. variable_length_btree_add_pivot.  Insert a new pivot in the parent for
 *    the new child.  This step requires a write-lock on the parent.
 *    The parent can be completely unlocked as soon as this step is
 *    complete.
 *
 * 3. variable_length_btree_split_{leaf,index}_build_right_node
 *    Fill in the contents of the right child.  No lock on parent
 *    required.
 *
 * 4. variable_length_btree_truncate_{leaf,index}
 *    Truncate (and optionally defragment) the old child.  This is the
 *    only step that requires a write-lock on the old child.
 *
 * Note: if we wanted to maintain rank information in the parent when
 * splitting one of its children, we could do that by holding the lock
 * on the parent a bit longer.  But we don't need that in the
 * memtable, so not bothering for now.
 */

/* Requires:
   - claim on parent
   - claim on child
   Upon completion:
   - all nodes unlocked
   - the insertion is complete
*/
static inline int
variable_length_btree_split_child_leaf(cache *                             cc,
                                       const variable_length_btree_config *cfg,
                                       mini_allocator *                    mini,
                                       variable_length_btree_scratch *scratch,
                                       variable_length_btree_node *   parent,
                                       uint64 index_of_child_in_parent,
                                       variable_length_btree_node *child,
                                       leaf_incorporate_spec *     spec,
                                       uint64 *generation) // OUT
{
   variable_length_btree_node right_child;

   /* p: claim, c: claim, rc: - */

   leaf_splitting_plan plan =
      variable_length_btree_build_leaf_splitting_plan(cfg, child->hdr, spec);

   /* p: claim, c: claim, rc: - */

   variable_length_btree_alloc(cc,
                               mini,
                               variable_length_btree_height(child->hdr),
                               NULL_SLICE,
                               NULL,
                               PAGE_TYPE_MEMTABLE,
                               &right_child);

   /* p: claim, c: claim, rc: write */

   variable_length_btree_node_lock(cc, cfg, parent);
   {
      /* limit the scope of pivot_key, since subsequent mutations of the nodes
       * may invalidate the memory it points to. */
      slice pivot_key =
         variable_length_btree_splitting_pivot(cfg, child->hdr, spec, plan);
      bool success = variable_length_btree_insert_index_entry(
         cfg,
         parent->hdr,
         index_of_child_in_parent + 1,
         pivot_key,
         right_child.addr,
         VARIABLE_LENGTH_BTREE_UNKNOWN,
         VARIABLE_LENGTH_BTREE_UNKNOWN,
         VARIABLE_LENGTH_BTREE_UNKNOWN);
      platform_assert(success);
   }
   variable_length_btree_node_full_unlock(cc, cfg, parent);

   /* p: fully unlocked, c: claim, rc: write */

   variable_length_btree_split_leaf_build_right_node(
      cfg, child->hdr, spec, plan, right_child.hdr);

   /* p: fully unlocked, c: claim, rc: write */

   if (!plan.insertion_goes_left) {
      spec->idx -= plan.split_idx;
      bool incorporated =
         variable_length_btree_try_perform_leaf_incorporate_spec(
            cfg, right_child.hdr, spec, generation);
      platform_assert(incorporated);
   }
   variable_length_btree_node_full_unlock(cc, cfg, &right_child);

   /* p: fully unlocked, c: claim, rc: fully unlocked */

   variable_length_btree_node_lock(cc, cfg, child);
   variable_length_btree_split_leaf_cleanup_left_node(
      cfg, scratch, child->hdr, spec, plan, right_child.addr);
   if (plan.insertion_goes_left) {
      bool incorporated =
         variable_length_btree_try_perform_leaf_incorporate_spec(
            cfg, child->hdr, spec, generation);
      platform_assert(incorporated);
   }
   variable_length_btree_node_full_unlock(cc, cfg, child);

   /* p: fully unlocked, c: fully unlocked, rc: fully unlocked */

   return 0;
}

/* Requires:
   - claim on parent
   - claim on child
   Upon completion:
   - all nodes fully unlocked
   - insertion is complete
*/
static inline int
variable_length_btree_defragment_or_split_child_leaf(
   cache *                             cc,
   const variable_length_btree_config *cfg,
   mini_allocator *                    mini,
   variable_length_btree_scratch *     scratch,
   variable_length_btree_node *        parent,
   uint64                              index_of_child_in_parent,
   variable_length_btree_node *        child,
   leaf_incorporate_spec *             spec,
   uint64 *                            generation) // OUT
{
   uint64 nentries   = variable_length_btree_num_entries(child->hdr);
   uint64 live_bytes = 0;
   for (uint64 i = 0; i < nentries; i++) {
      if (!spec->was_found || i != spec->idx) {
         leaf_entry *entry =
            variable_length_btree_get_leaf_entry(cfg, child->hdr, i);
         live_bytes += sizeof_leaf_entry(entry);
      }
   }
   uint64 total_space_required =
      live_bytes + leaf_entry_size(spec->key, spec_message(spec))
      + (nentries + spec->was_found ? 0 : 1) * sizeof(index_entry);

   if (total_space_required <
       VARIABLE_LENGTH_BTREE_SPLIT_THRESHOLD(cfg->page_size)) {
      variable_length_btree_node_unclaim(cc, cfg, parent);
      variable_length_btree_node_unget(cc, cfg, parent);
      variable_length_btree_node_lock(cc, cfg, child);
      variable_length_btree_defragment_leaf(
         cfg, scratch, child->hdr, spec->was_found ? spec->idx : -1);
      bool incorporated =
         variable_length_btree_try_perform_leaf_incorporate_spec(
            cfg, child->hdr, spec, generation);
      platform_assert(incorporated);
      variable_length_btree_node_full_unlock(cc, cfg, child);
   } else {
      variable_length_btree_split_child_leaf(cc,
                                             cfg,
                                             mini,
                                             scratch,
                                             parent,
                                             index_of_child_in_parent,
                                             child,
                                             spec,
                                             generation);
   }

   return 0;
}

/*
 * Splitting a child index follows a similar pattern as splitting a child leaf.
 * The main difference is that we assume we start with write-locks on the parent
 *  and child (which fits better with the flow of the overall insert algorithm).
 */

/* Requires:
   - lock on parent
   - lock on child
   Upon completion:
   - lock on new_child
   - all other nodes unlocked
*/
static inline int
variable_length_btree_split_child_index(
   cache *                             cc,
   const variable_length_btree_config *cfg,
   mini_allocator *                    mini,
   variable_length_btree_scratch *     scratch,
   variable_length_btree_node *        parent,
   uint64                              index_of_child_in_parent,
   variable_length_btree_node *        child,
   const slice                         key_to_be_inserted,
   variable_length_btree_node *        new_child, // OUT
   int64 *                             next_child_idx)                         // IN/OUT
{
   variable_length_btree_node right_child;

   /* p: lock, c: lock, rc: - */

   uint64 idx = variable_length_btree_choose_index_split(cfg, child->hdr);

   /* p: lock, c: lock, rc: - */

   variable_length_btree_alloc(cc,
                               mini,
                               variable_length_btree_height(child->hdr),
                               NULL_SLICE,
                               NULL,
                               PAGE_TYPE_MEMTABLE,
                               &right_child);

   /* p: lock, c: lock, rc: lock */

   {
      /* limit the scope of pivot_key, since subsequent mutations of the nodes
       * may invalidate the memory it points to. */
      slice pivot_key = variable_length_btree_get_pivot(cfg, child->hdr, idx);
      variable_length_btree_insert_index_entry(cfg,
                                               parent->hdr,
                                               index_of_child_in_parent + 1,
                                               pivot_key,
                                               right_child.addr,
                                               VARIABLE_LENGTH_BTREE_UNKNOWN,
                                               VARIABLE_LENGTH_BTREE_UNKNOWN,
                                               VARIABLE_LENGTH_BTREE_UNKNOWN);
   }
   variable_length_btree_node_full_unlock(cc, cfg, parent);

   /* p: -, c: lock, rc: lock */

   if (*next_child_idx < idx) {
      *new_child = *child;
   } else {
      *new_child = right_child;
      *next_child_idx -= idx;
   }

   variable_length_btree_split_index_build_right_node(
      cfg, child->hdr, idx, right_child.hdr);

   /* p: -, c: lock, rc: lock */

   if (new_child->addr != right_child.addr) {
      variable_length_btree_node_full_unlock(cc, cfg, &right_child);
   }

   /* p: -, c: lock, rc: if nc == rc then lock else fully unlocked */

   variable_length_btree_truncate_index(cfg, scratch, child->hdr, idx);

   /* p: -, c: lock, rc: if nc == rc then lock else fully unlocked */

   if (new_child->addr != child->addr) {
      variable_length_btree_node_full_unlock(cc, cfg, child);
   }

   /* p:  -,
      c:  if nc == c  then locked else fully unlocked
      rc: if nc == rc then locked else fully unlocked */

   return 0;
}

/* Requires:
   - lock on parent
   - lock on child
   Upon completion:
   - lock on new_child
   - all other nodes unlocked
*/
static inline int
variable_length_btree_defragment_or_split_child_index(
   cache *                             cc,
   const variable_length_btree_config *cfg,
   mini_allocator *                    mini,
   variable_length_btree_scratch *     scratch,
   variable_length_btree_node *        parent,
   uint64                              index_of_child_in_parent,
   variable_length_btree_node *        child,
   const slice                         key_to_be_inserted,
   variable_length_btree_node *        new_child, // OUT
   int64 *                             next_child_idx)                         // IN/OUT
{
   uint64 nentries   = variable_length_btree_num_entries(child->hdr);
   uint64 live_bytes = 0;
   for (uint64 i = 0; i < nentries; i++) {
      index_entry *entry =
         variable_length_btree_get_index_entry(cfg, child->hdr, i);
      live_bytes += sizeof_index_entry(entry);
   }
   uint64 total_space_required = live_bytes + nentries * sizeof(index_entry);

   if (total_space_required <
       VARIABLE_LENGTH_BTREE_SPLIT_THRESHOLD(cfg->page_size)) {
      variable_length_btree_node_full_unlock(cc, cfg, parent);
      variable_length_btree_defragment_index(cfg, scratch, child->hdr);
      *new_child = *child;
   } else {
      variable_length_btree_split_child_index(cc,
                                              cfg,
                                              mini,
                                              scratch,
                                              parent,
                                              index_of_child_in_parent,
                                              child,
                                              key_to_be_inserted,
                                              new_child,
                                              next_child_idx);
   }

   return 0;
}


static inline uint64
add_possibly_unknown(uint32 a, int32 b)
{
   if (a != VARIABLE_LENGTH_BTREE_UNKNOWN &&
       b != VARIABLE_LENGTH_BTREE_UNKNOWN) {
      return a + b;
   } else {
      return VARIABLE_LENGTH_BTREE_UNKNOWN;
   }
}

static inline void
accumulate_node_ranks(const variable_length_btree_config *cfg,
                      const variable_length_btree_hdr *   hdr,
                      int                                 from,
                      int                                 to,
                      uint32 *                            num_kvs,
                      uint32 *                            key_bytes,
                      uint32 *                            message_bytes)
{
   debug_assert(from <= to);
   if (variable_length_btree_height(hdr) == 0) {
      for (int i = from; i < to; i++) {
         leaf_entry *entry = variable_length_btree_get_leaf_entry(cfg, hdr, i);
         *key_bytes =
            add_possibly_unknown(*key_bytes, leaf_entry_key_size(entry));
         *message_bytes = add_possibly_unknown(*message_bytes,
                                               leaf_entry_message_size(entry));
      }
      *num_kvs += to - from;
   } else {
      for (int i = from; i < to; i++) {
         index_entry *entry =
            variable_length_btree_get_index_entry(cfg, hdr, i);

         *num_kvs =
            add_possibly_unknown(*num_kvs, entry->pivot_data.num_kvs_in_tree);
         *key_bytes     = add_possibly_unknown(*key_bytes,
                                           entry->pivot_data.key_bytes_in_tree);
         *message_bytes = add_possibly_unknown(
            *message_bytes, entry->pivot_data.message_bytes_in_tree);
      }
   }
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_grow_root --
 *
 *      Adds a new root above the root.
 *
 * Requires: lock on root_node
 *
 * Upon return:
 * - root is locked
 *-----------------------------------------------------------------------------
 */
static inline int
variable_length_btree_grow_root(cache *                             cc,  // IN
                                const variable_length_btree_config *cfg, // IN
                                mini_allocator *            mini,      // IN/OUT
                                variable_length_btree_node *root_node) // OUT
{
   // allocate a new left node
   variable_length_btree_node child;
   variable_length_btree_alloc(cc,
                               mini,
                               variable_length_btree_height(root_node->hdr),
                               NULL_SLICE,
                               NULL,
                               PAGE_TYPE_MEMTABLE,
                               &child);

   // copy root to child
   memmove(child.hdr, root_node->hdr, variable_length_btree_page_size(cfg));
   variable_length_btree_node_unlock(cc, cfg, &child);
   variable_length_btree_node_unclaim(cc, cfg, &child);

   variable_length_btree_reset_node_entries(cfg, root_node->hdr);
   variable_length_btree_increment_height(root_node->hdr);
   slice new_pivot;
   if (variable_length_btree_height(child.hdr) == 0) {
      new_pivot = variable_length_btree_get_tuple_key(cfg, child.hdr, 0);
   } else {
      new_pivot = variable_length_btree_get_pivot(cfg, child.hdr, 0);
   }
   bool succeeded =
      variable_length_btree_set_index_entry(cfg,
                                            root_node->hdr,
                                            0,
                                            new_pivot,
                                            child.addr,
                                            VARIABLE_LENGTH_BTREE_UNKNOWN,
                                            VARIABLE_LENGTH_BTREE_UNKNOWN,
                                            VARIABLE_LENGTH_BTREE_UNKNOWN);
   platform_assert(succeeded);

   variable_length_btree_node_unget(cc, cfg, &child);
   return 0;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_insert --
 *
 *      Inserts the tuple into the dynamic variable_length_btree.
 *
 *      Return value:
 *      success       -- the tuple has been inserted
 *      locked        -- the insert failed, but the caller didn't fill the tree
 *      lock acquired -- the insert failed, and the caller filled the tree
 *-----------------------------------------------------------------------------
 */
platform_status
variable_length_btree_insert(cache *                             cc,      // IN
                             const variable_length_btree_config *cfg,     // IN
                             platform_heap_id                    heap_id, // IN
                             variable_length_btree_scratch *     scratch, // IN
                             uint64          root_addr,                   // IN
                             mini_allocator *mini,                        // IN
                             slice           key,                         // IN
                             slice           message,                     // IN
                             uint64 *        generation,                  // OUT
                             bool *          was_unique)                            // OUT
{
   platform_status       rc;
   leaf_incorporate_spec spec;
   uint64                leaf_wait = 1;

   variable_length_btree_node root_node;
   root_node.addr = root_addr;

   platform_assert(slice_length(key) <= MAX_INLINE_KEY_SIZE);
   platform_assert(slice_length(message) <= MAX_INLINE_MESSAGE_SIZE);

start_over:
   variable_length_btree_node_get(cc, cfg, &root_node, PAGE_TYPE_MEMTABLE);

   if (variable_length_btree_height(root_node.hdr) == 0) {
      rc = variable_length_btree_create_leaf_incorporate_spec(
         cfg, heap_id, root_node.hdr, key, message, &spec);
      if (!SUCCESS(rc)) {
         variable_length_btree_node_unget(cc, cfg, &root_node);
         return rc;
      }
      if (!variable_length_btree_node_claim(cc, cfg, &root_node)) {
         variable_length_btree_node_unget(cc, cfg, &root_node);
         destroy_leaf_incorporate_spec(&spec);
         goto start_over;
      }
      variable_length_btree_node_lock(cc, cfg, &root_node);
      if (variable_length_btree_try_perform_leaf_incorporate_spec(
             cfg, root_node.hdr, &spec, generation))
      {
         *was_unique = !spec.was_found;
         variable_length_btree_node_full_unlock(cc, cfg, &root_node);
         destroy_leaf_incorporate_spec(&spec);
         return STATUS_OK;
      }
      destroy_leaf_incorporate_spec(&spec);
      variable_length_btree_grow_root(cc, cfg, mini, &root_node);
      variable_length_btree_node_unlock(cc, cfg, &root_node);
      variable_length_btree_node_unclaim(cc, cfg, &root_node);
   }

   /* read lock on root_node, root_node is an index. */

   bool  found;
   int64 child_idx =
      variable_length_btree_find_pivot(cfg, root_node.hdr, key, &found);
   index_entry *parent_entry;

   if (child_idx < 0 ||
       variable_length_btree_index_is_full(cfg, root_node.hdr)) {
      if (!variable_length_btree_node_claim(cc, cfg, &root_node)) {
         variable_length_btree_node_unget(cc, cfg, &root_node);
         goto start_over;
      }
      variable_length_btree_node_lock(cc, cfg, &root_node);
      bool need_to_set_min_key = FALSE;
      if (child_idx < 0) {
         child_idx = 0;
         parent_entry =
            variable_length_btree_get_index_entry(cfg, root_node.hdr, 0);
         need_to_set_min_key = !variable_length_btree_set_index_entry(
            cfg,
            root_node.hdr,
            0,
            key,
            index_entry_child_addr(parent_entry),
            parent_entry->pivot_data.num_kvs_in_tree,
            parent_entry->pivot_data.key_bytes_in_tree,
            parent_entry->pivot_data.message_bytes_in_tree);
      }
      if (variable_length_btree_index_is_full(cfg, root_node.hdr)) {
         variable_length_btree_grow_root(cc, cfg, mini, &root_node);
         child_idx = 0;
      }
      if (need_to_set_min_key) {
         parent_entry =
            variable_length_btree_get_index_entry(cfg, root_node.hdr, 0);
         bool success = variable_length_btree_set_index_entry(
            cfg,
            root_node.hdr,
            0,
            key,
            index_entry_child_addr(parent_entry),
            parent_entry->pivot_data.num_kvs_in_tree,
            parent_entry->pivot_data.key_bytes_in_tree,
            parent_entry->pivot_data.message_bytes_in_tree);
         platform_assert(success);
      }
      variable_length_btree_node_unlock(cc, cfg, &root_node);
      variable_length_btree_node_unclaim(cc, cfg, &root_node);
   }

   parent_entry =
      variable_length_btree_get_index_entry(cfg, root_node.hdr, child_idx);

   /* root_node read-locked,
    * root_node is an index,
    * root_node min key is up to date,
    * root_node will not need to split
    */
   variable_length_btree_node parent_node = root_node;
   variable_length_btree_node child_node;
   child_node.addr = index_entry_child_addr(parent_entry);
   debug_assert(cache_page_valid(cc, child_node.addr));
   variable_length_btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);

   uint64 height = variable_length_btree_height(parent_node.hdr);
   while (height > 1) {
      /* loop invariant:
         - read lock on parent_node, parent_node is an index, parent_node min
         key is up to date, and parent_node will not need to split.
         - read lock on child_node
         - height >= 1
      */
      int64 next_child_idx =
         variable_length_btree_find_pivot(cfg, child_node.hdr, key, &found);
      if (next_child_idx < 0 ||
          variable_length_btree_index_is_full(cfg, child_node.hdr)) {
         if (!variable_length_btree_node_claim(cc, cfg, &parent_node)) {
            variable_length_btree_node_unget(cc, cfg, &parent_node);
            variable_length_btree_node_unget(cc, cfg, &child_node);
            goto start_over;
         }
         if (!variable_length_btree_node_claim(cc, cfg, &child_node)) {
            variable_length_btree_node_unclaim(cc, cfg, &parent_node);
            variable_length_btree_node_unget(cc, cfg, &parent_node);
            variable_length_btree_node_unget(cc, cfg, &child_node);
            goto start_over;
         }

         variable_length_btree_node_lock(cc, cfg, &parent_node);
         variable_length_btree_node_lock(cc, cfg, &child_node);

         bool need_to_set_min_key = FALSE;
         if (next_child_idx < 0) {
            next_child_idx           = 0;
            index_entry *child_entry = variable_length_btree_get_index_entry(
               cfg, child_node.hdr, next_child_idx);
            need_to_set_min_key = !variable_length_btree_set_index_entry(
               cfg,
               child_node.hdr,
               0,
               key,
               index_entry_child_addr(child_entry),
               child_entry->pivot_data.num_kvs_in_tree,
               child_entry->pivot_data.key_bytes_in_tree,
               child_entry->pivot_data.message_bytes_in_tree);
         }

         if (variable_length_btree_index_is_full(cfg, child_node.hdr)) {
            variable_length_btree_node new_child;
            variable_length_btree_defragment_or_split_child_index(
               cc,
               cfg,
               mini,
               scratch,
               &parent_node,
               child_idx,
               &child_node,
               key,
               &new_child,
               &next_child_idx);
            parent_node = new_child;
         } else {
            variable_length_btree_node_full_unlock(cc, cfg, &parent_node);
            parent_node = child_node;
         }

         if (need_to_set_min_key) { // new_child is guaranteed to be child in
                                    // this case
            index_entry *child_entry =
               variable_length_btree_get_index_entry(cfg, parent_node.hdr, 0);
            bool success = variable_length_btree_set_index_entry(
               cfg,
               parent_node.hdr,
               0,
               key,
               index_entry_child_addr(child_entry),
               child_entry->pivot_data.num_kvs_in_tree,
               child_entry->pivot_data.key_bytes_in_tree,
               child_entry->pivot_data.message_bytes_in_tree);
            platform_assert(success);
         }
         variable_length_btree_node_unlock(cc, cfg, &parent_node);
         variable_length_btree_node_unclaim(cc, cfg, &parent_node);
      } else {
         variable_length_btree_node_unget(cc, cfg, &parent_node);
         parent_node = child_node;
      }

      /* read lock on parent_node, which won't require a split. */

      child_idx = next_child_idx;
      parent_entry =
         variable_length_btree_get_index_entry(cfg, parent_node.hdr, child_idx);
      debug_assert(parent_entry->pivot_data.num_kvs_in_tree ==
                   VARIABLE_LENGTH_BTREE_UNKNOWN);
      debug_assert(parent_entry->pivot_data.key_bytes_in_tree ==
                   VARIABLE_LENGTH_BTREE_UNKNOWN);
      debug_assert(parent_entry->pivot_data.message_bytes_in_tree ==
                   VARIABLE_LENGTH_BTREE_UNKNOWN);
      child_node.addr = index_entry_child_addr(parent_entry);
      debug_assert(cache_page_valid(cc, child_node.addr));
      variable_length_btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
      height--;
   }

   /*
    * - read lock on parent_node, parent_node is an index, parent node
    *   min key is up to date, and parent_node will not need to split.
    * - read lock on child_node
    * - height of parent == 1
    */

   rc = variable_length_btree_create_leaf_incorporate_spec(
      cfg, heap_id, child_node.hdr, key, message, &spec);
   if (!SUCCESS(rc)) {
      variable_length_btree_node_unget(cc, cfg, &parent_node);
      variable_length_btree_node_unget(cc, cfg, &child_node);
      return rc;
   }

   /* If we don't need to split, then let go of the parent and do the
    * insert.  If we can't get a claim on the child, then start
    * over.
    */
   if (variable_length_btree_can_perform_leaf_incorporate_spec(
          cfg, child_node.hdr, &spec))
   {
      variable_length_btree_node_unget(cc, cfg, &parent_node);
      if (!variable_length_btree_node_claim(cc, cfg, &child_node)) {
         variable_length_btree_node_unget(cc, cfg, &child_node);
         destroy_leaf_incorporate_spec(&spec);
         goto start_over;
      }
      variable_length_btree_node_lock(cc, cfg, &child_node);
      bool incorporated =
         variable_length_btree_try_perform_leaf_incorporate_spec(
            cfg, child_node.hdr, &spec, generation);
      platform_assert(incorporated);
      variable_length_btree_node_full_unlock(cc, cfg, &child_node);
      destroy_leaf_incorporate_spec(&spec);
      *was_unique = !spec.was_found;
      return STATUS_OK;
   }

   /* Need to split or defrag the child. */
   if (!variable_length_btree_node_claim(cc, cfg, &parent_node)) {
      variable_length_btree_node_unget(cc, cfg, &parent_node);
      variable_length_btree_node_unget(cc, cfg, &child_node);
      destroy_leaf_incorporate_spec(&spec);
      goto start_over;
   }
   bool need_to_rebuild_spec = FALSE;
   while (!variable_length_btree_node_claim(cc, cfg, &child_node)) {
      variable_length_btree_node_unget(cc, cfg, &child_node);
      platform_sleep(leaf_wait);
      leaf_wait = leaf_wait > 2048 ? leaf_wait : 2 * leaf_wait;
      variable_length_btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
      need_to_rebuild_spec = TRUE;
   }
   if (need_to_rebuild_spec) {
      /* If we had to relenquish our lock, then our spec might be out of date,
       * so rebuild it. */
      destroy_leaf_incorporate_spec(&spec);
      rc = variable_length_btree_create_leaf_incorporate_spec(
         cfg, heap_id, child_node.hdr, key, message, &spec);
      if (!SUCCESS(rc)) {
         variable_length_btree_node_unget(cc, cfg, &parent_node);
         variable_length_btree_node_unclaim(cc, cfg, &child_node);
         variable_length_btree_node_unget(cc, cfg, &child_node);
         return rc;
      }
   }
   variable_length_btree_defragment_or_split_child_leaf(cc,
                                                        cfg,
                                                        mini,
                                                        scratch,
                                                        &parent_node,
                                                        child_idx,
                                                        &child_node,
                                                        &spec,
                                                        generation);
   destroy_leaf_incorporate_spec(&spec);
   *was_unique = !spec.was_found;
   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_lookup_node --
 *
 *      lookup_node finds the node of height stop_at_height with
 *      (node.min_key <= key < node.max_key) and returns it with a read lock
 *      held.
 *
 *      out_rank returns the rank of out_node amount nodes of height
 *      stop_at_height.
 *
 *      If any change is made here, please change
 *      variable_length_btree_lookup_async_with_ref too.
 *-----------------------------------------------------------------------------
 */
platform_status
variable_length_btree_lookup_node(
   cache *                       cc,        // IN
   variable_length_btree_config *cfg,       // IN
   uint64                        root_addr, // IN
   const slice                   key,       // IN
   uint16    stop_at_height,                // IN  search down to this height
   page_type type,                          // IN
   variable_length_btree_node
      *out_node,    // OUT returns the node of height
                    // stop_at_height in which key was found
   uint32 *kv_rank, // ranks must be all NULL or all non-NULL
   uint32 *key_byte_rank,
   uint32 *message_byte_rank)
{
   variable_length_btree_node node, child_node;
   uint32                     h;
   int64                      child_idx;

   if (kv_rank) {
      *kv_rank = *key_byte_rank = *message_byte_rank = 0;
   }

   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   node.addr = root_addr;
   variable_length_btree_node_get(cc, cfg, &node, type);

   for (h = variable_length_btree_height(node.hdr); h > stop_at_height; h--) {
      bool found;
      child_idx =
         slices_equal(key, positive_infinity)
            ? variable_length_btree_num_entries(node.hdr) - 1
            : variable_length_btree_find_pivot(cfg, node.hdr, key, &found);
      if (child_idx < 0) {
         child_idx = 0;
      }
      index_entry *entry =
         variable_length_btree_get_index_entry(cfg, node.hdr, child_idx);
      child_node.addr = index_entry_child_addr(entry);

      if (kv_rank) {
         accumulate_node_ranks(cfg,
                               node.hdr,
                               0,
                               child_idx,
                               kv_rank,
                               key_byte_rank,
                               message_byte_rank);
      }

      variable_length_btree_node_get(cc, cfg, &child_node, type);
      debug_assert(child_node.page->disk_addr == child_node.addr);
      variable_length_btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   *out_node = node;
   return STATUS_OK;
}


static inline void
variable_length_btree_lookup_with_ref(cache *                       cc,  // IN
                                      variable_length_btree_config *cfg, // IN
                                      uint64      root_addr,             // IN
                                      page_type   type,                  // IN
                                      const slice key,                   // IN
                                      variable_length_btree_node *node,  // OUT
                                      slice *                     data,  // OUT
                                      bool *                      found)                       // OUT
{
   variable_length_btree_lookup_node(
      cc, cfg, root_addr, key, 0, type, node, NULL, NULL, NULL);
   int64 idx = variable_length_btree_find_tuple(cfg, node->hdr, key, found);
   if (*found) {
      leaf_entry *entry =
         variable_length_btree_get_leaf_entry(cfg, node->hdr, idx);
      *data = leaf_entry_message_slice(entry);
   } else {
      variable_length_btree_node_unget(cc, cfg, node);
   }
}

platform_status
variable_length_btree_lookup(cache *                       cc,        // IN
                             variable_length_btree_config *cfg,       // IN
                             uint64                        root_addr, // IN
                             page_type                     type,      // IN
                             const slice                   key,       // IN
                             writable_buffer *             result)                 // OUT
{
   variable_length_btree_node node;
   slice                      data;
   platform_status            rc = STATUS_OK;
   bool                       local_found;

   variable_length_btree_lookup_with_ref(
      cc, cfg, root_addr, type, key, &node, &data, &local_found);
   if (local_found) {
      rc = writable_buffer_copy_slice(result, data);
      variable_length_btree_node_unget(cc, cfg, &node);
   }
   return rc;
}

platform_status
variable_length_btree_lookup_and_merge(cache *                       cc,  // IN
                                       variable_length_btree_config *cfg, // IN
                                       uint64           root_addr,        // IN
                                       page_type        type,             // IN
                                       const slice      key,              // IN
                                       writable_buffer *data,             // OUT
                                       bool *           local_found)                 // OUT
{
   variable_length_btree_node node;
   slice                      local_data;
   platform_status            rc = STATUS_OK;
   variable_length_btree_lookup_with_ref(
      cc, cfg, root_addr, type, key, &node, &local_data, local_found);
   if (*local_found) {
      if (writable_buffer_is_null(data)) {
         rc = writable_buffer_copy_slice(data, local_data);
      } else if (variable_length_btree_merge_tuples(cfg, key, local_data, data))
      {
         rc = STATUS_NO_MEMORY;
      }
      variable_length_btree_node_unget(cc, cfg, &node);
   }
   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_async_set_state --
 *      Set the state of the async variable_length_btree lookup state machine.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static inline void
variable_length_btree_async_set_state(
   variable_length_btree_async_ctxt *ctxt,
   variable_length_btree_async_state new_state)
{
   ctxt->prev_state = ctxt->state;
   ctxt->state      = new_state;
}


/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_async_callback --
 *
 *      Callback that's called when the async cache get loads a page into
 *      the cache. This function moves the async variable_length_btree lookup
 *state machine's state ahead, and calls the upper layer callback that'll
 *re-enqueue the variable_length_btree lookup for dispatch.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static void
variable_length_btree_async_callback(cache_async_ctxt *cache_ctxt)
{
   variable_length_btree_async_ctxt *ctxt = cache_ctxt->cbdata;

   platform_assert(SUCCESS(cache_ctxt->status));
   platform_assert(cache_ctxt->page);
   //   platform_log("%s:%d tid %2lu: ctxt %p is callback with page %p
   //   (%#lx)\n",
   //                __FILE__, __LINE__, platform_get_tid(), ctxt,
   //                cache_ctxt->page, ctxt->child_addr);
   ctxt->was_async = TRUE;
   platform_assert(ctxt->state == variable_length_btree_async_state_get_node);
   // Move state machine ahead and requeue for dispatch
   variable_length_btree_async_set_state(
      ctxt, variable_length_btree_async_state_get_index_complete);
   ctxt->cb(ctxt);
}


/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_lookup_async_with_ref --
 *
 *      State machine for the async variable_length_btree point lookup. This
 *uses hand over hand locking to descend the tree and every time a child node
 *needs to be looked up from the cache, it uses the async get api. A reference
 *to the parent node is held in variable_length_btree_async_ctxt->node while a
 *reference to the child page is obtained by the cache_get_async() in
 *      variable_length_btree_async_ctxt->cache_ctxt->page
 *
 * Results:
 *      See variable_length_btree_lookup_async(). if returning async_success and
 **found = TRUE, this returns with ref on the variable_length_btree leaf. Caller
 *must do unget() on node_out.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static cache_async_result
variable_length_btree_lookup_async_with_ref(
   cache *                           cc,        // IN
   variable_length_btree_config *    cfg,       // IN
   uint64                            root_addr, // IN
   slice                             key,       // IN
   variable_length_btree_node *      node_out,  // OUT
   slice *                           data,      // OUT
   bool *                            found,     // OUT
   variable_length_btree_async_ctxt *ctxt)      // IN
{
   cache_async_result          res  = 0;
   bool                        done = FALSE;
   variable_length_btree_node *node = &ctxt->node;

   do {
      switch (ctxt->state) {
         case variable_length_btree_async_state_start:
         {
            ctxt->child_addr = root_addr;
            node->page       = NULL;
            variable_length_btree_async_set_state(
               ctxt, variable_length_btree_async_state_get_node);
            // fallthrough
         }
         case variable_length_btree_async_state_get_node:
         {
            cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

            cache_ctxt_init(
               cc, variable_length_btree_async_callback, ctxt, cache_ctxt);
            res = cache_get_async(
               cc, ctxt->child_addr, PAGE_TYPE_BRANCH, cache_ctxt);
            switch (res) {
               case async_locked:
               case async_no_reqs:
                  //            platform_log("%s:%d tid %2lu: ctxt %p is
                  //            retry\n",
                  //                         __FILE__, __LINE__,
                  //                         platform_get_tid(), ctxt);
                  /*
                   * Ctxt remains at same state. The invocation is done, but
                   * the request isn't; and caller will re-invoke me.
                   */
                  done = TRUE;
                  break;
               case async_io_started:
                  //            platform_log("%s:%d tid %2lu: ctxt %p is
                  //            io_started\n",
                  //                         __FILE__, __LINE__,
                  //                         platform_get_tid(), ctxt);
                  // Invocation is done; request isn't. Callback will move
                  // state.
                  done = TRUE;
                  break;
               case async_success:
                  ctxt->was_async = FALSE;
                  variable_length_btree_async_set_state(
                     ctxt,
                     variable_length_btree_async_state_get_index_complete);
                  break;
               default:
                  platform_assert(0);
            }
            break;
         }
         case variable_length_btree_async_state_get_index_complete:
         {
            cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

            if (node->page) {
               // Unlock parent
               variable_length_btree_node_unget(cc, cfg, node);
            }
            variable_length_btree_node_get_from_cache_ctxt(
               cfg, cache_ctxt, node);
            debug_assert(node->addr == ctxt->child_addr);
            if (ctxt->was_async) {
               cache_async_done(cc, PAGE_TYPE_BRANCH, cache_ctxt);
            }
            if (variable_length_btree_height(node->hdr) == 0) {
               variable_length_btree_async_set_state(
                  ctxt, variable_length_btree_async_state_get_leaf_complete);
               break;
            }
            bool  found_pivot;
            int64 child_idx = variable_length_btree_find_pivot(
               cfg, node->hdr, key, &found_pivot);
            if (child_idx < 0) {
               child_idx = 0;
            }
            ctxt->child_addr =
               variable_length_btree_get_child_addr(cfg, node->hdr, child_idx);
            variable_length_btree_async_set_state(
               ctxt, variable_length_btree_async_state_get_node);
            break;
         }
         case variable_length_btree_async_state_get_leaf_complete:
         {
            int64 idx =
               variable_length_btree_find_tuple(cfg, node->hdr, key, found);
            if (*found) {
               *data =
                  variable_length_btree_get_tuple_message(cfg, node->hdr, idx);
               *node_out = *node;
            } else {
               variable_length_btree_node_unget(cc, cfg, node);
            }
            res  = async_success;
            done = TRUE;
            break;
         }
         default:
            platform_assert(0);
      }
   } while (!done);

   return res;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_lookup_async --
 *
 *      Async variable_length_btree point lookup. The ctxt should've been
 *initialized using variable_length_btree_ctxt_init(). The return value can be
 *either of: async_locked: A page needed by lookup is locked. User should retry
 *      request.
 *      async_no_reqs: A page needed by lookup is not in cache and the IO
 *      subsytem is out of requests. User should throttle.
 *      async_io_started: Async IO was started to read a page needed by the
 *      lookup into the cache. When the read is done, caller will be notified
 *      using ctxt->cb, that won't run on the thread context. It can be used
 *      to requeue the async lookup request for dispatch in thread context.
 *      When it's requeued, it must use the same function params except found.
 *      success: *found is TRUE if found, FALSE otherwise, data is stored in
 *      *data_out
 *
 * Results:
 *      Async result.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
cache_async_result
variable_length_btree_lookup_async(cache *                       cc,  // IN
                                   variable_length_btree_config *cfg, // IN
                                   uint64           root_addr,        // IN
                                   slice            key,              // IN
                                   writable_buffer *result,           // OUT
                                   variable_length_btree_async_ctxt *ctxt) // IN
{
   cache_async_result         res;
   variable_length_btree_node node;
   slice                      data;
   bool                       local_found;
   res = variable_length_btree_lookup_async_with_ref(
      cc, cfg, root_addr, key, &node, &data, &local_found, ctxt);
   if (res == async_success && local_found) {
      platform_status rc = writable_buffer_copy_slice(result, data);
      platform_assert_status_ok(rc); // FIXME
      variable_length_btree_node_unget(cc, cfg, &node);
   }

   return res;
}

cache_async_result
variable_length_btree_lookup_and_merge_async(
   cache *                           cc,          // IN
   variable_length_btree_config *    cfg,         // IN
   uint64                            root_addr,   // IN
   const slice                       key,         // IN
   writable_buffer *                 data,        // OUT
   bool *                            local_found, // OUT
   variable_length_btree_async_ctxt *ctxt)        // IN
{
   cache_async_result         res;
   variable_length_btree_node node;
   slice                      local_data;

   res = variable_length_btree_lookup_async_with_ref(
      cc, cfg, root_addr, key, &node, &local_data, local_found, ctxt);
   if (res == async_success && *local_found) {
      if (writable_buffer_is_null(data)) {
         platform_status rc = writable_buffer_copy_slice(data, local_data);
         platform_assert_status_ok(rc);
      } else {
         int rc =
            variable_length_btree_merge_tuples(cfg, key, local_data, data);
         platform_assert(rc == 0);
      }
      variable_length_btree_node_unget(cc, cfg, &node);
   }
   return res;
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_iterator_init --
 * variable_length_btree_iterator_get_curr --
 * variable_length_btree_iterator_advance --
 * variable_length_btree_iterator_at_end
 *
 * This iterator implementation supports an upper bound key ub.  Given
 * an upper bound, the iterator will return only keys strictly less
 * than ub.
 *
 * In order to avoid comparing every key with ub, it precomputes,
 * during initialization, the end leaf and end_idx of ub within that
 * leaf.
 *
 * The iterator interacts with concurrent updates to the tree as
 * follows.  Its guarantees very much depend on the fact that we do
 * not delete entries in the tree.
 *
 * The iterator is guaranteed to see all keys that are between the
 * lower and upper bounds and that were present in the tree when the
 * iterator was initialized.
 *
 * One issue is splits of the end node that we computed during
 * initialization.  If the end node splits after initialization but
 * before the iterator gets to the end node, then some of the keys
 * that we should visit may have been moved to the right sibling of
 * the end node.
 *
 * So, whenever the iterator reaches the end node, it immediately
 * checks whether the end node's generation has changed since the
 * iterator was initialized.  If it has, then the iterator recomputes
 * the end node and end_idx.
 *-----------------------------------------------------------------------------
 */
static bool
variable_length_btree_iterator_is_at_end(variable_length_btree_iterator *itor)
{
   return itor->curr.addr == itor->end_addr && itor->idx == itor->end_idx;
}

void
variable_length_btree_iterator_get_curr(iterator *base_itor,
                                        slice *   key,
                                        slice *   data)
{
   debug_assert(base_itor != NULL);
   variable_length_btree_iterator *itor =
      (variable_length_btree_iterator *)base_itor;
   debug_assert(itor->curr.hdr != NULL);
   // if (itor->at_end || itor->idx == itor->curr.hdr->num_entries) {
   //   variable_length_btree_print_tree(itor->cc, itor->cfg, itor->root_addr);
   //}
   debug_assert(!variable_length_btree_iterator_is_at_end(itor));
   debug_assert(itor->idx < variable_length_btree_num_entries(itor->curr.hdr));
   debug_assert(itor->curr.page != NULL);
   debug_assert(itor->curr.page->disk_addr == itor->curr.addr);
   debug_assert((char *)itor->curr.hdr == itor->curr.page->data);
   cache_validate_page(itor->cc, itor->curr.page, itor->curr.addr);
   if (itor->curr.hdr->height == 0) {
      *key = variable_length_btree_get_tuple_key(
         itor->cfg, itor->curr.hdr, itor->idx);
      *data = variable_length_btree_get_tuple_message(
         itor->cfg, itor->curr.hdr, itor->idx);
   } else {
      index_entry *entry = variable_length_btree_get_index_entry(
         itor->cfg, itor->curr.hdr, itor->idx);
      *key  = index_entry_key_slice(entry);
      *data = slice_create(sizeof(entry->pivot_data), &entry->pivot_data);
   }
}

static void
variable_length_btree_iterator_find_end(variable_length_btree_iterator *itor)
{
   variable_length_btree_node end;

   variable_length_btree_lookup_node(itor->cc,
                                     itor->cfg,
                                     itor->root_addr,
                                     itor->max_key,
                                     itor->height,
                                     itor->page_type,
                                     &end,
                                     NULL,
                                     NULL,
                                     NULL);
   itor->end_addr       = end.addr;
   itor->end_generation = end.hdr->generation;

   if (slices_equal(itor->max_key, positive_infinity)) {
      itor->end_idx = variable_length_btree_num_entries(end.hdr);
   } else {
      bool  found;
      int64 tmp;
      if (itor->height == 0) {
         tmp = variable_length_btree_find_tuple(
            itor->cfg, end.hdr, itor->max_key, &found);
         if (!found) {
            tmp++;
         }
      } else if (itor->height > end.hdr->height) {
         tmp = 0;
         itor->height =
            (uint32)-1; // So we will always exceed height in future lookups
      } else {
         tmp = variable_length_btree_find_pivot(
            itor->cfg, end.hdr, itor->max_key, &found);
         if (!found) {
            tmp++;
         }
      }
      itor->end_idx = tmp;
   }

   variable_length_btree_node_unget(itor->cc, itor->cfg, &end);
}

/*
 * Move to the next leaf when we've reached the end of one leaf but
 * haven't reached the end of the iterator.
 */
static void
variable_length_btree_iterator_advance_leaf(
   variable_length_btree_iterator *itor)
{
   cache *                       cc  = itor->cc;
   variable_length_btree_config *cfg = itor->cfg;

   uint64 last_addr = itor->curr.addr;
   uint64 next_addr = itor->curr.hdr->next_addr;
   variable_length_btree_node_unget(cc, cfg, &itor->curr);
   itor->curr.addr = next_addr;
   variable_length_btree_node_get(cc, cfg, &itor->curr, itor->page_type);
   itor->idx = 0;

   while (itor->curr.addr == itor->end_addr &&
          itor->curr.hdr->generation != itor->end_generation) {
      /* We need to recompute the end node and end_idx. (see
         comment at beginning of iterator implementation for
         high-level description)

         There's a potential for deadlock with concurrent inserters
         if we hold a read-lock on curr while looking up end, so we
         temporarily release curr.

         It is safe to relase curr because we are at index 0 of
         curr.  To see why, observe that, at this point, curr
         cannot be the first leaf in the tree (since we just
         followed a next pointer a few lines above).  And, for
         every leaf except the left-most leaf of the tree, no key
         can ever be inserted into the leaf that is smaller than
         the leaf's 0th entry, because its 0th entry is also its
         pivot in its parent.  Thus we are guaranteed that the
         first key curr will not change between the unget and the
         get. Hence we will not "go backwards" i.e. return a key
         smaller than the previous key) or skip any keys.
         Furthermore, even if another thread comes along and splits
         curr while we've released it, we will still want to
         continue at curr (since we're at the 0th entry).
      */
      variable_length_btree_node_unget(itor->cc, itor->cfg, &itor->curr);
      variable_length_btree_iterator_find_end(itor);
      variable_length_btree_node_get(
         itor->cc, itor->cfg, &itor->curr, itor->page_type);
   }

   // To prefetch:
   // 1. we just moved from one extent to the next
   // 2. this can't be the last extent
   if (itor->do_prefetch &&
       !variable_length_btree_addrs_share_extent(
          cfg, last_addr, itor->curr.addr) &&
       itor->curr.hdr->next_extent_addr != 0 &&
       !variable_length_btree_addrs_share_extent(
          cfg, itor->curr.addr, itor->end_addr)) {
      // IO prefetch the next extent
      cache_prefetch(cc, itor->curr.hdr->next_extent_addr, TRUE);
   }
}

platform_status
variable_length_btree_iterator_advance(iterator *base_itor)
{
   debug_assert(base_itor != NULL);
   variable_length_btree_iterator *itor =
      (variable_length_btree_iterator *)base_itor;

   // We should not be calling advance on an empty iterator
   debug_assert(!variable_length_btree_iterator_is_at_end(itor));
   debug_assert(itor->idx < variable_length_btree_num_entries(itor->curr.hdr));

   itor->idx++;

   if (!variable_length_btree_iterator_is_at_end(itor) &&
       itor->idx == variable_length_btree_num_entries(itor->curr.hdr)) {
      variable_length_btree_iterator_advance_leaf(itor);
   }

   debug_assert(variable_length_btree_iterator_is_at_end(itor) ||
                itor->idx < variable_length_btree_num_entries(itor->curr.hdr));

   return STATUS_OK;
}


platform_status
variable_length_btree_iterator_at_end(iterator *itor, bool *at_end)
{
   debug_assert(itor != NULL);
   *at_end = variable_length_btree_iterator_is_at_end(
      (variable_length_btree_iterator *)itor);

   return STATUS_OK;
}

void
variable_length_btree_iterator_print(iterator *itor)
{
   debug_assert(itor != NULL);
   variable_length_btree_iterator *variable_length_btree_itor =
      (variable_length_btree_iterator *)itor;

   platform_log("########################################\n");
   platform_log("## variable_length_btree_itor: %p\n", itor);
   platform_log("## root: %lu\n", variable_length_btree_itor->root_addr);
   platform_log("## curr %lu end %lu\n",
                variable_length_btree_itor->curr.addr,
                variable_length_btree_itor->end_addr);
   platform_log("## idx %lu end_idx %lu generation %lu\n",
                variable_length_btree_itor->idx,
                variable_length_btree_itor->end_idx,
                variable_length_btree_itor->end_generation);
   variable_length_btree_print_node(variable_length_btree_itor->cc,
                                    variable_length_btree_itor->cfg,
                                    &variable_length_btree_itor->curr,
                                    PLATFORM_DEFAULT_LOG_HANDLE);
}

const static iterator_ops variable_length_btree_iterator_ops = {
   .get_curr = variable_length_btree_iterator_get_curr,
   .at_end   = variable_length_btree_iterator_at_end,
   .advance  = variable_length_btree_iterator_advance,
   .print    = variable_length_btree_iterator_print,
};


/*
 *-----------------------------------------------------------------------------
 * Caller must guarantee:
 *    max_key (if not null) needs to be valid until at_end() returns true
 *-----------------------------------------------------------------------------
 */
void
variable_length_btree_iterator_init(cache *                         cc,
                                    variable_length_btree_config *  cfg,
                                    variable_length_btree_iterator *itor,
                                    uint64                          root_addr,
                                    page_type                       page_type,
                                    const slice                     min_key,
                                    const slice                     _max_key,
                                    bool                            do_prefetch,
                                    uint32                          height)
{
   platform_assert(root_addr != 0);
   debug_assert(page_type == PAGE_TYPE_MEMTABLE ||
                page_type == PAGE_TYPE_BRANCH);

   slice max_key;

   if (slice_is_null(_max_key)) {
      max_key = positive_infinity;
   } else if (!slice_is_null(min_key) &&
              variable_length_btree_key_compare(cfg, min_key, _max_key) > 0) {
      max_key = min_key;
   } else {
      max_key = _max_key;
   }

   ZERO_CONTENTS(itor);
   itor->cc          = cc;
   itor->cfg         = cfg;
   itor->root_addr   = root_addr;
   itor->do_prefetch = do_prefetch;
   itor->height      = height;
   itor->min_key     = min_key;
   itor->max_key     = max_key;
   itor->page_type   = page_type;
   itor->super.ops   = &variable_length_btree_iterator_ops;

   variable_length_btree_lookup_node(itor->cc,
                                     itor->cfg,
                                     itor->root_addr,
                                     min_key,
                                     itor->height,
                                     itor->page_type,
                                     &itor->curr,
                                     NULL,
                                     NULL,
                                     NULL);
   /* We have to claim curr in order to prevent possible deadlocks
    * with insertion threads while finding the end node.
    *
    * Note that we can't lookup end first because, if there's a split
    * between looking up end and looking up curr, we could end up in a
    * situation where end comes before curr in the tree!  (We could
    * prevent this by holding a claim on end while looking up curr,
    * but that would essentially be the same as the code below.)
    *
    * Note that the approach in advance (i.e. releasing and reaquiring
    * a lock on curr) is not viable here because we are not
    * necessarily searching for the 0th entry in curr.  Thus a split
    * of curr while we have released it could mean that we really want
    * to start at curr's right sibling (after the split).  So we'd
    * have to redo the search from scratch after releasing curr.
    *
    * So we take a claim on curr instead.
    */
   while (!variable_length_btree_node_claim(cc, cfg, &itor->curr)) {
      variable_length_btree_node_unget(cc, cfg, &itor->curr);
      variable_length_btree_lookup_node(itor->cc,
                                        itor->cfg,
                                        itor->root_addr,
                                        min_key,
                                        itor->height,
                                        itor->page_type,
                                        &itor->curr,
                                        NULL,
                                        NULL,
                                        NULL);
   }

   variable_length_btree_iterator_find_end(itor);

   /* Once we've found end, we can unclaim curr. */
   variable_length_btree_node_unclaim(cc, cfg, &itor->curr);

   bool  found;
   int64 tmp;
   if (slice_is_null(min_key)) {
      tmp = 0;
   } else if (itor->height == 0) {
      tmp = variable_length_btree_find_tuple(
         itor->cfg, itor->curr.hdr, min_key, &found);
      if (!found) {
         tmp++;
      }
   } else if (itor->height > itor->curr.hdr->height) {
      tmp = 0;
   } else {
      tmp = variable_length_btree_find_pivot(
         itor->cfg, itor->curr.hdr, min_key, &found);
      if (!found) {
         tmp++;
      }
      platform_assert(0 <= tmp);
   }
   itor->idx = tmp;

   if (!variable_length_btree_iterator_is_at_end(itor) &&
       itor->idx == variable_length_btree_num_entries(itor->curr.hdr)) {
      variable_length_btree_iterator_advance_leaf(itor);
   }

   if (itor->do_prefetch && itor->curr.hdr->next_extent_addr != 0 &&
       !variable_length_btree_addrs_share_extent(
          cfg, itor->curr.addr, itor->end_addr)) {
      // IO prefetch the next extent
      cache_prefetch(cc, itor->curr.hdr->next_extent_addr, TRUE);
   }

   debug_assert(variable_length_btree_iterator_is_at_end(itor) ||
                itor->idx < variable_length_btree_num_entries(itor->curr.hdr));
}

void
variable_length_btree_iterator_deinit(variable_length_btree_iterator *itor)
{
   debug_assert(itor != NULL);
   variable_length_btree_node_unget(itor->cc, itor->cfg, &itor->curr);
}

// generation number isn't used in packed variable_length_btrees
static inline void
variable_length_btree_pack_node_init_hdr(
   const variable_length_btree_config *cfg,
   variable_length_btree_hdr *         hdr,
   uint64                              next_extent,
   uint8                               height)
{
   variable_length_btree_init_hdr(cfg, hdr);
   hdr->next_extent_addr = next_extent;
   hdr->height           = height;
}

static inline void
variable_length_btree_pack_setup_start(variable_length_btree_pack_req *req)
{
   // we create a root here, but we won't build it with the rest
   // of the tree, we'll copy into it at the end
   req->root_addr = variable_length_btree_create(
      req->cc, req->cfg, &req->mini, PAGE_TYPE_BRANCH);
   req->height = 0;
}


static inline void
variable_length_btree_pack_setup_finish(variable_length_btree_pack_req *req,
                                        slice first_key)
{
   // set up the first leaf
   variable_length_btree_alloc(req->cc,
                               &req->mini,
                               0,
                               first_key,
                               &req->next_extent,
                               PAGE_TYPE_BRANCH,
                               &req->edge[0]);
   debug_assert(cache_page_valid(req->cc, req->next_extent));
   variable_length_btree_pack_node_init_hdr(
      req->cfg, req->edge[0].hdr, req->next_extent, 0);
}

static inline void
variable_length_btree_pack_loop(variable_length_btree_pack_req *req, // IN/OUT
                                slice                           key, // IN
                                slice                           message, // IN
                                bool *at_end) // IN/OUT
{
   if (!variable_length_btree_set_leaf_entry(
          req->cfg,
          req->edge[0].hdr,
          variable_length_btree_num_entries(req->edge[0].hdr),
          key,
          message)) {
      // the current leaf is full, allocate a new one and add to index
      variable_length_btree_node old_edge = req->edge[0];

      variable_length_btree_alloc(req->cc,
                                  &req->mini,
                                  0,
                                  key,
                                  &req->next_extent,
                                  PAGE_TYPE_BRANCH,
                                  &req->edge[0]);
      old_edge.hdr->next_addr = req->edge[0].addr;

      // initialize the new leaf edge
      debug_assert(cache_page_valid(req->cc, req->next_extent));
      variable_length_btree_pack_node_init_hdr(
         req->cfg, req->edge[0].hdr, req->next_extent, 0);
      bool result = variable_length_btree_set_leaf_entry(
         req->cfg, req->edge[0].hdr, 0, key, message);
      platform_assert(result);

      // this loop finds the first level with a free slot
      // along the way it allocates new index nodes as necessary
      uint16 i = 1;
      while (i <= req->height &&
             !variable_length_btree_set_index_entry(
                req->cfg,
                req->edge[i].hdr,
                variable_length_btree_num_entries(req->edge[i].hdr),
                key,
                req->edge[i - 1].addr,
                0,
                0,
                0)) {
         variable_length_btree_node_full_unlock(req->cc, req->cfg, &old_edge);
         old_edge = req->edge[i];

         variable_length_btree_alloc(req->cc,
                                     &req->mini,
                                     i,
                                     key,
                                     &req->next_extent,
                                     PAGE_TYPE_BRANCH,
                                     &req->edge[i]);
         old_edge.hdr->next_addr = req->edge[i].addr;

         // initialize the new index edge
         variable_length_btree_pack_node_init_hdr(
            req->cfg, req->edge[i].hdr, req->next_extent, i);
         variable_length_btree_set_index_entry(
            req->cfg, req->edge[i].hdr, 0, key, req->edge[i - 1].addr, 0, 0, 0);
         i++;
      }

      if (req->height < i) {
         slice smallest_key =
            variable_length_btree_height(old_edge.hdr)
               ? variable_length_btree_get_pivot(req->cfg, old_edge.hdr, 0)
               : variable_length_btree_get_tuple_key(req->cfg, old_edge.hdr, 0);
         // need to add a new root
         variable_length_btree_alloc(req->cc,
                                     &req->mini,
                                     i,
                                     smallest_key,
                                     &req->next_extent,
                                     PAGE_TYPE_BRANCH,
                                     &req->edge[i]);
         variable_length_btree_pack_node_init_hdr(
            req->cfg, req->edge[i].hdr, req->next_extent, i);
         req->height++;
         platform_assert(req->height);

         // add old root and it's younger sibling
         bool succeeded =
            variable_length_btree_set_index_entry(req->cfg,
                                                  req->edge[i].hdr,
                                                  0,
                                                  smallest_key,
                                                  old_edge.addr,
                                                  req->num_tuples,
                                                  req->key_bytes,
                                                  req->message_bytes);
         platform_assert(succeeded);
         succeeded = variable_length_btree_set_index_entry(
            req->cfg, req->edge[i].hdr, 1, key, req->edge[i - 1].addr, 0, 0, 0);
         platform_assert(succeeded);
      }
      variable_length_btree_node_full_unlock(req->cc, req->cfg, &old_edge);
   }

#if defined(VARIABLE_LENGTH_BTREE_TRACE)
   if (variable_length_btree_key_compare(req->cfg, key, trace_key) == 0) {
      platform_log("adding tuple to %lu, root addr %lu\n",
                   req->edge[0].addr,
                   *req->root_addr);
   }
#endif

   for (uint16 i = 1; i <= req->height; i++) {
      index_entry *entry = variable_length_btree_get_index_entry(
         req->cfg,
         req->edge[i].hdr,
         variable_length_btree_num_entries(req->edge[i].hdr) - 1);
      entry->pivot_data.num_kvs_in_tree++;
      entry->pivot_data.key_bytes_in_tree += slice_length(key);
      entry->pivot_data.message_bytes_in_tree += slice_length(message);
   }

   if (req->hash) {
      req->fingerprint_arr[req->num_tuples] =
         req->hash(slice_data(key), slice_length(key), req->seed);
   }

   req->num_tuples++;
   req->key_bytes += slice_length(key);
   req->message_bytes += slice_length(message);

   iterator_advance(req->itor);
   iterator_at_end(req->itor, at_end);
}


static inline void
variable_length_btree_pack_post_loop(variable_length_btree_pack_req *req,
                                     slice                           last_key)
{
   cache *                       cc  = req->cc;
   variable_length_btree_config *cfg = req->cfg;
   // we want to use the allocation node, so we copy the root created in the
   // loop into the variable_length_btree_create root
   variable_length_btree_node root;

   // if output tree is empty, dec_ref the tree
   if (req->num_tuples == 0) {
      req->root_addr = 0;
      return;
   }

   root.addr = req->root_addr;
   variable_length_btree_node_get(cc, cfg, &root, PAGE_TYPE_BRANCH);

   __attribute__((unused)) bool success =
      variable_length_btree_node_claim(cc, cfg, &root);
   debug_assert(success);
   variable_length_btree_node_lock(cc, cfg, &root);
   memmove(root.hdr,
           req->edge[req->height].hdr,
           variable_length_btree_page_size(cfg));
   // fix the root next extent
   root.hdr->next_extent_addr = 0;
   variable_length_btree_node_full_unlock(cc, cfg, &root);

   // release all the edge nodes;
   for (uint16 i = 0; i <= req->height; i++) {
      // go back and fix the dangling next extents
      for (uint64 addr =
              variable_length_btree_get_extent_base_addr(cfg, &req->edge[i]);
           addr != req->edge[i].addr;
           addr += variable_length_btree_page_size(cfg))
      {
         variable_length_btree_node node = {.addr = addr};
         variable_length_btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);
         success = variable_length_btree_node_claim(cc, cfg, &node);
         debug_assert(success);
         variable_length_btree_node_lock(cc, cfg, &node);
         node.hdr->next_extent_addr = 0;
         variable_length_btree_node_full_unlock(cc, cfg, &node);
      }
      req->edge[i].hdr->next_extent_addr = 0;
      variable_length_btree_node_full_unlock(cc, cfg, &req->edge[i]);
   }

   mini_release(&req->mini, last_key);
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_pack --
 *
 *      Packs a variable_length_btree from an iterator source. Dec_Refs the
 *      output tree if it's empty.
 *-----------------------------------------------------------------------------
 */
platform_status
variable_length_btree_pack(variable_length_btree_pack_req *req)
{
   variable_length_btree_pack_setup_start(req);

   slice key = NULL_SLICE, data;
   bool  at_end;

   iterator_at_end(req->itor, &at_end);

   if (!at_end) {
      iterator_get_curr(req->itor, &key, &data);
      variable_length_btree_pack_setup_finish(req, key);
   }

   while (!at_end) {
      iterator_get_curr(req->itor, &key, &data);
      variable_length_btree_pack_loop(req, key, data, &at_end);
   }

   variable_length_btree_pack_post_loop(req, key);
   platform_assert(IMPLIES(req->num_tuples == 0, req->root_addr == 0));
   return STATUS_OK;
}

/*
 * Returns the number of kv pairs (k,v ) w/ k < key.  Also returns
 * the total size of all such keys and messages.
 */
static inline void
variable_length_btree_get_rank(cache *                       cc,
                               variable_length_btree_config *cfg,
                               uint64                        root_addr,
                               const slice                   key,
                               uint32 *                      kv_rank,
                               uint32 *                      key_bytes_rank,
                               uint32 *                      message_bytes_rank)
{
   variable_length_btree_node leaf;

   variable_length_btree_lookup_node(cc,
                                     cfg,
                                     root_addr,
                                     key,
                                     0,
                                     PAGE_TYPE_BRANCH,
                                     &leaf,
                                     kv_rank,
                                     key_bytes_rank,
                                     message_bytes_rank);
   bool  found;
   int64 tuple_rank_in_leaf =
      variable_length_btree_find_tuple(cfg, leaf.hdr, key, &found);
   if (!found) {
      tuple_rank_in_leaf++;
   }
   accumulate_node_ranks(cfg,
                         leaf.hdr,
                         0,
                         tuple_rank_in_leaf,
                         kv_rank,
                         key_bytes_rank,
                         message_bytes_rank);
   variable_length_btree_node_unget(cc, cfg, &leaf);
}

/*
 * count_in_range returns the exact number of tuples in the given
 * variable_length_btree between min_key (inc) and max_key (excl).
 */
void
variable_length_btree_count_in_range(cache *                       cc,
                                     variable_length_btree_config *cfg,
                                     uint64                        root_addr,
                                     const slice                   min_key,
                                     const slice                   max_key,
                                     uint32 *                      kv_rank,
                                     uint32 *key_bytes_rank,
                                     uint32 *message_bytes_rank)
{
   uint32 min_kv_rank;
   uint32 min_key_bytes_rank;
   uint32 min_message_bytes_rank;

   variable_length_btree_get_rank(cc,
                                  cfg,
                                  root_addr,
                                  min_key,
                                  &min_kv_rank,
                                  &min_key_bytes_rank,
                                  &min_message_bytes_rank);
   variable_length_btree_get_rank(cc,
                                  cfg,
                                  root_addr,
                                  slice_is_null(max_key) ? positive_infinity
                                                         : max_key,
                                  kv_rank,
                                  key_bytes_rank,
                                  message_bytes_rank);
   if (min_kv_rank < *kv_rank) {
      *kv_rank            = *kv_rank - min_kv_rank;
      *key_bytes_rank     = *key_bytes_rank - min_key_bytes_rank;
      *message_bytes_rank = *message_bytes_rank - min_message_bytes_rank;
   } else {
      *kv_rank            = 0;
      *key_bytes_rank     = 0;
      *message_bytes_rank = 0;
   }
}

/*
 * variable_length_btree_count_in_range_by_iterator perform
 * variable_length_btree_count_in_range using an iterator instead of by
 * calculating ranks. Used for debugging purposes.
 */
void
variable_length_btree_count_in_range_by_iterator(
   cache *                       cc,
   variable_length_btree_config *cfg,
   uint64                        root_addr,
   const slice                   min_key,
   const slice                   max_key,
   uint32 *                      kv_rank,
   uint32 *                      key_bytes_rank,
   uint32 *                      message_bytes_rank)
{
   variable_length_btree_iterator variable_length_btree_itor;
   iterator *                     itor = &variable_length_btree_itor.super;
   variable_length_btree_iterator_init(cc,
                                       cfg,
                                       &variable_length_btree_itor,
                                       root_addr,
                                       PAGE_TYPE_BRANCH,
                                       min_key,
                                       max_key,
                                       TRUE,
                                       0);

   *kv_rank            = 0;
   *key_bytes_rank     = 0;
   *message_bytes_rank = 0;

   bool at_end;
   iterator_at_end(itor, &at_end);
   while (!at_end) {
      slice key, message;
      iterator_get_curr(itor, &key, &message);
      *kv_rank            = *kv_rank + 1;
      *key_bytes_rank     = *key_bytes_rank + slice_length(key);
      *message_bytes_rank = *message_bytes_rank + slice_length(message);
      iterator_advance(itor);
      iterator_at_end(itor, &at_end);
   }
   variable_length_btree_iterator_deinit(&variable_length_btree_itor);
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_print_node --
 * variable_length_btree_print_tree --
 *
 *      Prints out the contents of the node/tree.
 *-----------------------------------------------------------------------------
 */
void
variable_length_btree_print_locked_node(variable_length_btree_config *cfg,
                                        uint64                        addr,
                                        variable_length_btree_hdr *   hdr,
                                        platform_stream_handle        stream)
{
   data_config *dcfg = cfg->data_cfg;

   platform_log_stream("*******************\n");
   if (variable_length_btree_height(hdr) > 0) {
      platform_log_stream("**  INDEX NODE \n");
      platform_log_stream("**  addr: %lu \n", addr);
      platform_log_stream("**  ptr: %p\n", hdr);
      platform_log_stream("**  next_addr: %lu \n", hdr->next_addr);
      platform_log_stream("**  next_extent_addr: %lu \n",
                          hdr->next_extent_addr);
      platform_log_stream("**  generation: %lu \n", hdr->generation);
      platform_log_stream("**  height: %u \n",
                          variable_length_btree_height(hdr));
      platform_log_stream("**  next_entry: %u \n", hdr->next_entry);
      platform_log_stream("**  num_entries: %u \n",
                          variable_length_btree_num_entries(hdr));
      platform_log_stream("-------------------\n");
      platform_log_stream("Table\n");
      for (uint64 i = 0; i < hdr->num_entries; i++) {
         platform_log_stream(
            "  %lu:%u\n", i, variable_length_btree_get_table_entry(hdr, i));
      }
      platform_log_stream("\n");
      platform_log_stream("-------------------\n");
      for (uint64 i = 0; i < variable_length_btree_num_entries(hdr); i++) {
         index_entry *entry =
            variable_length_btree_get_index_entry(cfg, hdr, i);
         platform_log_stream("%2lu:%s -- %lu (%u, %u, %u)\n",
                             i,
                             key_string(dcfg, index_entry_key_slice(entry)),
                             entry->pivot_data.child_addr,
                             entry->pivot_data.num_kvs_in_tree,
                             entry->pivot_data.key_bytes_in_tree,
                             entry->pivot_data.message_bytes_in_tree);
      }
      platform_log_stream("\n");
   } else {
      platform_log_stream("**  LEAF NODE \n");
      platform_log_stream("**  addr: %lu \n", addr);
      platform_log_stream("**  ptr: %p\n", hdr);
      platform_log_stream("**  next_addr: %lu \n", hdr->next_addr);
      platform_log_stream("**  next_extent_addr: %lu \n",
                          hdr->next_extent_addr);
      platform_log_stream("**  generation: %lu \n", hdr->generation);
      platform_log_stream("**  height: %u \n",
                          variable_length_btree_height(hdr));
      platform_log_stream("**  next_entry: %u \n", hdr->next_entry);
      platform_log_stream("**  num_entries: %u \n",
                          variable_length_btree_num_entries(hdr));
      platform_log_stream("-------------------\n");
      for (uint64 i = 0; i < variable_length_btree_num_entries(hdr); i++) {
         platform_log_stream(
            "%lu:%u ", i, variable_length_btree_get_table_entry(hdr, i));
      }
      platform_log_stream("\n");
      platform_log_stream("-------------------\n");
      for (uint64 i = 0; i < variable_length_btree_num_entries(hdr); i++) {
         leaf_entry *entry = variable_length_btree_get_leaf_entry(cfg, hdr, i);
         platform_log_stream(
            "%2lu:%s -- %s\n",
            i,
            key_string(dcfg, leaf_entry_key_slice(entry)),
            message_string(dcfg, leaf_entry_message_slice(entry)));
      }
      platform_log_stream("-------------------\n");
      platform_log_stream("\n");
   }
}

void
variable_length_btree_print_node(cache *                       cc,
                                 variable_length_btree_config *cfg,
                                 variable_length_btree_node *  node,
                                 platform_stream_handle        stream)
{
   if (!cache_page_valid(cc, node->addr)) {
      platform_log_stream("*******************\n");
      platform_log_stream("** INVALID NODE \n");
      platform_log_stream("** addr: %lu \n", node->addr);
      platform_log_stream("-------------------\n");
      return;
   }
   variable_length_btree_node_get(cc, cfg, node, PAGE_TYPE_BRANCH);
   variable_length_btree_print_locked_node(cfg, node->addr, node->hdr, stream);
   variable_length_btree_node_unget(cc, cfg, node);
}

void
variable_length_btree_print_subtree(cache *                       cc,
                                    variable_length_btree_config *cfg,
                                    uint64                        addr,
                                    platform_stream_handle        stream)
{
   variable_length_btree_node node;
   node.addr = addr;
   variable_length_btree_print_node(cc, cfg, &node, stream);
   if (!cache_page_valid(cc, node.addr)) {
      return;
   }
   variable_length_btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);
   table_index idx;

   if (node.hdr->height > 0) {
      for (idx = 0; idx < node.hdr->num_entries; idx++) {
         variable_length_btree_print_subtree(
            cc,
            cfg,
            variable_length_btree_get_child_addr(cfg, node.hdr, idx),
            stream);
      }
   }
   variable_length_btree_node_unget(cc, cfg, &node);
}

void
variable_length_btree_print_tree(cache *                       cc,
                                 variable_length_btree_config *cfg,
                                 uint64                        root_addr)
{
   platform_open_log_stream();
   variable_length_btree_print_subtree(cc, cfg, root_addr, stream);
   platform_close_log_stream(PLATFORM_DEFAULT_LOG_HANDLE);
}

void
variable_length_btree_print_tree_stats(cache *                       cc,
                                       variable_length_btree_config *cfg,
                                       uint64                        addr)
{
   variable_length_btree_node node;
   node.addr = addr;
   variable_length_btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);

   platform_log("Tree stats: height %u\n", node.hdr->height);
   cache_print_stats(cc);

   variable_length_btree_node_unget(cc, cfg, &node);
}

/*
 * returns the space used in bytes by the range [start_key, end_key) in the
 * variable_length_btree
 */
uint64
variable_length_btree_space_use_in_range(cache *                       cc,
                                         variable_length_btree_config *cfg,
                                         uint64      root_addr,
                                         page_type   type,
                                         const slice start_key,
                                         const slice end_key)
{
   uint64 meta_head =
      variable_length_btree_root_to_meta_addr(cfg, root_addr, 0);
   uint64 extents_used = mini_keyed_extent_count(
      cc, cfg->data_cfg, type, meta_head, start_key, end_key);
   return extents_used * cfg->extent_size;
}

bool
variable_length_btree_verify_node(cache *                       cc,
                                  variable_length_btree_config *cfg,
                                  uint64                        addr,
                                  page_type                     type,
                                  bool                          is_left_edge)
{
   variable_length_btree_node node;
   node.addr = addr;
   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   variable_length_btree_node_get(cc, cfg, &node, type);
   table_index idx;
   bool        result = FALSE;

   platform_open_log_stream();
   for (idx = 0; idx < node.hdr->num_entries; idx++) {
      if (node.hdr->height == 0) {
         // leaf node
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (variable_length_btree_key_compare(
                   cfg,
                   variable_length_btree_get_tuple_key(cfg, node.hdr, idx),
                   variable_length_btree_get_tuple_key(
                      cfg, node.hdr, idx + 1)) >= 0) {
               platform_log_stream("out of order tuples\n");
               platform_log_stream("addr: %lu idx %2u\n", node.addr, idx);
               variable_length_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
      } else {
         // index node
         variable_length_btree_node child;
         child.addr = variable_length_btree_get_child_addr(cfg, node.hdr, idx);
         variable_length_btree_node_get(cc, cfg, &child, type);
         if (child.hdr->height != node.hdr->height - 1) {
            platform_log_stream("height mismatch\n");
            platform_log_stream("addr: %lu idx: %u\n", node.addr, idx);
            variable_length_btree_node_unget(cc, cfg, &child);
            variable_length_btree_node_unget(cc, cfg, &node);
            goto out;
         }
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (variable_length_btree_key_compare(
                   cfg,
                   variable_length_btree_get_pivot(cfg, node.hdr, idx),
                   variable_length_btree_get_pivot(cfg, node.hdr, idx + 1)) >=
                0) {
               variable_length_btree_node_unget(cc, cfg, &child);
               variable_length_btree_node_unget(cc, cfg, &node);
               variable_length_btree_print_tree(cc, cfg, addr);
               platform_log_stream("out of order pivots\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (0 < idx &&
                variable_length_btree_key_compare(
                   cfg,
                   variable_length_btree_get_pivot(cfg, node.hdr, idx),
                   variable_length_btree_get_tuple_key(cfg, child.hdr, 0)) !=
                   0) {
               platform_log_stream(
                  "pivot key doesn't match in child and parent\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu\n", child.addr);
               variable_length_btree_node_unget(cc, cfg, &child);
               variable_length_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (idx != variable_length_btree_num_entries(node.hdr) - 1 &&
                variable_length_btree_key_compare(
                   cfg,
                   variable_length_btree_get_pivot(cfg, node.hdr, idx + 1),
                   variable_length_btree_get_tuple_key(
                      cfg,
                      child.hdr,
                      variable_length_btree_num_entries(child.hdr) - 1)) < 0) {
               platform_log_stream("child tuple larger than parent bound\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               variable_length_btree_print_locked_node(
                  cfg, node.addr, node.hdr, PLATFORM_ERR_LOG_HANDLE);
               variable_length_btree_print_locked_node(
                  cfg, child.addr, child.hdr, PLATFORM_ERR_LOG_HANDLE);
               platform_assert(0);
               variable_length_btree_node_unget(cc, cfg, &child);
               variable_length_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         } else {
            // child index
            if (idx != variable_length_btree_num_entries(node.hdr) - 1 &&
                variable_length_btree_key_compare(
                   cfg,
                   variable_length_btree_get_pivot(cfg, node.hdr, idx + 1),
                   variable_length_btree_get_pivot(
                      cfg,
                      child.hdr,
                      variable_length_btree_num_entries(child.hdr) - 1)) < 0) {
               platform_log_stream("child pivot larger than parent bound\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               variable_length_btree_print_locked_node(
                  cfg, node.addr, node.hdr, PLATFORM_ERR_LOG_HANDLE);
               variable_length_btree_print_locked_node(
                  cfg, child.addr, child.hdr, PLATFORM_ERR_LOG_HANDLE);
               platform_assert(0);
               variable_length_btree_node_unget(cc, cfg, &child);
               variable_length_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         variable_length_btree_node_unget(cc, cfg, &child);
         bool child_is_left_edge = is_left_edge && idx == 0;
         if (!variable_length_btree_verify_node(
                cc, cfg, child.addr, type, child_is_left_edge)) {
            variable_length_btree_node_unget(cc, cfg, &node);
            goto out;
         }
      }
   }
   variable_length_btree_node_unget(cc, cfg, &node);
   result = TRUE;
out:
   platform_close_log_stream(PLATFORM_ERR_LOG_HANDLE);

   return result;
}

bool
variable_length_btree_verify_tree(cache *                       cc,
                                  variable_length_btree_config *cfg,
                                  uint64                        addr,
                                  page_type                     type)
{
   return variable_length_btree_verify_node(cc, cfg, addr, type, TRUE);
}

void
variable_length_btree_print_lookup(cache *                       cc,  // IN
                                   variable_length_btree_config *cfg, // IN
                                   uint64      root_addr,             // IN
                                   page_type   type,                  // IN
                                   const slice key)                   // IN
{
   variable_length_btree_node node, child_node;
   uint32                     h;
   int64                      child_idx;

   node.addr = root_addr;
   variable_length_btree_print_node(
      cc, cfg, &node, PLATFORM_DEFAULT_LOG_HANDLE);
   variable_length_btree_node_get(cc, cfg, &node, type);

   for (h = node.hdr->height; h > 0; h--) {
      bool found;
      child_idx = variable_length_btree_find_pivot(cfg, node.hdr, key, &found);
      if (child_idx < 0) {
         child_idx = 0;
      }
      child_node.addr =
         variable_length_btree_get_child_addr(cfg, node.hdr, child_idx);
      variable_length_btree_print_node(
         cc, cfg, &child_node, PLATFORM_DEFAULT_LOG_HANDLE);
      variable_length_btree_node_get(cc, cfg, &child_node, type);
      variable_length_btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   bool  found;
   int64 idx = variable_length_btree_find_tuple(cfg, node.hdr, key, &found);
   platform_log(
      "Matching index: %lu (%d) of %u\n", idx, found, node.hdr->num_entries);
   variable_length_btree_node_unget(cc, cfg, &node);
}

/*
 *-----------------------------------------------------------------------------
 * variable_length_btree_config_init --
 *
 *      Initialize variable_length_btree config values
 *-----------------------------------------------------------------------------
 */
void
variable_length_btree_config_init(
   variable_length_btree_config *variable_length_btree_cfg,
   data_config *                 data_cfg,
   uint64                        rough_count_height,
   uint64                        page_size,
   uint64                        extent_size)
{
   variable_length_btree_cfg->data_cfg = data_cfg;

   variable_length_btree_cfg->page_size          = page_size;
   variable_length_btree_cfg->extent_size        = extent_size;
   variable_length_btree_cfg->rough_count_height = rough_count_height;
}
