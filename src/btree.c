// Copyright 2018-2021 VMware, Inc. All rights reserved. -- VMware Confidential
// SPDX-License-Identifier: Apache-2.0

#include "btree_private.h"
#include "poison.h"

/*
 * *****************************************************************
 * Structure of a BTree node: Disk-resident structure:
 *
 *                                 hdr->next_entry
 *                                               |
 *   0                                           v     page_size
 *   -----------------------------------------------------------
 *   | header | offsets table ---> | empty space | <--- entries|
 *   -----------------------------------------------------------
 *
 *  header: struct btree_hdr{}
 *  entry : struct leaf_entry{}
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
 * free-space fragmentation.
 *
 * When a node runs out of free space, we measure its dead space.
 * If dead space is:
 *  - below a threshold, we split the node.
 *  - above the threshold, then we defragment the node instead of splitting it.
 * *****************************************************************
 */

/*
 * *****************************************************************
 * Locking rules for BTree:
 *    1. Locks must be acquired in the following order: read->claim->write
 *    2. If a thread holds two locks, it must hold the lock that dominates
 *       both locks. For instance, if it has locked two children, we must
 *       lock their parent.
 *    3. Threads may traverse from one node to the next without acquiring
 *       a lock upon the node that dominates them by first releasing the
 *       held lock and then taking a leap of faith by acquiring the lock
 *       on the second.
 *    4. They may also traverse down the tree to a single leaf using hand
 *       over hand locking.
 *    5. All threads follow the locking patterns in 3 or 4. They only hold
 *       a single lock at a time.
 *
 * Exceptions to these rules:
 *    1. find_btree_node_and_get_idx_bounds(): To find the end_idx of the
 *       range iterator, we acquire a read lock on the leaf which holds
 *       the max_key. However, at the same time we hold a claim on the
 *       current leaf. We may not be holding the node that dominates these
 *       two leaves.
 *    2. btree_split_child_leaf(): When splitting a leaf we hold write locks
 *       on the leaf we're splitting, its parent, and its original next leaf.
 *       However, this next leaf may have a different parent than the leaf
 *       we split.
 *
 * Why are these exceptions okay:
 *    Because by (5) we know that every other thread is holding only a single
 *    lock or is either an iterator finding the end_idx or performing a split.
 *    In either of these exception cases, we always acquire locks in increasing
 *    leaf order, thus, a thread will never hold a lock while attempting to
 *    acquire a lock on a previous leaf. As such, we can always safely wait for
 *    other threads to complete their work.
 * *****************************************************************
 */

/* Threshold for splitting instead of defragmenting. */
#define BTREE_SPLIT_THRESHOLD(page_size) ((page_size) / 2)

/*
 * After a split, the free space in the left node may be fragmented.
 * If there's less than this much contiguous free space, then we also
 * defrag the left node.
 */
#define BTREE_DEFRAGMENT_THRESHOLD(page_size) ((page_size) / 4)

/*
 * Branches keep track of the number of keys and the total size of
 * all keys and messages in their subtrees.  But memtables do not
 * (because it is difficult to maintain this information during
 * insertion).  However, the current implementation uses the same
 * data structure for both memtables and branches.  So memtables
 * store BTREE_UNKNOWN_COUNTER for these counters.
 */
#define BTREE_UNKNOWN_COUNTER (0x7fffffffUL)
static const btree_pivot_stats BTREE_PIVOT_STATS_UNKNOWN = {
   BTREE_UNKNOWN_COUNTER,
   BTREE_UNKNOWN_COUNTER,
   BTREE_UNKNOWN_COUNTER};


static inline uint8
btree_height(const btree_hdr *hdr)
{
   return hdr->height;
}

static inline table_entry
btree_get_table_entry(btree_hdr *hdr, int i)
{
   debug_assert(i < hdr->num_entries);
   return hdr->offsets[i];
}

static inline table_index
btree_num_entries(const btree_hdr *hdr)
{
   return hdr->num_entries;
}

static inline void
btree_increment_height(btree_hdr *hdr)
{
   hdr->height++;
}

static inline void
btree_reset_node_entries(const btree_config *cfg, btree_hdr *hdr)
{
   hdr->num_entries = 0;
   hdr->next_entry  = btree_page_size(cfg);
}


static inline uint64
index_entry_required_capacity(key pivot)
{
   return sizeof(index_entry) + key_length(pivot);
}

static inline uint64
leaf_entry_required_capacity(key tuple_key, const message msg)
{
   return sizeof(leaf_entry) + key_length(tuple_key) + message_length(msg);
}

static inline uint64
leaf_entry_key_size(const leaf_entry *entry)
{
   return entry->key_length;
}


static inline uint64
leaf_entry_message_size(const leaf_entry *entry)
{
   return entry->message_length;
}

/*********************************************************
 * Code for tracing operations involving a particular key
 *********************************************************/

// #define BTREE_KEY_TRACING

#ifdef BTREE_KEY_TRACING
static char trace_key[24] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

void
log_trace_key(key k, char *msg)
{
   if (key_lex_cmp(k, key_create(sizeof(trace_key), trace_key)) == 0) {
      platform_default_log("BTREE_TRACE_KEY: %s\n", msg);
   }
}

/* Output msg if this leaf contains the trace_key */
void
log_trace_leaf(const btree_config *cfg, const btree_hdr *hdr, char *msg)
{
   for (int i = 0; i < hdr->num_entries; i++) {
      key tuple_key = btree_get_tuple_key(cfg, hdr, i);
      log_trace_key(tuple_key, msg);
   }
}
#else
#   define log_trace_key(key, msg)
#   define log_trace_leaf(cfg, hdr, msg)
#endif /* BTREE_KEY_TRACING */


/**************************************
 * Basic get/set on index nodes
 **************************************/

static inline void
btree_fill_index_entry(const btree_config *cfg,
                       btree_hdr          *hdr,
                       index_entry        *entry,
                       key                 new_pivot_key,
                       uint64              new_addr,
                       btree_pivot_stats   stats)
{
   debug_assert((void *)hdr <= (void *)entry);
   debug_assert(diff_ptr(hdr, entry)
                   + index_entry_required_capacity(new_pivot_key)
                <= btree_page_size(cfg));
   copy_key_to_ondisk_key(&entry->pivot, new_pivot_key);
   entry->pivot_data.child_addr = new_addr;
   entry->pivot_data.stats      = stats;
}

bool32
btree_set_index_entry(const btree_config *cfg,
                      btree_hdr          *hdr,
                      table_index         k,
                      key                 new_pivot_key,
                      uint64              new_addr,
                      btree_pivot_stats   stats)
{
   platform_assert(
      k <= hdr->num_entries, "k=%d, num_entries=%d\n", k, hdr->num_entries);
   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k + 1;

   if (k < hdr->num_entries) {
      index_entry *old_entry = btree_get_index_entry(cfg, hdr, k);
      if (hdr->next_entry == diff_ptr(hdr, old_entry)
          && (diff_ptr(hdr, &hdr->offsets[new_num_entries])
                 + index_entry_required_capacity(new_pivot_key)
              <= hdr->next_entry + sizeof_index_entry(old_entry)))
      {
         /* special case to avoid creating fragmentation:
          * the old entry is the physically first entry in the node
          * and the new entry will fit in the space avaiable from the old
          * entry plus the free space preceding the old_entry.
          * In this case, just reset next_entry so we can insert the new entry.
          */
         hdr->next_entry += sizeof_index_entry(old_entry);
      } else if (index_entry_required_capacity(new_pivot_key)
                 <= sizeof_index_entry(old_entry))
      {
         /* old_entry is not the physically first in the node,
          * but new entry will fit inside it.
          */
         btree_fill_index_entry(
            cfg, hdr, old_entry, new_pivot_key, new_addr, stats);
         return TRUE;
      }
      /* Fall through */
   }

   if (hdr->next_entry < diff_ptr(hdr, &hdr->offsets[new_num_entries])
                            + index_entry_required_capacity(new_pivot_key))
   {
      return FALSE;
   }

   index_entry *new_entry = pointer_byte_offset(
      hdr, hdr->next_entry - index_entry_required_capacity(new_pivot_key));
   btree_fill_index_entry(cfg, hdr, new_entry, new_pivot_key, new_addr, stats);

   hdr->offsets[k]  = diff_ptr(hdr, new_entry);
   hdr->num_entries = new_num_entries;
   hdr->next_entry  = diff_ptr(hdr, new_entry);
   return TRUE;
}

static inline bool32
btree_insert_index_entry(const btree_config *cfg,
                         btree_hdr          *hdr,
                         uint32              k,
                         key                 new_pivot_key,
                         uint64              new_addr,
                         btree_pivot_stats   stats)
{
   bool32 succeeded = btree_set_index_entry(
      cfg, hdr, hdr->num_entries, new_pivot_key, new_addr, stats);
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
btree_fill_leaf_entry(const btree_config *cfg,
                      btree_hdr          *hdr,
                      leaf_entry         *entry,
                      key                 tuple_key,
                      message             msg)
{
   debug_assert(
      pointer_byte_offset(entry, leaf_entry_required_capacity(tuple_key, msg))
      <= pointer_byte_offset(hdr, btree_page_size(cfg)));
   copy_tuple_to_ondisk_tuple(entry, tuple_key, msg);
   debug_assert(ondisk_tuple_message_class(entry) == message_class(msg),
                "entry->type not large enough to hold message_class");
}

static inline bool32
btree_can_set_leaf_entry(const btree_config *cfg,
                         const btree_hdr    *hdr,
                         table_index         k,
                         key                 new_key,
                         message             new_message)
{
   if (hdr->num_entries < k)
      return FALSE;

   if (k < hdr->num_entries) {
      leaf_entry *old_entry = btree_get_leaf_entry(cfg, hdr, k);
      if (leaf_entry_required_capacity(new_key, new_message)
          <= sizeof_leaf_entry(old_entry))
      {
         return TRUE;
      }
      /* Fall through */
   }

   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k + 1;
   if (hdr->next_entry
       < diff_ptr(hdr, &hdr->offsets[new_num_entries])
            + leaf_entry_required_capacity(new_key, new_message))
   {
      return FALSE;
   }

   return TRUE;
}

bool32
btree_set_leaf_entry(const btree_config *cfg,
                     btree_hdr          *hdr,
                     table_index         k,
                     key                 new_key,
                     message             new_message)
{
   if (k < hdr->num_entries) {
      leaf_entry *old_entry = btree_get_leaf_entry(cfg, hdr, k);
      if (leaf_entry_required_capacity(new_key, new_message)
          <= sizeof_leaf_entry(old_entry))
      {
         btree_fill_leaf_entry(cfg, hdr, old_entry, new_key, new_message);
         return TRUE;
      }
      /* Fall through */
   }

   platform_assert(k <= hdr->num_entries);
   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k + 1;
   if (hdr->next_entry
       < diff_ptr(hdr, &hdr->offsets[new_num_entries])
            + leaf_entry_required_capacity(new_key, new_message))
   {
      return FALSE;
   }

   leaf_entry *new_entry = pointer_byte_offset(
      hdr,
      hdr->next_entry - leaf_entry_required_capacity(new_key, new_message));
   platform_assert(
      (void *)&hdr->offsets[new_num_entries] <= (void *)new_entry,
      "Offset addr 0x%p for index, new_num_entries=%lu is incorrect."
      " It should be <= new_entry=0x%p\n",
      &hdr->offsets[new_num_entries],
      new_num_entries,
      new_entry);
   btree_fill_leaf_entry(cfg, hdr, new_entry, new_key, new_message);

   hdr->offsets[k]  = diff_ptr(hdr, new_entry);
   hdr->num_entries = new_num_entries;
   hdr->next_entry  = diff_ptr(hdr, new_entry);
   platform_assert(0 < hdr->num_entries);

   return TRUE;
}

static inline bool32
btree_insert_leaf_entry(const btree_config *cfg,
                        btree_hdr          *hdr,
                        table_index         k,
                        key                 new_key,
                        message             new_message)
{
   debug_assert(k <= hdr->num_entries);
   bool32 succeeded =
      btree_set_leaf_entry(cfg, hdr, hdr->num_entries, new_key, new_message);
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
 * btree_find_pivot --
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
 * The C code below is a translation of the following verified Dafny
 * implementation.

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
btree_find_pivot(const btree_config *cfg,
                 const btree_hdr    *hdr,
                 key                 target,
                 bool32             *found)
{
   int64 lo = 0, hi = btree_num_entries(hdr);

   debug_assert(!key_is_null(target));

   *found = FALSE;

   while (lo < hi) {
      int64 mid = (lo + hi) / 2;
      int cmp = btree_key_compare(cfg, btree_get_pivot(cfg, hdr, mid), target);
      if (cmp == 0) {
         *found = TRUE;
         return mid;
      } else if (cmp < 0) {
         lo = mid + 1;
      } else {
         hi = mid;
      }
   }

   return lo - 1;
}

/*
 *-----------------------------------------------------------------------------
 * btree_find_tuple --
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
 * The C code below is a translation of the same Dafny implementation as above.
 */
static inline int64
btree_find_tuple(const btree_config *cfg,
                 const btree_hdr    *hdr,
                 key                 target,
                 bool32             *found)
{
   int64 lo = 0, hi = btree_num_entries(hdr);

   *found = FALSE;

   while (lo < hi) {
      int64 mid = (lo + hi) / 2;
      int   cmp =
         btree_key_compare(cfg, btree_get_tuple_key(cfg, hdr, mid), target);
      if (cmp == 0) {
         *found = TRUE;
         return mid;
      } else if (cmp < 0) {
         lo = mid + 1;
      } else {
         hi = mid;
      }
   }

   return lo - 1;
}

/*
 *-----------------------------------------------------------------------------
 * btree_leaf_incorporate_tuple
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
btree_merge_tuples(const btree_config *cfg,
                   key                 tuple_key,
                   message             old_data,
                   merge_accumulator  *new_data)
{
   return data_merge_tuples(cfg->data_cfg, tuple_key, old_data, new_data);
}

static message
spec_message(const leaf_incorporate_spec *spec)
{
   if (spec->old_entry_state == ENTRY_DID_NOT_EXIST) {
      return spec->msg.new_message;
   } else {
      return merge_accumulator_to_message(&spec->msg.merged_message);
   }
}

platform_status
btree_create_leaf_incorporate_spec(const btree_config    *cfg,
                                   platform_heap_id       heap_id,
                                   btree_hdr             *hdr,
                                   key                    tuple_key,
                                   message                msg,
                                   leaf_incorporate_spec *spec)
{
   spec->tuple_key = tuple_key;
   bool32 found;
   spec->idx             = btree_find_tuple(cfg, hdr, tuple_key, &found);
   spec->old_entry_state = found ? ENTRY_STILL_EXISTS : ENTRY_DID_NOT_EXIST;
   if (!found) {
      spec->msg.new_message = msg;
      spec->idx++;
      return STATUS_OK;
   } else {
      leaf_entry *entry      = btree_get_leaf_entry(cfg, hdr, spec->idx);
      message     oldmessage = leaf_entry_message(entry);
      bool32      success;
      success = merge_accumulator_init_from_message(
         &spec->msg.merged_message, heap_id, msg);
      if (!success) {
         return STATUS_NO_MEMORY;
      }
      if (btree_merge_tuples(
             cfg, tuple_key, oldmessage, &spec->msg.merged_message)) {
         merge_accumulator_deinit(&spec->msg.merged_message);
         return STATUS_NO_MEMORY;
      } else {
         return STATUS_OK;
      }
   }
}

void
destroy_leaf_incorporate_spec(leaf_incorporate_spec *spec)
{
   if (spec->old_entry_state != ENTRY_DID_NOT_EXIST) {
      merge_accumulator_deinit(&spec->msg.merged_message);
   }
}

static inline bool32
btree_can_perform_leaf_incorporate_spec(const btree_config          *cfg,
                                        btree_hdr                   *hdr,
                                        const leaf_incorporate_spec *spec)
{
   if (spec->old_entry_state == ENTRY_DID_NOT_EXIST) {
      return btree_can_set_leaf_entry(cfg,
                                      hdr,
                                      btree_num_entries(hdr),
                                      spec->tuple_key,
                                      spec->msg.new_message);
   } else if (spec->old_entry_state == ENTRY_STILL_EXISTS) {
      message merged = merge_accumulator_to_message(&spec->msg.merged_message);
      return btree_can_set_leaf_entry(
         cfg, hdr, spec->idx, spec->tuple_key, merged);
   } else {
      debug_assert(spec->old_entry_state == ENTRY_HAS_BEEN_REMOVED);
      message merged = merge_accumulator_to_message(&spec->msg.merged_message);
      return btree_can_set_leaf_entry(
         cfg, hdr, btree_num_entries(hdr), spec->tuple_key, merged);
   }
}

bool32
btree_try_perform_leaf_incorporate_spec(const btree_config          *cfg,
                                        btree_hdr                   *hdr,
                                        const leaf_incorporate_spec *spec,
                                        uint64                      *generation)
{
   bool32 success;
   switch (spec->old_entry_state) {
      case ENTRY_DID_NOT_EXIST:
         success = btree_insert_leaf_entry(
            cfg, hdr, spec->idx, spec->tuple_key, spec->msg.new_message);
         break;
      case ENTRY_STILL_EXISTS:
      {
         message merged =
            merge_accumulator_to_message(&spec->msg.merged_message);
         success =
            btree_set_leaf_entry(cfg, hdr, spec->idx, spec->tuple_key, merged);
         break;
      }
      case ENTRY_HAS_BEEN_REMOVED:
      {
         message merged =
            merge_accumulator_to_message(&spec->msg.merged_message);
         success = btree_insert_leaf_entry(
            cfg, hdr, spec->idx, spec->tuple_key, merged);
         break;
      }
      default:
         platform_assert(
            FALSE,
            "Unknown btree leaf_incorporate_spec->old_entry_state %d",
            spec->old_entry_state);
   }

   if (success) {
      *generation = hdr->generation++;
   }
   return success;
}

/*
 *-----------------------------------------------------------------------------
 * btree_defragment_leaf --
 *
 *      Defragment a node.  If spec != NULL, then we also remove the old
 *      entry that will be replaced by the insert, if such an old entry exists.
 *
 *      If spec is NULL or if no old entry exists, then we just defrag the node.
 *-----------------------------------------------------------------------------
 */
void
btree_defragment_leaf(const btree_config    *cfg, // IN
                      btree_scratch         *scratch,
                      btree_hdr             *hdr,
                      leaf_incorporate_spec *spec) // IN/OUT
{
   btree_hdr *scratch_hdr = (btree_hdr *)scratch->defragment_node.scratch_node;
   memcpy(scratch_hdr, hdr, btree_page_size(cfg));
   btree_reset_node_entries(cfg, hdr);
   uint64 dst_idx = 0;
   for (int64 i = 0; i < btree_num_entries(scratch_hdr); i++) {
      if (spec && spec->old_entry_state == ENTRY_STILL_EXISTS && spec->idx == i)
      {
         spec->old_entry_state = ENTRY_HAS_BEEN_REMOVED;
      } else {
         leaf_entry       *entry = btree_get_leaf_entry(cfg, scratch_hdr, i);
         debug_only bool32 success =
            btree_set_leaf_entry(cfg,
                                 hdr,
                                 dst_idx++,
                                 leaf_entry_key(entry),
                                 leaf_entry_message(entry));
         debug_assert(success);
      }
   }
}

static inline void
btree_truncate_leaf(const btree_config *cfg, // IN
                    btree_hdr          *hdr, // IN
                    uint64              target_entries)   // IN
{
   uint64 new_next_entry = btree_page_size(cfg);

   for (uint64 i = 0; i < target_entries; i++) {
      if (hdr->offsets[i] < new_next_entry)
         new_next_entry = hdr->offsets[i];
   }

   hdr->num_entries = target_entries;
   hdr->next_entry  = new_next_entry;
}

/*
 *-----------------------------------------------------------------------------
 * btree_split_leaf --
 *
 *      Splits the node at left_addr into a new node at right_addr.
 *
 *      Assumes write lock on both nodes.
 *-----------------------------------------------------------------------------
 */

static leaf_splitting_plan initial_plan = {0, FALSE};


static bool32
most_of_entry_is_on_left_side(uint64 total_bytes,
                              uint64 left_bytes,
                              uint64 entry_size)
{
   return left_bytes + sizeof(table_entry) + entry_size
          < (total_bytes + sizeof(table_entry) + entry_size) / 2;
}

/*
 * ----------------------------------------------------------------------------
 * Figure out how many entries we can put on the left side.
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
 * ----------------------------------------------------------------------------
 */
static uint64
plan_move_more_entries_to_left(const btree_config  *cfg,
                               const btree_hdr     *hdr,
                               uint64               max_entries,
                               uint64               total_bytes,
                               uint64               left_bytes,
                               leaf_splitting_plan *plan) // IN/OUT
{
   leaf_entry *entry;
   while (plan->split_idx < max_entries
          && (entry = btree_get_leaf_entry(cfg, hdr, plan->split_idx))
          && most_of_entry_is_on_left_side(
             total_bytes, left_bytes, sizeof_leaf_entry(entry)))
   {
      left_bytes += sizeof(table_entry) + sizeof_leaf_entry(entry);
      plan->split_idx++;
   }
   return left_bytes;
}

/*
 * ----------------------------------------------------------------------------
 * Choose a splitting point so that we are guaranteed to be able to
 * insert the given key-message pair into the correct node after the
 * split. Assumes all leaf entries are at most half the total free
 * space in an empty leaf.
 * ----------------------------------------------------------------------------
 */
leaf_splitting_plan
btree_build_leaf_splitting_plan(const btree_config          *cfg, // IN
                                const btree_hdr             *hdr,
                                const leaf_incorporate_spec *spec) // IN
{
   /* Split the content by bytes -- roughly half the bytes go to the
    * right node.  So count the bytes, including the new entry to be
    * inserted.
    */
   uint64 num_entries = btree_num_entries(hdr);
   uint64 entry_size =
      leaf_entry_required_capacity(spec->tuple_key, spec_message(spec));
   uint64 total_bytes = entry_size;

   for (uint64 i = 0; i < num_entries; i++) {
      if (i != spec->idx || spec->old_entry_state != ENTRY_STILL_EXISTS) {
         leaf_entry *entry = btree_get_leaf_entry(cfg, hdr, i);
         total_bytes += sizeof_leaf_entry(entry);
      }
   }
   uint64 new_num_entries = num_entries;
   new_num_entries += spec->old_entry_state == ENTRY_STILL_EXISTS ? 0 : 1;
   total_bytes += new_num_entries * sizeof(table_entry);

   /* Now figure out the number of entries to move, and figure out how
    * much free space will be created in the left_hdr by the split.
    */
   uint64              left_bytes = 0;
   leaf_splitting_plan plan       = initial_plan;

   /* Figure out how many of the items to the left of spec.idx can be
    * put into the left node.
    */
   left_bytes = plan_move_more_entries_to_left(
      cfg, hdr, spec->idx, total_bytes, left_bytes, &plan);

   /* Figure out whether our new entry can go into the left node.  If it
    * can't, then no subsequent entries can, either, so we're done.
    */
   if (plan.split_idx == spec->idx
       && most_of_entry_is_on_left_side(total_bytes, left_bytes, entry_size))
   {
      left_bytes += sizeof(table_entry) + entry_size;
      plan.insertion_goes_left = TRUE;
   } else {
      return plan;
   }
   if (spec->old_entry_state == ENTRY_STILL_EXISTS) {
      /* If our new entry is replacing an existing entry, then skip
       * that entry in our planning.
       */
      plan.split_idx++;
   }

   /* Figure out how many more entries after spec.idx can go into the
    * left node.
    */
   plan_move_more_entries_to_left(
      cfg, hdr, num_entries, total_bytes, left_bytes, &plan);

   return plan;
}

static inline key
btree_splitting_pivot(const btree_config          *cfg, // IN
                      const btree_hdr             *hdr,
                      const leaf_incorporate_spec *spec,
                      leaf_splitting_plan          plan)
{
   if (plan.split_idx == spec->idx
       && spec->old_entry_state != ENTRY_STILL_EXISTS
       && !plan.insertion_goes_left)
   {
      return spec->tuple_key;
   } else {
      return btree_get_tuple_key(cfg, hdr, plan.split_idx);
   }
}

static inline void
btree_split_leaf_build_right_node(const btree_config    *cfg,       // IN
                                  const btree_hdr       *left_hdr,  // IN
                                  uint64                 left_addr, // IN
                                  leaf_incorporate_spec *spec,      // IN
                                  leaf_splitting_plan    plan,      // IN
                                  btree_hdr             *right_hdr,
                                  uint64                *generation) // IN/OUT
{
   /* Build the right node. */
   memmove(right_hdr, left_hdr, sizeof(*right_hdr));
   right_hdr->generation++;
   right_hdr->prev_addr = left_addr;
   btree_reset_node_entries(cfg, right_hdr);
   uint64 num_left_entries = btree_num_entries(left_hdr);
   uint64 dst_idx          = 0;
   for (uint64 i = plan.split_idx; i < num_left_entries; i++) {
      if (spec->old_entry_state == ENTRY_STILL_EXISTS && i == spec->idx) {
         spec->old_entry_state = ENTRY_HAS_BEEN_REMOVED;
      } else {
         leaf_entry *entry = btree_get_leaf_entry(cfg, left_hdr, i);
         btree_set_leaf_entry(cfg,
                              right_hdr,
                              dst_idx,
                              leaf_entry_key(entry),
                              leaf_entry_message(entry));
         dst_idx++;
      }
   }

   if (!plan.insertion_goes_left) {
      spec->idx -= plan.split_idx;
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, right_hdr, spec, generation);
      platform_assert(incorporated);
   }
}

static inline void
btree_split_leaf_cleanup_left_node(const btree_config    *cfg, // IN
                                   btree_scratch         *scratch,
                                   btree_hdr             *left_hdr, // IN
                                   leaf_incorporate_spec *spec,     // IN
                                   leaf_splitting_plan    plan,
                                   uint64                 right_addr) // IN
{
   left_hdr->next_addr = right_addr;
   btree_truncate_leaf(cfg, left_hdr, plan.split_idx);
   left_hdr->generation++;
   if (plan.insertion_goes_left
       && !btree_can_perform_leaf_incorporate_spec(cfg, left_hdr, spec))
   {
      btree_defragment_leaf(cfg, scratch, left_hdr, spec);
   }
}

/*
 *-----------------------------------------------------------------------------
 * btree_split_index --
 *
 *      Splits the node at left_addr into a new node at right_addr.
 *
 *      Assumes write lock on both nodes.
 *-----------------------------------------------------------------------------
 */
static inline bool32
btree_index_is_full(const btree_config *cfg, // IN
                    const btree_hdr    *hdr)    // IN
{
   return hdr->next_entry < diff_ptr(hdr, &hdr->offsets[hdr->num_entries + 2])
                               + sizeof(index_entry)
                               + MAX_INLINE_KEY_SIZE(btree_page_size(cfg));
}

static inline uint64
btree_choose_index_split(const btree_config *cfg, // IN
                         const btree_hdr    *hdr)    // IN
{
   /* Split the content by bytes -- roughly half the bytes go to the
    * right node.  So count the bytes.
    */
   uint64 total_entry_bytes = 0;
   for (uint64 i = 0; i < btree_num_entries(hdr); i++) {
      index_entry *entry = btree_get_index_entry(cfg, hdr, i);
      total_entry_bytes += sizeof_index_entry(entry);
   }

   /* Now figure out the number of entries to move, and figure out how
    * much free space will be created in the left_hdr by the split.
    */
   uint64 target_left_entries  = 0;
   uint64 new_left_entry_bytes = 0;
   while (new_left_entry_bytes < total_entry_bytes / 2) {
      index_entry *entry = btree_get_index_entry(cfg, hdr, target_left_entries);
      new_left_entry_bytes += sizeof_index_entry(entry);
      target_left_entries++;
   }
   return target_left_entries;
}

static inline void
btree_split_index_build_right_node(const btree_config *cfg,        // IN
                                   const btree_hdr    *left_hdr,   // IN
                                   uint64     target_left_entries, // IN
                                   btree_hdr *right_hdr)           // IN/OUT
{
   uint64 target_right_entries =
      btree_num_entries(left_hdr) - target_left_entries;

   /* Build the right node. */
   memmove(right_hdr, left_hdr, sizeof(*right_hdr));
   right_hdr->generation++;
   btree_reset_node_entries(cfg, right_hdr);
   for (uint64 i = 0; i < target_right_entries; i++) {
      index_entry *entry =
         btree_get_index_entry(cfg, left_hdr, target_left_entries + i);
      bool32 succeeded = btree_set_index_entry(cfg,
                                               right_hdr,
                                               i,
                                               index_entry_key(entry),
                                               index_entry_child_addr(entry),
                                               entry->pivot_data.stats);
      platform_assert(succeeded);
   }
}

/*
 *-----------------------------------------------------------------------------
 * btree_defragment_index --
 *
 *      Defragment a node
 *-----------------------------------------------------------------------------
 */
void
btree_defragment_index(const btree_config *cfg, // IN
                       btree_scratch      *scratch,
                       btree_hdr          *hdr) // IN
{
   btree_hdr *scratch_hdr = (btree_hdr *)scratch->defragment_node.scratch_node;
   memcpy(scratch_hdr, hdr, btree_page_size(cfg));
   btree_reset_node_entries(cfg, hdr);
   for (uint64 i = 0; i < btree_num_entries(scratch_hdr); i++) {
      index_entry *entry     = btree_get_index_entry(cfg, scratch_hdr, i);
      bool32       succeeded = btree_set_index_entry(cfg,
                                               hdr,
                                               i,
                                               index_entry_key(entry),
                                               index_entry_child_addr(entry),
                                               entry->pivot_data.stats);
      platform_assert(succeeded);
   }
}

static inline void
btree_truncate_index(const btree_config *cfg, // IN
                     btree_scratch      *scratch,
                     btree_hdr          *hdr, // IN
                     uint64              target_entries)   // IN
{
   uint64 new_next_entry = btree_page_size(cfg);
   for (uint64 i = 0; i < target_entries; i++) {
      if (hdr->offsets[i] < new_next_entry) {
         new_next_entry = hdr->offsets[i];
      }
   }

   hdr->num_entries = target_entries;
   hdr->next_entry  = new_next_entry;
   hdr->generation++;

   if (new_next_entry < BTREE_DEFRAGMENT_THRESHOLD(btree_page_size(cfg))) {
      btree_defragment_index(cfg, scratch, hdr);
   }
}

/*
 *-----------------------------------------------------------------------------
 * btree_alloc --
 *
 *      Allocates a node from the preallocator. Will refill it if there are no
 *      more nodes available for the given height.
 *-----------------------------------------------------------------------------
 */
bool32
btree_alloc(cache          *cc,
            mini_allocator *mini,
            uint64          height,
            key             alloc_key,
            uint64         *next_extent,
            page_type       type,
            btree_node     *node)
{
   node->addr = mini_alloc(mini, height, alloc_key, next_extent);
   debug_assert(node->addr != 0);
   node->page = cache_alloc(cc, node->addr, type);

   // If this btree is for a memtable then pin all pages belonging to it
   if (type == PAGE_TYPE_MEMTABLE) {
      cache_pin(cc, node->page);
   }
   node->hdr = (btree_hdr *)(node->page->data);
   return TRUE;
}

/*
 *-----------------------------------------------------------------------------
 * btree_node_[get,release] --
 *
 *      Gets the node with appropriate lock or releases the lock.
 *-----------------------------------------------------------------------------
 */
static inline void
btree_node_get(cache              *cc,
               const btree_config *cfg,
               btree_node         *node,
               page_type           type)
{
   debug_assert(node->addr != 0);

   node->page = cache_get(cc, node->addr, TRUE, type);
   node->hdr  = (btree_hdr *)(node->page->data);
}

static inline bool32
btree_node_claim(cache              *cc,  // IN
                 const btree_config *cfg, // IN
                 btree_node         *node)        // IN
{
   return cache_try_claim(cc, node->page);
}

static inline void
btree_node_lock(cache              *cc,  // IN
                const btree_config *cfg, // IN
                btree_node         *node)        // IN
{
   cache_lock(cc, node->page);
   cache_mark_dirty(cc, node->page);
}

static inline void
btree_node_unlock(cache              *cc,  // IN
                  const btree_config *cfg, // IN
                  btree_node         *node)        // IN
{
   cache_unlock(cc, node->page);
}

static inline void
btree_node_unclaim(cache              *cc,  // IN
                   const btree_config *cfg, // IN
                   btree_node         *node)        // IN
{
   cache_unclaim(cc, node->page);
}

void
btree_node_unget(cache              *cc,  // IN
                 const btree_config *cfg, // IN
                 btree_node         *node)        // IN
{
   cache_unget(cc, node->page);
   node->page = NULL;
   node->hdr  = NULL;
}

static inline void
btree_node_full_unlock(cache              *cc,  // IN
                       const btree_config *cfg, // IN
                       btree_node         *node)        // IN
{
   btree_node_unlock(cc, cfg, node);
   btree_node_unclaim(cc, cfg, node);
   btree_node_unget(cc, cfg, node);
}

static inline void
btree_node_get_from_cache_ctxt(const btree_config *cfg,  // IN
                               cache_async_ctxt   *ctxt, // IN
                               btree_node         *node)         // OUT
{
   node->addr = ctxt->page->disk_addr;
   node->page = ctxt->page;
   node->hdr  = (btree_hdr *)node->page->data;
}


static inline bool32
btree_addrs_share_extent(cache *cc, uint64 left_addr, uint64 right_addr)
{
   allocator *al = cache_get_allocator(cc);
   return allocator_config_pages_share_extent(
      allocator_get_config(al), right_addr, left_addr);
}

static inline uint64
btree_root_to_meta_addr(const btree_config *cfg,
                        uint64              root_addr,
                        uint64              meta_page_no)
{
   return root_addr + (meta_page_no + 1) * btree_page_size(cfg);
}


/*----------------------------------------------------------
 * Creating and destroying B-trees.
 *----------------------------------------------------------
 */
uint64
btree_create(cache              *cc,
             const btree_config *cfg,
             mini_allocator     *mini,
             page_type           type)
{
   // get a free node for the root
   // we don't use the next_addr arr for this, since the root doesn't
   // maintain constant height
   allocator      *al = cache_get_allocator(cc);
   uint64          base_addr;
   platform_status rc = allocator_alloc(al, &base_addr, type);
   platform_assert_status_ok(rc);
   page_handle *root_page = cache_alloc(cc, base_addr, type);
   bool32       pinned    = (type == PAGE_TYPE_MEMTABLE);

   // set up the root
   btree_node root;
   root.page = root_page;
   root.addr = base_addr;
   root.hdr  = (btree_hdr *)root_page->data;

   btree_init_hdr(cfg, root.hdr);

   cache_mark_dirty(cc, root.page);

   // If this btree is for a memtable then pin all pages belonging to it
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
             root.addr + btree_page_size(cfg),
             0,
             BTREE_MAX_HEIGHT,
             type,
             type == PAGE_TYPE_BRANCH);

   return root.addr;
}

void
btree_inc_ref_range(cache              *cc,
                    const btree_config *cfg,
                    uint64              root_addr,
                    key                 start_key,
                    key                 end_key)
{
   debug_assert(btree_key_compare(cfg, start_key, end_key) <= 0);
   uint64 meta_page_addr = btree_root_to_meta_addr(cfg, root_addr, 0);
   mini_keyed_inc_ref(
      cc, cfg->data_cfg, PAGE_TYPE_BRANCH, meta_page_addr, start_key, end_key);
}

bool32
btree_dec_ref_range(cache              *cc,
                    const btree_config *cfg,
                    uint64              root_addr,
                    key                 start_key,
                    key                 end_key)
{
   debug_assert(btree_key_compare(cfg, start_key, end_key) <= 0);
   uint64 meta_page_addr = btree_root_to_meta_addr(cfg, root_addr, 0);
   return mini_keyed_dec_ref(
      cc, cfg->data_cfg, PAGE_TYPE_BRANCH, meta_page_addr, start_key, end_key);
}

bool32
btree_dec_ref(cache              *cc,
              const btree_config *cfg,
              uint64              root_addr,
              page_type           type)
{
   platform_assert(type == PAGE_TYPE_MEMTABLE);
   uint64 meta_head = btree_root_to_meta_addr(cfg, root_addr, 0);
   uint8  ref       = mini_unkeyed_dec_ref(cc, meta_head, type, TRUE);
   return ref == 0;
}

void
btree_block_dec_ref(cache *cc, btree_config *cfg, uint64 root_addr)
{
   uint64 meta_head = btree_root_to_meta_addr(cfg, root_addr, 0);
   mini_block_dec_ref(cc, meta_head);
}

void
btree_unblock_dec_ref(cache *cc, btree_config *cfg, uint64 root_addr)
{
   uint64 meta_head = btree_root_to_meta_addr(cfg, root_addr, 0);
   mini_unblock_dec_ref(cc, meta_head);
}

/*
 * *********************************************************************
 * The process of splitting a child leaf is divided into four steps in
 * order to minimize the amount of time that we hold write-locks on
 * the parent and child:
 *
 * 0. Start with claims on parent and child.
 *
 * 1. Allocate a node for the right child.  Hold a write lock on the new node.
 *
 * 2. btree_add_pivot.  Insert a new pivot in the parent for
 *    the new child.  This step requires a write-lock on the parent.
 *    The parent can be completely unlocked as soon as this step is
 *    complete.
 *
 * 3. btree_split_{leaf,index}_build_right_node
 *    Fill in the contents of the right child.  No lock on parent
 *    required.
 *
 * 4. btree_truncate_{leaf,index}
 *    Truncate (and optionally defragment) the old child.  This is the
 *    only step that requires a write-lock on the old child.
 *
 * Note: if we wanted to maintain rank information in the parent when
 * splitting one of its children, we could do that by holding the lock
 * on the parent a bit longer.  But we don't need that in the
 * memtable, so not bothering for now.
 * *********************************************************************
 */

/*
 * Requires:
 * - claim on parent
 * - claim on child
 *
 * Upon completion:
 * - all nodes unlocked
 * - the insertion is complete
 *
 * This function violates our locking rules. See comment at top of file.
 */
static inline int
btree_split_child_leaf(cache                 *cc,
                       const btree_config    *cfg,
                       mini_allocator        *mini,
                       btree_scratch         *scratch,
                       btree_node            *parent,
                       uint64                 index_of_child_in_parent,
                       btree_node            *child,
                       leaf_incorporate_spec *spec,
                       uint64                *generation) // OUT
{
   btree_node right_child;

   /*
    * We indicate locking using these labels
    * c  = child, the leaf we're splitting
    * p  = parent, the parent of child
    * rc = right child, the new leaf we're adding
    * cn = child next, the child's original next
    *
    * starting locks:
    * p: claim, c: claim, rc: -, cn: -
    */

   leaf_splitting_plan plan =
      btree_build_leaf_splitting_plan(cfg, child->hdr, spec);

   btree_alloc(cc,
               mini,
               btree_height(child->hdr),
               NULL_KEY,
               NULL,
               PAGE_TYPE_MEMTABLE,
               &right_child);
   /* p: claim, c: claim, rc: write, cn: - */

   btree_node_lock(cc, cfg, parent);
   /* p: write, c: claim, rc: write, cn: - */

   btree_node child_next;
   child_next.addr = child->hdr->next_addr;
   if (child_next.addr != 0) {
      btree_node_get(cc, cfg, &child_next, PAGE_TYPE_MEMTABLE);
      uint64 child_next_wait = 1;
      while (!btree_node_claim(cc, cfg, &child_next)) {
         btree_node_unget(cc, cfg, &child_next);
         platform_sleep_ns(child_next_wait);
         child_next_wait =
            child_next_wait > 2048 ? child_next_wait : 2 * child_next_wait;
         btree_node_get(cc, cfg, &child_next, PAGE_TYPE_MEMTABLE);
      }
      btree_node_lock(cc, cfg, &child_next);
   }
   /* p: write, c: claim, rc: write, cn: write if exists */

   btree_node_lock(cc, cfg, child);
   /* p: write, c: write, rc: write, cn: write if exists */

   {
      /* limit the scope of pivot_key, since subsequent mutations of the nodes
       * may invalidate the memory it points to.
       */
      key    pivot_key = btree_splitting_pivot(cfg, child->hdr, spec, plan);
      bool32 success   = btree_insert_index_entry(cfg,
                                                parent->hdr,
                                                index_of_child_in_parent + 1,
                                                pivot_key,
                                                right_child.addr,
                                                BTREE_PIVOT_STATS_UNKNOWN);
      platform_assert(success);
   }
   btree_node_full_unlock(cc, cfg, parent);
   /* p: unlocked, c: write, rc: write, cn: write if exists */

   // set prev pointer from child's original next to right_child
   if (child_next.addr != 0) {
      child_next.hdr->prev_addr = right_child.addr;
      btree_node_full_unlock(cc, cfg, &child_next);
   }
   /* p: unlocked, c: write, rc: write, cn: unlocked */

   btree_split_leaf_build_right_node(
      cfg, child->hdr, child->addr, spec, plan, right_child.hdr, generation);
   btree_node_full_unlock(cc, cfg, &right_child);
   /* p: unlocked, c: write, rc: unlocked, cn: unlocked */

   btree_split_leaf_cleanup_left_node(
      cfg, scratch, child->hdr, spec, plan, right_child.addr);
   if (plan.insertion_goes_left) {
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, child->hdr, spec, generation);
      platform_assert(incorporated);
   }
   btree_node_full_unlock(cc, cfg, child);
   /* p: unlocked, c: unlocked, rc: unlocked, cn: unlocked */

   return 0;
}

/*
 * Requires:
 * - claim on parent
 * - claim on child
 *
 * Upon completion:
 * - all nodes fully unlocked
 * - insertion is complete
 */
static inline int
btree_defragment_or_split_child_leaf(cache              *cc,
                                     const btree_config *cfg,
                                     mini_allocator     *mini,
                                     btree_scratch      *scratch,
                                     btree_node         *parent,
                                     uint64      index_of_child_in_parent,
                                     btree_node *child,
                                     leaf_incorporate_spec *spec,
                                     uint64                *generation) // OUT
{
   uint64 nentries   = btree_num_entries(child->hdr);
   uint64 live_bytes = 0;

   log_trace_leaf(cfg, child->hdr, "btree_defragment_or_split_child_leaf");

   for (uint64 i = 0; i < nentries; i++) {
      if (spec->old_entry_state != ENTRY_STILL_EXISTS || i != spec->idx) {
         leaf_entry *entry = btree_get_leaf_entry(cfg, child->hdr, i);
         live_bytes += sizeof_leaf_entry(entry);
      }
   }
   uint64 total_space_required =
      live_bytes
      + leaf_entry_required_capacity(spec->tuple_key, spec_message(spec))
      + (nentries + spec->old_entry_state == ENTRY_STILL_EXISTS ? 0 : 1)
           * sizeof(index_entry);

   if (total_space_required < BTREE_SPLIT_THRESHOLD(btree_page_size(cfg))) {
      btree_node_unclaim(cc, cfg, parent);
      btree_node_unget(cc, cfg, parent);
      btree_node_lock(cc, cfg, child);
      btree_defragment_leaf(cfg, scratch, child->hdr, spec);
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, child->hdr, spec, generation);
      platform_assert(incorporated);
      btree_node_full_unlock(cc, cfg, child);
   } else {
      btree_split_child_leaf(cc,
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
 * ----------------------------------------------------------------------------
 * Splitting a child index follows a similar pattern as splitting a child leaf.
 * The main difference is that we assume we start with write-locks on the parent
 *  and child (which fits better with the flow of the overall insert algorithm).
 *
 * Requires:
 * - lock on parent
 * - lock on child
 *
 * Upon completion:
 * - lock on new_child
 * - all other nodes unlocked
 * ----------------------------------------------------------------------------
 */
static inline int
btree_split_child_index(cache              *cc,
                        const btree_config *cfg,
                        mini_allocator     *mini,
                        btree_scratch      *scratch,
                        btree_node         *parent,
                        uint64              index_of_child_in_parent,
                        btree_node         *child,
                        key                 key_to_be_inserted,
                        btree_node         *new_child, // OUT
                        int64              *next_child_idx)         // IN/OUT
{
   btree_node right_child;

   /* p: lock, c: lock, rc: - */

   uint64 idx = btree_choose_index_split(cfg, child->hdr);

   /* p: lock, c: lock, rc: - */

   btree_alloc(cc,
               mini,
               btree_height(child->hdr),
               NULL_KEY,
               NULL,
               PAGE_TYPE_MEMTABLE,
               &right_child);

   /* p: lock, c: lock, rc: lock */

   {
      /* limit the scope of pivot_key, since subsequent mutations of the nodes
       * may invalidate the memory it points to.
       */
      key pivot_key = btree_get_pivot(cfg, child->hdr, idx);
      btree_insert_index_entry(cfg,
                               parent->hdr,
                               index_of_child_in_parent + 1,
                               pivot_key,
                               right_child.addr,
                               BTREE_PIVOT_STATS_UNKNOWN);
   }
   btree_node_full_unlock(cc, cfg, parent);

   /* p: -, c: lock, rc: lock */

   if (*next_child_idx < idx) {
      *new_child = *child;
   } else {
      *new_child = right_child;
      *next_child_idx -= idx;
   }

   btree_split_index_build_right_node(cfg, child->hdr, idx, right_child.hdr);

   /* p: -, c: lock, rc: lock */

   if (new_child->addr != right_child.addr) {
      btree_node_full_unlock(cc, cfg, &right_child);
   }

   /* p: -, c: lock, rc: if nc == rc then lock else fully unlocked */

   btree_truncate_index(cfg, scratch, child->hdr, idx);

   /* p: -, c: lock, rc: if nc == rc then lock else fully unlocked */

   if (new_child->addr != child->addr) {
      btree_node_full_unlock(cc, cfg, child);
   }

   /* p:  -,
    * c:  if nc == c  then locked else fully unlocked
    * rc: if nc == rc then locked else fully unlocked
    */
   return 0;
}

/*
 * ----------------------------------------------------------------------------
 * Requires:
 * - lock on parent
 * - lock on child
 *
 * Upon completion:
 * - lock on new_child
 * - all other nodes unlocked
 * ----------------------------------------------------------------------------
 */
static inline int
btree_defragment_or_split_child_index(cache              *cc,
                                      const btree_config *cfg,
                                      mini_allocator     *mini,
                                      btree_scratch      *scratch,
                                      btree_node         *parent,
                                      uint64      index_of_child_in_parent,
                                      btree_node *child,
                                      key         key_to_be_inserted,
                                      btree_node *new_child, // OUT
                                      int64      *next_child_idx) // IN/OUT
{
   uint64 nentries   = btree_num_entries(child->hdr);
   uint64 live_bytes = 0;
   for (uint64 i = 0; i < nentries; i++) {
      index_entry *entry = btree_get_index_entry(cfg, child->hdr, i);
      live_bytes += sizeof_index_entry(entry);
   }
   uint64 total_space_required = live_bytes + nentries * sizeof(index_entry);

   if (total_space_required < BTREE_SPLIT_THRESHOLD(btree_page_size(cfg))) {
      btree_node_full_unlock(cc, cfg, parent);
      btree_defragment_index(cfg, scratch, child->hdr);
      *new_child = *child;
   } else {
      btree_split_child_index(cc,
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
add_unknown(uint32 a, int32 b)
{
   if (a != BTREE_UNKNOWN_COUNTER && b != BTREE_UNKNOWN_COUNTER) {
      return a + b;
   } else {
      return BTREE_UNKNOWN_COUNTER;
   }
}

static inline void
btree_accumulate_pivot_stats(btree_pivot_stats *dest, btree_pivot_stats src)
{
   dest->num_kvs       = add_unknown(dest->num_kvs, src.num_kvs);
   dest->key_bytes     = add_unknown(dest->key_bytes, src.key_bytes);
   dest->message_bytes = add_unknown(dest->message_bytes, src.message_bytes);
}

static inline void
accumulate_node_ranks(const btree_config *cfg,
                      const btree_hdr    *hdr,
                      int                 from,
                      int                 to,
                      btree_pivot_stats  *stats)
{
   debug_assert(from <= to);
   if (btree_height(hdr) == 0) {
      for (int i = from; i < to; i++) {
         leaf_entry *entry = btree_get_leaf_entry(cfg, hdr, i);
         stats->key_bytes =
            add_unknown(stats->key_bytes, leaf_entry_key_size(entry));
         stats->message_bytes =
            add_unknown(stats->message_bytes, leaf_entry_message_size(entry));
      }
      stats->num_kvs += to - from;
   } else {
      for (int i = from; i < to; i++) {
         index_entry *entry = btree_get_index_entry(cfg, hdr, i);
         btree_accumulate_pivot_stats(stats, entry->pivot_data.stats);
      }
   }
}

/*
 *-----------------------------------------------------------------------------
 * btree_grow_root --
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
btree_grow_root(cache              *cc,   // IN
                const btree_config *cfg,  // IN
                mini_allocator     *mini, // IN/OUT
                btree_node         *root_node)    // OUT
{
   // allocate a new left node
   btree_node child;
   btree_alloc(cc,
               mini,
               btree_height(root_node->hdr),
               NULL_KEY,
               NULL,
               PAGE_TYPE_MEMTABLE,
               &child);

   // copy root to child
   memmove(child.hdr, root_node->hdr, btree_page_size(cfg));
   btree_node_unlock(cc, cfg, &child);
   btree_node_unclaim(cc, cfg, &child);

   btree_reset_node_entries(cfg, root_node->hdr);
   btree_increment_height(root_node->hdr);
   key new_pivot;
   if (btree_height(child.hdr) == 0) {
      new_pivot = btree_get_tuple_key(cfg, child.hdr, 0);
   } else {
      new_pivot = btree_get_pivot(cfg, child.hdr, 0);
   }
   bool32 succeeded = btree_set_index_entry(
      cfg, root_node->hdr, 0, new_pivot, child.addr, BTREE_PIVOT_STATS_UNKNOWN);
   platform_assert(succeeded);

   btree_node_unget(cc, cfg, &child);
   return 0;
}

/*
 *-----------------------------------------------------------------------------
 * btree_insert --
 *
 *      Inserts the tuple into the dynamic btree.
 *-----------------------------------------------------------------------------
 */
platform_status
btree_insert(cache              *cc,         // IN
             const btree_config *cfg,        // IN
             platform_heap_id    heap_id,    // IN
             btree_scratch      *scratch,    // IN
             uint64              root_addr,  // IN
             mini_allocator     *mini,       // IN
             key                 tuple_key,  // IN
             message             msg,        // IN
             uint64             *generation, // OUT
             bool32             *was_unique)             // OUT
{
   platform_status       rc;
   leaf_incorporate_spec spec;
   uint64                leaf_wait = 1;

   if (MAX_INLINE_KEY_SIZE(btree_page_size(cfg)) < key_length(tuple_key)) {
      return STATUS_BAD_PARAM;
   }

   if (MAX_INLINE_MESSAGE_SIZE(btree_page_size(cfg)) < message_length(msg)) {
      return STATUS_BAD_PARAM;
   }

   if (message_is_invalid_user_type(msg)) {
      return STATUS_BAD_PARAM;
   }

   btree_node root_node;
   root_node.addr = root_addr;

   log_trace_key(tuple_key, "btree_insert");

start_over:
   btree_node_get(cc, cfg, &root_node, PAGE_TYPE_MEMTABLE);

   if (btree_height(root_node.hdr) == 0) {
      rc = btree_create_leaf_incorporate_spec(
         cfg, heap_id, root_node.hdr, tuple_key, msg, &spec);
      if (!SUCCESS(rc)) {
         btree_node_unget(cc, cfg, &root_node);
         return rc;
      }
      if (!btree_node_claim(cc, cfg, &root_node)) {
         btree_node_unget(cc, cfg, &root_node);
         destroy_leaf_incorporate_spec(&spec);
         goto start_over;
      }
      btree_node_lock(cc, cfg, &root_node);
      if (btree_try_perform_leaf_incorporate_spec(
             cfg, root_node.hdr, &spec, generation))
      {
         *was_unique = spec.old_entry_state == ENTRY_DID_NOT_EXIST;
         btree_node_full_unlock(cc, cfg, &root_node);
         destroy_leaf_incorporate_spec(&spec);
         return STATUS_OK;
      }
      destroy_leaf_incorporate_spec(&spec);
      btree_grow_root(cc, cfg, mini, &root_node);
      btree_node_unlock(cc, cfg, &root_node);
      btree_node_unclaim(cc, cfg, &root_node);
   }

   /* read lock on root_node, root_node is an index. */

   bool32 found;
   int64  child_idx = btree_find_pivot(cfg, root_node.hdr, tuple_key, &found);
   index_entry *parent_entry;

   if (child_idx < 0 || btree_index_is_full(cfg, root_node.hdr)) {
      if (!btree_node_claim(cc, cfg, &root_node)) {
         btree_node_unget(cc, cfg, &root_node);
         goto start_over;
      }
      btree_node_lock(cc, cfg, &root_node);
      bool32 need_to_set_min_key = FALSE;
      if (child_idx < 0) {
         child_idx    = 0;
         parent_entry = btree_get_index_entry(cfg, root_node.hdr, 0);
         need_to_set_min_key =
            !btree_set_index_entry(cfg,
                                   root_node.hdr,
                                   0,
                                   tuple_key,
                                   index_entry_child_addr(parent_entry),
                                   parent_entry->pivot_data.stats);
      }
      if (btree_index_is_full(cfg, root_node.hdr)) {
         btree_grow_root(cc, cfg, mini, &root_node);
         child_idx = 0;
      }
      if (need_to_set_min_key) {
         parent_entry = btree_get_index_entry(cfg, root_node.hdr, 0);
         bool32 success =
            btree_set_index_entry(cfg,
                                  root_node.hdr,
                                  0,
                                  tuple_key,
                                  index_entry_child_addr(parent_entry),
                                  parent_entry->pivot_data.stats);
         platform_assert(success);
      }
      btree_node_unlock(cc, cfg, &root_node);
      btree_node_unclaim(cc, cfg, &root_node);
   }

   parent_entry = btree_get_index_entry(cfg, root_node.hdr, child_idx);

   /* root_node read-locked,
    * root_node is an index,
    * root_node min key is up to date,
    * root_node will not need to split
    */
   btree_node parent_node = root_node;
   btree_node child_node;
   child_node.addr = index_entry_child_addr(parent_entry);
   debug_assert(allocator_page_valid(cache_get_allocator(cc), child_node.addr));
   btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);

   uint64 height = btree_height(parent_node.hdr);
   while (height > 1) {
      /*
       * Loop invariant:
       * - read lock on parent_node, parent_node is an index, parent_node min
       *   key is up to date, and parent_node will not need to split.
       * - read lock on child_node
       * - height >= 1
       */
      int64 next_child_idx =
         btree_find_pivot(cfg, child_node.hdr, tuple_key, &found);
      if (next_child_idx < 0 || btree_index_is_full(cfg, child_node.hdr)) {
         if (!btree_node_claim(cc, cfg, &parent_node)) {
            btree_node_unget(cc, cfg, &parent_node);
            btree_node_unget(cc, cfg, &child_node);
            goto start_over;
         }
         if (!btree_node_claim(cc, cfg, &child_node)) {
            btree_node_unclaim(cc, cfg, &parent_node);
            btree_node_unget(cc, cfg, &parent_node);
            btree_node_unget(cc, cfg, &child_node);
            goto start_over;
         }

         btree_node_lock(cc, cfg, &parent_node);
         btree_node_lock(cc, cfg, &child_node);

         bool32 need_to_set_min_key = FALSE;
         if (next_child_idx < 0) {
            next_child_idx = 0;
            index_entry *child_entry =
               btree_get_index_entry(cfg, child_node.hdr, next_child_idx);
            need_to_set_min_key =
               !btree_set_index_entry(cfg,
                                      child_node.hdr,
                                      0,
                                      tuple_key,
                                      index_entry_child_addr(child_entry),
                                      child_entry->pivot_data.stats);
         }

         if (btree_index_is_full(cfg, child_node.hdr)) {
            btree_node new_child;
            btree_defragment_or_split_child_index(cc,
                                                  cfg,
                                                  mini,
                                                  scratch,
                                                  &parent_node,
                                                  child_idx,
                                                  &child_node,
                                                  tuple_key,
                                                  &new_child,
                                                  &next_child_idx);
            parent_node = new_child;
         } else {
            btree_node_full_unlock(cc, cfg, &parent_node);
            parent_node = child_node;
         }

         if (need_to_set_min_key) { // new_child is guaranteed to be child in
                                    // this case
            index_entry *child_entry =
               btree_get_index_entry(cfg, parent_node.hdr, 0);
            bool32 success =
               btree_set_index_entry(cfg,
                                     parent_node.hdr,
                                     0,
                                     tuple_key,
                                     index_entry_child_addr(child_entry),
                                     child_entry->pivot_data.stats);
            platform_assert(success);
         }
         btree_node_unlock(cc, cfg, &parent_node);
         btree_node_unclaim(cc, cfg, &parent_node);
      } else {
         btree_node_unget(cc, cfg, &parent_node);
         parent_node = child_node;
      }

      /* read lock on parent_node, which won't require a split. */

      child_idx    = next_child_idx;
      parent_entry = btree_get_index_entry(cfg, parent_node.hdr, child_idx);
      debug_assert(parent_entry->pivot_data.stats.num_kvs
                   == BTREE_UNKNOWN_COUNTER);
      debug_assert(parent_entry->pivot_data.stats.key_bytes
                   == BTREE_UNKNOWN_COUNTER);
      debug_assert(parent_entry->pivot_data.stats.message_bytes
                   == BTREE_UNKNOWN_COUNTER);
      child_node.addr = index_entry_child_addr(parent_entry);
      debug_assert(
         allocator_page_valid(cache_get_allocator(cc), child_node.addr));
      btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
      height--;
   }

   /*
    * - read lock on parent_node, parent_node is an index, parent node
    *   min key is up to date, and parent_node will not need to split.
    * - read lock on child_node
    * - height of parent == 1
    */
   rc = btree_create_leaf_incorporate_spec(
      cfg, heap_id, child_node.hdr, tuple_key, msg, &spec);
   if (!SUCCESS(rc)) {
      btree_node_unget(cc, cfg, &parent_node);
      btree_node_unget(cc, cfg, &child_node);
      return rc;
   }

   /* If we don't need to split, then let go of the parent and do the
    * insert.  If we can't get a claim on the child, then start
    * over.
    */
   if (btree_can_perform_leaf_incorporate_spec(cfg, child_node.hdr, &spec)) {
      btree_node_unget(cc, cfg, &parent_node);
      if (!btree_node_claim(cc, cfg, &child_node)) {
         btree_node_unget(cc, cfg, &child_node);
         destroy_leaf_incorporate_spec(&spec);
         goto start_over;
      }
      btree_node_lock(cc, cfg, &child_node);
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, child_node.hdr, &spec, generation);
      platform_assert(incorporated);
      btree_node_full_unlock(cc, cfg, &child_node);
      destroy_leaf_incorporate_spec(&spec);
      *was_unique = spec.old_entry_state == ENTRY_DID_NOT_EXIST;
      return STATUS_OK;
   }

   /* Need to split or defrag the child. */
   if (!btree_node_claim(cc, cfg, &parent_node)) {
      btree_node_unget(cc, cfg, &parent_node);
      btree_node_unget(cc, cfg, &child_node);
      destroy_leaf_incorporate_spec(&spec);
      goto start_over;
   }
   bool32 need_to_rebuild_spec = FALSE;
   while (!btree_node_claim(cc, cfg, &child_node)) {
      btree_node_unget(cc, cfg, &child_node);
      platform_sleep_ns(leaf_wait);
      leaf_wait = leaf_wait > 2048 ? leaf_wait : 2 * leaf_wait;
      btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
      need_to_rebuild_spec = TRUE;
   }
   if (need_to_rebuild_spec) {
      /* If we had to relenquish our lock, then our spec might be out of date,
       * so rebuild it.
       */
      destroy_leaf_incorporate_spec(&spec);
      rc = btree_create_leaf_incorporate_spec(
         cfg, heap_id, child_node.hdr, tuple_key, msg, &spec);
      if (!SUCCESS(rc)) {
         btree_node_unget(cc, cfg, &parent_node);
         btree_node_unclaim(cc, cfg, &child_node);
         btree_node_unget(cc, cfg, &child_node);
         return rc;
      }
   }
   btree_defragment_or_split_child_leaf(cc,
                                        cfg,
                                        mini,
                                        scratch,
                                        &parent_node,
                                        child_idx,
                                        &child_node,
                                        &spec,
                                        generation);
   destroy_leaf_incorporate_spec(&spec);
   *was_unique = spec.old_entry_state == ENTRY_DID_NOT_EXIST;
   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 * btree_lookup_node --
 *
 *      lookup_node finds the node of height stop_at_height with
 *      (node.min_key <= key < node.max_key) and returns it with a read lock
 *      held.
 *
 *      out_rank returns the rank of out_node amount nodes of height
 *      stop_at_height.
 *
 *      If any change is made here, please change
 *      btree_lookup_async_with_ref too.
 *-----------------------------------------------------------------------------
 */
platform_status
btree_lookup_node(cache             *cc,             // IN
                  btree_config      *cfg,            // IN
                  uint64             root_addr,      // IN
                  key                target,         // IN
                  uint16             stop_at_height, // IN
                  page_type          type,           // IN
                  btree_node        *out_node,       // OUT
                  btree_pivot_stats *stats)          // OUT
{
   btree_node node, child_node;
   uint32     h;
   int64      child_idx;

   if (stats) {
      memset(stats, 0, sizeof(*stats));
   }

   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   node.addr = root_addr;
   btree_node_get(cc, cfg, &node, type);

   for (h = btree_height(node.hdr); h > stop_at_height; h--) {
      bool32 found;
      child_idx = key_is_positive_infinity(target)
                     ? btree_num_entries(node.hdr) - 1
                     : btree_find_pivot(cfg, node.hdr, target, &found);
      if (child_idx < 0) {
         child_idx = 0;
      }
      index_entry *entry = btree_get_index_entry(cfg, node.hdr, child_idx);
      child_node.addr    = index_entry_child_addr(entry);

      if (stats) {
         accumulate_node_ranks(cfg, node.hdr, 0, child_idx, stats);
      }

      btree_node_get(cc, cfg, &child_node, type);
      debug_assert(child_node.page->disk_addr == child_node.addr);
      btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   *out_node = node;
   return STATUS_OK;
}


static inline void
btree_lookup_with_ref(cache        *cc,        // IN
                      btree_config *cfg,       // IN
                      uint64        root_addr, // IN
                      page_type     type,      // IN
                      key           target,    // IN
                      btree_node   *node,      // OUT
                      message      *msg,       // OUT
                      bool32       *found)           // OUT
{
   btree_lookup_node(cc, cfg, root_addr, target, 0, type, node, NULL);
   int64 idx = btree_find_tuple(cfg, node->hdr, target, found);
   if (*found) {
      leaf_entry *entry = btree_get_leaf_entry(cfg, node->hdr, idx);
      *msg              = leaf_entry_message(entry);
   } else {
      btree_node_unget(cc, cfg, node);
   }
}

platform_status
btree_lookup(cache             *cc,        // IN
             btree_config      *cfg,       // IN
             uint64             root_addr, // IN
             page_type          type,      // IN
             key                target,    // IN
             merge_accumulator *result)    // OUT
{
   btree_node      node;
   message         data;
   platform_status rc = STATUS_OK;
   bool32          local_found;

   btree_lookup_with_ref(
      cc, cfg, root_addr, type, target, &node, &data, &local_found);
   if (local_found) {
      bool32 success = merge_accumulator_copy_message(result, data);
      rc             = success ? STATUS_OK : STATUS_NO_MEMORY;
      btree_node_unget(cc, cfg, &node);
   }
   return rc;
}

platform_status
btree_lookup_and_merge(cache             *cc,        // IN
                       btree_config      *cfg,       // IN
                       uint64             root_addr, // IN
                       page_type          type,      // IN
                       key                target,    // IN
                       merge_accumulator *data,      // OUT
                       bool32            *local_found)          // OUT
{
   btree_node      node;
   message         local_data;
   platform_status rc = STATUS_OK;

   log_trace_key(target, "btree_lookup");

   btree_lookup_with_ref(
      cc, cfg, root_addr, type, target, &node, &local_data, local_found);
   if (*local_found) {
      if (merge_accumulator_is_null(data)) {
         bool32 success = merge_accumulator_copy_message(data, local_data);
         rc             = success ? STATUS_OK : STATUS_NO_MEMORY;
      } else if (btree_merge_tuples(cfg, target, local_data, data)) {
         rc = STATUS_NO_MEMORY;
      }
      btree_node_unget(cc, cfg, &node);
   }
   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * btree_async_set_state --
 *      Set the state of the async btree lookup state machine.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static inline void
btree_async_set_state(btree_async_ctxt *ctxt, btree_async_state new_state)
{
   ctxt->prev_state = ctxt->state;
   ctxt->state      = new_state;
}


/*
 *-----------------------------------------------------------------------------
 * btree_async_callback --
 *
 *      Callback that's called when the async cache get loads a page into
 *      the cache. This function moves the async btree lookup
 *      state machine's state ahead, and calls the upper layer callback
 *      that will re-enqueue the btree lookup for dispatch.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static void
btree_async_callback(cache_async_ctxt *cache_ctxt)
{
   btree_async_ctxt *ctxt = cache_ctxt->cbdata;

   platform_assert(SUCCESS(cache_ctxt->status));
   platform_assert(cache_ctxt->page);
   //   platform_default_log("%s:%d tid %2lu: ctxt %p is callback with page %p
   //   (%#lx)\n",
   //                __FILE__, __LINE__, platform_get_tid(), ctxt,
   //                cache_ctxt->page, ctxt->child_addr);
   ctxt->was_async = TRUE;
   platform_assert(ctxt->state == btree_async_state_get_node);
   // Move state machine ahead and requeue for dispatch
   btree_async_set_state(ctxt, btree_async_state_get_index_complete);
   ctxt->cb(ctxt);
}


/*
 *-----------------------------------------------------------------------------
 * btree_lookup_async_with_ref --
 *
 *      State machine for the async btree point lookup. This uses hand over
 *      hand locking to descend the tree and every time a child node needs to
 *      be looked up from the cache, it uses the async get api. A reference to
 *      the parent node is held in btree_async_ctxt->node while a reference to
 *      the child page is obtained by the cache_get_async() in
 *      btree_async_ctxt->cache_ctxt->page
 *
 * Results:
 *      See btree_lookup_async(). if returning async_success and
 *      found = TRUE, this returns with ref on the btree leaf. Caller
 *      must do unget() on node_out.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static cache_async_result
btree_lookup_async_with_ref(cache            *cc,        // IN
                            btree_config     *cfg,       // IN
                            uint64            root_addr, // IN
                            key               target,    // IN
                            btree_node       *node_out,  // OUT
                            message          *data,      // OUT
                            bool32           *found,     // OUT
                            btree_async_ctxt *ctxt)      // IN
{
   cache_async_result res  = 0;
   bool32             done = FALSE;
   btree_node        *node = &ctxt->node;

   do {
      switch (ctxt->state) {
         case btree_async_state_start:
         {
            ctxt->child_addr = root_addr;
            node->page       = NULL;
            btree_async_set_state(ctxt, btree_async_state_get_node);
            // fallthrough
         }
         case btree_async_state_get_node:
         {
            cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

            cache_ctxt_init(cc, btree_async_callback, ctxt, cache_ctxt);
            res = cache_get_async(
               cc, ctxt->child_addr, PAGE_TYPE_BRANCH, cache_ctxt);
            switch (res) {
               case async_locked:
               case async_no_reqs:
                  //            platform_default_log("%s:%d tid %2lu: ctxt %p is
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
                  //            platform_default_log("%s:%d tid %2lu: ctxt %p is
                  //            io_started\n",
                  //                         __FILE__, __LINE__,
                  //                         platform_get_tid(), ctxt);
                  // Invocation is done; request isn't. Callback will move
                  // state.
                  done = TRUE;
                  break;
               case async_success:
                  ctxt->was_async = FALSE;
                  btree_async_set_state(ctxt,
                                        btree_async_state_get_index_complete);
                  break;
               default:
                  platform_assert(0);
            }
            break;
         }
         case btree_async_state_get_index_complete:
         {
            cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

            if (node->page) {
               // Unlock parent
               btree_node_unget(cc, cfg, node);
            }
            btree_node_get_from_cache_ctxt(cfg, cache_ctxt, node);
            debug_assert(node->addr == ctxt->child_addr);
            if (ctxt->was_async) {
               cache_async_done(cc, PAGE_TYPE_BRANCH, cache_ctxt);
            }
            if (btree_height(node->hdr) == 0) {
               btree_async_set_state(ctxt, btree_async_state_get_leaf_complete);
               break;
            }
            bool32 found_pivot;
            int64  child_idx =
               btree_find_pivot(cfg, node->hdr, target, &found_pivot);
            if (child_idx < 0) {
               child_idx = 0;
            }
            ctxt->child_addr = btree_get_child_addr(cfg, node->hdr, child_idx);
            btree_async_set_state(ctxt, btree_async_state_get_node);
            break;
         }
         case btree_async_state_get_leaf_complete:
         {
            int64 idx = btree_find_tuple(cfg, node->hdr, target, found);
            if (*found) {
               *data     = btree_get_tuple_message(cfg, node->hdr, idx);
               *node_out = *node;
            } else {
               btree_node_unget(cc, cfg, node);
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
 * btree_lookup_async --
 *
 *      Async btree point lookup. The ctxt should've been
 *      initialized using btree_ctxt_init().
 *
 * The return value can be one of:
 *
 *   - async_locked: A page needed by lookup is locked. User should retry
 *     request.
 *   - async_no_reqs: A page needed by lookup is not in cache and the IO
 *     subsystem is out of requests. User should throttle.
 *   - async_io_started: Async IO was started to read a page needed by the
 *     lookup into the cache. When the read is done, caller will be notified
 *     using ctxt->cb, that won't run on the thread context. It can be used
 *     to requeue the async lookup request for dispatch in thread context.
 *     When it's requeued, it must use the same function params except found.
 *     success: *found is TRUE if found, FALSE otherwise, data is stored in
 *     *data_out
 *
 * Results:
 *      Async result.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
cache_async_result
btree_lookup_async(cache             *cc,        // IN
                   btree_config      *cfg,       // IN
                   uint64             root_addr, // IN
                   key                target,    // IN
                   merge_accumulator *result,    // OUT
                   btree_async_ctxt  *ctxt)       // IN
{
   cache_async_result res;
   btree_node         node;
   message            data;
   bool32             local_found;
   res = btree_lookup_async_with_ref(
      cc, cfg, root_addr, target, &node, &data, &local_found, ctxt);
   if (res == async_success && local_found) {
      bool32 success = merge_accumulator_copy_message(result, data);
      platform_assert(success); // FIXME
      btree_node_unget(cc, cfg, &node);
   }

   return res;
}

cache_async_result
btree_lookup_and_merge_async(cache             *cc,          // IN
                             btree_config      *cfg,         // IN
                             uint64             root_addr,   // IN
                             key                target,      // IN
                             merge_accumulator *data,        // OUT
                             bool32            *local_found, // OUT
                             btree_async_ctxt  *ctxt)         // IN
{
   cache_async_result res;
   btree_node         node;
   message            local_data;

   res = btree_lookup_async_with_ref(
      cc, cfg, root_addr, target, &node, &local_data, local_found, ctxt);
   if (res == async_success && *local_found) {
      if (merge_accumulator_is_null(data)) {
         bool32 success = merge_accumulator_copy_message(data, local_data);
         platform_assert(success);
      } else {
         int rc = btree_merge_tuples(cfg, target, local_data, data);
         platform_assert(rc == 0);
      }
      btree_node_unget(cc, cfg, &node);
   }
   return res;
}

/*
 *-----------------------------------------------------------------------------
 * btree_iterator_init     --
 * btree_iterator_prev     --
 * btree_iterator_curr     --
 * btree_iterator_next     --
 * btree_iterator_can_prev --
 * btree_iterator_can_next --
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
static bool32
btree_iterator_can_prev(iterator *base_itor)
{
   btree_iterator *itor = (btree_iterator *)base_itor;
   return itor->idx >= itor->curr_min_idx
          && (itor->curr_min_idx != itor->end_idx
              || itor->curr.addr != itor->end_addr);
}

static bool32
btree_iterator_can_next(iterator *base_itor)
{
   btree_iterator *itor = (btree_iterator *)base_itor;
   return itor->curr.addr != itor->end_addr
          || (itor->idx < itor->end_idx && itor->curr_min_idx != itor->end_idx);
}

void
btree_iterator_curr(iterator *base_itor, key *curr_key, message *data)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;
   debug_assert(itor->curr.hdr != NULL);
   /*
   if (itor->at_end || itor->idx == itor->curr.hdr->num_entries) {
      btree_print_tree(itor->cc, itor->cfg, itor->root_addr);
   }
   */
   debug_assert(iterator_can_curr(base_itor));
   debug_assert(itor->idx < btree_num_entries(itor->curr.hdr));
   debug_assert(itor->curr.page != NULL);
   debug_assert(itor->curr.page->disk_addr == itor->curr.addr);
   debug_assert((char *)itor->curr.hdr == itor->curr.page->data);
   cache_validate_page(itor->cc, itor->curr.page, itor->curr.addr);
   if (itor->curr.hdr->height == 0) {
      *curr_key = btree_get_tuple_key(itor->cfg, itor->curr.hdr, itor->idx);
      *data     = btree_get_tuple_message(itor->cfg, itor->curr.hdr, itor->idx);
      log_trace_key(*curr_key, "btree_iterator_get_curr");
   } else {
      index_entry *entry =
         btree_get_index_entry(itor->cfg, itor->curr.hdr, itor->idx);
      *curr_key = index_entry_key(entry);
      *data     = message_create(
         MESSAGE_TYPE_PIVOT_DATA,
         slice_create(sizeof(entry->pivot_data), &entry->pivot_data));
   }
}

// helper function to find a key within a btree node
// at a height specified by the iterator
static inline int64
find_key_in_node(btree_iterator *itor,
                 btree_hdr      *hdr,
                 key             target,
                 comparison      position_rule,
                 bool32         *found)
{
   bool32 loc_found;
   if (found == NULL) {
      found = &loc_found;
   }

   int64 tmp;
   if (itor->height == 0) {
      tmp = btree_find_tuple(itor->cfg, hdr, target, found);
   } else if (itor->height > hdr->height) {
      // so we will always exceed height in future lookups
      itor->height = (uint32)-1;
      return 0; // this iterator is invalid, so return 0 for all lookups
   } else {
      tmp = btree_find_pivot(itor->cfg, hdr, itor->min_key, found);
   }

   switch (position_rule) {
      case less_than:
         if (*found) {
            --tmp;
         }
         // fallthrough
      case less_than_or_equal:
         break;
      case greater_than_or_equal:
         if (!*found) {
            ++tmp;
         }
         break;
      case greater_than:
         ++tmp;
         break;
   }
   return tmp;
}

static void
btree_iterator_find_end(btree_iterator *itor)
{
   btree_node end;

   btree_lookup_node(itor->cc,
                     itor->cfg,
                     itor->root_addr,
                     itor->max_key,
                     itor->height,
                     itor->page_type,
                     &end,
                     NULL);
   itor->end_addr       = end.addr;
   itor->end_generation = end.hdr->generation;

   if (key_is_positive_infinity(itor->max_key)) {
      itor->end_idx = btree_num_entries(end.hdr);
   } else {
      itor->end_idx = find_key_in_node(
         itor, end.hdr, itor->max_key, greater_than_or_equal, NULL);
   }

   btree_node_unget(itor->cc, itor->cfg, &end);
}

/*
 * ----------------------------------------------------------------------------
 * Move to the next leaf when we've reached the end of one leaf but
 * haven't reached the end of the iterator.
 * ----------------------------------------------------------------------------
 */
static void
btree_iterator_next_leaf(btree_iterator *itor)
{
   cache        *cc  = itor->cc;
   btree_config *cfg = itor->cfg;

   uint64 last_addr = itor->curr.addr;
   uint64 next_addr = itor->curr.hdr->next_addr;
   btree_node_unget(cc, cfg, &itor->curr);
   itor->curr.addr = next_addr;
   btree_node_get(cc, cfg, &itor->curr, itor->page_type);
   itor->idx          = 0;
   itor->curr_min_idx = -1;

   while (itor->curr.addr == itor->end_addr
          && itor->curr.hdr->generation != itor->end_generation)
   {
      /*
       * We need to recompute the end node and end_idx. (see
       * comment at beginning of iterator implementation for
       * high-level description)
       *
       * There's a potential for deadlock with concurrent inserters
       * if we hold a read-lock on curr while looking up end, so we
       * temporarily release curr.
       *
       * It is safe to relase curr because we are at index 0 of
       * curr.  To see why, observe that, at this point, curr
       * cannot be the first leaf in the tree (since we just
       * followed a next pointer a few lines above).  And, for
       * every leaf except the left-most leaf of the tree, no key
       * can ever be inserted into the leaf that is smaller than
       * the leaf's 0th entry, because its 0th entry is also its
       * pivot in its parent.  Thus we are guaranteed that the
       * first key curr will not change between the unget and the
       * get. Hence we will not "go backwards" i.e. return a key
       * smaller than the previous key) or skip any keys.
       * Furthermore, even if another thread comes along and splits
       * curr while we've released it, we will still want to
       * continue at curr (since we're at the 0th entry).
       */
      btree_node_unget(itor->cc, itor->cfg, &itor->curr);
      btree_iterator_find_end(itor);
      btree_node_get(itor->cc, itor->cfg, &itor->curr, itor->page_type);
   }

   // To prefetch:
   // 1. we just moved from one extent to the next
   // 2. this can't be the last extent
   if (itor->do_prefetch
       && !btree_addrs_share_extent(cc, last_addr, itor->curr.addr)
       && itor->curr.hdr->next_extent_addr != 0
       && !btree_addrs_share_extent(cc, itor->curr.addr, itor->end_addr))
   {
      // IO prefetch the next extent
      cache_prefetch(cc, itor->curr.hdr->next_extent_addr, itor->page_type);
   }
}

/*
 * ----------------------------------------------------------------------------
 * Move to the previous leaf when we've reached the beginning of one leaf.
 * ----------------------------------------------------------------------------
 */
static void
btree_iterator_prev_leaf(btree_iterator *itor)
{
   cache        *cc  = itor->cc;
   btree_config *cfg = itor->cfg;

   debug_only uint64 curr_addr = itor->curr.addr;
   uint64            prev_addr = itor->curr.hdr->prev_addr;
   btree_node_unget(cc, cfg, &itor->curr);
   itor->curr.addr = prev_addr;
   btree_node_get(cc, cfg, &itor->curr, itor->page_type);

   /*
    * The previous leaf may have split in between our release of the
    * old curr node and the new one.  In this case, we can just walk
    * forward until we find the leaf whose successor is our old leaf.
    */
   while (itor->curr.hdr->next_addr != curr_addr) {
      uint64 next_addr = itor->curr.hdr->next_addr;
      btree_node_unget(cc, cfg, &itor->curr);
      itor->curr.addr = next_addr;
      btree_node_get(cc, cfg, &itor->curr, itor->page_type);
   }

   itor->idx = btree_num_entries(itor->curr.hdr) - 1;

   /* Do a quick check whether this entire leaf is within the range. */
   key first_key = itor->height ? btree_get_pivot(cfg, itor->curr.hdr, 0)
                                : btree_get_tuple_key(cfg, itor->curr.hdr, 0);
   if (btree_key_compare(cfg, itor->min_key, first_key) < 0) {
      itor->curr_min_idx = -1;
   } else {
      bool32 found;
      itor->curr_min_idx =
         itor->height
            ? btree_find_pivot(cfg, itor->curr.hdr, itor->min_key, &found)
            : btree_find_tuple(cfg, itor->curr.hdr, itor->min_key, &found);
      if (!found) {
         itor->curr_min_idx++;
      }
   }
   if (itor->curr.hdr->prev_addr == 0 && itor->curr_min_idx == -1) {
      itor->curr_min_idx = 0;
   }

   // FIXME: To prefetch:
   // 1. we just moved from one extent to the next
   // 2. this can't be the last extent
   /* if (itor->do_prefetch */
   /*     && !btree_addrs_share_extent(cc, last_addr, itor->curr.addr) */
   /*     && itor->curr.hdr->next_extent_addr != 0 */
   /*     && !btree_addrs_share_extent(cc, itor->curr.addr, itor->end_addr)) */
   /* { */
   /*    // IO prefetch the next extent */
   /*    cache_prefetch(cc, itor->curr.hdr->next_extent_addr, itor->page_type);
    */
   /* } */
}

platform_status
btree_iterator_next(iterator *base_itor)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;

   // We should not be calling advance on an empty iterator
   debug_assert(btree_iterator_can_next(base_itor));
   debug_assert(itor->idx < btree_num_entries(itor->curr.hdr));

   itor->idx++;

   if (itor->idx == btree_num_entries(itor->curr.hdr)
       && btree_iterator_can_next(base_itor))
   {
      btree_iterator_next_leaf(itor);
   }

   debug_assert(
      !btree_iterator_can_next(base_itor)
      || (0 <= itor->idx && itor->idx < btree_num_entries(itor->curr.hdr)));

   return STATUS_OK;
}

platform_status
btree_iterator_prev(iterator *base_itor)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;

   // We should not be calling prev on an empty iterator
   debug_assert(btree_iterator_can_prev(base_itor));
   debug_assert(itor->idx >= 0);

   itor->idx--;
   if (itor->curr_min_idx == -1 && itor->idx == -1) {
      btree_iterator_prev_leaf(itor);
   }

   debug_assert(
      !btree_iterator_can_prev(base_itor)
      || (0 <= itor->idx && itor->idx < btree_num_entries(itor->curr.hdr)));

   return STATUS_OK;
}

// This function voilates our locking rules. See comment at top of file.
static inline void
find_btree_node_and_get_idx_bounds(btree_iterator *itor,
                                   key             target,
                                   comparison      position_rule)
{
   // lookup the node that contains target
   btree_lookup_node(itor->cc,
                     itor->cfg,
                     itor->root_addr,
                     target,
                     itor->height,
                     itor->page_type,
                     &itor->curr,
                     NULL);

   /*
    * We have to claim curr in order to prevent possible deadlocks
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
   while (!btree_node_claim(itor->cc, itor->cfg, &itor->curr)) {
      btree_node_unget(itor->cc, itor->cfg, &itor->curr);
      btree_lookup_node(itor->cc,
                        itor->cfg,
                        itor->root_addr,
                        target,
                        itor->height,
                        itor->page_type,
                        &itor->curr,
                        NULL);
   }

   btree_iterator_find_end(itor);

   /* Once we've found end, we can unclaim curr. */
   btree_node_unclaim(itor->cc, itor->cfg, &itor->curr);

   // find the index of the minimum key
   bool32 found;
   int64  tmp = find_key_in_node(
      itor, itor->curr.hdr, itor->min_key, greater_than_or_equal, &found);
   // If min key doesn't exist in current node, but is:
   // 1) in range:     Min idx = smallest key > min_key
   // 2) out of range: Min idx = -1
   itor->curr_min_idx = !found && tmp == 0 ? --tmp : tmp;
   // if min_key is not within the current node but there is no previous node
   // then set curr_min_idx to 0
   if (itor->curr_min_idx == -1 && itor->curr.hdr->prev_addr == 0) {
      itor->curr_min_idx = 0;
   }

   // find the index of the actual target
   itor->idx =
      find_key_in_node(itor, itor->curr.hdr, target, position_rule, &found);

   // check if we already need to move to the prev/next leaf
   if (itor->curr.addr != itor->end_addr
       && itor->idx == btree_num_entries(itor->curr.hdr))
   {
      btree_iterator_next_leaf(itor);
      itor->curr_min_idx = 0; // we came from an irrelevant leaf
   }
   if (itor->curr_min_idx == -1 && itor->idx == -1) {
      btree_iterator_prev_leaf(itor);
   }
}

/*
 * Seek to a given key within the btree
 * seek_type defines where the iterator is positioned relative to the target
 * key.
 */
platform_status
btree_iterator_seek(iterator *base_itor, key seek_key, comparison seek_type)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;

   if (btree_key_compare(itor->cfg, seek_key, itor->min_key) < 0
       || btree_key_compare(itor->cfg, seek_key, itor->max_key) > 0)
   {
      return STATUS_BAD_PARAM;
   }

   // check if seek_key is within our current node
   key first_key = itor->height
                      ? btree_get_pivot(itor->cfg, itor->curr.hdr, 0)
                      : btree_get_tuple_key(itor->cfg, itor->curr.hdr, 0);
   key last_key =
      itor->height
         ? btree_get_pivot(itor->cfg, itor->curr.hdr, itor->end_idx - 1)
         : btree_get_tuple_key(itor->cfg, itor->curr.hdr, itor->end_idx - 1);

   if (btree_key_compare(itor->cfg, seek_key, first_key) >= 0
       && btree_key_compare(itor->cfg, seek_key, last_key) <= 0)
   {
      // seek_key is within our current leaf. So just directly search for it
      bool32 found;
      itor->idx =
         find_key_in_node(itor, itor->curr.hdr, seek_key, seek_type, &found);
      platform_assert(0 <= itor->idx);
   } else {
      // seek key is not within our current leaf. So find the correct leaf
      find_btree_node_and_get_idx_bounds(itor, seek_key, seek_type);
   }

   return STATUS_OK;
}

void
btree_iterator_print(iterator *itor)
{
   debug_assert(itor != NULL);
   btree_iterator *btree_itor = (btree_iterator *)itor;

   platform_default_log("########################################\n");
   platform_default_log("## btree_itor: %p\n", itor);
   platform_default_log("## root: %lu\n", btree_itor->root_addr);
   platform_default_log(
      "## curr %lu end %lu\n", btree_itor->curr.addr, btree_itor->end_addr);
   platform_default_log("## idx %lu end_idx %lu generation %lu\n",
                        btree_itor->idx,
                        btree_itor->end_idx,
                        btree_itor->end_generation);
   btree_print_node(Platform_default_log_handle,
                    btree_itor->cc,
                    btree_itor->cfg,
                    &btree_itor->curr,
                    btree_itor->page_type);
}

const static iterator_ops btree_iterator_ops = {
   .curr     = btree_iterator_curr,
   .can_prev = btree_iterator_can_prev,
   .can_next = btree_iterator_can_next,
   .next     = btree_iterator_next,
   .prev     = btree_iterator_prev,
   .seek     = btree_iterator_seek,
   .print    = btree_iterator_print,
};


/*
 *-----------------------------------------------------------------------------
 * Caller must guarantee:
 *    min_key and max_key need to be valid until iterator deinitialized
 *-----------------------------------------------------------------------------
 */
void
btree_iterator_init(cache          *cc,
                    btree_config   *cfg,
                    btree_iterator *itor,
                    uint64          root_addr,
                    page_type       page_type,
                    key             min_key,
                    key             max_key,
                    key             start_key,
                    comparison      start_type,
                    bool32          do_prefetch,
                    uint32          height)
{
   platform_assert(root_addr != 0);
   debug_assert(page_type == PAGE_TYPE_MEMTABLE
                || page_type == PAGE_TYPE_BRANCH);

   debug_assert(!key_is_null(min_key) && !key_is_null(max_key)
                && !key_is_null(start_key));

   if (btree_key_compare(cfg, min_key, max_key) > 0) {
      max_key = min_key;
   }
   if (btree_key_compare(cfg, start_key, min_key) < 0) {
      start_key = min_key;
   }
   if (btree_key_compare(cfg, start_key, max_key) > 0) {
      start_key = max_key;
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
   itor->super.ops   = &btree_iterator_ops;

   find_btree_node_and_get_idx_bounds(itor, start_key, start_type);

   if (itor->do_prefetch && itor->curr.hdr->next_extent_addr != 0
       && !btree_addrs_share_extent(cc, itor->curr.addr, itor->end_addr))
   {
      // IO prefetch the next extent
      cache_prefetch(cc, itor->curr.hdr->next_extent_addr, itor->page_type);
   }

   debug_assert(!iterator_can_curr((iterator *)itor)
                || itor->idx < btree_num_entries(itor->curr.hdr));
}

void
btree_iterator_deinit(btree_iterator *itor)
{
   debug_assert(itor != NULL);
   btree_node_unget(itor->cc, itor->cfg, &itor->curr);
}

/****************************
 * B-tree packing functions
 ****************************/

// generation number isn't used in packed btrees
static inline void
btree_pack_node_init_hdr(const btree_config *cfg,
                         btree_hdr          *hdr,
                         uint64              next_extent,
                         uint8               height)
{
   btree_init_hdr(cfg, hdr);
   hdr->next_extent_addr = next_extent;
   hdr->height           = height;
}

static inline void
btree_pack_setup_start(btree_pack_req *req)
{
   req->height = 0;
   ZERO_ARRAY(req->edge);
   ZERO_ARRAY(req->edge_stats);
   ZERO_ARRAY(req->num_edges);

   // we create a root here, but we won't build it with the rest
   // of the tree, we'll copy into it at the end
   req->root_addr =
      btree_create(req->cc, req->cfg, &req->mini, PAGE_TYPE_BRANCH);

   req->num_tuples    = 0;
   req->key_bytes     = 0;
   req->message_bytes = 0;
}


static inline btree_node *
btree_pack_get_current_node(btree_pack_req *req, uint64 height)
{
   if (0 < req->num_edges[height]) {
      return &req->edge[height][req->num_edges[height] - 1];
   } else {
      return NULL;
   }
}

static inline btree_pivot_stats *
btree_pack_get_current_node_stats(btree_pack_req *req, uint64 height)
{
   debug_assert(0 < req->num_edges[height]);
   return &req->edge_stats[height][req->num_edges[height] - 1];
}

static inline btree_node *
btree_pack_create_next_node(btree_pack_req *req, uint64 height, key pivot);

/*
 * Add the specified node to its parent. Creates a parent if necessary.
 */
static inline void
btree_pack_link_node(btree_pack_req *req,
                     uint64          height,
                     uint64          offset,
                     uint64          next_extent_addr)
{
   btree_node        *edge       = &req->edge[height][offset];
   btree_pivot_stats *edge_stats = &req->edge_stats[height][offset];
   key                pivot = height ? btree_get_pivot(req->cfg, edge->hdr, 0)
                                     : btree_get_tuple_key(req->cfg, edge->hdr, 0);
   edge->hdr->next_extent_addr = next_extent_addr;
   btree_node_unlock(req->cc, req->cfg, edge);
   btree_node_unclaim(req->cc, req->cfg, edge);
   // Cannot fully unlock edge yet because the key "pivot" may point into it.

   btree_node *parent = btree_pack_get_current_node(req, height + 1);

   if (!parent
       || !btree_set_index_entry(req->cfg,
                                 parent->hdr,
                                 btree_num_entries(parent->hdr),
                                 pivot,
                                 edge->addr,
                                 *edge_stats))
   {
      btree_pack_create_next_node(req, height + 1, pivot);
      parent         = btree_pack_get_current_node(req, height + 1);
      bool32 success = btree_set_index_entry(
         req->cfg, parent->hdr, 0, pivot, edge->addr, *edge_stats);
      platform_assert(success);
   }

   btree_accumulate_pivot_stats(
      btree_pack_get_current_node_stats(req, height + 1), *edge_stats);

   btree_node_unget(req->cc, req->cfg, edge);
   memset(edge_stats, 0, sizeof(*edge_stats));
}

static inline void
btree_pack_link_extent(btree_pack_req *req,
                       uint64          height,
                       uint64          next_extent_addr)
{
   for (int i = 0; i < req->num_edges[height]; i++) {
      btree_pack_link_node(req, height, i, next_extent_addr);
   }
   req->num_edges[height] = 0;
}

static inline btree_node *
btree_pack_create_next_node(btree_pack_req *req, uint64 height, key pivot)
{
   btree_node new_node;
   uint64     node_next_extent;
   btree_alloc(req->cc,
               &req->mini,
               height,
               pivot,
               &node_next_extent,
               PAGE_TYPE_BRANCH,
               &new_node);
   btree_pack_node_init_hdr(req->cfg, new_node.hdr, 0, height);

   if (0 < req->num_edges[height]) {
      btree_node *old_node     = btree_pack_get_current_node(req, height);
      old_node->hdr->next_addr = new_node.addr;
      new_node.hdr->prev_addr  = old_node->addr;
      if (!btree_addrs_share_extent(req->cc, old_node->addr, new_node.addr)) {
         btree_pack_link_extent(req, height, new_node.addr);
      }
   }

   if (req->height < height) {
      req->height = height;
   }

   req->edge[height][req->num_edges[height]] = new_node;
   req->num_edges[height]++;
   debug_assert(btree_pack_get_current_node_stats(req, height)->num_kvs == 0);
   return &req->edge[height][req->num_edges[height] - 1];
}

static inline platform_status
btree_pack_loop(btree_pack_req *req,       // IN/OUT
                key             tuple_key, // IN
                message         msg)               // IN
{
   log_trace_key(tuple_key, "btree_pack_loop");

   if (message_is_invalid_user_type(msg)) {
      return STATUS_INVALID_STATE;
   }

   btree_node *leaf = btree_pack_get_current_node(req, 0);

   if (!leaf
       || !btree_set_leaf_entry(
          req->cfg, leaf->hdr, btree_num_entries(leaf->hdr), tuple_key, msg))
   {
      leaf = btree_pack_create_next_node(req, 0, tuple_key);
      bool32 result =
         btree_set_leaf_entry(req->cfg, leaf->hdr, 0, tuple_key, msg);
      platform_assert(result);
   }

   btree_pivot_stats *leaf_stats = btree_pack_get_current_node_stats(req, 0);
   leaf_stats->num_kvs++;
   leaf_stats->key_bytes += key_length(tuple_key);
   leaf_stats->message_bytes += message_length(msg);

   log_trace_key(tuple_key, "btree_pack_loop (bottom)");

   if (req->hash) {
      platform_assert(req->num_tuples < req->max_tuples);
      req->fingerprint_arr[req->num_tuples] =
         req->hash(key_data(tuple_key), key_length(tuple_key), req->seed);
   }

   req->num_tuples++;
   req->key_bytes += key_length(tuple_key);
   req->message_bytes += message_length(msg);
   return STATUS_OK;
}


static inline void
btree_pack_post_loop(btree_pack_req *req, key last_key)
{
   cache        *cc  = req->cc;
   btree_config *cfg = req->cfg;
   // we want to use the allocation node, so we copy the root created in the
   // loop into the btree_create root
   btree_node root;

   // if output tree is empty, deallocate any preallocated extents
   if (req->num_tuples == 0) {
      mini_destroy_unused(&req->mini);
      req->root_addr = 0;
      return;
   }

   int h = 0;
   while (h < req->height || 1 < req->num_edges[h]) {
      btree_pack_link_extent(req, h, 0);
      h++;
   }

   root.addr = req->root_addr;
   btree_node_get(cc, cfg, &root, PAGE_TYPE_BRANCH);
   debug_only bool32 success = btree_node_claim(cc, cfg, &root);
   debug_assert(success);
   btree_node_lock(cc, cfg, &root);
   memmove(root.hdr, req->edge[req->height][0].hdr, btree_page_size(cfg));
   // fix the root next extent
   root.hdr->next_extent_addr = 0;
   btree_node_full_unlock(cc, cfg, &root);

   btree_node_full_unlock(cc, cfg, &req->edge[req->height][0]);

   mini_release(&req->mini, last_key);
}

static bool32
btree_pack_can_fit_tuple(btree_pack_req *req, key tuple_key, message data)
{
   return req->num_tuples < req->max_tuples;
}

static void
btree_pack_abort(btree_pack_req *req)
{
   for (uint16 i = 0; i <= req->height; i++) {
      for (uint16 j = 0; j < req->num_edges[i]; j++) {
         btree_node_full_unlock(req->cc, req->cfg, &req->edge[i][j]);
      }
   }

   btree_dec_ref_range(req->cc,
                       req->cfg,
                       req->root_addr,
                       NEGATIVE_INFINITY_KEY,
                       POSITIVE_INFINITY_KEY);
}

/*
 *-----------------------------------------------------------------------------
 * btree_pack --
 *
 *      Packs a btree from an iterator source. Dec_Refs the
 *      output tree if it's empty.
 *
 * Returns STATUS_LIMIT_EXCEEDED if the pack results in too many kv pairs.
 * Otherwise, returns standard errors, e.g. STATUS_NO_MEMORY, etc.
 *-----------------------------------------------------------------------------
 */
platform_status
btree_pack(btree_pack_req *req)
{
   btree_pack_setup_start(req);

   key     tuple_key = NEGATIVE_INFINITY_KEY;
   message data;

   while (iterator_can_next(req->itor)) {
      iterator_curr(req->itor, &tuple_key, &data);
      if (!btree_pack_can_fit_tuple(req, tuple_key, data)) {
         platform_error_log("%s(): req->num_tuples=%lu exceeded output size "
                            "limit, req->max_tuples=%lu\n",
                            __func__,
                            req->num_tuples,
                            req->max_tuples);
         btree_pack_abort(req);
         return STATUS_LIMIT_EXCEEDED;
      }
      platform_status rc = btree_pack_loop(req, tuple_key, data);
      if (!SUCCESS(rc)) {
         platform_error_log("%s error status: %d\n", __func__, rc.r);
         btree_pack_abort(req);
         return rc;
      }
      rc = iterator_next(req->itor);
      if (!SUCCESS(rc)) {
         platform_error_log("%s error status: %d\n", __func__, rc.r);
         btree_pack_abort(req);
         return rc;
      }
   }

   btree_pack_post_loop(req, tuple_key);
   platform_assert(IMPLIES(req->num_tuples == 0, req->root_addr == 0));
   return STATUS_OK;
}

/*
 * Returns the number of kv pairs (k,v ) w/ k < key.  Also returns
 * the total size of all such keys and messages.
 */
static inline void
btree_get_rank(cache             *cc,
               btree_config      *cfg,
               uint64             root_addr,
               key                target,
               btree_pivot_stats *stats)
{
   btree_node leaf;

   btree_lookup_node(
      cc, cfg, root_addr, target, 0, PAGE_TYPE_BRANCH, &leaf, stats);
   bool32 found;
   int64  tuple_rank_in_leaf = btree_find_tuple(cfg, leaf.hdr, target, &found);
   if (!found) {
      tuple_rank_in_leaf++;
   }
   accumulate_node_ranks(cfg, leaf.hdr, 0, tuple_rank_in_leaf, stats);
   btree_node_unget(cc, cfg, &leaf);
}

/*
 * count_in_range returns the exact number of tuples in the given
 * btree between min_key (inc) and max_key (excl).
 */
void
btree_count_in_range(cache             *cc,
                     btree_config      *cfg,
                     uint64             root_addr,
                     key                min_key,
                     key                max_key,
                     btree_pivot_stats *stats)
{
   btree_pivot_stats min_stats;

   debug_assert(!key_is_null(min_key) && !key_is_null(max_key));

   btree_get_rank(cc, cfg, root_addr, min_key, &min_stats);
   btree_get_rank(cc, cfg, root_addr, max_key, stats);
   if (min_stats.num_kvs < stats->num_kvs) {
      stats->num_kvs -= min_stats.num_kvs;
      stats->key_bytes -= min_stats.key_bytes;
      stats->message_bytes -= min_stats.message_bytes;
   } else {
      memset(stats, 0, sizeof(*stats));
   }
}

/*
 * btree_count_in_range_by_iterator perform
 * btree_count_in_range using an iterator instead of by
 * calculating ranks. Used for debugging purposes.
 */
void
btree_count_in_range_by_iterator(cache             *cc,
                                 btree_config      *cfg,
                                 uint64             root_addr,
                                 key                min_key,
                                 key                max_key,
                                 btree_pivot_stats *stats)
{
   btree_iterator btree_itor;
   iterator      *itor = &btree_itor.super;
   btree_iterator_init(cc,
                       cfg,
                       &btree_itor,
                       root_addr,
                       PAGE_TYPE_BRANCH,
                       min_key,
                       max_key,
                       min_key,
                       TRUE,
                       TRUE,
                       0);

   memset(stats, 0, sizeof(*stats));

   while (iterator_can_next(itor)) {
      key     curr_key;
      message msg;
      iterator_curr(itor, &curr_key, &msg);
      stats->num_kvs++;
      stats->key_bytes += key_length(curr_key);
      stats->message_bytes += message_length(msg);
      platform_status rc = iterator_next(itor);
      platform_assert_status_ok(rc);
   }
   btree_iterator_deinit(&btree_itor);
}

/* Print offset table entries, 4 entries per line, w/ auto-indentation. */
static void
btree_print_offset_table(platform_log_handle *log_handle, btree_hdr *hdr)
{
   platform_log(log_handle, "-------------------\n");
   platform_log(log_handle, "Offset Table num_entries=%d\n", hdr->num_entries);

   uint64 nentries = btree_num_entries(hdr);
   char   fmtstr[30];
   snprintf(
      fmtstr, sizeof(fmtstr), "[%%%s] %%-8u", DECIMAL_STRING_WIDTH(nentries));

   for (int i = 0; i < btree_num_entries(hdr); i++) {
      // New-line every n-offset entries
      if (i && ((i % 4) == 0)) {
         platform_log(log_handle, "\n");
      }
      platform_log(log_handle, fmtstr, i, btree_get_table_entry(hdr, i));
   }
   platform_log(log_handle, "\n");
}

// Macro to deal with printing Pivot stats 'sz' bytes as uninitialized
#define PIVOT_STATS_BYTES_AS_STR(sz)                                           \
   (((sz) == BTREE_UNKNOWN_COUNTER) ? "BTREE_UNKNOWN_COUNTER" : size_str((sz)))

static void
btree_print_btree_pivot_stats(platform_log_handle *log_handle,
                              btree_pivot_stats   *pivot_stats)
{
   // If nothing has been updated, yet, print a msg accordingly.
   if ((pivot_stats->num_kvs == BTREE_UNKNOWN_COUNTER)
       && (pivot_stats->key_bytes == BTREE_UNKNOWN_COUNTER)
       && (pivot_stats->message_bytes == BTREE_UNKNOWN_COUNTER))
   {
      platform_log(log_handle, "   (Pivot Stats=BTREE_UNKNOWN_COUNTER)\n");
      return;
   }

   // Indentation is dictated by outer caller
   platform_log(log_handle,
                "   (num_kvs=%u\n"
                "    key_bytes=%u (%s)\n"
                "    message_bytes=%u (%s))\n",
                pivot_stats->num_kvs,
                pivot_stats->key_bytes,
                PIVOT_STATS_BYTES_AS_STR(pivot_stats->key_bytes),
                pivot_stats->message_bytes,
                PIVOT_STATS_BYTES_AS_STR(pivot_stats->message_bytes));
}

static void
btree_print_btree_pivot_data(platform_log_handle *log_handle,
                             btree_pivot_data    *pivot_data)
{
   // Indentation is dictated by outer caller
   platform_log(log_handle, "   child_addr=%lu\n", pivot_data->child_addr);
   btree_print_btree_pivot_stats(log_handle, &pivot_data->stats);
}

static void
btree_print_index_entry(platform_log_handle *log_handle,
                        btree_config        *cfg,
                        index_entry         *entry,
                        uint64               entry_num)
{
   data_config *dcfg = cfg->data_cfg;
   platform_log(log_handle,
                "[%2lu]: key=%s\n",
                entry_num,
                key_string(dcfg, index_entry_key(entry)));
   btree_print_btree_pivot_data(log_handle, &entry->pivot_data);
}

static void
btree_print_index_node(platform_log_handle *log_handle,
                       btree_config        *cfg,
                       uint64               addr,
                       btree_hdr           *hdr,
                       page_type            type)
{
   platform_log(
      log_handle, "**  Page type: %s, INDEX NODE \n", page_type_str[type]);
   platform_log(log_handle, "**  Header ptr: %p\n", hdr);
   platform_log(log_handle, "**  addr: %lu \n", addr);
   platform_log(log_handle, "**  next_addr: %lu \n", hdr->next_addr);
   platform_log(
      log_handle, "**  next_extent_addr: %lu \n", hdr->next_extent_addr);
   platform_log(log_handle, "**  generation: %lu \n", hdr->generation);
   platform_log(log_handle, "**  height: %u \n", btree_height(hdr));
   platform_log(log_handle, "**  next_entry: %u \n", hdr->next_entry);
   platform_log(log_handle, "**  num_entries: %u \n", btree_num_entries(hdr));

   btree_print_offset_table(log_handle, hdr);

   platform_log(log_handle, "-------------------\n");
   platform_log(
      log_handle, "Array of %d index entries:\n", btree_num_entries(hdr));
   for (uint64 i = 0; i < btree_num_entries(hdr); i++) {
      index_entry *entry = btree_get_index_entry(cfg, hdr, i);
      btree_print_index_entry(log_handle, cfg, entry, i);
   }
   platform_log(log_handle, "\n");
}

static void
btree_print_leaf_entry(platform_log_handle *log_handle,
                       btree_config        *cfg,
                       leaf_entry          *entry,
                       uint64               entry_num)
{
   data_config *dcfg = cfg->data_cfg;
   platform_log(log_handle,
                "[%2lu]: %s -- %s\n",
                entry_num,
                key_string(dcfg, leaf_entry_key(entry)),
                message_string(dcfg, leaf_entry_message(entry)));
}

static void
btree_print_leaf_node(platform_log_handle *log_handle,
                      btree_config        *cfg,
                      uint64               addr,
                      btree_hdr           *hdr,
                      page_type            type)
{
   platform_log(
      log_handle, "**  Page type: %s, LEAF NODE \n", page_type_str[type]);
   platform_log(log_handle, "**  hdrptr: %p\n", hdr);
   platform_log(log_handle, "**  addr: %lu \n", addr);
   platform_log(log_handle, "**  next_addr: %lu \n", hdr->next_addr);
   platform_log(
      log_handle, "**  next_extent_addr: %lu \n", hdr->next_extent_addr);
   platform_log(log_handle, "**  generation: %lu \n", hdr->generation);
   platform_log(log_handle, "**  height: %u \n", btree_height(hdr));
   platform_log(log_handle, "**  next_entry: %u \n", hdr->next_entry);
   platform_log(log_handle, "**  num_entries: %u \n", btree_num_entries(hdr));

   btree_print_offset_table(log_handle, hdr);

   platform_log(log_handle, "-------------------\n");
   platform_log(
      log_handle, "Array of %d index leaf entries:\n", btree_num_entries(hdr));
   for (uint64 i = 0; i < btree_num_entries(hdr); i++) {
      leaf_entry *entry = btree_get_leaf_entry(cfg, hdr, i);
      btree_print_leaf_entry(log_handle, cfg, entry, i);
   }
   platform_log(log_handle, "-------------------\n");
   platform_log(log_handle, "\n");
}

/*
 *-----------------------------------------------------------------------------
 * btree_print_node --
 * btree_print_tree --
 *
 *      Prints out the contents of the node/tree.
 *-----------------------------------------------------------------------------
 */
void
btree_print_locked_node(platform_log_handle *log_handle,
                        btree_config        *cfg,
                        uint64               addr,
                        btree_hdr           *hdr,
                        page_type            type)
{
   platform_log(log_handle, "*******************\n");
   platform_log(log_handle, "BTree node at addr=%lu\n{\n", addr);
   if (btree_height(hdr) > 0) {
      btree_print_index_node(log_handle, cfg, addr, hdr, type);
   } else {
      btree_print_leaf_node(log_handle, cfg, addr, hdr, type);
   }
   platform_log(log_handle, "} -- End BTree node at addr=%lu\n", addr);
}


void
btree_print_node(platform_log_handle *log_handle,
                 cache               *cc,
                 btree_config        *cfg,
                 btree_node          *node,
                 page_type            type)
{
   if (!allocator_page_valid(cache_get_allocator(cc), node->addr)) {
      platform_log(log_handle, "*******************\n");
      platform_log(log_handle, "** INVALID NODE \n");
      platform_log(log_handle, "** addr: %lu \n", node->addr);
      platform_log(log_handle, "-------------------\n");
      return;
   }
   btree_node_get(cc, cfg, node, type);
   btree_print_locked_node(log_handle, cfg, node->addr, node->hdr, type);
   btree_node_unget(cc, cfg, node);
}

void
btree_print_subtree(platform_log_handle *log_handle,
                    cache               *cc,
                    btree_config        *cfg,
                    uint64               addr,
                    page_type            type)
{
   btree_node node;
   node.addr = addr;
   if (!allocator_page_valid(cache_get_allocator(cc), node.addr)) {
      platform_log(log_handle,
                   "Unallocated %s BTree node addr=%lu\n",
                   page_type_str[type],
                   addr);
      return;
   }
   // Print node's contents only if it's a validly allocated node.
   btree_print_node(log_handle, cc, cfg, &node, type);

   btree_node_get(cc, cfg, &node, type);
   table_index idx;

   if (node.hdr->height > 0) {
      int nentries = node.hdr->num_entries;
      platform_log(log_handle,
                   "\n---- Page type: %s, BTree sub-trees under addr=%lu"
                   " num_entries=%d"
                   ", height=%d {\n",
                   page_type_str[type],
                   addr,
                   nentries,
                   node.hdr->height);

      for (idx = 0; idx < nentries; idx++) {
         platform_log(
            log_handle, "\n-- Sub-tree index=%d of %d\n", idx, nentries);
         btree_print_subtree(log_handle,
                             cc,
                             cfg,
                             btree_get_child_addr(cfg, node.hdr, idx),
                             type);
      }
      platform_log(log_handle,
                   "\n} -- End BTree sub-trees under"
                   " addr=%lu\n",
                   addr);
   }
   btree_node_unget(cc, cfg, &node);
}

/*
 * Driver routine to print a Memtable BTree starting from root_addr.
 */
void
btree_print_memtable_tree(platform_log_handle *log_handle,
                          cache               *cc,
                          btree_config        *cfg,
                          uint64               root_addr)
{
   btree_print_subtree(log_handle, cc, cfg, root_addr, PAGE_TYPE_MEMTABLE);
}

/*
 * btree_print_tree()
 *
 * Driver routine to print a BTree of page-type 'type', starting from root_addr.
 */
void
btree_print_tree(platform_log_handle *log_handle,
                 cache               *cc,
                 btree_config        *cfg,
                 uint64               root_addr,
                 page_type            type)
{
   btree_print_subtree(log_handle, cc, cfg, root_addr, type);
}

void
btree_print_tree_stats(platform_log_handle *log_handle,
                       cache               *cc,
                       btree_config        *cfg,
                       uint64               addr)
{
   btree_node node;
   node.addr = addr;
   btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);

   platform_default_log("Tree stats: height %u\n", node.hdr->height);
   cache_print_stats(log_handle, cc);

   btree_node_unget(cc, cfg, &node);
}

/*
 * returns the space used in bytes by the range [start_key, end_key) in the
 * btree
 */
uint64
btree_space_use_in_range(cache        *cc,
                         btree_config *cfg,
                         uint64        root_addr,
                         page_type     type,
                         key           start_key,
                         key           end_key)
{
   uint64 meta_head    = btree_root_to_meta_addr(cfg, root_addr, 0);
   uint64 extents_used = mini_keyed_extent_count(
      cc, cfg->data_cfg, type, meta_head, start_key, end_key);
   return extents_used * btree_extent_size(cfg);
}

bool32
btree_verify_node(cache        *cc,
                  btree_config *cfg,
                  uint64        addr,
                  page_type     type,
                  bool32        is_left_edge)
{
   btree_node node;
   node.addr = addr;
   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   btree_node_get(cc, cfg, &node, type);
   table_index idx;
   bool32      result = FALSE;

   for (idx = 0; idx < node.hdr->num_entries; idx++) {
      if (node.hdr->height == 0) {
         // leaf node
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (btree_key_compare(cfg,
                                  btree_get_tuple_key(cfg, node.hdr, idx),
                                  btree_get_tuple_key(cfg, node.hdr, idx + 1))
                >= 0)
            {
               platform_error_log("out of order tuples\n");
               platform_error_log("addr: %lu idx %2u\n", node.addr, idx);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
      } else {
         // index node
         btree_node child;
         child.addr = btree_get_child_addr(cfg, node.hdr, idx);
         btree_node_get(cc, cfg, &child, type);
         if (child.hdr->height != node.hdr->height - 1) {
            platform_error_log("height mismatch\n");
            platform_error_log("addr: %lu idx: %u\n", node.addr, idx);
            btree_node_unget(cc, cfg, &child);
            btree_node_unget(cc, cfg, &node);
            goto out;
         }
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (btree_key_compare(cfg,
                                  btree_get_pivot(cfg, node.hdr, idx),
                                  btree_get_pivot(cfg, node.hdr, idx + 1))
                >= 0)
            {
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               btree_print_tree(Platform_error_log_handle, cc, cfg, addr, type);
               platform_error_log("out of order pivots\n");
               platform_error_log("addr: %lu idx %u\n", node.addr, idx);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (0 < idx
                && btree_key_compare(cfg,
                                     btree_get_pivot(cfg, node.hdr, idx),
                                     btree_get_tuple_key(cfg, child.hdr, 0))
                      != 0)
            {
               platform_error_log(
                  "pivot key doesn't match in child and parent\n");
               platform_error_log("addr: %lu idx %u\n", node.addr, idx);
               platform_error_log("child addr: %lu\n", child.addr);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (idx != btree_num_entries(node.hdr) - 1
                && btree_key_compare(
                      cfg,
                      btree_get_pivot(cfg, node.hdr, idx + 1),
                      btree_get_tuple_key(
                         cfg, child.hdr, btree_num_entries(child.hdr) - 1))
                      < 0)
            {
               platform_error_log("child tuple larger than parent bound\n");
               platform_error_log("addr: %lu idx %u\n", node.addr, idx);
               platform_error_log("child addr: %lu idx %u\n", child.addr, idx);
               btree_print_locked_node(
                  Platform_error_log_handle, cfg, node.addr, node.hdr, type);
               btree_print_locked_node(
                  Platform_error_log_handle, cfg, child.addr, child.hdr, type);
               platform_assert(0);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         } else {
            // child index
            if (idx != btree_num_entries(node.hdr) - 1
                && btree_key_compare(
                      cfg,
                      btree_get_pivot(cfg, node.hdr, idx + 1),
                      btree_get_pivot(
                         cfg, child.hdr, btree_num_entries(child.hdr) - 1))
                      < 0)
            {
               platform_error_log("child pivot larger than parent bound\n");
               platform_error_log("addr: %lu idx %u\n", node.addr, idx);
               platform_error_log("child addr: %lu idx %u\n", child.addr, idx);
               btree_print_locked_node(
                  Platform_error_log_handle, cfg, node.addr, node.hdr, type);
               btree_print_locked_node(
                  Platform_error_log_handle, cfg, child.addr, child.hdr, type);
               platform_assert(0);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         btree_node_unget(cc, cfg, &child);
         bool32 child_is_left_edge = is_left_edge && idx == 0;
         if (!btree_verify_node(cc, cfg, child.addr, type, child_is_left_edge))
         {
            btree_node_unget(cc, cfg, &node);
            goto out;
         }
      }
   }
   btree_node_unget(cc, cfg, &node);
   result = TRUE;

out:
   return result;
}

bool32
btree_verify_tree(cache *cc, btree_config *cfg, uint64 addr, page_type type)
{
   return btree_verify_node(cc, cfg, addr, type, TRUE);
}

void
btree_print_lookup(cache        *cc,        // IN
                   btree_config *cfg,       // IN
                   uint64        root_addr, // IN
                   page_type     type,      // IN
                   key           target)              // IN
{
   btree_node node, child_node;
   uint32     h;
   int64      child_idx;

   node.addr = root_addr;
   btree_print_node(Platform_default_log_handle, cc, cfg, &node, type);
   btree_node_get(cc, cfg, &node, type);

   for (h = node.hdr->height; h > 0; h--) {
      bool32 found;
      child_idx = btree_find_pivot(cfg, node.hdr, target, &found);
      if (child_idx < 0) {
         child_idx = 0;
      }
      child_node.addr = btree_get_child_addr(cfg, node.hdr, child_idx);
      btree_print_node(Platform_default_log_handle, cc, cfg, &child_node, type);
      btree_node_get(cc, cfg, &child_node, type);
      btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   bool32 found;
   int64  idx = btree_find_tuple(cfg, node.hdr, target, &found);
   platform_default_log(
      "Matching index: %lu (%d) of %u\n", idx, found, node.hdr->num_entries);
   btree_node_unget(cc, cfg, &node);
}

/*
 *-----------------------------------------------------------------------------
 * btree_config_init --
 *
 *      Initialize btree config values
 *-----------------------------------------------------------------------------
 */
void
btree_config_init(btree_config *btree_cfg,
                  cache_config *cache_cfg,
                  data_config  *data_cfg,
                  uint64        rough_count_height)
{
   btree_cfg->cache_cfg          = cache_cfg;
   btree_cfg->data_cfg           = data_cfg;
   btree_cfg->rough_count_height = rough_count_height;

   uint64 page_size           = btree_page_size(btree_cfg);
   uint64 max_inline_key_size = MAX_INLINE_KEY_SIZE(page_size);
   uint64 max_inline_msg_size = MAX_INLINE_MESSAGE_SIZE(page_size);
   uint64 max_entry_space     = sizeof(leaf_entry) + max_inline_key_size
                            + max_inline_msg_size + sizeof(table_entry);
   platform_assert(max_entry_space < (page_size - sizeof(btree_hdr)) / 2);
}
