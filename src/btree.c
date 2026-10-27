// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "btree_private.h"
#include "data_blob_build.h"
#include "platform_sleep.h"
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
 * A leaf split additionally locks the original next leaf, which may have a
 * different parent. Claims are acquired with bounded retries. Mutable-tree
 * iterators copy a leaf and release its read lock before traversing elsewhere;
 * they never retain a leaf lock while looking up a range endpoint.
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
#define BTREE_UNKNOWN_COUNTER   UINT64_MAX
#define BTREE_MAX_KNOWN_COUNTER (BTREE_UNKNOWN_COUNTER - 1)
static const btree_pivot_stats BTREE_PIVOT_STATS_UNKNOWN = {
   BTREE_UNKNOWN_COUNTER,
   BTREE_UNKNOWN_COUNTER,
   BTREE_UNKNOWN_COUNTER};


static platform_status
btree_blob_ref_init(ondisk_ref         *ref,
                    cache              *cc,
                    const btree_config *cfg,
                    uint64              root_addr,
                    page_type           type);


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
   hdr->num_entries   = 0;
   hdr->prefix_length = 0;
   hdr->next_entry    = btree_page_size(cfg);
}


static inline uint64
index_entry_required_capacity(key pivot, btree_pivot_stats stats)
{
   return sizeof(index_entry) + key_length(pivot)
          + (btree_stats_need_wide(stats) ? BTREE_WIDE_STATS_BYTES : 0);
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
   if (!ondisk_tuple_message_is_blob(entry)) {
      return entry->message_length;
   }
   slice sblob = slice_create(entry->message_length,
                              entry->key_and_message + entry->key_length);
   return blob_length(sblob);
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
                   + index_entry_required_capacity(new_pivot_key, stats)
                <= btree_page_size(cfg));
   copy_key_to_ondisk_key(&entry->pivot, new_pivot_key);
   entry->pivot_data.child_addr = new_addr;
   index_entry_set_stats(entry, stats);
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
                 + index_entry_required_capacity(new_pivot_key, stats)
              <= hdr->next_entry + sizeof_index_entry(old_entry)))
      {
         /* special case to avoid creating fragmentation:
          * the old entry is the physically first entry in the node
          * and the new entry will fit in the space avaiable from the old
          * entry plus the free space preceding the old_entry.
          * In this case, just reset next_entry so we can insert the new entry.
          */
         hdr->next_entry += sizeof_index_entry(old_entry);
      } else if (index_entry_required_capacity(new_pivot_key, stats)
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

   if (hdr->next_entry
       < diff_ptr(hdr, &hdr->offsets[new_num_entries])
            + index_entry_required_capacity(new_pivot_key, stats))
   {
      return FALSE;
   }

   index_entry *new_entry = pointer_byte_offset(
      hdr,
      hdr->next_entry - index_entry_required_capacity(new_pivot_key, stats));
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

static bool32
btree_copy_leaf_entry(const btree_config *cfg,
                      btree_hdr          *hdr,
                      table_index         k,
                      const leaf_entry   *entry)
{
   key     entry_key = ondisk_tuple_key(entry);
   message entry_msg = ondisk_tuple_message(
      ondisk_tuple_message_is_blob(entry) ? (cache *)1 : NULL, entry);
   return btree_set_leaf_entry(cfg, hdr, k, entry_key, entry_msg);
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
      int cmp = btree_key_compare(cfg, target, btree_get_pivot(cfg, hdr, mid));
      if (cmp == 0) {
         *found = TRUE;
         return mid;
      } else if (cmp > 0) {
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
         btree_key_compare(cfg, target, btree_get_tuple_key(cfg, hdr, mid));
      if (cmp == 0) {
         *found = TRUE;
         return mid;
      } else if (cmp > 0) {
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
   if (spec->use_new_message) {
      return spec->msg.new_message;
   } else {
      return merge_accumulator_to_message(&spec->msg.modified_message);
   }
}

static inline platform_status
btree_record_old_result(const btree_config          *cfg,
                        cache                       *cc,
                        uint64                       root_addr,
                        const btree_hdr             *hdr,
                        const leaf_incorporate_spec *spec,
                        btree_insert_results        *results)
{
   lookup_result *old_result = results->old_result_buffer;
   if (old_result == NULL || spec->old_entry_state != ENTRY_STILL_EXISTS) {
      return STATUS_OK;
   }

   leaf_entry *entry    = btree_get_leaf_entry(cfg, hdr, spec->idx);
   message     old_msg  = leaf_entry_message(cc, entry);
   ondisk_ref  blob_ref = ONDISK_REF_NULL;
   if (message_is_blob(old_msg)) {
      platform_status rc =
         btree_blob_ref_init(&blob_ref, cc, cfg, root_addr, PAGE_TYPE_MEMTABLE);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   platform_status rc = lookup_result_update(
      old_result, leaf_entry_key(entry), old_msg, &blob_ref);
   ondisk_ref_deinit(&blob_ref);
   return rc;
}

static inline platform_status
btree_insert_invoke_callback(btree_insert_results *results)
{
   /*
    * Callers have acquired every lock needed by the final incorporation and
    * have passed every operation that can make the insert restart.  After a
    * successful callback they must reach incorporation without returning an
    * error or dropping the target leaf.
    */
   if (results->callback != NULL) {
      return results->callback(results->callback_arg);
   }
   return STATUS_OK;
}

platform_status
btree_create_leaf_incorporate_spec(const btree_config    *cfg,
                                   cache                 *cc,
                                   mini_allocator        *mini,
                                   platform_heap_id       heap_id,
                                   btree_hdr             *hdr,
                                   key                    tuple_key,
                                   message                msg,
                                   leaf_incorporate_spec *spec)
{
   platform_status rc = STATUS_OK;
   spec->tuple_key    = tuple_key;
   bool32 found;
   spec->idx             = btree_find_tuple(cfg, hdr, tuple_key, &found);
   spec->old_entry_state = found ? ENTRY_STILL_EXISTS : ENTRY_DID_NOT_EXIST;

   if (!found) {
      spec->idx++;
      if (message_is_blob(msg)) {
         spec->use_new_message = FALSE;
         merge_accumulator_init(&spec->msg.modified_message, heap_id);
         rc = message_clone(
            &cfg->blob_cfg, cc, mini, msg, &spec->msg.modified_message);
      } else if (MAX_INLINE_MESSAGE_SIZE(btree_page_size(cfg))
                 < message_length(msg))
      {
         spec->use_new_message = FALSE;
         merge_accumulator_init(&spec->msg.modified_message, heap_id);
         rc = message_to_blob(
            &cfg->blob_cfg, cc, mini, msg, &spec->msg.modified_message);
      } else {
         spec->use_new_message = TRUE;
         spec->msg.new_message = msg;
      }
   } else {
      leaf_entry *entry      = btree_get_leaf_entry(cfg, hdr, spec->idx);
      message     oldmessage = leaf_entry_message(cc, entry);
      bool32      success;
      spec->use_new_message = FALSE;
      success               = merge_accumulator_init_from_message(
         &spec->msg.modified_message, heap_id, msg);
      if (!success) {
         return STATUS_NO_MEMORY;
      }
      if (btree_merge_tuples(
             cfg, tuple_key, oldmessage, &spec->msg.modified_message))
      {
         merge_accumulator_deinit(&spec->msg.modified_message);
         return STATUS_NO_MEMORY;
      } else if (merge_accumulator_is_blob(&spec->msg.modified_message)) {
         /*
          * A definitive incoming blob survives the merge unchanged. Take the
          * B-tree's own reference to its complete extents before storing the
          * descriptor in the leaf; the caller may release its owner as soon as
          * this insertion returns.
          */
         merge_accumulator owned_blob;
         merge_accumulator_init(&owned_blob, heap_id);
         rc = message_clone(
            &cfg->blob_cfg,
            cc,
            mini,
            merge_accumulator_to_message(&spec->msg.modified_message),
            &owned_blob);
         if (SUCCESS(rc)) {
            merge_accumulator_deinit(&spec->msg.modified_message);
            spec->msg.modified_message = owned_blob;
         } else {
            merge_accumulator_deinit(&owned_blob);
         }
      } else if (MAX_INLINE_MESSAGE_SIZE(btree_page_size(cfg))
                 < merge_accumulator_length(&spec->msg.modified_message))
      {
         rc = merge_accumulator_convert_to_blob(
            &cfg->blob_cfg, cc, mini, &spec->msg.modified_message);
      }
   }

   if (!SUCCESS(rc) && !spec->use_new_message) {
      merge_accumulator_deinit(&spec->msg.modified_message);
   }
   return rc;
}

void
destroy_leaf_incorporate_spec(leaf_incorporate_spec *spec)
{
   if (!spec->use_new_message) {
      merge_accumulator_deinit(&spec->msg.modified_message);
   }
}

static inline bool32
btree_can_perform_leaf_incorporate_spec(const btree_config          *cfg,
                                        btree_hdr                   *hdr,
                                        const leaf_incorporate_spec *spec)
{
   if (spec->old_entry_state == ENTRY_DID_NOT_EXIST) {
      return btree_can_set_leaf_entry(
         cfg, hdr, btree_num_entries(hdr), spec->tuple_key, spec_message(spec));
   } else if (spec->old_entry_state == ENTRY_STILL_EXISTS) {
      return btree_can_set_leaf_entry(
         cfg, hdr, spec->idx, spec->tuple_key, spec_message(spec));
   } else {
      debug_assert(spec->old_entry_state == ENTRY_HAS_BEEN_REMOVED);
      return btree_can_set_leaf_entry(
         cfg, hdr, btree_num_entries(hdr), spec->tuple_key, spec_message(spec));
   }
}

bool32
btree_try_perform_leaf_incorporate_spec(const btree_config          *cfg,
                                        btree_hdr                   *hdr,
                                        const leaf_incorporate_spec *spec,
                                        uint64                      *generation)
{
   bool32  success;
   message msg = spec_message(spec);
   switch (spec->old_entry_state) {
      case ENTRY_DID_NOT_EXIST:
         success =
            btree_insert_leaf_entry(cfg, hdr, spec->idx, spec->tuple_key, msg);
         break;
      case ENTRY_STILL_EXISTS:
      {
         success =
            btree_set_leaf_entry(cfg, hdr, spec->idx, spec->tuple_key, msg);
         break;
      }
      case ENTRY_HAS_BEEN_REMOVED:
      {
         success =
            btree_insert_leaf_entry(cfg, hdr, spec->idx, spec->tuple_key, msg);
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
            btree_copy_leaf_entry(cfg, hdr, dst_idx++, entry);
         debug_assert(success);
      }
   }
}

/*
 * Bytes that a leaf's entries and offset table will occupy once the leaf has
 * been defragmented and spec has been incorporated.  Excludes the header.
 */
uint64
btree_defragmented_leaf_bytes(const btree_config          *cfg,
                              const btree_hdr             *hdr,
                              const leaf_incorporate_spec *spec)
{
   uint64 nentries   = btree_num_entries(hdr);
   uint64 live_bytes = 0;
   for (uint64 i = 0; i < nentries; i++) {
      if (spec->old_entry_state != ENTRY_STILL_EXISTS || i != spec->idx) {
         leaf_entry *entry = btree_get_leaf_entry(cfg, hdr, i);
         live_bytes += sizeof_leaf_entry(entry);
      }
   }
   // The incorporated entry replaces the old entry if it still exists.
   uint64 final_nentries =
      nentries + (spec->old_entry_state == ENTRY_STILL_EXISTS ? 0 : 1);
   return live_bytes
          + leaf_entry_required_capacity(spec->tuple_key, spec_message(spec))
          + final_nentries * sizeof(table_entry);
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
                                  btree_insert_results  *results) // IN/OUT
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
         btree_copy_leaf_entry(cfg, right_hdr, dst_idx, entry);
         dst_idx++;
      }
   }

   if (!plan.insertion_goes_left) {
      spec->idx -= plan.split_idx;
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, right_hdr, spec, &results->leaf_generation);
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
                                               index_entry_stats(entry));
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
                                               index_entry_stats(entry));
      platform_assert(succeeded);
   }
}

/*
 * Bytes that an index node's entries and offset table will occupy once the
 * node has been defragmented.  Excludes the header.
 */
uint64
btree_defragmented_index_bytes(const btree_config *cfg, const btree_hdr *hdr)
{
   uint64 nentries   = btree_num_entries(hdr);
   uint64 live_bytes = 0;
   for (uint64 i = 0; i < nentries; i++) {
      index_entry *entry = btree_get_index_entry(cfg, hdr, i);
      live_bytes += sizeof_index_entry(entry);
   }
   return live_bytes + nentries * sizeof(table_entry);
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
static platform_status
btree_alloc(cache          *cc,
            mini_allocator *mini,
            uint64          height,
            uint64         *next_extent,
            page_type       type,
            btree_node     *node)
{
   platform_status rc = mini_alloc_page(
      mini, NUM_BLOB_BATCHES + height, &node->addr, next_extent);
   if (!SUCCESS(rc)) {
      node->page = (page_handle){0};
      node->hdr  = NULL;
      return rc;
   }
   cache_alloc(cc, node->addr, type, &node->page);

   // If this btree is for a memtable then pin all pages belonging to it
   if (type == PAGE_TYPE_MEMTABLE) {
      cache_pin(cc, &node->page);
   }
   node->hdr = (btree_hdr *)(page_handle_data(&node->page));
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * btree_node_[get,release] --
 *
 *      Gets the node with appropriate lock or releases the lock.
 *-----------------------------------------------------------------------------
 */
static inline platform_status
btree_node_get(cache              *cc,
               const btree_config *cfg,
               btree_node         *node,
               page_type           type)
{
   debug_assert(node->addr != 0);

   platform_status rc = cache_get(cc, node->addr, TRUE, type, &node->page);
   node->hdr = SUCCESS(rc) ? (btree_hdr *)page_handle_data(&node->page) : NULL;
   return rc;
}

/* Branch pages are untrusted. Read their fixed-size header before any
 * variable-size entry. PAGE_TYPE_MEMTABLE remains trusted, mutable in-memory
 * state. */
static platform_status
btree_read_branch_header(const btree_config *cfg,
                         const btree_hdr    *hdr,
                         page_type           type)
{
   if (type != PAGE_TYPE_BRANCH) {
      return STATUS_OK;
   }
   if (hdr == NULL) {
      return STATUS_INVALID_STATE;
   }

   uint64 page_size = btree_page_size(cfg);
   if (page_size < sizeof(*hdr) || hdr->height >= BTREE_MAX_HEIGHT
       || hdr->prefix_length > MAX_INLINE_KEY_SIZE(page_size)
       || (hdr->height != 0 && hdr->prefix_length != 0)
       || (hdr->flags & ~(BTREE_HDR_FIRST_IN_LEVEL | BTREE_HDR_LAST_IN_LEVEL))
             != 0
       || ((hdr->page_no == 0) != btree_hdr_is_first_in_level(hdr)))
   {
      return STATUS_INVALID_STATE;
   }

   uint64 max_entries = (page_size - sizeof(*hdr)) / sizeof(hdr->offsets[0]);
   if (max_entries < hdr->num_entries) {
      return STATUS_INVALID_STATE;
   }
   uint64 table_end =
      sizeof(*hdr) + (uint64)hdr->num_entries * sizeof(hdr->offsets[0]);
   if (hdr->next_entry < table_end
       || page_size - hdr->prefix_length < hdr->next_entry
       || hdr->num_entries == 0)
   {
      return STATUS_INVALID_STATE;
   }

   return STATUS_OK;
}

static platform_status
btree_node_read_header(cache              *cc,
                       const btree_config *cfg,
                       const btree_node   *node,
                       page_type           type)
{
   if (node == NULL || page_handle_is_null(&node->page) || node->hdr == NULL) {
      return STATUS_IO_ERROR;
   }
   if (type == PAGE_TYPE_BRANCH && page_handle_addr(&node->page) != node->addr)
   {
      return STATUS_INVALID_STATE;
   }
   return btree_read_branch_header(cfg, node->hdr, type);
}

static inline platform_status
btree_read_entry_bounds(const btree_config *cfg,
                        const btree_hdr    *hdr,
                        table_index         idx,
                        uint64              min_size,
                        uint64             *offset_out,
                        uint64             *remaining_out)
{
   if (idx >= hdr->num_entries) {
      return STATUS_INVALID_STATE;
   }
   uint64 offset    = hdr->offsets[idx];
   uint64 page_size = btree_page_size(cfg) - hdr->prefix_length;
   /* The offset is uint16, and the only callers pass sizeof(leaf_entry) or
    * sizeof(index_entry). Their positive sizes cannot overflow this uint64
    * sum. Checking the header endpoint also rejects offsets at/past the end. */
   if (offset < hdr->next_entry || page_size < offset + min_size) {
      return STATUS_INVALID_STATE;
   }
   *offset_out    = offset;
   *remaining_out = page_size - offset;
   return STATUS_OK;
}

static inline platform_status
btree_read_index_key(const btree_config *cfg,
                     const btree_hdr    *hdr,
                     table_index         idx,
                     const index_entry **entry_out)
{
   uint64          offset;
   uint64          remaining;
   platform_status rc = btree_read_entry_bounds(
      cfg, hdr, idx, sizeof(index_entry), &offset, &remaining);
   if (!SUCCESS(rc)) {
      return rc;
   }
   const index_entry *entry = (const index_entry *)((const char *)hdr + offset);
   if ((entry->pivot.flags & ~BTREE_INDEX_WIDE_STATS) != 0
       || sizeof_index_entry(entry) > remaining)
   {
      return STATUS_INVALID_STATE;
   }
   *entry_out = entry;
   return STATUS_OK;
}

static inline platform_status
btree_read_index_entry(cache              *cc,
                       const btree_config *cfg,
                       const btree_hdr    *hdr,
                       table_index         idx,
                       const index_entry **entry_out)
{
   (void)cc;
   platform_status rc = btree_read_index_key(cfg, hdr, idx, entry_out);
   return rc;
}

static inline platform_status
btree_read_leaf_key(const btree_config *cfg,
                    const btree_hdr    *hdr,
                    table_index         idx,
                    const leaf_entry  **entry_out)
{
   uint64          offset;
   uint64          remaining;
   platform_status rc = btree_read_entry_bounds(
      cfg, hdr, idx, sizeof(leaf_entry), &offset, &remaining);
   if (!SUCCESS(rc)) {
      return rc;
   }
   const leaf_entry *entry = (const leaf_entry *)((const char *)hdr + offset);
   if (entry->key_flags != ONDISK_KEY_DEFAULT_FLAGS
       || entry->key_length == ONDISK_KEY_NEGATIVE_INFINITY
       || entry->key_length == ONDISK_KEY_POSITIVE_INFINITY
       || entry->key_length > remaining - sizeof(*entry)
       || (uint64)entry->key_length + hdr->prefix_length
             > MAX_INLINE_KEY_SIZE(btree_page_size(cfg)))
   {
      return STATUS_INVALID_STATE;
   }
   *entry_out = entry;
   return STATUS_OK;
}

/* Every nonzero value representable by the type bits is a valid user type.
 * Together with the allowed-flags check below, rejecting zero therefore
 * rejects exactly the same bytes as the full user-type interval check. */
_Static_assert(MESSAGE_TYPE_INVALID == 0,
               "Leaf message validation requires zero to be invalid");
_Static_assert(MESSAGE_TYPE_MAX_VALID_USER_TYPE == ONDISK_MESSAGE_TYPE_MASK,
               "All nonzero on-disk message type values must be valid");

/* Descriptor scratch is needed only for blob messages. Keep that stack frame
 * out of the ordinary inline-message validation path. */
static __attribute__((noinline)) platform_status
btree_read_leaf_blob(cache *cc, const leaf_entry *entry)
{
   parsed_blob pblob;
   slice       descriptor = slice_create(entry->message_length,
                                   entry->key_and_message + entry->key_length);
   return blob_parse_descriptor(cc, descriptor, &pblob);
}

static inline platform_status
btree_read_leaf_message(cache              *cc,
                        const btree_config *cfg,
                        const btree_hdr    *hdr,
                        const leaf_entry   *entry)
{
   uint64       offset    = (const char *)entry - (const char *)hdr;
   uint64       remaining = btree_page_size(cfg) - hdr->prefix_length - offset;
   message_type message_class = ondisk_tuple_message_class(entry);
   if ((entry->flags & ~(ONDISK_MESSAGE_TYPE_MASK | ONDISK_MESSAGE_IS_BLOB))
          != 0
       || message_class == MESSAGE_TYPE_INVALID
       || (uint64)entry->key_length + entry->message_length
             > remaining - sizeof(*entry))
   {
      return STATUS_INVALID_STATE;
   }
   if (ondisk_tuple_message_is_blob(entry)) {
      platform_status rc = btree_read_leaf_blob(cc, entry);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

_Static_assert(MAX_INLINE_KEY_SIZE(MAX_PAGE_SIZE)
                  < ONDISK_KEY_POSITIVE_INFINITY,
               "Inline keys must exclude both infinity length encodings");

static inline platform_status
btree_read_leaf_entry(cache              *cc,
                      const btree_config *cfg,
                      const btree_hdr    *hdr,
                      table_index         idx,
                      const leaf_entry  **entry_out)
{
   uint64          offset;
   uint64          remaining;
   platform_status rc = btree_read_entry_bounds(
      cfg, hdr, idx, sizeof(leaf_entry), &offset, &remaining);
   if (!SUCCESS(rc)) {
      return rc;
   }
   const leaf_entry *entry = (const leaf_entry *)((const char *)hdr + offset);
   /* Checking the full key plus message below also checks the key's byte
    * range. Keep key-only reads separate because they do not inspect messages.
    */
   /* Supported pages are at most MAX_PAGE_SIZE bytes. The widened full-key
    * bound therefore rejects both infinity length encodings as well. */
   if (entry->key_flags != ONDISK_KEY_DEFAULT_FLAGS
       || (uint64)entry->key_length + hdr->prefix_length
             > MAX_INLINE_KEY_SIZE(btree_page_size(cfg)))
   {
      return STATUS_INVALID_STATE;
   }
   rc = btree_read_leaf_message(cc, cfg, hdr, entry);
   if (SUCCESS(rc)) {
      *entry_out = entry;
   }
   return rc;
}

static platform_status
btree_find_pivot_checked(const btree_config *cfg,
                         const btree_hdr    *hdr,
                         key                 target,
                         bool32             *found,
                         int64              *idx_out,
                         const index_entry **entry_out)
{
   int64              lo = 0, hi = btree_num_entries(hdr);
   const index_entry *best = NULL;
   *found                  = FALSE;
   while (lo < hi) {
      int64              mid = (lo + hi) / 2;
      const index_entry *entry;
      platform_status    rc = btree_read_index_key(cfg, hdr, mid, &entry);
      if (!SUCCESS(rc)) {
         return rc;
      }
      int cmp = btree_key_compare(cfg, target, index_entry_key(entry));
      if (cmp == 0) {
         *found   = TRUE;
         *idx_out = mid;
         if (entry_out != NULL) {
            *entry_out = entry;
         }
         return STATUS_OK;
      }
      if (cmp > 0) {
         best = entry;
         lo   = mid + 1;
      } else {
         hi = mid;
      }
   }
   *idx_out = lo - 1;
   if (entry_out != NULL) {
      *entry_out = best;
   }
   return STATUS_OK;
}

/* The caller initializes *found and selects callback semantics once for the
 * leaf. Keep all checked-key reads before invoking the user's comparator. */
static inline platform_status
btree_find_tuple_callback_checked(const btree_config *cfg,
                                  const btree_hdr    *hdr,
                                  key                 target,
                                  bool32             *found,
                                  int64              *idx_out,
                                  const leaf_entry  **entry_out)
{
   int64 lo = 0, hi = btree_num_entries(hdr);
   while (lo < hi) {
      int64             mid = (lo + hi) / 2;
      const leaf_entry *entry;
      platform_status   rc = btree_read_leaf_key(cfg, hdr, mid, &entry);
      if (!SUCCESS(rc)) {
         return rc;
      }
      int cmp =
         btree_key_compare(cfg, target, leaf_entry_key((leaf_entry *)entry));
      if (cmp == 0) {
         *found   = TRUE;
         *idx_out = mid;
         if (entry_out != NULL) {
            *entry_out = entry;
         }
         return STATUS_OK;
      }
      if (cmp > 0) {
         lo = mid + 1;
      } else {
         hi = mid;
      }
   }
   *idx_out = lo - 1;
   if (entry_out != NULL) {
      *entry_out = NULL;
   }
   return STATUS_OK;
}

static platform_status
btree_find_tuple_checked(const btree_config *cfg,
                         const btree_hdr    *hdr,
                         key                 target,
                         bool32             *found,
                         int64              *idx_out,
                         const leaf_entry  **entry_out)
{
   int64 lo = 0, hi = btree_num_entries(hdr);
   *found            = FALSE;
   bool32 compressed = hdr->prefix_length != 0;
   if (compressed) {
      int cmp = btree_compare_prefix(cfg, hdr, target);
      if (cmp != 0) {
         *idx_out = cmp < 0 ? -1 : hi - 1;
         if (entry_out != NULL) {
            *entry_out = NULL;
         }
         return STATUS_OK;
      }
      target = key_create(target.is_query_key,
                          key_length(target) - hdr->prefix_length,
                          (const char *)key_data(target) + hdr->prefix_length);
   }
   bool32 lexical = compressed
                    || (cfg->data_cfg->key_compare_is_lexical
                        && !key_is_negative_infinity(target)
                        && !key_is_positive_infinity(target));
   if (!lexical) {
      return btree_find_tuple_callback_checked(
         cfg, hdr, target, found, idx_out, entry_out);
   }
   while (lo < hi) {
      int64             mid = (lo + hi) / 2;
      const leaf_entry *entry;
      platform_status   rc = btree_read_leaf_key(cfg, hdr, mid, &entry);
      if (!SUCCESS(rc)) {
         return rc;
      }
      int cmp = slice_lex_cmp(key_slice(target),
                              key_slice(leaf_entry_key((leaf_entry *)entry)));
      if (cmp == 0) {
         *found   = TRUE;
         *idx_out = mid;
         if (entry_out != NULL) {
            *entry_out = entry;
         }
         return STATUS_OK;
      }
      if (cmp > 0) {
         lo = mid + 1;
      } else {
         hi = mid;
      }
   }
   *idx_out = lo - 1;
   if (entry_out != NULL) {
      *entry_out = NULL;
   }
   return STATUS_OK;
}

static platform_status
btree_node_get_checked(cache              *cc,
                       const btree_config *cfg,
                       btree_node         *node,
                       page_type           type)
{
   platform_status rc = btree_node_get(cc, cfg, node, type);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = btree_node_read_header(cc, cfg, node, type);
   if (!SUCCESS(rc) && !page_handle_is_null(&node->page)) {
      btree_node_unget(cc, cfg, node);
   }
   return rc;
}

/*
 * A branch-page header is untrusted disk data. Callers must check that its
 * height agrees with the position at which the page was reached before reading
 * an entry using that position's layout.
 *
 * Memtable pages are constructed in memory and deliberately remain off this
 * query-time validation path.
 */
static inline platform_status
btree_node_check_expected_height(const btree_node *node,
                                 page_type         type,
                                 uint32            expected_height)
{
   if (type == PAGE_TYPE_BRANCH && btree_height(node->hdr) != expected_height) {
      return STATUS_INVALID_STATE;
   }
   return STATUS_OK;
}

static inline bool32
btree_node_claim(cache              *cc,  // IN
                 const btree_config *cfg, // IN
                 btree_node         *node)        // IN
{
   return cache_try_claim(cc, &node->page);
}

static inline void
btree_node_lock(cache              *cc,  // IN
                const btree_config *cfg, // IN
                btree_node         *node)        // IN
{
   cache_lock(cc, &node->page);
}

static inline void
btree_node_unlock(cache              *cc,  // IN
                  const btree_config *cfg, // IN
                  btree_node         *node)        // IN
{
   cache_unlock(cc, &node->page);
}

static inline void
btree_node_unclaim(cache              *cc,  // IN
                   const btree_config *cfg, // IN
                   btree_node         *node)        // IN
{
   cache_unclaim(cc, &node->page);
}

void
btree_node_unget(cache              *cc,  // IN
                 const btree_config *cfg, // IN
                 btree_node         *node)        // IN
{
   cache_unget(cc, &node->page);
   node->page = (page_handle){0};
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

#define BTREE_NUM_MINI_BATCHES (NUM_BLOB_BATCHES + BTREE_MAX_HEIGHT)

static const page_type memtable_page_type_table[BTREE_NUM_MINI_BATCHES] = {
   [0 ... NUM_BLOB_BATCHES - 1]                     = PAGE_TYPE_BLOB,
   [NUM_BLOB_BATCHES... BTREE_NUM_MINI_BATCHES - 1] = PAGE_TYPE_MEMTABLE,
};

static const page_type branch_page_type_table[BTREE_NUM_MINI_BATCHES] = {
   [0 ... NUM_BLOB_BATCHES - 1]                     = PAGE_TYPE_BLOB,
   [NUM_BLOB_BATCHES... BTREE_NUM_MINI_BATCHES - 1] = PAGE_TYPE_BRANCH,
};

platform_status
btree_create(cache              *cc,
             const btree_config *cfg,
             mini_allocator     *mini,
             page_type           type,
             uint64             *root_addr)
{
   *root_addr = 0;
   // get a free node for the root
   // we don't use the next_addr arr for this, since the root doesn't
   // maintain constant height
   allocator      *al = cache_get_allocator(cc);
   uint64          base_addr;
   platform_status rc = allocator_alloc(al, &base_addr, type);
   if (!SUCCESS(rc)) {
      return rc;
   }
   page_handle  root_storage;
   page_handle *root_page = cache_alloc(cc, base_addr, type, &root_storage);
   bool32       pinned    = (type == PAGE_TYPE_MEMTABLE);

   // set up the root
   btree_node root;
   root.page = *root_page;
   root.addr = base_addr;
   root.hdr  = (btree_hdr *)page_handle_data(root_page);

   btree_init_hdr(cfg, root.hdr);

   // set up the mini allocator
   rc =
      mini_init_with_types(mini,
                           cc,
                           root.addr + btree_page_size(cfg),
                           0,
                           BTREE_NUM_MINI_BATCHES,
                           type,
                           type == PAGE_TYPE_BRANCH ? branch_page_type_table
                                                    : memtable_page_type_table,
                           NULL);
   if (SUCCESS(rc) && pinned) {
      cache_pin(cc, &root.page);
   }

   cache_unlock(cc, root_page);
   cache_unclaim(cc, root_page);
   cache_unget(cc, root_page);

   if (!SUCCESS(rc)) {
      refcount ref = allocator_dec_ref(al, base_addr, type);
      platform_assert(ref == AL_NO_REFS);
      cache_extent_discard(cc, base_addr, type);
      ref = allocator_dec_ref(al, base_addr, type);
      platform_assert(ref == AL_FREE);
      return rc;
   }

   *root_addr = root.addr;
   return STATUS_OK;
}

/* The B-tree owns branch format compatibility, independent of its container. */
platform_status
btree_validate_root(uint64 root_addr, uint16 format_version, page_type type)
{
   if (type == PAGE_TYPE_BRANCH
       && format_version != BTREE_BRANCH_FORMAT_VERSION)
   {
      platform_error_log("Unsupported branch format version %u at %lu; "
                         "this build requires version %u.\n",
                         format_version,
                         root_addr,
                         BTREE_BRANCH_FORMAT_VERSION);
      return STATUS_INVALID_STATE;
   }
   if (root_addr == 0) {
      return STATUS_INVALID_STATE;
   }
   return STATUS_OK;
}

platform_status
btree_inc_ref(cache *cc, const btree_config *cfg, uint64 root_addr)
{
   uint64 meta_page_addr = btree_root_to_meta_addr(cfg, root_addr, 0);
   return mini_inc_ref(cc, meta_page_addr);
}

/*
 * Record the reference a holder has on this branch during a crash-recovery
 * rebuild, enumerating the branch's extents if this is the first one to reach
 * it.  The recovery counterpart of btree_inc_ref(); see
 * mini_recover_references().
 */
platform_status
btree_recover_allocations(cache              *cc,
                          const btree_config *cfg,
                          uint64              root_addr,
                          uint16              format_version,
                          page_type           type)
{
   platform_status version_rc =
      btree_validate_root(root_addr, format_version, type);
   if (!SUCCESS(version_rc)) {
      return version_rc;
   }

   return mini_recover_references(
      cc, btree_root_to_meta_addr(cfg, root_addr, 0), type);
}

bool32
btree_dec_ref(cache              *cc,
              const btree_config *cfg,
              uint64              root_addr,
              page_type           type)
{
   uint64   meta_head = btree_root_to_meta_addr(cfg, root_addr, 0);
   refcount ref       = mini_dec_ref(cc, meta_head, type);
   return ref == 0;
}

platform_status
btree_get_root_height(cache              *cc,
                      const btree_config *cfg,
                      uint64              root_addr,
                      uint16              format_version,
                      page_type           type,
                      uint32             *height)
{
   platform_status version_rc =
      btree_validate_root(root_addr, format_version, type);
   if (!SUCCESS(version_rc)) {
      return version_rc;
   }

   if (height == NULL
       || (type != PAGE_TYPE_BRANCH && type != PAGE_TYPE_MEMTABLE))
   {
      return STATUS_BAD_PARAM;
   }

   btree_node      root = {.addr = root_addr};
   platform_status rc   = btree_node_get_checked(cc, cfg, &root, type);
   if (!SUCCESS(rc)) {
      return rc;
   }
   *height = btree_height(root.hdr);
   btree_node_unget(cc, cfg, &root);
   return STATUS_OK;
}

static platform_status
btree_blob_ref_inc(const ondisk_ref *ref)
{
   return btree_inc_ref(ref->cc, ref->arg, ref->addr);
}

static void
btree_blob_ref_dec(const ondisk_ref *ref)
{
   btree_dec_ref(ref->cc, ref->arg, ref->addr, ref->type);
}

static platform_status
btree_blob_ref_init(ondisk_ref         *ref,
                    cache              *cc,
                    const btree_config *cfg,
                    uint64              root_addr,
                    page_type           type)
{
   return ondisk_ref_init(
      ref, cc, root_addr, type, cfg, btree_blob_ref_inc, btree_blob_ref_dec);
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
static inline platform_status
btree_split_child_leaf(cache                 *cc,
                       const btree_config    *cfg,
                       mini_allocator        *mini,
                       btree_scratch         *scratch,
                       uint64                 root_addr,
                       btree_node            *parent,
                       uint64                 index_of_child_in_parent,
                       btree_node            *child,
                       leaf_incorporate_spec *spec,
                       btree_insert_results  *results) // OUT
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

   platform_status rc = btree_alloc(cc,
                                    mini,
                                    btree_height(child->hdr),
                                    NULL,
                                    PAGE_TYPE_MEMTABLE,
                                    &right_child);
   if (!SUCCESS(rc)) {
      btree_node_unclaim(cc, cfg, parent);
      btree_node_unget(cc, cfg, parent);
      btree_node_unclaim(cc, cfg, child);
      btree_node_unget(cc, cfg, child);
      return rc;
   }
   /* p: claim, c: claim, rc: write, cn: - */

   btree_node_lock(cc, cfg, parent);
   /* p: write, c: claim, rc: write, cn: - */

   btree_node child_next;
   child_next.addr = child->hdr->next_addr;
   if (child_next.addr != 0) {
      rc = btree_node_get(cc, cfg, &child_next, PAGE_TYPE_MEMTABLE);
      if (!SUCCESS(rc)) {
         btree_node_full_unlock(cc, cfg, &right_child);
         btree_node_full_unlock(cc, cfg, parent);
         btree_node_unclaim(cc, cfg, child);
         btree_node_unget(cc, cfg, child);
         return rc;
      }
      uint64 child_next_wait = 1;
      uint64 retries         = 0;
      while (!btree_node_claim(cc, cfg, &child_next)) {
         btree_node_unget(cc, cfg, &child_next);
         retries++;
         if (100 < retries) {
            btree_node_full_unlock(cc, cfg, &right_child);
            btree_node_full_unlock(cc, cfg, parent);
            btree_node_unclaim(cc, cfg, child);
            btree_node_unget(cc, cfg, child);
            return STATUS_BUSY;
         }
         platform_sleep_ns(child_next_wait);
         child_next_wait =
            child_next_wait > 2048 ? child_next_wait : 2 * child_next_wait;
         rc = btree_node_get(cc, cfg, &child_next, PAGE_TYPE_MEMTABLE);
         if (!SUCCESS(rc)) {
            btree_node_full_unlock(cc, cfg, &right_child);
            btree_node_full_unlock(cc, cfg, parent);
            btree_node_unclaim(cc, cfg, child);
            btree_node_unget(cc, cfg, child);
            return rc;
         }
      }
      btree_node_lock(cc, cfg, &child_next);
   }
   /* p: write, c: claim, rc: write, cn: write if exists */

   btree_node_lock(cc, cfg, child);
   /* p: write, c: write, rc: write, cn: write if exists */

   rc = btree_record_old_result(cfg, cc, root_addr, child->hdr, spec, results);
   if (!SUCCESS(rc)) {
      if (child_next.addr != 0) {
         btree_node_full_unlock(cc, cfg, &child_next);
      }
      btree_node_full_unlock(cc, cfg, &right_child);
      btree_node_full_unlock(cc, cfg, parent);
      btree_node_full_unlock(cc, cfg, child);
      return rc;
   }
   rc = btree_insert_invoke_callback(results);
   if (!SUCCESS(rc)) {
      if (child_next.addr != 0) {
         btree_node_full_unlock(cc, cfg, &child_next);
      }
      btree_node_full_unlock(cc, cfg, &right_child);
      btree_node_full_unlock(cc, cfg, parent);
      btree_node_full_unlock(cc, cfg, child);
      return rc;
   }

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
      cfg, child->hdr, child->addr, spec, plan, right_child.hdr, results);
   btree_node_full_unlock(cc, cfg, &right_child);
   /* p: unlocked, c: write, rc: unlocked, cn: unlocked */

   btree_split_leaf_cleanup_left_node(
      cfg, scratch, child->hdr, spec, plan, right_child.addr);
   if (plan.insertion_goes_left) {
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, child->hdr, spec, &results->leaf_generation);
      platform_assert(incorporated);
   }
   btree_node_full_unlock(cc, cfg, child);
   /* p: unlocked, c: unlocked, rc: unlocked, cn: unlocked */

   return STATUS_OK;
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
static inline platform_status
btree_defragment_or_split_child_leaf(cache              *cc,
                                     const btree_config *cfg,
                                     mini_allocator     *mini,
                                     btree_scratch      *scratch,
                                     uint64              root_addr,
                                     btree_node         *parent,
                                     uint64      index_of_child_in_parent,
                                     btree_node *child,
                                     leaf_incorporate_spec *spec,
                                     btree_insert_results  *results) // OUT
{
   log_trace_leaf(cfg, child->hdr, "btree_defragment_or_split_child_leaf");

   uint64 total_space_required =
      btree_defragmented_leaf_bytes(cfg, child->hdr, spec);

   if (total_space_required < BTREE_SPLIT_THRESHOLD(btree_page_size(cfg))) {
      btree_node_unclaim(cc, cfg, parent);
      btree_node_unget(cc, cfg, parent);
      btree_node_lock(cc, cfg, child);
      platform_status rc =
         btree_record_old_result(cfg, cc, root_addr, child->hdr, spec, results);
      if (!SUCCESS(rc)) {
         btree_node_full_unlock(cc, cfg, child);
         return rc;
      }
      rc = btree_insert_invoke_callback(results);
      if (!SUCCESS(rc)) {
         btree_node_full_unlock(cc, cfg, child);
         return rc;
      }
      btree_defragment_leaf(cfg, scratch, child->hdr, spec);
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, child->hdr, spec, &results->leaf_generation);
      platform_assert(incorporated);
      btree_node_full_unlock(cc, cfg, child);
   } else {
      return btree_split_child_leaf(cc,
                                    cfg,
                                    mini,
                                    scratch,
                                    root_addr,
                                    parent,
                                    index_of_child_in_parent,
                                    child,
                                    spec,
                                    results);
   }

   return STATUS_OK;
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
static inline platform_status
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

   platform_status rc = btree_alloc(cc,
                                    mini,
                                    btree_height(child->hdr),
                                    NULL,
                                    PAGE_TYPE_MEMTABLE,
                                    &right_child);
   if (!SUCCESS(rc)) {
      btree_node_full_unlock(cc, cfg, parent);
      btree_node_full_unlock(cc, cfg, child);
      return rc;
   }

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
   return STATUS_OK;
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
static inline platform_status
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
   uint64 total_space_required =
      btree_defragmented_index_bytes(cfg, child->hdr);

   if (total_space_required < BTREE_SPLIT_THRESHOLD(btree_page_size(cfg))) {
      btree_node_full_unlock(cc, cfg, parent);
      btree_defragment_index(cfg, scratch, child->hdr);
      *new_child = *child;
   } else {
      return btree_split_child_index(cc,
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

   return STATUS_OK;
}


static inline uint64
add_unknown(uint64 a, uint64 b)
{
   if (a > BTREE_MAX_KNOWN_COUNTER || b > BTREE_MAX_KNOWN_COUNTER - a) {
      return BTREE_UNKNOWN_COUNTER;
   }
   return a + b;
}

static inline void
btree_accumulate_pivot_stats(btree_pivot_stats *dest, btree_pivot_stats src)
{
   dest->num_kvs       = add_unknown(dest->num_kvs, src.num_kvs);
   dest->key_bytes     = add_unknown(dest->key_bytes, src.key_bytes);
   dest->message_bytes = add_unknown(dest->message_bytes, src.message_bytes);
}

static inline platform_status
accumulate_node_ranks(cache              *cc,
                      const btree_config *cfg,
                      const btree_hdr    *hdr,
                      page_type           type,
                      int                 from,
                      int                 to,
                      btree_pivot_stats  *stats)
{
   debug_assert(from <= to);
   if (btree_height(hdr) == 0) {
      for (int i = from; i < to; i++) {
         const leaf_entry *entry;
         if (type == PAGE_TYPE_BRANCH) {
            platform_status rc = btree_read_leaf_entry(cc, cfg, hdr, i, &entry);
            if (!SUCCESS(rc)) {
               return rc;
            }
         } else {
            entry = btree_get_leaf_entry(cfg, hdr, i);
         }
         stats->key_bytes = add_unknown(
            stats->key_bytes, leaf_entry_key_size(entry) + hdr->prefix_length);
         stats->message_bytes =
            add_unknown(stats->message_bytes, leaf_entry_message_size(entry));
      }
      stats->num_kvs = add_unknown(stats->num_kvs, to - from);
   } else {
      for (int i = from; i < to; i++) {
         const index_entry *entry;
         if (type == PAGE_TYPE_BRANCH) {
            platform_status rc =
               btree_read_index_entry(cc, cfg, hdr, i, &entry);
            if (!SUCCESS(rc)) {
               return rc;
            }
         } else {
            entry = btree_get_index_entry(cfg, hdr, i);
         }
         btree_accumulate_pivot_stats(stats, index_entry_stats(entry));
      }
   }
   return STATUS_OK;
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
static inline platform_status
btree_grow_root(cache              *cc,   // IN
                const btree_config *cfg,  // IN
                mini_allocator     *mini, // IN/OUT
                btree_node         *root_node)    // OUT
{
   // allocate a new left node
   btree_node      child;
   platform_status rc = btree_alloc(
      cc, mini, btree_height(root_node->hdr), NULL, PAGE_TYPE_MEMTABLE, &child);
   if (!SUCCESS(rc)) {
      return rc;
   }

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
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * btree_insert --
 *
 *      Inserts the tuple into the dynamic btree.
 *-----------------------------------------------------------------------------
 */
platform_status
btree_insert(cache                *cc,        // IN
             const btree_config   *cfg,       // IN
             platform_heap_id      heap_id,   // IN
             btree_scratch        *scratch,   // IN
             uint64                root_addr, // IN
             mini_allocator       *mini,      // IN
             key                   tuple_key, // IN
             message               msg,       // IN
             btree_insert_results *results)   // IN/OUT
{
   platform_status       rc;
   leaf_incorporate_spec spec;

   if (MAX_INLINE_KEY_SIZE(btree_page_size(cfg)) < key_length(tuple_key)) {
      return STATUS_BAD_PARAM;
   }

   if (message_is_invalid_user_type(msg)) {
      return STATUS_BAD_PARAM;
   }

   btree_node root_node;
   root_node.addr = root_addr;

   log_trace_key(tuple_key, "btree_insert");

   if (results->old_result_buffer != NULL) {
      lookup_result_reset(results->old_result_buffer);
   }

start_over:
   rc = btree_node_get(cc, cfg, &root_node, PAGE_TYPE_MEMTABLE);
   if (!SUCCESS(rc)) {
      return rc;
   }
   uint64 leaf_wait = 1;

   if (btree_height(root_node.hdr) == 0) {
      rc = btree_create_leaf_incorporate_spec(
         cfg, cc, mini, heap_id, root_node.hdr, tuple_key, msg, &spec);
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
      if (btree_can_perform_leaf_incorporate_spec(cfg, root_node.hdr, &spec)) {
         rc = btree_record_old_result(
            cfg, cc, root_addr, root_node.hdr, &spec, results);
         if (!SUCCESS(rc)) {
            btree_node_full_unlock(cc, cfg, &root_node);
            destroy_leaf_incorporate_spec(&spec);
            return rc;
         }
         rc = btree_insert_invoke_callback(results);
         if (!SUCCESS(rc)) {
            btree_node_full_unlock(cc, cfg, &root_node);
            destroy_leaf_incorporate_spec(&spec);
            return rc;
         }
         bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
            cfg, root_node.hdr, &spec, &results->leaf_generation);
         platform_assert(incorporated);
         btree_node_full_unlock(cc, cfg, &root_node);
         destroy_leaf_incorporate_spec(&spec);
         return STATUS_OK;
      }
      destroy_leaf_incorporate_spec(&spec);
      rc = btree_grow_root(cc, cfg, mini, &root_node);
      if (!SUCCESS(rc)) {
         btree_node_full_unlock(cc, cfg, &root_node);
         return rc;
      }
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
                                   index_entry_stats(parent_entry));
      }
      if (btree_index_is_full(cfg, root_node.hdr)) {
         rc = btree_grow_root(cc, cfg, mini, &root_node);
         if (!SUCCESS(rc)) {
            btree_node_full_unlock(cc, cfg, &root_node);
            return rc;
         }
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
                                  index_entry_stats(parent_entry));
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
#if SPLINTER_DEBUG
   bool child_node_is_valid =
      allocator_page_valid(cache_get_allocator(cc), child_node.addr);
   if (!child_node_is_valid) {
      btree_print_tree(Platform_default_log_handle,
                       cc,
                       (btree_config *)cfg,
                       parent_node.addr,
                       PAGE_TYPE_MEMTABLE);
   }
#endif // SPLINTER_DEBUG

   debug_assert(child_node_is_valid,
                "parent_node.addr=%lu, child_node.addr=%lu\n",
                parent_node.addr,
                child_node.addr);

   rc = btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
   if (!SUCCESS(rc)) {
      btree_node_unget(cc, cfg, &parent_node);
      return rc;
   }

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
                                      index_entry_stats(child_entry));
         }

         if (btree_index_is_full(cfg, child_node.hdr)) {
            btree_node new_child;
            rc = btree_defragment_or_split_child_index(cc,
                                                       cfg,
                                                       mini,
                                                       scratch,
                                                       &parent_node,
                                                       child_idx,
                                                       &child_node,
                                                       tuple_key,
                                                       &new_child,
                                                       &next_child_idx);
            if (!SUCCESS(rc)) {
               return rc;
            }
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
                                     index_entry_stats(child_entry));
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
      debug_assert(index_entry_stats(parent_entry).num_kvs
                   == BTREE_UNKNOWN_COUNTER);
      debug_assert(index_entry_stats(parent_entry).key_bytes
                   == BTREE_UNKNOWN_COUNTER);
      debug_assert(index_entry_stats(parent_entry).message_bytes
                   == BTREE_UNKNOWN_COUNTER);
      child_node.addr = index_entry_child_addr(parent_entry);
      debug_assert(
         allocator_page_valid(cache_get_allocator(cc), child_node.addr));
      rc = btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
      if (!SUCCESS(rc)) {
         btree_node_unget(cc, cfg, &parent_node);
         return rc;
      }
      height--;
   }

   /*
    * - read lock on parent_node, parent_node is an index, parent node
    *   min key is up to date, and parent_node will not need to split.
    * - read lock on child_node
    * - height of parent == 1
    */
   rc = btree_create_leaf_incorporate_spec(
      cfg, cc, mini, heap_id, child_node.hdr, tuple_key, msg, &spec);
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
      rc = btree_record_old_result(
         cfg, cc, root_addr, child_node.hdr, &spec, results);
      if (!SUCCESS(rc)) {
         btree_node_full_unlock(cc, cfg, &child_node);
         destroy_leaf_incorporate_spec(&spec);
         return rc;
      }
      rc = btree_insert_invoke_callback(results);
      if (!SUCCESS(rc)) {
         btree_node_full_unlock(cc, cfg, &child_node);
         destroy_leaf_incorporate_spec(&spec);
         return rc;
      }
      bool32 incorporated = btree_try_perform_leaf_incorporate_spec(
         cfg, child_node.hdr, &spec, &results->leaf_generation);
      platform_assert(incorporated);
      btree_node_full_unlock(cc, cfg, &child_node);
      destroy_leaf_incorporate_spec(&spec);
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
      rc        = btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
      if (!SUCCESS(rc)) {
         btree_node_unclaim(cc, cfg, &parent_node);
         btree_node_unget(cc, cfg, &parent_node);
         destroy_leaf_incorporate_spec(&spec);
         return rc;
      }
      need_to_rebuild_spec = TRUE;
   }
   if (need_to_rebuild_spec) {
      /* If we had to relenquish our lock, then our spec might be out of date,
       * so rebuild it.
       */
      destroy_leaf_incorporate_spec(&spec);
      rc = btree_create_leaf_incorporate_spec(
         cfg, cc, mini, heap_id, child_node.hdr, tuple_key, msg, &spec);
      if (!SUCCESS(rc)) {
         btree_node_unclaim(cc, cfg, &parent_node);
         btree_node_unget(cc, cfg, &parent_node);
         btree_node_unclaim(cc, cfg, &child_node);
         btree_node_unget(cc, cfg, &child_node);
         return rc;
      }
   }
   rc = btree_defragment_or_split_child_leaf(cc,
                                             cfg,
                                             mini,
                                             scratch,
                                             root_addr,
                                             &parent_node,
                                             child_idx,
                                             &child_node,
                                             &spec,
                                             results);
   destroy_leaf_incorporate_spec(&spec);
   if (STATUS_IS_EQ(rc, STATUS_BUSY)) {
      goto start_over;
   }
   if (!SUCCESS(rc)) {
      return rc;
   }
   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 * btree_lookup_node --
 *
 *      lookup_node finds the node of height stop_at_height with
 *      (node.min_key <= key < node.max_key) and returns it with a read lock
 *      held if get_node is true. If get_node is false, only out_node->addr is
 *      valid and no read lock is held on the target node.
 *
 *      out_rank returns the rank of out_node amount nodes of height
 *      stop_at_height.
 *
 *      If any change is made here, please change
 *      btree_lookup_async_with_ref too.
 *-----------------------------------------------------------------------------
 */
static platform_status
btree_lookup_node_internal(cache              *cc,             // IN
                           const btree_config *cfg,            // IN
                           uint64              root_addr,      // IN
                           key                 target,         // IN
                           uint16              stop_at_height, // IN
                           page_type           type,           // IN
                           bool32              get_node,       // IN
                           btree_node         *out_node,       // OUT
                           btree_pivot_stats  *stats)           // OUT
{
   btree_node node, child_node;
   uint32     h;
   int64      child_idx;

   if (stats) {
      memset(stats, 0, sizeof(*stats));
   }

   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   bool32 validate_on_demand = type == PAGE_TYPE_BRANCH;
   node.addr                 = root_addr;
   platform_status rc        = btree_node_get_checked(cc, cfg, &node, type);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (type == PAGE_TYPE_BRANCH && btree_height(node.hdr) < stop_at_height) {
      btree_node_unget(cc, cfg, &node);
      return STATUS_INVALID_STATE;
   }

   for (h = btree_height(node.hdr); h > stop_at_height; h--) {
      bool32             found;
      const index_entry *entry = NULL;
      if (key_is_positive_infinity(target)) {
         child_idx = btree_num_entries(node.hdr) - 1;
      } else if (validate_on_demand) {
         rc = btree_find_pivot_checked(
            cfg, node.hdr, target, &found, &child_idx, &entry);
         if (!SUCCESS(rc)) {
            btree_node_unget(cc, cfg, &node);
            return rc;
         }
      } else {
         child_idx = btree_find_pivot(cfg, node.hdr, target, &found);
      }
      if (child_idx < 0) {
         child_idx = 0;
      }
      if (validate_on_demand) {
         if (entry == NULL) {
            rc = btree_read_index_entry(cc, cfg, node.hdr, child_idx, &entry);
            if (!SUCCESS(rc)) {
               btree_node_unget(cc, cfg, &node);
               return rc;
            }
         }
      } else {
         entry = btree_get_index_entry(cfg, node.hdr, child_idx);
      }
      child_node.addr = index_entry_child_addr(entry);

      if (stats) {
         rc =
            accumulate_node_ranks(cc, cfg, node.hdr, type, 0, child_idx, stats);
         if (!SUCCESS(rc)) {
            btree_node_unget(cc, cfg, &node);
            return rc;
         }
      }

      if (!get_node && h == stop_at_height + 1) {
         out_node->addr = child_node.addr;
         out_node->page = (page_handle){0};
         out_node->hdr  = NULL;
         btree_node_unget(cc, cfg, &node);
         return STATUS_OK;
      }

      rc = btree_node_get_checked(cc, cfg, &child_node, type);
      if (!SUCCESS(rc)) {
         btree_node_unget(cc, cfg, &node);
         return rc;
      }
      rc = btree_node_check_expected_height(&child_node, type, h - 1);
      if (!SUCCESS(rc)) {
         btree_node_unget(cc, cfg, &child_node);
         btree_node_unget(cc, cfg, &node);
         return rc;
      }
      debug_assert(page_handle_addr(&child_node.page) == child_node.addr);
      btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   if (get_node) {
      *out_node = node;
   } else {
      out_node->addr = node.addr;
      out_node->page = (page_handle){0};
      out_node->hdr  = NULL;
      btree_node_unget(cc, cfg, &node);
   }
   return STATUS_OK;
}

platform_status
btree_lookup_node(cache              *cc,             // IN
                  const btree_config *cfg,            // IN
                  uint64              root_addr,      // IN
                  key                 target,         // IN
                  uint16              stop_at_height, // IN
                  page_type           type,           // IN
                  bool32              get_node,       // IN
                  btree_node         *out_node,       // OUT
                  btree_pivot_stats  *stats)           // OUT
{
   return btree_lookup_node_internal(cc,
                                     cfg,
                                     root_addr,
                                     target,
                                     stop_at_height,
                                     type,
                                     get_node,
                                     out_node,
                                     stats);
}

/*
 * IN Parameters:
 * - state->cc: the cache
 * - state->cfg: the btree config
 * - state->root_addr: the root address of the btree
 * - state->type: the type of the root node
 * - state->target: the key to look up
 * - state->stop_at_height: the height to stop at
 * - state->get_node: whether to return the node or only its address
 *
 * OUT Parameters:
 * - state->node: the node found if get_node is true, otherwise the node address
 * - state->stats: the stats of the node found
 *
 * LOCAL Variables:
 * - state->h: the height of the current node
 * - state->child_node: the child node
 */
static inline async_status
btree_lookup_node_async(btree_lookup_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   if (state->stats) {
      memset(state->stats, 0, sizeof(*state->stats));
   }

   debug_assert(state->type == PAGE_TYPE_BRANCH
                || state->type == PAGE_TYPE_MEMTABLE);
   state->node.addr      = state->root_addr;
   state->__async_result = STATUS_OK;
   async_await_call(state,
                    cache_get_async,
                    &state->cache_get_state,
                    state->cc,
                    state->node.addr,
                    state->type,
                    state->callback,
                    state->callback_arg);
   {
      platform_status rc = cache_get_async_state_result(&state->cache_get_state,
                                                        &state->node.page);
      if (!SUCCESS(rc)) {
         async_return(state, rc);
      }
   }
   state->node.hdr = (btree_hdr *)page_handle_data(&state->node.page);
   platform_status rc =
      btree_node_read_header(state->cc, state->cfg, &state->node, state->type);
   if (!SUCCESS(rc)) {
      btree_node_unget(state->cc, state->cfg, &state->node);
      async_return(state, rc);
   }
   if (state->type == PAGE_TYPE_BRANCH
       && btree_height(state->node.hdr) < state->stop_at_height)
   {
      btree_node_unget(state->cc, state->cfg, &state->node);
      async_return(state, STATUS_INVALID_STATE);
   }

   for (state->h = btree_height(state->node.hdr);
        state->h > state->stop_at_height;
        state->h--)
   {
      bool32             found;
      int64              child_idx;
      const index_entry *entry = NULL;
      if (key_is_positive_infinity(state->target)) {
         child_idx = btree_num_entries(state->node.hdr) - 1;
      } else if (state->type == PAGE_TYPE_BRANCH) {
         rc = btree_find_pivot_checked(state->cfg,
                                       state->node.hdr,
                                       state->target,
                                       &found,
                                       &child_idx,
                                       &entry);
         if (!SUCCESS(rc)) {
            btree_node_unget(state->cc, state->cfg, &state->node);
            async_return(state, rc);
         }
      } else {
         child_idx = btree_find_pivot(
            state->cfg, state->node.hdr, state->target, &found);
      }
      if (child_idx < 0) {
         child_idx = 0;
      }
      if (state->type == PAGE_TYPE_BRANCH) {
         if (entry == NULL) {
            rc = btree_read_index_entry(
               state->cc, state->cfg, state->node.hdr, child_idx, &entry);
            if (!SUCCESS(rc)) {
               btree_node_unget(state->cc, state->cfg, &state->node);
               async_return(state, rc);
            }
         }
      } else {
         entry = btree_get_index_entry(state->cfg, state->node.hdr, child_idx);
      }
      state->child_node.addr = index_entry_child_addr(entry);

      if (state->stats) {
         rc = accumulate_node_ranks(state->cc,
                                    state->cfg,
                                    state->node.hdr,
                                    state->type,
                                    0,
                                    child_idx,
                                    state->stats);
         if (!SUCCESS(rc)) {
            btree_node_unget(state->cc, state->cfg, &state->node);
            async_return(state, rc);
         }
      }

      if (!state->get_node && state->h == state->stop_at_height + 1) {
         btree_node_unget(state->cc, state->cfg, &state->node);
         state->node.addr = state->child_node.addr;
         state->node.page = (page_handle){0};
         state->node.hdr  = NULL;
         async_return(state);
      }

      async_await_call(state,
                       cache_get_async,
                       &state->cache_get_state,
                       state->cc,
                       state->child_node.addr,
                       state->type,
                       state->callback,
                       state->callback_arg);
      platform_status get_rc = cache_get_async_state_result(
         &state->cache_get_state, &state->child_node.page);
      if (!SUCCESS(get_rc)) {
         btree_node_unget(state->cc, state->cfg, &state->node);
         async_return(state, get_rc);
      }
      state->child_node.hdr =
         (btree_hdr *)page_handle_data(&state->child_node.page);

      rc = btree_node_read_header(
         state->cc, state->cfg, &state->child_node, state->type);
      if (!SUCCESS(rc)) {
         btree_node_unget(state->cc, state->cfg, &state->child_node);
         btree_node_unget(state->cc, state->cfg, &state->node);
         async_return(state, rc);
      }
      rc = btree_node_check_expected_height(
         &state->child_node, state->type, state->h - 1);
      if (!SUCCESS(rc)) {
         btree_node_unget(state->cc, state->cfg, &state->child_node);
         btree_node_unget(state->cc, state->cfg, &state->node);
         async_return(state, rc);
      }

      debug_assert(page_handle_addr(&state->child_node.page)
                   == state->child_node.addr);
      btree_node_unget(state->cc, state->cfg, &state->node);
      state->node = state->child_node;
   }

   if (!state->get_node) {
      state->child_node.addr = state->node.addr;
      btree_node_unget(state->cc, state->cfg, &state->node);
      state->node.addr = state->child_node.addr;
      state->node.page = (page_handle){0};
      state->node.hdr  = NULL;
   }
   async_return(state);
}

/*
 * IN Parameters:
 * - state->cc: the cache
 * - state->cfg: the btree config
 * - state->root_addr: the root address of the btree
 * - state->type: the type of the root node
 * - state->target: the key to look up
 *
 * OUT Parameters:
 * - state->node: the node found
 * - state->found_key: the key found in the leaf, or NULL_KEY otherwise.
 * - state->msg: the message of the target
 *
 * LOCAL Variables:
 * - state->stats: the stats of the node found
 * - state->stop_at_height: the height to stop at
 * - state->h: the height of the current node
 * - state->child_node: the child node
 */
static inline async_status
btree_lookup_with_ref_async(btree_lookup_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   state->stop_at_height = 0;
   state->stats          = NULL;
   state->get_node       = TRUE;
   async_await_subroutine(state, btree_lookup_node_async);
   if (!SUCCESS(state->__async_result)) {
      async_return(state);
   }

   state->found_key = NULL_KEY;
   bool32            found;
   int64             idx;
   const leaf_entry *entry = NULL;
   platform_status   rc    = STATUS_OK;
   if (state->type == PAGE_TYPE_BRANCH) {
      rc = btree_find_tuple_checked(
         state->cfg, state->node.hdr, state->target, &found, &idx, &entry);
      if (!SUCCESS(rc)) {
         btree_node_unget(state->cc, state->cfg, &state->node);
         async_return(state, rc);
      }
   } else {
      idx =
         btree_find_tuple(state->cfg, state->node.hdr, state->target, &found);
   }
   if (found) {
      if (state->type == PAGE_TYPE_BRANCH) {
         rc = btree_read_leaf_message(
            state->cc, state->cfg, state->node.hdr, entry);
         if (!SUCCESS(rc)) {
            btree_node_unget(state->cc, state->cfg, &state->node);
            async_return(state, rc);
         }
      } else {
         entry = btree_get_leaf_entry(state->cfg, state->node.hdr, idx);
      }
      state->found_key = state->node.hdr->prefix_length == 0
                            ? leaf_entry_key((leaf_entry *)entry)
                            : key_create(FALSE,
                                         key_length(state->target),
                                         key_data(state->target));
      state->msg       = leaf_entry_message(state->cc, (leaf_entry *)entry);
   } else {
      btree_node_unget(state->cc, state->cfg, &state->node);
   }
   async_return(state);
}


static inline platform_status
btree_lookup_with_ref(cache              *cc,        // IN
                      const btree_config *cfg,       // IN
                      uint64              root_addr, // IN
                      page_type           type,      // IN
                      key                 target,    // IN
                      btree_node         *node,      // OUT
                      key                *found_key, // OUT
                      message            *msg)                  // OUT
{
   platform_status rc    = STATUS_OK;
   bool32          found = FALSE;

   *found_key                = NULL_KEY;
   bool32 validate_on_demand = type == PAGE_TYPE_BRANCH;
   rc                        = btree_lookup_node_internal(
      cc, cfg, root_addr, target, 0, type, TRUE, node, NULL);
   if (!SUCCESS(rc)) {
      return rc;
   }
   int64             idx;
   const leaf_entry *entry = NULL;
   if (validate_on_demand) {
      rc =
         btree_find_tuple_checked(cfg, node->hdr, target, &found, &idx, &entry);
      if (!SUCCESS(rc)) {
         btree_node_unget(cc, cfg, node);
         return rc;
      }
   } else {
      idx = btree_find_tuple(cfg, node->hdr, target, &found);
   }
   if (found) {
      if (validate_on_demand) {
         rc = btree_read_leaf_message(cc, cfg, node->hdr, entry);
         if (!SUCCESS(rc)) {
            btree_node_unget(cc, cfg, node);
            return rc;
         }
      } else {
         entry = btree_get_leaf_entry(cfg, node->hdr, idx);
      }
      *found_key = node->hdr->prefix_length == 0
                      ? leaf_entry_key((leaf_entry *)entry)
                      : key_create(FALSE, key_length(target), key_data(target));
      *msg       = leaf_entry_message(cc, (leaf_entry *)entry);
   } else {
      btree_node_unget(cc, cfg, node);
   }
   return rc;
}

platform_status
btree_lookup(cache         *cc,             // IN
             btree_config  *cfg,            // IN
             uint64         root_addr,      // IN
             uint16         format_version, // IN
             page_type      type,           // IN
             key            target,         // IN
             lookup_result *result)         // OUT
{
   lookup_result_reset(result);
   return btree_lookup_and_merge(
      cc, cfg, root_addr, format_version, type, target, result, NULL);
}

platform_status
btree_lookup_and_merge(cache              *cc,             // IN
                       const btree_config *cfg,            // IN
                       uint64              root_addr,      // IN
                       uint16              format_version, // IN
                       page_type           type,           // IN
                       key                 target,         // IN
                       lookup_result      *result,         // IN/OUT
                       bool32             *local_found)                // OUT
{
   if (local_found != NULL) {
      *local_found = FALSE;
   }

   platform_status version_rc =
      btree_validate_root(root_addr, format_version, type);
   if (!SUCCESS(version_rc)) {
      return version_rc;
   }

   btree_node      node;
   key             found_key;
   message         local_data;
   platform_status rc = STATUS_OK;

   log_trace_key(target, "btree_lookup");

   rc = btree_lookup_with_ref(
      cc, cfg, root_addr, type, target, &node, &found_key, &local_data);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (!key_is_null(found_key)) {
      ondisk_ref blob_ref = ONDISK_REF_NULL;
      if (local_found != NULL) {
         *local_found = TRUE;
      }
      if (message_is_blob(local_data)) {
         rc = btree_blob_ref_init(&blob_ref, cc, cfg, root_addr, type);
      }
      if (SUCCESS(rc)) {
         rc = lookup_result_update(result, found_key, local_data, &blob_ref);
      }
      ondisk_ref_deinit(&blob_ref);
      btree_node_unget(cc, cfg, &node);
   }
   return rc;
}

/*
 * IN Parameters:
 * - state->cc: the cache
 * - state->cfg: the btree config
 * - state->root_addr: the root address of the btree
 * - state->type: the type of the root node
 * - state->target: the key to look up
 *
 * OUT Parameters:
 * - state->found_key: the key found in the leaf, or NULL_KEY otherwise.
 *
 * LOCAL Variables:
 * - state->node: the node found
 * - state->stats: the stats of the node found
 * - state->stop_at_height: the height to stop at
 * - state->h: the height of the current node
 * - state->child_node: the child node
 * - state->msg: the message of the target
 */
async_status
btree_lookup_and_merge_async(btree_lookup_async_state *state)
{
   async_begin(state, 0);

   platform_status version_rc =
      btree_validate_root(state->root_addr, state->format_version, state->type);
   if (!SUCCESS(version_rc)) {
      async_return(state, version_rc);
   }

   async_await_subroutine(state, btree_lookup_with_ref_async);

   if (!SUCCESS(state->__async_result)) {
      async_return(state, state->__async_result);
   }

   platform_status rc = STATUS_OK;
   if (!key_is_null(state->found_key)) {
      ondisk_ref blob_ref = ONDISK_REF_NULL;
      if (message_is_blob(state->msg)) {
         rc = btree_blob_ref_init(
            &blob_ref, state->cc, state->cfg, state->root_addr, state->type);
      }
      if (SUCCESS(rc)) {
         rc = lookup_result_update(state->result,
                                   state->found_key,
                                   state->msg,
                                   ondisk_ref_is_null(&blob_ref) ? NULL
                                                                 : &blob_ref);
      }
      ondisk_ref_deinit(&blob_ref);
      btree_node_unget(state->cc, state->cfg, &state->node);
   }
   async_return(state, rc);
}

async_status
btree_lookup_async(btree_lookup_async_state *state)
{
   lookup_result_reset(state->result);
   return btree_lookup_and_merge_async(state);
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
 * This iterator implementation supports lower and upper bound keys. The lower
 * bound may be exclusive or inclusive, and the upper bound may be exclusive or
 * inclusive.
 *
 * Bounds are computed once per leaf, not compared against every returned key.
 * Immutable trees precompute the end leaf address and defer end_idx until that
 * leaf is reached. Mutable-tree iterators copy each visited leaf, release its
 * read reference, and compute a local upper bound from the copied keys.
 *
 * The iterator interacts with concurrent updates to the tree as
 * follows.  Its guarantees very much depend on the fact that we do
 * not delete entries in the tree.
 *
 * The iterator is guaranteed to see all keys that are between the
 * lower and upper bounds and that were present in the tree when the
 * iterator was initialized.
 *
 * A copy retains the keys and successor link from before any subsequent split.
 * Its upper bound must therefore depend on its own keys, not on a live-tree
 * endpoint address that may name a newly split page skipped by that link.
 *-----------------------------------------------------------------------------
 */
static inline bool32
btree_iterator_curr_is_copy(btree_iterator *itor)
{
   return itor->curr.hdr != NULL
          && itor->curr.hdr == (btree_hdr *)itor->node_copy;
}

// helper function to find a key within a btree node
// at a height specified by the iterator
static inline platform_status
find_key_in_node(btree_iterator *itor,
                 btree_hdr      *hdr,
                 key             target,
                 comparison      position_rule,
                 bool32         *found,
                 int64          *idx_out)
{
   bool32 loc_found;
   if (found == NULL) {
      found = &loc_found;
   }

   int64           tmp;
   platform_status rc = STATUS_OK;
   if (itor->height == 0) {
      if (itor->page_type == PAGE_TYPE_BRANCH) {
         rc =
            btree_find_tuple_checked(itor->cfg, hdr, target, found, &tmp, NULL);
      } else {
         tmp = btree_find_tuple(itor->cfg, hdr, target, found);
      }
   } else if (itor->height > hdr->height) {
      // so we will always exceed height in future lookups
      itor->height = (uint32)-1;
      *found       = FALSE;
      *idx_out     = 0;
      return STATUS_OK;
   } else {
      if (itor->page_type == PAGE_TYPE_BRANCH) {
         rc =
            btree_find_pivot_checked(itor->cfg, hdr, target, found, &tmp, NULL);
      } else {
         tmp = btree_find_pivot(itor->cfg, hdr, target, found);
      }
   }
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
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
   *idx_out = tmp;
   return STATUS_OK;
}


/* Uncompressed leaves return keys directly from the pinned page. Keep their
 * iterators small and avoid initializing buffers they will never use. */
typedef struct btree_iterator_prefix_state {
   key_buffer decoded_key;
   uint64     prefix_page_addr;
   // Cache the buffer's logical high-water length, not its allocation size.
   // Keeping that length above the prefix preserves it when the buffer grows.
   char  *decoded_data;
   uint64 decoded_capacity;
} btree_iterator_prefix_state;

static platform_status
btree_iterator_ensure_prefix_state(btree_iterator *itor)
{
   // Use the page's prefix, not the construction policy, to choose this path:
   // compressed leaves can be reopened with compression disabled.
   if (itor->prefix_state == NULL) {
      itor->prefix_state =
         TYPED_ZALLOC(PROCESS_PRIVATE_HEAP_ID, itor->prefix_state);
      if (itor->prefix_state == NULL) {
         return STATUS_NO_MEMORY;
      }
      key_buffer_init(&itor->prefix_state->decoded_key,
                      PROCESS_PRIVATE_HEAP_ID);
   }
   return STATUS_OK;
}

/* Keep allocation, buffer growth and prefix replacement off the ordinary
 * per-key path. The suffix copy stays inline, including on compressed leaves.
 */
static __attribute__((noinline)) platform_status
btree_iterator_prepare_decode(btree_iterator *itor, uint64 key_length)
{
   platform_status rc = btree_iterator_ensure_prefix_state(itor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   btree_iterator_prefix_state *state = itor->prefix_state;
   if (key_buffer_length(&state->decoded_key) < key_length) {
      rc = key_buffer_resize(&state->decoded_key, key_length);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   state->decoded_data     = key_buffer_data(&state->decoded_key);
   state->decoded_capacity = key_buffer_length(&state->decoded_key);
   if (state->prefix_page_addr != itor->curr.addr) {
      memcpy(state->decoded_data,
             btree_leaf_prefix(itor->cfg, itor->curr.hdr),
             itor->curr.hdr->prefix_length);
      state->prefix_page_addr = itor->curr.addr;
   }
   return STATUS_OK;
}

/* Compare target with a node key without reconstructing compressed keys.
 * Branch entries retain the same validation as reads of full keys. */
static inline platform_status
btree_iterator_compare_node_key(btree_iterator *itor,
                                btree_hdr      *hdr,
                                uint64          idx,
                                key             target,
                                int            *result)
{
   if (itor->page_type != PAGE_TYPE_BRANCH) {
      key node_key = itor->height ? btree_get_pivot(itor->cfg, hdr, idx)
                                  : btree_get_tuple_key(itor->cfg, hdr, idx);
      *result      = btree_key_compare(itor->cfg, target, node_key);
      return STATUS_OK;
   }

   if (itor->height) {
      const index_entry *entry;
      platform_status    rc = btree_read_index_key(itor->cfg, hdr, idx, &entry);
      if (SUCCESS(rc)) {
         *result = btree_key_compare(itor->cfg, target, index_entry_key(entry));
      }
      return rc;
   }

   const leaf_entry *entry;
   platform_status   rc = btree_read_leaf_key(itor->cfg, hdr, idx, &entry);
   if (SUCCESS(rc)) {
      *result = btree_compare_leaf_key(itor->cfg, hdr, target, entry);
   }
   return rc;
}

/*
 * For copied leaves, end_addr is zero while the range continues into another
 * leaf. Only a terminating copy sets end_addr to curr.addr and caches its
 * exclusive upper bound in end_idx. Interior leaves thus retain the cheap
 * address-only bounds check. Copies always have valid bounds; no live-tree
 * endpoint lookup is needed. A split after copying may move max_key to a new
 * leaf that the copy's old next pointer deliberately skips.
 */
static inline platform_status
btree_iterator_set_copy_bounds(btree_iterator *itor)
{
   itor->end_addr      = itor->curr.addr;
   itor->end_idx_valid = TRUE;
   int64  num_entries  = btree_num_entries(itor->curr.hdr);
   bool32 has_next =
      btree_node_next_addr(itor->cfg, &itor->curr, itor->page_type) != 0;
   if (key_is_positive_infinity(itor->max_key) || num_entries == 0) {
      itor->end_addr = has_next ? 0 : itor->curr.addr;
      itor->end_idx  = num_entries;
      return STATUS_OK;
   }

   int             cmp;
   platform_status rc = btree_iterator_compare_node_key(
      itor, itor->curr.hdr, num_entries - 1, itor->max_key, &cmp);
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
   }
   if (cmp > 0) {
      itor->end_addr = has_next ? 0 : itor->curr.addr;
      itor->end_idx  = num_entries;
   } else {
      rc = find_key_in_node(itor,
                            itor->curr.hdr,
                            itor->max_key,
                            comparison_invert(itor->max_key_comparison),
                            NULL,
                            &itor->end_idx);
   }
   return rc;
}

static inline platform_status
btree_iterator_copy_curr_if_needed(btree_iterator *itor)
{
   if (!itor->copy_nodes || page_handle_is_null(&itor->curr.page)) {
      return STATUS_OK;
   }

   debug_assert(itor->node_copy != NULL);
   memcpy(itor->node_copy, itor->curr.hdr, btree_page_size(itor->cfg));
   btree_node_unget(itor->cc, itor->cfg, &itor->curr);
   itor->curr.hdr = (btree_hdr *)itor->node_copy;
   return btree_iterator_set_copy_bounds(itor);
}

static inline void
btree_iterator_release_curr(btree_iterator *itor)
{
   if (!page_handle_is_null(&itor->curr.page)) {
      btree_node_unget(itor->cc, itor->cfg, &itor->curr);
   } else {
      itor->curr.hdr = NULL;
   }
}

static inline platform_status
btree_iterator_get_curr_addr(btree_iterator *itor, uint64 addr)
{
   itor->curr.addr = addr;
   platform_status rc =
      btree_node_get_checked(itor->cc, itor->cfg, &itor->curr, itor->page_type);
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
   }
   rc = btree_node_check_expected_height(
      &itor->curr, itor->page_type, itor->height);
   if (!SUCCESS(rc)) {
      btree_node_unget(itor->cc, itor->cfg, &itor->curr);
      itor->validation_status = rc;
      return rc;
   }
   return btree_iterator_copy_curr_if_needed(itor);
}

static inline platform_status
btree_iterator_check_branch_page_no(btree_iterator *itor,
                                    uint64          expected_page_no)
{
   if (itor->page_type == PAGE_TYPE_BRANCH
       && itor->curr.hdr->page_no != expected_page_no)
   {
      itor->validation_status = STATUS_INVALID_STATE;
      return itor->validation_status;
   }
   return STATUS_OK;
}

static inline platform_status
btree_iterator_curr_live_prev_addr(btree_iterator *itor, uint64 *prev_addr)
{
   if (!btree_iterator_curr_is_copy(itor)) {
      *prev_addr =
         btree_node_prev_addr(itor->cfg, &itor->curr, itor->page_type);
      return STATUS_OK;
   }

   btree_node live_curr;
   live_curr.addr = itor->curr.addr;
   platform_status rc =
      btree_node_get_checked(itor->cc, itor->cfg, &live_curr, itor->page_type);
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
   }
   rc = btree_node_check_expected_height(
      &live_curr, itor->page_type, itor->height);
   if (!SUCCESS(rc)) {
      btree_node_unget(itor->cc, itor->cfg, &live_curr);
      itor->validation_status = rc;
      return rc;
   }
   *prev_addr = btree_node_prev_addr(itor->cfg, &live_curr, itor->page_type);
   btree_node_unget(itor->cc, itor->cfg, &live_curr);
   return STATUS_OK;
}

static inline platform_status
btree_iterator_end_key_beyond_curr(btree_iterator *itor, bool32 *result)
{
   debug_assert(itor->curr.addr == itor->end_addr);
   uint64 num_entries = btree_num_entries(itor->curr.hdr);

   if (key_is_positive_infinity(itor->max_key)) {
      *result =
         btree_node_next_addr(itor->cfg, &itor->curr, itor->page_type) != 0;
      return STATUS_OK;
   }
   if (num_entries == 0 || itor->height > btree_height(itor->curr.hdr)) {
      *result =
         num_entries == 0
         && btree_node_next_addr(itor->cfg, &itor->curr, itor->page_type) != 0;
      return STATUS_OK;
   }

   int             cmp;
   platform_status rc = btree_iterator_compare_node_key(
      itor, itor->curr.hdr, num_entries - 1, itor->max_key, &cmp);
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
   }
   *result =
      cmp > 0
      && btree_node_next_addr(itor->cfg, &itor->curr, itor->page_type) != 0;
   return STATUS_OK;
}

static platform_status
btree_iterator_find_end_addr(btree_iterator *itor)
{
   if (itor->curr.hdr != NULL && itor->curr.addr == itor->root_addr) {
      itor->end_addr      = itor->root_addr;
      itor->end_idx_valid = FALSE;
      return STATUS_OK;
   }

   btree_node      end;
   platform_status rc = btree_lookup_node(itor->cc,
                                          itor->cfg,
                                          itor->root_addr,
                                          itor->max_key,
                                          itor->height,
                                          itor->page_type,
                                          FALSE,
                                          &end,
                                          NULL);
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
   }
   itor->end_addr      = end.addr;
   itor->end_idx_valid = FALSE;
   return STATUS_OK;
}

static inline platform_status
btree_iterator_set_end_idx(btree_iterator *itor)
{
   debug_assert(itor->curr.addr == itor->end_addr);
   debug_assert(!itor->end_idx_valid);

   if (key_is_positive_infinity(itor->max_key)) {
      itor->end_idx       = btree_num_entries(itor->curr.hdr);
      itor->end_idx_valid = TRUE;
      return STATUS_OK;
   } else {
      platform_status rc =
         find_key_in_node(itor,
                          itor->curr.hdr,
                          itor->max_key,
                          comparison_invert(itor->max_key_comparison),
                          NULL,
                          &itor->end_idx);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   itor->end_idx_valid = TRUE;
   return STATUS_OK;
}

/*
 * Complete all fallible upper-bound preparation for the current position.
 * can_next(), can_prev(), and curr() are observation-only, so every operation
 * that can change the current page or range position calls this before
 * returning success.
 */
static platform_status
btree_iterator_prepare_end_idx(btree_iterator *itor)
{
   while (itor->curr.addr == itor->end_addr && !itor->end_idx_valid) {
      debug_assert(!itor->copy_nodes);
      bool32          beyond_curr;
      platform_status rc =
         btree_iterator_end_key_beyond_curr(itor, &beyond_curr);
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (beyond_curr) {
         uint64 old_end_addr = itor->end_addr;
         rc                  = btree_iterator_find_end_addr(itor);
         if (!SUCCESS(rc)) {
            return rc;
         }
         if (itor->curr.addr != itor->end_addr) {
            return STATUS_OK;
         }
         if (old_end_addr != itor->end_addr) {
            continue;
         }
      }

      rc = btree_iterator_set_end_idx(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

static bool32
btree_iterator_can_prev(iterator *base_itor)
{
   btree_iterator *itor = (btree_iterator *)base_itor;
   if (!SUCCESS(itor->validation_status)) {
      return FALSE;
   }
   debug_assert(itor->curr.addr != itor->end_addr || itor->end_idx_valid);
   return itor->idx >= itor->curr_min_idx
          && (itor->curr.addr != itor->end_addr
              || itor->curr_min_idx != itor->end_idx);
}

static bool32
btree_iterator_can_next(iterator *base_itor)
{
   btree_iterator *itor = (btree_iterator *)base_itor;
   if (!SUCCESS(itor->validation_status)) {
      return FALSE;
   }
   debug_assert(itor->curr.addr != itor->end_addr || itor->end_idx_valid);
   return itor->curr.addr != itor->end_addr
          || (itor->idx < itor->end_idx && itor->curr_min_idx != itor->end_idx);
}

/* Keep the uncompressed entry-validation path small. Compressed leaves pay
 * this call after validation; their decode algorithm and buffer ownership are
 * unchanged. */
static __attribute__((noinline)) platform_status
btree_iterator_decode_current(btree_iterator *itor, const leaf_entry *entry)
{
   uint64                       prefix_length = itor->curr.hdr->prefix_length;
   uint64                       key_length = prefix_length + entry->key_length;
   btree_iterator_prefix_state *state      = itor->prefix_state;
   platform_status              rc         = STATUS_OK;
   if (state == NULL || state->decoded_capacity < key_length
       || state->prefix_page_addr != itor->curr.addr)
   {
      rc    = btree_iterator_prepare_decode(itor, key_length);
      state = itor->prefix_state;
   }
   if (SUCCESS(rc)) {
      btree_copy_key_suffix(state->decoded_data + prefix_length,
                            entry->key_and_message,
                            entry->key_length);
   }
   return rc;
}

/*
 * curr() cannot report errors. Every positioning operation therefore reads
 * and checks the entry it is about to expose before returning success.
 */
static platform_status
btree_iterator_check_current(btree_iterator *itor)
{
   itor->validated_entry = NULL;
   if (itor->page_type != PAGE_TYPE_BRANCH
       || !btree_iterator_can_next(&itor->super)
       || !btree_iterator_can_prev(&itor->super))
   {
      return STATUS_OK;
   }

   platform_status rc;
   if (btree_height(itor->curr.hdr) == 0) {
      const leaf_entry *entry;
      rc = btree_read_leaf_entry(
         itor->cc, itor->cfg, itor->curr.hdr, itor->idx, &entry);
      if (SUCCESS(rc)) {
         itor->validated_entry = entry;
         if (itor->curr.hdr->prefix_length != 0) {
            rc = btree_iterator_decode_current(itor, entry);
         }
      }
   } else {
      const index_entry *entry;
      rc = btree_read_index_entry(
         itor->cc, itor->cfg, itor->curr.hdr, itor->idx, &entry);
      if (SUCCESS(rc)) {
         itor->validated_entry = entry;
      }
   }
   if (!SUCCESS(rc)) {
      itor->validated_entry   = NULL;
      itor->validation_status = rc;
   }
   return rc;
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
   if (!page_handle_is_null(&itor->curr.page)) {
      debug_assert(page_handle_addr(&itor->curr.page) == itor->curr.addr);
      debug_assert((char *)itor->curr.hdr
                   == page_handle_data(&itor->curr.page));
      cache_validate_page(itor->cc, &itor->curr.page, itor->curr.addr);
   } else {
      debug_assert(btree_iterator_curr_is_copy(itor));
   }
   if (itor->curr.hdr->height == 0) {
      leaf_entry *entry;
      if (itor->page_type == PAGE_TYPE_BRANCH) {
         debug_assert(itor->validated_entry != NULL);
         entry = (leaf_entry *)itor->validated_entry;
      } else {
         entry = btree_get_leaf_entry(itor->cfg, itor->curr.hdr, itor->idx);
      }
      *curr_key = itor->curr.hdr->prefix_length == 0
                     ? leaf_entry_key(entry)
                     : key_create(FALSE,
                                  (uint64)itor->curr.hdr->prefix_length
                                     + entry->key_length,
                                  itor->prefix_state->decoded_data);
      *data     = leaf_entry_message(itor->cc, entry);
      log_trace_key(*curr_key, "btree_iterator_get_curr");
   } else {
      index_entry *entry;
      if (itor->page_type == PAGE_TYPE_BRANCH) {
         debug_assert(itor->validated_entry != NULL);
         entry = (index_entry *)itor->validated_entry;
      } else {
         entry = btree_get_index_entry(itor->cfg, itor->curr.hdr, itor->idx);
      }
      *curr_key        = index_entry_key(entry);
      itor->pivot_data = (btree_pivot_data){
         .child_addr = entry->pivot_data.child_addr,
         .stats      = index_entry_stats(entry),
      };
      *data = message_create(
         MESSAGE_TYPE_PIVOT_DATA,
         NULL,
         slice_create(sizeof(itor->pivot_data), &itor->pivot_data));
   }
}

static async_status
btree_iterator_find_end_addr_async(btree_iterator_async_state *state,
                                   uint64                      depth)
{
   async_begin(state, depth);

   if (state->itor->curr.hdr != NULL
       && state->itor->curr.addr == state->itor->root_addr)
   {
      state->itor->end_addr      = state->itor->root_addr;
      state->itor->end_idx_valid = FALSE;
      async_return(state);
   }

   btree_lookup_async_state_init(&state->lookup_state,
                                 state->itor->cc,
                                 state->itor->cfg,
                                 state->itor->root_addr,
                                 state->format_version,
                                 state->itor->page_type,
                                 state->itor->max_key,
                                 NULL,
                                 state->callback,
                                 state->callback_arg);
   state->lookup_state.stop_at_height = state->itor->height;
   state->lookup_state.stats          = NULL;
   state->lookup_state.get_node       = FALSE;
   async_await(state,
               btree_lookup_node_async(&state->lookup_state, 0)
                  == ASYNC_STATUS_DONE);
   if (!SUCCESS(state->lookup_state.__async_result)) {
      state->itor->validation_status = state->lookup_state.__async_result;
      async_return(state, state->lookup_state.__async_result);
   }
   state->itor->end_addr      = state->lookup_state.node.addr;
   state->itor->end_idx_valid = FALSE;

   async_return(state);
}

static async_status
btree_iterator_prepare_end_idx_async(btree_iterator_async_state *state,
                                     uint64                      depth)
{
   async_begin(state, depth);

   while (state->itor->curr.addr == state->itor->end_addr
          && !state->itor->end_idx_valid)
   {
      debug_assert(!state->itor->copy_nodes);
      bool32          beyond_curr;
      platform_status rc =
         btree_iterator_end_key_beyond_curr(state->itor, &beyond_curr);
      if (!SUCCESS(rc)) {
         async_return(state, rc);
      }
      if (beyond_curr) {
         state->old_end_addr = state->itor->end_addr;
         async_await_subroutine(state, btree_iterator_find_end_addr_async);
         if (!SUCCESS(state->__async_result)) {
            async_return(state, state->__async_result);
         }
         if (state->itor->curr.addr != state->itor->end_addr) {
            async_return(state, STATUS_OK);
         }
         if (state->old_end_addr != state->itor->end_addr) {
            continue;
         }
      }

      rc = btree_iterator_set_end_idx(state->itor);
      if (!SUCCESS(rc)) {
         async_return(state, rc);
      }
   }

   async_return(state, STATUS_OK);
}

/*
 * ----------------------------------------------------------------------------
 * btree_prefetch_cursor --
 *
 *    Bidirectional extent-prefetcher for a btree_iterator. It reads extent
 *    addresses ahead of (or behind) the iterator from the branch's
 *    mini_allocator (via a mini_meta_cursor) and issues cache_prefetch for
 *    them, keeping ~lookahead leaf extents of IO in flight. Within the
 *    iterator's level (batch), the extents are in key order, so the cursor
 *    advances in lockstep with consumption. Internal-node extents are skipped;
 *    blob extents are prefetched for height-0 scans. See btree_prefetch_cursor
 *    in btree.h.
 * ----------------------------------------------------------------------------
 */
static inline uint64
btree_extent_base_addr(cache *cc, uint64 addr)
{
   allocator *al = cache_get_allocator(cc);
   return allocator_config_extent_base_addr(allocator_get_config(al), addr);
}

static inline mini_meta_cursor_status
btree_prefetch_cursor_step(btree_prefetch_cursor *pf,
                           uint64                *extent_addr,
                           uint64                *batch)
{
   mini_meta_cursor_status status =
      pf->going_forward ? mini_meta_cursor_next(&pf->meta_cursor)
                        : mini_meta_cursor_prev(&pf->meta_cursor);
   if (status == MINI_META_CURSOR_ENTRY) {
      mini_meta_cursor_curr(&pf->meta_cursor, extent_addr, batch);
   }
   return status;
}

/*
 * Issue prefetches until ~depth leaf extents are in flight, or the stream is
 * exhausted. Leaf extents count toward the depth; blob extents in the window
 * are prefetched but not counted; internal-node extents are skipped. Forward
 * fill stops at end_addr. Backward fill stops at the beginning of the extent
 * stream. Non-blocking: if a meta page needed to read further ahead isn't
 * resident yet, fill stops early (a single-page prefetch was issued) and
 * resumes later.
 */
static void
btree_prefetch_cursor_fill(btree_iterator *itor)
{
   btree_prefetch_cursor *pf = &itor->prefetch;
   while (!pf->at_end && pf->prefetched_ahead < pf->depth) {
      uint64                  extent_addr;
      uint64                  batch;
      mini_meta_cursor_status status =
         btree_prefetch_cursor_step(pf, &extent_addr, &batch);
      if (status == MINI_META_CURSOR_WOULD_BLOCK) {
         break;
      }
      if (status == MINI_META_CURSOR_INVALID) {
         mini_meta_cursor_deinit(&pf->meta_cursor);
         pf->state  = BTREE_PREFETCH_DISABLED;
         pf->at_end = TRUE;
         break;
      }
      if (status == MINI_META_CURSOR_END) {
         pf->at_end = TRUE;
         break;
      }
      if (batch == pf->leaf_batch) {
         cache_prefetch(itor->cc, extent_addr, itor->page_type);
         pf->prefetched_ahead++;
         if (pf->going_forward && itor->end_addr != 0
             && btree_addrs_share_extent(itor->cc, extent_addr, itor->end_addr))
         {
            pf->at_end = TRUE;
         }
      } else if (pf->prefetch_blobs && batch < NUM_BLOB_BATCHES) {
         cache_prefetch(itor->cc, extent_addr, PAGE_TYPE_BLOB);
      }
   }
}

/*
 * Try to position the (PRIMING) cursor on the iterator's current leaf extent.
 * Non-blocking: kicks off meta-page IO and leaves the cursor PRIMING when the
 * page is not resident yet. Reads the current leaf's meta_page_addr every call,
 * so it positions correctly even if the iterator advanced while priming. Fill
 * starts by moving in the scan direction, so the current extent is not
 * prefetched. Returns TRUE iff the cursor just became ACTIVE.
 */
static bool32
btree_prefetch_cursor_pump(btree_iterator *itor)
{
   btree_prefetch_cursor *pf = &itor->prefetch;

   uint64 meta_page_addr = itor->curr.hdr->meta_page_addr;
   if (meta_page_addr == 0) {
      // No extent list is available; use the header extent links.
      pf->state = BTREE_PREFETCH_DISABLED;
      return FALSE;
   }

   mini_meta_cursor_deinit(&pf->meta_cursor);
   uint64 cur_extent = btree_extent_base_addr(itor->cc, itor->curr.addr);
   mini_meta_cursor_status status = mini_meta_cursor_init(
      &pf->meta_cursor, itor->cc, itor->page_type, meta_page_addr, cur_extent);
   if (status == MINI_META_CURSOR_WOULD_BLOCK) {
      return FALSE;
   }
   if (status != MINI_META_CURSOR_ENTRY) {
      mini_meta_cursor_deinit(&pf->meta_cursor);
      pf->state = BTREE_PREFETCH_DISABLED;
      return FALSE;
   }

   pf->state            = BTREE_PREFETCH_ACTIVE;
   pf->at_end           = FALSE;
   pf->prefetched_ahead = 0;
   pf->depth            = BTREE_PREFETCH_RAMP_MIN;
   btree_prefetch_cursor_fill(itor);
   return TRUE;
}

/*
 * Called when the iterator crosses into a new leaf extent while the cursor is
 * ACTIVE: account for the consumed extent, ramp the depth toward the configured
 * cap, and refill the lookahead window.
 */
static void
btree_prefetch_cursor_on_boundary(btree_iterator *itor)
{
   btree_prefetch_cursor *pf = &itor->prefetch;
   if (pf->prefetched_ahead > 0) {
      pf->prefetched_ahead--;
   }
   if (pf->depth < pf->lookahead) {
      pf->depth *= 2;
      if (pf->depth > pf->lookahead) {
         pf->depth = pf->lookahead;
      }
   }
   btree_prefetch_cursor_fill(itor);
}

static inline bool32
btree_iterator_prefetch_enabled(btree_iterator *itor)
{
   return itor->prefetch.lookahead > 0;
}

/*
 * (Re)start deep prefetch at the iterator's current leaf. Non-blocking: kicks
 * off meta-page IO and leaves the cursor PRIMING unless the meta page is
 * already resident. Leaves the deep cursor DISABLED when deep prefetch does not
 * apply; the header extent links may still prefetch one extent.
 */
static void
btree_prefetch_cursor_start(btree_iterator *itor, bool32 going_forward)
{
   btree_prefetch_cursor *pf = &itor->prefetch;

   mini_meta_cursor_deinit(&pf->meta_cursor);
   pf->started          = TRUE;
   pf->state            = BTREE_PREFETCH_DISABLED;
   pf->going_forward    = going_forward;
   pf->at_end           = FALSE;
   pf->prefetched_ahead = 0;
   pf->depth            = BTREE_PREFETCH_RAMP_MIN;
   pf->leaf_batch       = NUM_BLOB_BATCHES + itor->height;
   pf->prefetch_blobs   = (itor->height == 0);

   if (!btree_iterator_prefetch_enabled(itor) || pf->lookahead <= 1
       || itor->page_type != PAGE_TYPE_BRANCH
       || page_handle_is_null(&itor->curr.page))
   {
      return;
   }

   pf->state = BTREE_PREFETCH_PRIMING;
   btree_prefetch_cursor_pump(itor);
}

/* Release the cursor's resources and turn it off. */
static void
btree_prefetch_cursor_deinit(btree_iterator *itor)
{
   btree_prefetch_cursor *pf = &itor->prefetch;
   mini_meta_cursor_deinit(&pf->meta_cursor);
   pf->started          = FALSE;
   pf->state            = BTREE_PREFETCH_DISABLED;
   pf->at_end           = FALSE;
   pf->prefetched_ahead = 0;
}

/*
 * Drive prefetching after the iterator advances one leaf, in either direction.
 * On a direction change, restarts the cursor in the new direction (resetting
 * the ramp). Otherwise pumps the non-blocking prime while PRIMING. Deep
 * prefetch refills after an extent-boundary crossing; header-link prefetch runs
 * after a crossing or direction restart, when the adjacent extent link is
 * useful and not just a duplicate prefetch for the current extent.
 */
static void
btree_iterator_prefetch_on_advance(btree_iterator *itor,
                                   uint64          last_addr,
                                   bool32          going_forward)
{
   cache                 *cc        = itor->cc;
   btree_prefetch_cursor *pf        = &itor->prefetch;
   bool32                 restarted = FALSE;

   // Iterator initialization and seek deliberately leave prefetch dormant.
   // The scan starts it only when it approaches an extent boundary.
   if (!pf->started) {
      return;
   }

   // Direction change: restart cursor in the new direction, resetting ramp.
   if (pf->state != BTREE_PREFETCH_DISABLED
       && pf->going_forward != going_forward)
   {
      btree_prefetch_cursor_start(itor, going_forward);
      restarted = TRUE;
   }

   bool32 positioned_now = FALSE;
   if (pf->state == BTREE_PREFETCH_PRIMING) {
      positioned_now = btree_prefetch_cursor_pump(itor);
   }
   bool32 crossed_extent =
      !btree_addrs_share_extent(cc, last_addr, itor->curr.addr);
   if (pf->state == BTREE_PREFETCH_ACTIVE) {
      if (crossed_extent && !positioned_now && !restarted) {
         btree_prefetch_cursor_on_boundary(itor);
      }
   } else if (btree_iterator_prefetch_enabled(itor)
              && (crossed_extent || restarted))
   {
      uint64 extent_addr = going_forward
                              ? btree_node_next_extent_addr(
                                   itor->cfg, &itor->curr, itor->page_type)
                              : btree_node_prev_extent_addr(
                                   itor->cfg, &itor->curr, itor->page_type);

      if (extent_addr != 0
          && (!going_forward || itor->end_addr == 0
              || !btree_addrs_share_extent(
                 cc, itor->curr.addr, itor->end_addr)))
      {
         cache_prefetch(cc, extent_addr, itor->page_type);
      }
   }
}

/*
 * Start read-ahead only when the scan reaches the last page of an extent in
 * its current direction. Starting it during iterator initialization reads an
 * extent for every branch even when the caller consumes only a few tuples.
 *
 * A direction change discards the old cursor. If the current page is also a
 * boundary page in the new direction, it is immediately restarted there;
 * otherwise it remains dormant until that boundary is reached.
 */
static void
btree_iterator_maybe_start_prefetch(btree_iterator *itor, bool32 going_forward)
{
   btree_prefetch_cursor *pf = &itor->prefetch;
   if (!btree_iterator_prefetch_enabled(itor)) {
      return;
   }

   if (pf->started && pf->going_forward != going_forward) {
      btree_prefetch_cursor_deinit(itor);
   }
   if (pf->started) {
      return;
   }

   uint64 page_size        = btree_page_size(itor->cfg);
   uint64 extent_size      = btree_extent_size(itor->cfg);
   uint64 page_in_extent   = (itor->curr.addr % extent_size) / page_size;
   uint64 pages_per_extent = extent_size / page_size;
   if ((going_forward && page_in_extent + 1 != pages_per_extent)
       || (!going_forward && page_in_extent != 0))
   {
      return;
   }

   uint64 adjacent_extent =
      going_forward
         ? btree_node_next_extent_addr(itor->cfg, &itor->curr, itor->page_type)
         : btree_node_prev_extent_addr(itor->cfg, &itor->curr, itor->page_type);

   // Remember that this boundary was considered even if the range or branch
   // ends here, so every tuple on the page does not repeat the same checks.
   pf->started       = TRUE;
   pf->going_forward = going_forward;

   if (adjacent_extent == 0
       || (going_forward && itor->end_addr != 0
           && btree_addrs_share_extent(
              itor->cc, itor->curr.addr, itor->end_addr))
       || (!going_forward && itor->curr_min_idx != -1))
   {
      return;
   }

   btree_prefetch_cursor_start(itor, going_forward);
   if (itor->prefetch.state != BTREE_PREFETCH_ACTIVE) {
      cache_prefetch(itor->cc, adjacent_extent, itor->page_type);
   }
}

/*
 * ----------------------------------------------------------------------------
 * Move to the next leaf when we've reached the end of one leaf but
 * haven't reached the end of the iterator.
 * ----------------------------------------------------------------------------
 */
static platform_status
btree_iterator_next_leaf(btree_iterator *itor)
{
   uint64 last_addr        = itor->curr.addr;
   uint64 expected_page_no = 0;
   if (itor->page_type == PAGE_TYPE_BRANCH) {
      if (itor->curr.hdr->page_no == UINT64_MAX) {
         itor->validation_status = STATUS_INVALID_STATE;
         return itor->validation_status;
      }
      expected_page_no = itor->curr.hdr->page_no + 1;
   }
   uint64 next_addr =
      btree_node_next_addr(itor->cfg, &itor->curr, itor->page_type);
   btree_iterator_release_curr(itor);
   platform_status rc = btree_iterator_get_curr_addr(itor, next_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = btree_iterator_check_branch_page_no(itor, expected_page_no);
   if (!SUCCESS(rc)) {
      return rc;
   }
   itor->idx          = 0;
   itor->curr_min_idx = -1;

   rc = btree_iterator_prepare_end_idx(itor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   btree_iterator_prefetch_on_advance(itor, last_addr, TRUE);
   return STATUS_OK;
}

static async_status
btree_iterator_next_leaf_async(btree_iterator_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   state->last_addr        = state->itor->curr.addr;
   state->expected_page_no = 0;
   if (state->itor->page_type == PAGE_TYPE_BRANCH) {
      if (state->itor->curr.hdr->page_no == UINT64_MAX) {
         state->itor->validation_status = STATUS_INVALID_STATE;
         async_return(state, state->itor->validation_status);
      }
      state->expected_page_no = state->itor->curr.hdr->page_no + 1;
   }
   state->next_addr = btree_node_next_addr(
      state->itor->cfg, &state->itor->curr, state->itor->page_type);
   btree_iterator_release_curr(state->itor);
   state->itor->curr.addr = state->next_addr;
   async_await_call(state,
                    cache_get_async,
                    &state->cache_get_state,
                    state->itor->cc,
                    state->itor->curr.addr,
                    state->itor->page_type,
                    state->callback,
                    state->callback_arg);
   state->itor->validation_status = cache_get_async_state_result(
      &state->cache_get_state, &state->itor->curr.page);
   if (!SUCCESS(state->itor->validation_status)) {
      async_return(state, state->itor->validation_status);
   }
   state->itor->curr.hdr =
      (btree_hdr *)page_handle_data(&state->itor->curr.page);
   platform_status rc = btree_node_read_header(state->itor->cc,
                                               state->itor->cfg,
                                               &state->itor->curr,
                                               state->itor->page_type);
   if (!SUCCESS(rc)) {
      btree_node_unget(state->itor->cc, state->itor->cfg, &state->itor->curr);
      state->itor->validation_status = rc;
      async_return(state, rc);
   }
   rc = btree_node_check_expected_height(
      &state->itor->curr, state->itor->page_type, state->itor->height);
   if (!SUCCESS(rc)) {
      btree_node_unget(state->itor->cc, state->itor->cfg, &state->itor->curr);
      state->itor->validation_status = rc;
      async_return(state, rc);
   }
   rc =
      btree_iterator_check_branch_page_no(state->itor, state->expected_page_no);
   if (!SUCCESS(rc)) {
      btree_node_unget(state->itor->cc, state->itor->cfg, &state->itor->curr);
      async_return(state, rc);
   }
   rc = btree_iterator_copy_curr_if_needed(state->itor);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   state->itor->idx          = 0;
   state->itor->curr_min_idx = -1;

   async_await_subroutine(state, btree_iterator_prepare_end_idx_async);
   if (!SUCCESS(state->__async_result)) {
      async_return(state, state->__async_result);
   }

   // Prefetching is all non-blocking, so it needs no awaits here.
   btree_iterator_prefetch_on_advance(state->itor, state->last_addr, TRUE);

   async_return(state);
}

/*
 * ----------------------------------------------------------------------------
 * Move to the previous leaf when we've reached the beginning of one leaf.
 * ----------------------------------------------------------------------------
 */
static platform_status
btree_iterator_prev_leaf(btree_iterator *itor)
{
   const btree_config *cfg = itor->cfg;

   uint64 last_addr = itor->curr.addr;
   /*
    * Copied nodes can have stale prev_addr values. Read the live current node
    * before moving backward so predecessor splits are not skipped.
    */
   uint64          prev_addr;
   platform_status rc = btree_iterator_curr_live_prev_addr(itor, &prev_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }
   uint64 expected_page_no = 0;
   if (itor->page_type == PAGE_TYPE_BRANCH) {
      if (itor->curr.hdr->page_no == 0) {
         itor->validation_status = STATUS_INVALID_STATE;
         return itor->validation_status;
      }
      expected_page_no = itor->curr.hdr->page_no - 1;
   }
   btree_iterator_release_curr(itor);
   rc = btree_iterator_get_curr_addr(itor, prev_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = btree_iterator_check_branch_page_no(itor, expected_page_no);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (itor->page_type == PAGE_TYPE_BRANCH
       && btree_node_next_addr(cfg, &itor->curr, itor->page_type) != last_addr)
   {
      itor->validation_status = STATUS_INVALID_STATE;
      return itor->validation_status;
   }

   /*
    * The previous leaf may have split in between our release of the
    * old curr node and the new one.  In this case, we can just walk
    * forward until we find the leaf whose successor is our old leaf.
    */
   while (itor->page_type != PAGE_TYPE_BRANCH
          && btree_node_next_addr(cfg, &itor->curr, itor->page_type)
                != last_addr)
   {
      uint64 next_addr =
         btree_node_next_addr(cfg, &itor->curr, itor->page_type);
      if (next_addr == 0 || next_addr == itor->curr.addr) {
         itor->validation_status = STATUS_INVALID_STATE;
         return itor->validation_status;
      }
      btree_iterator_release_curr(itor);
      rc = btree_iterator_get_curr_addr(itor, next_addr);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   itor->idx = btree_num_entries(itor->curr.hdr) - 1;

   /* Do a quick check whether this entire leaf is within the range. */
   int cmp;
   rc = btree_iterator_compare_node_key(
      itor, itor->curr.hdr, 0, itor->min_key, &cmp);
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
   }
   if (cmp < 0) {
      itor->curr_min_idx = -1;
   } else {
      rc = find_key_in_node(itor,
                            itor->curr.hdr,
                            itor->min_key,
                            itor->min_key_comparison,
                            NULL,
                            &itor->curr_min_idx);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   if (btree_node_prev_addr(cfg, &itor->curr, itor->page_type) == 0
       && itor->curr_min_idx == -1)
   {
      itor->curr_min_idx = 0;
   }

   rc = btree_iterator_prepare_end_idx(itor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   btree_iterator_prefetch_on_advance(itor, last_addr, FALSE);
   return STATUS_OK;
}

static async_status
btree_iterator_prev_leaf_async(btree_iterator_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   state->curr_addr = state->itor->curr.addr;
   if (btree_iterator_curr_is_copy(state->itor)) {
      state->live_curr.addr = state->curr_addr;
      async_await_call(state,
                       cache_get_async,
                       &state->cache_get_state,
                       state->itor->cc,
                       state->live_curr.addr,
                       state->itor->page_type,
                       state->callback,
                       state->callback_arg);
      state->itor->validation_status = cache_get_async_state_result(
         &state->cache_get_state, &state->live_curr.page);
      if (!SUCCESS(state->itor->validation_status)) {
         async_return(state, state->itor->validation_status);
      }
      state->live_curr.hdr =
         (btree_hdr *)page_handle_data(&state->live_curr.page);
      platform_status rc = btree_node_read_header(state->itor->cc,
                                                  state->itor->cfg,
                                                  &state->live_curr,
                                                  state->itor->page_type);
      if (!SUCCESS(rc)) {
         btree_node_unget(state->itor->cc, state->itor->cfg, &state->live_curr);
         state->itor->validation_status = rc;
         async_return(state, rc);
      }
      rc = btree_node_check_expected_height(
         &state->live_curr, state->itor->page_type, state->itor->height);
      if (!SUCCESS(rc)) {
         btree_node_unget(state->itor->cc, state->itor->cfg, &state->live_curr);
         state->itor->validation_status = rc;
         async_return(state, rc);
      }
      state->prev_addr = btree_node_prev_addr(
         state->itor->cfg, &state->live_curr, state->itor->page_type);
      btree_node_unget(state->itor->cc, state->itor->cfg, &state->live_curr);
   } else {
      state->prev_addr = btree_node_prev_addr(
         state->itor->cfg, &state->itor->curr, state->itor->page_type);
   }
   state->expected_page_no = 0;
   if (state->itor->page_type == PAGE_TYPE_BRANCH) {
      if (state->itor->curr.hdr->page_no == 0) {
         state->itor->validation_status = STATUS_INVALID_STATE;
         async_return(state, state->itor->validation_status);
      }
      state->expected_page_no = state->itor->curr.hdr->page_no - 1;
   }
   btree_iterator_release_curr(state->itor);
   state->itor->curr.addr = state->prev_addr;
   async_await_call(state,
                    cache_get_async,
                    &state->cache_get_state,
                    state->itor->cc,
                    state->itor->curr.addr,
                    state->itor->page_type,
                    state->callback,
                    state->callback_arg);
   state->itor->validation_status = cache_get_async_state_result(
      &state->cache_get_state, &state->itor->curr.page);
   if (!SUCCESS(state->itor->validation_status)) {
      async_return(state, state->itor->validation_status);
   }
   state->itor->curr.hdr =
      (btree_hdr *)page_handle_data(&state->itor->curr.page);
   platform_status rc = btree_node_read_header(state->itor->cc,
                                               state->itor->cfg,
                                               &state->itor->curr,
                                               state->itor->page_type);
   if (!SUCCESS(rc)) {
      btree_node_unget(state->itor->cc, state->itor->cfg, &state->itor->curr);
      state->itor->validation_status = rc;
      async_return(state, rc);
   }
   rc = btree_node_check_expected_height(
      &state->itor->curr, state->itor->page_type, state->itor->height);
   if (!SUCCESS(rc)) {
      btree_node_unget(state->itor->cc, state->itor->cfg, &state->itor->curr);
      state->itor->validation_status = rc;
      async_return(state, rc);
   }
   rc =
      btree_iterator_check_branch_page_no(state->itor, state->expected_page_no);
   if (!SUCCESS(rc)) {
      btree_node_unget(state->itor->cc, state->itor->cfg, &state->itor->curr);
      async_return(state, rc);
   }
   rc = btree_iterator_copy_curr_if_needed(state->itor);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   if (state->itor->page_type == PAGE_TYPE_BRANCH
       && btree_node_next_addr(
             state->itor->cfg, &state->itor->curr, state->itor->page_type)
             != state->curr_addr)
   {
      state->itor->validation_status = STATUS_INVALID_STATE;
      async_return(state, state->itor->validation_status);
   }

   /*
    * The previous leaf may have split in between our release of the
    * old curr node and the new one.  In this case, we can just walk
    * forward until we find the leaf whose successor is our old leaf.
    */
   while (state->itor->page_type != PAGE_TYPE_BRANCH
          && btree_node_next_addr(
                state->itor->cfg, &state->itor->curr, state->itor->page_type)
                != state->curr_addr)
   {
      state->next_addr = btree_node_next_addr(
         state->itor->cfg, &state->itor->curr, state->itor->page_type);
      if (state->next_addr == 0 || state->next_addr == state->itor->curr.addr) {
         state->itor->validation_status = STATUS_INVALID_STATE;
         async_return(state, state->itor->validation_status);
      }
      btree_iterator_release_curr(state->itor);
      state->itor->curr.addr = state->next_addr;

      async_await_call(state,
                       cache_get_async,
                       &state->cache_get_state,
                       state->itor->cc,
                       state->itor->curr.addr,
                       state->itor->page_type,
                       state->callback,
                       state->callback_arg);
      state->itor->validation_status = cache_get_async_state_result(
         &state->cache_get_state, &state->itor->curr.page);
      if (!SUCCESS(state->itor->validation_status)) {
         async_return(state, state->itor->validation_status);
      }
      state->itor->curr.hdr =
         (btree_hdr *)page_handle_data(&state->itor->curr.page);
      rc = btree_node_read_header(state->itor->cc,
                                  state->itor->cfg,
                                  &state->itor->curr,
                                  state->itor->page_type);
      if (!SUCCESS(rc)) {
         btree_node_unget(
            state->itor->cc, state->itor->cfg, &state->itor->curr);
         state->itor->validation_status = rc;
         async_return(state, rc);
      }
      rc = btree_node_check_expected_height(
         &state->itor->curr, state->itor->page_type, state->itor->height);
      if (!SUCCESS(rc)) {
         btree_node_unget(
            state->itor->cc, state->itor->cfg, &state->itor->curr);
         state->itor->validation_status = rc;
         async_return(state, rc);
      }
      rc = btree_iterator_copy_curr_if_needed(state->itor);
      if (!SUCCESS(rc)) {
         async_return(state, rc);
      }
   }

   state->itor->idx = btree_num_entries(state->itor->curr.hdr) - 1;

   /* Do a quick check whether this entire leaf is within the range. */
   int cmp;
   rc = btree_iterator_compare_node_key(
      state->itor, state->itor->curr.hdr, 0, state->itor->min_key, &cmp);
   if (!SUCCESS(rc)) {
      state->itor->validation_status = rc;
      async_return(state, rc);
   }
   if (cmp < 0) {
      state->itor->curr_min_idx = -1;
   } else {
      rc = find_key_in_node(state->itor,
                            state->itor->curr.hdr,
                            state->itor->min_key,
                            state->itor->min_key_comparison,
                            NULL,
                            &state->itor->curr_min_idx);
      if (!SUCCESS(rc)) {
         async_return(state, rc);
      }
   }
   if (btree_node_prev_addr(
          state->itor->cfg, &state->itor->curr, state->itor->page_type)
          == 0
       && state->itor->curr_min_idx == -1)
   {
      state->itor->curr_min_idx = 0;
   }

   async_await_subroutine(state, btree_iterator_prepare_end_idx_async);
   if (!SUCCESS(state->__async_result)) {
      async_return(state, state->__async_result);
   }

   // Prefetching is non-blocking, so no awaits needed here.
   btree_iterator_prefetch_on_advance(state->itor, state->curr_addr, FALSE);

   async_return(state);
}

platform_status
btree_iterator_next(iterator *base_itor)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;
   if (!SUCCESS(itor->validation_status)) {
      return itor->validation_status;
   }

   // We should not be calling advance on an empty iterator
   debug_assert(btree_iterator_can_next(base_itor));
   debug_assert(itor->idx < btree_num_entries(itor->curr.hdr));

   btree_iterator_maybe_start_prefetch(itor, TRUE);
   itor->idx++;

   if (itor->idx == btree_num_entries(itor->curr.hdr)
       && btree_iterator_can_next(base_itor))
   {
      platform_status rc = btree_iterator_next_leaf(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   debug_assert(
      !btree_iterator_can_next(base_itor)
      || (0 <= itor->idx && itor->idx < btree_num_entries(itor->curr.hdr)));

   return btree_iterator_check_current(itor);
}

platform_status
btree_iterator_prev(iterator *base_itor)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;
   if (!SUCCESS(itor->validation_status)) {
      return itor->validation_status;
   }

   // We should not be calling prev on an empty iterator
   debug_assert(btree_iterator_can_prev(base_itor));
   debug_assert(itor->idx >= 0);

   btree_iterator_maybe_start_prefetch(itor, FALSE);
   itor->idx--;
   if (itor->curr_min_idx == -1 && itor->idx == -1) {
      platform_status rc = btree_iterator_prev_leaf(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   debug_assert(
      !btree_iterator_can_prev(base_itor)
      || (0 <= itor->idx && itor->idx < btree_num_entries(itor->curr.hdr)));

   return btree_iterator_check_current(itor);
}

static inline void
btree_iterator_bound_idx(btree_iterator *itor, comparison position_rule)
{
   debug_assert(itor->curr.addr != itor->end_addr || itor->end_idx_valid);
   bool32 forward = comparison_is_forward(position_rule);

   if (itor->idx < itor->curr_min_idx) {
      itor->idx = forward ? itor->curr_min_idx : itor->curr_min_idx - 1;
   }

   if (itor->curr.addr == itor->end_addr && itor->idx >= itor->end_idx) {
      itor->idx = forward ? itor->end_idx : itor->end_idx - 1;
   }
}

static inline platform_status
btree_iterator_move_leaf_if_needed(btree_iterator *itor)
{
   if (itor->idx == btree_num_entries(itor->curr.hdr)
       && btree_iterator_can_next(&itor->super))
   {
      platform_status rc = btree_iterator_next_leaf(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
      itor->curr_min_idx = 0; // we came from an irrelevant leaf
   }
   if (itor->curr_min_idx == -1 && itor->idx == -1) {
      platform_status rc = btree_iterator_prev_leaf(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   return STATUS_OK;
}

static inline async_status
btree_iterator_move_leaf_if_needed_async(btree_iterator_async_state *state,
                                         uint64                      depth)
{
   async_begin(state, depth);

   if (state->itor->idx == btree_num_entries(state->itor->curr.hdr)
       && btree_iterator_can_next(&state->itor->super))
   {
      async_await_subroutine(state, btree_iterator_next_leaf_async);
      if (!SUCCESS(state->__async_result)) {
         async_return(state, state->__async_result);
      }
      state->itor->curr_min_idx = 0; // we came from an irrelevant leaf
   }
   if (state->itor->curr_min_idx == -1 && state->itor->idx == -1) {
      async_await_subroutine(state, btree_iterator_prev_leaf_async);
      if (!SUCCESS(state->__async_result)) {
         async_return(state, state->__async_result);
      }
   }

   async_return(state);
}

static inline platform_status
find_btree_node_and_get_idx_bounds(btree_iterator *itor,
                                   key             target,
                                   comparison      position_rule)
{
   if (itor->curr.hdr != NULL) {
      btree_iterator_release_curr(itor);
   }

   // lookup the node that contains target
   platform_status rc = btree_lookup_node(itor->cc,
                                          itor->cfg,
                                          itor->root_addr,
                                          target,
                                          itor->height,
                                          itor->page_type,
                                          TRUE,
                                          &itor->curr,
                                          NULL);
   if (!SUCCESS(rc)) {
      itor->validation_status = rc;
      return rc;
   }

   // Finish the start-node traversal before inspecting any other path.
   rc = btree_iterator_copy_curr_if_needed(itor);
   if (!SUCCESS(rc)) {
      return rc;
   }

   // find the index of the minimum key
   bool32 found;
   int64  tmp;
   rc = find_key_in_node(itor,
                         itor->curr.hdr,
                         itor->min_key,
                         itor->min_key_comparison,
                         &found,
                         &tmp);
   if (!SUCCESS(rc)) {
      return rc;
   }
   // If min key doesn't exist in current node, but is:
   // 1) in range:     Min idx = first key satisfying min_key_comparison
   // 2) out of range: Min idx = -1
   itor->curr_min_idx = !found && tmp == 0 ? tmp - 1 : tmp;
   // if min_key is not within the current node but there is no previous node
   // then set curr_min_idx to 0
   if (itor->curr_min_idx == -1
       && btree_node_prev_addr(itor->cfg, &itor->curr, itor->page_type) == 0)
   {
      itor->curr_min_idx = 0;
   }

   // find the index of the actual target
   rc = find_key_in_node(
      itor, itor->curr.hdr, target, position_rule, &found, &itor->idx);
   if (!SUCCESS(rc)) {
      return rc;
   }
   // Immutable trees retain the address-based endpoint optimization.
   if (!itor->copy_nodes) {
      rc = btree_iterator_find_end_addr(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   rc = btree_iterator_prepare_end_idx(itor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   btree_iterator_bound_idx(itor, position_rule);

   // check if we already need to move to the prev/next leaf
   rc = btree_iterator_move_leaf_if_needed(itor);
   if (!SUCCESS(rc)) {
      return rc;
   }
   return btree_iterator_check_current(itor);
}

static inline async_status
find_btree_node_and_get_idx_bounds_async(btree_iterator_async_state *state,
                                         uint64                      depth)
{
   async_begin(state, depth);

   // lookup the node that contains target
   btree_lookup_async_state_init(&state->lookup_state,
                                 state->itor->cc,
                                 state->itor->cfg,
                                 state->itor->root_addr,
                                 state->format_version,
                                 state->itor->page_type,
                                 state->target,
                                 NULL,
                                 state->callback,
                                 state->callback_arg);
   state->lookup_state.stop_at_height = state->itor->height;
   state->lookup_state.stats          = NULL;
   state->lookup_state.get_node       = TRUE;
   async_await(state,
               btree_lookup_node_async(&state->lookup_state, 0)
                  == ASYNC_STATUS_DONE);
   if (!SUCCESS(state->lookup_state.__async_result)) {
      state->itor->validation_status = state->lookup_state.__async_result;
      async_return(state, state->lookup_state.__async_result);
   }
   state->itor->curr = state->lookup_state.node;

   // No live leaf reference survives into another traversal or async await.
   platform_status rc = btree_iterator_copy_curr_if_needed(state->itor);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   // find the index of the minimum key
   rc = find_key_in_node(state->itor,
                         state->itor->curr.hdr,
                         state->itor->min_key,
                         state->itor->min_key_comparison,
                         &state->found,
                         &state->tmp);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }
   // If min key doesn't exist in current node, but is:
   // 1) in range:     Min idx = first key satisfying min_key_comparison
   // 2) out of range: Min idx = -1
   state->itor->curr_min_idx =
      !state->found && state->tmp == 0 ? state->tmp - 1 : state->tmp;
   // if min_key is not within the current node but there is no previous node
   // then set curr_min_idx to 0
   if (state->itor->curr_min_idx == -1
       && btree_node_prev_addr(
             state->itor->cfg, &state->itor->curr, state->itor->page_type)
             == 0)
   {
      state->itor->curr_min_idx = 0;
   }

   // find the index of the actual target
   rc = find_key_in_node(state->itor,
                         state->itor->curr.hdr,
                         state->target,
                         state->position_rule,
                         &state->found,
                         &state->itor->idx);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }
   if (!state->itor->copy_nodes) {
      async_await_subroutine(state, btree_iterator_find_end_addr_async);
      if (!SUCCESS(state->__async_result)) {
         async_return(state, state->__async_result);
      }
   }
   async_await_subroutine(state, btree_iterator_prepare_end_idx_async);
   if (!SUCCESS(state->__async_result)) {
      async_return(state, state->__async_result);
   }
   btree_iterator_bound_idx(state->itor, state->position_rule);

   // check if we already need to move to the prev/next leaf
   async_await_subroutine(state, btree_iterator_move_leaf_if_needed_async);
   if (!SUCCESS(state->__async_result)) {
      async_return(state, state->__async_result);
   }

   async_return(state, btree_iterator_check_current(state->itor));
}

/*
 * Seek to a given key within the btree
 * seek_type defines where the iterator is positioned relative to the target
 * key.
 */
platform_status
btree_iterator_seek(iterator *base_itor, comparison seek_type, key seek_key)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;
   if (!SUCCESS(itor->validation_status)) {
      return itor->validation_status;
   }

   if (btree_key_compare(itor->cfg, seek_key, itor->min_key) < 0
       || btree_key_compare(itor->cfg, seek_key, itor->max_key) > 0)
   {
      return STATUS_BAD_PARAM;
   }

   // A seek starts a new access pattern. Leave read-ahead dormant until the
   // caller establishes a direction and approaches an extent boundary.
   btree_prefetch_cursor_deinit(itor);

   uint64 num_entries       = btree_num_entries(itor->curr.hdr);
   bool32 seek_current_node = FALSE;
   if (num_entries != 0) {
      int             first_cmp;
      int             last_cmp;
      platform_status rc = btree_iterator_compare_node_key(
         itor, itor->curr.hdr, 0, seek_key, &first_cmp);
      if (!SUCCESS(rc)) {
         itor->validation_status = rc;
         return rc;
      }
      rc = btree_iterator_compare_node_key(
         itor, itor->curr.hdr, num_entries - 1, seek_key, &last_cmp);
      if (!SUCCESS(rc)) {
         itor->validation_status = rc;
         return rc;
      }
      seek_current_node = first_cmp >= 0 && last_cmp <= 0;
   }

   if (seek_current_node) {
      // seek_key is within our current leaf. So just directly search for it
      bool32          found;
      platform_status rc = find_key_in_node(
         itor, itor->curr.hdr, seek_key, seek_type, &found, &itor->idx);
      if (!SUCCESS(rc)) {
         return rc;
      }
      rc = btree_iterator_prepare_end_idx(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
      btree_iterator_bound_idx(itor, seek_type);
      rc = btree_iterator_move_leaf_if_needed(itor);
      if (!SUCCESS(rc)) {
         return rc;
      }
      platform_assert(-1 <= itor->idx);
   } else {
      // seek key is not within our current leaf. So find the correct leaf
      platform_status rc =
         find_btree_node_and_get_idx_bounds(itor, seek_key, seek_type);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }

   platform_status rc = btree_iterator_check_current(itor);
   if (!SUCCESS(rc)) {
      return rc;
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
   platform_default_log("## idx %lu end_idx %lu end_idx_valid %u\n",
                        btree_itor->idx,
                        btree_itor->end_idx,
                        btree_itor->end_idx_valid);
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
static platform_status
btree_iterator_init_common(cache              *cc,
                           const btree_config *cfg,
                           btree_iterator     *itor,
                           uint64              root_addr,
                           uint16              format_version,
                           page_type           page_type,
                           comparison          min_key_comparison,
                           key                 min_key,
                           comparison          max_key_comparison,
                           key                 max_key,
                           key                 start_key,
                           bool32              copy_nodes,
                           uint32              height,
                           uint32              prefetch_lookahead,
                           key                *normalized_start_key)
{
   ZERO_CONTENTS(itor);
   platform_status version_rc =
      btree_validate_root(root_addr, format_version, page_type);
   if (!SUCCESS(version_rc)) {
      return version_rc;
   }
   debug_assert(page_type == PAGE_TYPE_MEMTABLE
                || page_type == PAGE_TYPE_BRANCH);

   debug_assert(!key_is_null(min_key) && !key_is_null(max_key)
                && !key_is_null(start_key));
   debug_assert(min_key_comparison == greater_than
                || min_key_comparison == greater_than_or_equal);
   debug_assert(max_key_comparison == less_than
                || max_key_comparison == less_than_or_equal);

   if (btree_key_compare(cfg, min_key, max_key) > 0) {
      max_key            = min_key;
      min_key_comparison = greater_than_or_equal;
      max_key_comparison = less_than;
   }
   if (btree_key_compare(cfg, start_key, min_key) < 0) {
      start_key = min_key;
   }
   if (btree_key_compare(cfg, start_key, max_key) > 0) {
      start_key = max_key;
   }

   itor->cc                 = cc;
   itor->cfg                = cfg;
   itor->root_addr          = root_addr;
   itor->height             = height;
   itor->copy_nodes         = copy_nodes;
   itor->min_key_comparison = min_key_comparison;
   itor->min_key            = min_key;
   itor->max_key_comparison = max_key_comparison;
   itor->max_key            = max_key;
   itor->page_type          = page_type;
   itor->super.ops          = &btree_iterator_ops;
   itor->prefetch.lookahead = prefetch_lookahead;
   itor->validation_status  = STATUS_OK;
   if (copy_nodes) {
      itor->node_copy = TYPED_MANUAL_MALLOC(
         PROCESS_PRIVATE_HEAP_ID, itor->node_copy, btree_page_size(itor->cfg));
      if (itor->node_copy == NULL) {
         return STATUS_NO_MEMORY;
      }
   }

   *normalized_start_key = start_key;

   return STATUS_OK;
}

platform_status
btree_iterator_init(cache                             *cc,
                    const btree_config                *cfg,
                    btree_iterator                    *itor,
                    uint64                             root_addr,
                    uint16                             format_version,
                    page_type                          page_type,
                    const btree_iterator_init_options *options)
{
   resolved_key_range range = resolve_key_range(
      options->lower_limit, options->upper_limit, options->start_position);
   key start_key = range.start_key;

   platform_status rc = btree_iterator_init_common(cc,
                                                   cfg,
                                                   itor,
                                                   root_addr,
                                                   format_version,
                                                   page_type,
                                                   range.min_cmp,
                                                   range.min_key,
                                                   range.max_cmp,
                                                   range.max_key,
                                                   start_key,
                                                   options->copy_nodes,
                                                   options->height,
                                                   options->prefetch_lookahead,
                                                   &start_key);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = find_btree_node_and_get_idx_bounds(itor, start_key, range.start_cmp);
   if (!SUCCESS(rc)) {
      btree_iterator_deinit(itor);
      return rc;
   }

   debug_assert(!iterator_can_curr((iterator *)itor)
                || itor->idx < btree_num_entries(itor->curr.hdr));

   return STATUS_OK;
}

async_status
btree_iterator_init_async(btree_iterator_async_state *state)
{
   async_begin(state, 0);

   // State initialization resets parameters/continuations, not local results.
   state->__async_result = STATUS_OK;

   platform_status rc = btree_iterator_init_common(state->cc,
                                                   state->cfg,
                                                   state->itor,
                                                   state->root_addr,
                                                   state->format_version,
                                                   state->type,
                                                   state->min_key_comparison,
                                                   state->min_key,
                                                   state->max_key_comparison,
                                                   state->max_key,
                                                   state->start_key,
                                                   state->copy_nodes,
                                                   state->height,
                                                   state->prefetch_lookahead,
                                                   &state->target);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   state->position_rule = state->start_type;
   async_await_subroutine(state, find_btree_node_and_get_idx_bounds_async);
   if (!SUCCESS(state->__async_result)) {
      btree_iterator_deinit(state->itor);
      async_return(state, state->__async_result);
   }

   debug_assert(!iterator_can_curr((iterator *)state->itor)
                || state->itor->idx < btree_num_entries(state->itor->curr.hdr));

   async_return(state, STATUS_OK);
}

platform_status
btree_iterator_init_async_result(btree_iterator_async_state *state)
{
   return async_result(state);
}

void
btree_iterator_deinit(btree_iterator *itor)
{
   debug_assert(itor != NULL);
   btree_prefetch_cursor_deinit(itor);
   btree_iterator_release_curr(itor);
   if (itor->prefix_state != NULL) {
      key_buffer_deinit(&itor->prefix_state->decoded_key);
      platform_free(PROCESS_PRIVATE_HEAP_ID, itor->prefix_state);
      itor->prefix_state = NULL;
   }
   if (itor->node_copy != NULL) {
      platform_free(PROCESS_PRIVATE_HEAP_ID, itor->node_copy);
      itor->node_copy = NULL;
   }
}

/****************************
 * B-tree packing functions
 ****************************/

// Branch nodes use the header union for meta_page_addr.
static inline void
btree_pack_node_init_hdr(const btree_config *cfg, btree_hdr *hdr, uint8 height)
{
   btree_init_hdr(cfg, hdr);
   hdr->height = height;
}

static inline platform_status
btree_pack_setup_start(btree_pack_req *req)
{
   req->height = 0;
   ZERO_ARRAY(req->edge);
   ZERO_ARRAY(req->edge_stats);
   ZERO_ARRAY(req->num_edges);
   ZERO_ARRAY(req->level_has_nodes);
   ZERO_ARRAY(req->next_page_no);

   // we create a root here, but we won't build it with the rest
   // of the tree, we'll copy into it at the end
   platform_status rc = btree_create(
      req->cc, req->cfg, &req->mini, PAGE_TYPE_BRANCH, &req->root_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }

   req->num_tuples    = 0;
   req->key_bytes     = 0;
   req->message_bytes = 0;
   return STATUS_OK;
}


static inline btree_node *
btree_pack_get_current_node(btree_pack_req *req, uint64 height)
{
   platform_assert(height < BTREE_MAX_HEIGHT);
   if (0 < req->num_edges[height]) {
      platform_assert(req->num_edges[height] <= MAX_PAGES_PER_EXTENT);
      return &req->edge[height][req->num_edges[height] - 1];
   } else {
      return NULL;
   }
}

static inline btree_pivot_stats *
btree_pack_get_current_node_stats(btree_pack_req *req, uint64 height)
{
   platform_assert(height < BTREE_MAX_HEIGHT);
   platform_assert(0 < req->num_edges[height]
                   && req->num_edges[height] <= MAX_PAGES_PER_EXTENT);
   return &req->edge_stats[height][req->num_edges[height] - 1];
}

static inline platform_status
btree_pack_create_next_node(btree_pack_req *req,
                            uint64          height,
                            key             pivot,
                            btree_node    **node_out);

/*
 * Add the specified node to its parent. Creates a parent if necessary.
 */
static inline platform_status
btree_pack_link_node(btree_pack_req *req, uint64 height, uint64 offset)
{
   if (height >= BTREE_MAX_HEIGHT - 1) {
      platform_error_log("btree_pack_link_node: cannot link a node above "
                         "maximum height %u\n",
                         BTREE_MAX_HEIGHT - 1);
      return STATUS_LIMIT_EXCEEDED;
   }
   platform_assert(offset < req->num_edges[height]);

   btree_node        *edge       = &req->edge[height][offset];
   btree_pivot_stats *edge_stats = &req->edge_stats[height][offset];
   DECLARE_AUTO_KEY_BUFFER(pivot_buffer, PROCESS_PRIVATE_HEAP_ID);
   key pivot;
   if (height) {
      pivot = btree_get_pivot(req->cfg, edge->hdr, 0);
   } else {
      platform_status rc =
         btree_leaf_full_key(req->cfg,
                             edge->hdr,
                             btree_get_leaf_entry(req->cfg, edge->hdr, 0),
                             &pivot_buffer,
                             &pivot);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
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
      platform_status rc =
         btree_pack_create_next_node(req, height + 1, pivot, &parent);
      if (!SUCCESS(rc)) {
         btree_node_unget(req->cc, req->cfg, edge);
         return rc;
      }
      bool32 success = btree_set_index_entry(
         req->cfg, parent->hdr, 0, pivot, edge->addr, *edge_stats);
      platform_assert(success);
   }

   btree_accumulate_pivot_stats(
      btree_pack_get_current_node_stats(req, height + 1), *edge_stats);

   btree_node_unget(req->cc, req->cfg, edge);
   memset(edge_stats, 0, sizeof(*edge_stats));
   return STATUS_OK;
}

static inline platform_status
btree_pack_link_extent(btree_pack_req *req,
                       uint64          height,
                       bool32          last_extent_in_level,
                       uint64          next_extent_addr)
{
   if (height >= BTREE_MAX_HEIGHT - 1) {
      platform_error_log("btree_pack_link_extent: cannot link an extent above "
                         "maximum height %u\n",
                         BTREE_MAX_HEIGHT - 1);
      return STATUS_LIMIT_EXCEEDED;
   }
   platform_assert(0 < req->num_edges[height]
                   && req->num_edges[height] <= MAX_PAGES_PER_EXTENT);
   if (last_extent_in_level) {
      btree_hdr_set_last_in_level(
         req->edge[height][req->num_edges[height] - 1].hdr);
   }
   for (uint32 i = 0; i < req->num_edges[height]; i++) {
      req->edge[height][i].hdr->next_addr = next_extent_addr;
      platform_status rc = btree_pack_link_node(req, height, i);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   req->num_edges[height] = 0;
   return STATUS_OK;
}

static inline platform_status
btree_pack_create_next_node(btree_pack_req *req,
                            uint64          height,
                            key             pivot,
                            btree_node    **node_out)
{
   if (height >= BTREE_MAX_HEIGHT) {
      platform_error_log("btree_pack_create_next_node: requested height %lu "
                         "exceeds maximum height %u\n",
                         height,
                         BTREE_MAX_HEIGHT - 1);
      return STATUS_LIMIT_EXCEEDED;
   }

   btree_node      new_node;
   uint64          node_next_extent;
   platform_status rc = btree_alloc(req->cc,
                                    &req->mini,
                                    height,
                                    &node_next_extent,
                                    PAGE_TYPE_BRANCH,
                                    &new_node);
   if (!SUCCESS(rc)) {
      return rc;
   }
   btree_pack_node_init_hdr(req->cfg, new_node.hdr, height);
   platform_assert(req->next_page_no[height] != UINT64_MAX,
                   "branch level %lu has too many pages\n",
                   height);
   new_node.hdr->page_no = req->next_page_no[height]++;
   // Record where this node's extent is listed in the mini_allocator meta
   // stream, so an iterator can position a prefetch cursor in O(1). Must come
   // after init_hdr, which zeroes the header.
   new_node.hdr->meta_page_addr =
      mini_current_extent_meta_page(&req->mini, NUM_BLOB_BATCHES + height);

   if (!req->level_has_nodes[height]) {
      btree_hdr_set_first_in_level(new_node.hdr);
   }

   if (0 < req->num_edges[height]) {
      btree_node *old_node    = btree_pack_get_current_node(req, height);
      new_node.hdr->prev_addr = old_node->hdr->prev_addr;
      if (!btree_addrs_share_extent(req->cc, old_node->addr, new_node.addr)) {
         debug_assert(btree_page_is_last_in_extent(req->cfg, old_node->addr));
         debug_assert(btree_page_is_first_in_extent(req->cfg, new_node.addr));
         new_node.hdr->prev_addr =
            btree_extent_base_addr(req->cc, old_node->addr);
         platform_status rc =
            btree_pack_link_extent(req, height, FALSE, new_node.addr);
         if (!SUCCESS(rc)) {
            btree_node_full_unlock(req->cc, req->cfg, &new_node);
            return rc;
         }
      }
   }

   if (req->height < height) {
      req->height = height;
   }

   req->edge[height][req->num_edges[height]] = new_node;
   req->num_edges[height]++;
   req->level_has_nodes[height] = TRUE;
   debug_assert(btree_pack_get_current_node_stats(req, height)->num_kvs == 0);
   *node_out = &req->edge[height][req->num_edges[height] - 1];
   return STATUS_OK;
}

/* Tiny prefixes can impose reconstruction work without improving page
 * occupancy. This construction policy is per leaf; readers still accept
 * previously written leaves with shorter prefixes. */
#define BTREE_MIN_LEAF_PREFIX_LENGTH (4)

/* Pull a common prefix out only when the uncompressed leaf first fills.
 * The prefix lives at the page tail so appending offsets never moves it. */
static bool32
btree_pack_compress_leaf(btree_pack_req *req, btree_hdr *hdr)
{
   if (!req->cfg->data_cfg->key_compare_is_lexical || hdr->prefix_length != 0
       || hdr->num_entries < 2)
   {
      return FALSE;
   }
   key    first  = btree_get_tuple_key(req->cfg, hdr, 0);
   key    last   = btree_get_tuple_key(req->cfg, hdr, hdr->num_entries - 1);
   uint64 length = MIN(key_length(first), key_length(last));
   uint64 prefix = 0;
   while (prefix < length
          && ((const char *)key_data(first))[prefix]
                == ((const char *)key_data(last))[prefix])
   {
      prefix++;
   }
   if (prefix < BTREE_MIN_LEAF_PREFIX_LENGTH) {
      return FALSE;
   }
   char       scratch[MAX_PAGE_SIZE];
   btree_hdr *packed = (btree_hdr *)scratch;
   memcpy(packed, hdr, sizeof(*hdr));
   packed->num_entries   = 0;
   packed->prefix_length = prefix;
   packed->next_entry    = btree_page_size(req->cfg) - prefix;
   memcpy(scratch + packed->next_entry, key_data(first), prefix);
   for (uint64 i = 0; i < hdr->num_entries; i++) {
      leaf_entry *entry  = btree_get_leaf_entry(req->cfg, hdr, i);
      key         full   = leaf_entry_key(entry);
      key         suffix = key_create(FALSE,
                              key_length(full) - prefix,
                              (const char *)key_data(full) + prefix);
      bool32      fits   = btree_set_leaf_entry(
         req->cfg, packed, i, suffix, leaf_entry_message(req->cc, entry));
      platform_assert(fits);
   }
   /* The whole page is copied back and may be written to disk. Do not copy
    * uninitialized stack bytes from the free space between offsets and tuples.
    * Initialize just that gap; the header, offsets and payload are already set.
    */
   uint64 offsets_end =
      sizeof(*packed) + packed->num_entries * sizeof(packed->offsets[0]);
   debug_assert(offsets_end <= packed->next_entry);
   memset(scratch + offsets_end, 0, packed->next_entry - offsets_end);
   memcpy(hdr, scratch, btree_page_size(req->cfg));
   return TRUE;
}

static bool32
btree_pack_append_leaf(btree_pack_req *req,
                       btree_hdr      *hdr,
                       key             full,
                       message         msg)
{
   key suffix = full;
   if (hdr->prefix_length != 0) {
      if (btree_compare_prefix(req->cfg, hdr, full) != 0) {
         return FALSE;
      }
      suffix = key_create(FALSE,
                          key_length(full) - hdr->prefix_length,
                          (const char *)key_data(full) + hdr->prefix_length);
   }
   return btree_set_leaf_entry(req->cfg, hdr, hdr->num_entries, suffix, msg);
}

static inline platform_status
btree_pack_loop(btree_pack_req *req,       // IN/OUT
                key             tuple_key, // IN
                message         msg)               // IN
{
   platform_status rc;

   if (message_is_invalid_user_type(msg)) {
      return STATUS_INVALID_STATE;
   }

   uint64 key_size = key_length(tuple_key);
   if (req->num_tuples >= BTREE_MAX_KNOWN_COUNTER
       || key_size > BTREE_MAX_KNOWN_COUNTER - req->key_bytes)
   {
      platform_error_log(
         "btree_pack: output statistics exceed the on-disk limit\n");
      return STATUS_LIMIT_EXCEEDED;
   }

   log_trace_key(tuple_key, "btree_pack_loop");

   uint64 message_size = 0;
   if (!message_is_blob(msg)) {
      message_size = message_length(msg);
      if (message_size > BTREE_MAX_KNOWN_COUNTER - req->message_bytes) {
         platform_error_log(
            "btree_pack: output statistics exceed the on-disk limit\n");
         return STATUS_LIMIT_EXCEEDED;
      }
   }

   if (message_is_blob(msg)) {
      rc = message_clone(
         &req->cfg->blob_cfg, req->cc, &req->mini, msg, &req->blob_buffer);
      if (!SUCCESS(rc)) {
         return rc;
      }
      msg = merge_accumulator_to_message(&req->blob_buffer);
   } else if (MAX_INLINE_MESSAGE_SIZE(btree_page_size(req->cfg))
              < message_length(msg))
   {
      rc = message_to_blob(
         &req->cfg->blob_cfg, req->cc, &req->mini, msg, &req->blob_buffer);
      if (!SUCCESS(rc)) {
         return rc;
      }
      msg = merge_accumulator_to_message(&req->blob_buffer);
   }

   if (message_is_blob(msg)) {
      message_size = message_materialized_length(msg);
      if (message_size > BTREE_MAX_KNOWN_COUNTER - req->message_bytes) {
         platform_error_log(
            "btree_pack: output statistics exceed the on-disk limit\n");
         return STATUS_LIMIT_EXCEEDED;
      }
   }

   btree_node *leaf = btree_pack_get_current_node(req, 0);

   bool32 appended =
      leaf != NULL && btree_pack_append_leaf(req, leaf->hdr, tuple_key, msg);
   if (!appended && leaf != NULL && btree_pack_compress_leaf(req, leaf->hdr)) {
      appended = btree_pack_append_leaf(req, leaf->hdr, tuple_key, msg);
   }
   if (!appended) {
      rc = btree_pack_create_next_node(req, 0, tuple_key, &leaf);
      if (!SUCCESS(rc)) {
         return rc;
      }
      bool32 result =
         btree_set_leaf_entry(req->cfg, leaf->hdr, 0, tuple_key, msg);
      platform_assert(result);
   }

   btree_pivot_stats *leaf_stats = btree_pack_get_current_node_stats(req, 0);
   leaf_stats->num_kvs++;
   leaf_stats->key_bytes += key_size;
   leaf_stats->message_bytes += message_size;

   log_trace_key(tuple_key, "btree_pack_loop (bottom)");

   if (req->fingerprint_arr != NULL) {
      platform_assert(req->num_tuples < req->max_tuples);
      req->fingerprint_arr[req->num_tuples] =
         data_key_hash(req->cfg->data_cfg, tuple_key, req->seed);
   }

   req->num_tuples++;
   req->key_bytes += key_size;
   req->message_bytes += message_size;
   return STATUS_OK;
}


static inline platform_status
btree_pack_post_loop(btree_pack_req *req, key last_key)
{
   cache              *cc  = req->cc;
   const btree_config *cfg = req->cfg;
   // we want to use the allocation node, so we copy the root created in the
   // loop into the btree_create root
   btree_node root;

   // if output tree is empty, deallocate any preallocated extents
   if (req->num_tuples == 0) {
      mini_release(&req->mini);
      refcount r = mini_dec_ref(
         cc, btree_root_to_meta_addr(cfg, req->root_addr, 0), PAGE_TYPE_BRANCH);
      platform_assert(r == 0);
      req->root_addr = 0;
      return STATUS_OK;
   }

   uint64 h = 0;
   while (h < req->height || 1 < req->num_edges[h]) {
      platform_status rc = btree_pack_link_extent(req, h, TRUE, 0);
      if (!SUCCESS(rc)) {
         return rc;
      }
      h++;
   }

   btree_hdr_set_last_in_level(req->edge[req->height][0].hdr);
   root.addr          = req->root_addr;
   platform_status rc = btree_node_get(cc, cfg, &root, PAGE_TYPE_BRANCH);
   if (!SUCCESS(rc)) {
      return rc;
   }
   debug_only bool32 success = btree_node_claim(cc, cfg, &root);
   debug_assert(success);
   btree_node_lock(cc, cfg, &root);
   memmove(root.hdr, req->edge[req->height][0].hdr, btree_page_size(cfg));
   // The root is allocated outside the mini allocator's extent stream.
   root.hdr->meta_page_addr = 0;
   root.hdr->prev_addr      = 0;
   root.hdr->next_addr      = 0;
   btree_hdr_set_first_in_level(root.hdr);
   btree_hdr_set_last_in_level(root.hdr);
   btree_node_full_unlock(cc, cfg, &root);

   btree_node_full_unlock(cc, cfg, &req->edge[req->height][0]);

   mini_release(&req->mini);
   return STATUS_OK;
}

static bool32
btree_pack_can_fit_tuple(btree_pack_req *req, key tuple_key, message data)
{
   return req->max_tuples == 0 || req->num_tuples < req->max_tuples;
}

static void
btree_pack_abort(btree_pack_req *req)
{
   platform_assert(req->height < BTREE_MAX_HEIGHT);
   for (uint16 i = 0; i <= req->height; i++) {
      for (uint16 j = 0; j < req->num_edges[i]; j++) {
         if (!page_handle_is_null(&req->edge[i][j].page)) {
            btree_node_full_unlock(req->cc, req->cfg, &req->edge[i][j]);
         }
      }
   }

   /*
    * btree_create() reserves the mini allocator's next extents before the
    * pack starts.  The root reference below owns only the root and extents
    * that the pack actually linked into the tree; release the still-reserved
    * extents explicitly on every abort path.
    */
   mini_release(&req->mini);
   btree_dec_ref(req->cc, req->cfg, req->root_addr, PAGE_TYPE_BRANCH);
   req->root_addr = 0;
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
   platform_status rc = btree_pack_setup_start(req);
   if (!SUCCESS(rc)) {
      return rc;
   }

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
      rc = btree_pack_loop(req, tuple_key, data);
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

   rc = btree_pack_post_loop(req, tuple_key);
   if (!SUCCESS(rc)) {
      platform_error_log("%s error status: %d\n", __func__, rc.r);
      btree_pack_abort(req);
      return rc;
   }
   platform_assert(IMPLIES(req->num_tuples == 0, req->root_addr == 0));
   return STATUS_OK;
}

/*
 * Returns the number of kv pairs (k,v ) w/ k < key.  Also returns
 * the total size of all such keys and messages.
 */
static inline platform_status
btree_get_rank(cache              *cc,
               const btree_config *cfg,
               uint64              root_addr,
               key                 target,
               btree_pivot_stats  *stats)
{
   btree_node leaf;

   platform_status rc = btree_lookup_node(
      cc, cfg, root_addr, target, 0, PAGE_TYPE_BRANCH, TRUE, &leaf, stats);
   if (!SUCCESS(rc)) {
      return rc;
   }
   bool32 found;
   int64  tuple_rank_in_leaf;
   rc = btree_find_tuple_checked(
      cfg, leaf.hdr, target, &found, &tuple_rank_in_leaf, NULL);
   if (!SUCCESS(rc)) {
      btree_node_unget(cc, cfg, &leaf);
      return rc;
   }
   if (!found) {
      tuple_rank_in_leaf++;
   }
   rc = accumulate_node_ranks(
      cc, cfg, leaf.hdr, PAGE_TYPE_BRANCH, 0, tuple_rank_in_leaf, stats);
   btree_node_unget(cc, cfg, &leaf);
   return rc;
}

/*
 * count_in_range returns the exact number of tuples in the given
 * btree between min_key (inc) and max_key (excl).
 */
platform_status
btree_count_in_range(cache              *cc,
                     const btree_config *cfg,
                     uint64              root_addr,
                     uint16              format_version,
                     key                 min_key,
                     key                 max_key,
                     btree_pivot_stats  *stats)
{
   btree_pivot_stats min_stats;

   debug_assert(!key_is_null(min_key) && !key_is_null(max_key));
   memset(stats, 0, sizeof(*stats));

   platform_status version_rc =
      btree_validate_root(root_addr, format_version, PAGE_TYPE_BRANCH);
   if (!SUCCESS(version_rc)) {
      return version_rc;
   }

   platform_status rc = btree_get_rank(cc, cfg, root_addr, min_key, &min_stats);
   if (!SUCCESS(rc)) {
      return rc;
   }
   rc = btree_get_rank(cc, cfg, root_addr, max_key, stats);
   if (!SUCCESS(rc)) {
      memset(stats, 0, sizeof(*stats));
      return rc;
   }
   if (min_stats.num_kvs < stats->num_kvs) {
      stats->num_kvs -= min_stats.num_kvs;
      stats->key_bytes -= min_stats.key_bytes;
      stats->message_bytes -= min_stats.message_bytes;
   } else {
      memset(stats, 0, sizeof(*stats));
   }
   return STATUS_OK;
}

/*
 * btree_count_in_range_by_iterator perform
 * btree_count_in_range using an iterator instead of by
 * calculating ranks. Used for debugging purposes.
 */
platform_status
btree_count_in_range_by_iterator(cache             *cc,
                                 btree_config      *cfg,
                                 uint64             root_addr,
                                 uint16             format_version,
                                 key                min_key,
                                 key                max_key,
                                 btree_pivot_stats *stats)
{
   btree_iterator  btree_itor;
   iterator       *itor = &btree_itor.super;
   platform_status rc =
      btree_iterator_init(cc,
                          cfg,
                          &btree_itor,
                          root_addr,
                          format_version,
                          PAGE_TYPE_BRANCH,
                          &(const btree_iterator_init_options){
                             .lower_limit = key_greater_than_or_equal(min_key),
                             .upper_limit = key_less_than(max_key),
                             .start_position = key_start_at_or_after(min_key),
                             .prefetch_lookahead = 1});
   if (!SUCCESS(rc)) {
      return rc;
   }

   memset(stats, 0, sizeof(*stats));

   while (iterator_can_next(itor)) {
      key     curr_key;
      message msg;
      iterator_curr(itor, &curr_key, &msg);
      stats->num_kvs++;
      stats->key_bytes += key_length(curr_key);
      stats->message_bytes += message_materialized_length(msg);
      rc = iterator_next(itor);
      if (!SUCCESS(rc)) {
         break;
      }
   }
   btree_iterator_deinit(&btree_itor);
   return rc;
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
                "   (num_kvs=%lu\n"
                "    key_bytes=%lu (%s)\n"
                "    message_bytes=%lu (%s))\n",
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
                        const btree_config  *cfg,
                        index_entry         *entry,
                        uint64               entry_num)
{
   data_config *dcfg = cfg->data_cfg;
   platform_log(log_handle,
                "[%2lu]: key=%s\n",
                entry_num,
                key_string(dcfg, index_entry_key(entry)));
   btree_pivot_data decoded = {.child_addr = entry->pivot_data.child_addr,
                               .stats      = index_entry_stats(entry)};
   btree_print_btree_pivot_data(log_handle, &decoded);
}

static platform_status
btree_print_index_node(platform_log_handle *log_handle,
                       const btree_config  *cfg,
                       cache               *cc,
                       uint64               addr,
                       btree_hdr           *hdr,
                       page_type            type)
{
   platform_log(
      log_handle, "**  Page type: %s, INDEX NODE \n", page_type_str[type]);
   platform_log(log_handle, "**  Header ptr: %p\n", hdr);
   platform_log(log_handle, "**  addr: %lu \n", addr);
   platform_log(log_handle,
                "**  prev_addr: %lu \n",
                btree_hdr_prev_addr(cfg, hdr, addr, type));
   platform_log(log_handle,
                "**  next_addr: %lu \n",
                btree_hdr_next_addr(cfg, hdr, addr, type));
   if (type == PAGE_TYPE_BRANCH) {
      platform_log(log_handle, "**  flags: %u \n", hdr->flags);
      platform_log(log_handle, "**  page_no: %lu \n", hdr->page_no);
      platform_log(
         log_handle, "**  meta_page_addr: %lu \n", hdr->meta_page_addr);
      platform_log(log_handle,
                   "**  prev_extent_addr: %lu \n",
                   btree_hdr_prev_extent_addr(cfg, hdr, addr, type));
      platform_log(log_handle,
                   "**  next_extent_addr: %lu \n",
                   btree_hdr_next_extent_addr(cfg, hdr, addr, type));
   } else {
      platform_log(log_handle, "**  generation: %lu \n", hdr->generation);
   }
   platform_log(log_handle, "**  height: %u \n", btree_height(hdr));
   platform_log(log_handle, "**  next_entry: %u \n", hdr->next_entry);
   platform_log(log_handle, "**  num_entries: %u \n", btree_num_entries(hdr));

   btree_print_offset_table(log_handle, hdr);

   platform_log(log_handle, "-------------------\n");
   platform_log(
      log_handle, "Array of %d index entries:\n", btree_num_entries(hdr));
   for (uint64 i = 0; i < btree_num_entries(hdr); i++) {
      const index_entry *entry;
      platform_status    rc;
      if (type == PAGE_TYPE_BRANCH) {
         rc = btree_read_index_entry(cc, cfg, hdr, i, &entry);
         if (!SUCCESS(rc)) {
            return rc;
         }
      } else {
         entry = btree_get_index_entry(cfg, hdr, i);
      }
      btree_print_index_entry(log_handle, cfg, (index_entry *)entry, i);
   }
   platform_log(log_handle, "\n");
   return STATUS_OK;
}

static platform_status
btree_print_leaf_entry(platform_log_handle *log_handle,
                       const btree_config  *cfg,
                       cache               *cc,
                       const btree_hdr     *hdr,
                       leaf_entry          *entry,
                       uint64               entry_num)
{
   data_config *dcfg = cfg->data_cfg;
   DECLARE_AUTO_KEY_BUFFER(buffer, PROCESS_PRIVATE_HEAP_ID);
   key             full_key;
   platform_status rc =
      btree_leaf_full_key(cfg, hdr, entry, &buffer, &full_key);
   if (!SUCCESS(rc)) {
      return rc;
   }
   platform_log(log_handle,
                "[%2lu]: %s -- %s\n",
                entry_num,
                key_string(dcfg, full_key),
                message_string(dcfg, leaf_entry_message(cc, entry)));
   return STATUS_OK;
}

static platform_status
btree_print_leaf_node(platform_log_handle *log_handle,
                      const btree_config  *cfg,
                      cache               *cc,
                      uint64               addr,
                      btree_hdr           *hdr,
                      page_type            type)
{
   platform_log(
      log_handle, "**  Page type: %s, LEAF NODE \n", page_type_str[type]);
   platform_log(log_handle, "**  hdrptr: %p\n", hdr);
   platform_log(log_handle, "**  addr: %lu \n", addr);
   platform_log(log_handle,
                "**  prev_addr: %lu \n",
                btree_hdr_prev_addr(cfg, hdr, addr, type));
   platform_log(log_handle,
                "**  next_addr: %lu \n",
                btree_hdr_next_addr(cfg, hdr, addr, type));
   if (type == PAGE_TYPE_BRANCH) {
      platform_log(log_handle, "**  flags: %u \n", hdr->flags);
      platform_log(log_handle, "**  page_no: %lu \n", hdr->page_no);
      platform_log(
         log_handle, "**  meta_page_addr: %lu \n", hdr->meta_page_addr);
      platform_log(log_handle,
                   "**  prev_extent_addr: %lu \n",
                   btree_hdr_prev_extent_addr(cfg, hdr, addr, type));
      platform_log(log_handle,
                   "**  next_extent_addr: %lu \n",
                   btree_hdr_next_extent_addr(cfg, hdr, addr, type));
   } else {
      platform_log(log_handle, "**  generation: %lu \n", hdr->generation);
   }
   platform_log(log_handle, "**  height: %u \n", btree_height(hdr));
   platform_log(log_handle, "**  next_entry: %u \n", hdr->next_entry);
   platform_log(log_handle, "**  num_entries: %u \n", btree_num_entries(hdr));

   btree_print_offset_table(log_handle, hdr);

   platform_log(log_handle, "-------------------\n");
   platform_log(
      log_handle, "Array of %d index leaf entries:\n", btree_num_entries(hdr));
   for (uint64 i = 0; i < btree_num_entries(hdr); i++) {
      const leaf_entry *entry;
      platform_status   rc;
      if (type == PAGE_TYPE_BRANCH) {
         rc = btree_read_leaf_entry(cc, cfg, hdr, i, &entry);
         if (!SUCCESS(rc)) {
            return rc;
         }
      } else {
         entry = btree_get_leaf_entry(cfg, hdr, i);
      }
      rc = btree_print_leaf_entry(
         log_handle, cfg, cc, hdr, (leaf_entry *)entry, i);
      if (!SUCCESS(rc)) {
         return rc;
      }
   }
   platform_log(log_handle, "-------------------\n");
   platform_log(log_handle, "\n");
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * btree_print_node --
 * btree_print_tree --
 *
 *      Prints out the contents of the node/tree.
 *-----------------------------------------------------------------------------
 */
static platform_status
btree_diagnostic_node_get(cache              *cc,
                          const btree_config *cfg,
                          btree_node         *node,
                          page_type           type)
{
   if (type == PAGE_TYPE_BRANCH) {
      return btree_node_get_checked(cc, cfg, node, type);
   }
   if (type != PAGE_TYPE_MEMTABLE
       || !allocator_page_valid(cache_get_allocator(cc), node->addr))
   {
      return STATUS_INVALID_STATE;
   }
   return btree_node_get(cc, cfg, node, type);
}

static void
btree_print_invalid_node(platform_log_handle *log_handle, uint64 addr)
{
   platform_log(log_handle, "*******************\n");
   platform_log(log_handle, "** INVALID NODE \n");
   platform_log(log_handle, "** addr: %lu \n", addr);
   platform_log(log_handle, "-------------------\n");
}

void
btree_print_locked_node(platform_log_handle *log_handle,
                        const btree_config  *cfg,
                        cache               *cc,
                        uint64               addr,
                        btree_hdr           *hdr,
                        page_type            type)
{
   if (type == PAGE_TYPE_BRANCH
       && !SUCCESS(btree_read_branch_header(cfg, hdr, type)))
   {
      btree_print_invalid_node(log_handle, addr);
      return;
   }
   platform_log(log_handle, "*******************\n");
   platform_log(log_handle, "BTree node at addr=%lu\n{\n", addr);
   platform_status rc;
   if (btree_height(hdr) > 0) {
      rc = btree_print_index_node(log_handle, cfg, cc, addr, hdr, type);
   } else {
      rc = btree_print_leaf_node(log_handle, cfg, cc, addr, hdr, type);
   }
   if (!SUCCESS(rc)) {
      btree_print_invalid_node(log_handle, addr);
      return;
   }
   platform_log(log_handle, "} -- End BTree node at addr=%lu\n", addr);
}


void
btree_print_node(platform_log_handle *log_handle,
                 cache               *cc,
                 const btree_config  *cfg,
                 btree_node          *node,
                 page_type            type)
{
   platform_status rc = btree_diagnostic_node_get(cc, cfg, node, type);
   if (!SUCCESS(rc)) {
      btree_print_invalid_node(log_handle, node->addr);
      return;
   }
   btree_print_locked_node(log_handle, cfg, cc, node->addr, node->hdr, type);
   btree_node_unget(cc, cfg, node);
}

static inline platform_status
btree_read_index_entry_for_type(cache              *cc,
                                const btree_config *cfg,
                                const btree_hdr    *hdr,
                                page_type           type,
                                table_index         idx,
                                const index_entry **entry)
{
   if (type == PAGE_TYPE_BRANCH) {
      return btree_read_index_entry(cc, cfg, hdr, idx, entry);
   }
   *entry = btree_get_index_entry(cfg, hdr, idx);
   return STATUS_OK;
}

static void
btree_print_subtree_at_height(platform_log_handle *log_handle,
                              cache               *cc,
                              btree_config        *cfg,
                              uint64               addr,
                              page_type            type,
                              bool32               check_height,
                              uint32               expected_height)
{
   btree_node      node = {.addr = addr};
   platform_status rc   = btree_diagnostic_node_get(cc, cfg, &node, type);
   if (!SUCCESS(rc)
       || (check_height && type == PAGE_TYPE_BRANCH
           && btree_height(node.hdr) != expected_height))
   {
      if (SUCCESS(rc)) {
         btree_node_unget(cc, cfg, &node);
      }
      platform_log(log_handle,
                   "Invalid %s BTree node addr=%lu\n",
                   page_type_str[type],
                   addr);
      return;
   }

   btree_print_locked_node(log_handle, cfg, cc, node.addr, node.hdr, type);

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

      for (table_index idx = 0; idx < nentries; idx++) {
         platform_log(
            log_handle, "\n-- Sub-tree index=%d of %d\n", idx, nentries);
         const index_entry *entry;
         rc = btree_read_index_entry_for_type(
            cc, cfg, node.hdr, type, idx, &entry);
         if (!SUCCESS(rc)) {
            platform_log(log_handle,
                         "Invalid %s BTree node addr=%lu entry=%u\n",
                         page_type_str[type],
                         addr,
                         idx);
            break;
         }
         btree_print_subtree_at_height(log_handle,
                                       cc,
                                       cfg,
                                       index_entry_child_addr(entry),
                                       type,
                                       type == PAGE_TYPE_BRANCH,
                                       node.hdr->height - 1);
      }
      platform_log(log_handle,
                   "\n} -- End BTree sub-trees under"
                   " addr=%lu\n",
                   addr);
   }
   btree_node_unget(cc, cfg, &node);
}

void
btree_print_subtree(platform_log_handle *log_handle,
                    cache               *cc,
                    btree_config        *cfg,
                    uint64               addr,
                    page_type            type)
{
   btree_print_subtree_at_height(log_handle, cc, cfg, addr, type, FALSE, 0);
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
 * Driver routine to print a BTree of page-type 'type', starting from
 * root_addr.
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
                       uint64               addr,
                       page_type            type)
{
   btree_node      node = {.addr = addr};
   platform_status rc   = btree_diagnostic_node_get(cc, cfg, &node, type);
   if (!SUCCESS(rc)) {
      btree_print_invalid_node(log_handle, addr);
      return;
   }

   platform_default_log("Tree stats: height %u\n", node.hdr->height);
   cache_print_stats(log_handle, cc);

   btree_node_unget(cc, cfg, &node);
}

/*
 * returns the space used in bytes by the range [start_key, end_key) in the
 * btree
 */
platform_status
btree_space_use_bytes(cache              *cc,
                      const btree_config *cfg,
                      uint64              root_addr,
                      page_type           type,
                      uint64             *bytes)
{
   uint64 meta_head = btree_root_to_meta_addr(cfg, root_addr, 0);
   return mini_space_use_bytes(cc, meta_head, type, bytes);
}

bool32
btree_verify_node(cache        *cc,
                  btree_config *cfg,
                  uint64        addr,
                  page_type     type,
                  bool32        is_left_edge)
{
   btree_node node = {.addr = addr};
   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   platform_status rc = btree_diagnostic_node_get(cc, cfg, &node, type);
   if (!SUCCESS(rc)) {
      return FALSE;
   }
   table_index idx;
   bool32      result = FALSE;

   for (idx = 0; idx < node.hdr->num_entries; idx++) {
      if (type == PAGE_TYPE_BRANCH) {
         if (node.hdr->height == 0) {
            const leaf_entry *entry;
            rc = btree_read_leaf_entry(cc, cfg, node.hdr, idx, &entry);
         } else {
            const index_entry *entry;
            rc = btree_read_index_entry(cc, cfg, node.hdr, idx, &entry);
         }
         if (!SUCCESS(rc)) {
            btree_node_unget(cc, cfg, &node);
            goto out;
         }
      }
      if (node.hdr->height == 0) {
         // leaf node
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (type == PAGE_TYPE_BRANCH) {
               const leaf_entry *next;
               rc = btree_read_leaf_entry(cc, cfg, node.hdr, idx + 1, &next);
               if (!SUCCESS(rc)) {
                  btree_node_unget(cc, cfg, &node);
                  goto out;
               }
            }
            key first  = btree_get_tuple_key(cfg, node.hdr, idx);
            key second = btree_get_tuple_key(cfg, node.hdr, idx + 1);
            // A user comparator may require full keys even when its ordering
            // is lexical. Compressed entries contain only suffixes.
            int cmp = node.hdr->prefix_length != 0
                         ? slice_lex_cmp(key_slice(first), key_slice(second))
                         : btree_key_compare(cfg, first, second);
            if (cmp >= 0) {
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
         rc         = btree_diagnostic_node_get(cc, cfg, &child, type);
         if (!SUCCESS(rc)) {
            btree_node_unget(cc, cfg, &node);
            goto out;
         }
         if (child.hdr->height != node.hdr->height - 1) {
            platform_error_log("height mismatch\n");
            platform_error_log("addr: %lu idx: %u\n", node.addr, idx);
            btree_node_unget(cc, cfg, &child);
            btree_node_unget(cc, cfg, &node);
            goto out;
         }
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (type == PAGE_TYPE_BRANCH) {
               const index_entry *next;
               rc = btree_read_index_entry(cc, cfg, node.hdr, idx + 1, &next);
               if (!SUCCESS(rc)) {
                  btree_node_unget(cc, cfg, &child);
                  btree_node_unget(cc, cfg, &node);
                  goto out;
               }
            }
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
            if (type == PAGE_TYPE_BRANCH) {
               const leaf_entry *child_entry;
               rc = btree_read_leaf_entry(cc, cfg, child.hdr, 0, &child_entry);
               if (!SUCCESS(rc)) {
                  btree_node_unget(cc, cfg, &child);
                  btree_node_unget(cc, cfg, &node);
                  goto out;
               }
            }
            if (0 < idx
                && btree_compare_leaf_key(
                      cfg,
                      child.hdr,
                      btree_get_pivot(cfg, node.hdr, idx),
                      btree_get_leaf_entry(cfg, child.hdr, 0))
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
            if (type == PAGE_TYPE_BRANCH
                && idx != btree_num_entries(node.hdr) - 1)
            {
               const index_entry *next;
               const leaf_entry  *last;
               rc = btree_read_index_entry(cc, cfg, node.hdr, idx + 1, &next);
               if (SUCCESS(rc)) {
                  rc = btree_read_leaf_entry(cc,
                                             cfg,
                                             child.hdr,
                                             btree_num_entries(child.hdr) - 1,
                                             &last);
               }
               if (!SUCCESS(rc)) {
                  btree_node_unget(cc, cfg, &child);
                  btree_node_unget(cc, cfg, &node);
                  goto out;
               }
            }
            if (idx != btree_num_entries(node.hdr) - 1
                && btree_compare_leaf_key(
                      cfg,
                      child.hdr,
                      btree_get_pivot(cfg, node.hdr, idx + 1),
                      btree_get_leaf_entry(
                         cfg, child.hdr, btree_num_entries(child.hdr) - 1))
                      < 0)
            {
               platform_error_log("child tuple larger than parent bound\n");
               platform_error_log("addr: %lu idx %u\n", node.addr, idx);
               platform_error_log("child addr: %lu idx %u\n", child.addr, idx);
               btree_print_locked_node(Platform_error_log_handle,
                                       cfg,
                                       cc,
                                       node.addr,
                                       node.hdr,
                                       type);
               btree_print_locked_node(Platform_error_log_handle,
                                       cfg,
                                       cc,
                                       child.addr,
                                       child.hdr,
                                       type);
               platform_assert(0);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         } else {
            // child index
            if (type == PAGE_TYPE_BRANCH
                && idx != btree_num_entries(node.hdr) - 1)
            {
               const index_entry *next;
               const index_entry *last;
               rc = btree_read_index_entry(cc, cfg, node.hdr, idx + 1, &next);
               if (SUCCESS(rc)) {
                  rc = btree_read_index_entry(cc,
                                              cfg,
                                              child.hdr,
                                              btree_num_entries(child.hdr) - 1,
                                              &last);
               }
               if (!SUCCESS(rc)) {
                  btree_node_unget(cc, cfg, &child);
                  btree_node_unget(cc, cfg, &node);
                  goto out;
               }
            }
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
               btree_print_locked_node(Platform_error_log_handle,
                                       cfg,
                                       cc,
                                       node.addr,
                                       node.hdr,
                                       type);
               btree_print_locked_node(Platform_error_log_handle,
                                       cfg,
                                       cc,
                                       child.addr,
                                       child.hdr,
                                       type);
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

   node.addr          = root_addr;
   platform_status rc = btree_diagnostic_node_get(cc, cfg, &node, type);
   if (!SUCCESS(rc)) {
      btree_print_invalid_node(Platform_default_log_handle, node.addr);
      return;
   }
   btree_print_locked_node(
      Platform_default_log_handle, cfg, cc, node.addr, node.hdr, type);

   for (h = node.hdr->height; h > 0; h--) {
      bool32 found;
      if (type == PAGE_TYPE_BRANCH) {
         rc = btree_find_pivot_checked(
            cfg, node.hdr, target, &found, &child_idx, NULL);
         if (!SUCCESS(rc)) {
            btree_print_invalid_node(Platform_default_log_handle, node.addr);
            btree_node_unget(cc, cfg, &node);
            return;
         }
      } else {
         child_idx = btree_find_pivot(cfg, node.hdr, target, &found);
      }
      if (child_idx < 0) {
         child_idx = 0;
      }
      const index_entry *entry;
      rc = btree_read_index_entry_for_type(
         cc, cfg, node.hdr, type, child_idx, &entry);
      if (!SUCCESS(rc)) {
         btree_print_invalid_node(Platform_default_log_handle, node.addr);
         btree_node_unget(cc, cfg, &node);
         return;
      }
      child_node.addr = index_entry_child_addr(entry);
      rc              = btree_diagnostic_node_get(cc, cfg, &child_node, type);
      if (!SUCCESS(rc)
          || (type == PAGE_TYPE_BRANCH
              && btree_height(child_node.hdr) != h - 1))
      {
         if (SUCCESS(rc)) {
            btree_node_unget(cc, cfg, &child_node);
         }
         btree_print_invalid_node(Platform_default_log_handle, child_node.addr);
         btree_node_unget(cc, cfg, &node);
         return;
      }
      btree_print_locked_node(Platform_default_log_handle,
                              cfg,
                              cc,
                              child_node.addr,
                              child_node.hdr,
                              type);
      btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   bool32 found;
   int64  idx;
   if (type == PAGE_TYPE_BRANCH) {
      rc = btree_find_tuple_checked(cfg, node.hdr, target, &found, &idx, NULL);
      if (!SUCCESS(rc)) {
         btree_print_invalid_node(Platform_default_log_handle, node.addr);
         btree_node_unget(cc, cfg, &node);
         return;
      }
   } else {
      idx = btree_find_tuple(cfg, node.hdr, target, &found);
   }
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
                  data_config  *data_cfg)
{
   btree_cfg->cache_cfg   = cache_cfg;
   btree_cfg->data_cfg    = data_cfg;
   btree_cfg->page_size   = cache_config_page_size(cache_cfg);
   btree_cfg->extent_size = cache_config_extent_size(cache_cfg);
   btree_cfg->blob_cfg    = (blob_build_config){
         .extent_batch             = 0,
         .page_batch               = 1,
         .subpage_batch            = 2,
         .blob_max_descriptor_size = BLOB_DEFAULT_MAX_DESCRIPTOR_SIZE,
   };

   uint64 page_size           = btree_page_size(btree_cfg);
   uint64 max_inline_key_size = MAX_INLINE_KEY_SIZE(page_size);
   uint64 max_inline_msg_size = MAX_INLINE_MESSAGE_SIZE(page_size);
   uint64 max_entry_space     = sizeof(leaf_entry) + max_inline_key_size
                            + max_inline_msg_size + sizeof(table_entry);
   platform_assert(max_entry_space < (page_size - sizeof(btree_hdr)) / 2);
}
