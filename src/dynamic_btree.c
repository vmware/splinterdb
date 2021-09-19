/* **********************************************************
 * Copyright 2018-2020 VMware, Inc.  All rights reserved. -- VMware Confidential
 * **********************************************************/


/*
 * btree_node.c --
 *
 *     This file contains the operations on individual b-tree nodes.
 */

#include "dynamic_btree.h"
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
 * gives the offset of each entry, in logically sorted order.
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

typedef uint16 entry_index; //  So we can make this bigger for bigger nodes.
typedef uint16 node_offset; //  So we can make this bigger for bigger nodes.
typedef uint16 node_index;  //  So we can make this bigger for bigger nodes.
typedef node_offset table_entry;
typedef uint8 inline_key_size;
typedef uint8 inline_message_size;

#define DYNAMIC_BTREE_MAX_HEIGHT 32

/***********************
 * Node headers
 ***********************/

#define DYNAMIC_BTREE_UNKNOWN (0x7fffffffUL)

struct PACKED dynamic_btree_hdr {
   uint64      next_addr;
   uint64      next_extent_addr;
   uint64      generation;
   uint8       height;
   node_offset next_entry;
   entry_index num_entries;
   table_entry offsets[];
};

static inline uint64
dynamic_btree_page_size(const dynamic_btree_config *cfg)
{
   return cfg->page_size;
}

static inline uint8
dynamic_btree_height(const dynamic_btree_hdr   *hdr)
{
   return hdr->height;
}

static inline table_entry *
dynamic_btree_get_table(dynamic_btree_hdr   *hdr)
{
   return hdr->offsets;
}

static inline entry_index
dynamic_btree_num_entries(const dynamic_btree_hdr   *hdr)
{
   return hdr->num_entries;
}

static inline void
dynamic_btree_increment_height(dynamic_btree_hdr   *hdr)
{
   hdr->height++;
}

static inline void
dynamic_btree_reset_node_entries(const dynamic_btree_config *cfg,
                         dynamic_btree_hdr          *hdr)
{
  hdr->num_entries = 0;
  hdr->next_entry = dynamic_btree_page_size(cfg);
}




/***********************************
 * Node entries
 ***********************************/

typedef struct PACKED index_entry {
  uint64          child_addr;
  uint32          num_kvs_in_tree;
  uint32          key_bytes_in_tree;
  uint32          message_bytes_in_tree;
  inline_key_size key_size;
  char            key[];
} index_entry;

_Static_assert(sizeof(index_entry) == sizeof(uint64) + 3 * sizeof(uint32) + sizeof(inline_key_size), "index_entry has wrong size");
_Static_assert(offsetof(index_entry, key) == sizeof(index_entry), "index_entry key has wrong offset");

typedef struct PACKED leaf_entry {
  inline_key_size     key_size;
  inline_message_size message_size;
  char                key_and_message[];
} leaf_entry;

_Static_assert(sizeof(leaf_entry) == sizeof(inline_key_size) + sizeof(inline_message_size), "leaf_entry has wrong size");
_Static_assert(offsetof(leaf_entry, key_and_message) == sizeof(leaf_entry), "leaf_entry key_and_data has wrong offset");


static inline uint64 index_entry_size(const slice key)
{
  return sizeof(index_entry) + slice_length(key);
}

static inline uint64 sizeof_index_entry(const index_entry *entry)
{
  return sizeof(*entry) + entry->key_size;
}

static inline slice index_entry_key_slice(index_entry *entry)
{
  return slice_create(entry->key_size, entry->key);
}

static inline char *index_entry_key_data(index_entry *entry)
{
  return entry->key;
}

__attribute__((unused))
static inline uint64 index_entry_key_size(const index_entry *entry)
{
  return entry->key_size;
}

static inline uint8 index_entry_child_addr(const index_entry *entry)
{
  return entry->child_addr;
}

static inline uint64 sizeof_leaf_entry(const leaf_entry *entry)
{
  return sizeof(*entry) + entry->key_size + entry->message_size;
}

static inline uint64 leaf_entry_size(const slice key, const slice message)
{
  return sizeof(leaf_entry) + slice_length(key) + slice_length(message);
}

static inline slice leaf_entry_key_slice(leaf_entry *entry)
{
  return slice_create(entry->key_size, entry->key_and_message);
}

__attribute__((unused))
static inline char *leaf_entry_key_data(leaf_entry *entry)
{
  return entry->key_and_message;
}

static inline uint64 leaf_entry_key_size(const leaf_entry *entry)
{
  return entry->key_size;
}

static inline slice leaf_entry_message_slice(leaf_entry *entry)
{
  return slice_create(entry->message_size, entry->key_and_message);
}

__attribute__((unused))
static inline char *leaf_entry_message_data(leaf_entry *entry)
{
  return entry->key_and_message + entry->key_size;
}

static inline uint64 leaf_entry_message_size(const leaf_entry *entry)
{
  return entry->message_size;
}




/**************************************
 * Basic get/set on index nodes
 **************************************/

static inline index_entry *
dynamic_btree_get_index_entry(const dynamic_btree_config *cfg,
                              const dynamic_btree_hdr    *hdr,
                              uint32                      k)
{
   platform_assert(diff_ptr(hdr, &hdr->offsets[hdr->num_entries + 1]) <= hdr->offsets[k]);
   platform_assert(hdr->offsets[k] <= dynamic_btree_page_size(cfg) - sizeof(index_entry));
   return (index_entry *)((uint8 *)hdr + hdr->offsets[k]);
}

static inline slice dynamic_btree_get_pivot(const dynamic_btree_config *cfg,
                                            const dynamic_btree_hdr    *hdr,
                                            uint32                      k)
{
  return index_entry_key_slice(dynamic_btree_get_index_entry(cfg, hdr, k));
}

static inline uint64 dynamic_btree_get_child_addr(const dynamic_btree_config *cfg,
                                                  const dynamic_btree_hdr    *hdr,
                                                  uint32                      k)
{
  return index_entry_child_addr(dynamic_btree_get_index_entry(cfg, hdr, k));
}

static inline void
dynamic_btree_fill_index_entry(index_entry *entry, slice new_pivot_key, uint64 new_addr, uint32 kv_pairs, uint32 key_bytes, uint32 message_bytes)
{
   memcpy(index_entry_key_data(entry), slice_data(new_pivot_key), slice_length(new_pivot_key));
   entry->key_size = slice_length(new_pivot_key);
   entry->child_addr = new_addr;
   entry->num_kvs_in_tree = kv_pairs;
   entry->key_bytes_in_tree = key_bytes;
   entry->message_bytes_in_tree = message_bytes;
}

static inline bool
dynamic_btree_set_index_entry(const dynamic_btree_config *cfg,
                              dynamic_btree_hdr          *hdr,
                              uint32                      k,
                              slice                       new_pivot_key,
                              uint64                      new_addr,
                              int64                       kv_pairs,
                              int64                       key_bytes,
                              int64                       message_bytes)
{
   if (k == 0)
      new_pivot_key.length = 0;

   if (k < hdr->num_entries) {
     index_entry *old_entry = dynamic_btree_get_index_entry(cfg, hdr, k);
     if (index_entry_size(new_pivot_key) <= sizeof_index_entry(old_entry)) {
       dynamic_btree_fill_index_entry(old_entry, new_pivot_key, new_addr, kv_pairs, key_bytes, message_bytes);
       return TRUE;
     }
     /* Fall through */
   }

   platform_assert(k <= hdr->num_entries);
   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k;
   if (hdr->next_entry - index_entry_size(new_pivot_key) < diff_ptr(hdr, &hdr->offsets[new_num_entries + 1])) {
     return FALSE;
   }

   index_entry *new_entry = (index_entry *)((char *)hdr + hdr->next_entry - index_entry_size(new_pivot_key));
   dynamic_btree_fill_index_entry(new_entry, new_pivot_key, new_addr, kv_pairs, key_bytes, message_bytes);

   hdr->offsets[k] = diff_ptr(hdr, new_entry);
   hdr->num_entries = new_num_entries;
   hdr->next_entry = diff_ptr(hdr, new_entry);
   return TRUE;
}

static inline bool
dynamic_btree_insert_index_entry(const dynamic_btree_config *cfg,
                                 dynamic_btree_hdr          *hdr,
                                 uint32                      k,
                                 slice                       new_pivot_key,
                                 uint64                      new_addr,
                                 int64                       kv_pairs,
                                 int64                       key_bytes,
                                 int64                       message_bytes)
{
  bool succeeded = dynamic_btree_set_index_entry(cfg, hdr, hdr->num_entries, new_pivot_key, new_addr, kv_pairs, key_bytes, message_bytes);
  if (succeeded) {
    node_offset this_entry_offset = hdr->offsets[hdr->num_entries - 1];
    memmove(&hdr->offsets[k+1], &hdr->offsets[k], (hdr->num_entries - k - 1) * sizeof(hdr->offsets[0]));
    hdr->offsets[k] = this_entry_offset;
  }
  return succeeded;
}





/**************************************
 * Basic get/set on leaf nodes
 **************************************/

static inline leaf_entry *
dynamic_btree_get_leaf_entry(const dynamic_btree_config *cfg,
                             const dynamic_btree_hdr    *hdr,
                             uint32                      k)
{
   platform_assert(diff_ptr(hdr, &hdr->offsets[hdr->num_entries + 1]) <= hdr->offsets[k]);
   platform_assert(hdr->offsets[k] <= dynamic_btree_page_size(cfg) - sizeof(leaf_entry));
   return (leaf_entry *)((uint8 *)hdr + hdr->offsets[k]);
}

static inline slice dynamic_btree_get_tuple_key(const dynamic_btree_config *cfg,
                                                const dynamic_btree_hdr    *hdr,
                                                uint32                      k)
{
  return leaf_entry_key_slice(dynamic_btree_get_leaf_entry(cfg, hdr, k));
}

static inline slice dynamic_btree_get_tuple_message(const dynamic_btree_config *cfg,
                                                    const dynamic_btree_hdr    *hdr,
                                                    uint32                      k)
{
  return leaf_entry_message_slice(dynamic_btree_get_leaf_entry(cfg, hdr, k));
}

static inline void
dynamic_btree_fill_leaf_entry(leaf_entry *entry, slice key, slice message)
{
  memcpy(entry->key_and_message, slice_data(key), slice_length(key));
  memcpy(entry->key_and_message + slice_length(key), slice_data(message), slice_length(message));
  entry->key_size = slice_length(key);
  entry->message_size = slice_length(message);
}

static inline bool
dynamic_btree_set_leaf_entry(const dynamic_btree_config *cfg,
                             dynamic_btree_hdr          *hdr,
                             uint32                      k,
                             slice                       new_key,
                             slice                       new_message)
{
   if (k < hdr->num_entries) {
     leaf_entry *old_entry = dynamic_btree_get_leaf_entry(cfg, hdr, k);
     if (leaf_entry_size(new_key, new_message) <= sizeof_leaf_entry(old_entry)) {
       dynamic_btree_fill_leaf_entry(old_entry, new_key, new_message);
       return TRUE;
     }
     /* Fall through */
   }

   platform_assert(k <= hdr->num_entries);
   uint64 new_num_entries = k < hdr->num_entries ? hdr->num_entries : k;
   if (hdr->next_entry - leaf_entry_size(new_key, new_message) < diff_ptr(hdr, &hdr->offsets[new_num_entries + 1])) {
     return FALSE;
   }

   leaf_entry *new_entry = (leaf_entry *)((char *)hdr + hdr->next_entry - leaf_entry_size(new_key, new_message));
   dynamic_btree_fill_leaf_entry(new_entry, new_key, new_message);

   hdr->offsets[k] = diff_ptr(hdr, new_entry);
   hdr->num_entries = new_num_entries;
   hdr->next_entry = diff_ptr(hdr, new_entry);

   return TRUE;
}

static inline bool
dynamic_btree_insert_leaf_entry(const dynamic_btree_config *cfg,
                                dynamic_btree_hdr          *hdr,
                                uint32                      k,
                                slice                       new_key,
                                slice                       new_message)
{
  bool succeeded = dynamic_btree_set_leaf_entry(cfg, hdr, hdr->num_entries, new_key, new_message);
  if (succeeded) {
    node_offset this_entry_offset = hdr->offsets[hdr->num_entries - 1];
    memmove(&hdr->offsets[k+1], &hdr->offsets[k], (hdr->num_entries - k - 1) * sizeof(hdr->offsets[0]));
    hdr->offsets[k] = this_entry_offset;
  }
  return succeeded;
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_find_pivot --
 *
 *      performs a binary search to find the extremal pivot relative to comp.
 *      attempts to reduce branches.
 *
 *      in the first step, computes the largest power of 2 less than num_entries,
 *      then starts the binary search on the two overlapping ranges of that
 *      size which cover all the indices. E.g. for 7, the power of 2 is 4
 *      and the ranges are:
 *      (1 2 3 (4) 5 6 7)
 *
 *      the binary search then continues on a range which is a power of two
 *
 *      NOTE: if comp is less_than or less_than_or_equal, there must be a valid
 *      pivot index to return.
 *
 *-----------------------------------------------------------------------------
 */

typedef enum lookup_type {
   less_than,
   less_than_or_equal,
   greater_than,
   greater_than_or_equal,
} lookup_type;

static inline uint32
round_down_logarithm(uint32 size)
{
   if (size <= 1) return 0;
   return (8 * sizeof(uint32)) - __builtin_clz(size - 1);
}

static inline void
dynamic_btree_update_lowerbound(uint16      *lo,
                                uint16      *mid,
                                int          cmp,
                                lookup_type  comp)
{
   switch(comp) {
      case less_than:
      case greater_than_or_equal:
         if (cmp < 0) *lo = *mid;
         break;
      case less_than_or_equal:
      case greater_than:
         if (cmp <= 0) *lo = *mid;
         break;
      default:
         platform_assert(0);
   }
}

static inline uint32
dynamic_btree_find_pivot(const dynamic_btree_config *cfg,
                         const dynamic_btree_hdr    *hdr,
                         slice                       key,
                         lookup_type                 comp)
{
   uint16 lo_idx = 0, mid_idx;
   uint32 i;
   int cmp;
   uint32 size = dynamic_btree_num_entries(hdr);

   // handle small sizes with special cases
   if (size == 0) {
      return 0;
   }

   if (size == 1) {
      //cmp = dynamic_btree_key_compare(cfg, dynamic_btree_get_pivot(cfg, hdr, 0), key);
      switch (comp) {
         case less_than:
         case less_than_or_equal:
            return 0;
         case greater_than:
         case greater_than_or_equal:
            return 1;
      }
   }

   // compute round down po2 then start binary search on overlapping ranges
   mid_idx = size - (1u << (round_down_logarithm(size) - 1));
   size = 1u << (round_down_logarithm(size) - 1);
   cmp = mid_idx == 0 ? -1 : dynamic_btree_key_compare(cfg, dynamic_btree_get_pivot(cfg, hdr, mid_idx), key);
   dynamic_btree_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);

   // continue binary search (ranges are now powers of 2)
   for (i = round_down_logarithm(size); i != 0; i--) {
      size /= 2;
      mid_idx = lo_idx + size;
      cmp = mid_idx == 0 ? -1 : dynamic_btree_key_compare(cfg, dynamic_btree_get_pivot(cfg, hdr, mid_idx), key);
      dynamic_btree_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);
   }

   // adjust final result based on comp
   switch(comp) {
      case less_than:
      case less_than_or_equal:
         return lo_idx;
      case greater_than:
      case greater_than_or_equal:
         return lo_idx + 1;
      default:
         platform_assert(0);
   }
   // should be unreachable
   return (uint16)-1;
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_find_tuple --
 *
 *      Returns idx such that
 *          - -1 <= idx < num_entries
 *          - forall i | 0 <= i <= idx         :: key_i <= key
 *          - forall i | idx < i < num_entries :: key   <  key_i
 *      Also
 *          - *found == 0 || *found == 1
 *          - *found == 1 <==> (0 <= idx && key_idx == key)
 *
 *-----------------------------------------------------------------------------
 */

/*
 * The C code below is a translation of the following verified dafny implementation.

method bsearch(s: seq<int>, k: int) returns (idx: int, f: bool)
  requires forall i, j | 0 <= i < j < |s| :: s[i] < s[j]
  ensures -1 <= idx < |s|
  ensures forall i | 0 <= i <= idx :: s[i] <= k
  ensures forall i | idx < i < |s| :: k < s[i]
  ensures f <==> (0 <= idx && s[idx] == k)
{
  var lo := -1;
  var hi := |s| - 1;

  f := false;

  while lo < hi
    invariant -1 <= lo <= hi < |s|
    invariant forall i | 0 <= i <= lo :: s[i] <= k
    invariant forall i | hi < i < |s| :: k < s[i]
    invariant f <==> (0 <= lo && s[lo] == k)
  {
    var mid := 1 + (lo + hi) / 2;
    if s[mid] <= k {
      lo := mid;
      f := s[mid] == k;
    } else {
      hi := mid-1;
    }
  }

  idx := lo;
}

*/

static inline int32
dynamic_btree_find_tuple(const dynamic_btree_config *cfg,
                         const dynamic_btree_hdr    *hdr,
                         slice                       key,
                         bool                       *found)
{
   int64 lo = -1, hi = dynamic_btree_num_entries(hdr) - 1;

   *found = 0;

   while (lo < hi)
   {
     int64 mid = 1 + (lo + hi) / 2;
     int cmp = mid == 0 ? -1 : dynamic_btree_key_compare(cfg, dynamic_btree_get_tuple_key(cfg, hdr, mid), key);
     if (cmp <= 0) {
       lo = mid;
       *found = cmp ? 0 : 1;
     } else {
       hi = mid - 1;
     }
   }

   return lo;
}

/*
 *-----------------------------------------------------------------------------
 * dynamic_btree_leaf_incorporate_tuple
 *
 *   Adds the give key and value to node (must be a leaf).
 *
 *   Returns code indicates whether the key was new, old, or the incorporation failed.
 *
 *   Pre-conditions:
 *      1. write lock on node
 *      2. node is a leaf
 *      3. node has space for another tuple
 *-----------------------------------------------------------------------------
 */

typedef enum leaf_add_result {
  leaf_add_result_no_room,
  leaf_add_result_new_key,
  leaf_add_result_existing_key
} leaf_add_result;

static inline slice
dynamic_btree_merge_tuples(const dynamic_btree_config *cfg,
                           slice                       key,
                           slice                       old_data,
                           slice                       new_data,
                           char                        merged_data[static MAX_INLINE_MESSAGE_SIZE])
{
   // FIXME: [yfogel 2020-01-11] If/when we have start/end compaction callbacks
   //    this call is actually violating the contract (it's being called
   //    outside of [start,end].
   //    If/when we add those other callbacks.. we could just call them right
   //    here (as if it was a tiny compaction), or add a separate parameter
   //    to the existing callbacks to indicate it's a one-off
   //    Until/unless we add start/end this doesn't matter
  slice tmp = slice_create(0, merged_data);
  slice_copy_contents(&tmp, new_data);
  data_merge_tuples(cfg->data_cfg, key, old_data, &tmp);
  return tmp;
}

static inline leaf_add_result
dynamic_btree_leaf_incorporate_tuple(const dynamic_btree_config *cfg,
                                     dynamic_btree_scratch      *scratch,
                                     dynamic_btree_hdr          *hdr,
                                     slice                       key,
                                     slice                       message,
                                     uint64                     *generation)
{
   bool succeeded;
   bool found;
   debug_assert(dynamic_btree_height(cfg, hdr) == 0);
   debug_assert(!dynamic_btree_is_packed(cfg, hdr));

   uint64 idx = dynamic_btree_find_tuple(cfg, hdr, key, &found);

   if (!found) {
     succeeded = dynamic_btree_insert_leaf_entry(cfg, hdr, idx, key, message);
     if (succeeded) {
       *generation = hdr->generation++;
       return leaf_add_result_new_key;
     } else {
       return leaf_add_result_no_room;
     }
   }

   leaf_entry *entry = dynamic_btree_get_leaf_entry(cfg, hdr, idx);
   slice merged_message = dynamic_btree_merge_tuples(cfg, key, leaf_entry_message_slice(entry), message, scratch->add_tuple.merged_data);
   succeeded = dynamic_btree_set_leaf_entry(cfg, hdr, idx, key, merged_message);
   // FIXME: [yfogel 2020-01-11] We should fail the insert if any of:
   //  - old data is INVALID
   //  - new data is INVALID (probably failed earlier)
   //  - merged is INVALID

   if (succeeded) {
     *generation = hdr->generation++;
     return leaf_add_result_existing_key;
   } else {
     return leaf_add_result_no_room;
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_split_leaf --
 *
 *      Splits the node at left_addr into a new node at right_addr.
 *
 *      Assumes write lock on both nodes.
 *
 *-----------------------------------------------------------------------------
 */

/* Choose a splitting point so that we are guaranteed to be able to
   insert the given key-message pair into the correct node after the
   split. Assumes all leaf entries are at most half the total free
   space in an empty leaf. */
static inline uint64
dynamic_btree_choose_leaf_split(const dynamic_btree_config *cfg, // IN
                                const dynamic_btree_hdr    *hdr, // IN
                                const slice                 key, // IN
                                const slice                 message, // IN
                                slice                      *splitting_key) // OUT
{
   /* Split the content by bytes -- roughly half the bytes go to the
      right node.  So count the bytes, including the new entry to be
      inserted. */
  uint64 total_entry_bytes = leaf_entry_size(key, message);
   for (uint64 i = 0; i < dynamic_btree_num_entries(hdr); i++) {
     leaf_entry *entry  = dynamic_btree_get_leaf_entry(cfg, hdr, i);
     total_entry_bytes += sizeof_leaf_entry(entry);
   }
   total_entry_bytes += (dynamic_btree_num_entries(hdr) + 1) * sizeof(table_entry);

   bool found;
   uint64 idx = dynamic_btree_find_tuple(cfg, hdr, key, &found);
   uint64 total_node_space = cfg->page_size - sizeof(*hdr);

   /* Now figure out the number of entries to move, and figure out how
      much free space will be created in the left_hdr by the split. */
   uint64 target_left_entries  = 0;
   uint64 new_left_entry_bytes = 0;
   while (new_left_entry_bytes < total_entry_bytes / 2) {
     leaf_entry *entry = dynamic_btree_get_leaf_entry(cfg, hdr, target_left_entries);
     if (target_left_entries == idx) {
       if (new_left_entry_bytes + sizeof(table_entry) + leaf_entry_size(key, message) < total_node_space) {
         new_left_entry_bytes += sizeof(table_entry) + leaf_entry_size(key, message);
         *splitting_key = leaf_entry_key_slice(entry);
         if (new_left_entry_bytes >= total_entry_bytes / 2)
           break;
       } else {
         break;
       }
     }
     if (new_left_entry_bytes + sizeof(table_entry) + sizeof_leaf_entry(entry) < total_node_space) {
       new_left_entry_bytes += sizeof(table_entry) + sizeof_leaf_entry(entry);
       target_left_entries++;
       if (target_left_entries == idx) {
         *splitting_key = key;
       } else {
         leaf_entry *next_entry = dynamic_btree_get_leaf_entry(cfg, hdr, target_left_entries);
         *splitting_key = leaf_entry_key_slice(next_entry);
       }
     } else {
       break;
     }
   }
   return target_left_entries;
}

static inline void
dynamic_btree_split_leaf_build_right_node(const dynamic_btree_config *cfg, // IN
                                          const dynamic_btree_hdr    *left_hdr, // IN
                                          uint64                      target_left_entries, // IN
                                          dynamic_btree_hdr          *right_hdr) // IN/OUT
{
  uint64 target_right_entries = dynamic_btree_num_entries(left_hdr) - target_left_entries;

   /* Build the right node. */
   memmove(right_hdr, left_hdr, sizeof(*right_hdr));
   dynamic_btree_reset_node_entries(cfg, right_hdr);
   for (uint64 i = 0; i < target_right_entries; i++) {
     leaf_entry *entry = dynamic_btree_get_leaf_entry(cfg, left_hdr, target_left_entries + i);
     dynamic_btree_set_leaf_entry(cfg, right_hdr, i, leaf_entry_key_slice(entry), leaf_entry_message_slice(entry));
   }

   platform_assert(0);
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_defragment_leaf --
 *
 *      Defragment a node
 *
 *-----------------------------------------------------------------------------
 */
static inline void
dynamic_btree_defragment_leaf(const dynamic_btree_config *cfg, // IN
                              dynamic_btree_scratch      *scratch,
                              dynamic_btree_hdr          *hdr) // IN
{
  dynamic_btree_hdr *scratch_hdr = (dynamic_btree_hdr *)scratch->defragment_node.scratch_node;
  memcpy(scratch_hdr, hdr, dynamic_btree_page_size(cfg));
  dynamic_btree_reset_node_entries(cfg, hdr);
  for (uint64 i = 0; i < dynamic_btree_num_entries(scratch_hdr); i++) {
    leaf_entry *entry = dynamic_btree_get_leaf_entry(cfg, scratch_hdr, i);
    dynamic_btree_set_leaf_entry(cfg, hdr, i, leaf_entry_key_slice(entry), leaf_entry_message_slice(entry));
  }
}

static inline void
dynamic_btree_truncate_leaf(const dynamic_btree_config *cfg, // IN
                    dynamic_btree_scratch      *scratch,
                    dynamic_btree_hdr          *hdr, // IN
                    uint64              target_entries) // IN
{
   uint64 new_next_entry = dynamic_btree_page_size(cfg);

   for (uint64 i = 0; i < target_entries; i++) {
      if (hdr->offsets[i] < new_next_entry)
         new_next_entry = hdr->offsets[i];
   }

   hdr->num_entries = target_entries;

   if (new_next_entry < dynamic_btree_page_size(cfg) / 4)
      dynamic_btree_defragment_leaf(cfg, scratch, hdr);
   else
      hdr->next_entry = new_next_entry;
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_split_index --
 *
 *      Splits the node at left_addr into a new node at right_addr.
 *
 *      Assumes write lock on both nodes.
 *
 *-----------------------------------------------------------------------------
 */

static inline bool
dynamic_btree_index_is_full(const dynamic_btree_config *cfg, // IN
                            const dynamic_btree_hdr    *hdr) // IN
{
  return hdr->next_entry - sizeof(index_entry) - MAX_KEY_SIZE < diff_ptr(hdr, &hdr->offsets[hdr->num_entries + 2]);
}

static inline uint64
dynamic_btree_choose_index_split(const dynamic_btree_config *cfg, // IN
                                 const dynamic_btree_hdr    *hdr) // IN
{
   /* Split the content by bytes -- roughly half the bytes go to the
      right node.  So count the bytes. */
   uint64 total_entry_bytes = 0;
   for (uint64 i = 0; i < dynamic_btree_num_entries(hdr); i++) {
     index_entry *entry  = dynamic_btree_get_index_entry(cfg, hdr, i);
     total_entry_bytes += sizeof_index_entry(entry);
   }

   /* Now figure out the number of entries to move, and figure out how
      much free space will be created in the left_hdr by the split. */
   uint64 target_left_entries  = 0;
   uint64 new_left_entry_bytes = 0;
   while (new_left_entry_bytes < total_entry_bytes / 2) {
     index_entry *entry = dynamic_btree_get_index_entry(cfg, hdr, target_left_entries);
     new_left_entry_bytes += sizeof_index_entry(entry);
     target_left_entries++;
   }
   return target_left_entries;
}

static inline void
dynamic_btree_split_index_build_right_node(const dynamic_btree_config *cfg, // IN
                                           const dynamic_btree_hdr    *left_hdr, // IN
                                           uint64                      target_left_entries, // IN
                                           dynamic_btree_hdr          *right_hdr) // IN/OUT
{
  uint64 target_right_entries = dynamic_btree_num_entries(left_hdr) - target_left_entries;

   /* Build the right node. */
   memmove(right_hdr, left_hdr, sizeof(*right_hdr));
   dynamic_btree_reset_node_entries(cfg, right_hdr);
   for (uint64 i = 0; i < target_right_entries; i++) {
     index_entry *entry = dynamic_btree_get_index_entry(cfg, left_hdr, target_left_entries + i);
     dynamic_btree_set_index_entry(cfg, right_hdr, i, index_entry_key_slice(entry), index_entry_child_addr(entry),
                                   entry->num_kvs_in_tree, entry->key_bytes_in_tree, entry->message_bytes_in_tree);
   }

   platform_assert(0);
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_defragment_index --
 *
 *      Defragment a node
 *
 *-----------------------------------------------------------------------------
 */
static inline void
dynamic_btree_defragment_index(const dynamic_btree_config *cfg, // IN
                               dynamic_btree_scratch      *scratch,
                               dynamic_btree_hdr          *hdr) // IN
{
  dynamic_btree_hdr *scratch_hdr = (dynamic_btree_hdr *)scratch->defragment_node.scratch_node;
  memcpy(scratch_hdr, hdr, dynamic_btree_page_size(cfg));
  dynamic_btree_reset_node_entries(cfg, hdr);
  for (uint64 i = 0; i < dynamic_btree_num_entries(scratch_hdr); i++) {
    index_entry *entry = dynamic_btree_get_index_entry(cfg, scratch_hdr, i);
    dynamic_btree_set_index_entry(cfg, hdr, i, index_entry_key_slice(entry), index_entry_child_addr(entry),
                                  entry->num_kvs_in_tree, entry->key_bytes_in_tree, entry->message_bytes_in_tree);
  }
}

static inline void
dynamic_btree_truncate_index(const dynamic_btree_config *cfg, // IN
                             dynamic_btree_scratch      *scratch,
                             dynamic_btree_hdr          *hdr, // IN
                             uint64                      target_entries) // IN
{
   uint64 new_next_entry = dynamic_btree_page_size(cfg);
   for (uint64 i = 0; i < target_entries; i++)
      if (hdr->offsets[i] < new_next_entry)
         new_next_entry = hdr->offsets[i];

   hdr->num_entries = target_entries;

   if (new_next_entry < dynamic_btree_page_size(cfg) / 4)
      dynamic_btree_defragment_index(cfg, scratch, hdr);
   else
      hdr->next_entry = new_next_entry;
}

static inline void
dynamic_btree_init_hdr(const dynamic_btree_config *cfg,
                       dynamic_btree_hdr          *hdr)
{
   memset(hdr, 0, sizeof(*hdr));
   hdr->next_entry       = cfg->page_size;
}



































/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_alloc --
 *
 *      Allocates a node from the preallocator. Will refill it if there are no
 *      more nodes available for the given height.
 *
 *      Note: dynamic_btree_alloc returns the addr unlocked.
 *
 *-----------------------------------------------------------------------------
 */

void
dynamic_btree_alloc(cache *               cc,
                    mini_allocator       *mini,
                    uint64                height,
                    slice                 key,
                    uint64 *              next_extent,
                    page_type             type,
                    dynamic_btree_node *  node)
{
   node->addr = mini_allocator_alloc(mini, height, key, next_extent);
   debug_assert(node->addr != 0);
   node->page = cache_alloc(cc, node->addr, type);
   node->hdr  = (dynamic_btree_hdr *)(node->page->data);
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_node_[get,release] --
 *
 *      Gets the node with appropriate lock or releases the lock.
 *
 *-----------------------------------------------------------------------------
 */

static inline void
dynamic_btree_node_get(cache                      *cc,
                       const dynamic_btree_config *cfg,
                       dynamic_btree_node         *node,
                       page_type                   type)
{
   debug_assert(node->addr != 0);

   node->page = cache_get(cc, node->addr, TRUE, type);
   node->hdr  = (dynamic_btree_hdr *)(node->page->data);
}

static inline bool
dynamic_btree_node_claim(cache              *cc,   // IN
                 const dynamic_btree_config *cfg,  // IN
                 dynamic_btree_node         *node) // IN
{
   return cache_claim(cc, node->page);
}

static inline void
dynamic_btree_node_lock(cache              *cc,   // IN
                const dynamic_btree_config *cfg,  // IN
                dynamic_btree_node         *node) // IN
{
   cache_lock(cc, node->page);
   cache_mark_dirty(cc, node->page);
}

static inline void
dynamic_btree_node_unlock(cache              *cc,   // IN
                  const dynamic_btree_config *cfg,  // IN
                  dynamic_btree_node         *node) // IN
{
   cache_unlock(cc, node->page);
}

static inline void
dynamic_btree_node_unclaim(cache              *cc,   // IN
                   const dynamic_btree_config *cfg,  // IN
                   dynamic_btree_node         *node) // IN
{
   cache_unclaim(cc, node->page);
}

void
dynamic_btree_node_unget(cache              *cc,   // IN
                 const dynamic_btree_config *cfg,  // IN
                 dynamic_btree_node         *node) // IN
{
   cache_unget(cc, node->page);
   node->page = NULL;
   node->hdr  = NULL;
}

static inline void
dynamic_btree_node_full_unlock(cache              *cc,   // IN
                       const dynamic_btree_config *cfg,  // IN
                       dynamic_btree_node         *node) // IN
{
   dynamic_btree_node_unlock(cc, cfg, node);
   dynamic_btree_node_unclaim(cc, cfg, node);
   dynamic_btree_node_unget(cc, cfg, node);
}

static inline void
dynamic_btree_node_get_from_cache_ctxt(const dynamic_btree_config *cfg, // IN
                                       cache_async_ctxt           *ctxt, // IN
                                       dynamic_btree_node         *node) // OUT
{
   node->addr = ctxt->page->disk_addr;
   node->page = ctxt->page;
   node->hdr = (dynamic_btree_hdr *)node->page->data;
}





static inline bool
dynamic_btree_addrs_share_extent(const dynamic_btree_config *cfg,
                                 uint64                      left_addr,
                                 uint64                      right_addr)
{
   return right_addr / cfg->extent_size == left_addr / cfg->extent_size;
}

static inline uint64
dynamic_btree_get_extent_base_addr(const dynamic_btree_config *cfg,
                                   dynamic_btree_node         *node)
{
   return node->addr / cfg->extent_size * cfg->extent_size;
}

static inline uint64
dynamic_btree_root_to_meta_addr(const dynamic_btree_config *cfg,
                                uint64                      root_addr,
                                uint64                      meta_page_no)
{
   return root_addr + (meta_page_no + 1) * cfg->page_size;
}












/*----------------------------------------------------------
 * Creating and destroying B-trees.
 *
 *
 *
 *
 *
 *----------------------------------------------------------
 */




uint64
dynamic_btree_init(cache                      *cc,
                   const dynamic_btree_config *cfg,
                   mini_allocator             *mini,
                   page_type                   type)
{
   // get a free node for the root
   // we don't use the next_addr arr for this, since the root doesn't
   // maintain constant height
   allocator *     al   = cache_allocator(cc);
   uint64          base_addr;
   platform_status rc = allocator_alloc_extent(al, &base_addr);
   platform_assert_status_ok(rc);
   page_handle *root_page = cache_alloc(cc, base_addr, type);

   // FIXME: [yfogel 2020-07-01] maybe here (or refactor?)
   //    we need to be able to have range tree initialized
   // set up the root
   dynamic_btree_node root;
   root.page = root_page;
   root.addr = base_addr;
   root.hdr  = (dynamic_btree_hdr *)root_page->data;

   dynamic_btree_init_hdr(cfg, root.hdr);

   cache_mark_dirty(cc, root.page);
   // release root
   cache_unlock(cc, root_page);
   cache_unclaim(cc, root_page);
   cache_unget(cc, root_page);

   // set up the mini allocator
   mini_allocator_init(mini, cc, cfg->data_cfg, root.addr + cfg->page_size, 0,
                       DYNAMIC_BTREE_MAX_HEIGHT, type);

   return root.addr;
}

/*
 * By separating should_zap and zap, this allows us to use a single
 * refcount to control multiple b-trees.  For example a branch
 * in splinter that has a point tree and range-delete tree
 */
bool
dynamic_btree_should_zap_dec_ref(cache              *cc,
                         const dynamic_btree_config *cfg,
                         uint64                      root_addr,
                         page_type                   type)
{
   // FIXME: [yfogel 2020-07-06] Should we assert that both cfgs provide
   //       work the same w.r.t. root_to_meta_addr?
   //       Right now we're always passing in the point config
   uint64       meta_page_addr = dynamic_btree_root_to_meta_addr(cfg, root_addr, 0);
   page_handle *meta_page;
   uint64       wait = 1;

   debug_assert(type == PAGE_TYPE_MEMTABLE || type == PAGE_TYPE_BRANCH);
   while (1) {
      meta_page = cache_get(cc, meta_page_addr, TRUE, type);
      if (cache_claim(cc, meta_page))
         break;
      cache_unget(cc, meta_page);
      platform_sleep(wait);
      wait *= 2;
   }
   cache_lock(cc, meta_page);

   // ALEX: We don't hold the root lock, but we only dealloc from here
   // so there shouldn't be a race between this refcount check and the
   // dealloc
   uint64 ref = cache_get_ref(cc, root_addr);
   //if (ref != 2)
   //   platform_log("dec_ref %lu; %u\n", root_addr, ref - 1);
   //else
   //   platform_log("dec_ref %lu; %u\n", root_addr, 0);

   bool should_zap;
   if (ref > 2) {
      cache_dealloc(cc, root_addr, type);
      should_zap = FALSE;
   } else {
      // we are responsible for zapping the whole tree
      // If we're talking about a branch we should zap the whole branch
      should_zap = TRUE;
   }
   cache_unlock(cc, meta_page);
   cache_unclaim(cc, meta_page);
   cache_unget(cc, meta_page);
   return should_zap;
}

void
dynamic_btree_inc_range(cache              *cc,
                const dynamic_btree_config *cfg,
                uint64                      root_addr,
                const slice                 start_key,
                const slice                 end_key)
{
   uint64 meta_page_addr = dynamic_btree_root_to_meta_addr(cfg, root_addr, 0);
   if (!slice_is_null(start_key) && !slice_is_null(end_key)) {
      debug_assert(dynamic_btree_key_compare(cfg, start_key, end_key) < 0);
   }
   mini_allocator_inc_range(cc, cfg->data_cfg, PAGE_TYPE_BRANCH,
                            meta_page_addr, start_key, end_key);
}

bool
dynamic_btree_zap_range(cache              *cc,
                const dynamic_btree_config *cfg,
                uint64                      root_addr,
                const slice                 start_key,
                const slice                 end_key,
                page_type                   type)
{
   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   debug_assert(type == PAGE_TYPE_BRANCH || slice_is_null(start_key));

   if (!slice_is_null(start_key) && !slice_is_null(end_key)) {
      platform_assert(dynamic_btree_key_compare(cfg, start_key, end_key) < 0);
   }

   uint64 meta_page_addr = dynamic_btree_root_to_meta_addr(cfg, root_addr, 0);
   bool fully_zapped = mini_allocator_zap(cc, cfg->data_cfg, meta_page_addr,
                                          start_key, end_key, type);
   return fully_zapped;
}

bool dynamic_btree_zap(cache              *cc,
               const dynamic_btree_config *cfg,
               uint64                      root_addr,
               page_type                   type)
{
   return dynamic_btree_zap_range(cc, cfg, root_addr, null_slice, null_slice, type);
}

page_handle *
dynamic_btree_blind_inc(cache        *cc,
                dynamic_btree_config *cfg,
                uint64                root_addr,
                page_type             type)
{
   //platform_log("(%2lu)blind inc %14lu\n", platform_get_tid(), root_addr);
   uint64 meta_page_addr = dynamic_btree_root_to_meta_addr(cfg, root_addr, 0);
   return mini_allocator_blind_inc(cc, meta_page_addr);
}

void
dynamic_btree_blind_zap(cache              *cc,
                const dynamic_btree_config *cfg,
                page_handle                *meta_page,
                page_type                   type)
{
   //platform_log("(%2lu)blind zap %14lu\n", platform_get_tid(), root_addr);
   mini_allocator_blind_zap(cc, type, meta_page);
}














/**********************************************************************
 * The process of splitting a child is divided into five steps in
 * order to minimize the amount of time that we hold write-locks on
 * the parent and child:
 *
 * 1. dynamic_btree_alloc_shared_node.
 *    Allocate a shared node that will become the right child after
 *    the split.  This does not require any locks on the parent (but,
 *    typically, the thread performing the split will hold a claim on
 *    the parent durin this step).
 *
 * 2. dynamic_btree_add_shared_pivot.
 *    Insert a new pivot in the parent for the new child.  This pivot
 *    points to the old child.  This step requires a write-lock on the
 *    parent.  The parent can be completely unlocked as soon as this
 *    step is complete.
 *
 * 3. dynamic_btree_split_{leaf,index}_build_right_node
 *    Fill in the contents of the right child.  No lock on parent
 *    required.
 *
 * 4. cache_unshare.
 *    Unshare the old and new child.  This is the moment that the new
 *    child becomes visible to other threads.  As soon as this step is
 *    completed, the new child can be unlocked completely.
 *
 * 5. dynamic_btree_truncate_{leaf,index}
 *    Truncate (and optionally defragment) the old child.  This is the
 *    only step that requires a write-lock on the old child.
 *
 *
 *
 * Note: if we wanted to maintain rank information in the parent when
 * splitting one of its children, we could do that by holding the lock
 * on the parent a bit longer.  But we don't need that in the
 * memtable, so not bothering now.
 */


/* Requires write-lock on parent.
   Requires read-lock on child. */
static inline uint64
dynamic_btree_add_shared_pivot(const dynamic_btree_config *cfg,
                       dynamic_btree_node                 *parent,
                       uint64                              parents_pivot_idx,
                       const slice                         key_to_be_inserted,
                       const slice                         message_to_be_inserted,
                       const dynamic_btree_node           *child,
                       dynamic_btree_node                 *new_child)
{
   debug_assert(dynamic_btree_height(parent->hdr) != 0);

   uint64 childs_split_position;
   slice pivot_key;
   if (dynamic_btree_height(child->hdr) != 0) {
      childs_split_position = dynamic_btree_choose_index_split(cfg, child->hdr);
      pivot_key = dynamic_btree_get_pivot(cfg, child->hdr, childs_split_position);
   } else {
      childs_split_position = dynamic_btree_choose_leaf_split(cfg, child->hdr, key_to_be_inserted, message_to_be_inserted, &pivot_key);
      debug_assert(0 < childs_split_position);
   }

   dynamic_btree_insert_index_entry(cfg, parent->hdr, parents_pivot_idx + 1, pivot_key, new_child->addr,
                                    DYNAMIC_BTREE_UNKNOWN, DYNAMIC_BTREE_UNKNOWN, DYNAMIC_BTREE_UNKNOWN);

   index_entry *entry = dynamic_btree_get_index_entry(cfg, parent->hdr, parents_pivot_idx);
   entry->num_kvs_in_tree = entry->key_bytes_in_tree = entry->message_bytes_in_tree = DYNAMIC_BTREE_UNKNOWN;

   return childs_split_position;
}

/* Requires:
   - claim on parent
   - claim on child
   Upon completion:
   - parent is unlocked
   - claim on new_child
   - other child is unlocked
  Upon return: new_child is the child for key_of_interest
*/
static inline int
dynamic_btree_split_child(cache                      *cc,
                          const dynamic_btree_config *cfg,
                          mini_allocator             *mini,
                          dynamic_btree_scratch      *scratch,
                          dynamic_btree_node         *parent,
                          uint64                      index_of_child_in_parent,
                          const slice                 key_to_be_inserted,
                          const slice                 message_to_be_inserted,
                          dynamic_btree_node         *child,
                          dynamic_btree_node         *new_child) // OUT
{
  dynamic_btree_node right_child;

  /* p: claim, c: claim, nc: - */

  /* dynamic_btree_alloc_shared_node(cc, cfg, mini, child, &right_child); */
  dynamic_btree_alloc(cc, mini, dynamic_btree_height(child->hdr), null_slice, NULL, PAGE_TYPE_MEMTABLE, &right_child);
  cache_share(cc, child->page, right_child.page);

  /* p: claim, c: claim, nc: write */

  dynamic_btree_node_lock(cc, cfg, parent);
  uint64 childs_split_position = dynamic_btree_add_shared_pivot(cfg, parent, index_of_child_in_parent + 1,
                                                                key_to_be_inserted, message_to_be_inserted,
                                                                child, &right_child);
  { /* pivot_key is not safe to access after we release the lock on
       parent, so limit its scope. */
    slice pivot_key = dynamic_btree_get_pivot(cfg, parent->hdr, index_of_child_in_parent + 1);
    if (dynamic_btree_key_compare(cfg, key_to_be_inserted, pivot_key) < 0)
      *new_child = *child;
    else
      *new_child = right_child;
  }
  dynamic_btree_node_full_unlock(cc, cfg, parent);

  /* p: unlocked, c: claim, rc: write */

  if (dynamic_btree_height(child->hdr) == 0) {
    dynamic_btree_split_leaf_build_right_node(cfg, child->hdr, childs_split_position, right_child.hdr);
  } else {
    dynamic_btree_split_index_build_right_node(cfg, child->hdr, childs_split_position, right_child.hdr);
  }

  /* p: unlocked, c: claim, rc: write */

  cache_unshare(cc, right_child.page);
  dynamic_btree_node_unlock(cc, cfg, &right_child);
  if (new_child->addr == child->addr) {
    dynamic_btree_node_unclaim(cc, cfg, &right_child);
    dynamic_btree_node_unget(cc, cfg, &right_child);
  }

  /* p: unlocked, c: claim, rc: if rc == nc then claim else unlocked */

  dynamic_btree_node_lock(cc, cfg, child);
  if (dynamic_btree_height(child->hdr) == 0) {
    dynamic_btree_truncate_leaf(cfg, scratch, child->hdr, childs_split_position);
  } else {
    dynamic_btree_truncate_index(cfg, scratch, child->hdr, childs_split_position);
  }
  dynamic_btree_node_unlock(cc, cfg, child);

  /* p: unlocked, c: claim, rc: if rc == nc then claim else unlocked */

  if (new_child->addr != child->addr) {
    dynamic_btree_node_unclaim(cc, cfg, child);
    dynamic_btree_node_unget(cc, cfg, child);
  }

  /* p: unlocked, c: if c == nc then claim else unlocked, rc: if rc == nc then claim else unlocked */

  return 0;
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_grow_root --
 *
 *      Adds a new root above the root.
 *
 * Requires: claim on root_node
 *
 * Upon return:
 * - root is still claimed
 * - child is claimed
 *
 *-----------------------------------------------------------------------------
 */

static inline uint64 add_unknown(uint32 a, int32 b)
{
  if (a != DYNAMIC_BTREE_UNKNOWN && b != DYNAMIC_BTREE_UNKNOWN)
    return a + b;
  else return DYNAMIC_BTREE_UNKNOWN;
}

static inline void accumulate_node_ranks(const dynamic_btree_config *cfg,
                                         const dynamic_btree_hdr    *hdr,
                                         int                         from,
                                         int                         to,
                                         uint32                     *num_kvs,
                                         uint32                     *key_bytes,
                                         uint32                     *message_bytes)
{
  int i;

  if (dynamic_btree_height(hdr) == 0) {
    for (i = from; i < to; i++) {
      leaf_entry *entry = dynamic_btree_get_leaf_entry(cfg, hdr, i);
      *key_bytes = add_unknown(*key_bytes, leaf_entry_key_size(entry));
      *message_bytes = add_unknown(*message_bytes, leaf_entry_message_size(entry));
    }
    *num_kvs = to - from;
  } else {
    for (i = from; i < to; i++) {
      index_entry *entry = dynamic_btree_get_index_entry(cfg, hdr, i);
      *num_kvs = add_unknown(*num_kvs, entry->num_kvs_in_tree);
      *key_bytes = add_unknown(*key_bytes, entry->key_bytes_in_tree);
      *message_bytes = add_unknown(*message_bytes, entry->message_bytes_in_tree);
    }
  }
}

static inline int
dynamic_btree_grow_root(cache                      *cc, // IN
                        const dynamic_btree_config *cfg, // IN
                        mini_allocator             *mini, // IN/OUT
                        dynamic_btree_node         *root_node, // IN/OUT
                        dynamic_btree_node         *child) // OUT
{
   uint32 num_kvs = 0, key_bytes = 0, message_bytes = 0;
   accumulate_node_ranks(cfg, root_node->hdr, 0, dynamic_btree_num_entries(root_node->hdr), &num_kvs, &key_bytes, &message_bytes);

   // allocate a new left node
   dynamic_btree_alloc(cc, mini, dynamic_btree_height(root_node->hdr), null_slice, NULL, PAGE_TYPE_MEMTABLE, child);

   // copy root to child
   memmove(child->hdr, root_node->hdr, dynamic_btree_page_size(cfg));
   dynamic_btree_node_unlock(cc, cfg, child);

   dynamic_btree_node_lock(cc, cfg, root_node);

   dynamic_btree_reset_node_entries(cfg, root_node->hdr);
   dynamic_btree_increment_height(root_node->hdr);
   static char dummy_data[1];
   static slice dummy_pivot = (slice){.length = 0, .data = dummy_data };
   dynamic_btree_set_index_entry(cfg, root_node->hdr, 0, dummy_pivot, child->addr,
                                 num_kvs, key_bytes, message_bytes);

   dynamic_btree_node_unlock(cc, cfg, root_node);

   return 0;
}

/* Requires:
   - claim on root
   Upon completion:
   - root is unlocked
   - read-lock on child of root for key_to_be_inserted
   - other child of root is unlocked
*/
static inline int
dynamic_btree_split_root(cache                      *cc,
                         const dynamic_btree_config *cfg,
                         mini_allocator             *mini,
                         dynamic_btree_scratch      *scratch,
                         dynamic_btree_node         *root_node,
                         const slice                 key_to_be_inserted,
                         const slice                 message_to_be_inserted,
                         dynamic_btree_node         *child) // OUT
{
  dynamic_btree_node child_of_root;

  int failure = dynamic_btree_grow_root(cc, cfg, mini, root_node, &child_of_root);
  if (failure)
    return failure;

  failure = dynamic_btree_split_child(cc, cfg, mini, scratch, root_node, 0,
                              key_to_be_inserted, message_to_be_inserted,
                              &child_of_root, child);

  return failure;
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_insert --
 *
 *      Inserts the tuple into the dynamic dynamic_btree.
 *
 *      Return value:
 *      success       -- the tuple has been inserted
 *      locked        -- the insert failed, but the caller didn't fill the tree
 *      lock acquired -- the insert failed, and the caller filled the tree
 *
 *-----------------------------------------------------------------------------
 */

platform_status
dynamic_btree_insert(cache                      *cc, // IN
                     const dynamic_btree_config *cfg, // IN
                     dynamic_btree_scratch      *scratch, // IN
                     uint64                      root_addr, // IN
                     mini_allocator             *mini, // IN
                     slice                       key, // IN
                     slice                       message, // IN
                     uint64                     *generation, // OUT
                     bool                       *was_unique) // OUT
{
   uint64 leaf_wait = 1;

   dynamic_btree_node root_node;
   root_node.addr = root_addr;


start_over:
   dynamic_btree_node_get(cc, cfg, &root_node, PAGE_TYPE_MEMTABLE);

   if (dynamic_btree_height(root_node.hdr) == 0) {
     if (dynamic_btree_node_claim(cc, cfg, &root_node)) {
       dynamic_btree_node_lock(cc, cfg, &root_node);
       leaf_add_result result = dynamic_btree_leaf_incorporate_tuple(cfg, scratch, root_node.hdr, key, message, generation);
       if (result == leaf_add_result_no_room) {
         dynamic_btree_node child;
         dynamic_btree_node_unlock(cc, cfg, &root_node);
         dynamic_btree_split_root(cc, cfg, mini, scratch, &root_node, key, message, &child);
         if (dynamic_btree_node_claim(cc, cfg, &child)) {
           dynamic_btree_node_lock(cc, cfg, &child);
           result = dynamic_btree_leaf_incorporate_tuple(cfg, scratch, child.hdr, key, message, generation);
           dynamic_btree_node_full_unlock(cc, cfg, &child);
           /* Fall through */
         } else {
           dynamic_btree_node_unget(cc, cfg, &child);
           goto start_over;
         }
       }
       if (result == leaf_add_result_no_room) {
         goto start_over;
       } else if (result == leaf_add_result_existing_key) {
         *was_unique = FALSE;
       } else {
         *was_unique = TRUE;
       }
       return STATUS_OK;
     } else {
       dynamic_btree_node_unget(cc, cfg, &root_node);
       goto start_over;
     }
   }

   /* read-lock on root */
   /* root is _not_ a leaf if we get here. */

   dynamic_btree_node parent_node = root_node;

   if (dynamic_btree_index_is_full(cfg, root_node.hdr)) {
     if (dynamic_btree_node_claim(cc, cfg, &root_node)) {
       dynamic_btree_split_root(cc, cfg, mini, scratch, &root_node, key, message, &parent_node);
     } else {
       dynamic_btree_node_unget(cc, cfg, &root_node);
       goto start_over;
     }
   }

   /* read lock on parent_node, parent_node is an index, and
      parent_node will not need to split. */

   uint32 child_idx = dynamic_btree_find_pivot(cfg, parent_node.hdr, key, less_than_or_equal);
   index_entry *parent_entry = dynamic_btree_get_index_entry(cfg, parent_node.hdr, child_idx);
   dynamic_btree_node child_node;
   child_node.addr = index_entry_child_addr(parent_entry);
   debug_assert(cache_page_valid(cc, child_node.addr));
   dynamic_btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);

   uint64 height = dynamic_btree_height(parent_node.hdr);
   while (height > 1) {
     /* loop invariant:
        - read lock on parent_node, parent_node is an index, and parent_node will not need to split.
        - read lock on child_node
        - height > 1
     */
      if (dynamic_btree_index_is_full(cfg, child_node.hdr)) {
        if (!dynamic_btree_node_claim(cc, cfg, &parent_node)) {
          dynamic_btree_node_unget(cc, cfg, &parent_node);
          dynamic_btree_node_unget(cc, cfg, &child_node);
          goto start_over;
        }
        if (!dynamic_btree_node_claim(cc, cfg, &child_node)) {
          dynamic_btree_node_unclaim(cc, cfg, &parent_node);
          dynamic_btree_node_unget(cc, cfg, &parent_node);
          dynamic_btree_node_unget(cc, cfg, &child_node);
          goto start_over;
        }
        dynamic_btree_node new_child;
        dynamic_btree_split_child(cc, cfg, mini, scratch, &parent_node, child_idx, key, message, &child_node, &new_child);
        dynamic_btree_node_unclaim(cc, cfg, &new_child);
        parent_node = new_child;
      } else {
        dynamic_btree_node_unget(cc, cfg, &parent_node);
        parent_node = child_node;
      }
      /* read lock on parent_node, which won't require a split. */

      child_idx = dynamic_btree_find_pivot(cfg, parent_node.hdr, key, less_than_or_equal);
      parent_entry = dynamic_btree_get_index_entry(cfg, parent_node.hdr, child_idx);
      debug_assert(parent_entry->num_kvs_in_tree == DYNAMIC_BTREE_UNKNOWN);
      debug_assert(parent_entry->key_bytes_in_tree == DYNAMIC_BTREE_UNKNOWN);
      debug_assert(parent_entry->message_bytes_in_tree == DYNAMIC_BTREE_UNKNOWN);
      child_node.addr = index_entry_child_addr(parent_entry);
      debug_assert(cache_page_valid(cc, child_node.addr));
      dynamic_btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
      height--;
   }

   /*
      - read lock on parent_node, parent_node is an index, and parent_node will not need to split.
      - read lock on child_node
      - height of parent == 1
   */
   while (!dynamic_btree_node_claim(cc, cfg, &child_node)) {
     dynamic_btree_node_unget(cc, cfg, &child_node);
     platform_sleep(leaf_wait);
     leaf_wait = leaf_wait > 2048 ? leaf_wait : 2 * leaf_wait;
     dynamic_btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
   }
   dynamic_btree_node_lock(cc, cfg, &child_node);

   /*
      - read lock on parent_node, parent_node is an index, and parent_node will not need to split.
      - write lock on child_node
      - height of parent == 1
   */
   leaf_add_result result = dynamic_btree_leaf_incorporate_tuple(cfg, scratch, child_node.hdr, key, message, generation);
   if (result == leaf_add_result_no_room) {
     dynamic_btree_node new_child;
     if (!dynamic_btree_node_claim(cc, cfg, &parent_node)) {
       dynamic_btree_node_unget(cc, cfg, &parent_node);
       dynamic_btree_node_unlock(cc, cfg, &child_node);
       dynamic_btree_node_unclaim(cc, cfg, &child_node);
       dynamic_btree_node_unget(cc, cfg, &child_node);
       goto start_over;
     }
     dynamic_btree_node_unlock(cc, cfg, &child_node);
     dynamic_btree_split_child(cc, cfg, mini, scratch, &parent_node, child_idx, key, message, &child_node, &new_child);
     /* only thing we have now is claim on new_child */
     dynamic_btree_node_lock(cc, cfg, &new_child);
     result = dynamic_btree_leaf_incorporate_tuple(cfg, scratch, new_child.hdr, key, message, generation);
     platform_assert(result != leaf_add_result_no_room);
     dynamic_btree_node_full_unlock(cc, cfg, &new_child);
     /* no locks of any kind */
   } else {
     dynamic_btree_node_unget(cc, cfg, &parent_node);
     dynamic_btree_node_full_unlock(cc, cfg, &child_node);
     /* no locks of any kind */
   }
   if (result == leaf_add_result_existing_key)
     *was_unique = FALSE;
   else
     *was_unique = TRUE;
   return STATUS_OK;
}


/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_lookup_node --
 *
 *      lookup_node finds the node of height stop_at_height with
 *      (node.min_key <= key < node.max_key) and returns it with a read lock
 *      held.
 *
 *      out_rank returns the rank of out_node amount nodes of height
 *      stop_at_height.
 *
 *      If any change is made here, please change dynamic_btree_lookup_async_with_ref
 *      too.
 *
 *-----------------------------------------------------------------------------
 */

platform_status
dynamic_btree_lookup_node(cache                *cc, // IN
                          dynamic_btree_config *cfg, // IN
                          uint64                root_addr, // IN
                          const slice           key, // IN
                          uint16                stop_at_height, // IN  search down to this height
                          page_type             type, // IN
                          dynamic_btree_node   *out_node, // OUT returns the node of height stop_at_height in which key was found
                          uint32               *kv_rank, // ranks must be all NULL or all non-NULL
                          uint32               *key_byte_rank,
                          uint32               *message_byte_rank)
{
   dynamic_btree_node node, child_node;
   uint32 h;
   uint16 child_idx;

   if (kv_rank) {
     *kv_rank = *key_byte_rank = *message_byte_rank = 0;
   }

   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   node.addr = root_addr;
   dynamic_btree_node_get(cc, cfg, &node, type);

   for (h = dynamic_btree_height(node.hdr); h > stop_at_height; h--) {
      child_idx = dynamic_btree_find_pivot(cfg, node.hdr, key, less_than_or_equal);
      index_entry *entry = dynamic_btree_get_index_entry(cfg, node.hdr, child_idx);
      child_node.addr = index_entry_child_addr(entry);

      if (kv_rank) {
        accumulate_node_ranks(cfg, node.hdr, 0, child_idx, kv_rank, key_byte_rank, message_byte_rank);
      }

      dynamic_btree_node_get(cc, cfg, &child_node, type);
      debug_assert(child_node.page->disk_addr == child_node.addr);
      dynamic_btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   *out_node = node;
   return STATUS_OK;
}


inline void
dynamic_btree_lookup_with_ref(cache                 *cc, // IN
                              dynamic_btree_config  *cfg, // IN
                              uint64                 root_addr, // IN
                              page_type              type, // IN
                              const slice            key, // IN
                              dynamic_btree_node    *node, // OUT
                              slice                 *data, // OUT
                              bool                  *found) // OUT
{
   dynamic_btree_lookup_node(cc, cfg, root_addr, key, 0, type, node, NULL, NULL, NULL);
   uint16 idx = dynamic_btree_find_tuple(cfg, node->hdr, key, found);
   if (*found) {
     leaf_entry *entry = dynamic_btree_get_leaf_entry(cfg, node->hdr, idx);
     *data = leaf_entry_message_slice(entry);
   }
}

// FIXME: [nsarmicanic 2020-08-11] change key and data to void*
// same for the external entire APIs
void
dynamic_btree_lookup(cache                *cc, // IN
                     dynamic_btree_config *cfg, // IN
                     uint64                root_addr, // IN
                     const slice           key, // IN
                     slice                *data_out, // OUT
                     bool                 *found) // OUT
{
   dynamic_btree_node node;
   slice data;
   dynamic_btree_lookup_with_ref(cc, cfg, root_addr, PAGE_TYPE_BRANCH, key,
                                 &node, &data, found);
   if (*found) {
     slice_copy_contents(data_out, data);
   }
   dynamic_btree_node_unget(cc, cfg, &node);
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_async_set_state --
 *
 *      Set the state of the async dynamic_btree lookup state machine.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static inline void
dynamic_btree_async_set_state(dynamic_btree_async_ctxt  *ctxt,
                              dynamic_btree_async_state  new_state)
{
   ctxt->prev_state = ctxt->state;
   ctxt->state = new_state;
}


/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_async_callback --
 *
 *      Callback that's called when the async cache get loads a page into
 *      the cache. This function moves the async dynamic_btree lookup state machine's
 *      state ahead, and calls the upper layer callback that'll re-enqueue
 *      the dynamic_btree lookup for dispatch.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static void
dynamic_btree_async_callback(cache_async_ctxt *cache_ctxt)
{
   dynamic_btree_async_ctxt *ctxt = cache_ctxt->cbdata;

   platform_assert(SUCCESS(cache_ctxt->status));
   platform_assert(cache_ctxt->page);
//   platform_log("%s:%d tid %2lu: ctxt %p is callback with page %p (%#lx)\n",
//                __FILE__, __LINE__, platform_get_tid(), ctxt,
//                cache_ctxt->page, ctxt->child_addr);
   ctxt->was_async = TRUE;
   platform_assert(ctxt->state == dynamic_btree_async_state_get_node);
   // Move state machine ahead and requeue for dispatch
   dynamic_btree_async_set_state(ctxt, dynamic_btree_async_state_get_index_complete);
   ctxt->cb(ctxt);
}


/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_lookup_async_with_ref --
 *
 *      State machine for the async dynamic_btree point lookup. This uses hand over
 *      hand locking to descend the tree and every time a child node needs to
 *      be looked up from the cache, it uses the async get api. A reference
 *      to the parent node is held in dynamic_btree_async_ctxt->node while a reference
 *      to the child page is obtained by the cache_get_async() in
 *      dynamic_btree_async_ctxt->cache_ctxt->page
 *
 * Results:
 *      See dynamic_btree_lookup_async(). if returning async_success and *found = TRUE,
 *      this returns with ref on the dynamic_btree leaf. Caller must do unget() on
 *      node_out.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

cache_async_result
dynamic_btree_lookup_async_with_ref(cache                    *cc, // IN
                                    dynamic_btree_config     *cfg, // IN
                                    uint64                    root_addr, // IN
                                    slice                     key, // IN
                                    dynamic_btree_node       *node_out, // OUT
                                    slice                    *data, // OUT
                                    bool                     *found, // OUT
                                    dynamic_btree_async_ctxt *ctxt) // IN
{
   cache_async_result  res  = 0;
   bool                done = FALSE;
   dynamic_btree_node *node = &ctxt->node;

   do {
      switch (ctxt->state) {
      case dynamic_btree_async_state_start:
      {
         ctxt->child_addr = root_addr;
         node->page = NULL;
         dynamic_btree_async_set_state(ctxt, dynamic_btree_async_state_get_node);
         // fallthrough
      }
      case dynamic_btree_async_state_get_node:
      {
         cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

         cache_ctxt_init(cc, dynamic_btree_async_callback, ctxt, cache_ctxt);
         res = cache_get_async(cc, ctxt->child_addr, PAGE_TYPE_BRANCH,
                               cache_ctxt);
         switch (res) {
         case async_locked:
         case async_no_reqs:
//            platform_log("%s:%d tid %2lu: ctxt %p is retry\n",
//                         __FILE__, __LINE__, platform_get_tid(), ctxt);
            /*
             * Ctxt remains at same state. The invocation is done, but
             * the request isn't; and caller will re-invoke me.
             */
            done = TRUE;
            break;
         case async_io_started:
//            platform_log("%s:%d tid %2lu: ctxt %p is io_started\n",
//                         __FILE__, __LINE__, platform_get_tid(), ctxt);
            // Invocation is done; request isn't. Callback will move state.
            done = TRUE;
            break;
         case async_success:
            ctxt->was_async = FALSE;
            dynamic_btree_async_set_state(ctxt, dynamic_btree_async_state_get_index_complete);
            break;
         default:
            platform_assert(0);
         }
         break;
      }
      case dynamic_btree_async_state_get_index_complete:
      {
         cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

         if (node->page) {
            // Unlock parent
            dynamic_btree_node_unget(cc, cfg, node);
         }
         dynamic_btree_node_get_from_cache_ctxt(cfg, cache_ctxt, node);
         debug_assert(node->addr == ctxt->child_addr);
         if (ctxt->was_async) {
            cache_async_done(cc, PAGE_TYPE_BRANCH, cache_ctxt);
         }
         if (dynamic_btree_height(node->hdr) == 0) {
            dynamic_btree_async_set_state(ctxt, dynamic_btree_async_state_get_leaf_complete);
            break;
         }
         uint16 child_idx = dynamic_btree_find_pivot(cfg, node->hdr, key, less_than_or_equal);
         ctxt->child_addr = dynamic_btree_get_child_addr(cfg, node->hdr, child_idx);
         dynamic_btree_async_set_state(ctxt, dynamic_btree_async_state_get_node);
         break;
      }
      case dynamic_btree_async_state_get_leaf_complete:
      {
         const uint16 idx = dynamic_btree_find_tuple(cfg, node->hdr, key, found);
         if (*found) {
            *data = dynamic_btree_get_tuple_message(cfg, node->hdr, idx);
            *node_out = *node;
         } else {
            dynamic_btree_node_unget(cc, cfg, node);
         }
         res = async_success;
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
 *
 * dynamic_btree_lookup_async --
 *
 *      Async dynamic_btree point lookup. The ctxt should've been initialized using
 *      dynamic_btree_ctxt_init(). The return value can be either of:
 *      async_locked: A page needed by lookup is locked. User should retry
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
 *
 *-----------------------------------------------------------------------------
 */

cache_async_result
dynamic_btree_lookup_async(cache                    *cc, // IN
                           dynamic_btree_config     *cfg, // IN
                           uint64                    root_addr, // IN
                           slice                     key, // IN
                           slice                    *data_out, // OUT
                           bool                     *found, // OUT
                           dynamic_btree_async_ctxt *ctxt) // IN
{
   cache_async_result res;
   dynamic_btree_node node;
   slice data;

   res = dynamic_btree_lookup_async_with_ref(cc, cfg, root_addr, key,
                                             &node, &data, found, ctxt);
   if (res == async_success && *found) {
      slice_copy_contents(data_out, data);
      dynamic_btree_node_unget(cc, cfg, &node);
   }

   return res;
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_iterator_init --
 * dynamic_btree_iterator_get_curr --
 * dynamic_btree_iterator_advance --
 * dynamic_btree_iterator_at_end
 *
 *      initializes a dynamic_btree iterator
 *
 *-----------------------------------------------------------------------------
 */

void
dynamic_btree_iterator_get_curr(iterator   *base_itor,
                                slice      *key,
                                slice      *data)
{
   debug_assert(base_itor != NULL);
   dynamic_btree_iterator *itor = (dynamic_btree_iterator *)base_itor;
   debug_assert(itor->curr.hdr != NULL);
   //if (itor->at_end || itor->idx == itor->curr.hdr->num_entries) {
   //   dynamic_btree_print_tree(itor->cc, itor->cfg, itor->root_addr);
   //}
   debug_assert(!itor->at_end);
   debug_assert(itor->idx < itor->curr.hdr->num_entries);
   debug_assert(itor->curr.page != NULL);
   debug_assert(itor->curr.page->disk_addr == itor->curr.addr);
   debug_assert((char *)itor->curr.hdr == itor->curr.page->data);
   cache_validate_page(itor->cc, itor->curr.page, itor->curr.addr);
   debug_assert(itor->curr_key != NULL);
   *key = itor->curr_key;
   debug_assert((itor->curr_data != NULL) == (itor->height == 0));
   *data = itor->curr_data;
}


static inline void
debug_dynamic_btree_check_unexpected_at_end(debug_only dynamic_btree_iterator *itor)
{
#if SPLINTER_DEBUG
   if (itor->curr.addr != itor->end.addr || itor->idx != itor->end_idx) {
      if (itor->idx == itor->curr.hdr->num_entries && itor->curr.hdr->next_addr == 0) {
         platform_log("dynamic_btree_iterator bad at_end %lu curr %lu idx %u "
                      "end %lu end_idx %u\n", itor->root_addr,
                      itor->start_addr, itor->start_idx,
                      itor->end.addr, itor->end_idx);
         dynamic_btree_print_tree(itor->cc, itor->cfg, itor->root_addr);
         platform_assert(0);
      }
   }
#endif
}


static bool
dynamic_btree_iterator_is_at_end(dynamic_btree_iterator *itor)
{
   debug_assert(itor != NULL);
   debug_assert(!itor->empty_itor);
   debug_assert(itor->idx <= itor->curr.hdr->num_entries);
   if (itor->is_live) {
      if (itor->curr.hdr->next_addr == 0 && itor->idx == itor->curr.hdr->num_entries) {
         // reached end before max key or no max key
         return TRUE;
      }
      if (!slice_is_null(itor->max_key)) {
         slice key;
         if (itor->height == 0) {
            key = dynamic_btree_get_tuple_key(itor->cfg, itor->curr.hdr, itor->idx);
         } else {
            key = dynamic_btree_get_pivot(itor->cfg, itor->curr.hdr, itor->idx);
         }
         return dynamic_btree_key_compare(itor->cfg, itor->max_key, key) <= 0;
      }
      return FALSE;
   }

   if (itor->end.addr == 0) {
      // There is no max_key
      debug_assert(itor->max_key == NULL);
      return itor->curr.hdr->next_addr == 0 && itor->idx == itor->curr.hdr->num_entries;
   }

   debug_dynamic_btree_check_unexpected_at_end(itor);

   debug_assert(itor->max_key != NULL);
   return itor->curr.addr == itor->end.addr && itor->idx == itor->end_idx;
}

static bool
dynamic_btree_iterator_is_last_tuple(dynamic_btree_iterator *itor)
{
   debug_assert(!itor->at_end);
   debug_assert(!dynamic_btree_iterator_is_at_end(itor));
   debug_assert(itor->height == 0);
   debug_assert(itor->cfg->type == data_type_range);
   debug_assert(itor->idx < itor->curr.hdr->num_entries);
   dynamic_btree_config *cfg = itor->cfg;
   if (itor->is_live) {
      if (itor->curr.hdr->next_addr == 0 &&
          itor->idx + 1 == itor->curr.hdr->num_entries)
      {
         /*
          * reached the very last element in entire tree
          * This can happen if max_key == null or reached end before max key
          */
         return TRUE;
      }
      if (!slice_is_null(itor->max_key)) {
         slice next_key;
         if (itor->idx + 1 == itor->curr.hdr->num_entries) {
            // at last element of the leaf; get first element of next leaf
            dynamic_btree_node next = { .addr = itor->curr.hdr->next_addr };
            dynamic_btree_node_get(itor->cc, cfg, &next, itor->page_type);
            next_key = dynamic_btree_get_tuple_key(cfg, next.hdr, 0);
            dynamic_btree_node_unget(itor->cc, cfg, &next);
         } else {
            // get next element of this leaf
            next_key = dynamic_btree_get_tuple_key(cfg, itor->curr.hdr, itor->idx + 1);
         }
         return dynamic_btree_key_compare(cfg, itor->max_key, next_key) <= 0;
      }
      return FALSE;
   } else {
      if (itor->end.addr == 0) {
         /*
          * There is no max key
          * It's the last tuple if we're in the last leaf and idx is the last
          * tuple in the leaf.
          */

         debug_assert(itor->max_key == NULL);

         return itor->curr.hdr->next_addr == 0 &&
                itor->idx + 1 == itor->curr.hdr->num_entries;
      }
      // there is a max key
      // FIXME: [yfogel 2020-07-14] when less_than is an available query
      //    we only have one case.  We will always point to a REAL tuple
      //    that is the last tuple included.
      //    If nothing found that way, it means entire tree (or at least
      //    entire iteration) is empty.
      //    finding -1 in a leaf is impossible on any but 0th leaf (you'd
      //    find instead the previous leaf)
      //    Depends on if the less_than query for find_node is smart
      //    it should be of course.
      //    so -1 means iteration is 100% empty and you're done
      //    (empty dynamic_btree or no matches)
      //    the "ugly" part of doing this, is that end becomes INCLUSIVE
      //    we can workaround that by increasing index by 1
      //    then we remove ALL rollovers except during advance going to next
      //    (after the is_last test)

      debug_assert(itor->max_key != NULL);

      if (itor->end_idx) {
         // It's the last tuple if on the end leaf and just before end_idx
         return itor->curr.addr == itor->end.addr &&
                itor->end_idx == itor->idx + 1;
      } else {
         // It's the last tuple if on 2nd-to-end leaf and last tuple on the leaf
         return itor->curr.hdr->next_addr == itor->end.addr &&
                itor->curr.hdr->num_entries == itor->idx + 1;
      }
   }
}

// Set curr_key and curr_data acording to height
static void
dynamic_btree_iterator_set_curr_key_and_data(dynamic_btree_iterator *itor)
{
   debug_assert(itor != NULL);
   debug_assert(itor->curr.hdr != NULL);
   debug_assert(!itor->at_end);
   debug_assert(itor->idx < itor->curr.hdr->num_entries);
   debug_assert(itor->curr.page != NULL);
   debug_assert(itor->curr.page->disk_addr == itor->curr.addr);
   debug_assert((char *)itor->curr.hdr == itor->curr.page->data);
   cache_validate_page(itor->cc, itor->curr.page, itor->curr.addr);

  if (itor->height == 0) {
     itor->curr_key = dynamic_btree_get_tuple_key(itor->cfg, itor->curr.hdr, itor->idx);
     itor->curr_data = dynamic_btree_get_tuple_message(itor->cfg, itor->curr.hdr, itor->idx);
     debug_assert(itor->curr_data != NULL);
  } else {
     itor->curr_key = dynamic_btree_get_pivot(itor->cfg, itor->curr.hdr, itor->idx);
  }
  debug_assert(itor->curr_key != NULL);
}

static void
debug_dynamic_btree_iterator_validate_next_tuple(debug_only dynamic_btree_iterator *itor)
{
#if SPLINTER_DEBUG
   dynamic_btree_config *cfg = itor->cfg;
   debug_assert(dynamic_btree_key_compare(cfg, itor->debug_prev_key, itor->curr_key) < 0);
   if (itor->max_key) {
      debug_assert(dynamic_btree_key_compare(cfg, itor->curr_key, itor->max_key) < 0);
   }
   if (itor->cfg->type == data_type_range) {
      int cmp = dynamic_btree_key_compare(cfg, itor->debug_prev_end_key, itor->curr_key);
      if (itor->debug_is_packed) {
         // Not a memtable, touching ranges are not allowed.
         // FIXME: [nsarmicanic 2020-08-11] replace <= with <
         //    once merge_iterator merges touching ranges
         debug_assert(cmp <= 0);
      } else {
         // It is a memtable, touching ranges are allowed.
         debug_assert(cmp <= 0);
      }
      if (itor->max_key) {
         debug_assert(
               dynamic_btree_key_compare(cfg, itor->curr_data, itor->max_key) <= 0);
      }
   }
#endif
}

static void
debug_dynamic_btree_iterator_save_last_tuple(debug_only dynamic_btree_iterator *itor)
{
#if SPLINTER_DEBUG
   itor->debug_is_packed = dynamic_btree_is_packed(itor->cfg, &itor->curr);

   uint64 key_size = itor->cfg->data_cfg->key_size;
   memmove(itor->debug_prev_key, itor->curr_key, key_size);
   if (itor->cfg->type == data_type_range) {
      memmove(itor->debug_prev_end_key, itor->curr_data, key_size);
   }
#endif
}

/* robj: WTF?  Why are we comparing curr_data with anything? */
static void
dynamic_btree_iterator_clamp_end(dynamic_btree_iterator *itor)
{
   if (1
       && itor->height == 0
       && dynamic_btree_iterator_is_last_tuple(itor)
       && dynamic_btree_key_compare(itor->cfg, itor->curr_data, itor->max_key) > 0)
   {
      // FIXME: [aconway 2020-09-11] Handle this better
      itor->curr_data = itor->max_key;
   }
}

platform_status
dynamic_btree_iterator_advance(iterator *base_itor)
{
   debug_assert(base_itor != NULL);
   dynamic_btree_iterator *itor = (dynamic_btree_iterator *)base_itor;
   // We should not be calling advance on an empty iterator
   debug_assert(!itor->empty_itor);
   debug_assert(!itor->at_end);
   debug_assert(!dynamic_btree_iterator_is_at_end(itor));

   cache *cc = itor->cc;
   dynamic_btree_config *cfg = itor->cfg;

   itor->curr_key = null_slice;
   itor->curr_data = null_slice;
   itor->idx++;
   debug_assert(itor->idx <= itor->curr.hdr->num_entries);

   /*
    * FIXME: [aconway 2021-04-10] This check breaks ranges queries for live
    * memtables. It causes them to stop prematurely as a result of
    * dynamic_btree-splitting. That change requires iterators not to advance to the
    * next dynamic_btree node when they hit num_entries, because that node may have
    * been deallocated.
    *
    *itor->at_end = dynamic_btree_iterator_is_at_end(itor);
    *if (itor->at_end) {
    *   return STATUS_OK;
    *}
    */

   if (itor->idx == itor->curr.hdr->num_entries) {
      // exhausted this node; need to move to next node
      if (itor->curr.hdr->next_addr != 0 &&
            (itor->end.addr == 0 || itor->curr.addr != itor->end.addr)) {

         uint64 last_addr = itor->curr.addr;
         //if (itor->curr.addr == trace_addr) {
         //   platform_log("iterating through %lu\n", trace_addr);
         //}
         dynamic_btree_node next = { 0 };
         if (itor->curr.hdr->next_addr != 0) {
            next.addr = itor->curr.hdr->next_addr;
            dynamic_btree_node_get(cc, cfg, &next, itor->page_type);
            if (itor->page_type == PAGE_TYPE_MEMTABLE) {
               debug_assert(!dynamic_btree_is_packed(cfg, &next));
            } else {
               debug_assert(dynamic_btree_is_packed(cfg, &next));
            }
         }
         dynamic_btree_node_unget(cc, cfg, &itor->curr);
         itor->curr = next;
         itor->idx = 0;

         // To prefetch:
         // 1. dynamic_btree must be static
         // 2. curr node must start extent
         // 3. which can't be the last extent
         // 4. and we can't be at the end
         if (1 && itor->do_prefetch
               && itor->curr.addr != 0
               && !dynamic_btree_addrs_share_extent(cfg, itor->curr.addr, last_addr)
               && itor->curr.hdr->next_extent_addr != 0
               && (itor->end.addr == 0
                  || !dynamic_btree_addrs_share_extent(cfg, itor->curr.addr, itor->end.addr))) {
            // IO prefetch the next extent
            cache_prefetch(cc, itor->curr.hdr->next_extent_addr, TRUE);
         }
      } else {
         // We already know we are at the end
         itor->at_end = TRUE;
         debug_assert(dynamic_btree_iterator_is_at_end(itor));
         return STATUS_OK;
      }
   }

   itor->at_end = dynamic_btree_iterator_is_at_end(itor);
   if (itor->at_end) {
      return STATUS_OK;
   }

   dynamic_btree_iterator_set_curr_key_and_data(itor);

   dynamic_btree_iterator_clamp_end(itor);

   debug_dynamic_btree_iterator_validate_next_tuple(itor);
   debug_dynamic_btree_iterator_save_last_tuple(itor);

   return STATUS_OK;
}


platform_status
dynamic_btree_iterator_at_end(iterator *itor,
                      bool *at_end)
{
   debug_assert(itor != NULL);
   dynamic_btree_iterator *dynamic_btree_itor = (dynamic_btree_iterator *)itor;
   *at_end = dynamic_btree_itor->at_end;

   return STATUS_OK;
}

void
dynamic_btree_iterator_print(iterator *itor)
{
   debug_assert(itor != NULL);
   dynamic_btree_iterator *dynamic_btree_itor = (dynamic_btree_iterator *)itor;

   platform_log("########################################\n");
   platform_log("## dynamic_btree_itor: %p\n", itor);
   platform_log("## root: %lu\n", dynamic_btree_itor->root_addr);
   platform_log("## curr %lu end %lu\n",
                dynamic_btree_itor->curr.addr, dynamic_btree_itor->end.addr);
   platform_log("## idx %u end_idx %u\n",
                dynamic_btree_itor->idx, dynamic_btree_itor->end_idx);
   dynamic_btree_print_node(dynamic_btree_itor->cc, dynamic_btree_itor->cfg, &dynamic_btree_itor->curr,
                    PLATFORM_DEFAULT_LOG_HANDLE);
}

const static iterator_ops dynamic_btree_iterator_ops = {
   .get_curr = dynamic_btree_iterator_get_curr,
   .at_end   = dynamic_btree_iterator_at_end,
   .advance  = dynamic_btree_iterator_advance,
   .print    = dynamic_btree_iterator_print,
};


/*
 *-----------------------------------------------------------------------------
 *
 * Caller must guarantee:
 *    min_key needs to be valid until the first call to advance() returns
 *    max_key (if not null) needs to be valid until at_end() returns true
 * If we are iterating over ranges
 *    (data_type == data_type_range && height == 0)
 *
 * dynamic_btree_iterator on range_delete (leaves only) clamps the ranges outputted to
 *  the input min/max.  It also includes all ranges that overlap the [min,max)
 *  range, and not just keys that overlap it. (Potentially 1 extra range
 *  included in the iteration)
 *
 *-----------------------------------------------------------------------------
 */
void
dynamic_btree_iterator_init(cache          *cc,
                    dynamic_btree_config   *cfg,
                    dynamic_btree_iterator *itor,
                    uint64          root_addr,
                    page_type       page_type,
                    const slice     min_key,
                    const slice     max_key,
                    bool            do_prefetch,
                    bool            is_live,
                    uint32          height)
{
   debug_assert(cfg->type == data_type);

   ZERO_CONTENTS(itor);
   itor->cc          = cc;
   itor->cfg         = cfg;
   itor->root_addr   = root_addr;
   itor->do_prefetch = do_prefetch;
   itor->is_live     = is_live;
   itor->height      = height;
   itor->min_key     = min_key;
   itor->max_key     = max_key;
   itor->page_type   = page_type;
   itor->super.ops   = &dynamic_btree_iterator_ops;
   /*
    * start_is_clamped is true if we don't need to make any changes
    * (e.g. it's already been restricted to the min_key/max_key range)
    */
   bool start_is_clamped = TRUE;

   // FIXME: [nsarmicanic 2020-07-15]
   // May want to have a goto emtpy itor

   // Check if this is an empty iterator
   if ((itor->root_addr == 0) ||
       (!slice_is_null(max_key) && dynamic_btree_key_compare(cfg, min_key, max_key) >= 0)) {
      itor->at_end = TRUE;
      itor->empty_itor = TRUE;
      return;
   }

   debug_assert(page_type == PAGE_TYPE_MEMTABLE ||
                page_type == PAGE_TYPE_BRANCH);

   // if the iterator height is greater than the actual dynamic_btree height,
   // set values so that at_end will be TRUE
   if (height > 0) {
      dynamic_btree_node temp = {
         .addr = root_addr,
      };
      dynamic_btree_node_get(cc, cfg, &temp, page_type);
      if (page_type == PAGE_TYPE_MEMTABLE) {
         debug_assert(!dynamic_btree_is_packed(cfg, &temp));
      } else {
         debug_assert(dynamic_btree_is_packed(cfg, &temp));
      }
      uint32 root_height = dynamic_btree_height(temp.hdr);
      dynamic_btree_node_unget(cc, cfg, &temp);
      if (root_height < height) {
         // There is nothing higher than root
         itor->at_end = TRUE;
         itor->empty_itor = TRUE;
         return;
      }
   }

   // get end point first:
   // we don't hold a lock on the the end node, so this way if curr == end, we
   // don't try to hold two read locks on that node, which can cause deadlocks
   if (!is_live && !slice_is_null(max_key)) {
     dynamic_btree_lookup_node(cc, cfg, root_addr, max_key, height, page_type, &itor->end, NULL, NULL, NULL);
      if (page_type == PAGE_TYPE_MEMTABLE) {
         debug_assert(!dynamic_btree_is_packed(cfg, &itor->end));
      } else {
         debug_assert(dynamic_btree_is_packed(cfg, &itor->end));
      }
      bool found;
      if (height == 0) {
         itor->end_idx = dynamic_btree_find_tuple(cfg, itor->end.hdr, max_key, &found);
      } else {
         itor->end_idx = dynamic_btree_find_pivot(cfg, itor->end.hdr, max_key, greater_than_or_equal);
      }

      debug_assert(itor->end_idx == itor->end.hdr->num_entries || ((height == 0) &&
            dynamic_btree_key_compare(cfg, max_key, dynamic_btree_get_tuple(cfg, &itor->end, itor->end_idx)) <= 0)
            || ((height != 0) && dynamic_btree_key_compare(cfg, max_key,
                  dynamic_btree_get_pivot(cfg, &itor->end, itor->end_idx)) <= 0));
      debug_assert(itor->end_idx == itor->end.hdr->num_entries || itor->end_idx == 0 ||
            ((height == 0) && dynamic_btree_key_compare(cfg, max_key,
               dynamic_btree_get_tuple(cfg, &itor->end, itor->end_idx - 1)) > 0) || ((height != 0)
            && dynamic_btree_key_compare(cfg, max_key,
               dynamic_btree_get_pivot(cfg, &itor->end, itor->end_idx - 1)) > 0));

      dynamic_btree_node_unget(cc, cfg, &itor->end);
   }

   dynamic_btree_lookup_node(cc, cfg, root_addr, min_key, height, page_type, &itor->curr, NULL, NULL, NULL);
   if (page_type == PAGE_TYPE_MEMTABLE) {
      debug_assert(!dynamic_btree_is_packed(cfg, &itor->curr));
   } else {
      debug_assert(dynamic_btree_is_packed(cfg, &itor->curr));
   }
   // empty
   if (itor->curr.hdr->num_entries == 0 && itor->curr.hdr->next_addr == 0) {
      itor->at_end = TRUE;
      return;
   }

   if (height == 0) {
      //FIXME: [yfogel 2020-07-08] no need for extra compare when less_than_or_equal is supproted
      bool found;
      itor->idx = dynamic_btree_find_tuple(cfg, itor->curr.hdr, min_key, &found);
    } else {
      itor->idx = dynamic_btree_find_pivot(cfg, itor->curr.hdr, min_key, greater_than_or_equal);
   }

   // we need to check this at-end condition because its possible that the next
   // extent has been sliced
   if (1 && itor->end.addr != 0
         && itor->curr.addr == itor->end.addr
         && itor->idx == itor->end_idx) {
      itor->at_end = TRUE;
      itor->empty_itor = TRUE;
      return;
   }

   if (itor->idx == itor->curr.hdr->num_entries && itor->curr.hdr->next_addr != 0) {
      // min_key is larger than all tuples in the curr node, so advance
      dynamic_btree_node next = { .addr = itor->curr.hdr->next_addr };
      dynamic_btree_node_get(cc, cfg, &next, page_type);
      itor->idx = 0;
      dynamic_btree_node_unget(cc, cfg, &itor->curr);
      itor->curr = next;
   }

   itor->start_addr = itor->curr.addr;
   itor->start_idx = itor->idx;

   debug_assert((itor->is_live) || (itor->idx != itor->curr.hdr->num_entries
         || (itor->curr.addr == itor->end.addr && itor->idx == itor->end_idx)));

   if (itor->curr.hdr->next_addr != 0) {
      if (itor->do_prefetch) {
         // IO prefetch
         uint64 next_extent_addr = itor->curr.hdr->next_extent_addr;
         if (next_extent_addr != 0               // prefetch if not last extent
               && (itor->end.addr == 0           // and either no upper bound
               || !dynamic_btree_addrs_share_extent(cfg, itor->curr.addr, itor->end.addr))) {
                                                 // or ub not yet reached
            // IO prefetch the next extent
            debug_assert(cache_page_valid(cc, next_extent_addr));
            cache_prefetch(cc, next_extent_addr, TRUE);
         }
      }
   }

   itor->at_end = dynamic_btree_iterator_is_at_end(itor);
   if (itor->at_end) {
      // Special case of an empty iterator
      // FIXME: [nsarmicanic 2020-07-15] Can do unget here and set
      // addr and empty_itor appropriately
      itor->empty_itor = TRUE;
      return;
   }

   // Set curr_key and curr_data
   dynamic_btree_iterator_set_curr_key_and_data(itor);

   // If we need to clamp start set curr_key to min_key
   if (!start_is_clamped) {
      itor->curr_key = min_key;
   }

   dynamic_btree_iterator_clamp_end(itor);

   debug_assert(dynamic_btree_key_compare(cfg, min_key, itor->curr_key) <= 0);
   debug_dynamic_btree_iterator_save_last_tuple(itor);
}

void
dynamic_btree_iterator_deinit(dynamic_btree_iterator *itor)
{
   debug_assert(itor != NULL);
   if (itor->curr.addr != 0) {
      dynamic_btree_node_unget(itor->cc, itor->cfg, &itor->curr);
   } else {
      platform_assert(itor->empty_itor);
   }
}

typedef struct {
   // from pack_req
   cache                *cc;
   dynamic_btree_config *cfg;

   iterator *itor;

   hash_fn       hash;
   uint32       *fingerprint_arr;
   unsigned int  seed;

   uint64 *root_addr;           // pointers to pack_req's root_addr
   uint64 *num_tuples;          // pointers to pack_req's num_tuples

   uint64 num_kvs;
   uint64 key_bytes;
   uint64 message_bytes;

   // internal data
   uint64 next_extent;
   uint16 height;

   dynamic_btree_node edge[DYNAMIC_BTREE_MAX_HEIGHT];
   uint64             idx[DYNAMIC_BTREE_MAX_HEIGHT];

   mini_allocator mini;
} dynamic_btree_pack_internal;

// generation number isn't used in packed dynamic_btrees
static inline void
dynamic_btree_pack_node_init_hdr(const dynamic_btree_config *cfg,
                                 dynamic_btree_hdr  *hdr,
                                 uint64              next_extent,
                                 uint8               height)
{
   dynamic_btree_init_hdr(cfg, hdr);
   hdr->next_extent_addr = next_extent;
   hdr->height = height;
}

static inline void
dynamic_btree_pack_setup(dynamic_btree_pack_req *req, dynamic_btree_pack_internal *tree)
{
   tree->cc = req->cc;
   tree->cfg = req->cfg;
   tree->itor = req->itor;
   tree->hash = req->hash;
   tree->fingerprint_arr = req->fingerprint_arr;
   tree->seed = req->seed;
   tree->root_addr = &req->root_addr;
   tree->num_tuples = &req->num_tuples;
   *(tree->num_tuples) = 0;

   cache *cc = tree->cc;
   dynamic_btree_config *cfg = tree->cfg;

   // FIXME: [yfogel 2020-07-02] Where is the transition between branch and tree (Alex)
   // 1. Mini allocator? Pre-fetching?
   // 2. Refcount? Shared

   // we create a root here, but we won't build it with the rest
   // of the tree, we'll copy into it at the end
   *(tree->root_addr) = dynamic_btree_init(cc, cfg, &tree->mini, TRUE);
   tree->height = 0;

   // set up the first leaf
   dynamic_btree_alloc(cc, &tree->mini, 0, data_key_negative_infinity, &tree->next_extent, PAGE_TYPE_BRANCH, &tree->edge[0]);
   debug_assert(cache_page_valid(cc, tree->next_extent));
   dynamic_btree_pack_node_init_hdr(cfg, tree->edge[0].hdr, tree->next_extent, 0);
}

static inline void
dynamic_btree_pack_loop(dynamic_btree_pack_internal *tree, // IN/OUT
                        slice                        key, // IN
                        slice                        message, // IN
                        bool                        *at_end) // IN/OUT
{
   uint16 i = 0;
   if (!dynamic_btree_set_leaf_entry(tree->cfg, tree->edge[0].hdr, tree->idx[0], key, message)) {
      // the current leaf is full, allocate a new one and add to index
      // FIXME: [yfogel 2020-07-02] we can use 2 dynamic handle or ... (Ask Alex)
      dynamic_btree_node old_edge = tree->edge[0];

      dynamic_btree_node new_edge;
      dynamic_btree_alloc(tree->cc, &tree->mini, 0, key, &tree->next_extent, PAGE_TYPE_BRANCH, &new_edge);
      old_edge.hdr->next_addr = new_edge.addr;
      dynamic_btree_node_full_unlock(tree->cc, tree->cfg, &old_edge);

      // initialize the new leaf edge
      tree->edge[0] = new_edge;
      tree->idx[0] = 0;
      debug_assert(cache_page_valid(tree->cc, tree->next_extent));
      dynamic_btree_pack_node_init_hdr(tree->cfg, new_edge.hdr, tree->next_extent, 0);
      bool result = dynamic_btree_set_leaf_entry(tree->cfg, new_edge.hdr, 0, key, message);
      platform_assert(result);

      // this loop finds the first level with a free slot
      // along the way it allocates new index nodes as necessary
      i = 1;
      while (i < tree->height &&
             dynamic_btree_set_index_entry(tree->cfg, tree->edge[i].hdr, tree->idx[i], key, new_edge.addr,
                                           0, 0, 0)) {
         old_edge = tree->edge[i];

         dynamic_btree_alloc(tree->cc, &tree->mini, i, key, &tree->next_extent, PAGE_TYPE_BRANCH, &new_edge);
         old_edge.hdr->next_addr = new_edge.addr;
         dynamic_btree_node_full_unlock(tree->cc, tree->cfg, &old_edge);

         // initialize the new index edge
         dynamic_btree_pack_node_init_hdr(tree->cfg, new_edge.hdr, tree->next_extent, i);
         dynamic_btree_set_index_entry(tree->cfg, tree->edge[i].hdr, 0, key, tree->edge[i - 1].addr,
                                       0, 0, 0);
         tree->edge[i] = new_edge;
         tree->idx[i] = 1;
         //platform_log("adding %lu to %lu at pos 0\n",
         //                     edge[i-1].addr, edge[i].addr);
         i++;
      }

      if (i == tree->height) {
         // need to add a new root
         slice first_key;
         if (i == 1) {
           first_key = dynamic_btree_get_tuple_key(tree->cfg, old_edge.hdr, 0);
         } else {
            first_key = dynamic_btree_get_pivot(tree->cfg, old_edge.hdr, 0);
         }
         dynamic_btree_alloc(tree->cc, &tree->mini, i, first_key, &tree->next_extent, PAGE_TYPE_BRANCH, &tree->edge[i]);
         dynamic_btree_pack_node_init_hdr(tree->cfg, tree->edge[i].hdr, tree->next_extent, i);
         tree->height++;

         // add old root and it's younger sibling
         dynamic_btree_set_index_entry(tree->cfg, tree->edge[i].hdr, 0, first_key, old_edge.addr,
                                       tree->num_kvs, tree->key_bytes, tree->message_bytes);
         dynamic_btree_set_index_entry(tree->cfg, tree->edge[i].hdr, 1, key, tree->edge[i - 1].addr,
                                       0, 0, 0);
         tree->idx[tree->height] = 2;
      } else {
         // add to the index with a free slot
         /* dynamic_btree_add_pivot_at_pos(tree->cfg, &tree->edge[i], &tree->edge[i - 1], */
         /*                        tree->idx[i], FALSE); */
         //platform_log("adding %lu to %lu at pos %u\n",
         //                     edge[i - 1].addr, edge[i].addr, idx[i]);
         tree->idx[i]++;
      }
   }

#if defined(DYNAMIC_BTREE_TRACE)
   if (dynamic_btree_key_compare(tree->cfg, key, trace_key) == 0)
     platform_log("adding tuple to %lu, root addr %lu\n",
                  tree->edge[0].addr, *tree->root_addr);
#endif
   //if (idx[0] != 0) {
   //   int comp = dynamic_btree_key_compare(cfg, dynamic_btree_get_tuple(cfg, &edge[0], idx[0] - 1), key);
   //   if (comp >= 0) {
   //      char key_str[128], last_key_str[128];
   //      dynamic_btree_key_to_string(cfg, key, key_str);
   //      dynamic_btree_key_to_string(cfg, dynamic_btree_get_tuple(cfg, &edge[0], idx[0] - 1), last_key_str);
   //      platform_log("dynamic_btree_pack OOO keys: \n%s \n%s\n%d\n",
   //                           last_key_str, key_str, comp);
   //      iterator_print(req->itor);
   //      platform_assert(0);
   //   }
   //}

   for (i = 1; i < tree->height; i++) {
     index_entry *entry = dynamic_btree_get_index_entry(tree->cfg, tree->edge[i].hdr, tree->idx[i] - 1);
     entry->num_kvs_in_tree++;
     entry->key_bytes_in_tree += slice_length(key);
     entry->message_bytes_in_tree += slice_length(message);
   }
   tree->num_kvs++;
   tree->key_bytes += slice_length(key);
   tree->message_bytes += slice_length(message);

   tree->idx[0]++;
   if (tree->hash) {
     tree->fingerprint_arr[*(tree->num_tuples)] =
       tree->hash(slice_data(key), slice_length(key), tree->seed);
   }
   (*(tree->num_tuples))++;

   iterator_advance(tree->itor);
   iterator_at_end(tree->itor, at_end);
}


static inline void
dynamic_btree_pack_post_loop(dynamic_btree_pack_internal *tree)
{
   cache *cc = tree->cc;
   dynamic_btree_config *cfg = tree->cfg;
   // we want to use the allocation node, so we copy the root created in the
   // loop into the dynamic_btree_init root
   dynamic_btree_node root;
   root.addr = *(tree->root_addr);
   dynamic_btree_node_get(cc, cfg, &root, PAGE_TYPE_BRANCH);

   __attribute__((unused))
      bool success = dynamic_btree_node_claim(cc, cfg, &root);
   debug_assert(success);
   dynamic_btree_node_lock(cc, cfg, &root);
   memmove(root.hdr, tree->edge[tree->height].hdr, dynamic_btree_page_size(cfg));
   // fix the root next extent
   root.hdr->next_extent_addr = 0;
   dynamic_btree_node_full_unlock(cc, cfg, &root);

   // release all the edge nodes;
   for (uint16 i = 0; i <= tree->height; i++) {
      // go back and fix the dangling next extents
      for (uint64 addr = dynamic_btree_get_extent_base_addr(cfg, &tree->edge[i]);
            addr != tree->edge[i].addr;
            addr += dynamic_btree_page_size(cfg)) {
         dynamic_btree_node node = { .addr = addr };
         dynamic_btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);
         success = dynamic_btree_node_claim(cc, cfg, &node);
         debug_assert(success);
         dynamic_btree_node_lock(cc, cfg, &node);
         node.hdr->next_extent_addr = 0;
         dynamic_btree_node_full_unlock(cc, cfg, &node);
      }
      tree->edge[i].hdr->next_extent_addr = 0;
      dynamic_btree_node_full_unlock(cc, cfg, &tree->edge[i]);
   }

   mini_allocator_release(&tree->mini, data_key_positive_infinity);

   // if output tree is empty, zap the tree
   if (*(tree->num_tuples) == 0) {
      dynamic_btree_zap(tree->cc, tree->cfg, *(tree->root_addr), PAGE_TYPE_BRANCH);
      *(tree->root_addr) = 0;
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_pack --
 *
 *      Packs a dynamic_btree from an iterator source. Zaps the output tree if it's
 *      empty.
 *
 *-----------------------------------------------------------------------------
 */

platform_status
dynamic_btree_pack(dynamic_btree_pack_req *req)
{
   dynamic_btree_pack_internal tree;
   ZERO_STRUCT(tree);

   dynamic_btree_pack_setup(req, &tree);

   slice key, data;
   bool at_end;

   iterator_at_end(tree.itor, &at_end);
   while (!at_end) {
      iterator_get_curr(tree.itor, &key, &data);
      dynamic_btree_pack_loop(&tree, key, data, &at_end);
      // FIXME: [tjiaheng 2020-07-29] find out how we can use req->max_tuples
      // here
      // if (req->max_tuples != 0 && *(tree.num_tuples) == req->max_tuples) {
      //    at_end = TRUE;
      // }
   }

   dynamic_btree_pack_post_loop(&tree);
   platform_assert(IMPLIES(req->num_tuples == 0, req->root_addr == 0));
   return STATUS_OK;
}

/*
 * Returns the number of kv pairs (k,v ) w/ k < key.  Also returns
 * the total size of all such keys and messages.
 */
static inline void
dynamic_btree_get_rank(cache                *cc,
                       dynamic_btree_config *cfg,
                       uint64                root_addr,
                       const slice           key,
                       uint32               *kv_rank,
                       uint32               *key_bytes_rank,
                       uint32               *message_bytes_rank)
{
   dynamic_btree_node leaf;

   dynamic_btree_lookup_node(cc, cfg, root_addr, key, 0, PAGE_TYPE_BRANCH, &leaf,
                             kv_rank, key_bytes_rank, message_bytes_rank);
   bool found;
   uint64 tuple_rank_in_leaf =
     dynamic_btree_find_tuple(cfg, leaf.hdr, key, &found);
   accumulate_node_ranks(cfg, leaf.hdr, 0, tuple_rank_in_leaf, kv_rank, key_bytes_rank, message_bytes_rank);
   dynamic_btree_node_unget(cc, cfg, &leaf);
}

/*
 * count_in_range returns the exact number of tuples in the given dynamic_btree between
 * min_key (inc) and max_key (excl).
 */

void
dynamic_btree_count_in_range(cache                *cc,
                             dynamic_btree_config *cfg,
                             uint64                root_addr,
                             const slice           min_key,
                             const slice           max_key,
                             uint32               *kv_rank,
                             uint32               *key_bytes_rank,
                             uint32               *message_bytes_rank)
{
   uint32 min_kv_rank;
   uint32 min_key_bytes_rank;
   uint32 min_message_bytes_rank;
   dynamic_btree_node root;
   root.addr = root_addr;

   dynamic_btree_get_rank(cc, cfg, root_addr, min_key, &min_kv_rank, &min_key_bytes_rank, &min_message_bytes_rank);
   dynamic_btree_get_rank(cc, cfg, root_addr, max_key, kv_rank, key_bytes_rank, message_bytes_rank);
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
 * dynamic_btree_count_in_range_by_iterator perform dynamic_btree_count_in_range using an
 * iterator instead of by calculating ranks. Used for debugging purposes.
 */

void
dynamic_btree_count_in_range_by_iterator(cache                *cc,
                                         dynamic_btree_config *cfg,
                                         uint64                root_addr,
                                         const slice           min_key,
                                         const slice           max_key,
                                         uint32               *kv_rank,
                                         uint32               *key_bytes_rank,
                                         uint32               *message_bytes_rank)
{
   dynamic_btree_iterator dynamic_btree_itor;
   iterator *itor = &dynamic_btree_itor.super;
   dynamic_btree_iterator_init(cc, cfg, &dynamic_btree_itor, root_addr, PAGE_TYPE_BRANCH,
         min_key, max_key, TRUE, FALSE, 0);

   *kv_rank = 0;
   *key_bytes_rank = 0;
   *message_bytes_rank = 0;

   bool at_end;
   iterator_at_end(itor, &at_end);
   while (!at_end) {
      slice key, message;
      iterator_get_curr(itor, &key, &message);
      *kv_rank = *kv_rank + 1;
      *key_bytes_rank = *key_bytes_rank + slice_length(key);
      *message_bytes_rank = *message_bytes_rank + slice_length(message);
      iterator_advance(itor);
      iterator_at_end(itor, &at_end);
   }
   dynamic_btree_iterator_deinit(&dynamic_btree_itor);
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_print_node --
 * dynamic_btree_print_tree --
 *
 *      Prints out the contents of the node/tree.
 *
 *-----------------------------------------------------------------------------
 */

void
dynamic_btree_print_locked_node(dynamic_btree_config   *cfg,
                                dynamic_btree_node     *node,
                                platform_stream_handle  stream)
{
   char key_string[128];
   char data_string[256];
   platform_log_stream("*******************\n");
   if (dynamic_btree_height(node->hdr) > 0) {
      platform_log_stream("**  INDEX NODE \n");
      platform_log_stream("**  addr: %lu \n", node->addr);
      platform_log_stream("**  ptr: %p\n", node->hdr);
      platform_log_stream("**  next_addr: %lu \n", node->hdr->next_addr);
      platform_log_stream("**  next_extent_addr: %lu \n", node->hdr->next_extent_addr);
      platform_log_stream("**  generation: %lu \n", node->hdr->generation);
      platform_log_stream("**  height: %u \n", dynamic_btree_height(node->hdr));
      platform_log_stream("**  next_entry: %u \n", node->hdr->next_entry);
      platform_log_stream("**  num_entries: %u \n", dynamic_btree_num_entries(node->hdr));
      platform_log_stream("-------------------\n");
      table_entry *table = dynamic_btree_get_table(node->hdr);
      for (uint64 i = 0; i < node->hdr->num_entries; i++)
        platform_log_stream("%lu:%u ", i, table[i]);
      platform_log_stream("\n");
      platform_log_stream("-------------------\n");
      for (uint64 i = 0; i < dynamic_btree_num_entries(node->hdr); i++) {
         index_entry *entry = dynamic_btree_get_index_entry(cfg, node->hdr, i);
         dynamic_btree_key_to_string(cfg, index_entry_key_slice(entry), key_string);
         platform_log_stream("%2lu:%s -- %lu (%u, %u, %u)\n",
                             i, key_string, entry->child_addr, entry->num_kvs_in_tree, entry->key_bytes_in_tree, entry->message_bytes_in_tree);
      }
      platform_log_stream("\n");
   } else {
      platform_log_stream("**  LEAF NODE \n");
      platform_log_stream("**  addr: %lu \n", node->addr);
      platform_log_stream("**  ptr: %p\n", node->hdr);
      platform_log_stream("**  next_addr: %lu \n", node->hdr->next_addr);
      platform_log_stream("**  next_extent_addr: %lu \n", node->hdr->next_extent_addr);
      platform_log_stream("**  generation: %lu \n", node->hdr->generation);
      platform_log_stream("**  height: %u \n", dynamic_btree_height(node->hdr));
      platform_log_stream("**  next_entry: %u \n", node->hdr->next_entry);
      platform_log_stream("**  num_entries: %u \n", dynamic_btree_num_entries(node->hdr));
      platform_log_stream("-------------------\n");
      table_entry *table = dynamic_btree_get_table(node->hdr);
      for (uint64 i = 0; i < dynamic_btree_num_entries(node->hdr); i++)
        platform_log_stream("%lu:%u ", i, table[i]);
      platform_log_stream("\n");
      platform_log_stream("-------------------\n");
      for (uint64 i = 0; i < dynamic_btree_num_entries(node->hdr); i++) {
         leaf_entry *entry = dynamic_btree_get_leaf_entry(cfg, node->hdr, i);
         dynamic_btree_key_to_string(cfg, leaf_entry_key_slice(entry), key_string);
         dynamic_btree_key_to_string(cfg, leaf_entry_message_slice(entry), key_string);
         platform_log_stream("%2lu:%s -- %s\n", i, key_string, data_string);
      }
      platform_log_stream("-------------------\n");
      platform_log_stream("\n");
   }
}

void
dynamic_btree_print_node(cache          *cc,
                 dynamic_btree_config   *cfg,
                 dynamic_btree_node     *node,
                 platform_stream_handle  stream)
{
   if (!cache_page_valid(cc, node->addr)) {
      platform_log_stream("*******************\n");
      platform_log_stream("** INVALID NODE \n");
      platform_log_stream("** addr: %lu \n", node->addr);
      platform_log_stream("-------------------\n");
      return;
   }
   dynamic_btree_node_get(cc, cfg, node, PAGE_TYPE_BRANCH);
   dynamic_btree_print_locked_node(cfg, node, stream);
   dynamic_btree_node_unget(cc, cfg, node);
}

void
dynamic_btree_print_subtree(cache                  *cc,
                            dynamic_btree_config   *cfg,
                            uint64                  addr,
                            platform_stream_handle  stream)
{
   dynamic_btree_node node;
   node.addr = addr;
   dynamic_btree_print_node(cc, cfg, &node, stream);
   if (!cache_page_valid(cc, node.addr)) {
      return;
   }
   dynamic_btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);
   uint32 idx;

   if (node.hdr->height > 0) {
      for (idx = 0; idx < node.hdr->num_entries; idx++) {
         dynamic_btree_print_subtree(cc, cfg, dynamic_btree_get_child_addr(cfg, node.hdr, idx), stream);
      }
   }
   dynamic_btree_node_unget(cc, cfg, &node);
}

void
dynamic_btree_print_tree(cache        *cc,
                 dynamic_btree_config *cfg,
                 uint64                root_addr)
{
   platform_open_log_stream();
   dynamic_btree_print_subtree(cc, cfg, root_addr, stream);
   platform_close_log_stream(PLATFORM_DEFAULT_LOG_HANDLE);
}

void
dynamic_btree_print_tree_stats(cache        *cc,
                       dynamic_btree_config *cfg,
                       uint64                addr)
{
   dynamic_btree_node node;
   node.addr = addr;
   dynamic_btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);

   platform_log("Tree stats: height %u\n", node.hdr->height);
   cache_print_stats(cc);

   dynamic_btree_node_unget(cc, cfg, &node);
}

/*
 * returns the space used in bytes by the range [start_key, end_key) in the
 * dynamic_btree
 */

uint64
dynamic_btree_space_use_in_range(cache        *cc,
                         dynamic_btree_config *cfg,
                         uint64                root_addr,
                         page_type             type,
                         const slice           start_key,
                         const slice           end_key)
{
   uint64 meta_head = dynamic_btree_root_to_meta_addr(cfg, root_addr, 0);
   uint64 extents_used = mini_allocator_count_extents_in_range(cc,
         cfg->data_cfg, type, meta_head, start_key, end_key);
   return extents_used * cfg->extent_size;
}

bool
dynamic_btree_verify_node(cache                *cc,
                          dynamic_btree_config *cfg,
                          uint64                addr,
                          page_type             type,
                          bool                  is_left_edge)
{
   dynamic_btree_node node;
   node.addr = addr;
   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   dynamic_btree_node_get(cc, cfg, &node, type);
   uint32 idx;
   bool result = FALSE;

   platform_open_log_stream();
   for (idx = 0; idx < node.hdr->num_entries; idx++) {
      if (node.hdr->height == 0) {
         // leaf node
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (dynamic_btree_key_compare(cfg,
                                          dynamic_btree_get_tuple_key(cfg, node.hdr, idx),
                                          dynamic_btree_get_tuple_key(cfg, node.hdr, idx + 1)) >= 0) {
               platform_log_stream("out of order tuples\n");
               platform_log_stream("addr: %lu idx %2u\n", node.addr, idx);
               dynamic_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
      } else {
         // index node
         dynamic_btree_node child;
         child.addr = dynamic_btree_get_child_addr(cfg, node.hdr, idx);
         dynamic_btree_node_get(cc, cfg, &child, type);
         if (child.hdr->height != node.hdr->height - 1) {
            platform_log_stream("height mismatch\n");
            platform_log_stream("addr: %lu idx: %u\n", node.addr, idx);
            dynamic_btree_node_unget(cc, cfg, &child);
            dynamic_btree_node_unget(cc, cfg, &node);
            goto out;
         }
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (dynamic_btree_key_compare(cfg,
                                          dynamic_btree_get_pivot(cfg, node.hdr, idx),
                                          dynamic_btree_get_pivot(cfg, node.hdr, idx + 1)) >= 0) {
               dynamic_btree_node_unget(cc, cfg, &child);
               dynamic_btree_node_unget(cc, cfg, &node);
               dynamic_btree_print_tree(cc, cfg, addr);
               platform_log_stream("out of order pivots\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (idx == 0 && is_left_edge) {
               if (dynamic_btree_key_compare(cfg,
                                             dynamic_btree_get_pivot(cfg, node.hdr, idx),
                                             data_key_negative_infinity) != 0) {
                  platform_log_stream("left edge pivot not min key\n");
                  platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
                  platform_log_stream("child addr: %lu\n", child.addr);
                  dynamic_btree_node_unget(cc, cfg, &child);
                  dynamic_btree_node_unget(cc, cfg, &node);
                  goto out;
               }
            } else if (dynamic_btree_key_compare(cfg,
                                                 dynamic_btree_get_pivot(cfg, node.hdr, idx),
                                                 dynamic_btree_get_tuple_key(cfg, child.hdr, 0)) != 0) {
               platform_log_stream("pivot key doesn't match in child and parent\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu\n", child.addr);
               dynamic_btree_node_unget(cc, cfg, &child);
               dynamic_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         } else {
            // child index
            if (dynamic_btree_key_compare(cfg,
                                          dynamic_btree_get_pivot(cfg, node.hdr, idx),
                                          dynamic_btree_get_pivot(cfg, child.hdr, 0)) != 0) {
               platform_log_stream("pivot key doesn't match in child and parent\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               dynamic_btree_node_unget(cc, cfg, &child);
               dynamic_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (idx != dynamic_btree_num_entries(node.hdr) - 1
                && dynamic_btree_key_compare(cfg,
                                             dynamic_btree_get_pivot(cfg, node.hdr, idx + 1),
                                             dynamic_btree_get_tuple_key(cfg, child.hdr, dynamic_btree_num_entries(child.hdr) - 1)) < 0) {
               platform_log_stream("child tuple larger than parent bound\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               dynamic_btree_print_locked_node(cfg, &node, PLATFORM_ERR_LOG_HANDLE);
               dynamic_btree_print_locked_node(cfg, &child, PLATFORM_ERR_LOG_HANDLE);
               platform_assert(0);
               dynamic_btree_node_unget(cc, cfg, &child);
               dynamic_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         } else {
            // child index
            if (idx != dynamic_btree_num_entries(node.hdr) - 1
                && dynamic_btree_key_compare(cfg,
                                             dynamic_btree_get_pivot(cfg, node.hdr, idx + 1),
                                             dynamic_btree_get_pivot(cfg, child.hdr, dynamic_btree_num_entries(child.hdr) - 1)) < 0) {
               platform_log_stream("child pivot larger than parent bound\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               dynamic_btree_print_locked_node(cfg, &node, PLATFORM_ERR_LOG_HANDLE);
               dynamic_btree_print_locked_node(cfg, &child, PLATFORM_ERR_LOG_HANDLE);
               platform_assert(0);
               dynamic_btree_node_unget(cc, cfg, &child);
               dynamic_btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         dynamic_btree_node_unget(cc, cfg, &child);
         bool child_is_left_edge = is_left_edge && idx == 0;
         if (!dynamic_btree_verify_node(cc, cfg, child.addr, type,
                                child_is_left_edge)) {
            dynamic_btree_node_unget(cc, cfg, &node);
            goto out;
         }
      }
   }
   dynamic_btree_node_unget(cc, cfg, &node);
   result = TRUE;
out:
   platform_close_log_stream(PLATFORM_ERR_LOG_HANDLE);

   return result;
}

bool
dynamic_btree_verify_tree(cache *cc,
                  dynamic_btree_config *cfg,
                  uint64 addr,
                  page_type type)
{
   return dynamic_btree_verify_node(cc, cfg, addr, type, TRUE);
}

void
dynamic_btree_print_lookup(cache                *cc, // IN
                           dynamic_btree_config *cfg, // IN
                           uint64                root_addr, // IN
                           page_type             type, // IN
                           const slice           key) // IN
{
   dynamic_btree_node node, child_node;
   uint32 h;
   uint16 child_idx;

   node.addr = root_addr;
   dynamic_btree_print_node(cc, cfg, &node, PLATFORM_DEFAULT_LOG_HANDLE);
   dynamic_btree_node_get(cc, cfg, &node, type);

   for (h = node.hdr->height; h > 0; h--) {
      child_idx = dynamic_btree_find_pivot(cfg, node.hdr, key, less_than_or_equal);
      child_node.addr = dynamic_btree_get_child_addr(cfg, node.hdr, child_idx);
      dynamic_btree_print_node(cc, cfg, &child_node, PLATFORM_DEFAULT_LOG_HANDLE);
      dynamic_btree_node_get(cc, cfg, &child_node, type);
      dynamic_btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   bool found;
   uint16 idx = dynamic_btree_find_tuple(cfg, node.hdr, key, &found);
   platform_log("Matching index: %u (%d) of %u\n", idx, found, node.hdr->num_entries);
   dynamic_btree_node_unget(cc, cfg, &node);
}

uint64
dynamic_btree_extent_count(cache        *cc,
                   dynamic_btree_config *cfg,
                   uint64        root_addr)
{
   uint64 meta_head = dynamic_btree_root_to_meta_addr(cfg, root_addr, 0);
   return mini_allocator_extent_count(cc, PAGE_TYPE_BRANCH, meta_head);
}

/*
 *-----------------------------------------------------------------------------
 *
 * dynamic_btree_config_init --
 *
 *      Initialize dynamic_btree config values
 *
 *-----------------------------------------------------------------------------
 */


void
dynamic_btree_config_init(dynamic_btree_config *dynamic_btree_cfg,
                          data_config          *data_cfg,
                          uint64                rough_count_height,
                          uint64                page_size,
                          uint64                extent_size)
{
   dynamic_btree_cfg->data_cfg = data_cfg;

   dynamic_btree_cfg->page_size   = page_size;
   dynamic_btree_cfg->extent_size = extent_size;
   dynamic_btree_cfg->rough_count_height = rough_count_height;
}
