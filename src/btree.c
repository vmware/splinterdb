// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * btree.c --
 *
 *     This file contains the implementation for b-trees.
 */

#include <emmintrin.h>

#include "platform.h"

#include "util.h"
#include "btree.h"
#include "task.h"
#include "pl_splinter_trace.h"

#include "poison.h"

#define BTREE_WAIT 1
#define BTREE_NUM_TUPLE_GRANULARITY 128

#define TRACE_CLASS_NAME SPLINTER
TRACE_DEFINE_TOKEN_BUCKETS(SPLINTER)

//#define BTREE_TRACE
#if defined(BTREE_TRACE)
char trace_key[24] = { 0xe4, 0x1e, 0x51, 0xfa, 0xf0, 0x71, 0x43, 0x36,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
uint64 trace_addr = 503087104;
uint64 trace_root = 500563968;
page_handle *trace_page = NULL;
#endif

static inline uint64
btree_root_to_meta_addr(cache *cc,
                        btree_config *cfg,
                        uint64 root_addr,
                        uint64 meta_page_no)
{
   return root_addr + (meta_page_no + 1) * cfg->page_size;
}

void            btree_iterator_get_curr (iterator *itor, char **key, char **data, data_type *type);
platform_status btree_iterator_at_end   (iterator *itor, bool *at_end);
platform_status btree_iterator_advance  (iterator *itor);
void            btree_iterator_print    (iterator *itor);

const static iterator_ops btree_iterator_ops = {
   .get_curr = btree_iterator_get_curr,
   .at_end   = btree_iterator_at_end,
   .advance  = btree_iterator_advance,
   .print    = btree_iterator_print,
};

struct PACKED btree_hdr {
   uint64 next_addr;
   uint64 next_extent_addr;
   uint64 generation;
   uint16 num_entries;
   uint8 height;
   uint8 is_packed;
};

/*
 *-----------------------------------------------------------------------------
 *
 * btree_alloc --
 *
 *      Allocates a node from the preallocator. Will refill it if there are no
 *      more nodes available for the given height.
 *
 *      Note: btree_alloc returns the addr unlocked.
 *
 *-----------------------------------------------------------------------------
 */

void
btree_alloc(cache *         cc,
            mini_allocator *mini,
            uint64          height,
            char *          key,
            uint64 *        next_extent,
            page_type       type,
            btree_node *    node)
{
   node->addr = mini_allocator_alloc(mini, height, key, next_extent);
   debug_assert(node->addr != 0);
   node->page = cache_alloc(cc, node->addr, type);
   node->hdr  = (btree_hdr *)(node->page->data);
}


static inline void
btree_merge_tuples(btree_config *cfg,
                   const char *key,
                   const char *old_data,
                   char *new_data)
{
   // FIXME: [yfogel 2020-01-11] If/when we have start/end compaction callbacks
   //    this call is actually violating the contract (it's being called
   //    outside of [start,end].
   //    If/when we add those other callbacks.. we could just call them right
   //    here (as if it was a tiny compaction), or add a separate parameter
   //    to the existing callbacks to indicate it's a one-off
   //    Until/unless we add start/end this doesn't matter
   return data_merge_tuples(cfg->data_cfg, key, old_data, new_data);
}


/*
 *-----------------------------------------------------------------------------
 *
 * btree_node_[get,release] --
 *
 *      Gets the node with appropriate lock or releases the lock.
 *
 *-----------------------------------------------------------------------------
 */

void
btree_node_get(cache        *cc,
               btree_config *cfg,
               btree_node   *node,
               page_type     type)
{
   debug_assert(node->addr != 0);

   node->page = cache_get(cc, node->addr, TRUE, type);
   node->hdr = (btree_hdr *)(node->page->data);
}

bool
btree_node_claim(cache *cc,           // IN
                 btree_config *cfg,   // IN
                 btree_node *node)    // IN
{
   return cache_claim(cc, node->page);
}

void
btree_node_lock(cache *cc,           // IN
                btree_config *cfg,   // IN
                btree_node *node)    // IN
{
   cache_lock(cc, &node->page);
   cache_mark_dirty(cc, node->page);
   node->hdr = (btree_hdr *)(node->page->data);
}

void
btree_node_unlock(cache *cc,           // IN
                  btree_config *cfg,   // IN
                  btree_node *node)    // IN
{
   cache_unlock(cc, node->page);
}

void
btree_node_unclaim(cache *cc,           // IN
                   btree_config *cfg,   // IN
                   btree_node *node)    // IN
{
   cache_unclaim(cc, node->page);
}

void
btree_node_unget(cache *cc,           // IN
                 btree_config *cfg,   // IN
                 btree_node *node)    // IN
{
   cache_unget(cc, node->page);
   node->page = NULL;
   node->hdr = NULL;
}

static inline void
btree_node_full_unlock(cache *cc,           // IN
                       btree_config *cfg,   // IN
                       btree_node *node)    // IN
{
   btree_node_unlock(cc, cfg, node);
   btree_node_unclaim(cc, cfg, node);
   btree_node_unget(cc, cfg, node);
}

static inline void
btree_node_get_from_cache_ctxt(btree_config *cfg,        // IN
                               cache_async_ctxt *ctxt,   // IN
                               btree_node *node)         // OUT
{
   node->addr = ctxt->page->disk_addr;
   node->page = ctxt->page;
   node->hdr = (btree_hdr *)node->page->data;
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_[get,add,find]_pivot --
 * btree_[get,add,find]_tuple --
 * btree_pivot_addr --
 * btree_get_data --
 *
 *      get returns a pointer to the object
 *      add adds the pointed to the object
 *      find returns the index of the extremal key satisfying the lookup_type
 *            condition wrt key
 *      addr returns the addr at pivot k
 *      data returns the data at pivot k
 *
 *-----------------------------------------------------------------------------
 */

typedef enum lookup_type {
   less_than,
   less_than_or_equal,
   greater_than,
   greater_than_or_equal,
} lookup_type;

static inline uint8 *
btree_get_table(btree_config *cfg,
                btree_node   *node)
{
   return (uint8 *)node->hdr + sizeof(btree_hdr);
}

static inline uint16
btree_num_entries(btree_config *cfg,
                  btree_node   *node)
{
   return node->hdr->num_entries;
}

static inline void
btree_inc_num_entries(btree_config *cfg,
                      btree_node   *node)
{
   node->hdr->num_entries++;
}

static inline uint16
btree_height(btree_config *cfg,
             btree_node   *node)
{
   return node->hdr->height;
}

static inline bool
btree_is_packed(btree_config *cfg,
                btree_node   *node)
{
   return node->hdr->is_packed;
}

static inline bool
btree_node_is_full(btree_config *cfg,
                   btree_node *node)
{
   if (btree_height(cfg, node) == 0) {
      return (btree_is_packed(cfg, node) &&
            btree_num_entries(cfg, node) == cfg->tuples_per_packed_leaf)
         || (!btree_is_packed(cfg, node) &&
               btree_num_entries(cfg, node) == cfg->tuples_per_leaf);
   }
   return (btree_is_packed(cfg, node) &&
         btree_num_entries(cfg, node) == cfg->pivots_per_packed_index)
      || (!btree_is_packed(cfg, node) &&
            btree_num_entries(cfg, node) == cfg->pivots_per_index);
}


static inline char *
btree_get_pivot(btree_config *cfg,
                btree_node   *node,
                uint32        k)
{
   btree_hdr *hdr = node->hdr;
   uint8 idx;
   uint8 *table;
   if (hdr->is_packed) {
      return (char *)hdr + sizeof(btree_hdr)
         + k * (btree_key_size(cfg) + sizeof(uint64));
   }
   table = btree_get_table(cfg, node);
   idx = table[k];
   return (char *)table + cfg->pivots_per_index * sizeof(uint8)
         + idx * (btree_key_size(cfg) + sizeof(uint64));
}

static inline uint64
btree_pivot_addr(btree_config *cfg,
                 btree_node *node,
                 uint32 k)
{
   return *(uint64 *)(btree_get_pivot(cfg, node, k) + btree_key_size(cfg));
}

static inline void
btree_set_pivot_key_and_addr(btree_config *cfg,
                             btree_node *node,
                             uint32 k,
                             const char *new_pivot_key,
                             uint64 new_addr)
{
   char *pivot_key = btree_get_pivot(cfg, node, k);
   uint64 *addr = (uint64*)(pivot_key + btree_key_size(cfg));
   memmove(pivot_key, new_pivot_key, btree_key_size(cfg));
   *addr = new_addr;
}

/*
 * shifts pivots in unpacked btree node right by shift pivots starting at
 * idx'th pivot
 *
 * this shift only affects the redirection table
 */
static inline uint8
btree_shift_unpacked_pivots(btree_config *cfg,
                            btree_node   *node,
                            uint32        idx)
{
   debug_assert(!btree_is_packed(cfg, node));
   debug_assert(btree_height(cfg, node) != 0);
   uint8 *table = btree_get_table(cfg, node);
   uint8 free_entry = table[btree_num_entries(cfg, node)];
   size_t bytes_to_shift =
      (btree_num_entries(cfg, node) - idx) * sizeof(*table);
   const uint8 *src_entry = &table[idx];
   uint8 *dest_entry = &table[idx + 1];
   memmove(dest_entry, src_entry, bytes_to_shift);
   table[idx] = free_entry;
   return free_entry;
}

static inline char *
btree_get_tuple(btree_config *cfg,
                btree_node   *node,
                uint32        k)
{
   btree_hdr *hdr = node->hdr;
   uint8 idx;
   uint8 *table;
   if (hdr->is_packed) {
      return ((char *)hdr) + sizeof(*hdr) + k * (btree_key_size(cfg) + btree_message_size(cfg));
   }
   table = btree_get_table(cfg, node);
   idx = table[k];
   return (char *)table + cfg->tuples_per_leaf * sizeof(uint8)
         + idx * (btree_key_size(cfg) + btree_message_size(cfg));
}

static inline char *
btree_get_data(btree_config *cfg,
               btree_node *node,
               uint32 k)
{
   return btree_get_tuple(cfg, node, k) + btree_key_size(cfg);
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_find_pivot --
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

static inline uint32
round_down_logarithm(uint32 size)
{
   if (size <= 1) return 0;
   return (8 * sizeof(uint32)) - __builtin_clz(size - 1);
}

static inline void
btree_update_lowerbound(uint16 *lo,
                        uint16 *mid,
                        int cmp,
                        lookup_type comp)
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

/*
 *-----------------------------------------------------------------------------
 *
 * btree_find_pivot --
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


static inline uint32
btree_find_pivot(btree_config *cfg,
                 btree_node   *node,
                 const char   *key,
                 lookup_type   comp)
{
   uint16 lo_idx = 0, mid_idx;
   uint32 i;
   int cmp;
   uint32 size = btree_num_entries(cfg, node);

   // handle small sizes with special cases
   if (size == 0) {
      return 0;
   }

   if (size == 1) {
      cmp = btree_key_compare(cfg, btree_get_pivot(cfg, node, 0), key);
      switch (comp) {
         case less_than:
            debug_assert(cmp < 0);
            return 0;
         case less_than_or_equal:
            debug_assert(cmp <= 0);
            return 0;
         case greater_than:
            return cmp > 0 ? 0 : 1;
         case greater_than_or_equal:
            return cmp >= 0 ? 0 : 1;
      }
   }

   // compute round down po2 then start binary search on overlapping ranges
   mid_idx = size - (1u << (round_down_logarithm(size) - 1));
   size = 1u << (round_down_logarithm(size) - 1);
   cmp = btree_key_compare(cfg, btree_get_pivot(cfg, node, mid_idx), key);
   btree_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);

   // continue binary search (ranges are now powers of 2)
   for (i = round_down_logarithm(size); i != 0; i--) {
      size /= 2;
      mid_idx = lo_idx + size;
      cmp = btree_key_compare(cfg, btree_get_pivot(cfg, node, mid_idx), key);
      btree_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);
   }

   // adjust final result based on comp
   switch(comp) {
      case less_than:
      case less_than_or_equal:
         return lo_idx;
      case greater_than:
         if (lo_idx == 0 &&
               btree_key_compare(cfg, btree_get_pivot(cfg, node, 0), key) > 0) {
            return 0;
         }
         return lo_idx + 1;
      case greater_than_or_equal:
         if (lo_idx == 0 &&
               btree_key_compare(cfg, btree_get_pivot(cfg, node, 0), key) >= 0) {
            return 0;
         }
         return lo_idx + 1;
      default:
         platform_assert(0);
   }
   // should be unreachable
   return (uint16)-1;
}

/*
 * overwrite the pivot in parent at position pos with a pointer to child_node.
 *
 * write lock must be held on parent and read lock on child
 */
static inline void
btree_add_pivot_at_pos(btree_config *cfg,
                       btree_node   *parent_node,
                       btree_node   *child_node,
                       uint16        pos,
                       bool          is_new_root)
{
   debug_assert(btree_height(cfg, parent_node) != 0);

   // get the pivot key for the child
   const char *pivot_key;
   if (is_new_root) {
      // new root: use the min key so that the left edge pivots are all min key
      debug_assert(pos == 0);
      pivot_key = cfg->data_cfg->min_key;
   } else if (btree_height(cfg, child_node) == 0) {
      // child is a leaf
      pivot_key = btree_get_tuple(cfg, child_node, 0);
   } else {
      // child is an internal node
      pivot_key = btree_get_pivot(cfg, child_node, 0);
   }

   uint64 child_addr = child_node->addr;
   btree_set_pivot_key_and_addr(cfg, parent_node, pos, pivot_key, child_addr);
   btree_inc_num_entries(cfg, parent_node);
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_find_tuple --
 *
 *      performs a binary search to find the extremal tuple relative to comp.
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
 *      tuple index to return.
 *
 *-----------------------------------------------------------------------------
 */

static inline uint32
btree_find_tuple(btree_config *cfg,
                 btree_node   *node,
                 const char   *key,
                 lookup_type   comp)
{
   // FIXME: [yfogel 2020-07-08]
   //     We actually want to change the return type
   //     (and all the various indexes also) to be int32
   //     That way it's just return -1 where the definition is
   //     much more simple and straightforward
   uint16 lo_idx = 0, mid_idx;
   uint32 i;
   int cmp;
   uint32 size = btree_num_entries(cfg, node);

   // handle small sizes with special cases
   if (size == 0)
      return 0;

   if (size == 1) {
      cmp = btree_key_compare(cfg, btree_get_tuple(cfg, node, 0), key);
      switch (comp) {
         case less_than:
            debug_assert(cmp < 0);
            return 0;
         case less_than_or_equal:
            debug_assert(cmp <= 0);
            return 0;
         case greater_than:
            return cmp > 0 ? 0 : 1;
         case greater_than_or_equal:
            return cmp >= 0 ? 0 : 1;
      }
   }

   // compute round down po2 then start binary search on overlapping ranges
   mid_idx = size - (1u << (round_down_logarithm(size) - 1));
   size = 1u << (round_down_logarithm(size) - 1);
   cmp = btree_key_compare(cfg, btree_get_tuple(cfg, node, mid_idx), key);
   btree_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);

   // continue binary search (ranges are now powers of 2)
   for (i = round_down_logarithm(size); i != 0; i--) {
      size /= 2;
      mid_idx = lo_idx + size;
      cmp = btree_key_compare(cfg, btree_get_tuple(cfg, node, mid_idx), key);
      btree_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);
   }

   // adjust final result based on comp
   switch(comp) {
      case less_than:
      case less_than_or_equal:
         return lo_idx;
      case greater_than:
         if (lo_idx == 0 &&
               btree_key_compare(cfg, btree_get_tuple(cfg, node, 0), key) > 0) {
            return 0;
         }
         return lo_idx + 1;
      case greater_than_or_equal:
         if (lo_idx == 0 &&
               btree_key_compare(cfg, btree_get_tuple(cfg, node, 0), key) >= 0) {
            return 0;
         }
         return lo_idx + 1;
      default:
         platform_assert(0);
   }
   // should be unreachable
   return (uint16)-1;
}

/*
 * shifts tuples in packed btree node right by shift tuples starting at idx'th
 * tuple
 */
__attribute__ ((unused)) static inline void
btree_shift_packed_tuples(btree_config *cfg,
                          btree_node   *node,
                          uint32        idx,
                          uint32        shift)
{
   debug_assert(btree_is_packed(cfg, node));
   debug_assert(btree_height(cfg, node) == 0);
   char *dest_tuple = btree_get_tuple(cfg, node, idx + shift);
   const char *src_tuple  = btree_get_tuple(cfg, node, idx);
   uint64 tuples_to_shift = btree_num_entries(cfg, node) - idx;
   size_t bytes_to_shift = tuples_to_shift * btree_tuple_size(cfg);
   memmove(dest_tuple, src_tuple, bytes_to_shift);
}

/*
 * shifts tuples in unpacked btree node right by shift tuples starting at
 * idx'th tuple
 *
 * this shift only affects the redirection table
 */
static inline uint8
btree_shift_unpacked_tuples(btree_config *cfg,
                            btree_node   *node,
                            uint32        idx,
                            uint32        shift)
{
   debug_assert(!btree_is_packed(cfg, node));
   debug_assert(btree_height(cfg, node) == 0);
   uint8 *table = btree_get_table(cfg, node);
   uint8 free_entry = table[btree_num_entries(cfg, node)];
   size_t bytes_to_shift =
      (btree_num_entries(cfg, node) - idx) * sizeof(*table);
   const uint8 *src_entry = &table[idx];
   uint8 *dest_entry = &table[idx + shift];
   memmove(dest_entry, src_entry, bytes_to_shift);
   table[idx] = free_entry;
   return free_entry;
}

/*
 * Adds the give key and value to node (must be a leaf).
 *
 * Returns TRUE if a new tuple was added and FALSE if the new tuple was merged
 * into an existing tuple with the same key.
 *
 * Pre-conditions:
 *    1. write lock on node
 *    2. node is a leaf
 *    3. node has space for another tuple
 */
static inline bool
btree_add_tuple(btree_config  *cfg,
                btree_scratch *scratch,
                btree_node    *node,
                const char    *key,
                const char    *data,
                uint64        *generation)
{
   debug_assert(btree_height(cfg, node) == 0);
   debug_assert(!btree_node_is_full(cfg, node));
   debug_assert(!btree_is_packed(cfg, node));

   uint32 idx = btree_find_tuple(cfg, node, key, greater_than_or_equal);

   *generation = node->hdr->generation++;

   /*
    * Only overwrite if we really found a match.
    * if idx==n_entries then btree_get_tuple will return pointer to something
    * that isn't a tuple.
    * idx is 0 if there are no entries but that's a special case of returning
    * n_entries.
    */
   if (idx < btree_num_entries(cfg, node) &&
       btree_key_compare(cfg, btree_get_tuple(cfg, node, idx), key) == 0)
   {
      const uint64 message_size = btree_message_size(cfg);
      char *old_data = scratch->add_tuple.old_data;
      char * const btree_data = btree_get_data(cfg, node, idx);

      // merge if we have the key already
      memmove(old_data, btree_data, message_size);
      memmove(btree_data, data, message_size);
      btree_merge_tuples(cfg, key, old_data, btree_data);

      // FIXME: [yfogel 2020-01-11] we should perhaps copy the key over as well
      //    It compares the same but in theory we don't compare the whole key.
      // FIXME: [yfogel 2020-01-11] We should fail the insert if any of:
      //  - old data is INVALID
      //  - new data is INVALID (probably failed earlier)
      //  - merged is INVALID

      return FALSE;
   }

   __attribute__ ((unused)) uint8 free_entry =
      btree_shift_unpacked_tuples(cfg, node, idx, 1);

   memmove(btree_get_tuple(cfg, node, idx), key, btree_key_size(cfg));
   memmove(btree_get_data(cfg, node, idx), data, btree_message_size(cfg));
   node->hdr->num_entries++;

   TRACESplinter(1, SPLINTERTraceBTreeAddTuple,
                 idx, node->hdr->num_entries, free_entry);

   return TRUE;
}

static inline void
btree_add_tuple_at_pos(btree_config *cfg,
                       btree_node *node,
                       char *key,
                       char *data,
                       uint16 pos)
{
   memmove(btree_get_tuple(cfg, node, pos), key, btree_key_size(cfg));
   memmove(btree_get_data(cfg, node, pos), data, btree_message_size(cfg));
   node->hdr->num_entries++;
}


/*
 *-----------------------------------------------------------------------------
 *
 * UTILITY FUNCTIONS
 *
 * btree_node_is_full --
 *
 *      Returns true if the node is full and false otherwise.
 *
 *      Assumes node has already been gotten.
 *
 *-----------------------------------------------------------------------------
 */

static inline bool
btree_addrs_share_extent(btree_config *cfg,
                         uint64        left_addr,
                         uint64        right_addr)
{
   return right_addr / cfg->extent_size == left_addr / cfg->extent_size;
}

static inline uint64
btree_page_size(btree_config *cfg)
{
   return cfg->page_size;
}

static inline uint64
btree_get_extent_base_addr(btree_config *cfg,
                           btree_node   *node)
{
   return node->addr / cfg->extent_size * cfg->extent_size;
}

__attribute__ ((unused))
static inline bool
btree_range_valid(cache        *cc,
                  btree_config *cfg,
                  uint64        root_addr,
                  page_type     type,
                  char         *min_key,
                  char         *max_key)
{
   btree_node root = { .addr = root_addr };
   btree_node_get(cc, cfg, &root, type);
   char *btree_min_key, *btree_max_key;
   bool range_valid = TRUE;
   if (root.hdr->height == 0) {
      btree_min_key = btree_get_tuple(cfg, &root, 0);
      btree_max_key = btree_get_tuple(cfg, &root, root.hdr->num_entries);
   } else {
      btree_min_key = btree_get_pivot(cfg, &root, 0);
      btree_max_key = btree_get_pivot(cfg, &root, root.hdr->num_entries);
   }
   if (btree_key_compare(cfg, btree_min_key, max_key) >= 0)
      range_valid = FALSE;
   if (btree_key_compare(cfg, btree_max_key, min_key) <= 0)
      range_valid = FALSE;
   btree_node_unget(cc, cfg, &root);
   return range_valid;
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_split_node --
 *
 *      Splits the node at left_addr into a new node at right_addr. Uses
 *      anonymous pages for both and swaps in when complete.
 *
 *      Assumes claim is held on left_node. Returns with write lock.
 *      Assumes right_node has a free addr, but isn't held
 *
 *-----------------------------------------------------------------------------
 */

void
btree_split_node(cache *cc,              // IN
                 btree_config *cfg,      // IN
                 btree_node *left_node,  // IN
                 btree_node *right_node) // IN
{
   uint16 target_right_entries = btree_num_entries(cfg, left_node) / 2;
   uint16 target_left_entries = btree_num_entries(cfg, left_node) - target_right_entries;

   /*
    * 4 parts to the node
    * 1- Beginning (up to excluding table)
    * 2- left portion of table
    * 3- right portion of table
    * 4- after table
    * Total should be btree_page_size(cfg));
    * Parts 2 and 3 swap order when being copied to right node
    */
   const uint8 *left_table = btree_get_table(cfg, left_node);
   const ptrdiff_t diff = diff_ptr(left_node->hdr, left_table);
   platform_assert(diff >= 0);
   const size_t header_to_table = diff;
   const size_t suffix = btree_page_size(cfg)
                         - header_to_table
                         - target_left_entries
                         - target_right_entries;

   // Copy part 1 (beginning up to table)
   memmove(right_node->hdr, left_node->hdr, header_to_table);

   uint8 *right_table = btree_get_table(cfg, right_node);

   // Copy part 3 (target_right_entries get copied and shifted to the beginning)
   memmove(&right_table[0],
           &left_table[target_left_entries],
           target_right_entries);

   // Copy part 2 (target_left entries get copied afterwards)
   memmove(&right_table[target_right_entries],
           &left_table[0],
           target_left_entries);

   // Copy part 4 (end of table up to end of node)
   memmove(&right_table[target_left_entries + target_right_entries],
           &left_table[target_left_entries + target_right_entries],
           suffix);

   right_node->hdr->num_entries = target_right_entries;
   right_node->hdr->next_addr = left_node->hdr->next_addr;
   //btree_node_lock(cc, cfg, left_node);
   left_node->hdr->num_entries = target_left_entries;
   left_node->hdr->next_addr = right_node->addr;

   // unshare
   cache_unshare(cc, right_node->page);
   right_node->page = right_node->page;
   right_node->hdr = right_node->hdr;
}

/*
 * add_shared_pivot allocates a new_child as an alias of child and adds a pivot
 * in parent which points to it. new_child will contain the alloc'd page on
 * return.
 *
 * must hold write lock on parent and read lock on child. new_child is
 * overwritten
 */

void
btree_add_shared_pivot(cache          *cc,
                       btree_config   *cfg,
                       mini_allocator *mini,
                       btree_node     *parent,
                       btree_node     *child,
                       btree_node     *new_child)
{
   debug_assert(!btree_is_packed(cfg, parent));
   debug_assert(!btree_is_packed(cfg, child));
   debug_assert(btree_height(cfg, parent) != 0);

   uint16 height = btree_height(cfg, child);
   btree_alloc(cc, mini, height, NULL, NULL, PAGE_TYPE_MEMTABLE, new_child);
   cache_share(cc, child->page, new_child->page);
   uint16 child_num_entries = btree_num_entries(cfg, child);
   uint16 new_entry_num = child_num_entries - child_num_entries / 2;
   char *pivot_key;
   if (btree_height(cfg, child) != 0) {
      pivot_key = btree_get_pivot(cfg, child, new_entry_num);
   } else {
      pivot_key = btree_get_tuple(cfg, child, new_entry_num);
   }
   uint32 pivot_idx = btree_find_pivot(cfg, parent, pivot_key, greater_than);

   debug_assert(pivot_idx != 0);

   // move the entries to make room
   btree_shift_unpacked_pivots(cfg, parent, pivot_idx);

   // set the key and addr of the pivot
   uint64 child_addr = new_child->addr;
   btree_set_pivot_key_and_addr(cfg, parent, pivot_idx, pivot_key, child_addr);
   btree_inc_num_entries(cfg, parent);
}


/*
 *-----------------------------------------------------------------------------
 *
 * btree_split_root --
 *
 *      Splits the root. Assumes write lock held and returns with a read lock
 *      only. Children are returned unlocked.
 *
 *-----------------------------------------------------------------------------
 */

int btree_split_root(btree_config   *cfg,       // IN
                     cache          *cc,        // IN
                     mini_allocator *mini,      // IN/OUT
                     btree_node     *root_node) // IN/OUT
{
   // allocate a new left node
   btree_node left_node;
   uint16     height = btree_height(cfg, root_node);
   btree_alloc(cc, mini, height, NULL, NULL, PAGE_TYPE_MEMTABLE, &left_node);

   // copy root to left, then split
   memmove(left_node.hdr, root_node->hdr, btree_page_size(cfg));

   // fix root hdr and add new pivots
   root_node->hdr->num_entries = 0;
   if (btree_height(cfg, root_node) == 0) {
      // need to reset the root node table since there may be more pivots per
      // index than tuples per leaf
      uint8 *table = btree_get_table(cfg, root_node);
      for (ptrdiff_t i = 0; i < cfg->pivots_per_index; i++) {
         table[i] = i;
      }
   }
   root_node->hdr->height++;
   btree_add_pivot_at_pos(cfg, root_node, &left_node, 0, TRUE);
   btree_node right_node;
   btree_add_shared_pivot(cc, cfg, mini, root_node, &left_node, &right_node);

   // release root
   btree_node_unlock(cc, cfg, root_node);
   btree_node_unclaim(cc, cfg, root_node);

   // split
   btree_split_node(cc, cfg, &left_node, &right_node);

   btree_node_full_unlock(cc, cfg, &left_node);
   btree_node_full_unlock(cc, cfg, &right_node);

   return 0;
}

uint64
btree_init(cache          *cc,
           btree_config   *cfg,
           mini_allocator *mini,
           bool            is_packed)
{
   // get a free node for the root
   // we don't use the next_addr arr for this, since the root doesn't
   // maintain constant height
   page_type type = is_packed ? PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;
   allocator *     al   = cache_allocator(cc);
   uint64          base_addr;
   platform_status rc = allocator_alloc_extent(al, &base_addr);
   platform_assert_status_ok(rc);
   
   page_handle *root_page = cache_alloc(cache_get_volatile_cache(cc), base_addr, type);
   //page_handle *root_page = cache_alloc(cc, base_addr, type);

   // FIXME: [yfogel 2020-07-01] maybe here (or refactor?)
   //    we need to be able to have range tree initialized
   // set up the root
   btree_node root;
   root.page = root_page;
   root.addr = base_addr;
   root.hdr  = (btree_hdr *)root_page->data;

   root.hdr->next_addr   = 0;
   root.hdr->generation  = 0;
   root.hdr->num_entries = 0;
   root.hdr->height      = 0;
   root.hdr->is_packed   = is_packed;

   if (!is_packed) {
      uint8 *table = btree_get_table(cfg, &root);
      for (uint64 i = 0; i < cfg->tuples_per_leaf; i++) {
         table[i] = i;
      }
   }
   cache_mark_dirty(cc, root.page);
   // release root
   cache_unlock(cc, root_page);
   cache_unclaim(cc, root_page);
   cache_unget(cc, root_page);

   // set up the mini allocator
   mini_allocator_init(mini, cc, cfg->data_cfg, root.addr + cfg->page_size, 0,
         BTREE_MAX_HEIGHT, type);

   return root.addr;
}

/*
 * By separating should_zap and zap, this allows us to use a single
 * refcount to control multiple b-trees.  For example a branch
 * in splinter that has a point tree and range-delete tree
 */
bool
btree_should_zap_dec_ref(cache        *cc,
                         btree_config *cfg,
                         uint64        root_addr,
                         page_type     type)
{
   // FIXME: [yfogel 2020-07-06] Should we assert that both cfgs provide
   //       work the same w.r.t. root_to_meta_addr?
   //       Right now we're always passing in the point config
   uint64       meta_page_addr = btree_root_to_meta_addr(cc, cfg, root_addr, 0);
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
   cache_lock(cc, &meta_page);

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
btree_inc_range(cache        *cc,
                btree_config *cfg,
                uint64        root_addr,
                const char   *start_key,
                const char   *end_key)
{
   ThreadContext * ctx = cache_get_context(cc);
   start_nontx_withlocks(ctx);
   uint64 meta_page_addr = btree_root_to_meta_addr(cc, cfg, root_addr, 0);
   if (start_key != NULL && end_key != NULL) {
      debug_assert(btree_key_compare(cfg, start_key, end_key) < 0);
   }
   mini_allocator_inc_range(cc, cfg->data_cfg, PAGE_TYPE_BRANCH,
         meta_page_addr, start_key, end_key);
   end_nontx_withlocks(ctx);
}

bool
btree_zap_range(cache        *cc,
                btree_config *cfg,
                uint64        root_addr,
                const char   *start_key,
                const char   *end_key,
                page_type     type)
{
   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   debug_assert(type == PAGE_TYPE_BRANCH || start_key == NULL);

   if (start_key != NULL && end_key != NULL) {
      platform_assert(btree_key_compare(cfg, start_key, end_key) < 0);
   }

   uint64 meta_page_addr = btree_root_to_meta_addr(cc, cfg, root_addr, 0);
   bool fully_zapped = mini_allocator_zap(cc, cfg->data_cfg, meta_page_addr,
         start_key, end_key, type);
   return fully_zapped;
}

bool btree_zap(cache        *cc,
               btree_config *cfg,
               uint64        root_addr,
               page_type     type)
{
   return btree_zap_range(cc, cfg, root_addr, NULL, NULL, type);
}

page_handle *
btree_blind_inc(cache        *cc,
                btree_config *cfg,
                uint64        root_addr,
                page_type     type)
{
   //platform_log("(%2lu)blind inc %14lu\n", platform_get_tid(), root_addr);
   uint64 meta_page_addr = btree_root_to_meta_addr(cc, cfg, root_addr, 0);
   return mini_allocator_blind_inc(cc, meta_page_addr);
}

void
btree_blind_zap(cache        *cc,
                btree_config *cfg,
                page_handle  *meta_page,
                page_type     type)
{
   //platform_log("(%2lu)blind zap %14lu\n", platform_get_tid(), root_addr);
   mini_allocator_blind_zap(cc, type, meta_page);
}


/*
 *-----------------------------------------------------------------------------
 *
 * btree_insert --
 *
 *      Inserts the tuple into the dynamic btree.
 *
 *      Return value:
 *      success       -- the tuple has been inserted
 *      locked        -- the insert failed, but the caller didn't fill the tree
 *      lock acquired -- the insert failed, and the caller filled the tree
 *
 *-----------------------------------------------------------------------------
 */


platform_status
btree_insert(cache          *cc,         // IN
             btree_config   *cfg,        // IN
             btree_scratch  *scratch,    // IN
             uint64          root_addr,  // IN
             mini_allocator *mini,       // IN
             const char     *key,        // IN
             const char     *data,       // IN
             uint64         *generation, // OUT
             bool           *was_unique) // OUT
{
   uint64 wait = 1, leaf_wait = 1;

   platform_status rc = STATUS_OK;

   btree_node root_node;
   root_node.addr = root_addr;

start_over:
   btree_node_get(cc, cfg, &root_node, PAGE_TYPE_MEMTABLE);

   if (btree_node_is_full(cfg, &root_node) &&
         btree_node_claim(cc, cfg, &root_node)) {
      // root is full and we got the claim
      btree_node_lock(cc, cfg, &root_node);
      btree_split_root(cfg, cc, mini, &root_node);
   }

   // have a read lock on root and we are not responsible for splitting.
   while (btree_height(cfg, &root_node) == 0) {
      // upgrade to claim
      if (btree_node_claim(cc, cfg, &root_node)) {
         break;
      }
      btree_node_unget(cc, cfg, &root_node);
      platform_sleep(wait);
      wait = wait > 2048 ? wait : 2 * wait;
      btree_node_get(cc, cfg, &root_node, PAGE_TYPE_MEMTABLE);
   }
   wait = 1;

   uint8 height = btree_height(cfg, &root_node);
   if (height == 0) {
      // we have a claim

      if (btree_node_is_full(cfg, &root_node)) {
         btree_node_lock(cc, cfg, &root_node);
         btree_split_root(cfg, cc, mini, &root_node);
      } else {
         btree_node_lock(cc, cfg, &root_node);
         *was_unique =
            btree_add_tuple(cfg, scratch, &root_node, key, data, generation);
         btree_node_full_unlock(cc, cfg, &root_node);
#if defined(BTREE_TRACE)
         if (btree_key_compare(cfg, key, trace_key) == 0)
            platform_log("adding tuple to %lu, root addr %lu\n",
                                 root_node.addr, root_node.addr);
#endif
         goto out;
      }
   }

   btree_node parent_node = root_node;

   for (height = btree_height(cfg, &root_node); height != 0; height--) {
      uint32 child_idx =
         btree_find_pivot(cfg, &parent_node, key, less_than_or_equal);
      btree_node child_node;
      child_node.addr = btree_pivot_addr(cfg, &parent_node, child_idx);
      debug_assert(cache_page_valid(cc, child_node.addr));
      btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);

      if (btree_node_is_full(cfg, &child_node)) {
         btree_node_unget(cc, cfg, &child_node);
         if (btree_node_is_full(cfg, &parent_node)
               || !btree_node_claim(cc, cfg, &parent_node)) {
            btree_node_unget(cc, cfg, &parent_node);
            goto start_over;
         }
         // hold parent claim

         debug_assert(child_idx < parent_node.hdr->num_entries);
         debug_assert(btree_pivot_addr(cfg, &parent_node, child_idx) == child_node.addr);

         btree_node_get(cc, cfg, &child_node, PAGE_TYPE_MEMTABLE);
         if (btree_node_claim(cc, cfg, &child_node)) {
            // hold child claim
            if (!btree_node_is_full(cfg, &child_node)) {
               // someone already split the child, redo this height
               btree_node_unclaim(cc, cfg, &parent_node);
               btree_node_unclaim(cc, cfg, &child_node);
               btree_node_unget(cc, cfg, &child_node);
               height++;
               continue;
            }
            // hold parent and child claims

            btree_node_lock(cc, cfg, &parent_node);
            btree_node_lock(cc, cfg, &child_node);
            btree_node new_child_node;
            btree_add_shared_pivot(cc, cfg, mini, &parent_node, &child_node,
                  &new_child_node);
            btree_node_full_unlock(cc, cfg, &parent_node);

            btree_split_node(cc, cfg, &child_node, &new_child_node);

            btree_node_unlock(cc, cfg, &child_node);
            btree_node_unclaim(cc, cfg, &child_node);
            btree_node_unlock(cc, cfg, &new_child_node);
            btree_node_unclaim(cc, cfg, &new_child_node);

            char *new_child_pivot;
            if (new_child_node.hdr->height == 0)
               new_child_pivot = btree_get_tuple(cfg, &new_child_node, 0);
            else
               new_child_pivot = btree_get_pivot(cfg, &new_child_node, 0);

            if (btree_key_compare(cfg, key, new_child_pivot) < 0) {
               // we want the left child, so release the right
               btree_node_unget(cc, cfg, &new_child_node);
            } else {
               // we want the right child, so release the left and reassign
               btree_node_unget(cc, cfg, &child_node);
               child_node = new_child_node;
            }
         } else {
            // hold parent claim, failed to get child claim
            btree_node_unget(cc, cfg, &child_node);
            btree_node_unclaim(cc, cfg, &parent_node);
            platform_sleep(wait);
            wait = wait > 2048 ? wait : 2 * wait;
            height++;
            continue;
         }
      }
      wait = 1;

      // child does not need to be split
      if (height == 1) {
         // if child is a leaf, try to claim and start the round over if it
         // fails, we need to be careful because otherwise child could fill
         // while we unget and retry
         if (!btree_node_claim(cc, cfg, &child_node)) {
            btree_node_unget(cc, cfg, &child_node);
            if (parent_node.page != NULL) {
               platform_sleep(leaf_wait);
               leaf_wait = leaf_wait > 2048 ? leaf_wait : 2 * leaf_wait;
               height++;
               continue;
            } else {
               goto start_over;
            }
         }
         btree_node_lock(cc, cfg, &child_node);
      }

      if (parent_node.page != NULL) {
         btree_node_unget(cc, cfg, &parent_node);
      }
      parent_node = child_node;
   }

   // have a leaf with a write lock
   *was_unique =
      btree_add_tuple(cfg, scratch, &parent_node, key, data, generation);

   btree_node_full_unlock(cc, cfg, &parent_node);
out:
   return rc;
}


/*
 *-----------------------------------------------------------------------------
 *
 * btree_lookup_node --
 *
 *      lookup_node finds the node of height stop_at_height with
 *      (node.min_key <= key < node.max_key) and returns it with a read lock
 *      held.
 *
 *      out_rank returns the rank of out_node amount nodes of height
 *      stop_at_height.
 *
 *      If any change is made here, please change btree_lookup_async_with_ref
 *      too.
 *
 *-----------------------------------------------------------------------------
 */

platform_status
btree_lookup_node(cache        *cc,             // IN
                  btree_config *cfg,            // IN
                  uint64        root_addr,      // IN
                  const char   *key,            // IN
                  uint16        stop_at_height, // IN  search down to this height
                  page_type     type,           // IN
                  uint64       *out_rank,       // OUT returns the rank of the out_node among nodes of its height
                  btree_node   *out_node)       // OUT returns the node of height stop_at_height in which key was found
{
   btree_node node, child_node;
   uint32 h;
   uint16 child_idx;

   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   node.addr = root_addr;
   btree_node_get(cc, cfg, &node, type);

   uint64 ppi = btree_is_packed(cfg, &node)
              ? cfg->pivots_per_packed_index
              : cfg->pivots_per_index;

   *out_rank = 0;
   for (h = btree_height(cfg, &node); h > stop_at_height; h--) {
      *out_rank *= ppi;
      child_idx = btree_find_pivot(cfg, &node, key, less_than_or_equal);
      *out_rank += child_idx;
      child_node.addr = btree_pivot_addr(cfg, &node, child_idx);

      btree_node_get(cc, cfg, &child_node, type);
      debug_assert(child_node.page->disk_addr == child_node.addr);
      btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   *out_node = node;
   return STATUS_OK;
}


inline void
btree_lookup_with_ref(cache         *cc,        // IN
                      btree_config  *cfg,       // IN
                      uint64         root_addr, // IN
                      btree_node    *node,      // IN/OUT
                      page_type      type,      // IN
                      const char    *key,       // IN
                      char         **data,      // OUT
                      bool          *found)     // OUT
{
   uint64 dummy;
   btree_lookup_node(cc, cfg, root_addr, key, 0, type, &dummy, node);
   uint16 idx = btree_find_tuple(cfg, node, key, greater_than_or_equal);
   if (node->hdr->num_entries > 0 && idx < node->hdr->num_entries
         && btree_key_compare(cfg, btree_get_tuple(cfg, node, idx), key) == 0) {
      *data = btree_get_data(cfg, node, idx);
      *found = TRUE;
   } else {
      *found = FALSE;
      btree_node_unget(cc, cfg, node);
   }
}

// FIXME: [nsarmicanic 2020-08-11] change key and data to void*
// same for the external entire APIs
void
btree_lookup(cache        *cc,        // IN
             btree_config *cfg,       // IN
             uint64        root_addr, // IN
             const char   *key,       // IN
             char         *data_out,  // OUT
             bool         *found)     // OUT
{
   btree_node node;
   char *data;
   btree_lookup_with_ref(cc, cfg, root_addr, &node, PAGE_TYPE_BRANCH, key,
                         &data, found);
   if (*found) {
      memmove(data_out, data, btree_message_size(cfg));
      btree_node_unget(cc, cfg, &node);
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_lookup_async --
 *
 *      Async btree point lookup. The ctxt should've been initialized using
 *      btree_ctxt_init(). The return value can be either of:
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
btree_lookup_async(cache *cc,              // IN
                   btree_config *cfg,      // IN
                   uint64 root_addr,       // IN
                   char *key,              // IN
                   char *data_out,         // OUT
                   bool *found,            // OUT
                   btree_async_ctxt *ctxt) // IN
{
   cache_async_result res;
   btree_node node;
   char *data;

   res = btree_lookup_async_with_ref(cc, cfg, root_addr, &node, key, &data,
                                     found, ctxt);
   if (res == async_success && *found) {
      memmove(data_out, data, btree_message_size(cfg));
      btree_node_unget(cc, cfg, &node);
   }

   return res;
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_async_set_state --
 *
 *      Set the state of the async btree lookup state machine.
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
btree_async_set_state(btree_async_ctxt *ctxt,
                      btree_async_state new_state)
{
   ctxt->prev_state = ctxt->state;
   ctxt->state = new_state;
}


/*
 *-----------------------------------------------------------------------------
 *
 * btree_async_callback --
 *
 *      Callback that's called when the async cache get loads a page into
 *      the cache. This function moves the async btree lookup state machine's
 *      state ahead, and calls the upper layer callback that'll re-enqueue
 *      the btree lookup for dispatch.
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
btree_async_callback(cache_async_ctxt *cache_ctxt)
{
   btree_async_ctxt *ctxt = cache_ctxt->cbdata;

   platform_assert(SUCCESS(cache_ctxt->status));
   platform_assert(cache_ctxt->page);
//   platform_log("%s:%d tid %2lu: ctxt %p is callback with page %p (%#lx)\n",
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
 *
 * btree_lookup_async_with_ref --
 *
 *      State machine for the async btree point lookup. This uses hand over
 *      hand locking to descend the tree and every time a child node needs to
 *      be looked up from the cache, it uses the async get api. A reference
 *      to the parent node is held in btree_async_ctxt->node while a reference
 *      to the child page is obtained by the cache_get_async() in
 *      btree_async_ctxt->cache_ctxt->page
 *
 * Results:
 *      See btree_lookup_async(). if returning async_success and *found = TRUE,
 *      this returns with ref on the btree leaf. Caller must do unget() on
 *      node_out.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

cache_async_result
btree_lookup_async_with_ref(cache *cc,              // IN
                            btree_config *cfg,      // IN
                            uint64 root_addr,       // IN
                            btree_node *node_out,   // IN/OUT
                            char *key,              // IN
                            char **data,            // OUT
                            bool *found,            // OUT
                            btree_async_ctxt *ctxt) // IN
{
   cache_async_result res = 0;
   bool done = FALSE;
   btree_node *node = &ctxt->node;

   do {
      switch (ctxt->state) {
      case btree_async_state_start:
      {
         ctxt->child_addr = root_addr;
         node->page = NULL;
         btree_async_set_state(ctxt, btree_async_state_get_node);
         // fallthrough
      }
      case btree_async_state_get_node:
      {
         cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

         cache_ctxt_init(cc, btree_async_callback, ctxt, cache_ctxt);
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
            btree_async_set_state(ctxt, btree_async_state_get_index_complete);
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
         if (btree_height(cfg, node) == 0) {
            btree_async_set_state(ctxt, btree_async_state_get_leaf_complete);
            break;
         }
         uint16 child_idx = btree_find_pivot(cfg, node, key,
                                             less_than_or_equal);
         ctxt->child_addr = btree_pivot_addr(cfg, node, child_idx);
         btree_async_set_state(ctxt, btree_async_state_get_node);
         break;
      }
      case btree_async_state_get_leaf_complete:
      {
         const uint16 idx = btree_find_tuple(cfg, node, key,
                                             greater_than_or_equal);
         const uint16 num_entries = btree_num_entries(cfg, node);
         if (num_entries > 0 && idx < num_entries &&
             btree_key_compare(cfg, btree_get_tuple(cfg, node, idx), key)==0) {
            *data = btree_get_data(cfg, node, idx);
            *found = TRUE;
            *node_out = *node;
         } else {
            *found = FALSE;
            btree_node_unget(cc, cfg, node);
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
 * btree_iterator_init --
 * btree_iterator_get_curr --
 * btree_iterator_advance --
 * btree_iterator_at_end
 *
 *      initializes a btree iterator
 *
 *-----------------------------------------------------------------------------
 */

void
btree_iterator_get_curr(iterator   *base_itor,
                        char      **key,
                        char      **data,
                        data_type  *type)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;
   debug_assert(itor->curr.hdr != NULL);
   //if (itor->at_end || itor->idx == itor->curr.hdr->num_entries) {
   //   btree_print_tree(itor->cc, itor->cfg, itor->root_addr);
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
   *type = itor->cfg->type;
}


static inline void
debug_btree_check_unexpected_at_end(debug_only btree_iterator *itor)
{
#if SPLINTER_DEBUG
   if (itor->curr.addr != itor->end.addr || itor->idx != itor->end_idx) {
      if (itor->idx == itor->curr.hdr->num_entries && itor->curr.hdr->next_addr == 0) {
         platform_log("btree_iterator bad at_end %lu curr %lu idx %u "
                      "end %lu end_idx %u\n", itor->root_addr,
                      itor->start_addr, itor->start_idx,
                      itor->end.addr, itor->end_idx);
         btree_print_tree(itor->cc, itor->cfg, itor->root_addr);
         platform_assert(0);
      }
   }
#endif
}


static bool
btree_iterator_is_at_end(btree_iterator *itor)
{
   debug_assert(itor != NULL);
   debug_assert(!itor->empty_itor);
   debug_assert(itor->idx <= itor->curr.hdr->num_entries);
   if (itor->is_live) {
      if (itor->curr.hdr->next_addr == 0 && itor->idx == itor->curr.hdr->num_entries) {
         // reached end before max key or no max key
         return TRUE;
      }
      if (itor->max_key != NULL) {
         char *key;
         if (itor->height == 0) {
            key = btree_get_tuple(itor->cfg, &itor->curr, itor->idx);
         } else {
            key = btree_get_pivot(itor->cfg, &itor->curr, itor->idx);
         }
         return btree_key_compare(itor->cfg, itor->max_key, key) <= 0;
      }
      return FALSE;
   }

   if (itor->end.addr == 0) {
      // There is no max_key
      debug_assert(itor->max_key == NULL);
      return itor->curr.hdr->next_addr == 0 && itor->idx == itor->curr.hdr->num_entries;
   }

   debug_btree_check_unexpected_at_end(itor);

   debug_assert(itor->max_key != NULL);
   return itor->curr.addr == itor->end.addr && itor->idx == itor->end_idx;
}

static bool
btree_iterator_is_last_tuple(btree_iterator *itor)
{
   debug_assert(!itor->at_end);
   debug_assert(!btree_iterator_is_at_end(itor));
   debug_assert(itor->height == 0);
   debug_assert(itor->cfg->type == data_type_range);
   debug_assert(itor->idx < itor->curr.hdr->num_entries);
   btree_config *cfg = itor->cfg;
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
      if (itor->max_key != NULL) {
         char *next_key;
         if (itor->idx + 1 == itor->curr.hdr->num_entries) {
            // at last element of the leaf; get first element of next leaf
            btree_node next = { .addr = itor->curr.hdr->next_addr };
            btree_node_get(itor->cc, cfg, &next, itor->page_type);
            next_key = btree_get_tuple(cfg, &next, 0);
            btree_node_unget(itor->cc, cfg, &next);
         } else {
            // get next element of this leaf
            next_key = btree_get_tuple(cfg, &itor->curr,
                                       itor->idx + 1);
         }
         return btree_key_compare(cfg, itor->max_key, next_key) <= 0;
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
      //    (empty btree or no matches)
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
btree_iterator_set_curr_key_and_data(btree_iterator *itor)
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
     itor->curr_key = btree_get_tuple(itor->cfg, &itor->curr, itor->idx);
     itor->curr_data = btree_get_data(itor->cfg, &itor->curr, itor->idx);
     debug_assert(itor->curr_data != NULL);
  } else {
     itor->curr_key = btree_get_pivot(itor->cfg, &itor->curr, itor->idx);
  }
  debug_assert(itor->curr_key != NULL);
}

static void
debug_btree_iterator_validate_next_tuple(debug_only btree_iterator *itor)
{
#if SPLINTER_DEBUG
   btree_config *cfg = itor->cfg;
   debug_assert(btree_key_compare(cfg, itor->debug_prev_key, itor->curr_key) < 0);
   if (itor->max_key) {
      debug_assert(btree_key_compare(cfg, itor->curr_key, itor->max_key) < 0);
   }
   if (itor->cfg->type == data_type_range) {
      int cmp = btree_key_compare(cfg, itor->debug_prev_end_key, itor->curr_key);
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
               btree_key_compare(cfg, itor->curr_data, itor->max_key) <= 0);
      }
   }
#endif
}

static void
debug_btree_iterator_save_last_tuple(debug_only btree_iterator *itor)
{
#if SPLINTER_DEBUG
   itor->debug_is_packed = btree_is_packed(itor->cfg, &itor->curr);

   uint64 key_size = itor->cfg->data_cfg->key_size;
   memmove(itor->debug_prev_key, itor->curr_key, key_size);
   if (itor->cfg->type == data_type_range) {
      memmove(itor->debug_prev_end_key, itor->curr_data, key_size);
   }
#endif
}

static void
btree_iterator_clamp_end(btree_iterator *itor)
{
   if (1
       && itor->height == 0
       && itor->cfg->type == data_type_range
       && btree_iterator_is_last_tuple(itor)
       && btree_key_compare(itor->cfg, itor->curr_data, itor->max_key) > 0)
   {
      // FIXME: [aconway 2020-09-11] Handle this better
      itor->curr_data = (char *) itor->max_key;
   }
}

btree_hdr* hdr_buf = NULL;
btree_hdr* prev = NULL;
platform_status
btree_iterator_advance(iterator *base_itor)
{
   debug_assert(base_itor != NULL);
   btree_iterator *itor = (btree_iterator *)base_itor;
   // We should not be calling advance on an empty iterator
   debug_assert(!itor->empty_itor);
   debug_assert(!itor->at_end);
   debug_assert(!btree_iterator_is_at_end(itor));

   cache *cc = itor->cc;
   btree_config *cfg = itor->cfg;

   itor->curr_key = NULL;
   itor->curr_data = NULL;
   itor->idx++;
   debug_assert(itor->idx <= itor->curr.hdr->num_entries);

   /*
    * FIXME: [aconway 2021-04-10] This check breaks ranges queries for live
    * memtables. It causes them to stop prematurely as a result of
    * btree-splitting. That change requires iterators not to advance to the
    * next btree node when they hit num_entries, because that node may have
    * been deallocated.
    *
    *itor->at_end = btree_iterator_is_at_end(itor);
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
         btree_node next = { 0 };
         if (itor->curr.hdr->next_addr != 0) {
            next.addr = itor->curr.hdr->next_addr;
            btree_node_get(cc, cfg, &next, itor->page_type);
            if (itor->page_type == PAGE_TYPE_MEMTABLE) {
               debug_assert(!btree_is_packed(cfg, &next));
            } else {
               debug_assert(btree_is_packed(cfg, &next));
            }
         }
	 
/*
         if(hdr_buf != NULL){
	    itor->curr.hdr = prev;
            memmove(itor->curr.hdr, hdr_buf, 4096);
	 }
         else{
            hdr_buf = TYPED_ARRAY_MALLOC(platform_get_heap_id(), hdr_buf, 4096);
	 }
*/



	 /*
         void *prefetch_hdr = (void*)(itor->curr.hdr);

         for(uint64 prefetch_offset = 0; prefetch_offset < 1024 * 4;
                      prefetch_offset = prefetch_offset + 64)
         {
             _mm_prefetch((char*)(prefetch_hdr + prefetch_offset), 1);
         }
	 */



         btree_node_unget(cc, cfg, &itor->curr);

         itor->curr = next;
         itor->idx = 0;

/*
	 prev = itor->curr.hdr;
	 memmove(hdr_buf, itor->curr.hdr, 4096);
	 itor->curr.hdr = hdr_buf;
	 itor->curr.page->data = (char*)hdr_buf;
*/



	 /*
	 void *prefetch_hdr = (void*)(itor->curr.hdr->next_addr);

         for(uint64 prefetch_offset = 0; prefetch_offset < 1024 * 4;
                      prefetch_offset = prefetch_offset + 64)
         {
             _mm_prefetch((char*)(prefetch_hdr + prefetch_offset), 1);
         }
	 */


         // To prefetch:
         // 1. btree must be static
         // 2. curr node must start extent
         // 3. which can't be the last extent
         // 4. and we can't be at the end
         if (1 && itor->do_prefetch
               && itor->curr.addr != 0
               && btree_is_packed(cfg, &itor->curr)
               && !btree_addrs_share_extent(cfg, itor->curr.addr, last_addr)
               && itor->curr.hdr->next_extent_addr != 0
               && (itor->end.addr == 0
                  || !btree_addrs_share_extent(cfg, itor->curr.addr, itor->end.addr))) {
            // IO prefetch the next extent
            cache_prefetch(
               cc, itor->curr.hdr->next_extent_addr, PAGE_TYPE_BRANCH);
         }
      } else {
         // We already know we are at the end
         itor->at_end = TRUE;
         debug_assert(btree_iterator_is_at_end(itor));
         return STATUS_OK;
      }
   }

   itor->at_end = btree_iterator_is_at_end(itor);
   if (itor->at_end) {
      return STATUS_OK;
   }

   btree_iterator_set_curr_key_and_data(itor);

   btree_iterator_clamp_end(itor);

   debug_btree_iterator_validate_next_tuple(itor);
   debug_btree_iterator_save_last_tuple(itor);

   return STATUS_OK;
}


platform_status
btree_iterator_at_end(iterator *itor,
                      bool *at_end)
{
   debug_assert(itor != NULL);
   btree_iterator *btree_itor = (btree_iterator *)itor;
   *at_end = btree_itor->at_end;

   return STATUS_OK;
}

void
btree_iterator_print(iterator *itor)
{
   debug_assert(itor != NULL);
   btree_iterator *btree_itor = (btree_iterator *)itor;

   platform_log("########################################\n");
   platform_log("## btree_itor: %p\n", itor);
   platform_log("## root: %lu\n", btree_itor->root_addr);
   platform_log("## curr %lu end %lu\n",
                btree_itor->curr.addr, btree_itor->end.addr);
   platform_log("## idx %u end_idx %u\n",
                btree_itor->idx, btree_itor->end_idx);
   btree_print_node(btree_itor->cc, btree_itor->cfg, &btree_itor->curr,
                    PLATFORM_DEFAULT_LOG_HANDLE);
}


/*
 *-----------------------------------------------------------------------------
 *
 * Caller must guarantee:
 *    min_key needs to be valid until the first call to advance() returns
 *    max_key (if not null) needs to be valid until at_end() returns true
 * If we are iterating over ranges
 *    (data_type == data_type_range && height == 0)
 *
 * btree_iterator on range_delete (leaves only) clamps the ranges outputted to
 *  the input min/max.  It also includes all ranges that overlap the [min,max)
 *  range, and not just keys that overlap it. (Potentially 1 extra range
 *  included in the iteration)
 *
 *-----------------------------------------------------------------------------
 */
void
btree_iterator_init(cache          *cc,
                    btree_config   *cfg,
                    btree_iterator *itor,
                    uint64          root_addr,
                    page_type       page_type,
                    const char     *min_key,
                    const char     *max_key,
                    bool            do_prefetch,
                    bool            is_live,
                    uint32          height,
                    data_type       data_type)
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
   itor->super.ops   = &btree_iterator_ops;
   uint64 dummy;
   /*
    * start_is_clamped is true if we don't need to make any changes
    * (e.g. it's already been restricted to the min_key/max_key range)
    */
   bool start_is_clamped = TRUE;

   // FIXME: [nsarmicanic 2020-07-15]
   // May want to have a goto emtpy itor

   // Check if this is an empty iterator
   if ((itor->root_addr == 0) ||
       (max_key != NULL && btree_key_compare(cfg, min_key, max_key) >= 0)) {
      itor->at_end = TRUE;
      itor->empty_itor = TRUE;
      return;
   }

   debug_assert(page_type == PAGE_TYPE_MEMTABLE ||
                page_type == PAGE_TYPE_BRANCH);

   // if the iterator height is greater than the actual btree height,
   // set values so that at_end will be TRUE
   if (height > 0) {
      btree_node temp = {
         .addr = root_addr,
      };
      btree_node_get(cc, cfg, &temp, page_type);
      if (page_type == PAGE_TYPE_MEMTABLE) {
         debug_assert(!btree_is_packed(cfg, &temp));
      } else {
         debug_assert(btree_is_packed(cfg, &temp));
      }
      uint32 root_height = btree_height(cfg, &temp);
      btree_node_unget(cc, cfg, &temp);
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
   if (!is_live && max_key != NULL) {
      btree_lookup_node(cc, cfg, root_addr, max_key, height, page_type, &dummy,
                        &itor->end);
      if (page_type == PAGE_TYPE_MEMTABLE) {
         debug_assert(!btree_is_packed(cfg, &itor->end));
      } else {
         debug_assert(btree_is_packed(cfg, &itor->end));
      }
      if (height == 0) {
         itor->end_idx = btree_find_tuple(cfg, &itor->end, max_key, greater_than_or_equal);
      } else {
         itor->end_idx = btree_find_pivot(cfg, &itor->end, max_key, greater_than_or_equal);
      }

      debug_assert(itor->end_idx == itor->end.hdr->num_entries || ((height == 0) &&
            btree_key_compare(cfg, max_key, btree_get_tuple(cfg, &itor->end, itor->end_idx)) <= 0)
            || ((height != 0) && btree_key_compare(cfg, max_key,
                  btree_get_pivot(cfg, &itor->end, itor->end_idx)) <= 0));
      debug_assert(itor->end_idx == itor->end.hdr->num_entries || itor->end_idx == 0 ||
            ((height == 0) && btree_key_compare(cfg, max_key,
               btree_get_tuple(cfg, &itor->end, itor->end_idx - 1)) > 0) || ((height != 0)
            && btree_key_compare(cfg, max_key,
               btree_get_pivot(cfg, &itor->end, itor->end_idx - 1)) > 0));

      btree_node_unget(cc, cfg, &itor->end);
   }

   btree_lookup_node(cc, cfg, root_addr, min_key, height, page_type, &dummy,
                     &itor->curr);
   if (page_type == PAGE_TYPE_MEMTABLE) {
      debug_assert(!btree_is_packed(cfg, &itor->curr));
   } else {
      debug_assert(btree_is_packed(cfg, &itor->curr));
   }
   // empty
   if (itor->curr.hdr->num_entries == 0 && itor->curr.hdr->next_addr == 0) {
      itor->at_end = TRUE;
      return;
   }

   if (height == 0) {
      //FIXME: [yfogel 2020-07-08] no need for extra compare when less_than_or_equal is supproted
      itor->idx = btree_find_tuple(cfg, &itor->curr, min_key, greater_than_or_equal);
      if (data_type == data_type_range && itor->idx > 0) {
         char *end_data = btree_get_data(cfg, &itor->curr, itor->idx-1);
         if (btree_key_compare(cfg, end_data, min_key) > 0) {
            /*
             * [start_key,end_data) overlaps min_key
             * it MUST be on the leaf where that key would belong. So we have to
             * look to the left on the same leaf, but only on the same leaf.
             */
            itor->idx--;
            start_is_clamped = FALSE;
         }
      }
   } else {
      itor->idx = btree_find_pivot(cfg, &itor->curr, min_key, greater_than_or_equal);
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
      btree_node next = { .addr = itor->curr.hdr->next_addr };
      btree_node_get(cc, cfg, &next, page_type);
      itor->idx = 0;
      btree_node_unget(cc, cfg, &itor->curr);
      itor->curr = next;
   }

   itor->start_addr = itor->curr.addr;
   itor->start_idx = itor->idx;

   debug_assert((itor->is_live) || (itor->idx != itor->curr.hdr->num_entries
         || (itor->curr.addr == itor->end.addr && itor->idx == itor->end_idx)));

   if (itor->curr.hdr->next_addr != 0) {
      if (itor->do_prefetch && btree_is_packed(cfg, &itor->curr)) {
         // IO prefetch
         uint64 next_extent_addr = itor->curr.hdr->next_extent_addr;
         if (next_extent_addr != 0               // prefetch if not last extent
               && (itor->end.addr == 0           // and either no upper bound
               || !btree_addrs_share_extent(cfg, itor->curr.addr, itor->end.addr))) {
                                                 // or ub not yet reached
            // IO prefetch the next extent
            debug_assert(cache_page_valid(cc, next_extent_addr));
            cache_prefetch(cc, next_extent_addr, PAGE_TYPE_BRANCH);
         }
      }
   }

   itor->at_end = btree_iterator_is_at_end(itor);
   if (itor->at_end) {
      // Special case of an empty iterator
      // FIXME: [nsarmicanic 2020-07-15] Can do unget here and set
      // addr and empty_itor appropriately
      itor->empty_itor = TRUE;
      return;
   }

   // Set curr_key and curr_data
   btree_iterator_set_curr_key_and_data(itor);

   // If we need to clamp start set curr_key to min_key
   if (!start_is_clamped) {
      itor->curr_key = (char *)min_key;
   }

   btree_iterator_clamp_end(itor);

   debug_assert(btree_key_compare(cfg, min_key, itor->curr_key) <= 0);
   debug_btree_iterator_save_last_tuple(itor);
}

void
btree_iterator_deinit(btree_iterator *itor)
{
   debug_assert(itor != NULL);
   if (itor->curr.addr != 0) {
      btree_node_unget(itor->cc, itor->cfg, &itor->curr);
   } else {
      platform_assert(itor->empty_itor);
   }
}

typedef struct {
   // from pack_req
   cache          *cc;
   btree_config   *cfg;

   iterator       *itor;

   hash_fn         hash;
   uint32         *fingerprint_arr;
   unsigned int    seed;

   uint64         *root_addr;  // pointers to pack_req's root_addr
   uint64         *num_tuples; // pointers to pack_req's num_tuples

   // internal data
   uint64          next_extent;
   uint16          height;

   btree_node      edge[BTREE_MAX_HEIGHT];
   uint64          idx[BTREE_MAX_HEIGHT];
   btree_node      new_edge;
   btree_node      old_edge;

   mini_allocator  mini;
} btree_pack_internal;

// generation number isn't used in packed btrees
static inline void
btree_pack_node_init_hdr(btree_node *node,
                         uint64 next_extent,
                         uint8 height,
                         bool is_packed)
{
   node->hdr->next_addr = 0;
   node->hdr->next_extent_addr = next_extent;
   node->hdr->num_entries = 0;
   node->hdr->height = height;
   node->hdr->is_packed = is_packed;
}

static inline void
btree_pack_setup(btree_pack_req *req, btree_pack_internal *tree)
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
   btree_config *cfg = tree->cfg;

   // FIXME: [yfogel 2020-07-02] Where is the transition between branch and tree (Alex)
   // 1. Mini allocator? Pre-fetching?
   // 2. Refcount? Shared

   // we create a root here, but we won't build it with the rest
   // of the tree, we'll copy into it at the end
   *(tree->root_addr) = btree_init(cc, cfg, &tree->mini, TRUE);
   tree->height = 0;

   // set up the first leaf
   char first_key[MAX_KEY_SIZE] = { 0 };
   btree_alloc(cc,
               &tree->mini,
               0,
               first_key,
               &tree->next_extent,
               PAGE_TYPE_BRANCH,
               &tree->edge[0]);
   debug_assert(cache_page_valid(cc, tree->next_extent));
   btree_pack_node_init_hdr(&tree->edge[0], tree->next_extent, 0, TRUE);
}

static inline void
btree_pack_loop(btree_pack_internal *tree,   // IN/OUT
                char                *key,    // IN
                char                *data,   // IN
                bool                *at_end) // IN/OUT
{
   if (tree->idx[0] == tree->cfg->tuples_per_packed_leaf) {
      // the current leaf is full, allocate a new one and add to index
      tree->old_edge = tree->edge[0];
      btree_alloc(tree->cc,
                  &tree->mini,
                  0,
                  key,
                  &tree->next_extent,
                  PAGE_TYPE_BRANCH,
                  &tree->new_edge);
      tree->edge[0].hdr->next_addr = tree->new_edge.addr;
      btree_node_full_unlock(tree->cc, tree->cfg, &tree->edge[0]);

      // initialize the new leaf edge
      tree->edge[0] = tree->new_edge;
      debug_assert(cache_page_valid(tree->cc, tree->next_extent));
      btree_pack_node_init_hdr(&tree->edge[0], tree->next_extent, 0, TRUE);

#if defined(BTREE_TRACE)
      if (btree_key_compare(tree->cfg, key, trace_key) == 0)
         platform_log("adding tuple to %lu, root addr %lu\n",
                      tree->edge[0].addr, *tree->root_addr);
#endif
      btree_add_tuple_at_pos(tree->cfg, &tree->edge[0], key, data, 0);
      tree->idx[0] = 1;
      // FIXME: [nsarmicanic 2020-07-02] Not needed for range
      if (tree->hash) {
         tree->fingerprint_arr[*(tree->num_tuples)] =
            tree->hash(key, tree->cfg->data_cfg->key_size, tree->seed);
      }
      (*(tree->num_tuples))++;

      iterator_advance(tree->itor);
      iterator_at_end(tree->itor, at_end);

      uint16 i;
      // this loop finds the first level with a free slot
      // along the way it allocates new index nodes as necessary
      for (i = 1; tree->idx[i] == tree->cfg->pivots_per_packed_index; i++) {
         tree->old_edge = tree->edge[i];
         btree_alloc(tree->cc,
                     &tree->mini,
                     i,
                     key,
                     &tree->next_extent,
                     PAGE_TYPE_BRANCH,
                     &tree->new_edge);
         tree->edge[i].hdr->next_addr = tree->new_edge.addr;
         btree_node_full_unlock(tree->cc, tree->cfg, &tree->edge[i]);

         // initialize the new index edge
         tree->edge[i] = tree->new_edge;
         btree_pack_node_init_hdr(&tree->edge[i], tree->next_extent, i, TRUE);
         btree_add_pivot_at_pos(tree->cfg, &tree->edge[i], &tree->edge[i - 1],
                                0, FALSE);
         //platform_log("adding %lu to %lu at pos 0\n",
         //                     edge[i-1].addr, edge[i].addr);
         tree->idx[i] = 1;
      }

      if (i > tree->height) {
         // need to add a new root
         char first_key[MAX_KEY_SIZE] = { 0 };
         btree_alloc(tree->cc,
                     &tree->mini,
                     i,
                     first_key,
                     &tree->next_extent,
                     PAGE_TYPE_BRANCH,
                     &tree->edge[i]);
         btree_pack_node_init_hdr(&tree->edge[i], tree->next_extent, i, TRUE);
         tree->height++;

         // add old root and it's younger sibling
         btree_add_pivot_at_pos(tree->cfg, &tree->edge[tree->height],
                                &tree->old_edge, 0, TRUE);
         //platform_log("adding %lu to %lu at pos 0\n",
         //                     old_edge.addr, edge[height].addr);
         btree_add_pivot_at_pos(tree->cfg, &tree->edge[tree->height],
                                &tree->edge[tree->height - 1], 1, FALSE);
         //platform_log("adding %lu to %lu at pos 1\n",
         //                     edge[height - 1].addr, edge[height].addr);
         tree->idx[tree->height] = 2;
      } else {
         // add to the index with a free slot
         btree_add_pivot_at_pos(tree->cfg, &tree->edge[i], &tree->edge[i - 1],
                                tree->idx[i], FALSE);
         //platform_log("adding %lu to %lu at pos %u\n",
         //                     edge[i - 1].addr, edge[i].addr, idx[i]);
         tree->idx[i]++;
      }
   } else {
#if defined(BTREE_TRACE)
      if (btree_key_compare(tree->cfg, key, trace_key) == 0)
         platform_log("adding tuple to %lu, root addr %lu\n",
                              tree->edge[0].addr, *tree->root_addr);
#endif
      btree_add_tuple_at_pos(tree->cfg, &tree->edge[0], key, data, tree->idx[0]);
      //if (idx[0] != 0) {
      //   int comp = btree_key_compare(cfg, btree_get_tuple(cfg, &edge[0], idx[0] - 1), key);
      //   if (comp >= 0) {
      //      char key_str[128], last_key_str[128];
      //      btree_key_to_string(cfg, key, key_str);
      //      btree_key_to_string(cfg, btree_get_tuple(cfg, &edge[0], idx[0] - 1), last_key_str);
      //      platform_log("btree_pack OOO keys: \n%s \n%s\n%d\n",
      //                           last_key_str, key_str, comp);
      //      iterator_print(req->itor);
      //      platform_assert(0);
      //   }
      //}

      tree->idx[0]++;
      if (tree->hash) {
         tree->fingerprint_arr[*(tree->num_tuples)] =
            tree->hash(key, tree->cfg->data_cfg->key_size, tree->seed);
      }
      (*(tree->num_tuples))++;

      iterator_advance(tree->itor);
      iterator_at_end(tree->itor, at_end);
   }
}


static inline void
btree_pack_post_loop(btree_pack_internal *tree)
{
   cache *cc = tree->cc;
   btree_config *cfg = tree->cfg;
   // we want to use the allocation node, so we copy the root created in the
   // loop into the btree_init root
   btree_node root;
   root.addr = *(tree->root_addr);
   btree_node_get(cc, cfg, &root, PAGE_TYPE_BRANCH);

   __attribute__((unused))
      bool success = btree_node_claim(cc, cfg, &root);
   debug_assert(success);
   btree_node_lock(cc, cfg, &root);
   memmove(root.hdr, tree->edge[tree->height].hdr, cfg->page_size);
   // fix the root next extent
   root.hdr->next_extent_addr = 0;
   btree_node_full_unlock(cc, cfg, &root);

   // release all the edge nodes;
   for (uint16 i = 0; i <= tree->height; i++) {
      // go back and fix the dangling next extents
      for (uint64 addr = btree_get_extent_base_addr(cfg, &tree->edge[i]);
            addr != tree->edge[i].addr;
            addr += btree_page_size(cfg)) {
         btree_node node = { .addr = addr };
         btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);
         success = btree_node_claim(cc, cfg, &node);
         debug_assert(success);
         btree_node_lock(cc, cfg, &node);
         node.hdr->next_extent_addr = 0;
         btree_node_full_unlock(cc, cfg, &node);
      }
      tree->edge[i].hdr->next_extent_addr = 0;
      btree_node_full_unlock(cc, cfg, &tree->edge[i]);
   }

   char last_key[MAX_KEY_SIZE];
   memset(last_key, 0xff, MAX_KEY_SIZE);
   mini_allocator_release(&tree->mini, last_key);

   // if output tree is empty, zap the tree
   if (*(tree->num_tuples) == 0) {
      btree_zap(tree->cc, tree->cfg, *(tree->root_addr), PAGE_TYPE_BRANCH);
      *(tree->root_addr) = 0;
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_pack --
 *
 *      Packs a btree from an iterator source. Zaps the output tree if it's
 *      empty.
 *
 *-----------------------------------------------------------------------------
 */

platform_status
btree_pack(btree_pack_req *req)
{
   ThreadContext * ctx = cache_get_context(req->cc);
   start_nontx(ctx);
   btree_pack_internal tree;
   ZERO_STRUCT(tree);

   btree_pack_setup(req, &tree);

   char *key, *data;
   data_type type;
   bool at_end;

   iterator_at_end(tree.itor, &at_end);
   while (!at_end) {
      iterator_get_curr(tree.itor, &key, &data, &type);
      //void *prefetch_hdr = (void*)(((btree_iterator *)(tree.itor))->curr.hdr);
      //__builtin_prefetch((void*)(key));
      //__builtin_prefetch((void*)(data));
      
      /*
      for(uint64 prefetch_offset = 0; prefetch_offset < 1024 * 4; 
		      prefetch_offset = prefetch_offset + 64)
      {
         _mm_prefetch((char*)(prefetch_hdr + prefetch_offset), 0);
      }
      */

      debug_assert(type == req->cfg->type);
      btree_pack_loop(&tree, key, data, &at_end);
      // FIXME: [tjiaheng 2020-07-29] find out how we can use req->max_tuples
      // here
      // if (req->max_tuples != 0 && *(tree.num_tuples) == req->max_tuples) {
      //    at_end = TRUE;
      // }
   }

   btree_pack_post_loop(&tree);
   platform_assert(IMPLIES(req->num_tuples == 0, req->root_addr == 0));
   end_nontx(ctx);
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 *
 * branch_pack --
 *
 *      Packs a branch (point and range btree) from an iterator source. Zaps the
 *      empty output tree.
 *
 *-----------------------------------------------------------------------------
 */

platform_status
branch_pack(platform_heap_id hid, branch_pack_req *req)
{
   ThreadContext * ctx = cache_get_context(req->point_req.cc);
   start_nontx(ctx);
   btree_pack_internal *point_tree = TYPED_MALLOC(hid, point_tree);
   btree_pack_internal *range_tree = TYPED_MALLOC(hid, range_tree);
   ZERO_CONTENTS(point_tree);
   ZERO_CONTENTS(range_tree);

   btree_pack_setup(&req->point_req, point_tree);
   btree_pack_setup(&req->range_req, range_tree);

   btree_pack_internal *trees[NUM_DATA_TYPES];
   trees[data_type_point] = point_tree;
   trees[data_type_range] = range_tree;

   char *key, *data;
   data_type type;
   bool at_end;

   /*
    * We are assuming point and range req have the same max_tuples and itor,
    * thus using them interchangeably below
    */
   platform_assert(req->point_req.max_tuples == req->range_req.max_tuples);
   platform_assert(req->point_req.itor == req->range_req.itor);

   // FIXME: [tjiaheng 2020-07-29] find out how we can use req->max_tuples
   // here
   // uint64 max_tuples_per_tree = req->point_req.max_tuples;
   iterator *itor = req->point_req.itor;

   iterator_at_end(itor, &at_end);
   while (!at_end) {
      iterator_get_curr(itor, &key, &data, &type);
      //_mm_prefetch((char*)(key),_MM_HINT_T0);
      //_mm_prefetch((char*)(data),_MM_HINT_T0);
      //__builtin_prefetch((void*)(key));
      //__builtin_prefetch((void*)(data));
      btree_pack_loop(trees[type], key, data, &at_end);
      // FIXME: [tjiaheng 2020-07-29] find out how we can use req->max_tuples
      // here
      // if (max_tuples_per_tree != 0
      //     && *(trees[type]->num_tuples) == max_tuples_per_tree) {
      //    at_end = TRUE;
      // }
   }

   btree_pack_post_loop(point_tree);
   platform_assert(IMPLIES(req->point_req.num_tuples == 0, req->point_req.root_addr == 0));
   btree_pack_post_loop(range_tree);
   platform_assert(IMPLIES(req->range_req.num_tuples == 0, req->range_req.root_addr == 0));
   platform_assert(req->range_req.root_addr == 0);

   platform_free(hid, point_tree);
   platform_free(hid, range_tree);
   end_nontx(ctx);
   return STATUS_OK;
}

/*
 * Returns the rank of key in the btree with root addr root_addr.
 */
static inline uint64
btree_get_rank(cache        *cc,
               btree_config *cfg,
               uint64        root_addr,
               const char   *key)
{
   btree_node leaf;
   uint64 leaf_rank;
   btree_lookup_node(cc, cfg, root_addr, key, 0, PAGE_TYPE_BRANCH, &leaf_rank,
         &leaf);
   uint64 tuple_rank_in_leaf =
      btree_find_tuple(cfg, &leaf, key, greater_than_or_equal);
   btree_node_unget(cc, cfg, &leaf);
   return leaf_rank * cfg->tuples_per_packed_leaf + tuple_rank_in_leaf;
}

/*
 * count_in_range returns the exact number of tuples in the given btree between
 * min_key (inc) and _max_key (excl).
 */

uint64
btree_count_in_range(cache        *cc,
                     btree_config *cfg,
                     uint64        root_addr,
                     const char   *min_key,
                     const char   *max_key)
{
   uint64 min_rank = btree_get_rank(cc, cfg, root_addr, min_key);
   uint64 max_rank = btree_get_rank(cc, cfg, root_addr, max_key);

   return max_rank < min_rank ? 0 : max_rank - min_rank;
}

/*
 * btree_count_in_range_by_iterator perform btree_count_in_range using an
 * iterator instead of by calculating ranks. Used for debugging purposes.
 */

uint64
btree_count_in_range_by_iterator(cache        *cc,
                                 btree_config *cfg,
                                 uint64        root_addr,
                                 const char   *min_key,
                                 const char   *max_key)
{
   btree_iterator btree_itor;
   iterator *itor = &btree_itor.super;
   btree_iterator_init(cc, cfg, &btree_itor, root_addr, PAGE_TYPE_BRANCH,
         min_key, max_key, TRUE, FALSE, 0, data_type_point);
   bool at_end;
   uint64 count= 0;
   iterator_at_end(itor, &at_end);
   while (!at_end) {
      iterator_advance(itor);
      count++;
      iterator_at_end(itor, &at_end);
   }
   btree_iterator_deinit(&btree_itor);

   return count;
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_print_node --
 * btree_print_tree --
 *
 *      Prints out the contents of the node/tree.
 *
 *-----------------------------------------------------------------------------
 */

void
btree_print_locked_node(cache *cc,
                        btree_config *cfg,
                        btree_node *node,
                        platform_stream_handle stream)
{
   char key_string[128];
   char data_string[256];
   platform_log_stream("*******************\n");
   if (btree_height(cfg, node) > 0) {
      platform_log_stream("**  INDEX NODE \n");
      platform_log_stream("**  height: %u \n", btree_height(cfg, node));
      platform_log_stream("**  addr: %lu \n", node->addr);
      platform_log_stream("**  next_addr: %lu \n", node->hdr->next_addr);
      platform_log_stream("**  num_entries: %u \n", btree_num_entries(cfg, node));
      platform_log_stream("**  ptr: %p\n", node->hdr);
      platform_log_stream("**  is_packed: %u\n", node->hdr->is_packed);
      platform_log_stream("-------------------\n");
      if (!node->hdr->is_packed) {
         uint8 *table = btree_get_table(cfg, node);
         for (uint64 i = 0; i < cfg->pivots_per_index; i++)
            platform_log_stream("%lu:%u ", i, table[i]);
         platform_log_stream("\n");
         platform_log_stream("-------------------\n");
      }
      for (uint64 i = 0; i < btree_num_entries(cfg, node); i++) {
         btree_key_to_string(cfg, btree_get_pivot(cfg, node, i), key_string);
         platform_log_stream("%2lu:%s -- %lu\n",
                             i, key_string, btree_pivot_addr(cfg, node, i));
      }
      platform_log_stream("\n");
   } else {
      platform_log_stream("**  LEAF NODE \n");
      platform_log_stream("**  addr: %lu \n", node->addr);
      platform_log_stream("**  next_addr: %lu \n", node->hdr->next_addr);
      platform_log_stream("**  next_extent_addr: %lu \n",
                          node->hdr->next_extent_addr);
      platform_log_stream("**  num_entries: %u \n",
                          btree_num_entries(cfg, node));
      platform_log_stream("**  ptr: %p\n", node->hdr);
      platform_log_stream("**  is_packed: %u\n", node->hdr->is_packed);
      platform_log_stream("-------------------\n");
      if (!node->hdr->is_packed) {
         uint8 *table = btree_get_table(cfg, node);
         for (uint64 i = 0; i < cfg->tuples_per_leaf; i++)
            platform_log_stream("%lu:%u ", i, table[i]);
         platform_log_stream("\n");
         platform_log_stream("-------------------\n");
      }
      for (uint64 i = 0; i < btree_num_entries(cfg, node); i++) {
         btree_key_to_string(cfg, btree_get_tuple(cfg, node, i), key_string);
         btree_message_to_string(cfg, btree_get_data(cfg, node, i), data_string);
         platform_log_stream("%2lu:%s -- %s\n", i, key_string, data_string);
      }
      platform_log_stream("-------------------\n");
      platform_log_stream("\n");
   }
}

void
btree_print_node(cache *cc,
                 btree_config *cfg,
                 btree_node *node,
                 platform_stream_handle stream)
{
   if (!cache_page_valid(cc, node->addr)) {
      platform_log_stream("*******************\n");
      platform_log_stream("** INVALID NODE \n");
      platform_log_stream("** addr: %lu \n", node->addr);
      platform_log_stream("-------------------\n");
      return;
   }
   btree_node_get(cc, cfg, node, PAGE_TYPE_BRANCH);
   btree_print_locked_node(cc, cfg, node, stream);
   btree_node_unget(cc, cfg, node);
}

void
btree_print_subtree(cache *cc,
                    btree_config *cfg,
                    uint64 addr,
                    platform_stream_handle stream)
{
   btree_node node;
   node.addr = addr;
   btree_print_node(cc, cfg, &node, stream);
   if (!cache_page_valid(cc, node.addr)) {
      return;
   }
   btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);
   uint32 idx;

   if (node.hdr->height > 0) {
      for (idx = 0; idx < node.hdr->num_entries; idx++) {
         btree_print_subtree(cc, cfg, btree_pivot_addr(cfg, &node, idx), stream);
      }
   }
   btree_node_unget(cc, cfg, &node);
}

void
btree_print_tree(cache *cc,
                 btree_config *cfg,
                 uint64 root_addr)
{
   platform_open_log_stream();
   btree_print_subtree(cc, cfg, root_addr, stream);
   platform_close_log_stream(PLATFORM_DEFAULT_LOG_HANDLE);
}

void
btree_print_tree_stats(cache *cc,
                       btree_config *cfg,
                       uint64 addr)
{
   btree_node node;
   node.addr = addr;
   btree_node_get(cc, cfg, &node, PAGE_TYPE_BRANCH);

   platform_log("Tree stats: height %u\n", node.hdr->height);
   cache_print_stats(cc);

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
                         const char   *start_key,
                         const char   *end_key)
{
   uint64 meta_head = btree_root_to_meta_addr(cc, cfg, root_addr, 0);
   uint64 extents_used = mini_allocator_count_extents_in_range(cc,
         cfg->data_cfg, type, meta_head, start_key, end_key);
   return extents_used * cfg->extent_size;
}

bool
btree_verify_node(cache *cc,
                  btree_config *cfg,
                  uint64 addr,
                  page_type type,
                  bool is_left_edge)
{
   btree_node node;
   node.addr = addr;
   debug_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   btree_node_get(cc, cfg, &node, type);
   uint32 idx;
   bool result = FALSE;

   platform_open_log_stream();
   for (idx = 0; idx < node.hdr->num_entries; idx++) {
      if (node.hdr->height == 0) {
         // leaf node
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (btree_key_compare(cfg, btree_get_tuple(cfg, &node, idx),
                     btree_get_tuple(cfg, &node, idx + 1)) >= 0) {
               platform_log_stream("out of order tuples\n");
               platform_log_stream("addr: %lu idx %2u\n", node.addr, idx);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
      } else {
         // index node
         btree_node child;
         child.addr = btree_pivot_addr(cfg, &node, idx);
         btree_node_get(cc, cfg, &child, type);
         if (child.hdr->height != node.hdr->height - 1) {
            platform_log_stream("height mismatch\n");
            platform_log_stream("addr: %lu idx: %u\n", node.addr, idx);
            btree_node_unget(cc, cfg, &child);
            btree_node_unget(cc, cfg, &node);
            goto out;
         }
         if (node.hdr->num_entries > 0 && idx < node.hdr->num_entries - 1) {
            if (btree_key_compare(cfg, btree_get_pivot(cfg, &node, idx),
                     btree_get_pivot(cfg, &node, idx + 1)) >= 0) {
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               btree_print_tree(cc, cfg, addr);
               platform_log_stream("out of order pivots\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (idx == 0 && is_left_edge) {
               if (btree_key_compare(cfg, btree_get_pivot(cfg, &node, idx),
                     cfg->data_cfg->min_key) != 0) {
                  platform_log_stream("left edge pivot not min key\n");
                  platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
                  platform_log_stream("child addr: %lu\n", child.addr);
                  btree_node_unget(cc, cfg, &child);
                  btree_node_unget(cc, cfg, &node);
                  goto out;
               }
            } else if (btree_key_compare(cfg, btree_get_pivot(cfg, &node, idx),
                     btree_get_tuple(cfg, &child, 0)) != 0) {
               platform_log_stream("pivot key doesn't match in child and parent\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu\n", child.addr);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         } else {
            // child index
            if (btree_key_compare(cfg, btree_get_pivot(cfg, &node, idx),
                     btree_get_pivot(cfg, &child, 0)) != 0) {
               platform_log_stream("pivot key doesn't match in child and parent\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         if (child.hdr->height == 0) {
            // child leaf
            if (idx != btree_num_entries(cfg, &node) - 1
                  && btree_key_compare(cfg, btree_get_pivot(cfg, &node, idx + 1),
                     btree_get_tuple(cfg, &child, btree_num_entries(cfg, &child) - 1)) < 0) {
               platform_log_stream("child tuple larger than parent bound\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               btree_print_locked_node(cc, cfg, &node,
                                       PLATFORM_ERR_LOG_HANDLE);
               btree_print_locked_node(cc, cfg, &child,
                                       PLATFORM_ERR_LOG_HANDLE);
               platform_assert(0);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         } else {
            // child index
            if (idx != btree_num_entries(cfg, &node) - 1
                  && btree_key_compare(cfg, btree_get_pivot(cfg, &node, idx + 1),
                     btree_get_pivot(cfg, &child, btree_num_entries(cfg, &child) - 1)) < 0) {
               platform_log_stream("child pivot larger than parent bound\n");
               platform_log_stream("addr: %lu idx %u\n", node.addr, idx);
               platform_log_stream("child addr: %lu idx %u\n", child.addr, idx);
               btree_print_locked_node(cc, cfg, &node,
                                       PLATFORM_ERR_LOG_HANDLE);
               btree_print_locked_node(cc, cfg, &child,
                                       PLATFORM_ERR_LOG_HANDLE);
               platform_assert(0);
               btree_node_unget(cc, cfg, &child);
               btree_node_unget(cc, cfg, &node);
               goto out;
            }
         }
         btree_node_unget(cc, cfg, &child);
         bool child_is_left_edge = is_left_edge && idx == 0;
         if (!btree_verify_node(cc, cfg, child.addr, type,
                                child_is_left_edge)) {
            btree_node_unget(cc, cfg, &node);
            goto out;
         }
      }
   }
   btree_node_unget(cc, cfg, &node);
   result = TRUE;
out:
   platform_close_log_stream(PLATFORM_ERR_LOG_HANDLE);

   return result;
}

bool
btree_verify_tree(cache *cc,
                  btree_config *cfg,
                  uint64 addr,
                  page_type type)
{
   return btree_verify_node(cc, cfg, addr, type, TRUE);
}

void
btree_print_lookup(cache *cc,            // IN
                   btree_config *cfg,    // IN
                   uint64 root_addr,     // IN
                   page_type type,       // IN
                   char *key)            // IN
{
   btree_node node, child_node;
   uint32 h;
   uint16 child_idx;

   node.addr = root_addr;
   btree_print_node(cc, cfg, &node, PLATFORM_DEFAULT_LOG_HANDLE);
   btree_node_get(cc, cfg, &node, type);

   for (h = node.hdr->height; h > 0; h--) {
      child_idx = btree_find_pivot(cfg, &node, key, less_than_or_equal);
      child_node.addr = btree_pivot_addr(cfg, &node, child_idx);
      btree_print_node(cc, cfg, &child_node, PLATFORM_DEFAULT_LOG_HANDLE);
      btree_node_get(cc, cfg, &child_node, type);
      btree_node_unget(cc, cfg, &node);
      node = child_node;
   }

   uint16 idx = btree_find_tuple(cfg, &node, key, greater_than_or_equal);
   platform_log("Matching index: %u of %u\n", idx, node.hdr->num_entries);
   btree_node_unget(cc, cfg, &node);
}

uint64
btree_extent_count(cache        *cc,
                   btree_config *cfg,
                   uint64        root_addr)
{
   uint64 meta_head = btree_root_to_meta_addr(cc, cfg, root_addr, 0);
   return mini_allocator_extent_count(cc, PAGE_TYPE_BRANCH, meta_head);
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree_config_init --
 *
 *      Initialize btree config values
 *
 *-----------------------------------------------------------------------------
 */


void
btree_config_init(btree_config *btree_cfg,
                  data_config  *data_cfg,
                  data_type     type,
                  uint64        rough_count_height,
                  uint64        page_size,
                  uint64        extent_size)
{
   uint64 unpacked_tuple_size;
   uint64 packed_tuple_size;
   uint64 unpacked_pivot_size;
   uint64 packed_pivot_size;
   uint64 btree_page_size;

   btree_cfg->data_cfg = data_cfg;

   btree_cfg->page_size   = page_size;
   btree_cfg->extent_size = extent_size;
   btree_cfg->rough_count_height = rough_count_height;

   // computed config paramenters
   packed_tuple_size = data_cfg->key_size + data_cfg->message_size;
   // unpacked tuples have an index which adds an extra byte to the max;
   unpacked_tuple_size = packed_tuple_size + sizeof(uint8);
   // key_size pluse address size
   packed_pivot_size = data_cfg->key_size + sizeof(uint64);
   // same as above
   unpacked_pivot_size = packed_pivot_size + sizeof(uint8);
   btree_page_size = btree_cfg->page_size - sizeof(btree_hdr);
   /*
    * max_tuples_per_tree is inexact and we keep using packed_tuple_size rather
    * than the more accurate unpacked_tuple_size for consistency
    */
   btree_cfg->tuples_per_leaf = btree_page_size / unpacked_tuple_size;
   btree_cfg->tuples_per_packed_leaf = btree_page_size / packed_tuple_size;
   btree_cfg->pivots_per_index = btree_page_size / unpacked_pivot_size;
   btree_cfg->pivots_per_packed_index = btree_page_size / packed_pivot_size;

   /*
    * Since the index (in unpacked btrees) is only 1 byte/8 bits, we can hold at
    * most 1<<8 (256) entries in each node.  Clamp the tuple limits in unpacked
    * btrees to the range [0..1<<8]
    */
   if (btree_cfg->tuples_per_leaf > MAX_UNPACKED_IDX) {
      btree_cfg->tuples_per_leaf = MAX_UNPACKED_IDX;
   }
   if (btree_cfg->pivots_per_index > MAX_UNPACKED_IDX) {
      btree_cfg->pivots_per_index = MAX_UNPACKED_IDX;
   }

   btree_cfg->type = type;
}
