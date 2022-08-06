// Copyright 2018-2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk_node.h --
 *
 *     This file contains low-level functions that node and manipulate trunk
 *     node members, such as the header, pivots and associated metadata,
 *     bundles and associated filters, and branches.
 */

#ifndef __TRUNK_NODE_H
#define __TRUNK_NODE_H

/*
 * These are hard-coded to values so that statically allocated
 * structures sized by these limits can fit within 4K byte pages.
 *
 * NOTE: The bundle and sub-bundle related limits below are used to size arrays
 * of structures in splinter_trunk_hdr{}; i.e. Splinter pages of type
 * PAGE_TYPE_TRUNK. So these constants do affect disk-resident structures.
 */
#define TRUNK_MAX_PIVOTS            (20)
#define TRUNK_MAX_BUNDLES           (12)
#define TRUNK_MAX_SUBBUNDLES        (24)
#define TRUNK_MAX_SUBBUNDLE_FILTERS (24U)

/*
 * During Splinter configuration, the fanout parameter is provided by the user.
 * SplinterDB defers internal node splitting in order to use hand-over-hand
 * locking. As a result, index nodes may temporarily have more pivots than the
 * fanout. Therefore, the number of pivot keys is over-provisioned by this
 * value.
 */
#define TRUNK_EXTRA_PIVOT_KEYS (6)

/*
 * A subbundle is a collection of branches which originated in the same node.
 * It is used to organize branches with their routing filters when they are
 * flushed or otherwise moved or reorganized. A query to the node uses the
 * routing filter to filter the branches in the subbundle.
 * Disk-resident artifact.
 */
typedef uint16 trunk_subbundle_state_t;
typedef enum trunk_subbundle_state {
   SB_STATE_INVALID = 0,
   SB_STATE_UNCOMPACTED_INDEX,
   SB_STATE_UNCOMPACTED_LEAF,
   SB_STATE_COMPACTED, // compacted subbundles are always index
} trunk_subbundle_state;

/*
 *-----------------------------------------------------------------------------
 * Splinter Sub-bundle: Disk-resident structure on PAGE_TYPE_TRUNK pages.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK trunk_subbundle {
   trunk_subbundle_state_t state;
   uint16                  start_branch;
   uint16                  end_branch;
   uint16                  start_filter;
   uint16                  end_filter;
} trunk_subbundle;

/*
 *-----------------------------------------------------------------------------
 * Splinter Bundle: Disk-resident structure on PAGE_TYPE_TRUNK pages.
 *
 * A flush moves branches from the parent to a bundle in the child. The bundle
 * is then compacted with a compact_bundle job.
 *
 * Branches are organized into subbundles.
 *
 * When a compact_bundle job completes, the branches in the bundle are replaced
 * with the outputted branch of the compaction and the bundle is marked
 * compacted. If there is not an earlier uncompacted bundle, the bundle can be
 * released and the compacted branch can become a whole branch. This is to
 * maintain the invariant that the outstanding bundles form a contiguous range.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK trunk_bundle {
   uint16 start_subbundle;
   uint16 end_subbundle;
   uint64 num_tuples;
   uint64 num_kv_bytes;
} trunk_bundle;

/*
 *-----------------------------------------------------------------------------
 * Trunk headers: Disk-resident structure
 *
 * Contains metadata for trunk nodes. See below for comments on fields.
 * Found on pages of page type == PAGE_TYPE_TRUNK
 *
 * Generation numbers are used by asynchronous processes to detect node splits.
 *    internal nodes: Splits increment the generation number of the left node.
 *       If a process visits a node with generation number g, then returns at a
 *       later point, it can find all the nodes which it splits into by search
 *       right until it reaches a node with generation number g (inclusive).
 *    leaves: Splits increment the generation numbers of all the resulting
 *       leaves. This is because there are no processes which need to revisit
 *       all the created leaves.
 *-----------------------------------------------------------------------------
 */
struct ONDISK trunk_hdr {
   uint16 num_pivot_keys;   // number of used pivot keys (== num_children + 1)
   uint16 height;           // height of the node
   uint64 next_addr;        // PBN of the node's successor (0 if no successor)
   uint64 generation;       // counter incremented on a node split
   uint64 pivot_generation; // counter incremented when new pivots are added

   uint16 start_branch;      // first live branch
   uint16 start_frac_branch; // first fractional branch (branch in a bundle)
   uint16 end_branch;        // successor to the last live branch
   uint16 start_bundle;      // first live bundle
   uint16 end_bundle;        // successor to the last live bundle
   uint16 start_subbundle;   // first live subbundle
   uint16 end_subbundle;     // successor to the last live subbundle
   uint16 start_sb_filter;   // first subbundle filter
   uint16 end_sb_filter;     // successor to the last sb filter

   trunk_bundle    bundle[TRUNK_MAX_BUNDLES];
   trunk_subbundle subbundle[TRUNK_MAX_SUBBUNDLES];
   routing_filter  sb_filter[TRUNK_MAX_SUBBUNDLE_FILTERS];
};

/*
 *-----------------------------------------------------------------------------
 * Splinter Pivot Data: Disk-resident structure on Trunk pages
 *
 * A pivot consists of the pivot key (of size cfg.key_size) followed by a
 * trunk_pivot_data. An array of this ( <pivot-key>, <trunk_pivot_data> )
 * pair appears on trunk pages, following the end of struct trunk_hdr{}.
 * This array is sized by configured max_pivot_keys hard-limit.
 *
 * The generation is used by asynchronous processes to determine when a pivot
 * has split
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK trunk_pivot_data {
   uint64 addr;                // PBN of the child
   uint64 num_kv_bytes_whole;  // # kv bytes for this pivot in whole branches
   uint64 num_kv_bytes_bundle; // # kv bytes for this pivot in bundles
   uint64 num_tuples_whole;    // # tuples for this pivot in whole branches
   uint64 num_tuples_bundle;   // # tuples for this pivot in bundles
   uint64 generation;          // receives new higher number when pivot splits
   uint16 start_branch;        // first branch live (not used in leaves)
   uint16 start_bundle;        // first bundle live (not used in leaves)
   routing_filter filter;      // routing filter for keys in this pivot
   int64          srq_idx;     // index in the space rec queue
} trunk_pivot_data;

/*
 *-----------------------------------------------------------------------------
 * Trunk Node Access Wrappers
 *-----------------------------------------------------------------------------
 */

static inline void
trunk_node_get(cache *cc, uint64 addr, trunk_node *node)
{
   debug_assert(addr != 0);
   node->addr = addr;
   node->page = cache_get(cc, node->addr, TRUE, PAGE_TYPE_TRUNK);
   node->hdr = (trunk_hdr *)(node->page->data);
}

static inline void
trunk_node_unget(cache *cc, trunk_node *node)
{
   cache_unget(cc, node->page);
   node->page = NULL;
   node->hdr = NULL;
}

static inline void
trunk_node_claim(cache *cc, trunk_node *node)
{
   uint64 wait = 1;
   while (!cache_claim(cc, node->page)) {
      uint64 addr = node->addr;
      trunk_node_unget(cc, node);
      platform_sleep(wait);
      wait  = wait > 2048 ? wait : 2 * wait;
      trunk_node_get(cc, addr, node);
   }
}

static inline void
trunk_node_unclaim(cache *cc, trunk_node *node)
{
   cache_unclaim(cc, node->page);
}

static inline void
trunk_node_lock(cache *cc, trunk_node *node)
{
   cache_lock(cc, node->page);
   cache_mark_dirty(cc, node->page);
}

static inline void
trunk_node_unlock(cache *cc, trunk_node *node)
{
   cache_unlock(cc, node->page);
}

static inline void
trunk_alloc(cache *cc, mini_allocator *mini, uint64 height, trunk_node *node)
{
   node->addr = mini_alloc(mini, height, NULL_SLICE, NULL);
   debug_assert(node->addr != 0);
   node->page = cache_alloc(cc, node->addr, PAGE_TYPE_TRUNK);
   node->hdr = (trunk_hdr *)(node->page->data);
}

static inline cache_async_result
trunk_node_get_async(cache *cc, uint64 addr, trunk_async_ctxt *ctxt)
{
   return cache_get_async(cc, addr, PAGE_TYPE_TRUNK, &ctxt->cache_ctxt);
}

static inline void
trunk_node_async_done(trunk_handle *spl, trunk_async_ctxt *ctxt)
{
   cache_async_done(spl->cc, PAGE_TYPE_TRUNK, &ctxt->cache_ctxt);
}


/*
 *-----------------------------------------------------------------------------
 * Basic Header Access/Manipulation Functions
 *-----------------------------------------------------------------------------
 */

static inline uint16
trunk_height(trunk_node *node)
{
   return node->hdr->height;
}

static inline bool
trunk_is_leaf(trunk_node *node)
{
   return trunk_height(node) == 0;
}

static inline bool
trunk_is_index(trunk_node *node)
{
   return !trunk_is_leaf(node);
}

static inline uint64
trunk_next_addr(trunk_node *node)
{
   return node->hdr->next_addr;
}

static inline void
trunk_set_next_addr(trunk_node *node, uint64 addr)
{
   node->hdr->next_addr = addr;
}

#endif // __TRUNK_NODE_H
