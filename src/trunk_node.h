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

/*
 * During Splinter configuration, the fanout parameter is provided by the user.
 * SplinterDB defers internal node splitting in order to use hand-over-hand
 * locking. As a result, index nodes may temporarily have more pivots than the
 * fanout. Therefore, the number of pivot keys is over-provisioned by this
 * value.
 */
#define TRUNK_EXTRA_PIVOT_KEYS (6)

#define TRUNK_BUNDLE_END_GENERATION (UINT64_MAX)
/*
 *-----------------------------------------------------------------------------
 * Trunk Bundle:  structure on PAGE_TYPE_TRUNK pages.
 *
 * A flush moves branches from the parent to a bundle in the child. The bundle
 * is then compacted with a compact_bundle job.
 *
 * When a compact_bundle job completes, the branches in the bundle are replaced
 * with the output branch of the compaction. Bundles are then added to the
 * pivot routing filters in order, at which point the bundle structure is
 * dropped and the output branch becomes a whole branch.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK trunk_bundle {
   uint64 generation;
   uint64 num_branches;
   routing_filter filter;
   trunk_branch branch[];
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
   uint16 num_pivot_keys;     // number of used pivot keys (== num_children + 1)
   uint16 height;             // height of the node
   uint64 next_addr;          // PBN of the node's successor (0 if no successor)
   uint64 generation;         // counter incremented on a node split
   uint64 pivot_generation;   // counter incremented when new pivots are added
   uint64 bundle_generration; // counter incremented whenever a bundle is added

   uint64 start_bundle_offset;
   uint64 end_bundle_offset;

   uint64 num_whole_branches;
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
   uint64 start_branch;        // generation number of oldest live branch
   uint64 start_bundle         // generation number of oldest live bundle
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
   node->hdr  = (trunk_hdr *)(node->page->data);
}

static inline void
trunk_node_unget(cache *cc, trunk_node *node)
{
   cache_unget(cc, node->page);
   node->page = NULL;
   node->hdr  = NULL;
}

static inline void
trunk_node_claim(cache *cc, trunk_node *node)
{
   uint64 wait = 1;
   while (!cache_claim(cc, node->page)) {
      uint64 addr = node->addr;
      trunk_node_unget(cc, node);
      platform_sleep(wait);
      wait = wait > 2048 ? wait : 2 * wait;
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
   node->hdr  = (trunk_hdr *)(node->page->data);
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

/*
 *-----------------------------------------------------------------------------
 * Bundle Manipulation and Access Functions
 *-----------------------------------------------------------------------------
 */

/*
 * Return a pointer to the first bundle in a node.
 */
static inline trunk_bundle *
trunk_first_bundle(trunk_node *node)
{
   char *cursor = node->hdr;
   cursor += &node->hdr->start_bundle_offset;
   return (trunk_bundle *)cursor;
}

/*
 * Given a pointer to a bundle, returns a pointer to the next bundle.
 */
static inline trunk_bundle *
trunk_next_bundle(trunk_bundle *bundle)
{
   char *cursor = bundle;
   cursor += sizeof(*bundle) + bundle->num_branches * sizeof(trunk_branch);
   return (trunk_bundle *)cursor;
}

/*
 * Returns a pointer to the bundle if the bundle is still live and NULL otherwise.
 */
static inline trunk_bundle *
trunk_get_bundle(trunk_node *node, uint64 generation)
{
   trunk_bundle *bundle;
   for (bundle = trunk_first_bundle(node);
        bundle->generation != TRUNK_BUNDLE_END_GENERATION;
        bundle = trunk_next_bundle(bundle))
   {
      if (bundle->generation == generation) {
         return bundle;
      }
   }

   // Bundle not found
   return NULL;
}

/*
 * Creates a new bundle in the node with the given routing filter and branch
 * array. Returns the generation number of the bundle in the generation
 * parameters.
 */
static inline platform_status
trunk_create_new_bundle(trunk_handle   *spl,
                        trunk_node     *node,
                        routing_filter *filter,
                        uint64          num_branches,
                        trunk_branch   *branch,
                        uint64         *generation)
{
   trunk_bundle *bundle;
   for (bundle = trunk_first_bundle(node);
        bundle->generation != TRUNK_BUNDLE_END_GENERATION;
        bundle = trunk_next_bundle(bundle))
   {
      // No-op
   }

   uint64 bundle_size = sizeof(trunk_bundle) + num_branches * sizeof(trunk_branch);
   char *cursor = bundle;
   // Used size includes the size of the generation number in the end bundle
   uint64 used_size = cursor - node->page->data + sizeof(uint64);
   if (used_size + bundle_size > trunk_page_size(spl)) {
      *generation = TRUNK_BUNDLE_END_GENERATION;
      return STATUS_NO_SPACE;
   }

   bundle->generation = node->hdr->bundle_generation++;
   bundle->num_branches = num_branches;
   bundle->filter = rf;
   memmove(bundle->branch, branch, num_branches * sizeof(trunk_branch));
   *generation = bundle->generation;

   // Set the past the end bundle generation to TRUNK_BUNDLE_END_GENERATION
   cursor += sizeof(*bundle) + bundle->num_branches * sizeof(trunk_branch);
   bundle = (trunk_bundle *)cursor;
   bundle->generation = TRUNK_BUNDLE_END_GENERATION;

   return STATUS_OK;
}

static inline bool
trunk_remove_bundle(trunk_bundle *bundle)
{
   trunk_bundle *next_bundle = trunk_next_bundle(bundle);





#endif // __TRUNK_NODE_H
