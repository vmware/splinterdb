// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk.c --
 *
 *     This file contains the implementation for SplinterDB.
 */

#include "platform.h"

#include "trunk.h"
#include "btree.h"
#include "memtable.h"
#include "routing_filter.h"
#include "shard_log.h"
#include "merge.h"
#include "task.h"
#include "util.h"
#include "srq.h"

#include "poison.h"

#define LATENCYHISTO_SIZE 15

static const int64 latency_histo_buckets[LATENCYHISTO_SIZE] = {
   1,          // 1   ns
   10,         // 10  ns
   100,        // 100 ns
   500,        // 500 ns
   1000,       // 1   us
   5000,       // 5   us
   10000,      // 10  us
   100000,     // 100 us
   500000,     // 500 us
   1000000,    // 1   ms
   5000000,    // 5   ms
   10000000,   // 10  ms
   100000000,  // 100 ms
   1000000000, // 1   s
   10000000000 // 10  s
};

/*
 * At any time, one Memtable is "active" for inserts / updates.
 * At any time, the most # of Memtables that can be active or in one of these
 * states, such as, compaction, incorporation, reclamation, is given by this
 * limit.
 */
#define TRUNK_NUM_MEMTABLES (4)

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
 * For a "small" range query, you don't want to prefetch pages.
 * This is the minimal # of items requested before we turn ON prefetching.
 * (Empirically established through past experiments, for small key-value
 * pairs. So, _may_ be less efficient in general cases. Needs a revisit.)
 */
#define TRUNK_PREFETCH_MIN (16384)

/* Some randomly chosen Splinter super-block checksum seed. */
#define TRUNK_SUPER_CSUM_SEED (42)

/*
 * During Splinter configuration, the fanout parameter is provided by the user.
 * SplinterDB defers internal node splitting in order to use hand-over-hand
 * locking. As a result, index nodes may temporarily have more pivots than the
 * fanout. Therefore, the number of pivot keys is over-provisioned by this
 * value.
 */
#define TRUNK_EXTRA_PIVOT_KEYS (6)

/*
 * Trunk logging functions.
 *
 * If verbose_logging_enabled is enabled in trunk_config, these functions print
 * to cfg->log_handle.
 */

static inline bool32
trunk_verbose_logging_enabled(trunk_handle *spl)
{
   return spl->cfg.verbose_logging_enabled;
}

static inline platform_log_handle *
trunk_log_handle(trunk_handle *spl)
{
   platform_assert(trunk_verbose_logging_enabled(spl));
   platform_assert(spl->cfg.log_handle != NULL);
   return spl->cfg.log_handle;
}

static inline platform_status
trunk_open_log_stream_if_enabled(trunk_handle           *spl,
                                 platform_stream_handle *stream)
{
   if (trunk_verbose_logging_enabled(spl)) {
      return platform_open_log_stream(stream);
   }
   return STATUS_OK;
}

static inline void
trunk_close_log_stream_if_enabled(trunk_handle           *spl,
                                  platform_stream_handle *stream)
{
   if (trunk_verbose_logging_enabled(spl)) {
      platform_assert(stream != NULL);
      platform_close_log_stream(stream, trunk_log_handle(spl));
   }
}

#define trunk_log_stream_if_enabled(spl, _stream, message, ...)                \
   do {                                                                        \
      if (trunk_verbose_logging_enabled(spl)) {                                \
         platform_log_stream(                                                  \
            (_stream), "[%3lu] " message, platform_get_tid(), ##__VA_ARGS__);  \
      }                                                                        \
   } while (0)

#define trunk_default_log_if_enabled(spl, message, ...)                        \
   do {                                                                        \
      if (trunk_verbose_logging_enabled(spl)) {                                \
         platform_default_log(message, __VA_ARGS__);                           \
      }                                                                        \
   } while (0)

/*
 *-----------------------------------------------------------------------------
 * SplinterDB Structure:
 *
 *       SplinterDB is a size-tiered Be-tree. It has a superstructure called
 *       the trunk tree, which consists of trunk nodes. Each trunk node
 *       contains pointers to a collection of branches. Each branch is a B-tree
 *       which stores key-value pairs (tuples). All the actual data is stored
 *       in the branches, and the trunk indexes and organizes the data.
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 * Substructures:
 *
 *       B-trees:
 *          SplinterDB makes use of B-trees, which come in two flavors, dynamic
 *          and static.
 *
 *          dynamic: Dynamic B-trees are used in the memtable (see
 *             below) and are mutable B-trees, supporting
 *             insertions. The mutable operations on B-trees must use
 *             a btree_dynamic_handle.
 *
 *          static: Static B-trees are used as branches and are
 *             immutable. Static btrees are accessed
 *             using their root_addr, which is thinly wrapped using
 *             their root_addr, which is thinly wrapped using
 *             btree_static_handle.
 *-----------------------------------------------------------------------------
 */


/*
 *-----------------------------------------------------------------------------
 * Insertion Path:
 *
 *       Memtable Insertions are first inserted into a memtable, which
 *          is a dynamic btree. SplinterDB uses
 *          multiple memtables so that when one memtable fills,
 *          insertions can continue into another memtable while the
 *          first is incorporated.
 *
 *          As part of this process, the generation number of the leaf into
 *          which the new tuple is placed is returned and stored in the log (if
 *          used) in order to establish a per-key temporal ordering.  The
 *          memtable also keeps a list of fingerprints, fp_arr, which are used
 *          to build the filter when the memtable becomes a branch.
 *
 *       Incorporation When the memtable fills, it is incorporated
 *          into the root node. The memtable locks itself to inserts
 *          (but not lookups), Splinter switches the active memtable,
 *          then the filter is built from the fp_arr, and the
 *          btree in the memtable is inserted into the
 *          root as a new (distinct) branch.  Then the memtable is
 *          reinitialized with a new (empty) btree and unlocked.
 *
 *       Flushing
 *          A node is considered full when it has max_tuples_per_node tuples
 *          (set to be fanout * memtable_capacity) or when it has
 *          max_branches_per_node branches. The first condition ensures that
 *          data moves down the tree and the second limits the number of
 *          branches on a root-to-leaf path and therefore the worst-case lookup
 *          cost.
 *
 *          When a node fills, a flush is initiated to each pivot (child) of
 *          the node which has at least max_branches_per_node live branches. If
 *          the node is still full, it picks the pivot which has the most
 *          tuples and flushes to that child and repeats this process until the
 *          node is no longer full.
 *
 *          A flush consists of flushing all the branches which are live for
 *          the pivot into a bundle in the child. A bundle is a contiguous
 *          range of branches in a trunk node, see trunk node documentation
 *          below. A flush to a given pivot makes all branches and bundles in
 *          the parent no longer "live" for that pivot.
 *
 *       Compaction (after flush)
 *          After a flush completes, a compact_bundle job is issued for the
 *          bundle which was created. This job first checks if the node is full
 *          and if so flushes until it is no longer full. Then it compacts all
 *          the tuples in the bundle which are live for the node (are within
 *          the node's key range and have not been flushed), and replaces the
 *          bundle with the resulting compacted branch.
 *
 *       Split (internal)
 *          During a flush, if the child has more pivots than the configured
 *          fanout, it is split. Note that pivots are added at other times (to
 *          the parent of an internal or leaf split), so nodes may
 *          temporarily exceed the fanout. Splits are not initiated then,
 *          because the hand-over-hand locking protocol means that the lock of
 *          the grandparent is not held and it is awkward for try to acquire
 *          locks going up the tree.
 *
 *          An internal node split is a logical split: the trunk node is
 *          copied, except the first (fanout/2) pivots become the pivots of
 *          the left node and the remaining pivots become the right node. No
 *          compaction is initiated, and the branches and bundles of the node
 *          pre-split are shared between the new left and right nodes.
 *
 *       Split (leaf)
 *          When a leaf has more than cfg->max_tuples_per_node (fanout *
 *          memtable_capacity), it is considered full.
 *
 *          When a leaf is full, it is split logically: new pivots are
 *          calculated, new leaves are created with those pivots as min/max
 *          keys, and all the branches in the leaf at the time of the split are
 *          shared between them temporarily as a single bundle in each.  This
 *          split happens synchronously with the flush.
 *
 *          A compact_bundle job is issued for each new leaf, which
 *          asynchronously compacts the shared branches into a single unshared
 *          branch with the tuples from each new leaf's range.
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 * Interactions between Concurrent Processes
 *
 *       The design of SplinterDB allows flushes, compactions, internal node
 *       split and leaf splits to happen concurrently, even within the same
 *       node. The ways in which these processes can interact are detailed
 *       here.
 *
 *  o Flushes and compactions:
 *
 *       1. While a compaction has been scheduled or is in process, a flush may
 *          occur. This will flush the bundle being compacted to the child and
 *          the in-progress compaction will continue as usual. Note that the
 *          tuples which are flushed will still be compacted if the compaction
 *          is in progress, which results in some wasted work.
 *       2. As a result of 1., while a compaction has been scheduled, its
 *          bundle may be flushed to all children, so that it is no longer
 *          live. In this case, when the compact_bundle job initiates, it
 *          detects that the bundle is not live and aborts before compaction.
 *       3. Similarly, if the bundle for an in-progress compaction is flushed
 *          to all children, when it completes, it will detect that the bundle
 *          is no longer live and it will discard the output.
 *
 *  o Flushes and internal/leaf splits:
 *
 *          Flushes and internal/leaf splits are synchronous and do not
 *          interact.
 *
 *  o Internal splits and compaction:
 *
 *       4. If an internal split occurs in a node which has a scheduled
 *          compaction, when the compact_bundle job initiates it will detect
 *          the node split using the node's generation number
 *          (hdr->generation). It then creates a separate compact_bundle job on
 *          the new sibling.
 *       5. If an internal split occurs in a node with an in-progress
 *          compaction, the bundle being compacted is copied to the new
 *          sibling.  When the compact_bundle job finishes compaction and
 *          fetches the node to replace the bundle, the node split is detected
 *          using the generation number, and the bundle is replaced in the new
 *          sibling as well. Note that the output of the compaction will
 *          contain tuples for both the node and its new sibling.
 *
 *  o Leaf splits and compaction:
 *
 *       6. If a compaction is scheduled or in progress when a leaf split
 *          triggers, the leaf split will start its own compaction job on the
 *          bundle being compacted. When the compaction job initiates or
 *          finishes, it will detect the leaf split using the generation number
 *          of the leaf, and abort.
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 * Trunk Nodes: splinter trunk_hdr{}: Disk-resident structure
 *
 *   A trunk node, on pages of PAGE_TYPE_TRUNK type, consists of the following:
 *
 *       Header
 *          meta data
 *       ---------
 *       Array of bundles
 *          When a collection of branches are flushed into a node, they are
 *          organized into a bundle. This bundle will be compacted into a
 *          single branch by a call to trunk_compact_bundle. Bundles are
 *          implemented as a collection of subbundles, each of which covers a
 *          range of branches.
 *       ----------
 *       Array of subbundles
 *          A subbundle consists of the branches from a single ancestor (really
 *          that ancestor's pivot). During a flush, all the whole branches in
 *          the parent are collected into a subbundle in the child and any
 *          subbundles in the parent are copied to the child.
 *
 *          Subbundles function properly in the current design, but are not
 *          used for anything. They are going to be used for routing filters.
 *       ----------
 *       Array of pivots: Each node has a pivot corresponding to each
 *          child as well as an additional last pivot which contains
 *          an exclusive upper bound key for the node. Each pivot has
 *          a key which is an inclusive lower bound for the keys in
 *          its child node (as well as the btree
 *          rooted there). This means that the key for the 0th pivot
 *          is an inclusive lower bound for all keys in the node.
 *          Each pivot also has its own start_branch, which is used to
 *          determine which branches have tuples for that pivot (the
 *          range start_branch to end_branch).
 *
 *          Each pivot's key is accessible via a call to trunk_get_pivot() and
 *          the remaining data is accessible via a call to
 *          trunk_get_pivot_data().
 *
 *          The number of pivots on a trunk page has two different limits:
 *           - A user-configurable static soft limit (fanout)
 *           - An internally determined hard limit (max_pivot_keys), based on
 *             the specified 'fanout' setting.
 *
 *          When the soft limit is reached, it will cause the node to split the
 *          next time it is flushed into (see internal node splits above).
 *          Note that multiple pivots can be added to the parent of a leaf
 *          during a split and multiple splits could theoretically occur before
 *          the node is flushed into again, so the fanout limit may temporarily
 *          be exceeded by multiple pivots.
 *
 *          The hard limit is the amount of physical space in the node which can
 *          be used for pivots and cannot be exceeded.
 *
 *  Limits: The default fanout is 8 and the hard limit is 3x the fanout. Note
 *          that the additional last pivot (containing the exclusive upper
 *          bound to the node) counts towards the hard limit (because it uses
 *          physical space), but not the soft limit.
 *       ----------
 *       Array of branches
 *          Whole branches: The branches from hdr->start_branch to
 *             hdr->start_frac_branch are "whole" branches, each of which is
 *             the output of a compaction or incorporation.
 *          Fractional branches: From hdr->start_frac_branch to hdr->end_branch
 *             are "fractional" branches that are part of bundles and are in
 *             the process of being compacted into whole branches.
 *
 *          Logically, each whole branch and each bundle counts toward the
 *          number of branches in the node (or pivot), since each bundle
 *          represents a single branch after compaction.
 *
 *          There are two limits on the number of branches in a node. The soft
 *          limit (max_branches_per_node) refers to logical branches (each
 *          whole branch and each bundle counts as a logical branch), and when
 *          there are more logical branches than the soft limit, the node is
 *          considered full and flushed until there are fewer branches than the
 *          soft limit. The hard limit (hard_max_branches_per_node) is the
 *          number of branches (whole and fractional) for which there is
 *          physical room in the node, and as a result cannot be exceeded. An
 *          attempt to flush _into_ a node which is at the hard limit will fail.
 *-----------------------------------------------------------------------------
 */


/*
 *-----------------------------------------------------------------------------
 * structs
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 * Splinter Super Block: Disk-resident structure.
 * Super block lives on page of page type == PAGE_TYPE_SUPERBLOCK.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK trunk_super_block {
   uint64 root_addr; // Address of the root of the trunk for the instance
                     // referenced by this superblock.
   uint64      next_node_id;
   uint64      meta_tail;
   uint64      log_addr;
   uint64      log_meta_addr;
   uint64      timestamp;
   bool32      checkpointed;
   bool32      unmounted;
   checksum128 checksum;
} trunk_super_block;

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
typedef struct ONDISK trunk_hdr {
   uint64 node_id;
   uint16 num_pivot_keys;   // number of used pivot keys (== num_children + 1)
   uint16 height;           // height of the node
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
} trunk_hdr;

/*
 *-----------------------------------------------------------------------------
 * Splinter Pivot Data: Disk-resident structure on Trunk pages
 *
 * A trunk_pivot_data struct consists of the trunk_pivot_data header
 * followed by cfg.max_key_size bytes of space for the pivot key.  An
 * array of trunk_pivot_datas appears on trunk pages, following the
 * end of struct trunk_hdr{}. This array is sized by configured
 * max_pivot_keys hard-limit.
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
   ondisk_key     pivot;
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
   while (!cache_try_claim(cc, node->page)) {
      uint64 addr = node->addr;
      trunk_node_unget(cc, node);
      platform_sleep_ns(wait);
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
   node->addr = mini_alloc(mini, height, NULL);
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
trunk_node_height(trunk_node *node)
{
   return node->hdr->height;
}

static inline bool32
trunk_node_is_leaf(trunk_node *node)
{
   return trunk_node_height(node) == 0;
}

/*
 *-----------------------------------------------------------------------------
 * Compaction Requests
 *-----------------------------------------------------------------------------
 */

/*
 * Used to specify compaction bundle "task" request. These enums specify
 * the compaction bundle request type. (Not disk-resident.)
 */
typedef enum trunk_compaction_type {
   TRUNK_COMPACTION_TYPE_INVALID = 0,
   TRUNK_COMPACTION_TYPE_MEMTABLE,
   TRUNK_COMPACTION_TYPE_FLUSH,
   TRUNK_COMPACTION_TYPE_LEAF_SPLIT,
   TRUNK_COMPACTION_TYPE_SINGLE_LEAF_SPLIT,
   TRUNK_COMPACTION_TYPE_SPACE_REC,
   NUM_TRUNK_COMPACTION_TYPES,
} trunk_compaction_type;

/*
 *-----------------------------------------------------------------------------
 * Compact Bundle Requests
 *
 * Arguments and scratch space to compact bundle number <bundle_no> in the
 * node[s] at height <height> spanning the range [start_key, end_key). <addr>
 * is the address of the node where the bundle was initially created, and is
 * used to revisit the node to create the iterators which become inputs to the
 * compaction.
 *-----------------------------------------------------------------------------
 */
struct trunk_compact_bundle_req {

   // Inputs
   trunk_handle         *spl;
   uint64                addr;
   key_buffer            start_key;
   key_buffer            end_key;
   uint64                node_id;
   uint16                height;
   uint16                bundle_no;
   trunk_compaction_type type;

   // Computed as part of the compaction process
   uint64  pivot_generation[TRUNK_MAX_PIVOTS];
   uint64  max_pivot_generation;
   uint64  input_pivot_tuple_count[TRUNK_MAX_PIVOTS];
   uint64  output_pivot_tuple_count[TRUNK_MAX_PIVOTS];
   uint64  input_pivot_kv_byte_count[TRUNK_MAX_PIVOTS];
   uint64  output_pivot_kv_byte_count[TRUNK_MAX_PIVOTS];
   uint64  tuples_reclaimed;
   uint64  kv_bytes_reclaimed;
   uint32 *fp_arr;
};

// for for_each_node
typedef bool32 (*node_fn)(trunk_handle *spl, uint64 addr, void *arg);

// Used by trunk_compact_bundle()
typedef struct {
   iterator  *itor_arr[TRUNK_RANGE_ITOR_MAX_BRANCHES];
   uint64     num_saved_pivot_keys;
   key_buffer saved_pivot_keys[TRUNK_MAX_PIVOTS];
   key_buffer req_original_start_key;
} compact_bundle_scratch;

/*
 * Union of various data structures that can live on the per-thread
 * scratch memory provided by the task subsystem and are needed by
 * splinter's task dispatcher routines.
 */
typedef union {
   compact_bundle_scratch compact_bundle;
} trunk_task_scratch;

/*
 *-----------------------------------------------------------------------------
 * Trunk Handle
 *-----------------------------------------------------------------------------
 */

static inline data_config *
trunk_data_config(trunk_handle *spl)
{
   return spl->cfg.data_cfg;
}

static inline uint64
trunk_page_size(const trunk_config *cfg)
{
   return cache_config_page_size(cfg->cache_cfg);
}

static inline uint64
trunk_pages_per_extent(const trunk_config *cfg)
{
   return cache_config_pages_per_extent(cfg->cache_cfg);
}

static inline uint16
trunk_tree_height(trunk_handle *spl)
{
   trunk_node root;
   trunk_node_get(spl->cc, spl->root_addr, &root);
   uint16 tree_height = trunk_node_height(&root);
   trunk_node_unget(spl->cc, &root);
   return tree_height;
}

static uint64
trunk_hdr_size()
{
   return sizeof(trunk_hdr);
}

/*
 * Returns the number of children of the node
 */
static inline uint16
trunk_num_children(trunk_handle *spl, trunk_node *node)
{
   debug_assert(node->hdr->num_pivot_keys >= 2);
   return node->hdr->num_pivot_keys - 1;
}

/*
 * Returns the number of pivot keys in the node. This is equal to the number of
 * children + 1 for the upper bound pivot key.
 */
static inline uint16
trunk_num_pivot_keys(trunk_handle *spl, trunk_node *node)
{
   debug_assert(node->hdr->num_pivot_keys >= 2);
   return node->hdr->num_pivot_keys;
}

static inline uint16
trunk_start_branch(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->start_branch;
}

static inline uint16
trunk_end_branch(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->end_branch;
}


/*
 *-----------------------------------------------------------------------------
 * Super block functions
 *-----------------------------------------------------------------------------
 */
static void
trunk_set_super_block(trunk_handle *spl,
                      bool32        is_checkpoint,
                      bool32        is_unmount,
                      bool32        is_create)
{
   uint64             super_addr;
   page_handle       *super_page;
   trunk_super_block *super;
   uint64             wait = 1;
   platform_status    rc;

   if (is_create) {
      rc = allocator_alloc_super_addr(spl->al, spl->id, &super_addr);
   } else {
      rc = allocator_get_super_addr(spl->al, spl->id, &super_addr);
   }
   platform_assert_status_ok(rc);
   super_page = cache_get(spl->cc, super_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   while (!cache_try_claim(spl->cc, super_page)) {
      platform_sleep_ns(wait);
      wait *= 2;
   }
   wait = 1;
   cache_lock(spl->cc, super_page);

   super = (trunk_super_block *)super_page->data;
   if (spl->trunk_context.root != NULL) {
      super->root_addr = spl->trunk_context.root->addr;
   } else {
      super->root_addr = 0;
   }
   super->meta_tail = mini_meta_tail(&spl->mini);
   if (spl->cfg.use_log) {
      if (spl->log) {
         super->log_addr      = log_addr(spl->log);
         super->log_meta_addr = log_meta_addr(spl->log);
      } else {
         super->log_addr      = 0;
         super->log_meta_addr = 0;
      }
   }
   super->timestamp    = platform_get_real_time();
   super->checkpointed = is_checkpoint;
   super->unmounted    = is_unmount;
   super->checksum =
      platform_checksum128(super,
                           sizeof(trunk_super_block) - sizeof(checksum128),
                           TRUNK_SUPER_CSUM_SEED);

   cache_mark_dirty(spl->cc, super_page);
   cache_unlock(spl->cc, super_page);
   cache_unclaim(spl->cc, super_page);
   cache_unget(spl->cc, super_page);
   cache_page_sync(spl->cc, super_page, TRUE, PAGE_TYPE_SUPERBLOCK);
}

static trunk_super_block *
trunk_get_super_block_if_valid(trunk_handle *spl, page_handle **super_page)
{
   uint64             super_addr;
   trunk_super_block *super;

   platform_status rc = allocator_get_super_addr(spl->al, spl->id, &super_addr);
   platform_assert_status_ok(rc);
   *super_page = cache_get(spl->cc, super_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   super       = (trunk_super_block *)(*super_page)->data;

   if (!platform_checksum_is_equal(
          super->checksum,
          platform_checksum128(super,
                               sizeof(trunk_super_block) - sizeof(checksum128),
                               TRUNK_SUPER_CSUM_SEED)))
   {
      cache_unget(spl->cc, *super_page);
      *super_page = NULL;
      return NULL;
   }

   return super;
}

static void
trunk_release_super_block(trunk_handle *spl, page_handle *super_page)
{
   cache_unget(spl->cc, super_page);
}

/*
 *-----------------------------------------------------------------------------
 * Circular Buffer Arithmetic
 *
 *       X_add and X_sub add or subtract the offset in the arithmetic of the
 *       circular buffer for X.
 *
 *       X_in_range returns TRUE if the given index is in the range [start,
 *       end] in the circular buffer for X.
 *-----------------------------------------------------------------------------
 */

static inline uint16
trunk_add_branch_number(trunk_handle *spl, uint16 branch_no, uint16 offset)
{
   return (branch_no + offset) % spl->cfg.hard_max_branches_per_node;
}

static inline uint16
trunk_subtract_branch_number(trunk_handle *spl, uint16 branch_no, uint16 offset)
{
   return (branch_no + spl->cfg.hard_max_branches_per_node - offset)
          % spl->cfg.hard_max_branches_per_node;
}

static inline uint16
trunk_subtract_bundle_number(trunk_handle *spl, uint16 start, uint16 end)
{
   return (start + TRUNK_MAX_BUNDLES - end) % TRUNK_MAX_BUNDLES;
}

static inline bool32
trunk_bundle_in_range(trunk_handle *spl,
                      uint16        bundle_no,
                      uint16        start,
                      uint16        end)
{
   return trunk_subtract_bundle_number(spl, bundle_no, start)
          < trunk_subtract_bundle_number(spl, end, start);
}

static inline uint16
trunk_subtract_subbundle_number(trunk_handle *spl, uint16 start, uint16 end)
{
   return (start + TRUNK_MAX_SUBBUNDLES - end) % TRUNK_MAX_SUBBUNDLES;
}

static inline uint16
trunk_add_subbundle_filter_number(trunk_handle *spl, uint16 start, uint16 end)
{
   return (start + end) % TRUNK_MAX_SUBBUNDLE_FILTERS;
}

/*
 *-----------------------------------------------------------------------------
 * Bundle functions
 *-----------------------------------------------------------------------------
 */

static inline uint16
trunk_start_bundle(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->start_bundle;
}

static inline uint16
trunk_end_bundle(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->end_bundle;
}

/*
 * Returns TRUE if the bundle is live in the node and FALSE otherwise.
 */
static inline bool32
trunk_bundle_live(trunk_handle *spl, trunk_node *node, uint16 bundle_no)
{
   return trunk_bundle_in_range(spl,
                                bundle_no,
                                trunk_start_bundle(spl, node),
                                trunk_end_bundle(spl, node));
}

static inline trunk_bundle *
trunk_get_bundle(trunk_handle *spl, trunk_node *node, uint16 bundle_no)
{
   debug_assert(trunk_bundle_live(spl, node, bundle_no),
                "Attempt to get a dead bundle.\n"
                "addr: %lu, bundle_no: %u, start_bundle: %u, end_bundle: %u\n",
                node->addr,
                bundle_no,
                trunk_start_bundle(spl, node),
                trunk_end_bundle(spl, node));
   return &node->hdr->bundle[bundle_no];
}

static inline trunk_subbundle *
trunk_get_subbundle(trunk_handle *spl, trunk_node *node, uint16 subbundle_no)
{
   return &node->hdr->subbundle[subbundle_no];
}

static inline routing_filter *
trunk_get_sb_filter(trunk_handle *spl, trunk_node *node, uint16 filter_no)
{
   debug_assert(filter_no < TRUNK_MAX_SUBBUNDLE_FILTERS,
                "filter_no=%u should be < TRUNK_MAX_SUBBUNDLE_FILTERS (%u)",
                filter_no,
                TRUNK_MAX_SUBBUNDLE_FILTERS);
   return &node->hdr->sb_filter[filter_no];
}

static inline uint16
trunk_start_sb_filter(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->start_sb_filter;
}

static inline uint16
trunk_end_sb_filter(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->end_sb_filter;
}

static inline uint16
trunk_subbundle_filter_count(trunk_handle    *spl,
                             trunk_node      *node,
                             trunk_subbundle *sb)
{
   return trunk_subtract_subbundle_number(
      spl, sb->end_filter, sb->start_filter);
}

static inline routing_filter *
trunk_subbundle_filter(trunk_handle    *spl,
                       trunk_node      *node,
                       trunk_subbundle *sb,
                       uint16           filter_off)
{
   uint16 start_filter = sb->start_filter;
   uint16 filter_no =
      trunk_add_subbundle_filter_number(spl, start_filter, filter_off);
   debug_assert(filter_off < trunk_subbundle_filter_count(spl, node, sb));
   return trunk_get_sb_filter(spl, node, filter_no);
}

debug_only static inline uint16
trunk_subbundle_branch_count(trunk_handle    *spl,
                             trunk_node      *node,
                             trunk_subbundle *sb)
{
   return trunk_subtract_branch_number(spl, sb->end_branch, sb->start_branch);
}

static inline uint16
trunk_end_subbundle(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->end_subbundle;
}

static inline uint16
trunk_start_subbundle_for_lookup(trunk_handle *spl, trunk_node *node)
{
   return trunk_subtract_subbundle_number(
      spl, trunk_end_subbundle(spl, node), 1);
}

/*
 *-----------------------------------------------------------------------------
 * Pivot functions
 *-----------------------------------------------------------------------------
 */

/*
 * A pivot consists of cfg.key_size bytes of space for the pivot key, followed
 * by a struct trunk_pivot_data. Return the total size of a pivot.
 */
static uint64
trunk_pivot_size(trunk_handle *spl)
{
   return sizeof(trunk_pivot_data) + trunk_max_key_size(spl);
}

static inline trunk_pivot_data *
trunk_get_pivot_data(trunk_handle *spl, trunk_node *node, uint16 pivot_no)
{
   return (trunk_pivot_data *)(((char *)node->hdr) + sizeof(*node->hdr)
                               + pivot_no * trunk_pivot_size(spl));
}

static inline key
trunk_get_pivot(trunk_handle *spl, trunk_node *node, uint16 pivot_no)
{
   platform_assert((pivot_no < spl->cfg.max_pivot_keys),
                   "pivot_no = %d, cfg.max_pivot_keys = %lu",
                   pivot_no,
                   spl->cfg.max_pivot_keys);
   trunk_pivot_data *pdata = trunk_get_pivot_data(spl, node, pivot_no);
   return ondisk_key_to_key(&pdata->pivot);
}

static inline void
trunk_set_num_pivot_keys(trunk_handle *spl,
                         trunk_node   *node,
                         uint16        num_pivot_keys)
{
   debug_assert(num_pivot_keys >= 2);
   debug_assert(num_pivot_keys <= spl->cfg.max_pivot_keys);
   node->hdr->num_pivot_keys = num_pivot_keys;
}

static inline void
trunk_set_initial_pivots(trunk_handle *spl, trunk_node *node)
{
   trunk_set_num_pivot_keys(spl, node, 2);

   trunk_pivot_data *pdata = trunk_get_pivot_data(spl, node, 0);
   ZERO_CONTENTS(pdata);
   pdata->srq_idx = -1;
   copy_key_to_ondisk_key(&pdata->pivot, NEGATIVE_INFINITY_KEY);

   pdata = trunk_get_pivot_data(spl, node, 1);
   ZERO_CONTENTS(pdata);
   copy_key_to_ondisk_key(&pdata->pivot, POSITIVE_INFINITY_KEY);
}

static inline uint64
trunk_inc_pivot_generation(trunk_handle *spl, trunk_node *node)
{
   return node->hdr->pivot_generation++;
}

static inline void
trunk_set_pivot_data_new_root(trunk_handle *spl,
                              trunk_node   *node,
                              uint64        child_addr)
{
   debug_assert(trunk_node_height(node) != 0);
   trunk_pivot_data *pdata = trunk_get_pivot_data(spl, node, 0);

   pdata->addr                = child_addr;
   pdata->num_tuples_whole    = 0;
   pdata->num_kv_bytes_whole  = 0;
   pdata->num_tuples_bundle   = 0;
   pdata->num_kv_bytes_bundle = 0;
   pdata->start_branch        = trunk_start_branch(spl, node);
   pdata->start_bundle        = trunk_end_bundle(spl, node);
   ZERO_STRUCT(pdata->filter);
}

/*
 * Used by find_pivot
 */
static inline uint32
lowerbound(uint32 size)
{
   if (size <= 1)
      return 0;
   return (8 * sizeof(uint32)) - __builtin_clz(size - 1);
}

/*
 * Used by find_pivot
 */
static inline void
trunk_update_lowerbound(uint16 *lo, uint16 *mid, int cmp, comparison comp)
{
   switch (comp) {
      case less_than:
      case greater_than_or_equal:
         if (cmp < 0)
            *lo = *mid;
         break;
      case less_than_or_equal:
      case greater_than:
         if (cmp <= 0)
            *lo = *mid;
         break;
      default:
         platform_assert(0);
   }
}

/*
 * find_pivot performs a binary search for the extremal pivot that satisfies
 * comp, e.g. if comp == greater_than, find_pivot finds the smallest pivot
 * which is greater than key. It returns the found pivot's index.
 */
static inline uint16
trunk_find_pivot(trunk_handle *spl,
                 trunk_node   *node,
                 key           target,
                 comparison    comp)
{
   debug_assert(node != NULL);
   uint16 lo_idx = 0, mid_idx;
   uint32 i;
   int    cmp;
   uint32 size = trunk_num_children(spl, node);

   if (size == 0) {
      return 0;
   }

   if (size == 1) {
      cmp = trunk_key_compare(spl, trunk_get_pivot(spl, node, 0), target);
      switch (comp) {
         case less_than:
            debug_assert(cmp < 0);
            return 0;
         case less_than_or_equal:
            debug_assert(cmp <= 0,
                         "cmp=%d, key=%s",
                         cmp,
                         key_string(spl->cfg.data_cfg, target));
            return 0;
         case greater_than:
            return cmp > 0 ? 0 : 1;
         case greater_than_or_equal:
            return cmp >= 0 ? 0 : 1;
         default:
            platform_assert(0);
      }
   }

   // binary search for the pivot
   mid_idx = size - (1u << (lowerbound(size) - 1));
   size    = 1u << (lowerbound(size) - 1);
   cmp = trunk_key_compare(spl, trunk_get_pivot(spl, node, mid_idx), target);
   trunk_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);

   for (i = lowerbound(size); i != 0; i--) {
      size /= 2;
      mid_idx = lo_idx + size;
      cmp = trunk_key_compare(spl, trunk_get_pivot(spl, node, mid_idx), target);
      trunk_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);
   }

   switch (comp) {
      case less_than:
      case less_than_or_equal:
         return lo_idx;
      case greater_than:
      case greater_than_or_equal:
         return lo_idx + 1;
      default:
         platform_assert(0);
         return (0);
   }
}

/*
 * branch_live_for_pivot returns TRUE if the branch is live for the pivot and
 * FALSE otherwise.
 */
static inline bool32
trunk_branch_live_for_pivot(trunk_handle *spl,
                            trunk_node   *node,
                            uint64        branch_no,
                            uint16        pivot_no)
{
   trunk_pivot_data *pdata = trunk_get_pivot_data(spl, node, pivot_no);
   return trunk_subtract_branch_number(spl, branch_no, pdata->start_branch)
          < trunk_subtract_branch_number(
             spl, node->hdr->end_branch, pdata->start_branch);
}

static void
trunk_add_pivot_new_root(trunk_handle *spl,
                         trunk_node   *parent,
                         trunk_node   *child)
{
   trunk_set_initial_pivots(spl, parent);
   uint64 child_addr = child->addr;
   trunk_set_pivot_data_new_root(spl, parent, child_addr);
}

static inline uint16
trunk_pivot_start_subbundle(trunk_handle     *spl,
                            trunk_node       *node,
                            trunk_pivot_data *pdata)
{
   if (pdata->start_bundle == trunk_end_bundle(spl, node)) {
      return trunk_end_subbundle(spl, node);
   }
   trunk_bundle *bundle = trunk_get_bundle(spl, node, pdata->start_bundle);
   return bundle->start_subbundle;
}

static inline uint16
trunk_pivot_end_subbundle_for_lookup(trunk_handle     *spl,
                                     trunk_node       *node,
                                     trunk_pivot_data *pdata)
{
   return trunk_subtract_subbundle_number(
      spl, trunk_pivot_start_subbundle(spl, node, pdata), 1);
}

/*
 *-----------------------------------------------------------------------------
 * Higher-level Branch and Bundle Functions
 *-----------------------------------------------------------------------------
 */
static bool32
trunk_for_each_subtree(trunk_handle *spl, uint64 addr, node_fn func, void *arg)
{
   // func may be deallocation, so first apply to subtree
   trunk_node node;
   trunk_node_get(spl->cc, addr, &node);
   if (!trunk_node_is_leaf(&node)) {
      uint16 num_children = trunk_num_children(spl, &node);
      for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
         trunk_pivot_data *pdata = trunk_get_pivot_data(spl, &node, pivot_no);
         bool32            succeeded_on_subtree =
            trunk_for_each_subtree(spl, pdata->addr, func, arg);
         if (!succeeded_on_subtree) {
            goto failed_on_subtree;
         }
      }
   }
   trunk_node_unget(spl->cc, &node);
   return func(spl, addr, arg);

failed_on_subtree:
   trunk_node_unget(spl->cc, &node);
   return FALSE;
}

/*
 * trunk_for_each_node() is an iterator driver function to walk through all
 * nodes in a Splinter tree, and to execute the work-horse 'func' function on
 * each node.
 *
 * Returns: TRUE, if 'func' was successful on all nodes. FALSE, otherwise.
 */
static bool32
trunk_for_each_node(trunk_handle *spl, node_fn func, void *arg)
{
   return trunk_for_each_subtree(spl, spl->root_addr, func, arg);
}


/*
 *-----------------------------------------------------------------------------
 * Branch functions
 *-----------------------------------------------------------------------------
 */

/*
 * has_vacancy returns TRUE unless there is not enough physical space in the
 * node to add another branch
 */

static inline trunk_branch *
trunk_get_branch(trunk_handle *spl, trunk_node *node, uint32 k)
{
   debug_assert(sizeof(trunk_hdr)
                   + spl->cfg.max_pivot_keys * trunk_pivot_size(spl)
                   + (k + 1) * sizeof(trunk_branch)
                < trunk_page_size(&spl->cfg));

   char *cursor = node->page->data;
   cursor += sizeof(trunk_hdr) + spl->cfg.max_pivot_keys * trunk_pivot_size(spl)
             + k * sizeof(trunk_branch);
   return (trunk_branch *)cursor;
}

static inline void
trunk_zap_branch_range(trunk_handle *spl,
                       trunk_branch *branch,
                       key           start_key,
                       key           end_key,
                       page_type     type)
{
   platform_assert(type == PAGE_TYPE_BRANCH);
   platform_assert((key_is_null(start_key) && key_is_null(end_key))
                   || (type != PAGE_TYPE_MEMTABLE && !key_is_null(start_key)));
   platform_assert(branch->root_addr != 0, "root_addr=%lu", branch->root_addr);
   btree_dec_ref(
      spl->cc, &spl->cfg.btree_cfg, branch->root_addr, PAGE_TYPE_BRANCH);
}

/*
 *-----------------------------------------------------------------------------
 * trunk_btree_lookup_async
 *
 * Pre-conditions:
 *    The ctxt should've been initialized using
 *    btree_ctxt_init(). If *found `data` has the most
 *    recent answer. the current memtable is older than the most
 *    recent answer
 *
 *    The return value can be either of:
 *      async_locked: A page needed by lookup is locked. User should retry
 *      request.
 *      async_no_reqs: A page needed by lookup is not in cache and the IO
 *      subsystem is out of requests. User should throttle.
 *      async_io_started: Async IO was started to read a page needed by the
 *      lookup into the cache. When the read is done, caller will be notified
 *      using ctxt->cb, that won't run on the thread context. It can be used
 *      to requeue the async lookup request for dispatch in thread context.
 *      When it's requeued, it must use the same function params except found.
 *      success: *found is TRUE if found, FALSE otherwise, data is stored in
 *      *data_out
 *-----------------------------------------------------------------------------
 */
static cache_async_result
trunk_btree_lookup_and_merge_async(trunk_handle      *spl,    // IN
                                   trunk_branch      *branch, // IN
                                   key                target, // IN
                                   merge_accumulator *data,   // OUT
                                   btree_async_ctxt  *ctxt)    // IN
{
   cache             *cc  = spl->cc;
   btree_config      *cfg = &spl->cfg.btree_cfg;
   cache_async_result res;
   bool32             local_found;

   res = btree_lookup_and_merge_async(
      cc, cfg, branch->root_addr, target, data, &local_found, ctxt);
   return res;
}


/*
 *-----------------------------------------------------------------------------
 * Memtable Functions
 *-----------------------------------------------------------------------------
 */

static memtable *
trunk_try_get_memtable(trunk_handle *spl, uint64 generation)
{
   uint64    memtable_idx = generation % TRUNK_NUM_MEMTABLES;
   memtable *mt           = &spl->mt_ctxt->mt[memtable_idx];
   if (mt->generation != generation) {
      mt = NULL;
   }
   return mt;
}

/*
 * returns the memtable with generation number generation. Caller must ensure
 * that there exists a memtable with the appropriate generation.
 */
static memtable *
trunk_get_memtable(trunk_handle *spl, uint64 generation)
{
   uint64    memtable_idx = generation % TRUNK_NUM_MEMTABLES;
   memtable *mt           = &spl->mt_ctxt->mt[memtable_idx];
   platform_assert(mt->generation == generation,
                   "mt->generation=%lu, mt_ctxt->generation=%lu, "
                   "mt_ctxt->generation_retired=%lu, generation=%lu\n",
                   mt->generation,
                   spl->mt_ctxt->generation,
                   spl->mt_ctxt->generation_retired,
                   generation);
   return mt;
}

static trunk_compacted_memtable *
trunk_get_compacted_memtable(trunk_handle *spl, uint64 generation)
{
   uint64 memtable_idx = generation % TRUNK_NUM_MEMTABLES;

   // this call asserts the generation is correct
   memtable *mt = trunk_get_memtable(spl, generation);
   platform_assert(mt->state != MEMTABLE_STATE_READY);

   return &spl->compacted_memtable[memtable_idx];
}

static inline void
trunk_memtable_inc_ref(trunk_handle *spl, uint64 mt_gen)
{
   memtable *mt = trunk_get_memtable(spl, mt_gen);
   allocator_inc_ref(spl->al, mt->root_addr);
}


static void
trunk_memtable_dec_ref(trunk_handle *spl, uint64 generation)
{
   memtable *mt = trunk_get_memtable(spl, generation);
   memtable_dec_ref_maybe_recycle(spl->mt_ctxt, mt);

   // the branch in the compacted memtable is now in the tree, so don't zap it,
   // we don't try to zero out the cmt because that would introduce a race.
}


/*
 * Wrappers for creating/destroying memtable iterators. Increments/decrements
 * the memtable ref count and cleans up if ref count == 0
 */
static void
trunk_memtable_iterator_init(trunk_handle   *spl,
                             btree_iterator *itor,
                             uint64          root_addr,
                             key             min_key,
                             key             max_key,
                             key             start_key,
                             comparison      start_type,
                             bool32          is_live,
                             bool32          inc_ref)
{
   if (inc_ref) {
      allocator_inc_ref(spl->al, root_addr);
   }
   btree_iterator_init(spl->cc,
                       &spl->cfg.btree_cfg,
                       itor,
                       root_addr,
                       PAGE_TYPE_MEMTABLE,
                       min_key,
                       max_key,
                       start_key,
                       start_type,
                       FALSE,
                       0);
}

static void
trunk_memtable_iterator_deinit(trunk_handle   *spl,
                               btree_iterator *itor,
                               uint64          mt_gen,
                               bool32          dec_ref)
{
   btree_iterator_deinit(itor);
   if (dec_ref) {
      trunk_memtable_dec_ref(spl, mt_gen);
   }
}

/*
 * Attempts to insert (key, data) into the current memtable.
 *
 * Returns:
 *    success if succeeded
 *    locked if the current memtable is full
 *    lock_acquired if the current memtable is full and this thread is
 *       responsible for flushing it.
 */
static platform_status
trunk_memtable_insert(trunk_handle *spl, key tuple_key, message msg)
{
   uint64 generation;

   platform_status rc =
      memtable_maybe_rotate_and_begin_insert(spl->mt_ctxt, &generation);
   while (STATUS_IS_EQ(rc, STATUS_BUSY)) {
      // Memtable isn't ready, do a task if available; may be required to
      // incorporate memtable that we're waiting on
      task_perform_one_if_needed(spl->ts, 0);
      rc = memtable_maybe_rotate_and_begin_insert(spl->mt_ctxt, &generation);
   }
   if (!SUCCESS(rc)) {
      goto out;
   }

   // this call is safe because we hold the insert lock
   memtable *mt = trunk_get_memtable(spl, generation);
   uint64    leaf_generation; // used for ordering the log
   rc = memtable_insert(
      spl->mt_ctxt, mt, spl->heap_id, tuple_key, msg, &leaf_generation);
   if (!SUCCESS(rc)) {
      goto unlock_insert_lock;
   }

   if (spl->cfg.use_log) {
      int crappy_rc = log_write(spl->log, tuple_key, msg, leaf_generation);
      if (crappy_rc != 0) {
         goto unlock_insert_lock;
      }
   }

unlock_insert_lock:
   memtable_end_insert(spl->mt_ctxt);
out:
   return rc;
}

/*
 * Compacts the memtable with generation generation and builds its filter.
 * Returns a pointer to the memtable.
 */
static memtable *
trunk_memtable_compact_and_build_filter(trunk_handle  *spl,
                                        uint64         generation,
                                        const threadid tid)
{
   timestamp comp_start = platform_get_timestamp();

   memtable *mt = trunk_get_memtable(spl, generation);

   memtable_transition(mt, MEMTABLE_STATE_FINALIZED, MEMTABLE_STATE_COMPACTING);
   mini_release(&mt->mini);

   trunk_compacted_memtable *cmt =
      trunk_get_compacted_memtable(spl, generation);
   trunk_branch *new_branch = &cmt->branch;
   ZERO_CONTENTS(new_branch);

   uint64         memtable_root_addr = mt->root_addr;
   btree_iterator btree_itor;
   iterator      *itor = &btree_itor.super;

   trunk_memtable_iterator_init(spl,
                                &btree_itor,
                                memtable_root_addr,
                                NEGATIVE_INFINITY_KEY,
                                POSITIVE_INFINITY_KEY,
                                NEGATIVE_INFINITY_KEY,
                                greater_than_or_equal,
                                FALSE,
                                FALSE);
   btree_pack_req req;
   btree_pack_req_init(&req,
                       spl->cc,
                       &spl->cfg.btree_cfg,
                       itor,
                       spl->cfg.max_tuples_per_node,
                       spl->cfg.filter_cfg.hash,
                       spl->cfg.filter_cfg.seed,
                       spl->heap_id);
   uint64 pack_start;
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_compactions++;
      pack_start = platform_get_timestamp();
   }

   platform_status pack_status = btree_pack(&req);
   platform_assert(SUCCESS(pack_status),
                   "platform_status of btree_pack: %d\n",
                   pack_status.r);

   platform_assert(req.num_tuples <= spl->cfg.max_tuples_per_node);
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_compaction_pack_time_ns +=
         platform_timestamp_elapsed(pack_start);
      spl->stats[tid].root_compaction_tuples += req.num_tuples;
      if (req.num_tuples > spl->stats[tid].root_compaction_max_tuples) {
         spl->stats[tid].root_compaction_max_tuples = req.num_tuples;
      }
   }
   trunk_memtable_iterator_deinit(spl, &btree_itor, FALSE, FALSE);

   new_branch->root_addr = req.root_addr;

   platform_assert(req.num_tuples > 0);
   uint64 filter_build_start;
   if (spl->cfg.use_stats) {
      filter_build_start = platform_get_timestamp();
   }

   cmt->req         = TYPED_ZALLOC(spl->heap_id, cmt->req);
   cmt->req->spl    = spl;
   cmt->req->fp_arr = req.fingerprint_arr;
   cmt->req->type   = TRUNK_COMPACTION_TYPE_MEMTABLE;
   uint32 *dup_fp_arr =
      TYPED_ARRAY_MALLOC(spl->heap_id, dup_fp_arr, req.num_tuples);
   memmove(dup_fp_arr, cmt->req->fp_arr, req.num_tuples * sizeof(uint32));
   routing_filter empty_filter = {0};

   platform_status rc = routing_filter_add(spl->cc,
                                           &spl->cfg.filter_cfg,
                                           &empty_filter,
                                           &cmt->filter,
                                           cmt->req->fp_arr,
                                           req.num_tuples,
                                           0);

   platform_assert(SUCCESS(rc));
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_filter_time_ns +=
         platform_timestamp_elapsed(filter_build_start);
      spl->stats[tid].root_filters_built++;
      spl->stats[tid].root_filter_tuples += req.num_tuples;
   }

   btree_pack_req_deinit(&req, spl->heap_id);
   cmt->req->fp_arr = dup_fp_arr;
   if (spl->cfg.use_stats) {
      uint64 comp_time = platform_timestamp_elapsed(comp_start);
      spl->stats[tid].root_compaction_time_ns += comp_time;
      if (comp_start > spl->stats[tid].root_compaction_time_max_ns) {
         spl->stats[tid].root_compaction_time_max_ns = comp_time;
      }
      cmt->wait_start = platform_get_timestamp();
   }

   memtable_transition(mt, MEMTABLE_STATE_COMPACTING, MEMTABLE_STATE_COMPACTED);
   return mt;
}

/*
 * Cases:
 * 1. memtable set to COMP before try_continue tries to set it to incorp
 *       try_continue will successfully assign itself to incorp the memtable
 * 2. memtable set to COMP after try_continue tries to set it to incorp
 *       should_wait will be set to generation, so try_start will incorp
 */
static inline bool32
trunk_try_start_incorporate(trunk_handle *spl, uint64 generation)
{
   bool32 should_start = FALSE;

   memtable_lock_incorporation_lock(spl->mt_ctxt);
   memtable *mt = trunk_try_get_memtable(spl, generation);
   if ((mt == NULL)
       || (generation != memtable_generation_to_incorporate(spl->mt_ctxt)))
   {
      should_start = FALSE;
      goto unlock_incorp_lock;
   }
   should_start = memtable_try_transition(
      mt, MEMTABLE_STATE_COMPACTED, MEMTABLE_STATE_INCORPORATION_ASSIGNED);

unlock_incorp_lock:
   memtable_unlock_incorporation_lock(spl->mt_ctxt);
   return should_start;
}

static inline bool32
trunk_try_continue_incorporate(trunk_handle *spl, uint64 next_generation)
{
   bool32 should_continue = FALSE;

   memtable_lock_incorporation_lock(spl->mt_ctxt);
   memtable *mt = trunk_try_get_memtable(spl, next_generation);
   if (mt == NULL) {
      should_continue = FALSE;
      goto unlock_incorp_lock;
   }
   should_continue = memtable_try_transition(
      mt, MEMTABLE_STATE_COMPACTED, MEMTABLE_STATE_INCORPORATION_ASSIGNED);
   memtable_increment_to_generation_to_incorporate(spl->mt_ctxt,
                                                   next_generation);

unlock_incorp_lock:
   memtable_unlock_incorporation_lock(spl->mt_ctxt);
   return should_continue;
}

/*
 * Function to incorporate the memtable to the root.
 * Carries out the following steps :
 *  1. Claim and copy the root.
 *  2. Add the memtable to the new root as a new compacted bundle.
 *  3. If the new root is full, flush until it is no longer full. Also flushes
 *     any full descendents.
 *  4. If necessary, split the new root.
 *  5. Lock lookup lock (blocks lookups, which must obtain a read lock on the
 *     lookup lock).
 *  6. Transition memtable state and increment generation_retired.
 *  7. Update root to new_root and unlock all locks (root lock, lookup lock,
 *     new root lock).
 *  8. Enqueue the filter building task.
 *  9. Decrement the now-incorporated memtable ref count and recycle if no
 *     references.
 *
 * This functions has some preconditions prior to being called.
 *  --> Trunk root node should be write locked.
 *  --> The memtable should have inserts blocked (can_insert == FALSE)
 */
static void
trunk_memtable_incorporate_and_flush(trunk_handle  *spl,
                                     uint64         generation,
                                     const threadid tid)
{
   trunk_node new_root;
   trunk_modification_begin(&spl->trunk_context);

   platform_stream_handle stream;
   platform_status        rc = trunk_open_log_stream_if_enabled(spl, &stream);
   platform_assert_status_ok(rc);
   trunk_log_stream_if_enabled(
      spl,
      &stream,
      "incorporate memtable gen %lu into new root %lu\n",
      generation,
      new_root.addr);
   trunk_log_stream_if_enabled(
      spl, &stream, "----------------------------------------\n");

   // Add the memtable to the new root as a new compacted bundle
   trunk_compacted_memtable *cmt =
      trunk_get_compacted_memtable(spl, generation);
   trunk_compact_bundle_req *req = cmt->req;
   uint64                    flush_start;
   if (spl->cfg.use_stats) {
      flush_start = platform_get_timestamp();
   }
   rc = trunk_incorporate(
      &spl->trunk_context, cmt->filter, cmt->branch.root_addr);
   platform_assert_status_ok(rc);
   btree_dec_ref(
      spl->cc, &spl->cfg.btree_cfg, cmt->branch.root_addr, PAGE_TYPE_MEMTABLE);
   routing_filter_dec_ref(spl->cc, &cmt->filter);
   if (spl->cfg.use_stats) {
      spl->stats[tid].memtable_flush_wait_time_ns +=
         platform_timestamp_elapsed(cmt->wait_start);
   }

   trunk_log_stream_if_enabled(
      spl, &stream, "----------------------------------------\n");
   trunk_log_stream_if_enabled(spl, &stream, "\n");

   /*
    * Lock the lookup lock, blocking lookups.
    * Transition memtable state and increment memtable generation (blocks
    * lookups from accessing the memtable that's being incorporated).
    */
   memtable_block_lookups(spl->mt_ctxt);
   memtable *mt = trunk_get_memtable(spl, generation);
   // Normally need to hold incorp_mutex, but debug code and also guaranteed no
   // one is changing gen_to_incorp (we are the only thread that would try)
   debug_assert(generation == memtable_generation_to_incorporate(spl->mt_ctxt));
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATION_ASSIGNED, MEMTABLE_STATE_INCORPORATING);
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATING, MEMTABLE_STATE_INCORPORATED);
   memtable_increment_to_generation_retired(spl->mt_ctxt, generation);

   // Switch in the new root and release all locks
   trunk_modification_end(&spl->trunk_context);
   memtable_unblock_lookups(spl->mt_ctxt);

   // Enqueue the filter building task.
   trunk_log_stream_if_enabled(
      spl,
      &stream,
      "enqueuing build filter: range %s-%s, height %u, bundle %u\n",
      key_string(trunk_data_config(spl), key_buffer_key(&req->start_key)),
      key_string(trunk_data_config(spl), key_buffer_key(&req->end_key)),
      req->height,
      req->bundle_no);
   trunk_close_log_stream_if_enabled(spl, &stream);

   /*
    * Decrement the now-incorporated memtable ref count and recycle if no
    * references
    */
   memtable_dec_ref_maybe_recycle(spl->mt_ctxt, mt);

   if (spl->cfg.use_stats) {
      const threadid tid = platform_get_tid();
      flush_start        = platform_timestamp_elapsed(flush_start);
      spl->stats[tid].memtable_flush_time_ns += flush_start;
      spl->stats[tid].memtable_flushes++;
      if (flush_start > spl->stats[tid].memtable_flush_time_max_ns) {
         spl->stats[tid].memtable_flush_time_max_ns = flush_start;
      }
   }
}

/*
 * Main wrapper function to carry out incorporation of a memtable.
 *
 * If background threads are disabled this function is called inline in the
 * context of the foreground thread.  If background threads are enabled, this
 * function is called in the context of the memtable worker thread.
 */
static void
trunk_memtable_flush_internal(trunk_handle *spl, uint64 generation)
{
   const threadid tid = platform_get_tid();
   // pack and build filter.
   trunk_memtable_compact_and_build_filter(spl, generation, tid);

   // If we are assigned to do so, incorporate the memtable onto the root node.
   if (!trunk_try_start_incorporate(spl, generation)) {
      goto out;
   }
   do {
      trunk_memtable_incorporate_and_flush(spl, generation, tid);
      generation++;
   } while (trunk_try_continue_incorporate(spl, generation));
out:
   return;
}

static void
trunk_memtable_flush_internal_virtual(void *arg, void *scratch)
{
   trunk_memtable_args *mt_args = arg;
   trunk_memtable_flush_internal(mt_args->spl, mt_args->generation);
}

/*
 * Function to trigger a memtable incorporation. Called in the context of
 * the foreground doing insertions.
 * If background threads are not enabled, this function does the entire memtable
 * incorporation inline.
 * If background threads are enabled, this function just queues up the task to
 * carry out the incorporation, swaps the curr_memtable pointer, claims the
 * root and returns.
 */
static void
trunk_memtable_flush(trunk_handle *spl, uint64 generation)
{
   trunk_compacted_memtable *cmt =
      trunk_get_compacted_memtable(spl, generation);
   cmt->mt_args.spl        = spl;
   cmt->mt_args.generation = generation;
   task_enqueue(spl->ts,
                TASK_TYPE_MEMTABLE,
                trunk_memtable_flush_internal_virtual,
                &cmt->mt_args,
                FALSE);
}

static void
trunk_memtable_flush_virtual(void *arg, uint64 generation)
{
   trunk_handle *spl = arg;
   trunk_memtable_flush(spl, generation);
}

static inline uint64
trunk_memtable_root_addr_for_lookup(trunk_handle *spl,
                                    uint64        generation,
                                    bool32       *is_compacted)
{
   memtable *mt = trunk_get_memtable(spl, generation);
   platform_assert(memtable_ok_to_lookup(mt));

   if (memtable_ok_to_lookup_compacted(mt)) {
      // lookup in packed tree
      *is_compacted = TRUE;
      trunk_compacted_memtable *cmt =
         trunk_get_compacted_memtable(spl, generation);
      return cmt->branch.root_addr;
   } else {
      *is_compacted = FALSE;
      return mt->root_addr;
   }
}

/*
 * trunk_memtable_lookup
 *
 * Pre-conditions:
 *    If *found
 *       `data` has the most recent answer.
 *       the current memtable is older than the most recent answer
 *
 * Post-conditions:
 *    if *found, the data can be found in `data`.
 */
static platform_status
trunk_memtable_lookup(trunk_handle      *spl,
                      uint64             generation,
                      key                target,
                      merge_accumulator *data)
{
   cache *const        cc  = spl->cc;
   btree_config *const cfg = &spl->cfg.btree_cfg;
   bool32              memtable_is_compacted;
   uint64              root_addr = trunk_memtable_root_addr_for_lookup(
      spl, generation, &memtable_is_compacted);
   page_type type =
      memtable_is_compacted ? PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;
   platform_status rc;
   bool32          local_found;

   rc = btree_lookup_and_merge(
      cc, cfg, root_addr, type, target, data, &local_found);
   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * Filter functions
 *-----------------------------------------------------------------------------
 */

static inline routing_config *
trunk_routing_cfg(trunk_handle *spl)
{
   return &spl->cfg.filter_cfg;
}

static inline void
trunk_dec_filter(trunk_handle *spl, routing_filter *filter)
{
   if (filter->addr == 0) {
      return;
   }
   cache *cc = spl->cc;
   routing_filter_dec_ref(cc, filter);
}

static cache_async_result
trunk_filter_lookup_async(trunk_handle       *spl,
                          routing_config     *cfg,
                          routing_filter     *filter,
                          key                 target,
                          uint64             *found_values,
                          routing_async_ctxt *ctxt)
{
   return routing_filter_lookup_async(
      spl->cc, cfg, filter, target, found_values, ctxt);
}

/*
 * Branch iterator wrapper functions
 */

static void
trunk_branch_iterator_init(trunk_handle   *spl,
                           btree_iterator *itor,
                           uint64          branch_addr,
                           key             min_key,
                           key             max_key,
                           key             start_key,
                           comparison      start_type,
                           bool32          do_prefetch,
                           bool32          should_inc_ref)
{
   cache        *cc        = spl->cc;
   btree_config *btree_cfg = &spl->cfg.btree_cfg;
   if (branch_addr != 0 && should_inc_ref) {
      btree_inc_ref(cc, btree_cfg, branch_addr);
   }
   btree_iterator_init(cc,
                       btree_cfg,
                       itor,
                       branch_addr,
                       PAGE_TYPE_BRANCH,
                       min_key,
                       max_key,
                       start_key,
                       start_type,
                       do_prefetch,
                       0);
}

static void
trunk_branch_iterator_deinit(trunk_handle   *spl,
                             btree_iterator *itor,
                             bool32          should_dec_ref)
{
   if (itor->root_addr == 0) {
      return;
   }
   cache        *cc        = spl->cc;
   btree_config *btree_cfg = &spl->cfg.btree_cfg;
   btree_iterator_deinit(itor);
   if (should_dec_ref) {
      btree_dec_ref(cc, btree_cfg, itor->root_addr, PAGE_TYPE_BRANCH);
   }
}

/*
 *-----------------------------------------------------------------------------
 * Range functions and iterators
 *
 *      trunk_node_iterator
 *      trunk_iterator
 *-----------------------------------------------------------------------------
 */
static void
trunk_range_iterator_curr(iterator *itor, key *curr_key, message *data);
static bool32
trunk_range_iterator_can_prev(iterator *itor);
static bool32
trunk_range_iterator_can_next(iterator *itor);
static platform_status
trunk_range_iterator_next(iterator *itor);
static platform_status
trunk_range_iterator_prev(iterator *itor);
void
trunk_range_iterator_deinit(trunk_range_iterator *range_itor);

const static iterator_ops trunk_range_iterator_ops = {
   .curr     = trunk_range_iterator_curr,
   .can_prev = trunk_range_iterator_can_prev,
   .can_next = trunk_range_iterator_can_next,
   .next     = trunk_range_iterator_next,
   .prev     = trunk_range_iterator_prev,
};

platform_status
trunk_range_iterator_init(trunk_handle         *spl,
                          trunk_range_iterator *range_itor,
                          key                   min_key,
                          key                   max_key,
                          key                   start_key,
                          comparison            start_type,
                          uint64                num_tuples)
{
   debug_assert(!key_is_null(min_key));
   debug_assert(!key_is_null(max_key));
   debug_assert(!key_is_null(start_key));

   range_itor->spl          = spl;
   range_itor->super.ops    = &trunk_range_iterator_ops;
   range_itor->num_branches = 0;
   range_itor->num_tuples   = num_tuples;
   range_itor->merge_itor   = NULL;
   range_itor->can_prev     = TRUE;
   range_itor->can_next     = TRUE;

   if (trunk_key_compare(spl, min_key, start_key) > 0) {
      // in bounds, start at min
      start_key = min_key;
   }
   if (trunk_key_compare(spl, max_key, start_key) <= 0) {
      // out of bounds, start at max
      start_key = max_key;
   }

   // copy over global min and max
   key_buffer_init_from_key(&range_itor->min_key, spl->heap_id, min_key);
   key_buffer_init_from_key(&range_itor->max_key, spl->heap_id, max_key);

   ZERO_ARRAY(range_itor->compacted);

   // grab the lookup lock
   memtable_begin_lookup(spl->mt_ctxt);

   // memtables
   ZERO_ARRAY(range_itor->branch);
   // Note this iteration is in descending generation order
   range_itor->memtable_start_gen = memtable_generation(spl->mt_ctxt);
   range_itor->memtable_end_gen   = memtable_generation_retired(spl->mt_ctxt);
   range_itor->num_memtable_branches =
      range_itor->memtable_start_gen - range_itor->memtable_end_gen;
   for (uint64 mt_gen = range_itor->memtable_start_gen;
        mt_gen != range_itor->memtable_end_gen;
        mt_gen--)
   {
      platform_assert(
         (range_itor->num_branches < TRUNK_RANGE_ITOR_MAX_BRANCHES),
         "range_itor->num_branches=%lu should be < "
         " TRUNK_RANGE_ITOR_MAX_BRANCHES (%d).",
         range_itor->num_branches,
         TRUNK_RANGE_ITOR_MAX_BRANCHES);
      debug_assert(range_itor->num_branches < ARRAY_SIZE(range_itor->branch));

      bool32 compacted;
      uint64 root_addr =
         trunk_memtable_root_addr_for_lookup(spl, mt_gen, &compacted);
      range_itor->compacted[range_itor->num_branches] = compacted;
      if (compacted) {
         btree_inc_ref(spl->cc, &spl->cfg.btree_cfg, root_addr);
      } else {
         trunk_memtable_inc_ref(spl, mt_gen);
      }

      range_itor->branch[range_itor->num_branches] = root_addr;

      range_itor->num_branches++;
   }

   ondisk_node_handle root_handle;
   trunk_init_root_handle(&spl->trunk_context, &root_handle);

   memtable_end_lookup(spl->mt_ctxt);

   key_buffer_init(&range_itor->local_min_key, spl->heap_id);
   key_buffer_init(&range_itor->local_max_key, spl->heap_id);

   platform_status rc;
   uint64          old_num_branches = range_itor->num_branches;
   rc = trunk_collect_branches(&spl->trunk_context,
                               &root_handle,
                               start_key,
                               start_type,
                               TRUNK_RANGE_ITOR_MAX_BRANCHES,
                               &range_itor->num_branches,
                               range_itor->branch,
                               &range_itor->local_min_key,
                               &range_itor->local_max_key);
   trunk_ondisk_node_handle_deinit(&root_handle);
   platform_assert_status_ok(rc);

   for (uint64 i = old_num_branches; i < range_itor->num_branches; i++) {
      range_itor->compacted[i] = TRUE;
   }

   // have a leaf, use to establish local bounds
   if (trunk_key_compare(
          spl, key_buffer_key(&range_itor->local_min_key), min_key)
       <= 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_min_key, min_key);
      platform_assert_status_ok(rc);
   }
   if (trunk_key_compare(
          spl, key_buffer_key(&range_itor->local_max_key), max_key)
       >= 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_max_key, max_key);
      platform_assert_status_ok(rc);
   }

   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      uint64          branch_no   = range_itor->num_branches - i - 1;
      btree_iterator *btree_itor  = &range_itor->btree_itor[branch_no];
      uint64          branch_addr = range_itor->branch[branch_no];
      if (range_itor->compacted[branch_no]) {
         bool32 do_prefetch =
            range_itor->compacted[branch_no] && num_tuples > TRUNK_PREFETCH_MIN
               ? TRUE
               : FALSE;
         trunk_branch_iterator_init(spl,
                                    btree_itor,
                                    branch_addr,
                                    key_buffer_key(&range_itor->local_min_key),
                                    key_buffer_key(&range_itor->local_max_key),
                                    start_key,
                                    start_type,
                                    do_prefetch,
                                    FALSE);
      } else {
         bool32 is_live = branch_no == 0;
         trunk_memtable_iterator_init(
            spl,
            btree_itor,
            branch_addr,
            key_buffer_key(&range_itor->local_min_key),
            key_buffer_key(&range_itor->local_max_key),
            start_key,
            start_type,
            is_live,
            FALSE);
      }
      range_itor->itor[i] = &btree_itor->super;
   }

   rc = merge_iterator_create(spl->heap_id,
                              spl->cfg.data_cfg,
                              range_itor->num_branches,
                              range_itor->itor,
                              MERGE_FULL,
                              greater_than <= start_type,
                              &range_itor->merge_itor);
   platform_assert_status_ok(rc);

   bool32 in_range = iterator_can_curr(&range_itor->merge_itor->super);

   /*
    * if the merge itor is already exhausted, and there are more keys in the
    * db/range, move to prev/next leaf
    */
   if (!in_range && start_type >= greater_than) {
      key local_max = key_buffer_key(&range_itor->local_max_key);
      if (trunk_key_compare(spl, local_max, max_key) < 0) {
         trunk_range_iterator_deinit(range_itor);
         rc = trunk_range_iterator_init(spl,
                                        range_itor,
                                        min_key,
                                        max_key,
                                        local_max,
                                        start_type,
                                        range_itor->num_tuples);
         platform_assert_status_ok(rc);
      } else {
         range_itor->can_next = FALSE;
         range_itor->can_prev =
            iterator_can_prev(&range_itor->merge_itor->super);
      }
   }
   if (!in_range && start_type <= less_than_or_equal) {
      key local_min = key_buffer_key(&range_itor->local_min_key);
      if (trunk_key_compare(spl, local_min, min_key) > 0) {
         trunk_range_iterator_deinit(range_itor);
         rc = trunk_range_iterator_init(spl,
                                        range_itor,
                                        min_key,
                                        max_key,
                                        local_min,
                                        start_type,
                                        range_itor->num_tuples);
         platform_assert_status_ok(rc);
      } else {
         range_itor->can_prev = FALSE;
         range_itor->can_next =
            iterator_can_next(&range_itor->merge_itor->super);
      }
   }
   return rc;
}

static void
trunk_range_iterator_curr(iterator *itor, key *curr_key, message *data)
{
   debug_assert(itor != NULL);
   trunk_range_iterator *range_itor = (trunk_range_iterator *)itor;
   iterator_curr(&range_itor->merge_itor->super, curr_key, data);
}

static platform_status
trunk_range_iterator_next(iterator *itor)
{
   trunk_range_iterator *range_itor = (trunk_range_iterator *)itor;
   debug_assert(range_itor != NULL);
   platform_assert(range_itor->can_next);

   platform_status rc = iterator_next(&range_itor->merge_itor->super);
   if (!SUCCESS(rc)) {
      return rc;
   }
   range_itor->num_tuples++;
   range_itor->can_prev = TRUE;
   range_itor->can_next = iterator_can_next(&range_itor->merge_itor->super);
   if (!range_itor->can_next) {
      KEY_CREATE_LOCAL_COPY(rc,
                            min_key,
                            range_itor->spl->heap_id,
                            key_buffer_key(&range_itor->min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            max_key,
                            range_itor->spl->heap_id,
                            key_buffer_key(&range_itor->max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            local_max_key,
                            range_itor->spl->heap_id,
                            key_buffer_key(&range_itor->local_max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }

      // if there is more data to get, rebuild the iterator for next leaf
      if (trunk_key_compare(range_itor->spl, local_max_key, max_key) < 0) {
         uint64 temp_tuples = range_itor->num_tuples;
         trunk_range_iterator_deinit(range_itor);
         rc = trunk_range_iterator_init(range_itor->spl,
                                        range_itor,
                                        min_key,
                                        max_key,
                                        local_max_key,
                                        greater_than_or_equal,
                                        temp_tuples);
         if (!SUCCESS(rc)) {
            return rc;
         }
         debug_assert(range_itor->can_next
                      == iterator_can_next(&range_itor->merge_itor->super));
      }
   }

   return STATUS_OK;
}

static platform_status
trunk_range_iterator_prev(iterator *itor)
{
   trunk_range_iterator *range_itor = (trunk_range_iterator *)itor;
   debug_assert(itor != NULL);
   platform_assert(range_itor->can_prev);

   platform_status rc = iterator_prev(&range_itor->merge_itor->super);
   if (!SUCCESS(rc)) {
      return rc;
   }
   range_itor->num_tuples++;
   range_itor->can_next = TRUE;
   range_itor->can_prev = iterator_can_prev(&range_itor->merge_itor->super);
   if (!range_itor->can_prev) {
      KEY_CREATE_LOCAL_COPY(rc,
                            min_key,
                            range_itor->spl->heap_id,
                            key_buffer_key(&range_itor->min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            max_key,
                            range_itor->spl->heap_id,
                            key_buffer_key(&range_itor->max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            local_min_key,
                            range_itor->spl->heap_id,
                            key_buffer_key(&range_itor->local_min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }

      // if there is more data to get, rebuild the iterator for prev leaf
      if (trunk_key_compare(range_itor->spl, local_min_key, min_key) > 0) {
         trunk_range_iterator_deinit(range_itor);
         rc = trunk_range_iterator_init(range_itor->spl,
                                        range_itor,
                                        min_key,
                                        max_key,
                                        local_min_key,
                                        less_than,
                                        range_itor->num_tuples);
         if (!SUCCESS(rc)) {
            return rc;
         }
         debug_assert(range_itor->can_prev
                      == iterator_can_prev(&range_itor->merge_itor->super));
      }
   }

   return STATUS_OK;
}

static bool32
trunk_range_iterator_can_prev(iterator *itor)
{
   debug_assert(itor != NULL);
   trunk_range_iterator *range_itor = (trunk_range_iterator *)itor;

   return range_itor->can_prev;
}

static bool32
trunk_range_iterator_can_next(iterator *itor)
{
   debug_assert(itor != NULL);
   trunk_range_iterator *range_itor = (trunk_range_iterator *)itor;

   return range_itor->can_next;
}

void
trunk_range_iterator_deinit(trunk_range_iterator *range_itor)
{
   trunk_handle *spl = range_itor->spl;
   if (range_itor->merge_itor != NULL) {
      merge_iterator_destroy(range_itor->spl->heap_id, &range_itor->merge_itor);
      for (uint64 i = 0; i < range_itor->num_branches; i++) {
         btree_iterator *btree_itor = &range_itor->btree_itor[i];
         if (range_itor->compacted[i]) {
            uint64 root_addr = btree_itor->root_addr;
            trunk_branch_iterator_deinit(spl, btree_itor, FALSE);
            btree_dec_ref(
               spl->cc, &spl->cfg.btree_cfg, root_addr, PAGE_TYPE_BRANCH);
         } else {
            uint64 mt_gen = range_itor->memtable_start_gen - i;
            trunk_memtable_iterator_deinit(spl, btree_itor, mt_gen, FALSE);
            trunk_memtable_dec_ref(spl, mt_gen);
         }
      }
      key_buffer_deinit(&range_itor->min_key);
      key_buffer_deinit(&range_itor->max_key);
      key_buffer_deinit(&range_itor->local_min_key);
      key_buffer_deinit(&range_itor->local_max_key);
   }
}

/*
 *-----------------------------------------------------------------------------
 * Main Splinter API functions
 *
 *      insert
 *      lookup
 *      range
 *-----------------------------------------------------------------------------
 */

platform_status
trunk_insert(trunk_handle *spl, key tuple_key, message data)
{
   timestamp      ts;
   const threadid tid = platform_get_tid();
   if (spl->cfg.use_stats) {
      ts = platform_get_timestamp();
   }

   if (trunk_max_key_size(spl) < key_length(tuple_key)) {
      return STATUS_BAD_PARAM;
   }

   if (message_class(data) == MESSAGE_TYPE_DELETE) {
      data = DELETE_MESSAGE;
   }

   platform_status rc = trunk_memtable_insert(spl, tuple_key, data);
   if (!SUCCESS(rc)) {
      goto out;
   }

   task_perform_one_if_needed(spl->ts, spl->cfg.queue_scale_percent);

   if (spl->cfg.use_stats) {
      switch (message_class(data)) {
         case MESSAGE_TYPE_INSERT:
            spl->stats[tid].insertions++;
            platform_histo_insert(spl->stats[tid].insert_latency_histo,
                                  platform_timestamp_elapsed(ts));
            break;
         case MESSAGE_TYPE_UPDATE:
            spl->stats[tid].updates++;
            platform_histo_insert(spl->stats[tid].update_latency_histo,
                                  platform_timestamp_elapsed(ts));
            break;
         case MESSAGE_TYPE_DELETE:
            spl->stats[tid].deletions++;
            platform_histo_insert(spl->stats[tid].delete_latency_histo,
                                  platform_timestamp_elapsed(ts));
            break;
         default:
            platform_assert(0);
      }
   }

out:
   return rc;
}

// If any change is made in here, please make similar change in
// trunk_lookup_async
platform_status
trunk_lookup(trunk_handle *spl, key target, merge_accumulator *result)
{
   // look in memtables

   // 1. get read lock on lookup lock
   //     --- 2. for [mt_no = mt->generation..mt->gen_to_incorp]
   // 2. for gen = mt->generation; mt[gen % ...].gen == gen; gen --;
   //                also handles switch to READY ^^^^^

   merge_accumulator_set_to_null(result);

   memtable_begin_lookup(spl->mt_ctxt);
   uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
   uint64 mt_gen_end   = memtable_generation_retired(spl->mt_ctxt);
   platform_assert(mt_gen_start - mt_gen_end <= TRUNK_NUM_MEMTABLES);

   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      platform_status rc;
      rc = trunk_memtable_lookup(spl, mt_gen, target, result);
      platform_assert_status_ok(rc);
      if (merge_accumulator_is_definitive(result)) {
         memtable_end_lookup(spl->mt_ctxt);
         goto found_final_answer_early;
      }
   }

   ondisk_node_handle root_handle;
   platform_status    rc;
   rc = trunk_init_root_handle(&spl->trunk_context, &root_handle);
   // release memtable lookup lock before we handle any errors
   memtable_end_lookup(spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      return rc;
   }


   rc = trunk_merge_lookup(&spl->trunk_context, &root_handle, target, result);
   // Release the node handle before handling any errors
   trunk_ondisk_node_handle_deinit(&root_handle);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (!merge_accumulator_is_null(result)
       && !merge_accumulator_is_definitive(result))
   {
      data_merge_tuples_final(spl->cfg.data_cfg, target, result);
   }

found_final_answer_early:

   if (spl->cfg.use_stats) {
      threadid tid = platform_get_tid();
      if (!merge_accumulator_is_null(result)) {
         spl->stats[tid].lookups_found++;
      } else {
         spl->stats[tid].lookups_not_found++;
      }
   }

   /* Normalize DELETE messages to return a null merge_accumulator */
   if (!merge_accumulator_is_null(result)
       && merge_accumulator_message_class(result) == MESSAGE_TYPE_DELETE)
   {
      merge_accumulator_set_to_null(result);
   }

   return STATUS_OK;
}

/*
 * trunk_async_set_state sets the state of the async splinter
 * lookup state machine.
 */
static inline void
trunk_async_set_state(trunk_async_ctxt *ctxt, trunk_async_state new_state)
{
   ctxt->prev_state = ctxt->state;
   ctxt->state      = new_state;
}


/*
 * trunk_async_callback
 *
 *      Callback that's called when the async cache get for a trunk
 *      node loads a page for the child into the cache. This function
 *      moves the async splinter lookup state machine's state ahead,
 *      and calls the upper layer callback that'll re-enqueue the
 *      spinter lookup for dispatch.
 */
static void
trunk_async_callback(cache_async_ctxt *cache_ctxt)
{
   trunk_async_ctxt *ctxt =
      container_of(cache_ctxt, trunk_async_ctxt, cache_ctxt);
   platform_assert(SUCCESS(cache_ctxt->status));
   platform_assert(cache_ctxt->page);
   //   platform_default_log("%s:%d tid %2lu: ctxt %p is callback with page
   //   %p\n",
   //                __FILE__, __LINE__, platform_get_tid(), ctxt,
   //                cache_ctxt->page);
   ctxt->was_async = TRUE;
   // Move state machine ahead and requeue for dispatch
   if (UNLIKELY(ctxt->state == async_state_get_root_reentrant)) {
      trunk_async_set_state(ctxt, async_state_trunk_node_lookup);
   } else {
      debug_assert((ctxt->state == async_state_get_child_trunk_node_reentrant),
                   "ctxt->state=%d != expected state=%d",
                   ctxt->state,
                   async_state_get_child_trunk_node_reentrant);
      trunk_async_set_state(ctxt, async_state_unget_parent_trunk_node);
   }
   ctxt->cb(ctxt);
}


/*
 * trunk_filter_async_callback
 *
 *      Callback that's called when the async filter get api has loaded
 *      a page into cache. This just requeues the splinter lookup for
 *      dispatch at the same state, so that async filter get can be
 *      called again.
 */
static void
trunk_filter_async_callback(routing_async_ctxt *filter_ctxt)
{
   trunk_async_ctxt *ctxt =
      container_of(filter_ctxt, trunk_async_ctxt, filter_ctxt);
   //   platform_default_log("%s:%d tid %2lu: ctxt %p is callback\n",
   //                __FILE__, __LINE__, platform_get_tid(), ctxt);
   // Requeue for dispatch
   ctxt->cb(ctxt);
}

/*
 * trunk_btree_async_callback
 *
 *      Callback that's called when the async btree
 *      lookup api has loaded a page into cache. This just requeues
 *      the splinter lookup for dispatch at the same state, so that
 *      async btree lookup can be called again.
 */
static void
trunk_btree_async_callback(btree_async_ctxt *btree_ctxt)
{
   trunk_async_ctxt *ctxt =
      container_of(btree_ctxt, trunk_async_ctxt, btree_ctxt);
   //   platform_default_log("%s:%d tid %2lu: ctxt %p is callback\n",
   //                __FILE__, __LINE__, platform_get_tid(), ctxt);
   // Requeue for dispatch
   ctxt->cb(ctxt);
}


/*
 * Async splinter lookup. Caller must have called trunk_async_ctxt_init()
 * on the context before the first invocation.
 *
 * This uses hand over hand locking to descend the trunk tree and
 * every time a child node needs to be looked up from the cache, it
 * uses the async get api. A reference to the parent node is held in
 * trunk_async_ctxt->trunk_node while a reference to the child page
 * is obtained by the cache_get_async() into
 * trunk_async_ctxt->cache_ctxt->page
 *
 * Returns:
 *    async_success: results are available in *found and *result
 *    async_locked: caller needs to retry
 *    async_no_reqs: caller needs to retry but may want to throttle
 *    async_io_started: async IO was started; the caller will be informed
 *      via callback when it's done. After callback is called, the caller
 *      must call this again from thread context with the same key and result
 *      as the first invocation.
 *
 * Side-effects:
 *    Maintains state in *result. This helps avoid copying data between
 *    invocations. Caller must use the same pointers to key, result and
 *    found in different invocations of a lookup until it returns
 *    async_success. Caller must not modify the contents of those
 *    pointers.
 */
cache_async_result
trunk_lookup_async(trunk_handle      *spl,    // IN
                   key                target, // IN
                   merge_accumulator *result, // OUT
                   trunk_async_ctxt  *ctxt)    // IN/OUT
{
   cache_async_result res = 0;
   threadid           tid;

#if TRUNK_DEBUG
   cache_enable_sync_get(spl->cc, FALSE);
#endif
   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
   }
   trunk_node *node = &ctxt->trunk_node;
   bool32      done = FALSE;

   do {
      switch (ctxt->state) {
         case async_state_start:
         {
            merge_accumulator_set_to_null(result);
            trunk_async_set_state(ctxt, async_state_lookup_memtable);
            // fallthrough
         }
         case async_state_lookup_memtable:
         {
            memtable_begin_lookup(spl->mt_ctxt);
            uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
            uint64 mt_gen_end   = memtable_generation_retired(spl->mt_ctxt);
            for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
               platform_status rc;
               rc = trunk_memtable_lookup(spl, mt_gen, target, result);
               platform_assert_status_ok(rc);
               if (merge_accumulator_is_definitive(result)) {
                  trunk_async_set_state(ctxt,
                                        async_state_found_final_answer_early);
                  memtable_end_lookup(spl->mt_ctxt);
                  break;
               }
            }
            if (ctxt->state == async_state_found_final_answer_early) {
               break;
            }
            // fallthrough
         }
         case async_state_get_root_reentrant:
         {
            cache_ctxt_init(
               spl->cc, trunk_async_callback, NULL, &ctxt->cache_ctxt);
            res = trunk_node_get_async(spl->cc, spl->root_addr, ctxt);
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
                  trunk_async_set_state(ctxt, async_state_trunk_node_lookup);
                  ctxt->trunk_node.page = ctxt->cache_ctxt.page;
                  ctxt->trunk_node.hdr =
                     (trunk_hdr *)(ctxt->cache_ctxt.page->data);
                  memtable_end_lookup(spl->mt_ctxt);
                  break;
               default:
                  platform_assert(0);
            }
            break;
         }
         case async_state_trunk_node_lookup:
         {
            ctxt->height = trunk_node_height(node);
            uint16 pivot_no =
               trunk_find_pivot(spl, node, target, less_than_or_equal);
            debug_assert(pivot_no < trunk_num_children(spl, node));
            ctxt->pdata = trunk_get_pivot_data(spl, node, pivot_no);
            ctxt->sb_no = trunk_start_subbundle_for_lookup(spl, node);
            ctxt->end_sb_no =
               trunk_pivot_end_subbundle_for_lookup(spl, node, ctxt->pdata);
            ctxt->filter_no = 0;
            char key_str[128];
            trunk_key_to_string(spl, target, key_str);
            trunk_async_set_state(ctxt, async_state_subbundle_lookup);
            // fallthrough
         }
         case async_state_subbundle_lookup:
         {
            if (ctxt->sb_no == ctxt->end_sb_no) {
               debug_assert(ctxt->filter_no == 0);
               ctxt->lookup_state = async_lookup_state_pivot;
               trunk_async_set_state(ctxt, async_state_pivot_lookup);
               break;
            }
            ctxt->sb = trunk_get_subbundle(spl, node, ctxt->sb_no);
            if (ctxt->sb->state == SB_STATE_COMPACTED) {
               ctxt->lookup_state = async_lookup_state_compacted_subbundle;
            } else {
               ctxt->lookup_state = async_lookup_state_subbundle;
            }
            debug_assert(ctxt->filter_no
                         < trunk_subbundle_filter_count(spl, node, ctxt->sb));
            ctxt->filter =
               trunk_subbundle_filter(spl, node, ctxt->sb, ctxt->filter_no);
            trunk_async_set_state(ctxt, async_state_filter_lookup_start);
            break;
         }
         case async_state_pivot_lookup:
         {
            ctxt->sb     = NULL;
            ctxt->filter = &ctxt->pdata->filter;
            trunk_async_set_state(ctxt, async_state_filter_lookup_start);
            // fall through
         }
         case async_state_filter_lookup_start:
         {
            ctxt->value = ROUTING_NOT_FOUND;
            if (ctxt->filter->addr == 0) {
               platform_assert(ctxt->lookup_state == async_lookup_state_pivot);
               trunk_async_set_state(ctxt, async_state_next_in_node);
               break;
            }
            if (spl->cfg.use_stats) {
               spl->stats[tid].filter_lookups[ctxt->height]++;
            }
            routing_filter_ctxt_init(&ctxt->filter_ctxt,
                                     &ctxt->cache_ctxt,
                                     trunk_filter_async_callback);
            trunk_async_set_state(ctxt, async_state_filter_lookup_reentrant);
            break;
         }
         case async_state_filter_lookup_reentrant:
         {
            // bool32 is_leaf;
            // switch (ctxt->lookup_state) {
            //    case async_lookup_state_pivot:
            //       is_leaf = ctxt->height == 0;
            //       break;
            //    case async_lookup_state_subbundle:
            //       debug_assert(ctxt->sb != NULL);
            //       is_leaf = ctxt->sb->state == SB_STATE_UNCOMPACTED_LEAF;
            //       break;
            //    case async_lookup_state_compacted_subbundle:
            //       is_leaf = FALSE;
            //       break;
            // }

            routing_config *filter_cfg = trunk_routing_cfg(spl);

            res = trunk_filter_lookup_async(spl,
                                            filter_cfg,
                                            ctxt->filter,
                                            target,
                                            &ctxt->found_values,
                                            &ctxt->filter_ctxt);
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
                  // I don't own the cache context, filter does
                  trunk_async_set_state(ctxt, async_state_btree_lookup_start);
                  break;
               default:
                  platform_assert(0);
            }
            break;
         }
         case async_state_btree_lookup_start:
         {
            uint16 branch_no;
            switch (ctxt->lookup_state) {
               case async_lookup_state_pivot:
                  debug_assert(ctxt->pdata != NULL);
                  ctxt->value = routing_filter_get_next_value(
                     ctxt->found_values, ctxt->value);
                  if (ctxt->value == ROUTING_NOT_FOUND) {
                     trunk_async_set_state(ctxt, async_state_next_in_node);
                     continue;
                  }
                  branch_no = trunk_add_branch_number(
                     spl, ctxt->pdata->start_branch, ctxt->value);
                  break;
               case async_lookup_state_subbundle:
                  debug_assert(ctxt->sb != NULL);
                  ctxt->value = routing_filter_get_next_value(
                     ctxt->found_values, ctxt->value);
                  if (ctxt->value == ROUTING_NOT_FOUND) {
                     trunk_async_set_state(ctxt, async_state_next_in_node);
                     continue;
                  }
                  branch_no = trunk_add_branch_number(
                     spl, ctxt->sb->start_branch, ctxt->value);
                  branch_no = ctxt->sb->start_branch + ctxt->value;
                  break;
               case async_lookup_state_compacted_subbundle:
                  debug_assert(ctxt->sb != NULL);
                  if (ctxt->found_values == 0) {
                     ctxt->value = ROUTING_NOT_FOUND;
                     trunk_async_set_state(ctxt, async_state_next_in_node);
                     continue;
                  }
                  branch_no = ctxt->sb->start_branch;
                  break;
               default:
                  platform_error_log("Invalid async_lookup_state=%d\n",
                                     ctxt->lookup_state);
                  platform_assert(0);
            }
            ctxt->branch = trunk_get_branch(spl, node, branch_no);
            btree_ctxt_init(&ctxt->btree_ctxt,
                            &ctxt->cache_ctxt,
                            trunk_btree_async_callback);
            trunk_async_set_state(ctxt, async_state_btree_lookup_reentrant);
            break;
         }
         case async_state_btree_lookup_reentrant:
         {
            res = trunk_btree_lookup_and_merge_async(
               spl, ctxt->branch, target, result, &ctxt->btree_ctxt);
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
                  // I don't own the cache context, btree does
                  if (merge_accumulator_is_definitive(result)) {
                     trunk_async_set_state(
                        ctxt, async_state_found_final_answer_early);
                     trunk_node_unget(spl->cc, &ctxt->trunk_node);
                     ZERO_CONTENTS(&ctxt->trunk_node);
                     break;
                  } else if (spl->cfg.use_stats) {
                     const uint16 height = trunk_node_height(node);
                     spl->stats[tid].filter_false_positives[height]++;
                  }
                  trunk_async_set_state(ctxt, async_state_next_in_node);
                  break;
               default:
                  platform_assert(0);
            }
            break;
         }
         case async_state_next_in_node:
         {
            switch (ctxt->lookup_state) {
               case async_lookup_state_pivot:
                  debug_assert(ctxt->filter_no == 0);
                  if (ctxt->value == ROUTING_NOT_FOUND) {
                     trunk_async_set_state(ctxt, async_state_trunk_node_done);
                  } else {
                     trunk_async_set_state(ctxt,
                                           async_state_btree_lookup_start);
                  }
                  continue;
               case async_lookup_state_subbundle:
                  debug_assert(ctxt->filter_no == 0);
                  if (ctxt->value == ROUTING_NOT_FOUND) {
                     ctxt->sb_no =
                        trunk_subtract_subbundle_number(spl, ctxt->sb_no, 1);
                     trunk_async_set_state(ctxt, async_state_subbundle_lookup);
                     break;
                  } else {
                     trunk_async_set_state(ctxt,
                                           async_state_btree_lookup_start);
                  }
                  continue;
               case async_lookup_state_compacted_subbundle:
                  if (ctxt->found_values != 0) {
                     ctxt->sb_no =
                        trunk_subtract_subbundle_number(spl, ctxt->sb_no, 1);
                     ctxt->filter_no = 0;
                  } else {
                     ctxt->filter_no++;
                     uint16 sb_filter_count =
                        trunk_subbundle_filter_count(spl, node, ctxt->sb);
                     if (ctxt->filter_no >= sb_filter_count) {
                        debug_assert(ctxt->filter_no == sb_filter_count);
                        ctxt->sb_no =
                           trunk_subtract_subbundle_number(spl, ctxt->sb_no, 1);
                        ctxt->filter_no = 0;
                     }
                  }
                  trunk_async_set_state(ctxt, async_state_subbundle_lookup);
                  continue;
               default:
                  platform_error_log("Invalid async_lookup_state=%d\n",
                                     ctxt->lookup_state);
                  platform_assert(0);
            }
            break;
         }
         case async_state_trunk_node_done:
         {
            if (ctxt->height == 0) {
               if (!merge_accumulator_is_null(result)
                   && merge_accumulator_message_class(result)
                         != MESSAGE_TYPE_INSERT)
               {
                  data_merge_tuples_final(spl->cfg.data_cfg, target, result);
               }
               trunk_async_set_state(ctxt, async_state_end);
               trunk_node_unget(spl->cc, &ctxt->trunk_node);
               ZERO_CONTENTS(&ctxt->trunk_node);
               break;
            } else {
               trunk_async_set_state(
                  ctxt, async_state_get_child_trunk_node_reentrant);
               break;
            }
         }
         case async_state_get_child_trunk_node_reentrant:
         {
            cache_ctxt_init(
               spl->cc, trunk_async_callback, NULL, &ctxt->cache_ctxt);
            debug_assert(ctxt->pdata != NULL);
            res = trunk_node_get_async(spl->cc, ctxt->pdata->addr, ctxt);
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
                  trunk_async_set_state(ctxt,
                                        async_state_unget_parent_trunk_node);
                  break;
               default:
                  platform_assert(0);
            }
            break;
         }
         case async_state_unget_parent_trunk_node:
         {
            if (ctxt->was_async) {
               trunk_node_async_done(spl, ctxt);
            }
            trunk_node_unget(spl->cc, node);
            ctxt->pdata           = NULL;
            ctxt->trunk_node.page = ctxt->cache_ctxt.page;
            ctxt->trunk_node.hdr  = (trunk_hdr *)(ctxt->cache_ctxt.page->data);
            trunk_async_set_state(ctxt, async_state_trunk_node_lookup);
            break;
         }
         case async_state_found_final_answer_early:
         {
            trunk_async_set_state(ctxt, async_state_end);
            break;
         }
         case async_state_end:
         {
            if (spl->cfg.use_stats) {
               if (!merge_accumulator_is_null(result)) {
                  spl->stats[tid].lookups_found++;
               } else {
                  spl->stats[tid].lookups_not_found++;
               }
            }

            if (!merge_accumulator_is_null(result)) {
               message_type type = merge_accumulator_message_class(result);
               debug_assert(type == MESSAGE_TYPE_DELETE
                            || type == MESSAGE_TYPE_INSERT);
               if (type == MESSAGE_TYPE_DELETE) {
                  merge_accumulator_set_to_null(result);
               }
            }

            res  = async_success;
            done = TRUE;
            break;
         }
         default:
            platform_assert(0);
      }
   } while (!done);
#if TRUNK_DEBUG
   cache_enable_sync_get(spl->cc, TRUE);
#endif

   return res;
}


platform_status
trunk_range(trunk_handle  *spl,
            key            start_key,
            uint64         num_tuples,
            tuple_function func,
            void          *arg)
{
   trunk_range_iterator *range_itor =
      TYPED_MALLOC(PROCESS_PRIVATE_HEAP_ID, range_itor);
   platform_status rc = trunk_range_iterator_init(spl,
                                                  range_itor,
                                                  start_key,
                                                  POSITIVE_INFINITY_KEY,
                                                  start_key,
                                                  greater_than_or_equal,
                                                  num_tuples);
   if (!SUCCESS(rc)) {
      goto destroy_range_itor;
   }

   for (int i = 0; i < num_tuples && iterator_can_next(&range_itor->super); i++)
   {
      key     curr_key;
      message data;
      iterator_curr(&range_itor->super, &curr_key, &data);
      func(curr_key, data, arg);
      rc = iterator_next(&range_itor->super);
      if (!SUCCESS(rc)) {
         goto destroy_range_itor;
      }
   }

destroy_range_itor:
   trunk_range_iterator_deinit(range_itor);
   platform_free(PROCESS_PRIVATE_HEAP_ID, range_itor);
   return rc;
}


/*
 *-----------------------------------------------------------------------------
 * Create/destroy
 * XXX Fix this api to return platform_status
 *-----------------------------------------------------------------------------
 */
trunk_handle *
trunk_create(trunk_config     *cfg,
             allocator        *al,
             cache            *cc,
             task_system      *ts,
             allocator_root_id id,
             platform_heap_id  hid)
{
   trunk_handle *spl = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, spl, compacted_memtable, TRUNK_NUM_MEMTABLES);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   // Validate configured key-size is within limits.
   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   platform_batch_rwlock_init(&spl->trunk_root_lock);

   srq_init(&spl->srq, platform_get_module_id(), hid);

   // get a free node for the root
   //    we don't use the mini allocator for this, since the root doesn't
   //    maintain constant height
   uint64          root_addr;
   platform_status rc = allocator_alloc(spl->al, &root_addr, PAGE_TYPE_TRUNK);
   spl->root_addr     = root_addr;
   platform_assert_status_ok(rc);
   trunk_node root;
   root.addr = spl->root_addr;
   root.page = cache_alloc(spl->cc, root.addr, PAGE_TYPE_TRUNK);
   root.hdr  = (trunk_hdr *)root.page->data;

   ZERO_CONTENTS(root.hdr);

   // set up the mini allocator
   //    we use the root extent as the initial mini_allocator head
   uint64 meta_addr = spl->root_addr + trunk_page_size(cfg);
   mini_init(&spl->mini,
             cc,
             spl->cfg.data_cfg,
             meta_addr,
             0,
             TRUNK_MAX_HEIGHT,
             PAGE_TYPE_TRUNK);

   // set up the memtable context
   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   spl->mt_ctxt            = memtable_context_create(
      spl->heap_id, cc, mt_cfg, trunk_memtable_flush_virtual, spl);

   // set up the log
   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
   }

   // ALEX: For now we assume an init means destroying any present super blocks
   trunk_set_super_block(spl, FALSE, FALSE, TRUE);

   // set up the initial leaf
   trunk_node leaf;
   trunk_alloc(spl->cc, &spl->mini, 0, &leaf);
   memset(leaf.hdr, 0, trunk_page_size(&spl->cfg));
   trunk_set_initial_pivots(spl, &leaf);
   trunk_inc_pivot_generation(spl, &leaf);

   // add leaf to root and fix up root
   root.hdr->height = 1;
   trunk_add_pivot_new_root(spl, &root, &leaf);
   trunk_inc_pivot_generation(spl, &root);

   trunk_node_unlock(spl->cc, &leaf);
   trunk_node_unclaim(spl->cc, &leaf);
   trunk_node_unget(spl->cc, &leaf);

   trunk_node_unlock(spl->cc, &root);
   trunk_node_unclaim(spl->cc, &root);
   trunk_node_unget(spl->cc, &root);

   trunk_node_context_init(
      &spl->trunk_context, &spl->cfg.trunk_node_cfg, hid, cc, al, ts, 0);

   if (spl->cfg.use_stats) {
      spl->stats = TYPED_ARRAY_ZALLOC(spl->heap_id, spl->stats, MAX_THREADS);
      platform_assert(spl->stats);
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_status rc;
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[i].insert_latency_histo);
         platform_assert_status_ok(rc);
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[i].update_latency_histo);
         platform_assert_status_ok(rc);
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[i].delete_latency_histo);
         platform_assert_status_ok(rc);
      }
   }

   return spl;
}

/*
 * Open (mount) an existing splinter database
 */
trunk_handle *
trunk_mount(trunk_config     *cfg,
            allocator        *al,
            cache            *cc,
            task_system      *ts,
            allocator_root_id id,
            platform_heap_id  hid)
{
   trunk_handle *spl = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, spl, compacted_memtable, TRUNK_NUM_MEMTABLES);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   srq_init(&spl->srq, platform_get_module_id(), hid);

   platform_batch_rwlock_init(&spl->trunk_root_lock);

   // find the unmounted super block
   spl->root_addr                      = 0;
   uint64             latest_timestamp = 0;
   page_handle       *super_page;
   trunk_super_block *super = trunk_get_super_block_if_valid(spl, &super_page);
   if (super != NULL) {
      if (super->unmounted && super->timestamp > latest_timestamp) {
         spl->root_addr    = super->root_addr;
         spl->next_node_id = super->next_node_id;
         latest_timestamp  = super->timestamp;
      }
      trunk_release_super_block(spl, super_page);
   }

   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   spl->mt_ctxt            = memtable_context_create(
      spl->heap_id, cc, mt_cfg, trunk_memtable_flush_virtual, spl);

   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
   }

   trunk_set_super_block(spl, FALSE, FALSE, FALSE);

   trunk_node_context_init(&spl->trunk_context,
                           &spl->cfg.trunk_node_cfg,
                           hid,
                           cc,
                           al,
                           ts,
                           spl->root_addr);

   if (spl->cfg.use_stats) {
      spl->stats = TYPED_ARRAY_ZALLOC(spl->heap_id, spl->stats, MAX_THREADS);
      platform_assert(spl->stats);
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_status rc;
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[i].insert_latency_histo);
         platform_assert_status_ok(rc);
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[i].update_latency_histo);
         platform_assert_status_ok(rc);
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[i].delete_latency_histo);
         platform_assert_status_ok(rc);
      }
   }
   return spl;
}

/*
 * This function is only safe to call when all other calls to spl have returned
 * and all tasks have been complete.
 */
void
trunk_prepare_for_shutdown(trunk_handle *spl)
{
   // write current memtable to disk
   // (any others must already be flushing/flushed)

   if (!memtable_is_empty(spl->mt_ctxt)) {
      /*
       * memtable_force_finalize is not thread safe. Note also, we do not hold
       * the insert lock or rotate while flushing the memtable.
       */

      uint64 generation = memtable_force_finalize(spl->mt_ctxt);
      trunk_memtable_flush(spl, generation);
   }

   // finish any outstanding tasks and destroy task system for this table.
   platform_status rc = task_perform_until_quiescent(spl->ts);
   platform_assert_status_ok(rc);

   // destroy memtable context (and its memtables)
   memtable_context_destroy(spl->heap_id, spl->mt_ctxt);

   // release the log
   if (spl->cfg.use_log) {
      platform_free(spl->heap_id, spl->log);
   }

   // release the trunk mini allocator
   mini_release(&spl->mini);

   // flush all dirty pages in the cache
   cache_flush(spl->cc);
}

static bool32
trunk_destroy_node(trunk_handle *spl, uint64 addr, void *arg)
{
   trunk_node node;
   trunk_node_get(spl->cc, addr, &node);
   trunk_node_claim(spl->cc, &node);
   trunk_node_lock(spl->cc, &node);
   uint16 num_children = trunk_num_children(spl, &node);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      trunk_pivot_data *pdata = trunk_get_pivot_data(spl, &node, pivot_no);
      if (pdata->filter.addr != 0) {
         trunk_dec_filter(spl, &pdata->filter);
      }
      for (uint16 branch_no = pdata->start_branch;
           branch_no != trunk_end_branch(spl, &node);
           branch_no = trunk_add_branch_number(spl, branch_no, 1))
      {
         trunk_branch *branch    = trunk_get_branch(spl, &node, branch_no);
         key           start_key = trunk_get_pivot(spl, &node, pivot_no);
         key           end_key   = trunk_get_pivot(spl, &node, pivot_no + 1);

         trunk_zap_branch_range(
            spl, branch, start_key, end_key, PAGE_TYPE_BRANCH);
      }
   }
   uint16 start_filter = trunk_start_sb_filter(spl, &node);
   uint16 end_filter   = trunk_end_sb_filter(spl, &node);
   for (uint16 filter_no = start_filter; filter_no != end_filter; filter_no++) {
      routing_filter *filter = trunk_get_sb_filter(spl, &node, filter_no);
      trunk_dec_filter(spl, filter);
   }

   trunk_node_unlock(spl->cc, &node);
   trunk_node_unclaim(spl->cc, &node);
   trunk_node_unget(spl->cc, &node);
   return TRUE;
}

/*
 * Destroy a database such that it cannot be re-opened later
 */
void
trunk_destroy(trunk_handle *spl)
{
   srq_deinit(&spl->srq);
   trunk_prepare_for_shutdown(spl);
   trunk_node_context_deinit(&spl->trunk_context);
   trunk_for_each_node(spl, trunk_destroy_node, NULL);
   mini_dec_ref(spl->cc, spl->mini.meta_head, PAGE_TYPE_TRUNK, FALSE);
   // clear out this splinter table from the meta page.
   allocator_remove_super_addr(spl->al, spl->id);

   if (spl->cfg.use_stats) {
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[i].insert_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[i].update_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[i].delete_latency_histo);
      }
      platform_free(spl->heap_id, spl->stats);
   }
   platform_free(spl->heap_id, spl);
}

/*
 * Close (unmount) a database without destroying it.
 * It can be re-opened later with trunk_mount().
 */
void
trunk_unmount(trunk_handle **spl_in)
{
   trunk_handle *spl = *spl_in;
   srq_deinit(&spl->srq);
   trunk_prepare_for_shutdown(spl);
   trunk_set_super_block(spl, FALSE, TRUE, FALSE);
   if (spl->cfg.use_stats) {
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[i].insert_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[i].update_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[i].delete_latency_histo);
      }
      platform_free(spl->heap_id, spl->stats);
   }
   platform_free(spl->heap_id, spl);
   *spl_in = (trunk_handle *)NULL;
}

/*
 *-----------------------------------------------------------------------------
 * trunk_perform_task
 *
 *      do a batch of tasks
 *-----------------------------------------------------------------------------
 */
void
trunk_perform_tasks(trunk_handle *spl)
{
   task_perform_all(spl->ts);
   cache_cleanup(spl->cc);
}

/*
 *-----------------------------------------------------------------------------
 * Debugging and info functions
 *-----------------------------------------------------------------------------
 */

/*
 * verify_tree verifies each node with itself and its neighbors
 */
bool32
trunk_verify_tree(trunk_handle *spl)
{
   platform_default_log("trunk_verify_tree not implemented");
   return TRUE;
}

/*
 * Returns the amount of space used by each level of the tree
 */
static bool32
trunk_node_space_use(trunk_handle *spl, uint64 addr, void *arg)
{
   uint64    *bytes_used_on_level = (uint64 *)arg;
   uint64     bytes_used_in_node  = 0;
   trunk_node node;
   trunk_node_get(spl->cc, addr, &node);
   uint16 num_pivot_keys = trunk_num_pivot_keys(spl, &node);
   uint16 num_children   = trunk_num_children(spl, &node);
   for (uint16 branch_no = trunk_start_branch(spl, &node);
        branch_no != trunk_end_branch(spl, &node);
        branch_no = trunk_add_branch_number(spl, branch_no, 1))
   {
      trunk_branch *branch    = trunk_get_branch(spl, &node, branch_no);
      key           start_key = NULL_KEY;
      key           end_key   = NULL_KEY;
      for (uint16 pivot_no = 0; pivot_no < num_pivot_keys; pivot_no++) {
         if (1 && pivot_no != num_children
             && trunk_branch_live_for_pivot(spl, &node, branch_no, pivot_no))
         {
            if (key_is_null(start_key)) {
               start_key = trunk_get_pivot(spl, &node, pivot_no);
            }
         } else {
            if (!key_is_null(start_key)) {
               end_key = trunk_get_pivot(spl, &node, pivot_no);
               uint64 bytes_used_in_branch_range =
                  btree_space_use_in_range(spl->cc,
                                           &spl->cfg.btree_cfg,
                                           branch->root_addr,
                                           PAGE_TYPE_BRANCH,
                                           start_key,
                                           end_key);
               bytes_used_in_node += bytes_used_in_branch_range;
            }
            start_key = NULL_KEY;
            end_key   = NULL_KEY;
         }
      }
   }

   uint16 height = trunk_node_height(&node);
   bytes_used_on_level[height] += bytes_used_in_node;
   trunk_node_unget(spl->cc, &node);
   return TRUE;
}

void
trunk_print_space_use(platform_log_handle *log_handle, trunk_handle *spl)
{
   uint64 bytes_used_by_level[TRUNK_MAX_HEIGHT] = {0};
   trunk_for_each_node(spl, trunk_node_space_use, bytes_used_by_level);

   platform_log(log_handle,
                "Space used by level: trunk_tree_height=%d\n",
                trunk_tree_height(spl));
   for (uint16 i = 0; i <= trunk_tree_height(spl); i++) {
      platform_log(log_handle,
                   "%u: %lu bytes (%s)\n",
                   i,
                   bytes_used_by_level[i],
                   size_str(bytes_used_by_level[i]));
   }
   platform_log(log_handle, "\n");
}


/*
 * trunk_print_memtable() --
 *
 * Print the currently active Memtable, and the other Memtables being processed.
 * Memtable printing will drill-down to BTree printing which will keep
 * recursing.
 */
static void
trunk_print_memtable(platform_log_handle *log_handle, trunk_handle *spl)
{
   uint64 curr_memtable =
      memtable_generation(spl->mt_ctxt) % TRUNK_NUM_MEMTABLES;
   platform_log(log_handle, "&&&&&&&&&&&&&&&&&&&\n");
   platform_log(log_handle, "&&  MEMTABLES \n");
   platform_log(log_handle, "&&  curr: %lu\n", curr_memtable);
   platform_log(log_handle, "-------------------\n{\n");

   uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
   uint64 mt_gen_end   = memtable_generation_retired(spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      memtable *mt = trunk_get_memtable(spl, mt_gen);
      platform_log(log_handle,
                   "Memtable root_addr=%lu: gen %lu ref_count %u state %d\n",
                   mt->root_addr,
                   mt_gen,
                   allocator_get_refcount(spl->al, mt->root_addr),
                   mt->state);

      memtable_print(log_handle, spl->cc, mt);
   }
   platform_log(log_handle, "\n}\n");
}

/*
 * trunk_print()
 *
 * Driver routine to print a SplinterDB trunk, and all its sub-pages.
 */
void
trunk_print(platform_log_handle *log_handle, trunk_handle *spl)
{
   trunk_print_memtable(log_handle, spl);
   platform_default_log("trunk_print not implemented");
}

/*
 * trunk_print_super_block()
 *
 * Fetch a super-block for a running Splinter instance, and print its
 * contents.
 */
void
trunk_print_super_block(platform_log_handle *log_handle, trunk_handle *spl)
{
   page_handle       *super_page;
   trunk_super_block *super = trunk_get_super_block_if_valid(spl, &super_page);
   if (super == NULL) {
      return;
   }

   platform_log(log_handle, "Superblock root_addr=%lu {\n", super->root_addr);
   platform_log(log_handle,
                "meta_tail=%lu log_addr=%lu log_meta_addr=%lu\n",
                super->meta_tail,
                super->meta_tail,
                super->log_meta_addr);
   platform_log(log_handle,
                "timestamp=%lu, checkpointed=%d, unmounted=%d\n",
                super->timestamp,
                super->checkpointed,
                super->unmounted);
   platform_log(log_handle, "}\n\n");
   trunk_release_super_block(spl, super_page);
}

// clang-format off
void
trunk_print_insertion_stats(platform_log_handle *log_handle, trunk_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   uint64 avg_flush_wait_time, avg_flush_time, num_flushes;
   uint64 avg_compaction_tuples, pack_time_per_tuple, avg_setup_time;
   uint64 avg_filter_tuples, avg_filter_time, filter_time_per_tuple;
   uint32 h;
   threadid thr_i;
   trunk_node node;
   trunk_node_get(spl->cc, spl->root_addr, &node);
   uint32 height = trunk_node_height(&node);
   trunk_node_unget(spl->cc, &node);

   trunk_stats *global;

   global = TYPED_ZALLOC(spl->heap_id, global);
   if (global == NULL) {
      platform_error_log("Out of memory for statistics");
      return;
   }

   platform_histo_handle insert_lat_accum, update_lat_accum, delete_lat_accum;
   platform_histo_create(spl->heap_id,
                         LATENCYHISTO_SIZE + 1,
                         latency_histo_buckets,
                         &insert_lat_accum);
   platform_histo_create(spl->heap_id,
                         LATENCYHISTO_SIZE + 1,
                         latency_histo_buckets,
                         &update_lat_accum);
   platform_histo_create(spl->heap_id,
                         LATENCYHISTO_SIZE + 1,
                         latency_histo_buckets,
                         &delete_lat_accum);

   for (thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      platform_histo_merge_in(insert_lat_accum,
                              spl->stats[thr_i].insert_latency_histo);
      platform_histo_merge_in(update_lat_accum,
                              spl->stats[thr_i].update_latency_histo);
      platform_histo_merge_in(delete_lat_accum,
                              spl->stats[thr_i].delete_latency_histo);
      for (h = 0; h <= height; h++) {
         global->root_compactions                    += spl->stats[thr_i].root_compactions;
         global->root_compaction_pack_time_ns        += spl->stats[thr_i].root_compaction_pack_time_ns;
         global->root_compaction_tuples              += spl->stats[thr_i].root_compaction_tuples;
         if (spl->stats[thr_i].root_compaction_max_tuples >
               global->root_compaction_max_tuples) {
            global->root_compaction_max_tuples =
               spl->stats[thr_i].root_compaction_max_tuples;
         }
         global->root_compaction_time_ns             += spl->stats[thr_i].root_compaction_time_ns;
         if (spl->stats[thr_i].root_compaction_time_max_ns >
               global->root_compaction_time_max_ns) {
            global->root_compaction_time_max_ns =
               spl->stats[thr_i].root_compaction_time_max_ns;
         }

      }
      global->insertions                  += spl->stats[thr_i].insertions;
      global->updates                     += spl->stats[thr_i].updates;
      global->deletions                   += spl->stats[thr_i].deletions;
      global->discarded_deletes           += spl->stats[thr_i].discarded_deletes;

      global->memtable_flushes            += spl->stats[thr_i].memtable_flushes;
      global->memtable_flush_wait_time_ns += spl->stats[thr_i].memtable_flush_wait_time_ns;
      global->memtable_flush_time_ns      += spl->stats[thr_i].memtable_flush_time_ns;
      if (spl->stats[thr_i].memtable_flush_time_max_ns >
          global->memtable_flush_time_max_ns) {
         global->memtable_flush_time_max_ns =
            spl->stats[thr_i].memtable_flush_time_max_ns;
      }
      global->memtable_flush_root_full    += spl->stats[thr_i].memtable_flush_root_full;

      global->root_filters_built          += spl->stats[thr_i].root_filters_built;
      global->root_filter_tuples          += spl->stats[thr_i].root_filter_tuples;
      global->root_filter_time_ns         += spl->stats[thr_i].root_filter_time_ns;
   }

   platform_log(log_handle, "Overall Statistics\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| insertions:        %10lu\n", global->insertions);
   platform_log(log_handle, "| updates:           %10lu\n", global->updates);
   platform_log(log_handle, "| deletions:         %10lu\n", global->deletions);
   platform_log(log_handle, "| completed deletes: %10lu\n", global->discarded_deletes);
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| root stalls:       %10lu\n", global->memtable_flush_root_full);
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");

   platform_log(log_handle, "Latency Histogram Statistics\n");
   platform_histo_print(insert_lat_accum, "Insert Latency Histogram (ns):", log_handle);
   platform_histo_print(update_lat_accum, "Update Latency Histogram (ns):", log_handle);
   platform_histo_print(delete_lat_accum, "Delete Latency Histogram (ns):", log_handle);
   platform_histo_destroy(spl->heap_id, &insert_lat_accum);
   platform_histo_destroy(spl->heap_id, &update_lat_accum);
   platform_histo_destroy(spl->heap_id, &delete_lat_accum);


   platform_log(log_handle, "Flush Statistics\n");
   platform_log(log_handle, "---------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "  height | avg wait time (ns) | avg flush time (ns) | max flush time (ns) | full flushes | count flushes |\n");
   platform_log(log_handle, "---------|--------------------|---------------------|---------------------|--------------|---------------|\n");

   // memtable
   num_flushes = global->memtable_flushes;
   avg_flush_wait_time = num_flushes == 0 ? 0 : global->memtable_flush_wait_time_ns / num_flushes;
   avg_flush_time = num_flushes == 0 ? 0 : global->memtable_flush_time_ns / num_flushes;
   platform_log(log_handle, "memtable | %18lu | %19lu | %19lu | %12lu | %13lu |\n",
                avg_flush_wait_time, avg_flush_time,
                global->memtable_flush_time_max_ns, num_flushes, 0UL);

   platform_log(log_handle, "---------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");

   platform_log(log_handle, "Compaction Statistics\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "  height | compactions | avg setup time (ns) | time / tuple (ns) | avg tuples | max tuples | max time (ns) | empty | aborted | discarded |\n");
   platform_log(log_handle, "---------|-------------|---------------------|-------------------|------------|------------|---------------|-------|---------|-----------|\n");

   avg_setup_time = global->root_compactions == 0 ? 0
      : (global->root_compaction_time_ns - global->root_compaction_pack_time_ns)
            / global->root_compactions;
   avg_compaction_tuples = global->root_compactions == 0 ? 0
      : global->root_compaction_tuples / global->root_compactions;
   pack_time_per_tuple = global->root_compaction_tuples == 0 ? 0
      : global->root_compaction_pack_time_ns / global->root_compaction_tuples;
   platform_log(log_handle, "    root | %11lu | %19lu | %17lu | %10lu | %10lu | %13lu | %5lu | %2lu | %2lu | %3lu | %3lu |\n",
         global->root_compactions, avg_setup_time, pack_time_per_tuple,
         avg_compaction_tuples, global->root_compaction_max_tuples,
         global->root_compaction_time_max_ns, 0UL, 0UL, 0UL, 0UL, 0UL);
   platform_log(log_handle, "------------------------------------------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");

   platform_log(log_handle, "Filter Build Statistics\n");
   platform_log(log_handle, "---------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| height |   built | avg tuples | avg build time (ns) | build_time / tuple (ns) |\n");
   platform_log(log_handle, "---------|---------|------------|---------------------|-------------------------|\n");

   avg_filter_tuples = global->root_filters_built == 0 ? 0 :
      global->root_filter_tuples / global->root_filters_built;
   avg_filter_time = global->root_filters_built == 0 ? 0 :
      global->root_filter_time_ns / global->root_filters_built;
   filter_time_per_tuple = global->root_filter_tuples == 0 ? 0 :
      global->root_filter_time_ns / global->root_filter_tuples;

   platform_log(log_handle, "|   root | %7lu | %10lu | %19lu | %23lu |\n",
         global->root_filters_built, avg_filter_tuples,
         avg_filter_time, filter_time_per_tuple);

   trunk_node_print_insertion_stats(log_handle, &spl->trunk_context);

   task_print_stats(spl->ts);
   platform_log(log_handle, "\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   cache_print_stats(log_handle, spl->cc);
   platform_log(log_handle, "\n");
   platform_free(spl->heap_id, global);
}

void
trunk_print_lookup_stats(platform_log_handle *log_handle, trunk_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   threadid thr_i;
   uint32 h, rev_h;
   uint64 lookups;
   fraction avg_filter_lookups, avg_filter_false_positives, avg_branch_lookups;
   trunk_node node;
   trunk_node_get(spl->cc, spl->root_addr, &node);
   uint32 height = trunk_node_height(&node);
   trunk_node_unget(spl->cc, &node);

   trunk_stats *global;

   global = TYPED_ZALLOC(spl->heap_id, global);
   if (global == NULL) {
      platform_error_log("Out of memory for stats\n");
      return;
   }

   for (thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      for (h = 0; h <= height; h++) {
         global->filter_lookups[h]         += spl->stats[thr_i].filter_lookups[h];
         global->branch_lookups[h]         += spl->stats[thr_i].branch_lookups[h];
         global->filter_false_positives[h] += spl->stats[thr_i].filter_false_positives[h];
         global->filter_negatives[h]       += spl->stats[thr_i].filter_negatives[h];
      }
      global->lookups_found     += spl->stats[thr_i].lookups_found;
      global->lookups_not_found += spl->stats[thr_i].lookups_not_found;
   }
   lookups = global->lookups_found + global->lookups_not_found;

   platform_log(log_handle, "Overall Statistics\n");
   platform_log(log_handle, "-----------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| height:            %u\n", height);
   platform_log(log_handle, "| lookups:           %lu\n", lookups);
   platform_log(log_handle, "| lookups found:     %lu\n", global->lookups_found);
   platform_log(log_handle, "| lookups not found: %lu\n", global->lookups_not_found);
   platform_log(log_handle, "-----------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");

   platform_log(log_handle, "Filter/Branch Statistics\n");
   platform_log(log_handle, "-------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "height   | avg filter lookups | avg false pos | false pos rate | avg branch lookups |\n");
   platform_log(log_handle, "---------|--------------------|---------------|----------------|--------------------|\n");

   for (h = 0; h <= height; h++) {
      rev_h = height - h;
      if (lookups == 0) {
         avg_filter_lookups = zero_fraction;
         avg_filter_false_positives = zero_fraction;
         avg_branch_lookups = zero_fraction;
      } else {
         avg_filter_lookups =
            init_fraction(global->filter_lookups[rev_h], lookups);
         avg_filter_false_positives =
            init_fraction(global->filter_false_positives[rev_h], lookups);
         avg_branch_lookups = init_fraction(global->branch_lookups[rev_h],
                                            lookups);
      }

      uint64 filter_negatives = global->filter_lookups[rev_h];
      fraction false_positives_in_revision;
      if (filter_negatives == 0) {
         false_positives_in_revision = zero_fraction;
      } else {
         false_positives_in_revision =
         init_fraction(global->filter_false_positives[rev_h],
                       filter_negatives);
      }
      platform_log(log_handle, "%8u | "FRACTION_FMT(18, 2)" | "FRACTION_FMT(13, 4)" | "
                   FRACTION_FMT(14, 4)" | "FRACTION_FMT(18, 4)"\n",
                   rev_h, FRACTION_ARGS(avg_filter_lookups),
                   FRACTION_ARGS(avg_filter_false_positives),
                   FRACTION_ARGS(false_positives_in_revision),
                   FRACTION_ARGS(avg_branch_lookups));
   }
   platform_log(log_handle, "------------------------------------------------------------------------------------|\n");
   platform_log(log_handle, "\n");
   platform_free(spl->heap_id, global);
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   cache_print_stats(log_handle, spl->cc);
   platform_log(log_handle, "\n");
}
// clang-format on


void
trunk_print_lookup(trunk_handle        *spl,
                   key                  target,
                   platform_log_handle *log_handle)
{
   merge_accumulator data;
   merge_accumulator_init(&data, spl->heap_id);

   platform_stream_handle stream;
   platform_open_log_stream(&stream);
   uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
   uint64 mt_gen_end   = memtable_generation_retired(spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      bool32 memtable_is_compacted;
      uint64 root_addr = trunk_memtable_root_addr_for_lookup(
         spl, mt_gen, &memtable_is_compacted);
      platform_status rc;

      rc = btree_lookup(spl->cc,
                        &spl->cfg.btree_cfg,
                        root_addr,
                        PAGE_TYPE_MEMTABLE,
                        target,
                        &data);
      platform_assert_status_ok(rc);
      if (!merge_accumulator_is_null(&data)) {
         char    key_str[128];
         char    message_str[128];
         message msg = merge_accumulator_to_message(&data);
         trunk_key_to_string(spl, target, key_str);
         trunk_message_to_string(spl, msg, message_str);
         platform_log_stream(
            &stream,
            "Key %s found in memtable %lu (gen %lu comp %d) with data %s\n",
            key_str,
            root_addr,
            mt_gen,
            memtable_is_compacted,
            message_str);
         btree_print_lookup(spl->cc,
                            &spl->cfg.btree_cfg,
                            root_addr,
                            PAGE_TYPE_MEMTABLE,
                            target);
      }
   }

   platform_assert(0, "Not implemented");
}

void
trunk_reset_stats(trunk_handle *spl)
{
   if (spl->cfg.use_stats) {
      for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[thr_i].insert_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[thr_i].update_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                &spl->stats[thr_i].delete_latency_histo);

         memset(&spl->stats[thr_i], 0, sizeof(spl->stats[thr_i]));

         platform_status rc;
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[thr_i].insert_latency_histo);
         platform_assert_status_ok(rc);
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[thr_i].update_latency_histo);
         platform_assert_status_ok(rc);
         rc = platform_histo_create(spl->heap_id,
                                    LATENCYHISTO_SIZE + 1,
                                    latency_histo_buckets,
                                    &spl->stats[thr_i].delete_latency_histo);
         platform_assert_status_ok(rc);
      }
   }
}

// basic validation of data_config
static void
trunk_validate_data_config(const data_config *cfg)
{
   platform_assert(cfg->key_compare != NULL);
}

/*
 *-----------------------------------------------------------------------------
 * trunk_config_init --
 *
 *       Initialize splinter config
 *       This function calls btree_config_init
 *-----------------------------------------------------------------------------
 */
platform_status
trunk_config_init(trunk_config        *trunk_cfg,
                  cache_config        *cache_cfg,
                  data_config         *data_cfg,
                  log_config          *log_cfg,
                  uint64               memtable_capacity,
                  uint64               fanout,
                  uint64               max_branches_per_node,
                  uint64               btree_rough_count_height,
                  uint64               filter_remainder_size,
                  uint64               filter_index_size,
                  uint64               reclaim_threshold,
                  uint64               queue_scale_percent,
                  bool32               use_log,
                  bool32               use_stats,
                  bool32               verbose_logging,
                  platform_log_handle *log_handle)

{
   trunk_validate_data_config(data_cfg);

   platform_status rc = STATUS_BAD_PARAM;
   uint64          trunk_pivot_size;
   uint64          bytes_for_branches;
   routing_config *filter_cfg = &trunk_cfg->filter_cfg;

   ZERO_CONTENTS(trunk_cfg);
   trunk_cfg->cache_cfg = cache_cfg;
   trunk_cfg->data_cfg  = data_cfg;
   trunk_cfg->log_cfg   = log_cfg;

   trunk_cfg->fanout                  = fanout;
   trunk_cfg->max_branches_per_node   = max_branches_per_node;
   trunk_cfg->reclaim_threshold       = reclaim_threshold;
   trunk_cfg->queue_scale_percent     = queue_scale_percent;
   trunk_cfg->use_log                 = use_log;
   trunk_cfg->use_stats               = use_stats;
   trunk_cfg->verbose_logging_enabled = verbose_logging;
   trunk_cfg->log_handle              = log_handle;

   // Inline what we would get from trunk_pivot_size(trunk_handle *).
   trunk_pivot_size = data_cfg->max_key_size + sizeof(trunk_pivot_data);

   // Setting hard limit and check configuration for over-provisioning
   trunk_cfg->max_pivot_keys = trunk_cfg->fanout + TRUNK_EXTRA_PIVOT_KEYS;
   uint64 header_bytes       = sizeof(trunk_hdr);

   uint64 pivot_bytes = (trunk_cfg->max_pivot_keys
                         * (data_cfg->max_key_size + sizeof(trunk_pivot_data)));
   uint64 branch_bytes =
      trunk_cfg->max_branches_per_node * sizeof(trunk_branch);
   uint64 trunk_node_min_size   = header_bytes + pivot_bytes + branch_bytes;
   uint64 page_size             = cache_config_page_size(cache_cfg);
   uint64 available_pivot_bytes = page_size - header_bytes - branch_bytes;
   uint64 available_bytes_per_pivot =
      available_pivot_bytes / trunk_cfg->max_pivot_keys;

   // Deal with mis-configurations where we don't have available bytes per
   // pivot key
   uint64 available_bytes_per_pivot_key = 0;
   if (available_bytes_per_pivot > sizeof(trunk_pivot_data)) {
      available_bytes_per_pivot_key =
         available_bytes_per_pivot - sizeof(trunk_pivot_data);
   }

   if (trunk_node_min_size >= page_size) {
      platform_error_log("Trunk node min size=%lu bytes "
                         "does not fit in page size=%lu bytes as configured.\n"
                         "node->hdr: %lu bytes, "
                         "pivots: %lu bytes (max_pivot=%lu x %lu bytes),\n"
                         "branches %lu bytes (max_branches=%lu x %lu bytes).\n"
                         "Maximum key size supported with current "
                         "configuration: %lu bytes.\n",
                         trunk_node_min_size,
                         page_size,
                         header_bytes,
                         pivot_bytes,
                         trunk_cfg->max_pivot_keys,
                         trunk_pivot_size,
                         branch_bytes,
                         max_branches_per_node,
                         sizeof(trunk_branch),
                         available_bytes_per_pivot_key);
      return rc;
   }

   // Space left for branches past end of pivot array of [max_pivot_keys]
   bytes_for_branches = (page_size - trunk_hdr_size()
                         - (trunk_cfg->max_pivot_keys * trunk_pivot_size));

   // Internally determined hard-limit, which effectively depends on the
   // - configured page size and trunk header size
   // - user-specified configured key size
   // - user-specified fanout
   trunk_cfg->hard_max_branches_per_node =
      bytes_for_branches / sizeof(trunk_branch) - 1;

   // Initialize point message btree
   btree_config_init(&trunk_cfg->btree_cfg, cache_cfg, trunk_cfg->data_cfg);

   memtable_config_init(&trunk_cfg->mt_cfg,
                        &trunk_cfg->btree_cfg,
                        TRUNK_NUM_MEMTABLES,
                        memtable_capacity);

   // Has to be set after btree_config_init is called
   trunk_cfg->max_kv_bytes_per_node =
      trunk_cfg->fanout * trunk_cfg->mt_cfg.max_extents_per_memtable
      * cache_config_extent_size(cache_cfg) / MEMTABLE_SPACE_OVERHEAD_FACTOR;
   trunk_cfg->target_leaf_kv_bytes = trunk_cfg->max_kv_bytes_per_node / 2;
   trunk_cfg->max_tuples_per_node  = trunk_cfg->max_kv_bytes_per_node / 32;

   // filter config settings
   filter_cfg->cache_cfg = cache_cfg;

   filter_cfg->index_size     = filter_index_size;
   filter_cfg->seed           = 42;
   filter_cfg->hash           = trunk_cfg->data_cfg->key_hash;
   filter_cfg->data_cfg       = trunk_cfg->data_cfg;
   filter_cfg->log_index_size = 31 - __builtin_clz(filter_cfg->index_size);

   uint64 filter_max_fingerprints = trunk_cfg->max_tuples_per_node;
   uint64 filter_quotient_size = 64 - __builtin_clzll(filter_max_fingerprints);
   uint64 filter_fingerprint_size =
      filter_remainder_size + filter_quotient_size;
   filter_cfg->fingerprint_size = filter_fingerprint_size;
   uint64 max_value             = trunk_cfg->max_branches_per_node;
   size_t max_value_size        = 64 - __builtin_clzll(max_value);

   if (filter_fingerprint_size > 32 - max_value_size) {
      platform_default_log(
         "Fingerprint size %lu too large, max value size is %lu, "
         "setting to %lu\n",
         filter_fingerprint_size,
         max_value_size,
         32 - max_value_size);
      filter_cfg->fingerprint_size = 32 - max_value_size;
   }

   /*
    * Set filter index size
    *
    * In quick_filter_init() we have this assert:
    *   index / addrs_per_page < cfg->extent_size / cfg->page_size
    * where
    *   - cfg is of type quick_filter_config
    *   - index is less than num_indices, which equals to params.num_buckets /
    *     cfg->index_size. params.num_buckets should be less than
    *     trunk_cfg.max_tuples_per_node
    *   - addrs_per_page = cfg->page_size / sizeof(uint64)
    *   - pages_per_extent = cfg->extent_size / cfg->page_size
    *
    * Therefore we have the following constraints on filter-index-size:
    *   (max_tuples_per_node / filter_cfg.index_size) / addrs_per_page <
    *   pages_per_extent
    * ->
    *   max_tuples_per_node / filter_cfg.index_size < addrs_per_page *
    *   pages_per_extent
    * ->
    *   filter_cfg.index_size > (max_tuples_per_node / (addrs_per_page *
    *   pages_per_extent))
    */
   uint64 addrs_per_page   = trunk_page_size(trunk_cfg) / sizeof(uint64);
   uint64 pages_per_extent = trunk_pages_per_extent(trunk_cfg);
   while (filter_cfg->index_size <= (trunk_cfg->max_tuples_per_node
                                     / (addrs_per_page * pages_per_extent)))
   {
      platform_default_log("filter-index-size: %u is too small, "
                           "setting to %u\n",
                           filter_cfg->index_size,
                           filter_cfg->index_size * 2);
      filter_cfg->index_size *= 2;
      filter_cfg->log_index_size++;
   }

   trunk_node_config_init(&trunk_cfg->trunk_node_cfg,
                          data_cfg,
                          &trunk_cfg->btree_cfg,
                          filter_cfg,
                          memtable_capacity * fanout,
                          memtable_capacity,
                          fanout,
                          memtable_capacity,
                          use_stats);


   // When everything succeeds, return success.
   return STATUS_OK;
}

size_t
trunk_get_scratch_size()
{
   return sizeof(trunk_task_scratch);
}
