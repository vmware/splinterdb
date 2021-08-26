// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinter.c --
 *
 *     This file contains the implementation for splinterDB.
 */

#include "platform.h"

#include "splinter.h"
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
   1,              // 1   ns
   10,             // 10  ns
   100,            // 100 ns
   500,            // 500 ns
   1000,           // 1   us
   5000,           // 5   us
   10000,          // 10  us
   100000,         // 100 us
   500000,         // 500 us
   1000000,        // 1   ms
   5000000,        // 5   ms
   10000000,       // 10  ms
   100000000,      // 100 ms
   1000000000,     // 1   s
   10000000000     // 10  s
};

#define SPLINTER_NUM_MEMTABLES (4)
#define SPLINTER_HARD_MAX_NUM_TREES (128)
#define SPLINTER_MAX_PIVOTS (20)
#define SPLINTER_MAX_BUNDLES (12)
#define SPLINTER_MAX_SUBBUNDLES (24)
#define SPLINTER_MAX_FILTERS (24)
#define SPLINTER_PREFETCH_MIN (16384)
#define SPLINTER_MIN_SPACE_RECL (2048)
#define SPLINTER_SUPER_CSUM_SEED (42)

//#define SPLINTER_LOG

#if defined SPLINTER_LOG
#define splinter_open_log_stream() platform_open_log_stream()
#else
#define splinter_open_log_stream()
#endif

#if defined SPLINTER_LOG
#define splinter_close_log_stream() \
   platform_close_log_stream(PLATFORM_DEFAULT_LOG_HANDLE)
#else
#define splinter_close_log_stream()
#endif

/*
 * splinter_log_stream, splinter_log_node, splinter_log_key should be called
 * between splinter_open_log_stream and splinter_close_log_stream.
 */
#if defined SPLINTER_LOG
#define splinter_log_stream(message, ...)                   \
   platform_log_stream("(%lu) "message, platform_get_tid(), \
                       ##__VA_ARGS__);
#define splinter_default_log(...)                           \
   platform_default_log(__VA_ARGS__);
#define splinter_log_node(spl, node)                        \
   splinter_print_locked_node(spl, node, stream);
#else
#define splinter_log_stream(message, ...)
#define splinter_default_log(...)
#define splinter_log_node(spl, node)
#endif

#if defined SPLINTER_LOG
#define splinter_log_key(spl, key, message, ...)                      \
   {                                                                  \
      char __key_str[128];                                            \
      splinter_key_to_string(spl, key, __key_str);                    \
      platform_log_stream("(%lu) "message" %s\n", platform_get_tid(), \
                          ##__VA_ARGS__, __key_str);                  \
   }
#else
#define splinter_log_key(spl, key, message, ...)
#endif

/*
 *-----------------------------------------------------------------------------
 *
 * SplinterDB Structure:
 *
 *       SplinterDB is a size-tiered Be-tree. It has a superstructure called
 *       the trunk tree, which consists of trunk nodes. Each trunk node
 *       contains pointers to a collection of branches. Each branch is a B-tree
 *       which stores key-value pairs (tuples). All the actual data is stored
 *       in the branches, and the trunk indexes and organizes the data.
 *
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 *
 * Substructures:
 *
 *       B-trees:
 *          SplinterDB makes use of B-trees, which come in two flavors, dynamic
 *          and static.
 *          dynamic: Dynamic B-trees are used in the memtable (see below) and
 *             are mutable B-trees, supporting insertions. The mutable
 *             operations on B-trees must use a btree_dynamic_handle.
 *          static: Static B-trees are used as branches and are immutable.
 *             Static btrees are accessed using their root_addr, which is
 *             thinly wrapped using their root_addr, which is thinly wrapped
 *             using btree_static_handle.
 *
 *-----------------------------------------------------------------------------
 */


/*
 *-----------------------------------------------------------------------------
 *
 * Insertion Path:
 *
 *       Memtable
 *          Insertions are first inserted into a memtable, which is a dynamic
 *          btree. SplinterDB uses multiple memtables so that when one memtable
 *          fills, insertions can continue into another memtable while the
 *          first is incorporated.
 *
 *          As part of this process, the generation number of the leaf into
 *          which the new tuple is placed is returned and stored in the log (if
 *          used) in order to establish a per-key temporal ordering.  The
 *          memtable also keeps an list of fingerprints, fp_arr, which are used
 *          to build the filter when the memtable becomes a branch.
 *
 *       Incorporation
 *          When the memtable fills, it is incorporated into the root node. The
 *          memtable locks itself to inserts (but not lookups), splinter
 *          switches the active memtable, then the filter is built from the
 *          fp_arr, and the btree in the memtable is inserted into the root as
 *          a new (distinct) branch.  Then the memtable is reinitialized with a
 *          new (empty) btree and unlocked.
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
 *          the parent no longer live for that pivot.
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
 *          copied, except the first fanout/2 pivots become the pivots of the
 *          left node and the remaining pivots become the right node. No
 *          compaction is initiated, and the branches and bundles of the node
 *          pre-split are shared between the new left and right nodes.
 *
 *       Split (leaf)
 *          When a leaf has more than cfg->max_tuples_per_leaf (fanout *
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
 *
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 *
 * Interactions between Concurrent Processes
 *
 *       The design of SplinterDB allows flushes, compactions, internal node
 *       split and leaf splits to happen concurrently, even within the same
 *       node. The ways in which these processes can interact are detailed
 *       here.
 *
 *       Flushes and compactions:
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
 *       Flushes and internal/leaf splits:
 *          Flushes and internal/leaf splits are synchronous and do not
 *          interact.
 *
 *       Internal splits and compaction:
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
 *       Leaf splits and compaction:
 *       6. If a compaction is scheduled or in progress when a leaf split
 *          triggers, the leaf split will start its own compaction job on the
 *          bundle being compacted. When the compaction job initiates or
 *          finishes, it will detect the leaf split using the generation number
 *          of the leaf, and abort.
 *
 *-----------------------------------------------------------------------------
 */

/*
 *-----------------------------------------------------------------------------
 *
 * Trunk Nodes:
 *
 *       A trunk node consists of the following:
 *
 *       header
 *          meta data
 *       ---------
 *       array of bundles
 *          When a collection of branches are flushed into a node, they are
 *          organized into a bundle. This bundle will be compacted into a
 *          single branch by a call to splinter_compact_bundle. Bundles are
 *          implemented as a collection of subbundles, each of which covers a
 *          range of branches.
 *       ----------
 *       array of subbundles
 *          A subbundle consists of the branches from a single ancestor (really
 *          that ancestor's pivot). During a flush, all the whole branches in
 *          the parent are collected into a subbundle in the child and any
 *          subbundles in the parent are copied to the child.
 *
 *          Subbundles function properly in the current design, but are not
 *          used for anything. They are going to be used for routing filters.
 *       ----------
 *       array of pivots
 *          Each node has a pivot corresponding to each child as well as an
 *          additional last pivot which contains an exclusive upper bound key
 *          for the node. Each pivot has a key which is an inclusive lower
 *          bound for the keys in its child node (as well as the subtree rooted
 *          there). This means that the key for the 0th pivot is an inclusive
 *          lower bound for all keys in the node.  Each pivot also has its own
 *          start_branch, which is used to determine which branches have tuples
 *          for that pivot (the range start_branch to end_branch).
 *
 *          Each pivot's key is accessible via a call to splinter_get_pivot and
 *          the remaining data is accessible via a call to
 *          splinter_get_pivot_data.
 *
 *          The number of pivots has two different limits: a soft limit
 *          (fanout) and a hard limit (max_pivot_keys). When the soft limit is
 *          reached, it will cause the node to split the next time it is
 *          flushed into (see internal node splits above). Note that multiple
 *          pivots can be added to the parent of a leaf during a split and
 *          multiple splits could theoretically occur before the node is
 *          flushed into again, so the fanout limit may temporarily be exceeded
 *          by multiple pivots. The hard limit is the amount of physical space
 *          in the node which can be used for pivots and cannot be exceeded. By
 *          default the fanout is 8 and the hard limit is 3x the fanout. Note
 *          that the additional last pivot (containing the exclusive upper
 *          bound to the node) counts towards the hard limit (because it uses
 *          physical space), but not the soft limit.
 *       ----------
 *       array of branches
 *          whole branches: the branches from hdr->start_branch to
 *             hdr->start_frac_branch are "whole" branches, each of which is
 *             the output of a compaction or incorporation.
 *          fractional branches: from hdr->start_frac_branch to hdr->end_branch
 *             are "fractional" branches that are part of bundles and are in
 *             the process of being compacted into whole branches.
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
 *          attempt to flush into a node which is at the hard limit will fail.
 *
 *-----------------------------------------------------------------------------
 */


/*
 *-----------------------------------------------------------------------------
 *
 * structs
 *
 *-----------------------------------------------------------------------------
 */

// a super block
typedef struct splinter_super_block {
   uint64      root_addr;
   uint64      meta_tail;
   uint64      log_addr;
   uint64      log_meta_addr;
   uint64      timestamp;
   bool        checkpointed;
   bool        dismounted;
   checksum128 checksum;
} splinter_super_block;

/*
 * A subbundle is a collection of branches which originated in the same node.
 * It is used to organize branches with their routing filters when they are
 * flushed or otherwise moved or reorganized.  A query to the node uses the
 * routing filter to filter the branches in the subbundle.
 */

typedef enum splinter_subbundle_state {
   SB_STATE_UNCOMPACTED_INDEX,
   SB_STATE_UNCOMPACTED_LEAF,
   SB_STATE_COMPACTED,         // compacted subbundles are always index
} splinter_subbundle_state;

typedef struct PACKED splinter_subbundle {
   splinter_subbundle_state state;
   uint16                   start_branch;
   uint16                   end_branch;
   uint16                   start_filter;
   uint16                   end_filter;
} splinter_subbundle;

/*
 * A flush moves branches from the parent to a bundle in the child. The bundle
 * is then compacted with a compact_bundle job.
 *
 * Branches are organized into subbundles.
 *
 * When a compact_bundle job completes, the branches in the bundle are replaced
 * with the outputted branch of the compaction and the bundle is marked
 * compacted. If there is not an earlier uncompacted bundle, the bundle can be
 * released and the compacted branch can become a whole branch. This is
 * maintain the invariant that the outstanding bundles form a contiguous range.
 */

typedef struct PACKED splinter_bundle {
   uint16 start_subbundle;
   uint16 end_subbundle;
   uint64 num_tuples;
} splinter_bundle;

/*
 * Trunk headers
 *
 * Contains metadata for trunk nodes. See below for comments on fields.
 *
 * Generation numbers are used by asynchronous processes to detect node splits.
 *    internal nodes: Splits increment the generation number of the left node.
 *       If a process visits a node with generation number g, then returns at a
 *       later point, it can find all the nodes which it splits into by search
 *       right until it reaches a node with generation number g (inclusive).
 *    leaves: Splits increment the generation numbers of all the resulting
 *       leaves. This is because there are no processes which need to revisit
 *       all the created leaves.
 */

typedef struct PACKED splinter_trunk_hdr {
   uint16 num_pivot_keys;    // number of used pivot keys (== num_children + 1)
   uint16 height;            // height of the node
   uint64 next_addr;         // PBN of the node's successor (0 if no successor)
   uint64 generation;        // counter incremented on a node split
   uint64 pivot_generation;  // counter incremented when new pivots are added

   uint16 start_branch;      // first live branch
   uint16 start_frac_branch; // first fractional branch (branch in a bundle)
   uint16 end_branch;        // successor to the last live branch
   uint16 start_bundle;      // first live bundle
   uint16 end_bundle;        // successor to the last live bundle
   uint16 start_subbundle;   // first live subbundle
   uint16 end_subbundle;     // successor to the last live subbundle
   uint16 start_sb_filter;   // first subbundle filter
   uint16 end_sb_filter;     // successor to the last sb filter

   splinter_bundle    bundle[SPLINTER_MAX_BUNDLES];
   splinter_subbundle subbundle[SPLINTER_MAX_SUBBUNDLES];
   routing_filter     sb_filter[SPLINTER_MAX_FILTERS];
} splinter_trunk_hdr;

/*
 * a pivot consists of the pivot key (of size cfg.key_size) followed by a
 * splinter_pivot_data
 *
 * the generation is used by asynchronous processes to determine when a pivot
 * has split
 */

typedef struct splinter_pivot_data {
   uint64         addr;         // PBN of the child
   // FIXME: [tjiaheng 2020-07-23] do we want to also include range_num_tuples
   uint64         num_tuples;   // estimate of the # of tuples for this pivot
   uint64         generation;   // receives new higher number when pivot splits
   uint16         start_branch; // first branch live (not used in leaves)
   uint16         start_bundle; // first bundle live (not used in leaves)
   routing_filter filter;       // routing filter for keys in this pivot
   int64          srq_idx;      // index in the space rec queue
} splinter_pivot_data;

// arguments to a compact_bundle job
struct splinter_compact_bundle_req {
   splinter_handle *spl;
   uint64           addr;
   uint16           height;
   uint16           bundle_no;
   uint64           generation;
   uint64           filter_generation;
   uint64           pivot_generation[SPLINTER_MAX_PIVOTS];
   uint64           max_pivot_generation;
   uint64           input_pivot_count[SPLINTER_MAX_PIVOTS];
   uint64           output_pivot_count[SPLINTER_MAX_PIVOTS];
   bool             is_space_rec;
   uint64           tuples_reclaimed;
   uint32          *fp_arr;
   bool             should_build[SPLINTER_MAX_PIVOTS];
   routing_filter   old_filter[SPLINTER_MAX_PIVOTS];
   uint16           value[SPLINTER_MAX_PIVOTS];
   routing_filter   filter[SPLINTER_MAX_PIVOTS];
};

// an iterator which skips masked pivots
typedef struct splinter_btree_skiperator {
   iterator        super;
   uint64          curr;
   uint64          end;
   splinter_branch branch;
   btree_iterator  itor[SPLINTER_MAX_PIVOTS];
} splinter_btree_skiperator;

// for find_pivot
typedef enum lookup_type {
   less_than,
   less_than_or_equal,
   greater_than,
   greater_than_or_equal,
} lookup_type;

// for for_each_node
typedef bool (*node_fn)(splinter_handle *spl,
                        uint64           addr,
                        void            *arg);

// Used by splinter_compact_bundle()
typedef struct {
   splinter_btree_skiperator skip_itor[SPLINTER_MAX_TOTAL_DEGREE];
   iterator*                 itor_arr[SPLINTER_MAX_TOTAL_DEGREE];
   key_buffer                saved_pivot_keys[SPLINTER_MAX_PIVOTS];
} compact_bundle_scratch;
// Used by splinter_split_leaf()
typedef struct {
   char           pivot[SPLINTER_MAX_PIVOTS][MAX_KEY_SIZE];
   btree_iterator btree_itor[SPLINTER_MAX_TOTAL_DEGREE];
   iterator*      rough_itor[SPLINTER_MAX_TOTAL_DEGREE];
} split_leaf_scratch;

/*
 * Union of various data structures that can live on the per-thread
 * scratch memory provided by the task subsystem and are needed by
 * splinter's task dispatcher routines.
 */
typedef union {
   compact_bundle_scratch compact_bundle;
   split_leaf_scratch     split_leaf;
} splinter_task_scratch;


/*
 * Splinter specific state that gets created during initialization in
 * splinter_system_init(). Contains global state for splinter such as the
 * init thread, init thread's scratch memory, thread_id counter and an array
 * of all the threads, which acts like a map that is accessed by thread id
 * to get the thread pointer.
 *
 * This structure is passed around like an opaque structure to all the
 * entities that need to access it. Some of them are task creation and
 * execution, task queue and clockcache.
 */

typedef struct splinter_system_state {
   // lookup array for threads registered with this system.
   thread_task           *thread_lookup[MAX_THREADS];
   // array of threads for this system.
   thread_task            thread_tasks[MAX_THREADS];
   // scratch memory for each thread.
   splinter_task_scratch  task_scratch[MAX_THREADS];
   // IO handle (currently one splinter system has just one)
   platform_io_handle    *ioh;
   /*
    * bitmask used for generating and clearing thread id's.
    * If a bit is set to 0, it means we have an in use thread id for that
    * particular position, 1 means it is unset and that thread id is available
    * for use.
    */
   uint64                 tid_bitmask;
   // max thread id so far.
   threadid               max_tid;
} splinter_system_state;

/*
 *-----------------------------------------------------------------------------
 *
 * function declarations
 *
 *-----------------------------------------------------------------------------
 */

// clang-format off
static inline bool                 splinter_is_leaf                   (splinter_handle *spl, page_handle *node);
static inline uint64               splinter_next_addr                 (splinter_handle *spl, page_handle *node);
static inline int                  splinter_key_compare               (splinter_handle *spl, const char *key1, const char *key2);
static inline page_handle *        splinter_node_get                  (splinter_handle *spl, uint64 addr);
static inline void                 splinter_node_unget                (splinter_handle *spl, page_handle **node);
static inline void                 splinter_node_claim                (splinter_handle *spl, page_handle **node);
static inline void                 splinter_node_unclaim              (splinter_handle *spl, page_handle *node);
static inline void                 splinter_node_lock                 (splinter_handle *spl, page_handle *node);
static inline void                 splinter_node_unlock               (splinter_handle *spl, page_handle *node);
page_handle *                      splinter_alloc                     (splinter_handle *spl, uint64 height);
static inline char *               splinter_get_pivot                 (splinter_handle *spl, page_handle *node, uint16 pivot_no);
static inline splinter_pivot_data *splinter_get_pivot_data            (splinter_handle *spl, page_handle *node, uint16 pivot_no);
static inline uint16               splinter_find_pivot                (splinter_handle *spl, page_handle *node, char *key, lookup_type comp);
platform_status                    splinter_add_pivot                 (splinter_handle *spl, page_handle *parent, page_handle *child, uint16 pivot_no);
static inline uint16               splinter_num_children              (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_num_pivot_keys            (splinter_handle *spl, page_handle *node);
static inline void                 splinter_inc_num_pivot_keys        (splinter_handle *spl, page_handle *node);
static inline char *               splinter_max_key                   (splinter_handle *spl, page_handle *node);
void                               splinter_update_num_tuples         (splinter_handle *spl, page_handle *node, uint16 branch_no);
void                               splinter_pivot_recount_num_tuples  (splinter_handle *spl, page_handle *node, uint64 pivot_no);
static inline uint16               splinter_pivot_branch_count        (splinter_handle *spl, page_handle *node, splinter_pivot_data *pdata);
bool                               splinter_pivot_needs_count_flush   (splinter_handle *spl, page_handle *node, splinter_pivot_data *pdata);
static inline uint64               splinter_pivot_tuples_in_branch    (splinter_handle *spl, page_handle *node, uint16 pivot_no, uint16 branch_no);
static inline bool                 splinter_has_vacancy               (splinter_handle *spl, page_handle *node, uint16 num_new_branches);
static inline uint16               splinter_bundle_no_add             (splinter_handle *spl, uint16 start, uint16 end);
static inline uint16               splinter_bundle_no_sub             (splinter_handle *spl, uint16 start, uint16 end);
static inline splinter_bundle     *splinter_get_bundle                (splinter_handle *spl, page_handle *node, uint16 bundle_no);
static inline uint16               splinter_get_new_bundle            (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_bundle_start_branch       (splinter_handle *spl, page_handle *node, splinter_bundle *bundle);
static inline uint16               splinter_start_bundle              (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_inc_start_bundle          (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_end_bundle                (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_bundle_clear_subbundles   (splinter_handle *spl, page_handle *node, splinter_bundle *bundle);
static inline uint16               splinter_subbundle_no_add          (splinter_handle *spl, uint16 start, uint16 end);
static inline uint16               splinter_subbundle_no_sub          (splinter_handle *spl, uint16 start, uint16 end);
static inline uint16               splinter_end_subbundle             (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_end_sb_filter             (splinter_handle *spl, page_handle *node);
static inline splinter_branch     *splinter_get_branch                (splinter_handle *spl, page_handle *node, uint32 k);
static inline splinter_branch     *splinter_get_new_branch            (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_start_branch              (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_end_branch                (splinter_handle *spl, page_handle *node);
static inline uint16               splinter_start_frac_branch         (splinter_handle *spl, page_handle *node);
static inline void                 splinter_set_start_frac_branch     (splinter_handle *spl, page_handle *node, uint16 branch_no);
static inline uint16               splinter_branch_count              (splinter_handle *spl, page_handle *node);
static inline bool                 splinter_branch_valid              (splinter_handle *spl, page_handle *node, uint64 branch_no);
static inline bool                 splinter_branch_live               (splinter_handle *spl, page_handle *node, uint64 branch_no);
static inline bool                 splinter_branch_live_for_pivot     (splinter_handle *spl, page_handle *node, uint64 branch_no, uint16 pivot_no);
static inline bool                 splinter_branch_is_whole           (splinter_handle *spl, page_handle *node, uint64 branch_no);
splinter_bundle *                  splinter_flush_into_bundle         (splinter_handle *spl, page_handle *parent, page_handle *child, splinter_pivot_data *pdata, splinter_compact_bundle_req *req);
void                               splinter_replace_bundle_branches   (splinter_handle *spl, page_handle *node, splinter_branch *new_branch, splinter_compact_bundle_req *req);
static inline uint16               splinter_branch_no_add             (splinter_handle *spl, uint16 branch_no, uint16 offset);
static inline uint16               splinter_branch_no_sub             (splinter_handle *spl, uint16 branch_no, uint16 offset);
static inline void                 splinter_dec_ref                   (splinter_handle *spl, splinter_branch *branch, bool is_memtable);
static inline void                 splinter_zap_branch_range          (splinter_handle *spl, splinter_branch *branch, const char *start_key, const char *end_key, page_type type);
static inline void                 splinter_inc_intersection          (splinter_handle *spl, splinter_branch *branch, const char *key, bool is_memtable);
void                               splinter_memtable_flush_virtual    (void *arg, uint64 generation);
platform_status                    splinter_memtable_insert           (splinter_handle *spl, char *key, char *data);
void                               splinter_bundle_build_filters  (void *arg, void *scratch);
static inline void                 splinter_inc_filter                (splinter_handle *spl, routing_filter *filter);
static inline void                 splinter_dec_filter                (splinter_handle *spl, routing_filter *filter);
void                               splinter_compact_bundle            (void *arg, void *scratch);
int                                splinter_flush                     (splinter_handle *spl, page_handle *parent, splinter_pivot_data *pdata, bool is_space_rec);
int                                splinter_flush_fullest             (splinter_handle *spl, page_handle *node);
static inline bool                 splinter_needs_split               (splinter_handle *spl, page_handle *node);
int                                splinter_split_index               (splinter_handle *spl, page_handle *parent, page_handle *child, uint64 pivot_no);
void                               splinter_split_leaf                (splinter_handle *spl, page_handle *parent, page_handle *leaf, uint16 child_idx);
int                                splinter_split_root                (splinter_handle *spl, page_handle     *root);
void                               splinter_print                     (splinter_handle *spl);
void                               splinter_print_node                (splinter_handle *spl, uint64 addr, platform_stream_handle stream);
void                               splinter_print_locked_node         (splinter_handle *spl, page_handle *node, platform_stream_handle stream);
static void                        splinter_btree_skiperator_init     (splinter_handle *spl, splinter_btree_skiperator *skip_itor, page_handle *node, uint16 branch_idx, data_type data_type, key_buffer pivots[static SPLINTER_MAX_PIVOTS]);
void                               splinter_btree_skiperator_get_curr (iterator *itor, char **key, char **data, data_type *type);
platform_status                    splinter_btree_skiperator_advance  (iterator *itor);
platform_status                    splinter_btree_skiperator_at_end   (iterator *itor, bool *at_end);
void                               splinter_btree_skiperator_print    (iterator *itor);
void                               splinter_btree_skiperator_deinit   (splinter_handle *spl, splinter_btree_skiperator *skip_itor);
bool                               splinter_verify_node               (splinter_handle *spl, page_handle *node);
void                               splinter_maybe_reclaim_space       (splinter_handle *spl);
const static iterator_ops splinter_btree_skiperator_ops = {
   .get_curr = splinter_btree_skiperator_get_curr,
   .at_end   = splinter_btree_skiperator_at_end,
   .advance  = splinter_btree_skiperator_advance,
   .print    = splinter_btree_skiperator_print,
};

// clang-format on

/*
 *-----------------------------------------------------------------------------
 *
 * super block functions
 *
 *-----------------------------------------------------------------------------
 */
void
splinter_set_super_block(splinter_handle *spl,
                         bool             is_checkpoint,
                         bool             is_dismount,
                         bool             is_create)
{
   uint64                super_addr;
   page_handle          *super_page;
   splinter_super_block *super;
   uint64                wait = 1;
   platform_status rc;

   if (is_create) {
      rc = allocator_alloc_super_addr(spl->al, spl->id, &super_addr);
   } else {
      rc = allocator_get_super_addr(spl->al, spl->id, &super_addr);
   }
   platform_assert_status_ok(rc);
   super_page = cache_get(spl->cc, super_addr, TRUE, PAGE_TYPE_MISC);
   while (!cache_claim(spl->cc, super_page)) {
      platform_sleep(wait);
      wait *= 2;
   }
   wait = 1;
   cache_lock(spl->cc, super_page);

   super = (splinter_super_block *)super_page->data;
   super->root_addr     = spl->root_addr;
   super->meta_tail     = mini_allocator_meta_tail(&spl->mini);
   if (spl->cfg.use_log) {
      super->log_addr      = log_addr(spl->log);
      super->log_meta_addr = log_meta_addr(spl->log);
   }
   super->timestamp     = platform_get_real_time();
   super->checkpointed  = is_checkpoint;
   super->dismounted    = is_dismount;
   super->checksum      = platform_checksum128(super,
         sizeof(splinter_super_block) - sizeof(checksum128),
         SPLINTER_SUPER_CSUM_SEED);

   cache_mark_dirty(spl->cc, super_page);
   cache_unlock(spl->cc, super_page);
   cache_unclaim(spl->cc, super_page);
   cache_unget(spl->cc, super_page);
   cache_page_sync(spl->cc, super_page, TRUE, PAGE_TYPE_MISC);
}

splinter_super_block *
splinter_get_super_block_if_valid(splinter_handle  *spl,
                                  page_handle     **super_page)
{
   uint64                super_addr;
   splinter_super_block *super;

   platform_status rc =
      allocator_get_super_addr(spl->al, spl->id, &super_addr);
   platform_assert_status_ok(rc);
   *super_page = cache_get(spl->cc, super_addr, TRUE, PAGE_TYPE_MISC);
   super      = (splinter_super_block *)(*super_page)->data;

   if (!platform_checksum_is_equal(super->checksum,
            platform_checksum128(super,
               sizeof(splinter_super_block) - sizeof(checksum128),
               SPLINTER_SUPER_CSUM_SEED))) {
      cache_unget(spl->cc, *super_page);
      *super_page = NULL;
      return NULL;
   }

   return super;
}

void
splinter_release_super_block(splinter_handle *spl,
                             page_handle     *super_page)
{
   cache_unget(spl->cc, super_page);
}

/*
 *-----------------------------------------------------------------------------
 *
 * helper/wrapper functions
 *
 *-----------------------------------------------------------------------------
 */

static inline uint16
splinter_height(splinter_handle *spl,
                page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->height;
}

static inline uint16
splinter_tree_height(splinter_handle *spl)
{
   page_handle *root = splinter_node_get(spl, spl->root_addr);
   uint16 tree_height = splinter_height(spl, root);
   splinter_node_unget(spl, &root);
   return tree_height;
}

static inline bool
splinter_is_leaf(splinter_handle *spl,
                 page_handle     *node)
{
   return splinter_height(spl, node) == 0;
}

uint64
splinter_trunk_hdr_size()
{
   return sizeof(splinter_trunk_hdr);
}

/*
 * The logical branch count is the number of branches the node would have if
 * all compactions completed. This is the number of whole branches plus the
 * number of bundles.
 */

static inline uint16
splinter_logical_branch_count(splinter_handle *spl,
                              page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   // whole branches
   uint16 num_branches = splinter_branch_no_sub(spl, hdr->start_frac_branch,
         hdr->start_branch);
   // bundles
   uint16 num_bundles = splinter_bundle_no_sub(spl, hdr->end_bundle,
         hdr->start_bundle);
   return num_branches + num_bundles;
}

/*
 * A node is full if either it has too many tuples or if it has too many
 * logical branches.
 */

static inline bool
splinter_node_is_full(splinter_handle *spl,
                      page_handle     *node)
{
   uint64 num_tuples = 0;
   if (splinter_logical_branch_count(spl, node) >
         spl->cfg.max_branches_per_node) {
      return TRUE;
   }
   for (uint16 i = 0; i < splinter_num_children(spl, node); i++) {
      num_tuples += splinter_get_pivot_data(spl, node, i)->num_tuples;
   }
   // FIXME: [tjiaheng 2020-07-23] do we want to also include range_num_tuples
   return num_tuples > spl->cfg.max_tuples_per_node;
}

static inline uint64
splinter_next_addr(splinter_handle *spl,
                   page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->next_addr;
}

static inline void
splinter_set_next_addr(splinter_handle *spl,
                       page_handle     *node,
                       uint64           addr)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   hdr->next_addr = addr;
}

bool
splinter_for_each_node(splinter_handle *spl,
                       node_fn          func,
                       void            *arg)
{
   page_handle *root = splinter_node_get(spl, spl->root_addr);
   uint16 height = splinter_height(spl, root);
   splinter_node_unget(spl, &root);
   uint64 next_level_addr = spl->root_addr;
   for (uint64 i = 0; i <= height; i++) {
      page_handle *node = splinter_node_get(spl, next_level_addr);
      uint64 addr = next_level_addr;
      next_level_addr = splinter_get_pivot_data(spl, node, 0)->addr;
      splinter_node_unget(spl, &node);
      while (addr != 0) {
         // first get the next_addr, then apply func, in case func
         // deletes the node for example
         node = splinter_node_get(spl, addr);
         uint64 next_addr = splinter_next_addr(spl, node);
         splinter_node_unget(spl, &node);
         bool rc = func(spl, addr, arg);
         if (rc != TRUE) {
            return rc;
         }
         addr = next_addr;
      }
   }
   return TRUE;
}

static inline btree_config *
splinter_btree_config(splinter_handle *spl)
{
   return &spl->cfg.btree_cfg;
}


/*
 *-----------------------------------------------------------------------------
 *
 * Cache Wrappers
 *
 *-----------------------------------------------------------------------------
 */

static inline page_handle *
splinter_node_get(splinter_handle *spl,
                  uint64           addr)
{
   return cache_get(spl->cc, addr, TRUE, PAGE_TYPE_TRUNK);
}

static inline cache_async_result
splinter_node_get_async(splinter_handle     *spl,
                        uint64               addr,
                        splinter_async_ctxt *ctxt)
{
   return cache_get_async(spl->cc, addr, PAGE_TYPE_TRUNK, &ctxt->cache_ctxt);
}

static inline void
splinter_node_async_done(splinter_handle     *spl,
                         splinter_async_ctxt *ctxt)
{
   cache_async_done(spl->cc, PAGE_TYPE_TRUNK, &ctxt->cache_ctxt);
}

static inline void
splinter_node_unget(splinter_handle  *spl,
                    page_handle     **node)
{
   cache_unget(spl->cc, *node);
   *node = NULL;
}

static inline void
splinter_node_claim(splinter_handle  *spl,
                    page_handle     **node)
{
   uint64 wait = 1;
   while (!cache_claim(spl->cc, *node)) {
      uint64 addr = (*node)->disk_addr;
      splinter_node_unget(spl, node);
      platform_sleep(wait);
      wait = wait > 2048 ? wait : 2 * wait;
      *node = splinter_node_get(spl, addr);
   }
}

static inline void
splinter_node_unclaim(splinter_handle *spl,
                      page_handle     *node)
{
   cache_unclaim(spl->cc, node);
}

static inline void
splinter_node_lock(splinter_handle *spl,
                   page_handle     *node)
{
   cache_lock(spl->cc, node);
   cache_mark_dirty(spl->cc, node);
}

static inline void
splinter_node_unlock(splinter_handle *spl,
                     page_handle     *node)
{
   cache_unlock(spl->cc, node);
}

page_handle *
splinter_alloc(splinter_handle *spl, uint64 height)
{
   uint64 addr = mini_allocator_alloc(&spl->mini, height, NULL, NULL);
   return cache_alloc(spl->cc, addr, PAGE_TYPE_TRUNK);
}

void splinter_dealloc(splinter_handle *spl,
                      uint64           addr)
{
   cache_dealloc(spl->cc, addr, PAGE_TYPE_TRUNK);
}

/*
 *-----------------------------------------------------------------------------
 *
 * Circular Buffer Arithmetic
 *
 *       X_add and X_sub add or substract the offset in the arithmetic of the
 *       circular buffer for X.
 *
 *       X_in_range returns TRUE if the given index is in the range [start,
 *       end] in the circular buffer for X.
 *
 *-----------------------------------------------------------------------------
 */

static inline uint16
splinter_branch_no_add(splinter_handle *spl,
                       uint16           branch_no,
                       uint16           offset)
{
   return (branch_no + offset) % spl->cfg.hard_max_branches_per_node;
}

static inline uint16
splinter_branch_no_sub(splinter_handle *spl,
                       uint16           branch_no,
                       uint16           offset)
{
   return (branch_no + spl->cfg.hard_max_branches_per_node - offset)
      % spl->cfg.hard_max_branches_per_node;
}

static inline bool
splinter_branch_in_range(splinter_handle *spl,
                         uint16           branch_no,
                         uint16           start,
                         uint16           end)
{
   return splinter_branch_no_sub(spl, branch_no, start)
      < splinter_branch_no_sub(spl, end, start);
}

static inline uint16
splinter_bundle_no_add(splinter_handle *spl,
                       uint16           start,
                       uint16           end)
{
   return (start + end) % SPLINTER_MAX_BUNDLES;
}

static inline uint16
splinter_bundle_no_sub(splinter_handle *spl,
                       uint16           start,
                       uint16           end)
{
   return (start + SPLINTER_MAX_BUNDLES - end)
      % SPLINTER_MAX_BUNDLES;
}

static inline uint16
splinter_subbundle_no_add(splinter_handle *spl,
                          uint16           start,
                          uint16           end)
{
   return (start + end) % SPLINTER_MAX_SUBBUNDLES;
}

static inline uint16
splinter_subbundle_no_sub(splinter_handle *spl,
                          uint16           start,
                          uint16           end)
{
   return (start + SPLINTER_MAX_SUBBUNDLES - end)
      % SPLINTER_MAX_SUBBUNDLES;
}

/*
 *-----------------------------------------------------------------------------
 *
 * pivot functions
 *
 *-----------------------------------------------------------------------------
 */

static inline char *
splinter_get_pivot(splinter_handle *spl,
                   page_handle     *node,
                   uint16           pivot_no)
{
   platform_assert(pivot_no < spl->cfg.max_pivot_keys);
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return ((char*)hdr) + sizeof(*hdr) + pivot_no * splinter_pivot_size(spl);
}

static inline void
splinter_set_pivot(splinter_handle *spl,
                   page_handle     *node,
                   uint16           pivot_no,
                   const char      *pivot_key)
{
   debug_assert(pivot_no < splinter_num_pivot_keys(spl, node));

   char *dst_pivot_key = splinter_get_pivot(spl, node, pivot_no);
   memmove(dst_pivot_key, pivot_key, splinter_key_size(spl));

   // debug asserts (should be optimized away)
   if (pivot_no != 0) {
      __attribute__ ((unused)) const char *pred_pivot =
         splinter_get_pivot(spl, node, pivot_no - 1);
      debug_assert(splinter_key_compare(spl, pred_pivot, pivot_key) < 0);
   }
   if (pivot_no < splinter_num_children(spl, node)) {
      __attribute__ ((unused)) const char *succ_pivot =
         splinter_get_pivot(spl, node, pivot_no + 1);
      debug_assert(splinter_key_compare(spl, pivot_key, succ_pivot) < 0);
   }
}

static inline void
splinter_set_initial_pivots(splinter_handle *spl,
                            page_handle     *node,
                            const char      *min_key,
                            const char      *max_key)
{
   debug_assert(splinter_key_compare(spl, min_key, max_key) < 0);

   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   hdr->num_pivot_keys = 2;

   char *dst_pivot_key = splinter_get_pivot(spl, node, 0);
   memmove(dst_pivot_key, min_key, splinter_key_size(spl));
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
   ZERO_CONTENTS(pdata);
   pdata->srq_idx = -1;
   dst_pivot_key = splinter_get_pivot(spl, node, 1);
   memmove(dst_pivot_key, max_key, splinter_key_size(spl));
}

UNUSED_FUNCTION()
static inline char *
splinter_min_key(splinter_handle *spl,
                 page_handle     *node)
{
   return splinter_get_pivot(spl, node, 0);
}

static inline char *
splinter_max_key(splinter_handle *spl,
                 page_handle     *node)
{
   return splinter_get_pivot(spl, node, splinter_num_children(spl, node));
}

static inline uint64
splinter_pivot_generation(splinter_handle *spl,
                          page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->pivot_generation;
}

static inline uint64
splinter_inc_pivot_generation(splinter_handle *spl,
                              page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->pivot_generation++;
}

uint64
splinter_pivot_size(splinter_handle *spl)
{
   return splinter_key_size(spl) + sizeof(splinter_pivot_data);
}

uint64
splinter_pivot_message_size()
{
   return sizeof(splinter_pivot_data);
}

static inline splinter_pivot_data *
splinter_get_pivot_data(splinter_handle *spl,
                        page_handle     *node,
                        uint16           pivot_no)
{
   return (splinter_pivot_data *)(splinter_get_pivot(spl, node, pivot_no)
         + splinter_key_size(spl));
}

static inline void
splinter_set_pivot_data_new_root(splinter_handle *spl,
                                 page_handle     *node,
                                 uint64           child_addr)
{
   debug_assert(splinter_height(spl, node) != 0);
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);

   pdata->addr = child_addr;
   pdata->num_tuples = 0;
   pdata->start_branch = splinter_start_branch(spl, node);
   pdata->start_bundle = splinter_end_bundle(spl, node);
   ZERO_STRUCT(pdata->filter);
}

static inline void
splinter_copy_pivot_data_from_pred(splinter_handle *spl,
                                   page_handle     *node,
                                   uint16           pivot_no,
                                   uint64           child_addr)
{
   debug_assert(splinter_height(spl, node) != 0);
   debug_assert(pivot_no != 0);
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
   splinter_pivot_data *pred_pdata =
      splinter_get_pivot_data(spl, node, pivot_no - 1);

   memmove(pdata, pred_pdata, sizeof(*pdata));
   pdata->addr = child_addr;
   pdata->num_tuples = 0;
   pred_pdata->generation = splinter_inc_pivot_generation(spl, node);
   platform_assert(pdata->srq_idx == -1);
}

static inline uint16
splinter_pivot_start_branch(splinter_handle *spl,
                            page_handle     *node,
                            uint16           pivot_no)
{
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
   return pdata->start_branch;
}

static inline uint64
splinter_generation(splinter_handle *spl,
                    page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->generation;
}

static inline void
splinter_inc_generation(splinter_handle *spl,
                        page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   hdr->generation++;
}

/*
 * Used by find_pivot
 */
static inline uint32
lowerbound(uint32 size)
{
   if (size <= 1) return 0;
   return (8 * sizeof(uint32)) - __builtin_clz(size - 1);
}

/*
 * Used by find_pivot
 */
static inline void
splinter_update_lowerbound(uint16 *lo,
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
 * find_pivot performs a binary search for the extremal pivot that satisfies
 * comp, e.g. if comp == greater_than, find_pivot finds the smallest pivot
 * which is greater than key. It returns the found pivot's index.
 */
static inline uint16
splinter_find_pivot(splinter_handle *spl,
                    page_handle     *node,
                    char            *key,
                    lookup_type      comp)
{
   debug_assert(node != NULL);
   uint16 lo_idx = 0, mid_idx;
   uint32 i;
   int cmp;
   uint32 size = splinter_num_children(spl, node);

   if (size == 0) {
      return 0;
   }

   if (size == 1) {
      cmp = splinter_key_compare(spl, splinter_get_pivot(spl, node, 0), key);
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

   // binary search for the pivot
   mid_idx = size - (1u << (lowerbound(size) - 1));
   size = 1u << (lowerbound(size) - 1);
   cmp = splinter_key_compare(spl, splinter_get_pivot(spl, node, mid_idx), key);
   splinter_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);

   for (i = lowerbound(size); i != 0; i--) {
      size /= 2;
      mid_idx = lo_idx + size;
      cmp = splinter_key_compare(spl, splinter_get_pivot(spl, node, mid_idx),
            key);
      splinter_update_lowerbound(&lo_idx, &mid_idx, cmp, comp);
   }

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
}

/*
 * branch_live_for_pivot returns TRUE if the branch is live for the pivot and
 * FALSE otherwise.
 */
static inline bool
splinter_branch_live_for_pivot(splinter_handle *spl,
                               page_handle     *node,
                               uint64           branch_no,
                               uint16           pivot_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
   return splinter_branch_no_sub(spl, branch_no, pdata->start_branch)
      < splinter_branch_no_sub(spl, hdr->end_branch, pdata->start_branch);
}

/*
 * branch_is_whole returns TRUE if the branch is whole and FALSE if it is
 * fractional (part of a bundle) or dead.
 */
static inline bool
splinter_branch_is_whole(splinter_handle *spl,
                         page_handle     *node,
                         uint64           branch_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_branch_no_sub(spl, branch_no, hdr->start_branch)
      < splinter_branch_no_sub(spl, hdr->start_frac_branch, hdr->start_branch);
}

static inline void
splinter_shift_pivots(splinter_handle *spl,
                      page_handle     *node,
                      uint16           pivot_no,
                      uint16           shift)
{
   debug_assert(splinter_height(spl, node) != 0);
   debug_assert(splinter_num_pivot_keys(spl, node) + shift <
         spl->cfg.max_pivot_keys);
   debug_assert(pivot_no < splinter_num_pivot_keys(spl, node));

   char *dst_pivot = splinter_get_pivot(spl, node, pivot_no + shift);
   char *src_pivot = splinter_get_pivot(spl, node, pivot_no);
   uint16 pivots_to_shift = splinter_num_pivot_keys(spl, node) - pivot_no;
   size_t bytes_to_shift = pivots_to_shift * splinter_pivot_size(spl);
   memmove(dst_pivot, src_pivot, bytes_to_shift);
}

/*
 * add_pivot adds a pivot in parent at position pivot_no that points to child.
 */

platform_status
splinter_add_pivot(splinter_handle     *spl,
                   page_handle         *parent,
                   page_handle         *child,
                   uint16               pivot_no)  // position of new pivot
{
   // equality is allowed, because we can be adding a pivot at the end
   platform_assert(pivot_no <= splinter_num_children(spl, parent));
   platform_assert(pivot_no != 0);

   if (splinter_num_pivot_keys(spl, parent) >= spl->cfg.max_pivot_keys) {
      // No room to add a pivot
      debug_assert(splinter_num_pivot_keys(spl, parent) ==
                   spl->cfg.max_pivot_keys);
      return STATUS_LIMIT_EXCEEDED;
   }

   // move pivots in parent and add new pivot for child
   splinter_shift_pivots(spl, parent, pivot_no, 1);
   splinter_inc_num_pivot_keys(spl, parent);
   const char *pivot_key = splinter_get_pivot(spl, child, 0);
   splinter_set_pivot(spl, parent, pivot_no, pivot_key);

   uint64 child_addr = child->disk_addr;
   splinter_copy_pivot_data_from_pred(spl, parent, pivot_no, child_addr);

   return STATUS_OK;
}

void
splinter_add_pivot_new_root(splinter_handle *spl,
                            page_handle     *parent,
                            page_handle     *child)
{
   const char *pivot_key = splinter_get_pivot(spl, child, 0);
   __attribute__ ((unused)) const char *min_key = spl->cfg.data_cfg->min_key;
   debug_assert(splinter_key_compare(spl, pivot_key, min_key) == 0);

   const char *max_key = spl->cfg.data_cfg->max_key;
   splinter_set_initial_pivots(spl, parent, pivot_key, max_key);
   uint64 child_addr = child->disk_addr;
   splinter_set_pivot_data_new_root(spl, parent, child_addr);
}

// FIXME: [tjiaheng 2020-07-23] do we want to also include range_num_tuples
/*
 * update_num_tuples updates the estimate of the tuples (num_tuples) in each
 * pivot to include an estimate of those from new_branch.
 *
 * Used when adding new branches to a node as part of a flush.
 */
void
splinter_update_num_tuples(splinter_handle *spl,
                           page_handle     *node,
                           uint16           branch_no)
{
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      pdata->num_tuples +=
         splinter_pivot_tuples_in_branch(spl, node, pivot_no, branch_no);
   }
}

// FIXME: [tjiaheng 2020-07-23] do we want to also include range_num_tuples
/*
 * pivot_recount_num_tuples recounts num_tuples for the pivot at position
 * pivot_no using a rough count.
 *
 * Used after index splits.
 */
void
splinter_pivot_recount_num_tuples(splinter_handle *spl,
                                  page_handle     *node,
                                  uint64           pivot_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
   pdata->num_tuples = 0;
   for (uint64 branch_no = pdata->start_branch;
        branch_no != hdr->end_branch;
        branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
      pdata->num_tuples +=
         splinter_pivot_tuples_in_branch(spl, node, pivot_no, branch_no);
   }
}

// FIXME: [tjiaheng 2020-07-23] do we want to also include range_num_tuples

uint64
splinter_pivot_num_tuples(splinter_handle *spl,
                          page_handle     *node,
                          uint16           pivot_no)
{
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
   return pdata->num_tuples;
}

void
splinter_pivot_set_num_tuples(splinter_handle *spl,
                              page_handle     *node,
                              uint16           pivot_no,
                              uint64           num_tuples)
{
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
   pdata->num_tuples = num_tuples;
}

static inline uint64
splinter_pivot_tuples_to_reclaim(splinter_handle     *spl,
                                 splinter_pivot_data *pdata)
{
   uint64 tuples_in_pivot = pdata->filter.num_fingerprints;
   uint64 est_unique_tuples =
      routing_filter_estimate_unique_keys(&pdata->filter, &spl->cfg.leaf_filter_cfg);
   return tuples_in_pivot > est_unique_tuples ? tuples_in_pivot - est_unique_tuples : 0;
}

/*
 * Returns the number of whole branches which are live for the pivot
 */
static inline uint64
splinter_pivot_whole_branch_count(splinter_handle     *spl,
                                  page_handle         *node,
                                  splinter_pivot_data *pdata)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   if (!splinter_branch_is_whole(spl, node, pdata->start_branch))
      return 0;
   return splinter_branch_no_sub(spl, hdr->start_frac_branch, pdata->start_branch);
}

/*
 * Returns the number of bundles which are live for the pivot.
 */
static inline uint16
splinter_pivot_bundle_count(splinter_handle     *spl,
                            page_handle         *node,
                            splinter_pivot_data *pdata)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_bundle_no_sub(spl, hdr->end_bundle, pdata->start_bundle);
}

/*
 * Returns the number of subbundles which are live for the pivot.
 */
static inline uint16
splinter_pivot_subbundle_count(splinter_handle     *spl,
                               page_handle         *node,
                               splinter_pivot_data *pdata)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 pivot_start_subbundle;
   splinter_bundle *bundle;
   if (splinter_pivot_bundle_count(spl, node, pdata) == 0)
      return 0;

   bundle = splinter_get_bundle(spl, node, pdata->start_bundle);
   pivot_start_subbundle = bundle->start_subbundle;
   return splinter_subbundle_no_sub(spl, hdr->end_subbundle,
                                    pivot_start_subbundle);
}

static inline uint16
splinter_pivot_start_subbundle(splinter_handle     *spl,
                               page_handle         *node,
                               splinter_pivot_data *pdata)
{
   if (pdata->start_bundle == splinter_end_bundle(spl, node)) {
      return splinter_end_subbundle(spl, node);;
   }
   splinter_bundle *bundle =
      splinter_get_bundle(spl, node, pdata->start_bundle);
   return bundle->start_subbundle;
}

static inline uint16
splinter_pivot_end_subbundle_for_lookup(splinter_handle     *spl,
                                        page_handle         *node,
                                        splinter_pivot_data *pdata)
{
   return splinter_subbundle_no_sub(spl,
         splinter_pivot_start_subbundle(spl, node, pdata), 1);
}

/*
 * Returns the logical number of branches which are live for the pivot. A
 * logical branch is either a whole branch or a bundle.
 */
static inline uint16
splinter_pivot_logical_branch_count(splinter_handle     *spl,
                                    page_handle         *node,
                                    splinter_pivot_data *pdata)
{
   return splinter_pivot_whole_branch_count(spl, node, pdata)
      + splinter_pivot_bundle_count(spl, node, pdata);
}

/*
 * pivot_needs_flush returns TRUE if the pivot has too many logical branches
 * and FALSE otherwise.
 *
 * When a node is full because it has too many logical branches, all pivots
 * with too many live logical branches must be flushed in order to reduce the
 * branch count.
 */
static inline bool
splinter_pivot_needs_flush(splinter_handle     *spl,
                           page_handle         *node,
                           splinter_pivot_data *pdata)
{
   return splinter_pivot_logical_branch_count(spl, node, pdata)
      > spl->cfg.max_branches_per_node;
}

/*
 * Returns the number of branches which are live for the pivot.
 *
 * This counts each fractional branch independently as opposed to
 * pivot_whole_branch_count.
 */
static inline uint16
splinter_pivot_branch_count(splinter_handle     *spl,
                            page_handle         *node,
                            splinter_pivot_data *pdata)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_branch_no_sub(spl, hdr->end_branch, pdata->start_branch);
}

static inline uint64
splinter_pivot_tuples_in_btree(splinter_handle *spl,
                               page_handle     *node,
                               uint16           pivot_no,
                               uint64           root_addr)
{
   char *min_key = splinter_get_pivot(spl, node, pivot_no);
   char *max_key = splinter_get_pivot(spl, node, pivot_no + 1);
   return btree_count_in_range(spl->cc, splinter_btree_config(spl), root_addr,
         min_key, max_key);
}


/*
 * FIXME: [aconway 2020-08-11] Add description
 */
static inline uint64
splinter_pivot_tuples_in_branch(splinter_handle *spl,
                                page_handle     *node,
                                uint16           pivot_no,
                                uint16           branch_no)
{
   splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
   return splinter_pivot_tuples_in_btree(spl, node, pivot_no,
                                         branch->root_addr);
}

__attribute__ ((unused))
static inline uint64
splinter_pivot_tuples_in_branch_slow(splinter_handle *spl,
                                     page_handle     *node,
                                     uint16           pivot_no,
                                     uint16           branch_no)
{
   splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
   char *min_key = splinter_get_pivot(spl, node, pivot_no);
   char *max_key = splinter_get_pivot(spl, node, pivot_no + 1);
   return btree_count_in_range_by_iterator(spl->cc, splinter_btree_config(spl),
         branch->root_addr, min_key, max_key);
}

/*
 * leaf_count_num_tuples estimates the number of tuples in all branches except
 * the first, adds them to first_branch_tuples and stores the result in the
 * tuple estimate for the (only) pivot (in num_tuples).
 *
 * When a pack_leaf job finishes, the number of tuples in the compacted branch
 * (the first branch in the leaf) is known from the compaction
 * (first_branch_tuples). If any new branches have been added as the result of
 * a subsequent flush, the tuple count for the node must be updated to include
 * their tuples.
 */
// FIXME: [tjiaheng 2020-07-23] do we want to also include range_num_tuples
void
splinter_leaf_count_num_tuples(splinter_handle *spl,
                               page_handle     *node,
                               uint64           first_branch_tuples)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
   pdata->num_tuples = first_branch_tuples;
   for (uint16 branch_no = splinter_branch_no_add(spl, hdr->start_branch, 1);
        branch_no != hdr->end_branch;
        branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
      pdata->num_tuples +=
         splinter_pivot_tuples_in_branch(spl, node, 0, branch_no);
   }
}

/*
 * reset_start_branch sets the trunk start branch to the smallest start branch
 * of any pivot, and resets the trunk start bundle accordingly.
 *
 * After a node flush, there may be branches and bundles in the node which are
 * no longer live for any pivot. reset_start_branch identifies these, makes
 * sure they are dereferenced and updates the values in the header.
 */
static inline void
splinter_reset_start_branch(splinter_handle *spl,
                            page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 start_branch = hdr->end_branch;
   uint16 pivot_no, branch_no, bundle_no;
   splinter_bundle *bundle;

   // find the pivot with the smallest branch and bundle
   for (pivot_no = 0; pivot_no < splinter_num_children(spl, node); pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (splinter_branch_no_sub(spl, hdr->end_branch, pdata->start_branch) >
            splinter_branch_no_sub(spl, hdr->end_branch, start_branch))
         start_branch = pdata->start_branch;
   }

   // reset the start branch (and maybe the fractional branch)
   hdr->start_branch = start_branch;
   if (!splinter_branch_valid(spl, node, hdr->start_frac_branch)) {
      hdr->start_frac_branch = hdr->start_branch;
   }

   // kill any bundles that have no live branches
   for (bundle_no = hdr->start_bundle; bundle_no != hdr->end_bundle;
         bundle_no = splinter_bundle_no_add(spl, bundle_no, 1)) {
      bundle = splinter_get_bundle(spl, node, bundle_no);
      branch_no = splinter_bundle_start_branch(spl, node, bundle);
      if (!splinter_branch_live(spl, node, branch_no)) {
         /*
          * either all branches in the bundle are live or none are, so in this
          * case none are
          */
         splinter_bundle_clear_subbundles(spl, node, bundle);
         splinter_inc_start_bundle(spl, node);
         splinter_default_log("node %lu evicting bundle %hu\n",
                              node->disk_addr, bundle_no);
      }
   }
}

/*
 * pivot_clear clears all branches and bundles from the pivot
 *
 * Used when flushing the pivot.
 */

static inline void
splinter_pivot_clear(splinter_handle     *spl,
                     page_handle         *node,
                     splinter_pivot_data *pdata)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 start_branch = pdata->start_branch;
   pdata->start_branch = hdr->end_branch;
   pdata->start_bundle = hdr->end_bundle;
   pdata->num_tuples = 0;
   pdata->srq_idx = -1;
   if (start_branch == hdr->start_branch) {
      splinter_reset_start_branch(spl, node);
   }
   pdata->filter.addr = 0;
   pdata->filter.meta_head = 0;
   pdata->filter.num_fingerprints = 0;
}

/*
 * Returns the index of the pivot with pivot data pdata.
 */
static inline uint16
splinter_pdata_to_pivot_index(splinter_handle     *spl,
                              page_handle         *node,
                              splinter_pivot_data *pdata)
{
   uint64 byte_difference =  (char *)pdata
      - (char *)splinter_get_pivot_data(spl, node, 0);
   debug_assert(byte_difference % splinter_pivot_size(spl) == 0);
   return byte_difference / splinter_pivot_size(spl);
}

/*
 * Returns the number of children of the node
 */
static inline uint16
splinter_num_children(splinter_handle *spl,
                      page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   debug_assert(hdr->num_pivot_keys >= 2);
   return hdr->num_pivot_keys - 1;
}

/*
 * Returns the number of pivot keys in the node. This is equal to the number of
 * children + 1 for the upper bound pivot key.
 */
static inline uint16
splinter_num_pivot_keys(splinter_handle *spl,
                        page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   debug_assert(hdr->num_pivot_keys >= 2);
   return hdr->num_pivot_keys;
}

static inline void
splinter_inc_num_pivot_keys(splinter_handle *spl,
                            page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   debug_assert(hdr->num_pivot_keys >= 2);
   hdr->num_pivot_keys++;
   debug_assert(hdr->num_pivot_keys <= spl->cfg.max_pivot_keys);
}


/*
 * Returns the PBN of the node at height height whose key range contains key.
 *
 * Used to locate the parent of a leaf which has finished splitting in the case
 * where the parent might have changed as a result of a internal node split or
 * root split.
 */
uint64
splinter_find_node(splinter_handle *spl,
                   char            *key,
                   uint64           height)
{
   page_handle *node = splinter_node_get(spl, spl->root_addr);
   uint16 tree_height = splinter_height(spl, node);
   for (uint16 h = tree_height; h > height + 1; h--) {
      uint32 pivot_no = splinter_find_pivot(spl, node, key, less_than_or_equal);
      debug_assert(pivot_no < splinter_num_children(spl, node));
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      page_handle *child = splinter_node_get(spl, pdata->addr);
      splinter_node_unget(spl, &node);
      node = child;
   }
   uint32 pivot_no = splinter_find_pivot(spl, node, key, less_than_or_equal);
   debug_assert(pivot_no < splinter_num_children(spl, node));
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
   uint64 ret_addr = pdata->addr;
   splinter_node_unget(spl, &node);
   return ret_addr;
}

/*
 *-----------------------------------------------------------------------------
 *
 * bundle functions
 *
 *-----------------------------------------------------------------------------
 */

static inline splinter_bundle *
splinter_get_bundle(splinter_handle *spl,
                    page_handle     *node,
                    uint16           bundle_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return &hdr->bundle[bundle_no];
}

/*
 * get_new_bundle allocates a new bundle in the node and returns its index.
 *
 * FIXME: [aconway 2020-06-21] Name implies it returns a pointer to the bundle
 * FIXME: [aconway 2020-06-21] Currently crashes if there is no room in the
 * buffer
 */
static inline uint16
splinter_get_new_bundle(splinter_handle *spl,
                        page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 new_bundle_no = hdr->end_bundle;
   hdr->end_bundle = splinter_bundle_no_add(spl, hdr->end_bundle, 1);
   platform_assert(hdr->end_bundle != hdr->start_bundle);
   return new_bundle_no;
}

static inline uint16
splinter_start_bundle(splinter_handle *spl,
                      page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->start_bundle;
}

static inline uint16
splinter_end_bundle(splinter_handle *spl,
                    page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->end_bundle;
}

static inline uint16
splinter_inc_start_bundle(splinter_handle *spl,
                          page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   hdr->start_bundle = splinter_bundle_no_add(spl, hdr->start_bundle, 1);
   return hdr->start_bundle;
}

static inline splinter_subbundle *
splinter_get_subbundle(splinter_handle *spl,
                       page_handle     *node,
                       uint16           subbundle_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return &hdr->subbundle[subbundle_no];
}

static inline uint16
splinter_subbundle_no(splinter_handle    *spl,
                      page_handle        *node,
                      splinter_subbundle *sb)
{
   return sb - splinter_get_subbundle(spl, node, 0);
}

/*
 * get_new_subbundle allocates a new subbundle in the node and returns its
 * index.
 *
 * FIXME: [aconway 2020-06-21] Currently crashes if there is no room in the
 * buffer
 */
static inline splinter_subbundle *
splinter_get_new_subbundle(splinter_handle *spl,
                           page_handle     *node,
                           uint16           num_filters)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 new_subbundle_no = hdr->end_subbundle;
   hdr->end_subbundle = splinter_subbundle_no_add(spl, hdr->end_subbundle, 1);
   // ALEX: Need a way to handle this better
   platform_assert(hdr->end_subbundle != hdr->start_subbundle);

   // get filters
   splinter_subbundle *sb = splinter_get_subbundle(spl, node, new_subbundle_no);
   sb->start_filter = splinter_end_sb_filter(spl, node);
   hdr->end_sb_filter =
      splinter_subbundle_no_add(spl, hdr->end_sb_filter, num_filters);
   sb->end_filter = splinter_end_sb_filter(spl, node);
   sb->state = SB_STATE_COMPACTED;
   return sb;
}

static inline splinter_subbundle *
splinter_leaf_get_new_subbundle_at_head(splinter_handle *spl,
                                        page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 new_subbundle_no =
      splinter_subbundle_no_sub(spl, hdr->start_subbundle, 1);
   platform_assert(new_subbundle_no != hdr->end_subbundle);
   hdr->start_subbundle = new_subbundle_no;

   // get filters
   splinter_subbundle *sb = splinter_get_subbundle(spl, node, new_subbundle_no);
   sb->end_filter = hdr->start_sb_filter;
   sb->start_filter = splinter_subbundle_no_sub(spl, hdr->start_sb_filter, 1);
   platform_assert(sb->start_filter != hdr->end_sb_filter);
   hdr->start_sb_filter = sb->start_filter;
   sb->state = SB_STATE_UNCOMPACTED_LEAF;
   return sb;
}

static inline routing_filter *
splinter_get_sb_filter(splinter_handle *spl,
                       page_handle     *node,
                       uint16           filter_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return &hdr->sb_filter[filter_no];
}

static inline uint16
splinter_start_sb_filter(splinter_handle *spl,
                         page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->start_sb_filter;
}

static inline uint16
splinter_end_sb_filter(splinter_handle *spl,
                       page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->end_sb_filter;
}

static inline uint16
splinter_subbundle_filter_count(splinter_handle    *spl,
                                page_handle        *node,
                                splinter_subbundle *sb)
{
   return splinter_subbundle_no_sub(spl, sb->end_filter, sb->start_filter);
}

static inline uint16
splinter_bundle_filter_count(splinter_handle *spl,
                             page_handle     *node,
                             splinter_bundle *bundle)
{
   uint16 filter_count = 0;
   for (uint16 sb_no = bundle->start_subbundle;
        sb_no != bundle->end_subbundle;
        sb_no = splinter_subbundle_no_add(spl, sb_no, 1)) {
      splinter_subbundle *sb = splinter_get_subbundle(spl, node, sb_no);
      filter_count += splinter_subbundle_filter_count(spl, node, sb);
   }
   return filter_count;
}

static inline uint16
splinter_bundle_start_filter(splinter_handle *spl,
                             page_handle     *node,
                             splinter_bundle *bundle)
{
   uint16 sb_no = bundle->start_subbundle;
   splinter_subbundle *sb = splinter_get_subbundle(spl, node, sb_no);
   return sb->start_filter;
}

static inline uint16
splinter_bundle_end_filter(splinter_handle *spl,
                           page_handle     *node,
                           splinter_bundle *bundle)
{
   uint16 last_sb_no = splinter_subbundle_no_sub(spl, bundle->end_subbundle, 1);
   splinter_subbundle *sb = splinter_get_subbundle(spl, node, last_sb_no);
   return sb->end_filter;
}

static inline routing_filter *
splinter_subbundle_filter(splinter_handle    *spl,
                          page_handle        *node,
                          splinter_subbundle *sb,
                          uint16              filter_off)
{
   uint16 start_filter = sb->start_filter;
   uint16 filter_no = splinter_subbundle_no_add(spl, start_filter, filter_off);
   debug_assert(filter_off < splinter_subbundle_filter_count(spl, node, sb));
   return splinter_get_sb_filter(spl, node, filter_no);
}

__attribute__ ((unused))
static inline uint16
splinter_subbundle_branch_count(splinter_handle    *spl,
                                page_handle        *node,
                                splinter_subbundle *sb)
{
   return splinter_branch_no_sub(spl, sb->end_branch, sb->start_branch);
}

__attribute__ ((unused))
static inline uint16
splinter_start_subbundle(splinter_handle *spl,
                         page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->start_subbundle;
}

static inline uint16
splinter_end_subbundle(splinter_handle *spl,
                       page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->end_subbundle;
}

static inline uint16
splinter_start_subbundle_for_lookup(splinter_handle *spl,
                                    page_handle     *node)
{
   return splinter_subbundle_no_sub(spl, splinter_end_subbundle(spl, node), 1);
}

static inline uint16
splinter_bundle_clear_subbundles(splinter_handle *spl,
                                 page_handle     *node,
                                 splinter_bundle *bundle)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 start_filter = splinter_bundle_start_filter(spl, node, bundle);
   uint16 end_filter = splinter_bundle_end_filter(spl, node, bundle);
   for (uint16 filter_no = start_filter;
        filter_no != end_filter;
        filter_no = splinter_subbundle_no_add(spl, filter_no, 1)) {
      routing_filter *filter =
         splinter_get_sb_filter(spl, node, filter_no);
      splinter_dec_filter(spl, filter);
      //platform_log("dec filter %lu in %lu (%u)\n",
      //      filter->addr, node->disk_addr,
      //      allocator_get_refcount(spl->al, filter->addr));
   }
   hdr->start_sb_filter = end_filter;
   hdr->start_subbundle = bundle->end_subbundle;
   return hdr->start_subbundle;
}

/*
 * Removes all bundles except the given bundle.
 *
 * This function does not just clear compacted bundles into whole branches, but
 * removes bundles wholesale.
 *
 * Used in leaf splits to abort compactions in progress.
 */
static inline void
splinter_leaf_remove_bundles_except(splinter_handle *spl,
                                    page_handle     *node,
                                    uint16           bundle_no)
{
   debug_assert(splinter_height(spl, node) == 0);
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   uint16 last_bundle_no = splinter_end_bundle(spl, node);
   last_bundle_no = splinter_bundle_no_sub(spl, last_bundle_no, 1);
   debug_assert(bundle_no == last_bundle_no);
   hdr->start_bundle = bundle_no;
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
   pdata->start_bundle = hdr->start_bundle;
}

/*
 * Rebundles all branches and subbundles in a leaf into a single bundle.
 *
 * Used in leaf splits to abort compactions in progress.
 */
static inline uint16
splinter_leaf_rebundle_all_branches(splinter_handle *spl,
                                    page_handle     *node,
                                    uint64           target_num_tuples,
                                    bool             is_space_rec)
{
   debug_assert(splinter_height(spl, node) == 0);
   uint16 bundle_no = splinter_get_new_bundle(spl, node);
   if (splinter_branch_is_whole(spl, node, splinter_start_branch(spl, node))) {
      splinter_subbundle *sb =
         splinter_leaf_get_new_subbundle_at_head(spl, node);
      sb->start_branch = splinter_start_branch(spl, node);
      sb->end_branch = splinter_start_frac_branch(spl, node);
      routing_filter *filter = splinter_subbundle_filter(spl, node, sb, 0);
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
      *filter = pdata->filter;
      debug_assert(filter->addr != 0);
      ZERO_STRUCT(pdata->filter);
      debug_assert(splinter_subbundle_branch_count(spl, node, sb) != 0);
   }
   splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);
   bundle->num_tuples = target_num_tuples;
   bundle->start_subbundle = splinter_start_subbundle(spl, node);
   bundle->end_subbundle = splinter_end_subbundle(spl, node);
   splinter_leaf_remove_bundles_except(spl, node, bundle_no);
   splinter_set_start_frac_branch(spl, node, splinter_start_branch(spl, node));
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
   if (!is_space_rec && pdata->srq_idx != -1 &&
         spl->cfg.reclaim_threshold != UINT64_MAX) {
      //platform_log("Deleting %12lu-%lu (index %lu) from SRQ\n",
      //      node->disk_addr, pdata->generation, pdata->srq_idx);
      srq_delete(&spl->srq, pdata->srq_idx);
      srq_print(&spl->srq);
      pdata->srq_idx = -1;
   }
   pdata->generation = splinter_inc_pivot_generation(spl, node);
   return bundle_no;
}

/*
 * Returns the index of the first branch in the bundle.
 */
static inline uint16
splinter_bundle_start_branch(splinter_handle *spl,
                             page_handle     *node,
                             splinter_bundle *bundle)
{
   splinter_subbundle *subbundle
      = splinter_get_subbundle(spl, node, bundle->start_subbundle);
   return subbundle->start_branch;
}

/*
 * Returns the index of the successor to the last branch in the bundle.
 */
static inline uint16
splinter_bundle_end_branch(splinter_handle *spl,
                           page_handle     *node,
                           splinter_bundle *bundle)
{
   uint16 last_subbundle_no
      = splinter_subbundle_no_sub(spl, bundle->end_subbundle, 1);
   splinter_subbundle *subbundle
      = splinter_get_subbundle(spl, node, last_subbundle_no);
   return subbundle->end_branch;
}

/*
 * Returns the number of (by definition fractional) branches in the bundle.
 */
static inline uint16
splinter_bundle_branch_count(splinter_handle *spl,
                             page_handle     *node,
                             splinter_bundle *bundle)
{
   return splinter_branch_no_sub(spl,
         splinter_bundle_end_branch(spl, node, bundle),
         splinter_bundle_start_branch(spl, node, bundle));
}

static inline uint16
splinter_bundle_subbundle_count(splinter_handle *spl,
                                page_handle     *node,
                                splinter_bundle *bundle)
{
   return splinter_subbundle_no_sub(spl,
                                    bundle->end_subbundle,
                                    bundle->start_subbundle);
}

/*
 * Returns the number of live bundles in the node.
 */
static inline uint16
splinter_bundle_count(splinter_handle *spl,
                      page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_bundle_no_sub(spl, hdr->end_bundle, hdr->start_bundle);
}

/*
 * Returns the number of live subbundles in the node.
 */
static inline uint16
splinter_subbundle_count(splinter_handle *spl,
                         page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_subbundle_no_sub(spl, hdr->end_subbundle,
         hdr->start_subbundle);
}

/*
 * Returns TRUE if the bundle is live in the node and FALSE otherwise.
 */
static inline bool
splinter_bundle_live(splinter_handle *spl,
                     page_handle     *node,
                     uint16           bundle_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_bundle_no_sub(spl, bundle_no, hdr->start_bundle)
      < splinter_bundle_no_sub(spl, hdr->end_bundle, hdr->start_bundle);
}

/*
 * Returns TRUE if the bundle is valid in the node (live or == end_bundle) and
 * FALSE otherwise.
 */
static inline bool
splinter_bundle_valid(splinter_handle *spl,
                      page_handle     *node,
                      uint16           bundle_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_bundle_no_sub(spl, bundle_no, hdr->start_bundle)
      <= splinter_bundle_no_sub(spl, hdr->end_bundle, hdr->start_bundle);
}

/*
 * Returns TRUE if the bundle is live for the pivot and FALSE otherwise
 */
static inline bool
splinter_bundle_live_for_pivot(splinter_handle     *spl,
                               page_handle         *node,
                               uint16               bundle_no,
                               uint16               pivot_no)
{
   debug_assert(pivot_no < splinter_num_children(spl, node));
   splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);
   uint16 branch_no = splinter_bundle_start_branch(spl, node, bundle);
   return splinter_branch_live_for_pivot(spl, node, branch_no, pivot_no);
}

static inline uint16
splinter_start_frac_branch(splinter_handle *spl,
                           page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->start_frac_branch;
}

static inline void
splinter_set_start_frac_branch(splinter_handle *spl,
                               page_handle     *node,
                               uint16           branch_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   hdr->start_frac_branch = branch_no;
}

static inline void
splinter_reset_start_frac_branch(splinter_handle *spl,
                                 page_handle     *node)
{
   if (splinter_bundle_count(spl, node) == 0) {
      splinter_set_start_frac_branch(spl, node, splinter_end_branch(spl, node));
   } else {
      uint16 start_bundle = splinter_start_bundle(spl, node);
      splinter_bundle *bundle = splinter_get_bundle(spl, node, start_bundle);
      uint16 start_frac_branch =
         splinter_bundle_start_branch(spl, node, bundle);
      splinter_set_start_frac_branch(spl, node, start_frac_branch);
   }
}

static inline void
splinter_clear_bundle(splinter_handle *spl,
                      page_handle     *node,
                      uint16           bundle_no)
{
   platform_assert(bundle_no == splinter_start_bundle(spl, node));

   splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);

   splinter_bundle_clear_subbundles(spl, node, bundle);
   splinter_inc_start_bundle(spl, node);

   // update the pivot start bundles
   for (uint16 pivot_no = 0; pivot_no < splinter_num_children(spl, node); pivot_no++) {
      splinter_pivot_data *pdata =
         splinter_get_pivot_data(spl, node, pivot_no);
      if (!splinter_bundle_valid(spl, node, pdata->start_bundle)) {
         pdata->start_bundle = splinter_start_bundle(spl, node);
      }
   }

   // update the fractional start branch
   splinter_reset_start_frac_branch(spl, node);
}

static inline void
splinter_tuples_in_bundle(
      splinter_handle *spl,
      page_handle     *node,
      splinter_bundle *bundle,
      uint64           pivot_count[static SPLINTER_MAX_PIVOTS])
{
   // Can't ZERO_ARRAY because degerates to a uint64 *
   ZERO_CONTENTS_N(pivot_count, SPLINTER_MAX_PIVOTS);

   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 branch_no = splinter_bundle_start_branch(spl, node, bundle);
        branch_no != splinter_bundle_end_branch(spl, node, bundle);
        branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
      for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
         pivot_count[pivot_no] +=
            splinter_pivot_tuples_in_branch(spl, node, pivot_no, branch_no);
      }
   }
}

static inline void
splinter_pivot_add_bundle_num_tuples(
   splinter_handle *spl,
   page_handle     *node,
   splinter_bundle *bundle,
   uint64           pivot_count[SPLINTER_MAX_PIVOTS])
{
   bundle->num_tuples = 0;
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      pdata->num_tuples += pivot_count[pivot_no];
      bundle->num_tuples += pivot_count[pivot_no];
   }
}

static inline void
splinter_bundle_inc_pivot_rc(splinter_handle *spl,
                             page_handle     *node,
                             splinter_bundle *bundle)
{
   uint16 num_children = splinter_num_children(spl, node);
   cache *cc = spl->cc;
   btree_config *btree_cfg = &spl->cfg.btree_cfg;
   // Skip the first pivot, because that has been inc'd in the parent
   for (uint16 branch_no = splinter_bundle_start_branch(spl, node, bundle);
        branch_no != splinter_bundle_end_branch(spl, node, bundle);
        branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
      splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
      for (uint64 pivot_no = 1; pivot_no < num_children; pivot_no++) {
         const char *key = splinter_get_pivot(spl, node, pivot_no);
         btree_inc_range(cc, btree_cfg, branch->root_addr, key, NULL);
      }
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * branch functions
 *
 *-----------------------------------------------------------------------------
 */

/*
 * has_vacancy returns TRUE unless there is not enough physical space in the
 * node to add another branch
 */

/*
 * Returns the number of live branches (including fractional branches).
 */
static inline uint16
splinter_branch_count(splinter_handle *spl,
                      page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_branch_no_sub(spl, hdr->end_branch, hdr->start_branch);
}

static inline bool
splinter_has_vacancy(splinter_handle *spl,
                     page_handle     *node,
                     uint16           num_new_branches)
{
   uint16 branch_count = splinter_branch_count(spl, node);
   uint16 max_branches = spl->cfg.hard_max_branches_per_node;
   return branch_count + num_new_branches + 1 < max_branches;
}

static inline splinter_branch *
splinter_get_branch(splinter_handle *spl,
                    page_handle     *node,
                    uint32           k)
{
   debug_assert(sizeof(splinter_trunk_hdr) +
         spl->cfg.max_pivot_keys * splinter_pivot_size(spl) +
         (k + 1) * sizeof(splinter_branch) < spl->cfg.page_size);

   char *cursor = node->data;
   cursor += sizeof(splinter_trunk_hdr) +
      spl->cfg.max_pivot_keys * splinter_pivot_size(spl) +
      k * sizeof(splinter_branch);
   return (splinter_branch *)cursor;
}

/*
 * get_new_branch allocates a new branch in the node and returns a pointer to
 * it.
 *
 * FIXME: [aconway 2020-06-21] Currently crashes if there is no room in the
 * buffer
 */
static inline splinter_branch *
splinter_get_new_branch(splinter_handle *spl,
                        page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   splinter_branch *new_branch =
      splinter_get_branch(spl, node, hdr->end_branch);
   hdr->end_branch = splinter_branch_no_add(spl, hdr->end_branch, 1);
   debug_assert(hdr->end_branch != hdr->start_branch);
   return new_branch;
}

static inline uint16
splinter_branch_no(splinter_handle *spl,
                   page_handle     *node,
                   splinter_branch *branch)
{
   return branch - splinter_get_branch(spl, node, 0);
}

static inline uint16
splinter_start_branch(splinter_handle *spl,
                      page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->start_branch;
}

static inline uint16
splinter_end_branch(splinter_handle *spl,
                    page_handle     *node)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return hdr->end_branch;
}

/*
 * branch_live checks if branch_no is live for any pivot in the node.
 */
static inline bool
splinter_branch_live(splinter_handle *spl,
                     page_handle     *node,
                     uint64           branch_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_branch_in_range(spl, branch_no, hdr->start_branch,
         hdr->end_branch);
}

/*
 * branch_valid checks if branch_no is being used by any pivot or is
 * end_branch. Used to verify if a given entry is valid.
 */
__attribute__ ((unused))
static inline bool
splinter_branch_valid(splinter_handle *spl,
                      page_handle     *node,
                      uint64           branch_no)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   return splinter_branch_no_sub(spl, branch_no, hdr->start_branch)
      <= splinter_branch_no_sub(spl, hdr->end_branch, hdr->start_branch);
}

static inline uint64
splinter_process_generation_to_pos(splinter_handle             *spl,
                                   splinter_compact_bundle_req *req,
                                   uint64                       generation)
{
   uint64 pos = 0;
   while (pos != SPLINTER_MAX_PIVOTS &&
          req->pivot_generation[pos] != generation) {
      pos++;
   }
   return pos;
}

/*
 * replace_bundle_branches replaces the branches of an uncompacted bundle with
 * a newly compacted branch.
 *
 * This process is:
 * 1. de-ref the old branches of the bundle
 * 2. add the new branch (unless replacement_branch == NULL)
 * 3. move any remaining branches to maintain a contiguous array
 * 4. adjust pivot start branches if necessary
 * 5. mark bundle as compacted and remove all by its first subbundle
 * 6. move any remaining subbundles to maintain a contiguous array (and adjust
 *    any remaining bundles to account)
 */
void
splinter_replace_bundle_branches(splinter_handle             *spl,
                                 page_handle                 *node,
                                 splinter_branch             *repl_branch,
                                 splinter_compact_bundle_req *req)
{
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;
   debug_assert(req->height == splinter_height(spl, node));

   uint16 bundle_no = req->bundle_no;
   splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);
   uint16 bundle_start_branch = splinter_bundle_start_branch(spl, node, bundle);
   uint16 bundle_end_branch = splinter_bundle_end_branch(spl, node, bundle);
   uint16 branch_diff = splinter_bundle_branch_count(spl, node, bundle);

   // de-ref the dead branches
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 branch_no = bundle_start_branch;
        branch_no != bundle_end_branch;
        branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
      splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
      for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
         if (splinter_bundle_live_for_pivot(spl, node, bundle_no, pivot_no)) {
            const char *start_key = splinter_get_pivot(spl, node, pivot_no);
            const char *end_key = splinter_get_pivot(spl, node, pivot_no + 1);
            splinter_zap_branch_range(spl, branch, start_key, end_key,
                  PAGE_TYPE_BRANCH);
         }
      }
   }

   // add new branch
   uint16 new_branch_no = UINT16_MAX;
   if (repl_branch != NULL) {
      splinter_branch *new_branch =
         splinter_get_branch(spl, node, bundle_start_branch);
      *new_branch = *repl_branch;
      branch_diff--;
      new_branch_no = splinter_branch_no(spl, node, new_branch);

      // increment the fringes of the new branch along the pivots
      uint16 num_pivot_keys = splinter_num_pivot_keys(spl, node);
      for (uint16 pivot_no = 1; pivot_no < num_pivot_keys; pivot_no++) {
         const char *start_key = splinter_get_pivot(spl, node, pivot_no);
         splinter_inc_intersection(spl, new_branch, start_key, FALSE);
      }

      // slice out the pivots ranges for which this branch is already dead
      for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
         if (!splinter_bundle_live_for_pivot(spl, node, bundle_no, pivot_no)) {
            const char *start_key = splinter_get_pivot(spl, node, pivot_no);
            const char *end_key = splinter_get_pivot(spl, node, pivot_no + 1);
            splinter_zap_branch_range(spl, new_branch, start_key, end_key, PAGE_TYPE_BRANCH);
         }
      }
   }

   // move any remaining branches to maintain a contiguous array
   for (uint16 branch_no = bundle_end_branch;
        branch_no != hdr->end_branch;
        branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
      uint16 dst_branch_no =
         splinter_branch_no_sub(spl, branch_no, branch_diff);
      *splinter_get_branch(spl, node, dst_branch_no) =
         *splinter_get_branch(spl, node, branch_no);
   }

   /*
    * if the bundle has no keys, move the filters to form a contiguous array
    */
   if (repl_branch == NULL) {
      // decrement the ref counts of the old filters
      for (uint16 filter_no = splinter_bundle_start_filter(spl, node, bundle);
           filter_no != splinter_bundle_end_filter(spl, node, bundle);
           filter_no = splinter_subbundle_no_add(spl, filter_no, 1)) {
         routing_filter *old_filter =
            splinter_get_sb_filter(spl, node, filter_no);
         splinter_dec_filter(spl, old_filter);
         //platform_log("dec filter %lu in %lu (%u)\n",
         //      old_filter->addr, node->disk_addr,
         //      allocator_get_refcount(spl->al, old_filter->addr));
      }

      // move any later filters
      uint16 filter_diff = splinter_bundle_filter_count(spl, node, bundle);
      for (uint16 filter_no = splinter_bundle_end_filter(spl, node, bundle);
           filter_no != splinter_end_sb_filter(spl, node);
           filter_no = splinter_subbundle_no_add(spl, filter_no, 1)) {
         uint16 dst_filter_no =
            splinter_subbundle_no_sub(spl, filter_no, filter_diff);
         *splinter_get_sb_filter(spl, node, dst_filter_no) =
            *splinter_get_sb_filter(spl, node, filter_no);
      }
   }

   /*
    * the compacted bundle will have a single branch in a single subbundle
    * containing all the filters.
    */
   uint16 sb_diff = splinter_bundle_subbundle_count(spl, node, bundle);
   uint16 first_later_sb = bundle->end_subbundle;
   if (repl_branch != NULL) {
      uint16 sb_no = bundle->start_subbundle;
      splinter_subbundle *sb = splinter_get_subbundle(spl, node, sb_no);
      sb->end_branch = splinter_branch_no_add(spl, bundle_start_branch, 1);
      sb->end_filter = splinter_bundle_end_filter(spl, node, bundle);
      sb->state = SB_STATE_COMPACTED;
      sb_diff--;
      bundle->end_subbundle = splinter_subbundle_no_add(spl, sb_no, 1);
   }

   for (uint16 sb_no = first_later_sb;
        sb_no != hdr->end_subbundle;
        sb_no = splinter_subbundle_no_add(spl, sb_no, 1)) {
      splinter_subbundle *sb = splinter_get_subbundle(spl, node, sb_no);
      sb->start_branch =
         splinter_branch_no_sub(spl, sb->start_branch, branch_diff);
      sb->end_branch = splinter_branch_no_sub(spl, sb->end_branch, branch_diff);
      uint16 dst_sb_no = splinter_subbundle_no_sub(spl, sb_no, sb_diff);
      *splinter_get_subbundle(spl, node, dst_sb_no) = *sb;
   }
   hdr->end_subbundle =
      splinter_subbundle_no_sub(spl, hdr->end_subbundle, sb_diff);
   for (uint16 later_bundle_no = splinter_bundle_no_add(spl, bundle_no, 1);
        later_bundle_no != hdr->end_bundle;
        later_bundle_no = splinter_bundle_no_add(spl, later_bundle_no, 1)) {
      splinter_bundle *bundle = splinter_get_bundle(spl, node, later_bundle_no);
      bundle->start_subbundle =
         splinter_subbundle_no_sub(spl, bundle->start_subbundle, sb_diff);
      bundle->end_subbundle =
         splinter_subbundle_no_sub(spl, bundle->end_subbundle, sb_diff);
   }
   debug_assert(splinter_bundle_start_branch(spl, node, bundle) ==
                bundle_start_branch);

   if (repl_branch == NULL) {
      for (uint16 later_bundle_no = bundle_no;
           later_bundle_no != splinter_bundle_no_sub(spl, hdr->end_bundle, 1);
           later_bundle_no = splinter_bundle_no_add(spl, later_bundle_no, 1)) {
         uint16 src_later_bundle_no =
            splinter_bundle_no_add(spl, later_bundle_no, 1);
         *splinter_get_bundle(spl, node, later_bundle_no) =
            *splinter_get_bundle(spl, node, src_later_bundle_no);
      }
      hdr->end_bundle = splinter_bundle_no_sub(spl, hdr->end_bundle, 1);
   }

   // fix the pivot start branches
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (!splinter_branch_live_for_pivot(spl, node, bundle_start_branch,
               pivot_no)) {
         pdata->start_branch =
            splinter_branch_no_sub(spl, pdata->start_branch, branch_diff);
         debug_assert(splinter_branch_valid(spl, node, pdata->start_branch));
      }
   }

   // fix the pivot tuples
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      if (splinter_bundle_live_for_pivot(spl, node, bundle_no, pivot_no)) {
         splinter_pivot_data *pdata =
            splinter_get_pivot_data(spl, node, pivot_no);
         uint64 pos =
            splinter_process_generation_to_pos(spl, req, pdata->generation);
         platform_assert(pos != SPLINTER_MAX_PIVOTS);
         uint64 bundle_num_tuples = req->input_pivot_count[pos];
         debug_assert(pdata->num_tuples >= bundle_num_tuples);
         pdata->num_tuples -= bundle_num_tuples;
         if (repl_branch != NULL) {
            req->output_pivot_count[pos] =
               splinter_pivot_tuples_in_branch(spl, node, pivot_no,
               new_branch_no);
            pdata->num_tuples += req->output_pivot_count[pos];
         }
         req->tuples_reclaimed +=
             req->input_pivot_count[pos] - req->output_pivot_count[pos];
      }
   }

   // update the end_branch
   hdr->end_branch = splinter_branch_no_sub(spl, hdr->end_branch, branch_diff);
}

static inline void
splinter_inc_branch_range(splinter_handle *spl,
                          splinter_branch *branch,
                          const char      *start_key,
                          const char      *end_key)
{
   if (branch->root_addr) {
      btree_inc_range(spl->cc, &spl->cfg.btree_cfg, branch->root_addr,
            start_key, end_key);
   }
}

static inline void
splinter_zap_branch_range(splinter_handle *spl,
                          splinter_branch *branch,
                          const char      *start_key,
                          const char      *end_key,
                          page_type        type)
{
   platform_assert(type == PAGE_TYPE_BRANCH || type == PAGE_TYPE_MEMTABLE);
   platform_assert(0 || (1 && start_key == NULL
                           && end_key == NULL)
                     || (1 && type != PAGE_TYPE_MEMTABLE
                           && start_key != NULL));
   if (branch->root_addr) {
      btree_zap_range(spl->cc, &spl->cfg.btree_cfg, branch->root_addr,
            start_key, end_key, PAGE_TYPE_BRANCH);
   }
   if (branch->range_root_addr) {
      btree_zap_range(spl->cc, &spl->cfg.btree_cfg, branch->range_root_addr,
            start_key, end_key, PAGE_TYPE_BRANCH);
   }
   // FIXME: [yfogel 2020-07-06] If/when we add range-delete filter
   //    (e.g. on components) we should clean up the range filter here
}

/*
 * Decrement the ref count for branch and destroy it and its filter if it
 * reaches 0.
 */
static inline void
splinter_dec_ref(splinter_handle *spl,
                 splinter_branch *branch,
                 bool             is_memtable)
{
   page_type type = is_memtable ? PAGE_TYPE_MEMTABLE : PAGE_TYPE_BRANCH;
   splinter_zap_branch_range(spl, branch, NULL, NULL, type);
}

/*
 * Increment the ref count for all extents whose key range intersects with key
 */
static inline void
splinter_inc_intersection(splinter_handle *spl,
                          splinter_branch *branch,
                          const char      *key,
                          bool             is_memtable)
{
   platform_assert(IMPLIES(is_memtable, key == NULL));
   splinter_inc_branch_range(spl, branch, key, NULL);
}

/*
 * splinter_btree_lookup performs a lookup for key in branch.
 *
 * Pre-conditions:
 *    If *found
 *       `data` has the most recent answer.
 *       the current memtable is older than the most recent answer
 *
 * Post-conditions:
 *    if *found, the data can be found in `data`.
 */

static inline bool
splinter_btree_lookup(splinter_handle *spl,
                      splinter_branch *branch,
                      const char      *key,
                      char            *data,
                      bool            *found)
{
   bool needs_merge = *found;
   bool local_found;
   btree_node node;

   char *data_temp;
   cache *const cc = spl->cc;
   btree_config *const cfg = &spl->cfg.btree_cfg;
   data_config *data_cfg = spl->cfg.data_cfg;
   btree_lookup_with_ref(cc, cfg, branch->root_addr, &node, PAGE_TYPE_BRANCH,
                         key, &data_temp, &local_found);
   if (local_found) {
      *found = TRUE;
      if (needs_merge) {
         data_merge_tuples(data_cfg, key, data_temp, data);
      } else {
         memmove(data, data_temp, splinter_message_size(spl));
      }
      btree_node_unget(cc, cfg, &node);
   }
   return local_found;
}


/*
 *-----------------------------------------------------------------------------
 * splinter_btree_lookup_async
 *
 * Pre-conditions:
 *    The ctxt should've been initialized using btree_ctxt_init().
 *    If *found
 *       `data` has the most recent answer.
 *       the current memtable is older than the most recent answer
 *
 *    The return value can be either of:
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
 *-----------------------------------------------------------------------------
 */

static cache_async_result
splinter_btree_lookup_async(splinter_handle     *spl,      // IN
                            splinter_branch     *branch,   // IN
                            char                *key,      // IN
                            char                *data,     // OUT
                            bool                *found,    // IN/OUT
                            btree_async_ctxt    *ctxt)     // IN
{
   bool needs_merge = *found;
   bool local_found;
   btree_node node;

   char *data_temp;
   cache *const cc = spl->cc;
   btree_config *const cfg = &spl->cfg.btree_cfg;
   data_config *data_cfg = spl->cfg.data_cfg;
   cache_async_result res =
      btree_lookup_async_with_ref(cc, cfg, branch->root_addr, &node, key,
                                  &data_temp, &local_found, ctxt);
   if (res == async_success && local_found) {
      *found = TRUE;
      if (needs_merge) {
         data_merge_tuples(data_cfg, key, data_temp, data);
      } else {
         memmove(data, data_temp, splinter_message_size(spl));
      }
      btree_node_unget(cc, cfg, &node);
   }

   return res;
}


/*
 *-----------------------------------------------------------------------------
 *
 * Memtable Functions
 *
 *-----------------------------------------------------------------------------
 */

// FIXME: [aconway 2020-09-02] Both these functions (get_memtable and
// try_get_memtable) belong in memtable.[ch]
memtable *
splinter_try_get_memtable(splinter_handle *spl,
                          uint64           generation)
{
   // FIXME: [aconway 2020-09-01] change to cfg
   uint64 memtable_idx = generation % SPLINTER_NUM_MEMTABLES;
   memtable *mt = &spl->mt_ctxt->mt[memtable_idx];
   if (mt->generation != generation) {
      mt = NULL;
   }
   return mt;
}

/*
 * returns the memtable with generation number generation. Caller must ensure
 * that there exists a memtable with the appropriate generation.
 */
memtable *
splinter_get_memtable(splinter_handle *spl,
                      uint64           generation)
{
   // FIXME: [aconway 2020-09-01] change to cfg
   uint64 memtable_idx = generation % SPLINTER_NUM_MEMTABLES;
   memtable *mt = &spl->mt_ctxt->mt[memtable_idx];
   platform_assert(mt->generation == generation);
   return mt;
}

splinter_compacted_memtable *
splinter_get_compacted_memtable(splinter_handle *spl,
                                uint64           generation)
{
   uint64 memtable_idx = generation % SPLINTER_NUM_MEMTABLES;

   // this call asserts the generation is correct
   memtable *mt = splinter_get_memtable(spl, generation);
   platform_assert(mt->state != MEMTABLE_STATE_READY);

   return &spl->compacted_memtable[memtable_idx];
}

static inline void
splinter_memtable_inc_ref(splinter_handle *spl,
                          uint64           mt_gen)
{
   memtable *mt = splinter_get_memtable(spl, mt_gen);
   allocator_inc_refcount(spl->al, mt->root_addr);
}


void
splinter_memtable_dec_ref(splinter_handle *spl,
                          uint64           generation)
{
   memtable *mt = splinter_get_memtable(spl, generation);
   memtable_dec_ref_maybe_recycle(spl->mt_ctxt, mt);

   // the branch in the compacted memtable is now in the tree, so don't zap it,
   // we don't try to zero out the cmt because that would introduce a race.
}


/*
 * Wrappers for creating/destroying memtable iterators. Increments/decrements
 * the memtable ref count and cleans up if ref count == 0
 */
static void
splinter_memtable_iterator_init(splinter_handle *spl,
                                btree_iterator  *itor,
                                uint64           root_addr,
                                const char      *min_key,
                                const char      *max_key,
                                bool             is_live,
                                bool             inc_refcount)
{
   if (inc_refcount) {
      allocator_inc_refcount(spl->al, root_addr);
   }
   btree_iterator_init(spl->cc, &spl->cfg.btree_cfg, itor, root_addr,
         PAGE_TYPE_MEMTABLE, min_key, max_key, FALSE, is_live, 0,
         data_type_point);
}

static void
splinter_memtable_iterator_deinit(splinter_handle *spl,
                                  btree_iterator  *itor,
                                  uint64           mt_gen,
                                  bool             dec_refcount)
{
   btree_iterator_deinit(itor);
   if (dec_refcount) {
      splinter_memtable_dec_ref(spl, mt_gen);
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
platform_status
splinter_memtable_insert(splinter_handle *spl,
                         char            *key,
                         char            *data)
{
   page_handle *lock_page;
   uint64 generation;
   platform_status rc = memtable_maybe_rotate_and_get_insert_lock(spl->mt_ctxt,
         &generation, &lock_page);
   if (!SUCCESS(rc)) {
      goto out;
   }

   // this call is safe because we hold the insert lock
   memtable *mt = splinter_get_memtable(spl, generation);
   uint64 leaf_generation; // used for ordering the log
   rc = memtable_insert(spl->mt_ctxt, mt, key, data, &leaf_generation);
   if (!SUCCESS(rc)) {
      goto unlock_insert_lock;
   }

   if (spl->cfg.use_log) {
      int crappy_rc = log_write(spl->log, key, data, leaf_generation);
      if (crappy_rc != 0) {
         goto unlock_insert_lock;
      }
   }

unlock_insert_lock:
   memtable_unget_insert_lock(spl->mt_ctxt, lock_page);
out:
   return rc;
}

/*
 * Compacts the memtable with generation generation and builds its filter.
 * Returns a pointer to the memtable.
 */
static memtable *
splinter_memtable_compact_and_build_filter(splinter_handle *spl,
                                           uint64           generation,
                                           const threadid   tid)
{
   timestamp comp_start = platform_get_timestamp();

   // FIXME: [yfogel 2020-07-01] initialization needs to happen ~here
   //    for range_root_addr
   memtable *mt = splinter_get_memtable(spl, generation);

   memtable_transition(mt, MEMTABLE_STATE_FINALIZED, MEMTABLE_STATE_COMPACTING);
   mini_allocator_release(&mt->mini, NULL);

   splinter_compacted_memtable *cmt =
      splinter_get_compacted_memtable(spl, generation);
   splinter_branch *new_branch = &cmt->branch;
   ZERO_CONTENTS(new_branch);

   uint64          memtable_root_addr = mt->root_addr;
   btree_iterator  btree_itor;
   iterator       *itor = &btree_itor.super;
   const char     *min_key = spl->cfg.data_cfg->min_key;
   // FIXME: [yfogel 2020-07-01] needs to also do the range tree.

   splinter_memtable_iterator_init(spl, &btree_itor, memtable_root_addr,
         min_key, NULL, FALSE, FALSE);
   btree_pack_req  req;
   btree_pack_req_init(&req, spl->cc, &spl->cfg.btree_cfg, itor,
                       spl->cfg.max_tuples_per_node, spl->cfg.leaf_filter_cfg.hash,
                       spl->cfg.leaf_filter_cfg.seed, spl->heap_id);
   uint64 pack_start;
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_compactions++;
      pack_start = platform_get_timestamp();
   }
   btree_pack(&req);
   platform_assert(req.num_tuples <= spl->cfg.max_tuples_per_node);
   debug_assert(req.num_tuples <= spl->cfg.mt_cfg.max_tuples_per_memtable);
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_compaction_pack_time_ns
         += platform_timestamp_elapsed(pack_start);
      spl->stats[tid].root_compaction_tuples += req.num_tuples;
      if (req.num_tuples > spl->stats[tid].root_compaction_max_tuples) {
         spl->stats[tid].root_compaction_max_tuples = req.num_tuples;
      }
   }
   splinter_memtable_iterator_deinit(spl, &btree_itor, FALSE, FALSE);

   new_branch->root_addr = req.root_addr;

   platform_assert(req.num_tuples > 0);
   uint64 filter_build_start;
   if (spl->cfg.use_stats) {
      filter_build_start = platform_get_timestamp();
   }

   cmt->req = TYPED_ZALLOC(spl->heap_id, cmt->req);
   cmt->req->spl = spl;
   cmt->req->fp_arr = req.fingerprint_arr;
   uint32 *dup_fp_arr = TYPED_ARRAY_MALLOC(spl->heap_id, dup_fp_arr, req.num_tuples);
   memmove(dup_fp_arr, cmt->req->fp_arr, req.num_tuples * sizeof(uint32));
   routing_filter empty_filter = { 0 };
   // FIXME: [aconway 2020-09-14] was index
   platform_status rc = routing_filter_add(spl->cc, &spl->cfg.leaf_filter_cfg,
         spl->heap_id, &empty_filter, &cmt->filter, cmt->req->fp_arr,
         req.num_tuples, 0);
   //platform_log("cre filter %lu in %lu (%u)\n",
   //      cmt->filter.addr, spl->root_addr,
   //      allocator_get_refcount(spl->al, cmt->filter.addr));
   platform_assert(SUCCESS(rc));
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_filter_time_ns +=
         platform_timestamp_elapsed(filter_build_start);
      spl->stats[tid].root_filters_built++;
      spl->stats[tid].root_filter_tuples += req.num_tuples; }

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
static inline bool
splinter_try_start_incorporate(splinter_handle *spl,
                               uint64           generation)
{
   bool should_start = FALSE;

   memtable_lock_incorporation_lock(spl->mt_ctxt);
   memtable *mt = splinter_try_get_memtable(spl, generation);
   if ((mt == NULL) ||
       (generation != memtable_generation_to_incorporate(spl->mt_ctxt)))
   {
      should_start = FALSE;
      goto unlock_incorp_lock;
   }
   // FIXME: [aconway 2020-09-02] this should be a transation, not a try and
   // return TRUE;
   should_start = memtable_try_transition(
         mt, MEMTABLE_STATE_COMPACTED, MEMTABLE_STATE_INCORPORATION_ASSIGNED);

unlock_incorp_lock:
   memtable_unlock_incorporation_lock(spl->mt_ctxt);
   return should_start;
}

static inline bool
splinter_try_continue_incorporate(splinter_handle *spl,
                                  uint64           next_generation)
{
   bool should_continue = FALSE;

   memtable_lock_incorporation_lock(spl->mt_ctxt);
   memtable *mt = splinter_try_get_memtable(spl, next_generation);
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
 *  4. Lock root (block lookups -- lookups obtain a read lock on the root
 *     before performing lookup on memtable)
 *  5. Add the memtable to the root as a new compacted bundle
 *  6. If root is full, flush until it is no longer full
 *  7. If necessary, split the root
 *  8. Create a new empty memtable in the memtable array at position
 *     curr_memtable.
 *  9. Unlock the root
 *
 * This functions has some preconditions prior to being called.
 *  --> Trunk root node should be write locked.
 *  --> The memtable should have inserts blocked (can_insert == FALSE)
 */

static void
splinter_memtable_incorporate(splinter_handle *spl,
                              uint64           generation,
                              const threadid   tid)
{
   // FIXME: [yfogel 2020-08-26] this logic needs to be moved INTO incorporate
   // FIXME: [yfogel,aconway 2020-08-26] merge conflicts need to update this as
   //        well.  Also don't know if this code is correct even before we do
   //        our context change.
   //        see https://reviewboard.eng.vmware.com/r/1704658
   //if (new_branch.num_tuples == 0) {
   //   /*
   //    * If the memtable is empty, we do not want to incorporate it,
   //    * just release the claim , read lock and zap.
   //    */
   //   splinter_branch memtable_branch;
   //   ZERO_STRUCT(memtable_branch);
   //   memtable_branch.root_addr = spl->memtable[curr_memtable]->root_addr;
   //   splinter_clear_memtable(spl, spl->memtable[curr_memtable]);
   //   splinter_node_unclaim(spl, root);
   //   splinter_node_unget(spl, &root);
   //   splinter_dec_ref(spl, &memtable_branch, TRUE);
   //   return;
   //}

   // X. Get, claim and lock the lookup lock
   // FIXME: [aconway 2020-09-01] Should probably first get claims on lookup
   // lock and root, then upgrade to locks
   page_handle *mt_lookup_lock_page =
      memtable_uncontended_get_claim_lock_lookup_lock(spl->mt_ctxt);

   memtable_increment_to_generation_retired(spl->mt_ctxt, generation);

   // X. Get, claim and lock the root
   page_handle *root = splinter_node_get(spl, spl->root_addr);
   splinter_node_claim(spl, &root);
   // FIXME: [aconway 2020-08-31] Change this to account for possibly
   //        incorporating more than 1 memtable
   platform_assert(splinter_has_vacancy(spl, root, 1));
   splinter_node_lock(spl, root);

   splinter_open_log_stream();
   splinter_log_stream("incorporate memtable gen %lu into root %lu\n", generation, spl->root_addr);
   splinter_log_node(spl, root);
   splinter_log_stream("----------------------------------------\n");

   // X. Release lookup lock
   memtable_unlock_unclaim_unget_lookup_lock(spl->mt_ctxt, mt_lookup_lock_page);

   /*
    * X. Get a new branch in a bundle for the memtable
    */
   splinter_compacted_memtable *cmt =
      splinter_get_compacted_memtable(spl, generation);
   splinter_compact_bundle_req *req = cmt->req;
   req->bundle_no = splinter_get_new_bundle(spl, root);
   splinter_bundle *bundle = splinter_get_bundle(spl, root, req->bundle_no);
   splinter_subbundle *sb = splinter_get_new_subbundle(spl, root, 1);
   splinter_branch *branch = splinter_get_new_branch(spl, root);
   *branch = cmt->branch;
   bundle->start_subbundle = splinter_subbundle_no(spl, root, sb);
   bundle->end_subbundle = splinter_end_subbundle(spl, root);
   sb->start_branch = splinter_branch_no(spl, root, branch);
   sb->end_branch = splinter_end_branch(spl, root);
   sb->state = SB_STATE_COMPACTED;
   routing_filter *filter = splinter_subbundle_filter(spl, root, sb, 0);
   *filter = cmt->filter;
   req->spl = spl;
   req->addr = spl->root_addr;
   req->height = splinter_height(spl, root);
   req->generation = splinter_generation(spl, root);
   req->max_pivot_generation = splinter_pivot_generation(spl, root);
   // FIXME: [aconway 2020-09-01] This should happen during claim before
   // lock, for root maybe even just read (SPLINTER-87)
   splinter_tuples_in_bundle(spl, root, bundle, req->output_pivot_count);
   splinter_pivot_add_bundle_num_tuples(spl, root, bundle, req->output_pivot_count);
   uint16 num_children = splinter_num_children(spl, root);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      if (pivot_no != 0) {
         const char *key = splinter_get_pivot(spl, root, pivot_no);
         splinter_inc_intersection(spl, branch, key, FALSE);
      }
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, root, pivot_no);
      req->pivot_generation[pivot_no] = pdata->generation;
   }
   debug_assert(splinter_subbundle_branch_count(spl, root, sb) != 0);
   splinter_log_stream("enqueuing build filter %lu-%u\n",
         req->addr, req->bundle_no);
   task_enqueue(spl->ts, TASK_TYPE_NORMAL, splinter_bundle_build_filters, req,
                TRUE);

   // X. Incorporate new memtable into the bundle
   memtable *mt = splinter_get_memtable(spl, generation);
   // Normally need to hold incorp_mutex, but debug code and also guaranteed no
   // one is changing gen_to_incorp (we are the only thread that would try)
   debug_assert(generation == memtable_generation_to_incorporate(spl->mt_ctxt));
   memtable_transition(mt, MEMTABLE_STATE_INCORPORATION_ASSIGNED,
                           MEMTABLE_STATE_INCORPORATING);
   *branch = cmt->branch;
   *filter = cmt->filter;
   if (spl->cfg.use_stats) {
      spl->stats[tid].memtable_flush_wait_time_ns
         += platform_timestamp_elapsed(cmt->wait_start);
   }


   memtable_transition(
         mt, MEMTABLE_STATE_INCORPORATING, MEMTABLE_STATE_INCORPORATED);
   splinter_log_node(spl, root);
   splinter_log_stream("----------------------------------------\n");
   splinter_log_stream("\n");
   splinter_close_log_stream();

   // X. If root is full, flush until it is no longer full
   uint64 flush_start;
   if (spl->cfg.use_stats) {
      flush_start = platform_get_timestamp();
   }
   bool did_flush = FALSE;
   uint64 wait = 1;
   while (!did_flush && splinter_node_is_full(spl, root)) {
      did_flush = splinter_flush_fullest(spl, root);
      if (!did_flush) {
         splinter_node_unlock(spl, root);
         platform_sleep(wait);
         wait = wait > 2048 ? 2048 : 2 * wait;
         splinter_node_lock(spl, root);
      }
   }

   // X. If necessary, split the root
   if (splinter_needs_split(spl, root)) {
      splinter_split_root(spl, root);
   }

   // X. Unlock the root
   splinter_node_unlock(spl, root);
   splinter_node_unclaim(spl, root);
   splinter_node_unget(spl, &root);

   // X. Dec-ref the now-incorporated memtable
   memtable_dec_ref_maybe_recycle(spl->mt_ctxt, mt);

   if (spl->cfg.use_stats) {
      // FIXME: [yfogel 2020-08-26] for stats, may need to store more..
      // somewhere? especially when handed off
      const threadid tid = platform_get_tid();
      flush_start = platform_timestamp_elapsed(flush_start);
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
splinter_memtable_flush_internal(splinter_handle *spl,
                                 uint64           generation)
{
   const threadid tid = platform_get_tid();
   // pack and build filter.
   splinter_memtable_compact_and_build_filter(spl, generation, tid);

   // If we are assigned to do so, incorporate the memtable onto the root node.
   if (!splinter_try_start_incorporate(spl, generation)) {
      goto out;
   }
   do {
      splinter_memtable_incorporate(spl, generation, tid);
      generation++;
   } while (splinter_try_continue_incorporate(spl, generation));
out:
   return;
}

static void
splinter_memtable_flush_internal_virtual(void *arg, void *scratch)
{
   splinter_memtable_args *mt_args = arg;
   splinter_memtable_flush_internal(mt_args->spl, mt_args->generation);
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

void
splinter_memtable_flush(splinter_handle *spl,
                        uint64           generation)
{
   splinter_compacted_memtable *cmt =
      splinter_get_compacted_memtable(spl, generation);
   cmt->mt_args.spl = spl;
   cmt->mt_args.generation = generation;
   task_enqueue(spl->ts, TASK_TYPE_MEMTABLE,
         splinter_memtable_flush_internal_virtual, &cmt->mt_args, FALSE);
}

void
splinter_memtable_flush_virtual(void   *arg,
                                uint64  generation)
{
   splinter_handle *spl = arg;
   splinter_memtable_flush(spl, generation);
}

static inline uint64
splinter_memtable_root_addr_for_lookup(splinter_handle *spl,
                                       uint64           generation,
                                       bool            *is_compacted)
{
   // FIXME: [aconway 2020-09-02] This needs to be try or the caller needs to
   // try (and then pass the mt into here)
   memtable *mt = splinter_get_memtable(spl, generation);
   platform_assert(memtable_ok_to_lookup(mt));

   if (memtable_ok_to_lookup_compacted(mt)) {
      // lookup in packed tree
      *is_compacted = TRUE;
      splinter_compacted_memtable *cmt =
         splinter_get_compacted_memtable(spl, generation);
      return cmt->branch.root_addr;
   } else {
      *is_compacted = FALSE;
      return mt->root_addr;
   }
}

/*
 * splinter_memtable_lookup
 *
 * Pre-conditions:
 *    If *found
 *       `data` has the most recent answer.
 *       the current memtable is older than the most recent answer
 *
 * Post-conditions:
 *    if *found, the data can be found in `data`.
 */
static bool
splinter_memtable_lookup(splinter_handle *spl,
                         uint64           generation,
                         char            *key,
                         char            *data,
                         bool            *found)
{
   // FIXME: [aconway 2020-08-26] look in compacted if available
   bool needs_merge = *found;
   bool local_found;
   btree_node node;

   cache *const cc = spl->cc;
   btree_config *const cfg = &spl->cfg.btree_cfg;
   data_config *data_cfg = spl->cfg.data_cfg;
   char *data_temp;

   // FIXME: [aconway 2020-09-01] Maybe use this in stats
   bool memtable_is_compacted;
   uint64 root_addr = splinter_memtable_root_addr_for_lookup(spl, generation,
         &memtable_is_compacted);
   btree_lookup_with_ref(cc, cfg, root_addr, &node, PAGE_TYPE_MEMTABLE, key,
         &data_temp, &local_found);

   if (local_found) {
      *found = TRUE;
      if (needs_merge) {
         data_merge_tuples(data_cfg, key, data_temp, data);
      } else {
         memmove(data, data_temp, splinter_message_size(spl));
      }
      btree_node_unget(cc, cfg, &node);
      message_type type = data_message_class(data_cfg, data);
      if (type != MESSAGE_TYPE_UPDATE) {
         return FALSE;
      }
   }
   return TRUE;
}

/*
 *-----------------------------------------------------------------------------
 *
 * filter functions
 *
 *-----------------------------------------------------------------------------
 */

static inline routing_config *
splinter_routing_cfg(splinter_handle *spl,
                     bool             is_leaf)
{
   //if (is_leaf) {
      return &spl->cfg.leaf_filter_cfg;
   //} else {
   //   return &spl->cfg.index_filter_cfg;
   //}
}

static inline void
splinter_inc_filter(splinter_handle *spl,
                    routing_filter  *filter)
{
   debug_assert(filter->addr != 0);
   __attribute__ ((unused)) uint8 ref =
      allocator_inc_refcount(spl->al, filter->addr);
}

static inline void
splinter_dec_filter(splinter_handle *spl,
                    routing_filter  *filter)
{
   if (filter->addr == 0) {
      return;
   }
   cache *cc = spl->cc;
   page_handle *meta_page;
   uint64 wait = 100;
   while (1) {
      meta_page = cache_get(cc, filter->meta_head, TRUE, PAGE_TYPE_FILTER);
      if (cache_claim(cc, meta_page)) {
         break;
      }
      cache_unget(cc, meta_page);
      platform_sleep(wait);
      wait *= 2;
   }
   cache_lock(cc, meta_page);

   /*
    * This is the only entry point to dec this ref count, so we are guaranteed
    * that there isn't a race.
    */
   uint8 ref = allocator_get_refcount(spl->al, filter->addr);
   if (ref > 2) {
      cache_dealloc(cc, filter->addr, PAGE_TYPE_FILTER);
      cache_unlock(cc, meta_page);
      cache_unclaim(cc, meta_page);
      cache_unget(cc, meta_page);
      return;
   }

   // we are responsible for zapping the whole tree
   cache_unlock(cc, meta_page);
   cache_unclaim(cc, meta_page);
   cache_unget(cc, meta_page);

   routing_filter_zap(cc, filter);
}

static inline page_handle *
splinter_node_get_maybe_descend(splinter_handle             *spl,
                                splinter_compact_bundle_req *req)
{
   page_handle *node = splinter_node_get(spl, req->addr);
   while (splinter_height(spl, node) != req->height) {
      debug_assert(splinter_height(spl, node) > req->height);
      debug_assert(req->height != 0);
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
      req->addr = pdata->addr;
      splinter_open_log_stream();
      splinter_log_stream("build_filter descending from root\n");
      splinter_log_stream("enqueuing build filter %lu-%u\n",
            req->addr, req->bundle_no);
      splinter_log_node(spl, node);
      splinter_node_unget(spl, &node);
      node = splinter_node_get(spl, req->addr);
   }
   return node;
}

static inline page_handle *
splinter_node_get_claim_maybe_descend(splinter_handle             *spl,
                                      splinter_compact_bundle_req *req)
{
   page_handle *node;
   uint64 wait = 1;
   while (1) {
      node = splinter_node_get_maybe_descend(spl, req);
      if (cache_claim(spl->cc, node)) {
         break;
      }
      splinter_node_unget(spl, &node);
      platform_sleep(wait);
      wait = wait > 2048 ? wait : 2 * wait;
   }
   return node;
}

static inline bool
splinter_build_filter_should_abort(splinter_compact_bundle_req *req,
                                   page_handle                 *node)
{
   splinter_handle *spl = req->spl;
   uint16 height = splinter_height(spl, node);
   if (height == 0 && req->generation < splinter_generation(spl, node)) {
      splinter_open_log_stream();
      splinter_log_stream("build_filter leaf abort %lu-%u\n",
            req->addr, req->bundle_no);
      splinter_log_node(spl, node);
      splinter_close_log_stream();
      return TRUE;
   }
   return FALSE;
}

static inline bool
splinter_build_filter_should_skip(splinter_compact_bundle_req *req,
                                  page_handle                 *node)
{
   splinter_handle *spl = req->spl;
   if (!splinter_bundle_live(spl, node, req->bundle_no)) {
      splinter_open_log_stream();
      splinter_log_stream("build_filter flush abort %lu-%u (%u)\n",
            req->addr, req->bundle_no, req->height);
      splinter_log_node(spl, node);
      splinter_close_log_stream();
      return TRUE;
   }
   return FALSE;
}

static inline bool
splinter_build_filter_should_reenqueue(splinter_compact_bundle_req *req,
                                       page_handle                 *node)
{
   splinter_handle *spl = req->spl;
   if (req->bundle_no != splinter_start_bundle(spl, node)) {
      splinter_open_log_stream();
      splinter_log_stream("build filter for %lu bundle %u\n",
            req->addr, req->bundle_no);
      splinter_log_node(spl, node);
      splinter_log_stream("reenqueuing build filter %lu-%u\n",
            req->addr, req->bundle_no);
      splinter_close_log_stream();
      return TRUE;
   }
   return FALSE;
}

static inline void
splinter_prepare_build_filter(splinter_handle             *spl,
                              splinter_compact_bundle_req *req,
                              page_handle                 *node)
{
   uint16 height = splinter_height(spl, node);
   platform_assert(req->height == height);
   platform_assert(req->bundle_no == splinter_start_bundle(spl, node));

   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (splinter_bundle_live_for_pivot(spl, node, req->bundle_no, pivot_no)) {
         uint64 pos =
            splinter_process_generation_to_pos(spl, req, pdata->generation);
         platform_assert(pos != SPLINTER_MAX_PIVOTS);
         req->old_filter[pos] = pdata->filter;
         req->value[pos] = splinter_pivot_whole_branch_count(spl, node, pdata);
         req->should_build[pos] = TRUE;
      }
   }
}

static inline void
splinter_process_generation_to_fp_bounds(splinter_handle             *spl,
                                         splinter_compact_bundle_req *req,
                                         uint64                       generation,
                                         uint32                      *fp_start,
                                         uint32                      *fp_end)
{
   uint64 pos = 0;
   uint64 fp_start_int = 0;
   while (pos != SPLINTER_MAX_PIVOTS &&
          req->pivot_generation[pos] != generation) {
      fp_start_int += req->output_pivot_count[pos];
      pos++;
   }
   platform_assert(pos + 1 != SPLINTER_MAX_PIVOTS);
   uint64 fp_end_int = fp_start_int + req->output_pivot_count[pos];
   *fp_start = fp_start_int;
   *fp_end = fp_end_int;
}

static inline void
splinter_build_filters(splinter_handle             *spl,
                       splinter_compact_bundle_req *req)
{
   threadid tid;
   uint64 filter_build_start;
   uint16 height;
   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
      height = req->height;
      filter_build_start = platform_get_timestamp();
   }

   for (uint64 pos = 0; pos < SPLINTER_MAX_PIVOTS; pos++) {
      if (!req->should_build[pos]) {
         continue;
      }
      routing_filter old_filter = req->old_filter[pos];
      uint32 fp_start, fp_end;
      uint64 generation = req->pivot_generation[pos];
      splinter_process_generation_to_fp_bounds(spl, req, generation, &fp_start,
                                               &fp_end);
      uint32 *fp_arr = req->fp_arr + fp_start;
      uint32 num_fingerprints = fp_end - fp_start;
      if (num_fingerprints == 0) {
         if (old_filter.addr != 0) {
            splinter_inc_filter(spl, &old_filter);
         }
         req->filter[pos] = old_filter;
         continue;
      }
      routing_filter new_filter;
      routing_config *filter_cfg = &spl->cfg.leaf_filter_cfg;
      //routing_config *filter_cfg = height == 0 ?
      //   &spl->cfg.leaf_filter_cfg : &spl->cfg.index_filter_cfg;
      uint16 value = req->value[pos];
      platform_status rc = routing_filter_add(spl->cc, filter_cfg,
            spl->heap_id, &old_filter, &new_filter, fp_arr, num_fingerprints,
            value);
      //platform_log("cre filter %lu in %lu (%u), gen %lu\n",
      //      new_filter.addr, req->addr,
      //      allocator_get_refcount(spl->al, new_filter.addr), generation);
      platform_assert(SUCCESS(rc));

      req->filter[pos] = new_filter;
      req->should_build[pos] = FALSE;
      if (spl->cfg.use_stats) {
         spl->stats[tid].filters_built[height]++;
         spl->stats[tid].filter_tuples[height] += num_fingerprints;
      }
   }

   if (spl->cfg.use_stats) {
      spl->stats[tid].filter_time_ns[height]
         += platform_timestamp_elapsed(filter_build_start);
   }
}

static inline void
splinter_replace_routing_filter(splinter_handle             *spl,
                                splinter_compact_bundle_req *req,
                                page_handle                 *node)
{
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      uint64 pos =
         splinter_process_generation_to_pos(spl, req, pdata->generation);
      if (!splinter_bundle_live_for_pivot(spl, node, req->bundle_no, pivot_no)) {
         //platform_log("not live: %lu %lu %lu %lu\n",
         //      node->disk_addr, pdata->generation, pos, req->filter[pos].addr);
         if (pos != SPLINTER_MAX_PIVOTS && req->filter[pos].addr != 0) {
            splinter_dec_filter(spl, &req->filter[pos]);
            ZERO_CONTENTS(&req->filter[pos]);
            //platform_log("dec filter %lu in %lu (%u)\n",
            //      req->filter[pos].addr, node->disk_addr,
            //      allocator_get_refcount(spl->al, req->filter[pos].addr));
         }
         continue;
      }
      platform_assert(pos != SPLINTER_MAX_PIVOTS);
      debug_assert(pdata->generation < req->max_pivot_generation);
      splinter_dec_filter(spl, &pdata->filter);
      //platform_log("dec filter %lu in %lu (%u)\n",
      //      pdata->filter.addr, node->disk_addr,
      //      allocator_get_refcount(spl->al, pdata->filter.addr));
      pdata->filter = req->filter[pos];
      ZERO_CONTENTS(&req->filter[pos]);
      uint64 num_tuples_to_reclaim =
         splinter_pivot_tuples_to_reclaim(spl, pdata);
      //platform_log("New routing filter with %u fp %u unique (%lu to reclaim)\n",
      //      pdata->filter.num_fingerprints, pdata->filter.num_unique,
      //      num_tuples_to_reclaim);
      if (pdata->srq_idx != -1 && spl->cfg.reclaim_threshold != UINT64_MAX) {
         //platform_log("Updating %12lu-%lu:%8lu into SRQ\n",
         //   node->disk_addr, pdata->generation, num_tuples_to_reclaim);
         srq_update(&spl->srq, pdata->srq_idx, num_tuples_to_reclaim);
         srq_print(&spl->srq);
      } else if (num_tuples_to_reclaim > SPLINTER_MIN_SPACE_RECL && spl->cfg.reclaim_threshold != UINT64_MAX) {
         srq_data data = { .addr             = node->disk_addr,
                           .pivot_generation = pdata->generation,
                           .priority         = num_tuples_to_reclaim };
         //platform_log("Inserting %12lu-%lu:%8lu into SRQ\n",
         //      data.addr, data.pivot_generation, data.priority);
         pdata->srq_idx = srq_insert(&spl->srq, data);
         srq_print(&spl->srq);
      }
   }
}


/*
 * Asynchronous task function which builds routing filters for a compacted
 * bundle
 */
void
splinter_bundle_build_filters(void *arg,
                              void *scratch)
{
   splinter_compact_bundle_req *req = (splinter_compact_bundle_req *)arg;
   splinter_handle *spl = req->spl;

   uint64 generation;
   do {
      page_handle *node = splinter_node_get_maybe_descend(spl, req);
      platform_assert(node != NULL);

      splinter_open_log_stream();
      splinter_log_stream("build filter for %lu bundle %u pivot_gen %lu\n",
            req->addr, req->bundle_no, req->max_pivot_generation);
      splinter_log_node(spl, node);
      if (splinter_build_filter_should_abort(req, node)) {
         splinter_log_stream("leaf split, aborting\n");
         splinter_node_unget(spl, &node);
         goto out;
      }
      if (splinter_build_filter_should_skip(req, node)) {
         splinter_log_stream("bundle flushed, skipping\n");
         goto next_node;
      }

      if (splinter_build_filter_should_reenqueue(req, node)) {
         task_enqueue(spl->ts, TASK_TYPE_NORMAL,
                      splinter_bundle_build_filters, req, FALSE);
         splinter_log_stream("out of order, reequeuing\n");
         splinter_close_log_stream();
         splinter_node_unget(spl, &node);
         return;
      }

      for (uint64 i = 0; i < SPLINTER_MAX_PIVOTS; i++) {
         platform_assert(!req->should_build[i]);
      }
      splinter_prepare_build_filter(spl, req, node);
      req->filter_generation = splinter_generation(spl, node);
      splinter_node_unget(spl, &node);

      splinter_build_filters(spl, req);

      splinter_log_stream("----------------------------------------\n");

      do {
         node = splinter_node_get_claim_maybe_descend(spl, req);
         if (splinter_build_filter_should_abort(req, node)) {
            splinter_log_stream("replace_filter abort leaf split (%lu)\n", req->addr);
            splinter_node_unclaim(spl, node);
            splinter_node_unget(spl, &node);
            goto out;
         }
         splinter_node_lock(spl, node);
         splinter_replace_routing_filter(spl, req, node);
         if (splinter_bundle_live(spl, node, req->bundle_no)) {
            splinter_clear_bundle(spl, node, req->bundle_no);
         }
         splinter_node_unlock(spl, node);
         splinter_node_unclaim(spl, node);
         req->addr = splinter_next_addr(spl, node);
         generation = splinter_generation(spl, node);
         debug_assert(splinter_verify_node(spl, node));
         if (generation != req->filter_generation) {
            splinter_log_stream("replace_filter split to %lu\n", req->addr);
            debug_assert(req->height != 0);
            debug_assert(req->addr != 0);
            splinter_node_unget(spl, &node);
         }
      } while (generation != req->filter_generation);

      splinter_log_node(spl, node);
      splinter_log_stream("----------------------------------------\n");
      splinter_log_stream("\n");

next_node:
      req->addr = splinter_next_addr(spl, node);
      generation = splinter_generation(spl, node);
      debug_assert(splinter_verify_node(spl, node));
      splinter_node_unget(spl, &node);
      if (req->generation != generation) {
         splinter_log_stream("build_filter split to %lu\n", req->addr);
         debug_assert(req->height != 0);
         debug_assert(req->addr != 0);
      }
      splinter_close_log_stream();
   } while (req->generation != generation);

out:
   for (uint64 pos = 0; pos < SPLINTER_MAX_PIVOTS; pos++) {
      splinter_dec_filter(spl, &req->filter[pos]);
   }
   platform_free(spl->heap_id, req->fp_arr);
   platform_free(spl->heap_id, req);
   splinter_maybe_reclaim_space(spl);
   return;
}

static cache_async_result
splinter_filter_lookup_async(splinter_handle    *spl,
                             routing_config     *cfg,
                             routing_filter     *filter,
                             char               *key,
                             uint64             *found_values,
                             routing_async_ctxt *ctxt)
{
   return routing_filter_lookup_async(spl->cc, cfg, filter, key, found_values,
         ctxt);
}

/*
 *-----------------------------------------------------------------------------
 *
 * Flush Functions
 *
 *-----------------------------------------------------------------------------
 */

/*
 * flush_into_bundle flushes all live branches (including fractional branches)
 * for the pivot from parent to a new bundle in child and initializes the
 * compact_bundle_req.
 *
 * NOTE: parent and child must be write locked.
 */
splinter_bundle *
splinter_flush_into_bundle(splinter_handle             *spl,    // IN
                           page_handle                 *parent, // IN (modified)
                           page_handle                 *child,  // IN (modified)
                           splinter_pivot_data         *pdata,  // IN
                           splinter_compact_bundle_req *req)    // IN/OUT
{
   splinter_open_log_stream();
   splinter_log_stream("flush from %lu to %lu\n", parent->disk_addr,
                       child->disk_addr);
   splinter_log_node(spl, parent);
   splinter_log_node(spl, child);
   splinter_log_stream("----------------------------------------\n");

   req->spl = spl;
   req->addr = child->disk_addr;
   req->height = splinter_height(spl, child);
   debug_assert(req->addr != 0);
   req->bundle_no = splinter_get_new_bundle(spl, child);
   req->generation = splinter_generation(spl, child);
   req->max_pivot_generation = splinter_pivot_generation(spl, child);

   uint16 num_children = splinter_num_children(spl, child);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, child, pivot_no);
      req->pivot_generation[pivot_no] = pdata->generation;
   }

   splinter_bundle *bundle = splinter_get_bundle(spl, child, req->bundle_no);

   // if there are whole branches, flush them into a subbundle
   if (splinter_branch_is_whole(spl, parent, pdata->start_branch)) {
      splinter_subbundle *child_sb =
         splinter_get_new_subbundle(spl, child, 1);
      bundle->start_subbundle =
         splinter_subbundle_no(spl, child, child_sb);
      child_sb->state = SB_STATE_UNCOMPACTED_INDEX;

      // create a subbundle from the whole branches of the parent
      child_sb->start_branch = splinter_end_branch(spl, child);
      splinter_log_stream("subbundle %hu\n", bundle->start_subbundle);
      for (uint16 branch_no = pdata->start_branch;
           splinter_branch_is_whole(spl, parent, branch_no);
           branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
         splinter_branch *parent_branch =
            splinter_get_branch(spl, parent, branch_no);
         splinter_log_stream("%lu\n", parent_branch->root_addr);
         splinter_branch *new_branch = splinter_get_new_branch(spl, child);
         *new_branch = *parent_branch;
      }
      child_sb->end_branch = splinter_end_branch(spl, child);
      routing_filter *child_filter =
         splinter_subbundle_filter(spl, child, child_sb, 0);
      *child_filter = pdata->filter;
      ZERO_STRUCT(pdata->filter);
      debug_assert(splinter_subbundle_branch_count(spl, child, child_sb) != 0);
   } else {
      bundle->start_subbundle = splinter_end_subbundle(spl, child);
   }

   // for each subbundle in the parent, create a subbundle in the child
   if (splinter_pivot_bundle_count(spl, parent, pdata) != 0) {
      uint16 pivot_start_sb_no =
         splinter_pivot_start_subbundle(spl, parent, pdata);
      for (uint16 parent_sb_no = pivot_start_sb_no;
           parent_sb_no != splinter_end_subbundle(spl, parent);
           parent_sb_no = splinter_subbundle_no_add(spl, parent_sb_no, 1)) {
         splinter_subbundle *parent_sb =
            splinter_get_subbundle(spl, parent, parent_sb_no);
         uint16 filter_count =
            splinter_subbundle_filter_count(spl, parent, parent_sb);
         splinter_subbundle *child_sb =
            splinter_get_new_subbundle(spl, child, filter_count);
         child_sb->state = parent_sb->state;
         child_sb->start_branch = splinter_end_branch(spl, child);
         splinter_log_stream("subbundle %hu from subbundle %hu\n",
                             splinter_subbundle_no(spl, child, child_sb),
                             parent_sb_no);
         for (uint16 branch_no = parent_sb->start_branch;
              branch_no != parent_sb->end_branch;
              branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
            splinter_branch *parent_branch =
               splinter_get_branch(spl, parent, branch_no);
            splinter_log_stream("%lu\n", parent_branch->root_addr);
            splinter_branch *new_branch =
               splinter_get_new_branch(spl, child);
            *new_branch = *parent_branch;
         }
         child_sb->end_branch = splinter_end_branch(spl, child);
         for (uint16 i = 0; i < filter_count; i++) {
            routing_filter *child_filter =
               splinter_subbundle_filter(spl, child, child_sb, i);
            routing_filter *parent_filter =
               splinter_subbundle_filter(spl, parent, parent_sb, i);
            *child_filter = *parent_filter;
            splinter_inc_filter(spl, child_filter);
            //splinter_log_stream("inc filter %lu in %lu (%u)\n",
            //      child_filter->addr, child->disk_addr,
            //      allocator_get_refcount(spl->al, child_filter->addr));
         }
         debug_assert(splinter_subbundle_branch_count(spl,
                                                      child, child_sb) != 0);
      }
   }
   bundle->end_subbundle = splinter_end_subbundle(spl, child);

   // clear the branches in the parent's pivot
   splinter_pivot_clear(spl, parent, pdata);

   splinter_log_stream("----------------------------------------\n");
   splinter_log_node(spl, parent);
   splinter_log_node(spl, child);
   splinter_log_stream("flush done\n");
   splinter_log_stream("\n");
   splinter_close_log_stream();

   return bundle;
}

/*
 * room_to_flush checks that there is enough physical space in child to flush
 * from parent.
 *
 * NOTE: parent and child must have at least read locks
 */
static inline bool
splinter_room_to_flush(splinter_handle     *spl,
                       page_handle         *parent,
                       page_handle         *child,
                       splinter_pivot_data *pdata)
{
   uint16 child_branches = splinter_branch_count(spl, child);
   uint16 flush_branches = splinter_pivot_branch_count(spl, parent, pdata);
   uint16 child_bundles = splinter_bundle_count(spl, child);
   uint16 child_subbundles = splinter_subbundle_count(spl, child);
   uint16 flush_subbundles =
      splinter_pivot_subbundle_count(spl, parent, pdata) + 1;
   return child_branches + flush_branches < spl->cfg.hard_max_branches_per_node
      && child_bundles + 2 <= SPLINTER_MAX_BUNDLES
      && child_subbundles + flush_subbundles + 1 < SPLINTER_MAX_SUBBUNDLES;
}

/*
 * flush flushes from parent to the child indicated by pdata and returns TRUE
 * if the flush was successful and false otherwise. Failure can occur if there
 * is not enough space in the child.
 *
 * NOTE: parent must be write locked
 */
bool
splinter_flush(splinter_handle     *spl,
               page_handle         *parent,
               splinter_pivot_data *pdata,
               bool                 is_space_rec)
{
   uint64 wait_start, flush_start;
   if (spl->cfg.use_stats)
      wait_start = platform_get_timestamp();
   page_handle *child = splinter_node_get(spl, pdata->addr);
   threadid tid;
   platform_status rc;

   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
   }
   splinter_node_claim(spl, &child);

   if (!splinter_room_to_flush(spl, parent, child, pdata)) {
      platform_error_log("Flush failed: %lu %lu\n",
                         parent->disk_addr, child->disk_addr);
      if (spl->cfg.use_stats) {
         if (parent->disk_addr == spl->root_addr)
            spl->stats[tid].root_failed_flushes++;
         else
            spl->stats[tid].failed_flushes[splinter_height(spl, parent)]++;
      }
      splinter_node_unclaim(spl, child);
      splinter_node_unget(spl, &child);
      return FALSE;
   }

   if (!is_space_rec && pdata->srq_idx != -1 && spl->cfg.reclaim_threshold != UINT64_MAX) {
      //platform_log("Deleting %12lu-%lu (index %lu) from SRQ\n",
      //      parent->disk_addr, pdata->generation, pdata->srq_idx);
      srq_delete(&spl->srq, pdata->srq_idx);
      srq_print(&spl->srq);
      pdata->srq_idx = -1;
   }
   splinter_node_lock(spl, child);

   if (spl->cfg.use_stats) {
      if (parent->disk_addr == spl->root_addr) {
         spl->stats[tid].root_flush_wait_time_ns
            += platform_timestamp_elapsed(wait_start);
      } else {
         spl->stats[tid].flush_wait_time_ns[splinter_height(spl, parent)]
            += platform_timestamp_elapsed(wait_start);
      }
      flush_start = platform_get_timestamp();
   }

   // flush the branch references into a new bundle in the child
   splinter_compact_bundle_req *req = TYPED_ZALLOC(spl->heap_id, req);
   splinter_bundle *bundle = splinter_flush_into_bundle(spl, parent, child,
                                                        pdata, req);
   splinter_tuples_in_bundle(spl, child, bundle, req->input_pivot_count);
   splinter_pivot_add_bundle_num_tuples(spl, child, bundle,
         req->input_pivot_count);
   splinter_bundle_inc_pivot_rc(spl, child, bundle);
   debug_assert(cache_page_valid(spl->cc, req->addr));
   req->is_space_rec = is_space_rec;

   // split child if necessary
   if (splinter_needs_split(spl, child)) {
      if (splinter_is_leaf(spl, child)) {
         platform_free(spl->heap_id, req);
         uint16 child_idx = splinter_pdata_to_pivot_index(spl, parent, pdata);
         splinter_split_leaf(spl, parent, child, child_idx);
         debug_assert(splinter_verify_node(spl, child));
         return TRUE;
      } else {
         uint64 child_idx = splinter_pdata_to_pivot_index(spl, parent, pdata);
         splinter_split_index(spl, parent, child, child_idx);
      }
   }

   debug_assert(splinter_verify_node(spl, child));
   splinter_node_unlock(spl, child);
   splinter_node_unclaim(spl, child);
   splinter_node_unget(spl, &child);

   splinter_default_log("enqueuing compact_bundle %lu-%u\n",
                        req->addr, req->bundle_no);
   rc = task_enqueue(spl->ts, TASK_TYPE_NORMAL, splinter_compact_bundle, req,
                     FALSE);
   platform_assert_status_ok(rc);
   if (spl->cfg.use_stats) {
      flush_start = platform_timestamp_elapsed(flush_start);
      if (parent->disk_addr == spl->root_addr) {
         spl->stats[tid].root_flush_time_ns += flush_start;
         if (flush_start > spl->stats[tid].root_flush_time_max_ns) {
            spl->stats[tid].root_flush_time_max_ns = flush_start;
         }
      } else {
         const uint32 h = splinter_height(spl, parent);
         spl->stats[tid].flush_time_ns[h] += flush_start;
         if (flush_start > spl->stats[tid].flush_time_max_ns[h]) {
            spl->stats[tid].flush_time_max_ns[h] = flush_start;
         }
      }
   }
   return TRUE;
}

/*
 * flush_fullest first flushes any pivots with too many live logical branches.
 * If the node is still full, it then flushes the pivot with the most tuples.
 *
 * FIXME: [aconway 2020-06-21] The name doesn't make it clear what the function
 * does.
 */
bool
splinter_flush_fullest(splinter_handle *spl,
                       page_handle     *node)
{
   splinter_pivot_data *fullest_pivot_data = splinter_get_pivot_data(spl, node, 0);
   uint16 pivot_no;
   threadid tid;

   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
   }
   if (splinter_pivot_needs_flush(spl, node, fullest_pivot_data)) {
      splinter_flush(spl, node, fullest_pivot_data, FALSE);
      if (spl->cfg.use_stats) {
         if (node->disk_addr == spl->root_addr)
            spl->stats[tid].root_count_flushes++;
         else
            spl->stats[tid].count_flushes[splinter_height(spl, node)]++;
      }
   }
   for (pivot_no = 1; pivot_no < splinter_num_children(spl, node); pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      // if a pivot has too many branches, just flush it here
      if (splinter_pivot_needs_flush(spl, node, pdata)) {
         splinter_flush(spl, node, pdata, FALSE);
         if (spl->cfg.use_stats) {
            if (node->disk_addr == spl->root_addr)
               spl->stats[tid].root_count_flushes++;
            else
               spl->stats[tid].count_flushes[splinter_height(spl, node)]++;
         }
      }
      if (pdata->num_tuples > fullest_pivot_data->num_tuples) {
         fullest_pivot_data = pdata;
      }
   }
   if (splinter_node_is_full(spl, node)) {
      if (spl->cfg.use_stats) {
         if (node->disk_addr == spl->root_addr)
            spl->stats[tid].root_full_flushes++;
         else
            spl->stats[tid].full_flushes[splinter_height(spl, node)]++;
      }
      return splinter_flush(spl, node, fullest_pivot_data, FALSE);
   }
   return FALSE;
}

UNUSED_FUNCTION()
static inline uint64
branch_choose_root(splinter_branch *branch,
                   data_type type)
{
   switch (type) {
      case data_type_point:
         return branch->root_addr;
      case data_type_range:
         return branch->range_root_addr;
      case data_type_invalid:
      default:
         platform_assert(FALSE);
   }
}

static inline btree_config*
splinter_choose_btree_config(splinter_handle *spl,
                             data_type type)
{
   switch (type) {
      case data_type_point:
         return &spl->cfg.btree_cfg;
      case data_type_range:
         return &spl->cfg.range_btree_cfg;
      case data_type_invalid:
      default:
         platform_assert(FALSE);
   }
}

void
save_pivots_to_compact_bundle_scratch(splinter_handle        *spl,     // IN
                                      page_handle            *node,    // IN
                                      data_type               type,    // IN
                                      compact_bundle_scratch *scratch) // IN/OUT
{
   uint32 num_pivot_keys = splinter_num_pivot_keys(spl, node);

   btree_config *cfg = splinter_choose_btree_config(spl, type);

   debug_assert(num_pivot_keys < ARRAY_SIZE(scratch->saved_pivot_keys));

   // Save all num_pivots regular pivots and the upper bound pivot
   for (uint32 i = 0; i < num_pivot_keys; i++) {
      memmove(&scratch->saved_pivot_keys[i].k,
              splinter_get_pivot(spl, node, i),
              cfg->data_cfg->key_size);
   }
}

/*
 * Branch iterator wrapper functions
 */

void
splinter_branch_iterator_init(splinter_handle *spl,
                              btree_iterator  *itor,
                              splinter_branch *branch,
                              const char      *min_key,
                              const char      *max_key,
                              bool             do_prefetch,
                              data_type        data_type,
                              bool             should_inc_ref)
{
   cache *cc = spl->cc;
   btree_config *btree_cfg = data_type == data_type_point ?
      &spl->cfg.btree_cfg : &spl->cfg.range_btree_cfg;
   uint64 root_addr = data_type == data_type_point ?
      branch->root_addr : branch->range_root_addr;
   if (root_addr != 0 && should_inc_ref) {
      btree_inc_range(cc, btree_cfg, root_addr, min_key, max_key);
   }
   btree_iterator_init(cc, btree_cfg, itor, root_addr, PAGE_TYPE_BRANCH,
         min_key, max_key, do_prefetch, FALSE, 0, data_type);
}

void
splinter_branch_iterator_deinit(splinter_handle *spl,
                                btree_iterator  *itor,
                                bool             should_dec_ref)
{
   if (itor->root_addr == 0) {
      return;
   }
   cache *cc = spl->cc;
   btree_config *btree_cfg = &spl->cfg.btree_cfg;
   const char *min_key = itor->min_key;
   const char *max_key = itor->max_key;
   btree_iterator_deinit(itor);
   if (should_dec_ref) {
      btree_zap_range(cc, btree_cfg, itor->root_addr, min_key, max_key, PAGE_TYPE_BRANCH);
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * btree skiperator
 *
 *       an iterator which can skip over tuples in branches which aren't live
 *
 *-----------------------------------------------------------------------------
 */

static void
splinter_btree_skiperator_init(
      splinter_handle           *spl,
      splinter_btree_skiperator *skip_itor,
      page_handle               *node,
      uint16                     branch_idx,
      data_type                  data_type,
      key_buffer                 pivots[static SPLINTER_MAX_PIVOTS])
{
   ZERO_CONTENTS(skip_itor);
   skip_itor->super.ops = &splinter_btree_skiperator_ops;
   uint16 min_pivot_no = 0;
   uint16 max_pivot_no = splinter_num_children(spl, node);
   debug_assert(max_pivot_no < SPLINTER_MAX_PIVOTS);

   char *min_key = pivots[min_pivot_no].k;
   char *max_key = pivots[max_pivot_no].k;
   skip_itor->branch = *splinter_get_branch(spl, node, branch_idx);

   uint16 first_pivot = 0;
   bool iterator_started = FALSE;

   for (uint16 i = min_pivot_no; i < max_pivot_no + 1; i++) {
      bool branch_valid = i == max_pivot_no ?
         FALSE : splinter_branch_live_for_pivot(spl, node, branch_idx, i);
      if (branch_valid && !iterator_started) {
         first_pivot = i;
         iterator_started = TRUE;
      }
      if (!branch_valid && iterator_started) {
         // create a new btree iterator
         char *pivot_min_key = first_pivot == min_pivot_no ?
            min_key : pivots[first_pivot].k;
         char *pivot_max_key = i == max_pivot_no ? max_key : pivots[i].k;
         btree_iterator *btree_itor = &skip_itor->itor[skip_itor->end++];
         splinter_branch_iterator_init(spl, btree_itor, &skip_itor->branch,
               pivot_min_key, pivot_max_key, TRUE, data_type, TRUE);
         iterator_started = FALSE;
      }
   }

   bool at_end;
   if (skip_itor->curr != skip_itor->end)
      iterator_at_end(&skip_itor->itor[skip_itor->curr].super, &at_end);
   else
      at_end = TRUE;

   while (skip_itor->curr != skip_itor->end && at_end) {
      iterator_at_end(&skip_itor->itor[skip_itor->curr].super, &at_end);
      if (!at_end)
         break;
      skip_itor->curr++;
   }
}

void
splinter_btree_skiperator_get_curr(iterator   *itor,
                                   char      **key,
                                   char      **data,
                                   data_type  *type)
{
   debug_assert(itor != NULL);
   splinter_btree_skiperator *skip_itor = (splinter_btree_skiperator *)itor;
   iterator_get_curr(&skip_itor->itor[skip_itor->curr].super, key, data, type);
}

platform_status
splinter_btree_skiperator_advance(iterator *itor)
{
   debug_assert(itor != NULL);
   splinter_btree_skiperator *skip_itor = (splinter_btree_skiperator *)itor;
   platform_status rc =
      iterator_advance(&skip_itor->itor[skip_itor->curr].super);
   if (!SUCCESS(rc)) {
      return rc;
   }

   bool at_end;
   iterator_at_end(&skip_itor->itor[skip_itor->curr].super, &at_end);
   while (skip_itor->curr != skip_itor->end && at_end) {
      iterator_at_end(&skip_itor->itor[skip_itor->curr].super, &at_end);
      if (!at_end)
         break;
      skip_itor->curr++;
   }

   return STATUS_OK;
}

platform_status
splinter_btree_skiperator_at_end(iterator *itor,
                                 bool     *at_end)
{
   splinter_btree_skiperator *skip_itor = (splinter_btree_skiperator *)itor;
   if (skip_itor->curr == skip_itor->end) {
      *at_end = TRUE;
      return STATUS_OK;
   }

   iterator_at_end(&skip_itor->itor[skip_itor->curr].super, at_end);
   return STATUS_OK;
}

void
splinter_btree_skiperator_print(iterator *itor)
{
   splinter_btree_skiperator *skip_itor = (splinter_btree_skiperator *)itor;
   platform_log("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");
   platform_log("$$ skiperator: %p\n", skip_itor);
   platform_log("$$ curr: %lu\n", skip_itor->curr);
   iterator_print(&skip_itor->itor[skip_itor->curr].super);
}

void
splinter_btree_skiperator_deinit(splinter_handle           *spl,
                                 splinter_btree_skiperator *skip_itor)
{
   for (uint64 i = 0; i < skip_itor->end; i++) {
      splinter_branch_iterator_deinit(spl, &skip_itor->itor[i], TRUE);
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * Compaction Functions
 *
 *-----------------------------------------------------------------------------
 */


// FIXME: [yfogel 2020-07-02]
// figureout if this need to support both or can just support one and
// May want to add range_btree_pack
// probably want this to handle the branch
// and call the lower level pack_req_init twice
// but may want to wrap this function in another that handles two.
// API needs to change if you do that so yo ucan PASS IN filter cfg
// (or hash/seed) for example
static inline void
splinter_branch_pack_req_init(splinter_handle *spl,
                              iterator        *itor,
                              branch_pack_req *req)
{
   btree_pack_req_init(&req->point_req, spl->cc, &spl->cfg.btree_cfg, itor,
                       spl->cfg.max_tuples_per_node, spl->cfg.leaf_filter_cfg.hash,
                       spl->cfg.leaf_filter_cfg.seed, spl->heap_id);
   // only point tree uses filter for now
   btree_pack_req_init(&req->range_req, spl->cc, &spl->cfg.range_btree_cfg,
                       itor, spl->cfg.max_tuples_per_node, NULL, 0,
                       spl->heap_id);
}

__attribute__ ((unused))
static inline void
splinter_branch_pack_req_deinit(splinter_handle *spl,
                                branch_pack_req *req)
{
   // only point tree uses filter for now
   btree_pack_req_deinit(&req->point_req, spl->heap_id);
}

/*
 * compact_bundle compacts a bundle of flushed branches into a single branch
 *
 * See "Interactions between Concurrent Processes"
 * (numbering here mirrors that section)
 *
 * Interacts with splitting in two ways:
 * 4. Internal node split occurs between job issue and this compact_bundle call:
 *    the bundle was split too, issue compact_bundle on the new siblings
 * 6. Leaf split occurs before this call or during compaction:
 *    the bundle will be compacted as part of the split, so this compaction is
 *    aborted if split occurred before this call or discarded if it occurred
 *    during compaction.
 *
 * Node splits are determined using generation numbers (in trunk_hdr)
 *   internal: generation number of left node is incremented on split
 *      -- given generation number g of a node, all the nodes it split
 *         into can be found by searching right until a node with
 *         generation number g is found
 *   leaf: generation numbers of all leaves affected by split are
 *         incremented
 *      -- can tell if a leaf has split by checking if generation number
 *         has changed
 *
 * Algorithm:
 * 1.  Acquire node read lock
 * 2.  Flush if node is full (acquires write lock)
 * 3.  If the node has split before this call (interaction 4), this
 *     bundle exists in the new split siblings, so issue compact_bundles
 *     for those nodes
 * 4.  Abort if node is a leaf and started splitting (interaction 6)
 * 5.  The bundle may have been completely flushed by step 2, if so abort
 * 6.  Build iterators
 * 7.  Release read lock
 * 8.  Perform compaction
 * 9.  Build filter
 * 10. Clean up
 * 11. Reacquire read lock
 * 12. For each newly split sibling replace bundle with new branch unless
 *        a. node if leaf which has split, in which case discard (interaction 6)
 *        b. node is internal and bundle has been flushed
 */


void
splinter_compact_bundle(void *arg,
                        void *scratch_buf)
{
   platform_status rc;
   splinter_compact_bundle_req *req = arg;
   splinter_task_scratch *task_scratch = scratch_buf;
   compact_bundle_scratch *scratch = &task_scratch->compact_bundle;
   splinter_handle *spl = req->spl;
   __attribute__ ((unused)) threadid tid;

   /*
    * 1. Acquire node read lock
    */
   page_handle *node = splinter_node_get(spl, req->addr);

   /*
    * 2. Flush if node is full (acquires write lock)
    */
   uint16 height = splinter_height(spl, node);
   if (height != 0 && splinter_node_is_full(spl, node)) {
      splinter_node_claim(spl, &node);
      splinter_node_lock(spl, node);
      bool flush_successful = TRUE;
      while (flush_successful && splinter_node_is_full(spl, node))
         flush_successful = splinter_flush_fullest(spl, node);
      splinter_node_unlock(spl, node);
      splinter_node_unclaim(spl, node);
   }

   // timers for stats if enabled
   uint64 compaction_start, pack_start;

   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
      compaction_start = platform_get_timestamp();
      spl->stats[tid].compactions[height]++;
   }

   /*
    * 3. If the node has split before this call (interaction 4), this
    *    bundle was copied to the new sibling[s], so issue compact_bundles for
    *    those nodes
    */
   if (req->generation < splinter_generation(spl, node)) {
      if (height != 0) {
         debug_assert(splinter_next_addr(spl, node) != 0);
         splinter_compact_bundle_req *next_req =
            TYPED_MALLOC(spl->heap_id, next_req);
         memmove(next_req, req, sizeof(splinter_compact_bundle_req));
         next_req->addr = splinter_next_addr(spl, node);
         debug_assert(next_req->addr != 0);

         req->generation = splinter_generation(spl, node);

         splinter_default_log("compact_bundle split from %lu to %lu\n",
                              req->addr, next_req->addr);
         rc = task_enqueue(spl->ts, TASK_TYPE_NORMAL, splinter_compact_bundle,
                           next_req, FALSE);
         platform_assert_status_ok(rc);
      } else {
         /*
          * 4. Abort if node is a splitting leaf (interaction 6)
          */
         splinter_node_unget(spl, &node);
         splinter_default_log("compact_bundle abort leaf split %lu\n",
               req->addr);
         platform_free(spl->heap_id, req);
         if (spl->cfg.use_stats) {
            spl->stats[tid].compactions_aborted_leaf_split[height]++;
            spl->stats[tid].compaction_time_wasted_ns[height]
               += platform_timestamp_elapsed(compaction_start);
         }
         return;
      }
   }

   // store the generation of the node so we can detect splits after compaction
   uint64 start_generation = splinter_generation(spl, node);

   /*
    * 5. The bundle may have been completely flushed by 2., if so abort
    *       -- note this cannot happen in leaves (if the bundle isn't live, the
    *          generation number would change and it would be caught by step 4
    *          above).
    */
   if (!splinter_bundle_live(spl, node, req->bundle_no)) {
      debug_assert(height != 0);
      splinter_node_unget(spl, &node);
      splinter_default_log("compact_bundle abort flushed %lu\n", req->addr);
      platform_free(spl->heap_id, req);
      if (spl->cfg.use_stats) {
         spl->stats[tid].compactions_aborted_flushed[height]++;
         spl->stats[tid].compaction_time_wasted_ns[height]
            += platform_timestamp_elapsed(compaction_start);
      }
      return;
   }

   splinter_bundle *bundle    = splinter_get_bundle(spl, node, req->bundle_no);
   uint16 bundle_start_branch = splinter_bundle_start_branch(spl, node, bundle);
   uint16 bundle_end_branch   = splinter_bundle_end_branch(spl, node, bundle);
   uint16 num_branches        = splinter_bundle_branch_count(spl, node, bundle);
   uint16 num_trees           = num_branches * 2;

   /*
    * Update and delete messages need to be kept around until/unless they have
    * been applied all the way down to the very last branch tree.  Even once it
    * reaches the leaf, it isn't going to be applied to the last branch tree
    * unless the compaction includes the oldest B-tree in the leaf (the start
    * branch).
    */
   bool resolve_updates_and_discard_deletes =
      height == 0 && bundle_start_branch == splinter_start_branch(spl, node);

   splinter_open_log_stream();
   splinter_log_stream("compact_bundle addr %lu bundle %hu\n", req->addr,
                       req->bundle_no);

   /*
    * 6. Build iterators
    */
   // FIXME: [nsarmicanic 2020-07-02] calc num_trees and use it instead of
   //  num_branches
   //  Figure out num_itors or num_trees is a better name (For the entire func)
   platform_assert(num_trees <= ARRAY_SIZE(scratch->skip_itor));
   splinter_btree_skiperator *skip_itor_arr = scratch->skip_itor;
   iterator **itor_arr = scratch->itor_arr;

   save_pivots_to_compact_bundle_scratch(spl, node, data_type_point,
                                         scratch);

   uint16 tree_offset = 0;
   for (uint16 branch_no = bundle_start_branch; branch_no != bundle_end_branch;
         branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
      /*
       * We are iterating from oldest to newest branch
       *
       * RangeDeleteTree is older than PointTree in the same branch
       * therefor range_delete tree skiperator should be earlier
       * than point_tree skiperator
       */
      splinter_btree_skiperator_init(spl, &skip_itor_arr[tree_offset], node,
                                     branch_no, data_type_range,
                                     scratch->saved_pivot_keys);
      itor_arr[tree_offset] = &skip_itor_arr[tree_offset].super;
      tree_offset++;

      splinter_btree_skiperator_init(spl, &skip_itor_arr[tree_offset], node,
                                     branch_no, data_type_point,
                                     scratch->saved_pivot_keys);
      itor_arr[tree_offset] = &skip_itor_arr[tree_offset].super;
      tree_offset++;

      // FIXME: [nsarmicanic 2020-07-02] Is it branch ref count or tree refcoutn?
      //       refcount is per branch, but we should increment
      //       the refcount for each iterator created
      //       (it's two references)
      //       on creation it's trivial to just add one refrence
      //       but a little harder to only remove one reference at end
      //       so if it's easy to clean up just 1, that's fine, otherwise
      //       add one reference for EACH iterator created (each tree)
      //       if you feel there is perf on the tabel, leave a FIXME explaining
      //       the type of optimization before committing
   }
   splinter_log_node(spl, node);

   /*
    * 7. Release read lock
    */
   splinter_node_unget(spl, &node);

   /*
    * 8. Perform compaction
    */
   merge_iterator *merge_itor;
   // FIXME: [yfogel 2020-07-01] Determine if we properly
   // initialized range_cfg (probably since using standard inits?)
   rc = merge_iterator_create(
         spl->heap_id, spl->cfg.data_cfg, &spl->cfg.range_data_cfg,
         num_trees, itor_arr, resolve_updates_and_discard_deletes,
         resolve_updates_and_discard_deletes, TRUE, &merge_itor);
   platform_assert_status_ok(rc);
   branch_pack_req pack_req;
   splinter_branch_pack_req_init(spl, &merge_itor->super, &pack_req);
   req->fp_arr = pack_req.point_req.fingerprint_arr;
   if (spl->cfg.use_stats) {
      pack_start = platform_get_timestamp();
   }
   // FIXME: [yfogel 2020-07-06] is there a refcount inside here?
   //    need to be carefull to make sure we have refcounts right
   //    need to maybe kill one tree here if it's fully empty
   //    and then make sure refcount is on the remaining one's
   //    address
   branch_pack(spl->heap_id, &pack_req);
   if (spl->cfg.use_stats) {
      spl->stats[tid].compaction_pack_time_ns[height]
         += platform_timestamp_elapsed(pack_start);
   }
   btree_pack_req *point_req = &pack_req.point_req;
   btree_pack_req *range_req = &pack_req.range_req;
   //platform_assert(point_req->num_tuples <= spl->cfg.max_tuples_per_node);
   //platform_assert(range_req->num_tuples <= spl->cfg.max_tuples_per_node);

   splinter_branch new_branch;
   new_branch.root_addr = point_req->root_addr;
   // FIXME: [yfogel 2020-07-01] will need to initialize range root addr
   //    ~here
   new_branch.range_root_addr = range_req->root_addr;

   req->fp_arr = point_req->fingerprint_arr;

   splinter_log_stream("output: %lu\n", point_req->root_addr);
   if (spl->cfg.use_stats) {
      // FIXME: [aconway 2020-09-12] Need to check range as well at some point
      if (point_req->num_tuples == 0) {
         spl->stats[tid].compactions_empty[height]++;
      }
      spl->stats[tid].compaction_tuples[height] += point_req->num_tuples;
      if (point_req->num_tuples > spl->stats[tid].compaction_max_tuples[height]) {
         spl->stats[tid].compaction_max_tuples[height] = point_req->num_tuples;
      }
   }

   splinter_log_stream("output point: %lu\n", point_req->root_addr);
   splinter_log_stream("output range: %lu\n", range_req->root_addr);
   // FIXME: [aconway 2020-09-14] Can't deinit here, because build_filter will
   // use the fp_arr
   //splinter_branch_pack_req_deinit(spl, &pack_req);

   /*
    * 10. Clean up
    */
   rc = merge_iterator_destroy(spl->heap_id, &merge_itor);
   platform_assert_status_ok(rc);
   // FIXME: [nsarmicanic 2020-07-02] num_itors
   for (uint64 i = 0; i < num_trees; i++) {
      splinter_btree_skiperator_deinit(spl, &skip_itor_arr[i]);
   }

   /*
    * 11. Reacquire read lock
    */
   node = splinter_node_get(spl, req->addr);
   platform_assert(node != NULL);

   /*
    * 12. For each newly split sibling replace bundle with new branch
    */
   uint64 addr = req->addr;
   uint64 num_replacements = 0;
   uint64 generation;
   do {
      platform_assert(node != NULL);
      splinter_node_claim(spl, &node);
      splinter_node_lock(spl, node);

      splinter_log_node(spl, node);

      /*
       * 12a. ...unless node is a leaf which has split, in which case discard
       *      (interaction 6)
       *
       *      For leaves, the split will cover the compaction and we do not
       *      need to look for the bundle in the split siblings, so simply
       *      exit.
       */
      if (height == 0 && req->generation < splinter_generation(spl, node)) {
         splinter_log_stream("compact_bundle discard split %lu\n", req->addr);
         if (spl->cfg.use_stats) {
            spl->stats[tid].compactions_discarded_leaf_split[height]++;
            spl->stats[tid].compaction_time_wasted_ns[height]
               += platform_timestamp_elapsed(compaction_start);
         }
         splinter_node_unlock(spl, node);
         splinter_node_unclaim(spl, node);
         splinter_node_unget(spl, &node);

         splinter_dec_ref(spl, &new_branch, FALSE);
         platform_free(spl->heap_id, req->fp_arr);
         platform_free(spl->heap_id, req);
         goto out;
      }

      if (splinter_bundle_live(spl, node, req->bundle_no)) {
         if (point_req->num_tuples != 0) {
            splinter_replace_bundle_branches(spl, node, &new_branch, req);
            num_replacements++;
            splinter_log_stream("inserted %lu into %lu\n",
                                new_branch.root_addr, addr);
         } else {
            // FIXME: [yfogel 2020-07-02] come back
            // This may create an empty branch, set the empty one to null before getting in loop?
            // 1. Don't create it until the first element per tree: Perf cost
            // 2. Delete it after pack: Maybe wierd interaction with ref_count
            splinter_replace_bundle_branches(spl, node, NULL, req);
            splinter_log_stream("compact_bundle empty %lu\n", addr);
         }
      } else {
         /*
          * 12b. ...unless node is internal and bundle has been flushed
          */
         platform_assert(height != 0);
         splinter_log_stream("compact_bundle discarded flushed %lu\n", addr);
      }

      addr = splinter_next_addr(spl, node);
      generation = splinter_generation(spl, node);
      splinter_log_node(spl, node);
      debug_assert(splinter_verify_node(spl, node));

      if (1 && num_replacements != 0
            && point_req->num_tuples != 0
            && start_generation == generation) {
         const char *max_key = splinter_max_key(spl, node);
         splinter_zap_branch_range(spl, &new_branch, max_key, NULL,
               PAGE_TYPE_BRANCH);
      }

      splinter_node_unlock(spl, node);
      splinter_node_unclaim(spl, node);
      splinter_node_unget(spl, &node);
      if (start_generation != generation) {
         debug_assert(height != 0);
         debug_assert(addr != 0);
         node = splinter_node_get(spl, addr);
      }
   } while (start_generation != generation);

   if (spl->cfg.use_stats) {
      if (req->is_space_rec) {
         spl->stats[tid].space_rec_tuples_reclaimed[height] +=
            req->tuples_reclaimed;
      }
      spl->stats[tid].tuples_reclaimed[height] += req->tuples_reclaimed;
   }
   if (num_replacements == 0) {
      splinter_dec_ref(spl, &new_branch, FALSE);
      if (spl->cfg.use_stats) {
         spl->stats[tid].compactions_discarded_flushed[height]++;
         spl->stats[tid].compaction_time_wasted_ns[height]
            += platform_timestamp_elapsed(compaction_start);
      }
      platform_free(spl->heap_id, req);
   } else {
      if (spl->cfg.use_stats) {
         compaction_start = platform_timestamp_elapsed(compaction_start);
         spl->stats[tid].compaction_time_ns[height] += compaction_start;
         if (compaction_start > spl->stats[tid].compaction_time_max_ns[height]) {
            spl->stats[tid].compaction_time_max_ns[height] = compaction_start;
         }
      }
      splinter_log_stream("enqueuing build filter %lu-%u\n",
            req->addr, req->bundle_no);
      task_enqueue(spl->ts, TASK_TYPE_NORMAL, splinter_bundle_build_filters, req, TRUE);
   }
out:
   splinter_log_stream("\n");
   splinter_close_log_stream();
}


bool
splinter_flush_node(splinter_handle *spl, uint64 addr, void *arg)
{
   page_handle *node = splinter_node_get(spl, addr);
   splinter_node_claim(spl, &node);
   splinter_node_lock(spl, node);

   if (splinter_height(spl, node) != 0) {
      for (uint16 pivot_no = 0;
           pivot_no < splinter_num_children(spl, node);
           pivot_no++) {
         splinter_pivot_data *pdata =
            splinter_get_pivot_data(spl, node, pivot_no);
         if (splinter_pivot_branch_count(spl, node, pdata) != 0)
            splinter_flush(spl, node, pdata, FALSE);
      }
   }

   splinter_node_unlock(spl, node);
   splinter_node_unclaim(spl, node);
   splinter_node_unget(spl, &node);

   task_perform_all(spl->ts);

   node = splinter_node_get(spl, addr);
   splinter_node_claim(spl, &node);
   splinter_node_lock(spl, node);

   if (splinter_height(spl, node) == 1) {
      for (uint16 pivot_no = 0;
           pivot_no < splinter_num_children(spl, node);
           pivot_no++) {
         splinter_pivot_data *pdata =
            splinter_get_pivot_data(spl, node, pivot_no);
         page_handle *leaf = splinter_node_get(spl, pdata->addr);
         splinter_node_claim(spl, &leaf);
         splinter_node_lock(spl, leaf);
         splinter_split_leaf(spl, node, leaf, pivot_no);
      }
   }

   splinter_node_unlock(spl, node);
   splinter_node_unclaim(spl, node);
   splinter_node_unget(spl, &node);

   task_perform_all(spl->ts);

   return TRUE;
}


void
splinter_force_flush(splinter_handle *spl)
{
   // FIXME [yfogel/aconway 2020-07-29] There is a race condition where both
   //       this thread and (already existing thread) are both trying to flush
   //       single memtable same time)
   //       Need to fix that race.
   page_handle *lock_page;
   uint64 generation;
   platform_status rc =
      memtable_maybe_rotate_and_get_insert_lock(spl->mt_ctxt, &generation,
            &lock_page);
   platform_assert_status_ok(rc);
   task_perform_all(spl->ts);
   memtable_unget_insert_lock(spl->mt_ctxt, lock_page);
   task_perform_all(spl->ts);
   splinter_for_each_node(spl, splinter_flush_node, NULL);
}


/*
 *-----------------------------------------------------------------------------
 *
 * splitting functions
 *
 *-----------------------------------------------------------------------------
 */

static inline bool
splinter_needs_split(splinter_handle *spl,
                     page_handle     *node)
{
   uint16 height = splinter_height(spl, node);
   if (height == 0) {
      uint64 num_tuples = splinter_get_pivot_data(spl, node, 0)->num_tuples;
      return num_tuples > spl->cfg.max_tuples_per_node
         || splinter_logical_branch_count(spl, node) > spl->cfg.max_branches_per_node;
   }
   return splinter_num_children(spl, node) > spl->cfg.fanout;
}

int
splinter_split_index(splinter_handle *spl,
                     page_handle     *parent,
                     page_handle     *child,
                     uint64           pivot_no)
{
   splinter_open_log_stream();
   splinter_log_stream("split index %lu with parent %lu\n",
         child->disk_addr, parent->disk_addr);
   splinter_log_node(spl, parent);
   splinter_log_node(spl, child);
   page_handle *left_node = child;
   uint16 target_num_children = splinter_num_children(spl, left_node) / 2;
   uint16 height = splinter_height(spl, left_node);

   if (spl->cfg.use_stats)
      spl->stats[platform_get_tid()].index_splits++;

   // allocate right node and write lock it
   page_handle *right_node = splinter_alloc(spl, height);
   uint64       right_addr = right_node->disk_addr;

   // ALEX: Maybe worth figuring out the real page size
   memmove(right_node->data, left_node->data, spl->cfg.page_size);
   char *right_start_pivot = splinter_get_pivot(spl, right_node, 0);
   char *left_split_pivot =
      splinter_get_pivot(spl, left_node, target_num_children);
   uint16 pivots_to_copy =
      splinter_num_pivot_keys(spl, left_node) - target_num_children;
   size_t bytes_to_copy = pivots_to_copy * splinter_pivot_size(spl);
   memmove(right_start_pivot, left_split_pivot, bytes_to_copy);

   uint16 start_filter = splinter_start_sb_filter(spl, left_node);
   uint16 end_filter = splinter_end_sb_filter(spl, left_node);
   for (uint16 filter_no = start_filter;
        filter_no != end_filter;
        filter_no = splinter_subbundle_no_add(spl, filter_no, 1)) {
      routing_filter *filter =
         splinter_get_sb_filter(spl, left_node, filter_no);
      splinter_inc_filter(spl, filter);
      //platform_log("inc filter %lu in %lu (%u)\n",
      //      filter->addr, right_node->disk_addr,
      //      allocator_get_refcount(spl->al, filter->addr));
   }

   // set the headers appropriately
   splinter_trunk_hdr *left_hdr  = (splinter_trunk_hdr *)left_node->data;
   splinter_trunk_hdr *right_hdr = (splinter_trunk_hdr *)right_node->data;

   right_hdr->num_pivot_keys = left_hdr->num_pivot_keys - target_num_children;
   left_hdr->num_pivot_keys  = target_num_children + 1;

   right_hdr->next_addr = left_hdr->next_addr;
   left_hdr->next_addr  = right_addr;

   left_hdr->generation++;
   splinter_reset_start_branch(spl, right_node);
   splinter_reset_start_branch(spl, left_node);

   // fix the entries in the reclamation queue
   uint16 right_num_children = splinter_num_children(spl, right_node);
   for (uint16 pivot_no = 0; pivot_no < right_num_children; pivot_no++) {
      splinter_pivot_data *pdata =
         splinter_get_pivot_data(spl, right_node, pivot_no);
      if (pdata->srq_idx != -1 && spl->cfg.reclaim_threshold != UINT64_MAX) {
         //platform_log("Deleting %12lu-%lu (index %lu) from SRQ\n",
         //      left_node->disk_addr, pdata->generation, pdata->srq_idx);
         srq_data data_to_reinsert = srq_delete(&spl->srq, pdata->srq_idx);
         data_to_reinsert.addr = right_addr;
         //platform_log("Reinserting %12lu-%lu into SRQ\n",
         //      right_addr, pdata->generation);
         pdata->srq_idx = srq_insert(&spl->srq, data_to_reinsert);
      }
   }

   // add right child to parent
   platform_status rc =
      splinter_add_pivot(spl, parent, right_node, pivot_no + 1);
   platform_assert(SUCCESS(rc));
   splinter_pivot_recount_num_tuples(spl, parent, pivot_no);
   splinter_pivot_recount_num_tuples(spl, parent, pivot_no + 1);

   splinter_log_stream("----------------------------------------\n");
   splinter_log_node(spl, parent);
   splinter_log_node(spl, left_node);
   splinter_log_node(spl, right_node);
   splinter_close_log_stream();

   splinter_node_unlock(spl, right_node);
   splinter_node_unclaim(spl, right_node);
   splinter_node_unget(spl, &right_node);

   return 0;
}

/*
 * Estimate the number of unique keys in the pivot
 */

__attribute__ ((unused))
static inline uint64
splinter_pivot_estimate_unique_keys(splinter_handle     *spl,
                                    page_handle         *node,
                                    splinter_pivot_data *pdata)
{
   routing_filter filter[MAX_FILTERS];
   uint64 filter_no = 0;
   filter[filter_no++] = pdata->filter;

   uint64 num_sb_fp = 0;
   uint64 num_sb_unique = 0;
   for (uint16 sb_filter_no = splinter_start_sb_filter(spl, node);
        sb_filter_no != splinter_end_sb_filter(spl, node);
        sb_filter_no = splinter_subbundle_no_add(spl, sb_filter_no, 1))
   {
      routing_filter *sb_filter =
         splinter_get_sb_filter(spl, node, sb_filter_no);
      num_sb_fp += sb_filter->num_fingerprints;
      num_sb_unique += sb_filter->num_unique;
      filter[filter_no++] = *sb_filter;
   }

   uint32 num_unique = routing_filter_estimate_unique_fp(spl->cc,
         &spl->cfg.leaf_filter_cfg, spl->heap_id, filter, filter_no);

   num_unique =
      routing_filter_estimate_unique_keys_from_count(&spl->cfg.leaf_filter_cfg,
            num_unique);

   uint64 num_leaf_sb_fp = 0;
   for (uint16 bundle_no = pdata->start_bundle;
        bundle_no != splinter_end_bundle(spl, node);
        bundle_no = splinter_bundle_no_add(spl, bundle_no, 1))
   {
      splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);
      num_leaf_sb_fp += bundle->num_tuples;
   }
   uint64 est_num_leaf_sb_unique = num_sb_unique * num_leaf_sb_fp / num_sb_fp;
   uint64 est_num_non_leaf_sb_unique = num_sb_fp - est_num_leaf_sb_unique;

   //platform_error_log("num_unique %u sb_fp %lu sb_unique %lu num_leaf_sb_fp %lu\n",
   //      num_unique, num_sb_fp, num_sb_unique, num_leaf_sb_fp);
   //platform_error_log("est_leaf_sb_fp %lu est_non_leaf_sb_unique %lu\n",
   //      est_num_leaf_sb_unique, est_num_non_leaf_sb_unique);
   uint64 est_leaf_unique = num_unique - est_num_non_leaf_sb_unique;
   return est_leaf_unique;
}

/*
 * split_leaf splits a trunk leaf logically. It determines pivots to split on,
 * uses them to split the leaf and adds them to its parent. It then issues
 * compact_bundle jobs on each leaf to perform the actual compaction.
 *
 * Must be called with a lock on both the parent and child
 * Returns with lock on parent and releases child and all new leaves
 * The algorithm tries to downgrade to a claim as much as possible throughout
 *
 * The main loop starts with the current leaf (initially the original leaf),
 * then uses the rough iterator to find the next pivot. It copies the current
 * leaf to a new leaf, and sets the end key of the current leaf and start key
 * of the new leaf to the pivot. It then issues a compact_bundle job on the
 * current leaf and releases it. Finally, the loop continues with the new leaf
 * as current.
 *
 * Algorithm:
 * 1. Create a rough merge iterator on all the branches
 * 2. Use rough merge iterator to determine pivots for new leaves
 * 3. Clear old bundles from leaf and put all branches in a new bundle
 * 4. Create new leaf, adjust min/max keys and other metadata
 * 5. Add new leaf to parent
 * 6. Issue compact_bundle for last_leaf and release
 * 7. Repeat 4-6 on new leaf
 * 8. Clean up
 */

void
splinter_split_leaf(splinter_handle *spl,
                    page_handle     *parent,
                    page_handle     *leaf,
                    uint16           child_idx)
{
   const threadid tid = platform_get_tid();
   // FIXME: [aconway 2020-06-19] This scratch lookup feels a bit dirty
   splinter_task_scratch *task_scratch =
      task_system_get_thread_scratch(spl->ts, tid);
   split_leaf_scratch *scratch = &task_scratch->split_leaf;
   uint64 num_branches = splinter_branch_count(spl, leaf);
   uint64 start_branch = splinter_start_branch(spl, leaf);

   splinter_node_unlock(spl, parent);
   splinter_node_unlock(spl, leaf);

   splinter_open_log_stream();
   splinter_log_stream("split_leaf addr %lu\n", leaf->disk_addr);

   uint64 split_start;
   if (spl->cfg.use_stats) {
      spl->stats[tid].leaf_splits++;
      split_start = platform_get_timestamp();
   }

   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, leaf, 0);
   uint64 estimated_unique_keys =
      splinter_pivot_estimate_unique_keys(spl, leaf, pdata);
   uint64 num_tuples = splinter_pivot_num_tuples(spl, leaf, 0);
   if (estimated_unique_keys > num_tuples * 19 / 20) {
      estimated_unique_keys = num_tuples;
   }
   uint64 target_num_leaves = estimated_unique_keys / spl->cfg.target_leaf_tuples;
   if (1 && target_num_leaves == 1
         && estimated_unique_keys > 9 * spl->cfg.max_tuples_per_node / 10)
   {
      target_num_leaves = 2;
   }
   uint64 target_leaf_tuples = num_tuples / target_num_leaves;
   uint64 target_num_pivots =
      (target_leaf_tuples - 1) / spl->cfg.btree_cfg.tuples_per_leaf + 1;
   uint16 num_leaves;

   // copy pivot (in parent) of leaf
   memmove(scratch->pivot[0],
           splinter_min_key(spl, leaf),
           splinter_key_size(spl));

   if (target_num_leaves != 1) {
      // FIXME: [yfogel 2020-07-22] This entire function needs to support
      //    range_delete trees and does not yet.
      /*
       * 1. Create a rough merge iterator on all the branches
       *
       *    A rough merge iterator is a merge iterator on height 1 btree
       *    iterators. It uses height 1 pivots as a proxy for a count of tuples.
       *
       *    This count is an estimate with multiple sources of error:
       *       -- Last leaves in each btree are not counted (there is no upper
       *          bound pivot)
       *       -- A selected pivot from a branch may be between pivots for other
       *          branches
       *       -- min_key may be between pivots
       *       -- updates and deletes may be resolved resulting in fewer output
       *          tuples
       */

      platform_assert(num_branches <= ARRAY_SIZE(scratch->btree_itor));
      btree_iterator  *rough_btree_itor = scratch->btree_itor;
      iterator       **rough_itor = scratch->rough_itor;
      char            min_key[MAX_KEY_SIZE];
      char            max_key[MAX_KEY_SIZE];
      memmove(min_key, splinter_get_pivot(spl, leaf, 0), splinter_key_size(spl));
      memmove(max_key, splinter_get_pivot(spl, leaf, 1), splinter_key_size(spl));

      for (uint64 branch_offset = 0;
           branch_offset < num_branches;
           branch_offset++) {
         uint64 branch_no =
            splinter_branch_no_add(spl, start_branch, branch_offset);
         debug_assert(branch_no != splinter_end_branch(spl, leaf));
         splinter_branch *branch = splinter_get_branch(spl, leaf, branch_no);
         // FIXME: [yfogel 2020-07-22] need to also do the range_delete tree here
         //        pivots are already saved (see above)
         //        may need to increase size of scratch to 2x so room
         //        for range delete trees
         btree_iterator_init(
               spl->cc, &spl->cfg.btree_cfg,
               &rough_btree_itor[branch_offset], branch->root_addr,
               PAGE_TYPE_BRANCH, min_key, max_key, TRUE, FALSE, 1,
               data_type_point);
         rough_itor[branch_offset] = &rough_btree_itor[branch_offset].super;
      }

      merge_iterator *rough_merge_itor;
      platform_status rc =
         merge_iterator_create(spl->heap_id, spl->cfg.data_cfg,
                               &spl->cfg.range_data_cfg, num_branches,
                               rough_itor, FALSE, FALSE, FALSE,
                               &rough_merge_itor);
      platform_assert_status_ok(rc);

      /*
       * 2. Use rough merge iterator to determine pivots for new leaves
       */
      bool at_end;
      rc = iterator_at_end(&rough_merge_itor->super, &at_end);
      platform_assert_status_ok(rc);

      uint64 rough_count_pivots;
      for (num_leaves = 0; !at_end; num_leaves++) {
         rough_count_pivots = 0;
         while (1 && !at_end
                  && (0 || rough_count_pivots < target_num_pivots
                        || num_leaves == target_num_leaves - 1)) {
            iterator_advance(&rough_merge_itor->super);
            iterator_at_end(&rough_merge_itor->super, &at_end);
            rough_count_pivots++;
         }

         if (!at_end) {
            char *curr_key, *dummy_data;
            data_type dummy_type;
            iterator_get_curr(&rough_merge_itor->super, &curr_key,
                              &dummy_data, &dummy_type);
            // copy new pivot (in parent) of new leaf
            memmove(scratch->pivot[num_leaves + 1],
                    curr_key,
                    splinter_key_size(spl));
         }
      }

      // clean up the iterators
      rc = merge_iterator_destroy(spl->heap_id, &rough_merge_itor);
      platform_assert_status_ok(rc);
      for (uint64 i = 0; i < num_branches; i++) {
         // FIXME: [yfogel 2020-07-22] may need to modify for range_delete trees.
         // FIXME: [yfogel 2020-07-22] need to change initialization of this branch
         //    cause need both point and range initialized
         btree_iterator_deinit(&rough_btree_itor[i]);
      }
   } else {
      num_leaves = 1;
   }

   // copy max key of last new leaf (max key of leaf)
   memmove(scratch->pivot[num_leaves],
           splinter_max_key(spl, leaf),
           splinter_key_size(spl));

   /*
    * FIXME: [aconway 2020-07-16] This case can only happen with default params
    * with a height 8+ tree and worst case flush. We do not currently handle
    * this case.
    */
   platform_assert(num_leaves + splinter_num_pivot_keys(spl, parent)
         <= spl->cfg.max_pivot_keys);

   /*
    * 3. Clear old bundles from leaf and put all branches in a new bundle
    */
   splinter_node_lock(spl, parent);
   splinter_log_node(spl, parent);
   splinter_node_lock(spl, leaf);
   splinter_log_node(spl, leaf);

   uint16 bundle_no =
      splinter_leaf_rebundle_all_branches(spl, leaf, target_leaf_tuples, FALSE);
   splinter_inc_generation(spl, leaf);
   uint64 last_next_addr = splinter_next_addr(spl, leaf);

   for (uint16 leaf_no = 0; leaf_no < num_leaves; leaf_no++) {
      /*
       * 4. Create new leaf, adjust min/max keys and other metadata
       *
       *    Have lock on leaf (original leaf or last iteration) and parent
       *    This loop :
       *    1. allocates new_leaf
       *    2. copies leaf to new_leaf
       *    3. sets min_key and max_key on new_leaf
       *    4. sets next_addr on leaf
       *    5. incs all branches ref counts
       *    6. sets new_leaf tuple_count
       *    7. adds new_leaf to parent
       */

      page_handle *new_leaf;
      if (leaf_no != 0) {
         // allocate a new leaf
         new_leaf = splinter_alloc(spl, 0);

         // copy leaf to new leaf
         memmove(new_leaf->data, leaf->data, spl->cfg.page_size);
      } else {
         // just going to edit the min/max keys, etc. of original leaf
         new_leaf = leaf;
      }

      // adjust min key
      memmove(splinter_get_pivot(spl, new_leaf, 0),
              scratch->pivot[leaf_no],
              splinter_key_size(spl));
      // adjust max key
      memmove(splinter_get_pivot(spl, new_leaf, 1),
              scratch->pivot[leaf_no + 1],
              splinter_key_size(spl));

      // set new_leaf tuple_count
      splinter_bundle *bundle = splinter_get_bundle(spl, new_leaf, bundle_no);
      uint64 new_leaf_num_tuples[SPLINTER_MAX_PIVOTS];
      splinter_tuples_in_bundle(spl, new_leaf, bundle, new_leaf_num_tuples);
      splinter_pivot_set_num_tuples(spl, new_leaf, 0, new_leaf_num_tuples[0]);

      if (leaf_no != 0) {
         // set next_addr of leaf
         splinter_set_next_addr(spl, leaf, new_leaf->disk_addr);

         // inc the refs of all the branches
         for (uint16 branch_no = splinter_start_branch(spl, new_leaf);
              branch_no != splinter_end_branch(spl, new_leaf);
              branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
            splinter_branch *branch =
               splinter_get_branch(spl, new_leaf, branch_no);
            const char *min_key = splinter_min_key(spl, new_leaf);
            splinter_inc_intersection(spl, branch, min_key, FALSE);
         }

         // inc the refs of all the filters
         splinter_bundle *bundle =
            splinter_get_bundle(spl, new_leaf, bundle_no);
         uint16 start_filter =
            splinter_bundle_start_filter(spl, new_leaf, bundle);
         uint16 end_filter =
            splinter_bundle_end_filter(spl, new_leaf, bundle);
         for (uint16 filter_no = start_filter;
              filter_no != end_filter;
              filter_no = splinter_subbundle_no_add(spl, filter_no, 1)) {
            routing_filter *filter =
               splinter_get_sb_filter(spl, new_leaf, filter_no);
            splinter_inc_filter(spl, filter);
            //splinter_log_stream("inc filter %lu in %lu (%u)\n",
            //      filter->addr, new_leaf->disk_addr,
            //      allocator_get_refcount(spl->al, filter->addr));
         }

         /*
          * 5. Add new leaf to parent
          */
         platform_status rc =
            splinter_add_pivot(spl, parent, new_leaf, child_idx + leaf_no);
         platform_assert(SUCCESS(rc));

         /*
          * 6. Issue compact_bundle for leaf and release
          */
         splinter_compact_bundle_req *req = TYPED_ZALLOC(spl->heap_id, req);
         req->spl = spl;
         req->addr = leaf->disk_addr;
         // req->height already 0
         req->bundle_no = bundle_no;
         req->generation = splinter_generation(spl, leaf);
         req->max_pivot_generation = splinter_pivot_generation(spl, leaf);
         req->pivot_generation[0] = splinter_pivot_generation(spl, leaf) - 1;
         req->input_pivot_count[0] = splinter_pivot_num_tuples(spl, leaf, 0);

         splinter_default_log("enqueuing compact_bundle %lu-%u\n",
               req->addr, req->bundle_no);
         rc = task_enqueue(spl->ts, TASK_TYPE_NORMAL, splinter_compact_bundle,
                           req, FALSE);
         platform_assert(SUCCESS(rc));

         splinter_log_node(spl, leaf);

         debug_assert(splinter_verify_node(spl, leaf));
         splinter_node_unlock(spl, leaf);
         splinter_node_unclaim(spl, leaf);
         splinter_node_unget(spl, &leaf);
      }

      leaf = new_leaf;
   }

   // set next_addr of leaf (from last iteration)
   splinter_set_next_addr(spl, leaf, last_next_addr);
   splinter_compact_bundle_req *req = TYPED_ZALLOC(spl->heap_id, req);
   req->spl = spl;
   req->addr = leaf->disk_addr;
   // req->height already 0
   req->bundle_no = bundle_no;
   req->generation = splinter_generation(spl, leaf);
   req->max_pivot_generation = splinter_pivot_generation(spl, leaf);
   req->pivot_generation[0] = splinter_pivot_generation(spl, leaf) - 1;
   req->input_pivot_count[0] = splinter_pivot_num_tuples(spl, leaf, 0);

   // issue compact_bundle for leaf and release
   splinter_default_log("enqueuing compact_bundle %lu-%u\n",
                        req->addr, req->bundle_no);
   platform_status rc =
      task_enqueue(spl->ts, TASK_TYPE_NORMAL, splinter_compact_bundle, req,
                   FALSE);
   platform_assert(SUCCESS(rc));

   splinter_log_node(spl, leaf);

   debug_assert(splinter_verify_node(spl, leaf));
   splinter_node_unlock(spl, leaf);
   splinter_node_unclaim(spl, leaf);
   splinter_node_unget(spl, &leaf);

   /*
    * 8. Clean up
    */
   splinter_close_log_stream();

   if (spl->cfg.use_stats) {
      // Doesn't include the original leaf
      spl->stats[tid].leaf_splits_leaves_created += num_leaves - 1;
      uint64 split_time = platform_timestamp_elapsed(split_start);
      spl->stats[tid].leaf_split_time_ns += split_time;
         platform_timestamp_elapsed(split_start);
      if (split_time > spl->stats[tid].leaf_split_max_time_ns) {
         spl->stats[tid].leaf_split_max_time_ns = split_time;
      }
   }
}


int
splinter_split_root(splinter_handle *spl,
                    page_handle     *root)
{
   splinter_trunk_hdr *root_hdr = (splinter_trunk_hdr *)root->data;

   // allocate a new child node
   page_handle *       child     = splinter_alloc(spl, root_hdr->height);
   splinter_trunk_hdr *child_hdr = (splinter_trunk_hdr *)child->data;

   // copy root to child, fix up root, then split
   memmove(child_hdr, root_hdr, spl->cfg.page_size);
   // num_pivot_keys is changed by add_pivot_new_root below
   root_hdr->height++;
   debug_assert(root_hdr->next_addr == 0);
   // leave generation and pivot_generation
   root_hdr->start_branch      = 0;
   root_hdr->start_frac_branch = 0;
   root_hdr->end_branch        = 0;
   root_hdr->start_bundle      = 0;
   root_hdr->end_bundle        = 0;
   root_hdr->start_subbundle   = 0;
   root_hdr->end_subbundle     = 0;
   root_hdr->start_sb_filter   = 0;
   root_hdr->end_sb_filter     = 0;

   splinter_add_pivot_new_root(spl, root, child);

   splinter_split_index(spl, root, child, 0);

   splinter_node_unlock(spl, child);
   splinter_node_unclaim(spl, child);
   splinter_node_unget(spl, &child);

   return 0;
}


/*
 *-----------------------------------------------------------------------------
 *
 * range functions and iterators
 *
 *      splinter_node_iterator
 *      splinter_iterator
 *
 *-----------------------------------------------------------------------------
 */

void             splinter_range_iterator_get_curr (iterator *itor, char **key, char **data, data_type *type);
platform_status  splinter_range_iterator_at_end   (iterator *itor, bool *at_end);
platform_status  splinter_range_iterator_advance  (iterator *itor);
void             splinter_range_iterator_deinit   (splinter_range_iterator *range_itor);

const static iterator_ops splinter_range_iterator_ops = {
   .get_curr = splinter_range_iterator_get_curr,
   .at_end   = splinter_range_iterator_at_end,
   .advance  = splinter_range_iterator_advance,
};

platform_status
splinter_range_iterator_init(splinter_handle         *spl,
                             splinter_range_iterator *range_itor,
                             char                    *min_key,
                             char                    *max_key,
                             uint64                   num_tuples)
{
   range_itor->spl = spl;
   range_itor->super.ops = &splinter_range_iterator_ops;
   range_itor->num_branches = 0;
   range_itor->num_tuples = num_tuples;
   if (min_key == NULL) {
     min_key = spl->cfg.data_cfg->min_key;
   }
   memmove(range_itor->min_key, min_key, splinter_key_size(spl));
   if (max_key) {
      range_itor->has_max_key = TRUE;
      memmove(range_itor->max_key, max_key, splinter_key_size(spl));
   } else {
      range_itor->has_max_key = FALSE;
      memset(range_itor->max_key, 0, splinter_key_size(spl));
   }

   const char *hard_max_key = max_key ? max_key : spl->cfg.data_cfg->max_key;
   if (splinter_key_compare(spl, min_key, hard_max_key) == 0) {
      range_itor->at_end = TRUE;
      return STATUS_OK;
   }

   if (max_key && splinter_key_compare(spl, max_key, min_key) <= 0) {
      range_itor->at_end = TRUE;
      return STATUS_OK;
   }

   range_itor->at_end = FALSE;

   ZERO_ARRAY(range_itor->compacted);
   ZERO_ARRAY(range_itor->meta_page);

   // grab the lookup lock
   page_handle *mt_lookup_lock_page = memtable_get_lookup_lock(spl->mt_ctxt);

   // memtables
   ZERO_ARRAY(range_itor->branch);
   // Note this iteration is in descending generation order
   range_itor->memtable_start_gen = memtable_generation(spl->mt_ctxt);
   range_itor->memtable_end_gen = memtable_generation_retired(spl->mt_ctxt);
   range_itor->num_memtable_branches =
      range_itor->memtable_start_gen - range_itor->memtable_end_gen;
   for (uint64 mt_gen = range_itor->memtable_start_gen;
        mt_gen != range_itor->memtable_end_gen;
        mt_gen--) {
      platform_assert(range_itor->num_branches < SPLINTER_MAX_TOTAL_DEGREE);
      debug_assert(range_itor->num_branches < ARRAY_SIZE(range_itor->branch));

      bool compacted;
      uint64 root_addr =
         splinter_memtable_root_addr_for_lookup(spl, mt_gen, &compacted);
      range_itor->compacted[range_itor->num_branches] = compacted;
      page_type type = compacted ?  PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;
      if (compacted) {
         range_itor->meta_page[range_itor->num_branches] =
            btree_blind_inc(spl->cc, &spl->cfg.btree_cfg, root_addr, type);
      } else {
         splinter_memtable_inc_ref(spl, mt_gen);
      }

      range_itor->branch[range_itor->num_branches].root_addr = root_addr;
      // FIXME: [yfogel 2020-07-06] need to init range root addr
      range_itor->branch[range_itor->num_branches].range_root_addr = 0;

      range_itor->num_branches++;
   }

   page_handle *node = splinter_node_get(spl, spl->root_addr);
   memtable_unget_lookup_lock(spl->mt_ctxt, mt_lookup_lock_page);

   // index btrees
   uint16 height = splinter_height(spl, node);
   for (uint16 h = height; h > 0; h--) {
      uint16 pivot_no = splinter_find_pivot(spl, node, range_itor->min_key,
            less_than_or_equal);
      debug_assert(pivot_no < splinter_num_children(spl, node));
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);

      for (uint16 branch_offset = 0; branch_offset !=
           splinter_pivot_branch_count(spl, node, pdata);
           branch_offset++) {
         platform_assert(range_itor->num_branches < SPLINTER_MAX_TOTAL_DEGREE);
         debug_assert(range_itor->num_branches < ARRAY_SIZE(range_itor->branch));
         uint16 branch_no = splinter_branch_no_sub(spl,
               splinter_end_branch(spl, node), branch_offset + 1);
         range_itor->branch[range_itor->num_branches] =
            *splinter_get_branch(spl, node, branch_no);
         range_itor->compacted[range_itor->num_branches] = TRUE;
         uint64 root_addr = range_itor->branch[range_itor->num_branches].root_addr;

         range_itor->meta_page[range_itor->num_branches] =
            btree_blind_inc(spl->cc, &spl->cfg.btree_cfg, root_addr, PAGE_TYPE_BRANCH);
         range_itor->num_branches++;
      }

      page_handle *child = splinter_node_get(spl, pdata->addr);
      splinter_node_unget(spl, &node);
      node = child;
   }

   // leaf btrees
   for (uint16 branch_offset = 0;
        branch_offset != splinter_branch_count(spl, node);
        branch_offset++) {
      uint16 branch_no = splinter_branch_no_sub(spl,
            splinter_end_branch(spl, node), branch_offset + 1);
      range_itor->branch[range_itor->num_branches] =
         *splinter_get_branch(spl, node, branch_no);
      uint64 root_addr = range_itor->branch[range_itor->num_branches].root_addr;
      range_itor->meta_page[range_itor->num_branches] =
         btree_blind_inc(spl->cc, &spl->cfg.btree_cfg, root_addr, PAGE_TYPE_BRANCH);
      range_itor->compacted[range_itor->num_branches] = TRUE;
      range_itor->num_branches++;
   }

   // have a leaf, use to get rebuild key
   char *rebuild_key =
     !range_itor->has_max_key
     || splinter_key_compare(spl, splinter_max_key(spl, node), max_key) < 0
     ? splinter_max_key(spl, node)
     : max_key;
   memmove(range_itor->rebuild_key, rebuild_key, splinter_key_size(spl));
   if (max_key && splinter_key_compare(spl, max_key, rebuild_key) < 0) {
     memcpy(range_itor->local_max_key, max_key, splinter_key_size(spl));
   } else {
     memcpy(range_itor->local_max_key, rebuild_key, splinter_key_size(spl));
   }

   splinter_node_unget(spl, &node);

   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      uint64 branch_no = range_itor->num_branches - i - 1;
      btree_iterator *btree_itor = &range_itor->btree_itor[branch_no];
      splinter_branch *branch = &range_itor->branch[branch_no];
      if (range_itor->compacted[branch_no]) {
         bool do_prefetch = range_itor->compacted[branch_no] &&
            num_tuples > SPLINTER_PREFETCH_MIN ? TRUE : FALSE;
         splinter_branch_iterator_init(spl, btree_itor, branch,
               range_itor->min_key, range_itor->local_max_key, do_prefetch,
               data_type_point, FALSE);
      } else {
         uint64 mt_root_addr = branch->root_addr;
         bool is_live = branch_no == 0;
         splinter_memtable_iterator_init(spl, btree_itor, mt_root_addr,
               range_itor->min_key, range_itor->local_max_key, is_live, FALSE);
      }
      range_itor->itor[i] = &btree_itor->super;
   }

   platform_status rc = merge_iterator_create(spl->heap_id,
                                              spl->cfg.data_cfg,
                                              &spl->cfg.range_data_cfg,
                                              range_itor->num_branches,
                                              range_itor->itor, TRUE, TRUE,
                                              TRUE, &range_itor->merge_itor);
   if (!SUCCESS(rc)) {
      return rc;
   }

   bool at_end;
   iterator_at_end(&range_itor->merge_itor->super, &at_end);

   /*
    * if the merge itor is already exhausted, and there are more keys in the
    * db/range, move to next leaf
    */
   if (at_end) {
      splinter_range_iterator_deinit(range_itor);
      if (1 && splinter_key_compare(spl, range_itor->local_max_key,
               spl->cfg.data_cfg->max_key) != 0
            && (0 || !range_itor->has_max_key
                  || splinter_key_compare(spl, range_itor->local_max_key,
                     range_itor->max_key) < 0)) {
         rc = splinter_range_iterator_init(spl, range_itor,
               range_itor->rebuild_key, max_key, range_itor->num_tuples);
         if (!SUCCESS(rc)) {
            return rc;
         }
         iterator_at_end(&range_itor->merge_itor->super, &at_end);
      }
   }

   range_itor->at_end = at_end;

   return rc;
}

void
splinter_range_iterator_get_curr(iterator   *itor,
                                 char      **key,
                                 char      **data,
                                 data_type  *type)
{
   debug_assert(itor != NULL);
   splinter_range_iterator *range_itor = (splinter_range_iterator *)itor;
   iterator_get_curr(&range_itor->merge_itor->super, key, data, type);
}

platform_status
splinter_range_iterator_advance(iterator *itor)
{
   debug_assert(itor != NULL);
   splinter_range_iterator *range_itor = (splinter_range_iterator *)itor;
   iterator_advance(&range_itor->merge_itor->super);
   range_itor->num_tuples++;
   bool at_end;
   iterator_at_end(&range_itor->merge_itor->super, &at_end);
   platform_status rc;
   // robj: shouldn't this be a while loop, like in the init function?
   if (at_end) {
      splinter_range_iterator_deinit(range_itor);
      if (range_itor->has_max_key) {
         rc = splinter_range_iterator_init(range_itor->spl, range_itor,
               range_itor->rebuild_key, range_itor->max_key, range_itor->num_tuples);
      } else {
         rc = splinter_range_iterator_init(range_itor->spl, range_itor,
               range_itor->rebuild_key, NULL, range_itor->num_tuples);
      }
      if (!SUCCESS(rc)) {
         return rc;
      }
      if (!range_itor->at_end) {
        iterator_at_end(&range_itor->merge_itor->super, &at_end);
        platform_assert(!at_end);
      }
   }

   return STATUS_OK;
}

platform_status
splinter_range_iterator_at_end(iterator *itor,
                               bool     *at_end)
{
   debug_assert(itor != NULL);
   splinter_range_iterator *range_itor = (splinter_range_iterator *)itor;

   *at_end = range_itor->at_end;
   return STATUS_OK;
}

void
splinter_range_iterator_deinit(splinter_range_iterator *range_itor)
{
   // If the iterator is at end, then it has already been deinitialized
   if (range_itor->at_end) {
      return;
   }
   splinter_handle *spl = range_itor->spl;
   merge_iterator_destroy(range_itor->spl->heap_id, &range_itor->merge_itor);
   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      btree_iterator *btree_itor = &range_itor->btree_itor[i];
      if (range_itor->compacted[i]) {
         splinter_branch_iterator_deinit(spl, btree_itor, FALSE);
         page_handle *meta_page = range_itor->meta_page[i];
         btree_blind_zap(spl->cc, &spl->cfg.btree_cfg, meta_page, PAGE_TYPE_BRANCH);
      } else {
         uint64 mt_gen = range_itor->memtable_start_gen - i;
         splinter_memtable_iterator_deinit(spl, btree_itor, mt_gen, FALSE);
         splinter_memtable_dec_ref(spl, mt_gen);
      }
   }
}

/*
 * Given a node addr and pivot generation, find the pivot with that generation
 * among the node and its split descendents
 *
 * Returns node with a write loc
 */
splinter_pivot_data *
splinter_find_pivot_from_generation(splinter_handle *spl,
                                    page_handle     *leaf,
                                    uint64           pivot_generation)
{
   uint16 num_children = splinter_num_children(spl, leaf);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata =
         splinter_get_pivot_data(spl, leaf, pivot_no);
      if (pivot_generation == pdata->generation) {
         return pdata;
      }
   }
   return NULL;
}

platform_status
splinter_compact_leaf(splinter_handle *spl,
                      page_handle     *leaf)
{
   const threadid tid = platform_get_tid();
   // FIXME: [aconway 2020-06-19] This scratch lookup feels a bit dirty

   splinter_open_log_stream();
   splinter_log_stream("compact_leaf addr %lu\n", leaf->disk_addr);
   splinter_log_node(spl, leaf);

   uint64 sr_start;
   if (spl->cfg.use_stats) {
      spl->stats[tid].space_recs[0]++;
      sr_start = platform_get_timestamp();
   }

   // Clear old bundles from leaf and put all branches in a new bundle
   uint64 num_tuples = splinter_pivot_num_tuples(spl, leaf, 0);
   uint16 bundle_no =
      splinter_leaf_rebundle_all_branches(spl, leaf, num_tuples, TRUE);
   splinter_inc_generation(spl, leaf);

   // Issue compact_bundle for leaf and release
   splinter_compact_bundle_req *req = TYPED_ZALLOC(spl->heap_id, req);
   req->spl = spl;
   req->addr = leaf->disk_addr;
   // req->height already 0
   req->bundle_no = bundle_no;
   req->generation = splinter_generation(spl, leaf);
   req->max_pivot_generation = splinter_pivot_generation(spl, leaf);
   req->pivot_generation[0] = splinter_pivot_generation(spl, leaf) - 1;
   req->input_pivot_count[0] = splinter_pivot_num_tuples(spl, leaf, 0);
   req->is_space_rec = TRUE;

   splinter_default_log("enqueuing compact_bundle %lu-%u\n",
         req->addr, req->bundle_no);
   platform_status rc = task_enqueue(spl->ts, TASK_TYPE_NORMAL,
         splinter_compact_bundle, req, FALSE);
   platform_assert(SUCCESS(rc));

   splinter_log_node(spl, leaf);

   debug_assert(splinter_verify_node(spl, leaf));

   /*
    * 8. Clean up
    */
   splinter_close_log_stream();

   if (spl->cfg.use_stats) {
      // Doesn't include the original leaf
      uint64 sr_time = platform_timestamp_elapsed(sr_start);
      spl->stats[tid].space_rec_time_ns[0] += sr_time;
   }

   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 *
 * space reclamation
 *
 *-----------------------------------------------------------------------------
 */

bool
splinter_should_reclaim_space(splinter_handle *spl)
{
   if (spl->cfg.reclaim_threshold == UINT64_MAX) {
      return FALSE;
   }
   if (spl->cfg.reclaim_threshold == 0) {
      return TRUE;
   }
   uint64 in_use = allocator_in_use(spl->al);
   bool should_reclaim = in_use > spl->cfg.reclaim_threshold;
   return should_reclaim;
}

platform_status
splinter_reclaim_space(splinter_handle *spl)
{
   platform_assert(spl->cfg.reclaim_threshold != UINT64_MAX);
   while (TRUE) {
      //platform_log("Extract from SRQ\n");
      srq_data space_rec = srq_extract_max(&spl->srq);
      if (!srq_data_found(&space_rec)) {
         return STATUS_NOT_FOUND;
      }
      page_handle *node = splinter_node_get(spl, space_rec.addr);
      splinter_node_claim(spl, &node);
      splinter_pivot_data *pdata = splinter_find_pivot_from_generation(spl,
            node, space_rec.pivot_generation);
      if (pdata == NULL) {
         splinter_node_unclaim(spl, node);
         splinter_node_unget(spl, &node);
         continue;
      }
      pdata->srq_idx = -1;

      //platform_log("Space rec: %lu-%u\n",
      //      node->disk_addr, splinter_pdata_to_pivot_index(spl, node, pdata));

      splinter_node_lock(spl, node);
      if (splinter_is_leaf(spl, node)) {
         splinter_compact_leaf(spl, node);
      } else {
         uint64 sr_start;
         if (spl->cfg.use_stats) {
            sr_start = platform_get_timestamp();
         }
         bool flush_succeeded = splinter_flush(spl, node, pdata, TRUE);
         if (spl->cfg.use_stats) {
            const threadid tid = platform_get_tid();
            uint16 height = splinter_height(spl, node);
            spl->stats[tid].space_recs[height]++;
            spl->stats[tid].space_rec_time_ns[height] +=
               platform_timestamp_elapsed(sr_start);
         }
         if (!flush_succeeded) {
            splinter_node_unlock(spl, node);
            splinter_node_unclaim(spl, node);
            splinter_node_unget(spl, &node);
            continue;
         }
      }
      splinter_node_unlock(spl, node);
      splinter_node_unclaim(spl, node);
      splinter_node_unget(spl, &node);
      return STATUS_OK;
   }
}

void
splinter_maybe_reclaim_space(splinter_handle *spl)
{
   while (splinter_should_reclaim_space(spl)) {
      platform_status rc = splinter_reclaim_space(spl);
      if (STATUS_IS_EQ(rc, STATUS_NOT_FOUND)) {
         break;
      }
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * main API functions
 *
 *      insert
 *      lookup
 *      range
 *
 *-----------------------------------------------------------------------------
 */

platform_status
splinter_insert(splinter_handle *spl,
                char            *key,
                char            *data)
{
   timestamp ts;
   __attribute ((unused)) const threadid tid = platform_get_tid();
   data_config *data_cfg;
   if (spl->cfg.use_stats) {
      ts = platform_get_timestamp();
      data_cfg = spl->cfg.data_cfg;
   }

   platform_status rc = splinter_memtable_insert(spl, key, data);
   if (!SUCCESS(rc)) {
      goto out;
   }

   if (!task_system_use_bg_threads(spl->ts)) {
      task_perform_one(spl->ts);
   }

   if (spl->cfg.use_stats) {
      switch(data_message_class(data_cfg, data)) {
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

bool
splinter_filter_lookup(splinter_handle *spl,
                       page_handle     *node,
                       routing_filter  *filter,
                       routing_config  *cfg,
                       uint16           start_branch,
                       const char      *key,
                       char            *data,
                       bool            *found)
{
   uint16 height;
   threadid tid;
   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
      height = splinter_height(spl, node);
   }

   uint64 found_values;
   platform_status rc =
      routing_filter_lookup(spl->cc, cfg, filter, key, &found_values);
   platform_assert_status_ok(rc);
   if (spl->cfg.use_stats) {
      spl->stats[tid].filter_lookups[height]++;
   }
   uint16 next_value =
      routing_filter_get_next_value(found_values, ROUTING_NOT_FOUND);
   while (next_value != ROUTING_NOT_FOUND) {
      uint16 branch_no =
         splinter_branch_no_add(spl, start_branch, next_value);
      splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
      bool local_found = splinter_btree_lookup(spl, branch, key, data, found);
      if (spl->cfg.use_stats) {
         spl->stats[tid].branch_lookups[height]++;
      }
      if (local_found) {
         if (data_message_class(spl->cfg.data_cfg, data) != MESSAGE_TYPE_UPDATE) {
            return FALSE;
         }
      } else if(spl->cfg.use_stats) {
         spl->stats[tid].filter_false_positives[height]++;
      }
      next_value = routing_filter_get_next_value(found_values, next_value);
   }
   return TRUE;
}

bool
splinter_compacted_subbundle_lookup(splinter_handle    *spl,
                                    page_handle        *node,
                                    splinter_subbundle *sb,
                                    const char         *key,
                                    char               *data,
                                    bool               *found)
{
   debug_assert(sb->state == SB_STATE_COMPACTED);
   debug_assert(splinter_subbundle_branch_count(spl, node, sb) == 1);
   uint16 height;
   threadid tid;
   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
      height = splinter_height(spl, node);
   }

   uint16 filter_count = splinter_subbundle_filter_count(spl, node, sb);
   for (uint16 filter_no = 0; filter_no != filter_count; filter_no++) {
      if (spl->cfg.use_stats) {
         spl->stats[tid].filter_lookups[height]++;
      }
      uint64 found_values;
      routing_filter *filter =
         splinter_subbundle_filter(spl, node, sb, filter_no);
      debug_assert(filter->addr != 0);
      // FIXME: [aconway 2020-09-14] was index
      platform_status rc = routing_filter_lookup(spl->cc,
            &spl->cfg.leaf_filter_cfg, filter, key, &found_values);
      platform_assert_status_ok(rc);
      if (found_values) {
         uint16 branch_no = sb->start_branch;
         splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
         bool local_found =
            splinter_btree_lookup(spl, branch, key, data, found);
         if (spl->cfg.use_stats) {
            spl->stats[tid].branch_lookups[height]++;
         }
         if (local_found) {
            if (data_message_class(spl->cfg.data_cfg, data) != MESSAGE_TYPE_UPDATE) {
               return FALSE;
            }
         } else if(spl->cfg.use_stats) {
            spl->stats[tid].filter_false_positives[height]++;
         }
         return TRUE;
      }
   }
   return TRUE;
}

bool
splinter_bundle_lookup(splinter_handle *spl,
                       page_handle     *node,
                       splinter_bundle *bundle,
                       char            *key,
                       char            *data,
                       bool            *found)
{
   uint16 sb_count = splinter_bundle_subbundle_count(spl, node, bundle);
   for (uint16 sb_off = 0; sb_off != sb_count; sb_off++) {
      uint16 sb_no =
         splinter_subbundle_no_sub(spl, bundle->end_subbundle, sb_off + 1);
      splinter_subbundle *sb = splinter_get_subbundle(spl, node, sb_no);
      bool should_continue;
      if (sb->state == SB_STATE_COMPACTED) {
         should_continue = splinter_compacted_subbundle_lookup(spl, node, sb,
               key, data, found);
      } else {
         routing_filter *filter = splinter_subbundle_filter(spl, node, sb, 0);
         routing_config *cfg = &spl->cfg.leaf_filter_cfg;
         //routing_config *cfg = sb->state == SB_STATE_UNCOMPACTED_LEAF ?
         //   &spl->cfg.leaf_filter_cfg : &spl->cfg.index_filter_cfg;
         debug_assert(filter->addr != 0);
         should_continue = splinter_filter_lookup(spl, node, filter, cfg,
               sb->start_branch, key, data, found);
      }
      if (!should_continue) {
         return should_continue;
      }
   }
   return TRUE;
}

bool
splinter_pivot_lookup(splinter_handle     *spl,
                      page_handle         *node,
                      splinter_pivot_data *pdata,
                      char                *key,
                      char                *data,
                      bool                *found)
{
   // first check in bundles
   uint16 num_bundles = splinter_pivot_bundle_count(spl, node, pdata);
   for (uint16 bundle_off = 0; bundle_off != num_bundles; bundle_off++) {
      uint16 bundle_no = splinter_bundle_no_sub(spl,
            splinter_end_bundle(spl, node), bundle_off + 1);
      debug_assert(splinter_bundle_live(spl, node, bundle_no));
      splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);
      bool should_continue =
         splinter_bundle_lookup(spl, node, bundle, key, data, found);
      if (!should_continue) {
         return should_continue;
      }
   }

   routing_config *cfg = &spl->cfg.leaf_filter_cfg;
   //routing_config *cfg = splinter_height(spl, node) == 0 ?
   //                      &spl->cfg.leaf_filter_cfg : &spl->cfg.index_filter_cfg;
   return splinter_filter_lookup(spl, node, &pdata->filter, cfg,
         pdata->start_branch, key, data, found);
}

// If any change is made in here, please make similar change in splinter_lookup_async
platform_status
splinter_lookup(splinter_handle *spl,
                char            *key,
                char            *data,
                bool            *found)
{
   data_config *data_cfg = spl->cfg.data_cfg;
   message_type type;

   *found = FALSE;

   // look in memtables

   // 1. get read lock on lookup lock
   //     --- 2. for [mt_no = mt->generation..mt->gen_to_incorp]
   // 2. for gen = mt->generation; mt[gen % ...].gen == gen; gen --;
   //                also handles switch to READY ^^^^^

   bool found_in_memtable = FALSE;
   page_handle *mt_lookup_lock_page = memtable_get_lookup_lock(spl->mt_ctxt);
   uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
   uint64 mt_gen_end = memtable_generation_retired(spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      // FIXME: [aconway 2020-08-26] wrap with a branch lookup
      splinter_memtable_lookup(spl, mt_gen, key, data, found);
      if (*found) {
         type = data_message_class(data_cfg, data);
         if (type != MESSAGE_TYPE_UPDATE) {
            found_in_memtable = TRUE;
            goto found_final_answer_early;
         }
      }
   }

   // hold root read lock to prevent memtable flush
   page_handle *node = splinter_node_get(spl, spl->root_addr);

   // release memtable lookup lock
   memtable_unget_lookup_lock(spl->mt_ctxt, mt_lookup_lock_page);

   // look in index nodes
   uint16 height = splinter_height(spl, node);
   for (uint16 h = height; h > 0; h--) {
      uint16 pivot_no = splinter_find_pivot(spl, node, key, less_than_or_equal);
      debug_assert(pivot_no < splinter_num_children(spl, node));
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      bool should_continue =
         splinter_pivot_lookup(spl, node, pdata, key, data, found);
      if (!should_continue) {
         goto found_final_answer_early;
      }
      page_handle *child = splinter_node_get(spl, pdata->addr);
      splinter_node_unget(spl, &node);
      node = child;
   }

   // look in leaf
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
   bool should_continue = splinter_pivot_lookup(spl, node, pdata, key, data, found);
   if (!should_continue) {
      goto found_final_answer_early;
   }

   platform_assert(!*found || data_message_class(data_cfg, data) == MESSAGE_TYPE_UPDATE);
   if (*found) {
      data_merge_tuples_final(spl->cfg.data_cfg, key, data);
found_final_answer_early:
      type = data_message_class(data_cfg, data);
      *found = type != MESSAGE_TYPE_DELETE;
   }

   if (found_in_memtable) {
      // release memtable lookup lock
      memtable_unget_lookup_lock(spl->mt_ctxt, mt_lookup_lock_page);
   } else {
      splinter_node_unget(spl, &node);
   }
   if (spl->cfg.use_stats) {
      threadid tid = platform_get_tid();
      if (*found) {
         spl->stats[tid].lookups_found++;
      } else {
         spl->stats[tid].lookups_not_found++;
      }
   }

   return STATUS_OK;
}

/*
 * splinter_async_set_state sets the state of the async splinter
 * lookup state machine.
 */

static inline void
splinter_async_set_state(splinter_async_ctxt *ctxt,
                         splinter_async_state new_state)
{
   ctxt->prev_state = ctxt->state;
   ctxt->state = new_state;
}


/*
 * splinter_trunk_async_callback
 *
 *      Callback that's called when the async cache get for a trunk
 *      node loads a page for the child into the cache. This function
 *      moves the async splinter lookup state machine's state ahead,
 *      and calls the upper layer callback that'll re-enqueue the
 *      spinter lookup for dispatch.
 */

static void
splinter_trunk_async_callback(cache_async_ctxt *cache_ctxt)
{
   splinter_async_ctxt *ctxt = container_of(cache_ctxt, splinter_async_ctxt,
                                            cache_ctxt);
   platform_assert(SUCCESS(cache_ctxt->status));
   platform_assert(cache_ctxt->page);
//   platform_log("%s:%d tid %2lu: ctxt %p is callback with page %p\n",
//                __FILE__, __LINE__, platform_get_tid(), ctxt,
//                cache_ctxt->page);
   ctxt->was_async = TRUE;
   // Move state machine ahead and requeue for dispatch
   if (UNLIKELY(ctxt->state == async_state_get_root_reentrant)) {
      splinter_async_set_state(ctxt, async_state_trunk_node_lookup);
   } else {
      debug_assert(ctxt->state == async_state_get_child_trunk_node_reentrant);
      splinter_async_set_state(ctxt, async_state_unget_parent_trunk_node);
   }
   ctxt->cb(ctxt);
}


/*
 * splinter_filter_async_callback
 *
 *      Callback that's called when the async filter get api has loaded
 *      a page into cache. This just requeues the splinter lookup for
 *      dispatch at the same state, so that async filter get can be
 *      called again.
 */

static void
splinter_filter_async_callback(routing_async_ctxt *filter_ctxt)
{
   splinter_async_ctxt *ctxt = container_of(filter_ctxt, splinter_async_ctxt,
                                            filter_ctxt);
//   platform_log("%s:%d tid %2lu: ctxt %p is callback\n",
//                __FILE__, __LINE__, platform_get_tid(), ctxt);
   // Requeue for dispatch
   ctxt->cb(ctxt);
}


/*
 * splinter_btree_async_callback
 *
 *      Callback that's called when the async btree lookup api has loaded
 *      a page into cache. This just requeues the splinter lookup for
 *      dispatch at the same state, so that async btree lookup can be
 *      called again.
 */

static void
splinter_btree_async_callback(btree_async_ctxt *btree_ctxt)
{
   splinter_async_ctxt *ctxt = container_of(btree_ctxt, splinter_async_ctxt,
                                            btree_ctxt);
//   platform_log("%s:%d tid %2lu: ctxt %p is callback\n",
//                __FILE__, __LINE__, platform_get_tid(), ctxt);
   // Requeue for dispatch
   ctxt->cb(ctxt);
}


/*
 * Async splinter lookup. Caller must have called splinter_async_ctxt_init()
 * on the context before the first invocation.
 *
 * This uses hand over hand locking to descend the trunk tree and
 * every time a child node needs to be looked up from the cache, it
 * uses the async get api. A reference to the parent node is held in
 * splinter_async_ctxt->trunk_node while a reference to the child page
 * is obtained by the cache_get_async() into
 * splinter_async_ctxt->cache_ctxt->page
 *
 * Returns:
 *    async_success: results are available in *found and *data
 *    async_locked: caller needs to retry
 *    async_no_reqs: caller needs to retry but may want to throttle
 *    async_io_started: async IO was started; the caller will be informed
 *      via callback when it's done. After callback is called, the caller
 *      must call this again from thread context with the same key and data
 *      as the first invocation.
 *
 * Side-effects:
 *    Maintains state in *data. This helps avoid copying data between
 *    invocations. Caller must use the same pointers to key, data and
 *    found in different invocations of a lookup until it returns
 *    async_success. Caller must not modify the contents of those
 *    pointers.
 */

cache_async_result
splinter_lookup_async(splinter_handle     *spl,    // IN
                      char                *key,    // IN
                      char                *data,   // OUT
                      bool                *found,  // OUT
                      splinter_async_ctxt *ctxt)   // IN/OUT
{
   // FIXME: [yfogel 2020-08-26] giant switch statements are impossible to read
   //       this needs to be refactored.
   //       we can have a dispatch function that has the switch
   //       and static function calls to split up all this enormous stuff going
   //       on
   // FIXME: [yfogel 2020-08-26] need to restore/reapply/convert/...
   //       the lookup_async_changes from 272409ec66c AND from f1bee101262
   cache_async_result res = 0;
   threadid tid;
   data_config *data_cfg = spl->cfg.data_cfg;

#if SPLINTER_DEBUG
   cache_enable_sync_get(spl->cc, FALSE);
#endif
   if (spl->cfg.use_stats) {
      tid = platform_get_tid();
   }
   page_handle *node = ctxt->trunk_node;
   bool done = FALSE;

   do {
      switch (ctxt->state) {
      case async_state_start:
      {
         ctxt->found = FALSE;
         splinter_async_set_state(ctxt, async_state_lookup_memtable);
         // fallthrough
      }
      case async_state_lookup_memtable:
      {
         ctxt->mt_lock_page = memtable_get_lookup_lock(spl->mt_ctxt);
         uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
         uint64 mt_gen_end = memtable_generation_retired(spl->mt_ctxt);
         for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
            // FIXME: [aconway 2020-08-26] wrap with a branch lookup
            splinter_memtable_lookup(spl, mt_gen, key, data, &ctxt->found);
            if (ctxt->found) {
               ctxt->type = data_message_class(data_cfg, data);
               if (ctxt->type != MESSAGE_TYPE_UPDATE) {
                  splinter_async_set_state(ctxt,
                        async_state_found_final_answer_early);
                  break;
               }
            }
         }
         // fallthrough
      }
      case async_state_get_root_reentrant:
      {
         cache_ctxt_init(spl->cc, splinter_trunk_async_callback, NULL,
                         &ctxt->cache_ctxt);
         res = splinter_node_get_async(spl, spl->root_addr, ctxt);
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
            splinter_async_set_state(ctxt, async_state_trunk_node_lookup);
            platform_assert(node == NULL);
            ctxt->trunk_node = node = ctxt->cache_ctxt.page;
            memtable_unget_lookup_lock(spl->mt_ctxt, ctxt->mt_lock_page);
            ctxt->mt_lock_page = NULL;
            break;
         default:
            platform_assert(0);
         }
         break;
      }
      case async_state_trunk_node_lookup:
      {
         ctxt->height = splinter_height(spl, node);
         uint16 pivot_no = splinter_find_pivot(spl, node, key,
               less_than_or_equal);
         debug_assert(pivot_no < splinter_num_children(spl, node));
         ctxt->pdata = splinter_get_pivot_data(spl, node, pivot_no);
         ctxt->sb_no = splinter_start_subbundle_for_lookup(spl, node);
         ctxt->end_sb_no =
            splinter_pivot_end_subbundle_for_lookup(spl, node, ctxt->pdata);
         ctxt->filter_no = 0;
         char key_str[128];
         splinter_key_to_string(spl, key, key_str);
         splinter_async_set_state(ctxt, async_state_subbundle_lookup);
         // fallthrough
      }
      case async_state_subbundle_lookup:
      {
         if (ctxt->sb_no == ctxt->end_sb_no) {
            debug_assert(ctxt->filter_no == 0);
            ctxt->lookup_state = async_lookup_state_pivot;
            splinter_async_set_state(ctxt, async_state_pivot_lookup);
            break;
         }
         ctxt->sb = splinter_get_subbundle(spl, node, ctxt->sb_no);
         if (ctxt->sb->state == SB_STATE_COMPACTED) {
            ctxt->lookup_state = async_lookup_state_compacted_subbundle;
         } else {
            ctxt->lookup_state = async_lookup_state_subbundle;
         }
         debug_assert(ctxt->filter_no <
               splinter_subbundle_filter_count(spl, node, ctxt->sb));
         ctxt->filter =
            splinter_subbundle_filter(spl, node, ctxt->sb, ctxt->filter_no);
         splinter_async_set_state(ctxt, async_state_filter_lookup_start);
         break;
      }
      case async_state_pivot_lookup:
      {
         ctxt->sb = NULL;
         ctxt->filter = &ctxt->pdata->filter;
         splinter_async_set_state(ctxt, async_state_filter_lookup_start);
         // fall through
      }
      case async_state_filter_lookup_start:
      {
         ctxt->value = ROUTING_NOT_FOUND;
         if (ctxt->filter->addr == 0) {
            platform_assert(ctxt->lookup_state == async_lookup_state_pivot);
            splinter_async_set_state(ctxt, async_state_next_in_node);
            break;
         }
         if (spl->cfg.use_stats) {
            spl->stats[tid].filter_lookups[ctxt->height]++;
         }
         routing_filter_ctxt_init(&ctxt->filter_ctxt, &ctxt->cache_ctxt,
               splinter_filter_async_callback);
         splinter_async_set_state(ctxt, async_state_filter_lookup_reentrant);
         break;
      }
      case async_state_filter_lookup_reentrant:
      {
         //bool is_leaf;
         //switch (ctxt->lookup_state) {
         //   case async_lookup_state_pivot:
         //      is_leaf = ctxt->height == 0;
         //      break;
         //   case async_lookup_state_subbundle:
         //      debug_assert(ctxt->sb != NULL);
         //      is_leaf = ctxt->sb->state == SB_STATE_UNCOMPACTED_LEAF;
         //      break;
         //   case async_lookup_state_compacted_subbundle:
         //      is_leaf = FALSE;
         //      break;
         //}

         routing_config *filter_cfg = splinter_routing_cfg(spl, TRUE);
         res = splinter_filter_lookup_async(spl, filter_cfg, ctxt->filter, key,
               &ctxt->found_values, &ctxt->filter_ctxt);
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
            // I don't own the cache context, filter does
            splinter_async_set_state(ctxt, async_state_btree_lookup_start);
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
               ctxt->value =
                  routing_filter_get_next_value(ctxt->found_values, ctxt->value);
               if (ctxt->value == ROUTING_NOT_FOUND) {
                  splinter_async_set_state(ctxt, async_state_next_in_node);
                  continue;
               }
               branch_no = splinter_branch_no_add(spl,
                     ctxt->pdata->start_branch, ctxt->value);
               break;
            case async_lookup_state_subbundle:
               debug_assert(ctxt->sb != NULL);
               ctxt->value =
                  routing_filter_get_next_value(ctxt->found_values, ctxt->value);
               if (ctxt->value == ROUTING_NOT_FOUND) {
                  splinter_async_set_state(ctxt, async_state_next_in_node);
                  continue;
               }
               branch_no = splinter_branch_no_add(spl,
                     ctxt->sb->start_branch, ctxt->value);
               branch_no = ctxt->sb->start_branch + ctxt->value;
               break;
            case async_lookup_state_compacted_subbundle:
               debug_assert(ctxt->sb != NULL);
               if (ctxt->found_values == 0) {
                  ctxt->value = ROUTING_NOT_FOUND;
                  splinter_async_set_state(ctxt, async_state_next_in_node);
                  continue;
               }
               branch_no = ctxt->sb->start_branch;
               break;
         }
         ctxt->branch = splinter_get_branch(spl, node, branch_no);
         btree_ctxt_init(&ctxt->btree_ctxt, &ctxt->cache_ctxt,
                         splinter_btree_async_callback);
         splinter_async_set_state(ctxt, async_state_btree_lookup_reentrant);
         break;
      }
      case async_state_btree_lookup_reentrant:
      {
         res = splinter_btree_lookup_async(spl, ctxt->branch, key, data,
                                           &ctxt->found, &ctxt->btree_ctxt);
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
            // I don't own the cache context, btree does
            if (ctxt->found) {
               ctxt->type = data_message_class(data_cfg, data);
               if (ctxt->type != MESSAGE_TYPE_UPDATE) {
                  splinter_async_set_state(ctxt,
                             async_state_found_final_answer_early);
                  break;
               }
            } else if (spl->cfg.use_stats) {
               const uint16 height = splinter_height(spl, node);
               spl->stats[tid].filter_false_positives[height]++;
            }
            splinter_async_set_state(ctxt, async_state_next_in_node);
            break;
         default:
            platform_assert(0);
         }
         break;
      }
      case async_state_next_in_node:
      {
         switch(ctxt->lookup_state) {
            case async_lookup_state_pivot:
               debug_assert(ctxt->filter_no == 0);
               if (ctxt->value == ROUTING_NOT_FOUND) {
                  splinter_async_set_state(ctxt, async_state_trunk_node_done);
               } else {
                  splinter_async_set_state(ctxt,
                                           async_state_btree_lookup_start);
               }
               continue;
            case async_lookup_state_subbundle:
               debug_assert(ctxt->filter_no == 0);
               if (ctxt->value == ROUTING_NOT_FOUND) {
                  ctxt->sb_no = splinter_subbundle_no_sub(spl, ctxt->sb_no, 1);
                  splinter_async_set_state(ctxt, async_state_subbundle_lookup);
                  break;
               } else {
                  splinter_async_set_state(ctxt,
                                           async_state_btree_lookup_start);
               }
               continue;
            case async_lookup_state_compacted_subbundle:
               if (ctxt->found_values != 0) {
                  ctxt->sb_no = splinter_subbundle_no_sub(spl, ctxt->sb_no, 1);
                  ctxt->filter_no = 0;
               } else {
                  ctxt->filter_no++;
                  uint16 sb_filter_count =
                     splinter_subbundle_filter_count(spl, node, ctxt->sb);
                  if (ctxt->filter_no >= sb_filter_count) {
                     debug_assert(ctxt->filter_no == sb_filter_count);
                     ctxt->sb_no =
                        splinter_subbundle_no_sub(spl, ctxt->sb_no, 1);
                     ctxt->filter_no = 0;
                  }
               }
               splinter_async_set_state(ctxt, async_state_subbundle_lookup);
               continue;
         }
         break;
      }
      case async_state_trunk_node_done:
      {
         if (ctxt->height == 0) {
            if (ctxt->found && ctxt->type != MESSAGE_TYPE_INSERT) {
               data_merge_tuples_final(spl->cfg.data_cfg, key, data);
               ctxt->type = data_message_class(data_cfg, data);
               ctxt->found = ctxt->type != MESSAGE_TYPE_DELETE;
            }
            splinter_async_set_state(ctxt, async_state_end);
            break;
         } else {
            splinter_async_set_state(ctxt,
                  async_state_get_child_trunk_node_reentrant);
            break;
         }
      }
      case async_state_get_child_trunk_node_reentrant:
      {
         cache_ctxt_init(spl->cc, splinter_trunk_async_callback, NULL,
                         &ctxt->cache_ctxt);
         debug_assert(ctxt->pdata != NULL);
         res = splinter_node_get_async(spl, ctxt->pdata->addr, ctxt);
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
            splinter_async_set_state(ctxt, async_state_unget_parent_trunk_node);
            break;
         default:
            platform_assert(0);
         }
         break;
      }
      case async_state_unget_parent_trunk_node:
      {
         if (ctxt->was_async) {
            splinter_node_async_done(spl, ctxt);
         }
         splinter_node_unget(spl, &node);
         ctxt->pdata = NULL;
         ctxt->trunk_node = node = ctxt->cache_ctxt.page;
         splinter_async_set_state(ctxt, async_state_trunk_node_lookup);
         break;
      }
      case async_state_found_final_answer_early:
      {
         ctxt->found = ctxt->type != MESSAGE_TYPE_DELETE;
         splinter_async_set_state(ctxt, async_state_end);
         break;
      }
      case async_state_end:
      {
         if (ctxt->mt_lock_page != NULL) {
            memtable_unget_lookup_lock(spl->mt_ctxt, ctxt->mt_lock_page);
            ctxt->mt_lock_page = NULL;
            debug_assert(node == NULL);
         } else {
            splinter_node_unget(spl, &node);
         }
         ctxt->trunk_node = NULL;
         if (spl->cfg.use_stats) {
            if (ctxt->found) {
               spl->stats[tid].lookups_found++;
            } else {
               spl->stats[tid].lookups_not_found++;
            }
         }
         *found = ctxt->found;
         res = async_success;
         done = TRUE;
         break;
      }
      default:
         platform_assert(0);
      }
   } while (!done);
#if SPLINTER_DEBUG
   cache_enable_sync_get(spl->cc, TRUE);
#endif

   return res;
}


platform_status
splinter_range(splinter_handle *spl,
               char            *start_key,
               uint64           num_tuples,
               uint64          *tuples_returned,
               char            *out)
{
   splinter_range_iterator *range_itor = TYPED_MALLOC(spl->heap_id, range_itor);
   platform_status rc = splinter_range_iterator_init(spl, range_itor,
                                                     start_key, NULL,
                                                     num_tuples);
   if (!SUCCESS(rc)) {
      goto destroy_range_itor;
   }

   data_type type;
   bool at_end;
   iterator_at_end(&range_itor->super, &at_end);

   for (*tuples_returned = 0; *tuples_returned < num_tuples && !at_end; (*tuples_returned)++) {
      char *key, *data;
      iterator_get_curr(&range_itor->super, &key, &data, &type);
      char *next_key = out + *tuples_returned * (splinter_key_size(spl) + splinter_message_size(spl));
      char *next_data = next_key + splinter_key_size(spl);
      memmove(next_key, key, splinter_key_size(spl));
      memmove(next_data, data, splinter_message_size(spl));
      iterator_advance(&range_itor->super);
      iterator_at_end(&range_itor->super, &at_end);
   }

destroy_range_itor:
   splinter_range_iterator_deinit(range_itor);
   platform_free(spl->heap_id, range_itor);
   return rc;
}


/*
 *-----------------------------------------------------------------------------
 *
 * create/destroy
 * XXX Fix this api to return platform_status
 *
 *-----------------------------------------------------------------------------
 */

splinter_handle *
splinter_create(splinter_config  *cfg,
                allocator        *al,
                cache            *cc,
                task_system      *ts,
                allocator_root_id       id,
                platform_heap_id  hid)
{
   // FIXME: [yfogel 2020-03-30] handle all failures and properly cleanup
   splinter_handle *spl = TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, spl,
         compacted_memtable, SPLINTER_NUM_MEMTABLES);
   memmove(&spl->cfg, cfg, sizeof(*cfg));
   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id = id;
   spl->heap_id = hid;
   spl->ts = ts;

   srq_init(&spl->srq, platform_get_module_id(), hid);

   // get a free node for the root
   //    we don't use the mini allocator for this, since the root doesn't
   //    maintain constant height
   platform_status rc = allocator_alloc_extent(spl->al, &spl->root_addr);
   platform_assert_status_ok(rc);
   page_handle *root = cache_alloc(spl->cc, spl->root_addr, PAGE_TYPE_TRUNK);
   splinter_trunk_hdr *root_hdr = (splinter_trunk_hdr *)root->data;
   ZERO_CONTENTS(root_hdr);

   // set up the mini allocator
   //    we use the root extent as the initial mini_allocator head
   uint64 meta_addr = spl->root_addr + cfg->page_size;
   mini_allocator_init(&spl->mini,
                       cc,
                       spl->cfg.data_cfg,
                       meta_addr,
                       0,
                       SPLINTER_MAX_HEIGHT,
                       PAGE_TYPE_TRUNK);

   // set up the memtable context
   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   spl->mt_ctxt = memtable_context_create(spl->heap_id, cc, mt_cfg,
         splinter_memtable_flush_virtual, spl);

   // set up the log
   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
   }

   // ALEX: For now we assume an init means destroying any present super blocks
   splinter_set_super_block(spl, FALSE, FALSE, TRUE);

   // set up the initial leaf
   page_handle *       leaf     = splinter_alloc(spl, 0);
   splinter_trunk_hdr *leaf_hdr = (splinter_trunk_hdr *)leaf->data;
   memset(leaf_hdr, 0, spl->cfg.page_size);
   const char *min_key = spl->cfg.data_cfg->min_key;
   const char *max_key = spl->cfg.data_cfg->max_key;
   splinter_set_initial_pivots(spl, leaf, min_key, max_key);
   splinter_inc_pivot_generation(spl, leaf);

   // add leaf to root and fix up root
   root_hdr->height = 1;
   splinter_add_pivot_new_root(spl, root, leaf);
   splinter_inc_pivot_generation(spl, root);

   splinter_node_unlock(spl, leaf);
   splinter_node_unclaim(spl, leaf);
   splinter_node_unget(spl, &leaf);

   splinter_node_unlock(spl, root);
   splinter_node_unclaim(spl, root);
   splinter_node_unget(spl, &root);

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

   splinter_list_insert(&spl->links);

   // FIXME: [yfogel 2020-03-30] cleanup has to properly handle all the things
   //        that were allocated (possibly calling destroy/deinit something)
   return spl;
}

splinter_handle *
splinter_mount(splinter_config  *cfg,
               allocator        *al,
               cache            *cc,
               task_system      *ts,
               allocator_root_id       id,
               platform_heap_id  hid)
 {
   splinter_handle *spl = TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, spl,
         compacted_memtable, SPLINTER_NUM_MEMTABLES);
   memmove(&spl->cfg, cfg, sizeof(*cfg));
   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id = id;
   spl->heap_id = hid;
   spl->ts = ts;

   srq_init(&spl->srq, platform_get_module_id(), hid);

   // find the dismounted super block
   spl->root_addr = 0;
   uint64 meta_tail = 0;
   uint64 latest_timestamp = 0;
   page_handle          *super_page;
   splinter_super_block *super =
      splinter_get_super_block_if_valid(spl, &super_page);
   if (super != NULL) {
      if (super->dismounted && super->timestamp > latest_timestamp) {
         spl->root_addr = super->root_addr;
         meta_tail = super->meta_tail;
         latest_timestamp = super->timestamp;
      }
      splinter_release_super_block(spl, super_page);
   }
   if (spl->root_addr == 0) {
      // FIXME: [yfogel 2020-03-30] we forgot to clean up (e.g.
      //        free spl, possibly something else)
      return NULL;
   }
   uint64 meta_head = spl->root_addr + spl->cfg.page_size;

   // get a free node for the root
   // we don't use the next_addr arr for this, since the root doesn't
   // maintain constant height

   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   spl->mt_ctxt = memtable_context_create(spl->heap_id, cc, mt_cfg,
         splinter_memtable_flush_virtual, spl);

   mini_allocator_init(&spl->mini, cc, spl->cfg.data_cfg, meta_head, meta_tail,
         SPLINTER_MAX_HEIGHT, PAGE_TYPE_TRUNK);
   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
   }

   splinter_set_super_block(spl, FALSE, FALSE, FALSE);

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
   splinter_list_insert(&spl->links);
   // FIXME: [yfogel 2020-03-30] proper error handling for this entire function
   return spl;
}

void
splinter_deinit(splinter_handle *spl)
{
   // write current memtable to disk
   // (any others must already be flushing/flushed)

   // FIXME: [aconway 2020-08-24] This function is only safe to call when all
   //        other calls to spl have returned and all tasks have been complete.
   //        Therefore, this race shouldn't exist. Similarly, we don't hold the
   //        insert lock or rotate while flushing the memtable.
   if (!memtable_is_empty(spl->mt_ctxt)) {
      uint64 generation = memtable_force_finalize(spl->mt_ctxt);
      splinter_memtable_flush(spl, generation);
   }

   // finish any outstanding tasks and destroy task system for this table.
   task_perform_all(spl->ts);

   // destroy memtable context (and its memtables)
   memtable_context_destroy(spl->heap_id, spl->mt_ctxt);

   // release the log
   if (spl->cfg.use_log) {
      platform_free(spl->heap_id, spl->log);
   }

   // flush all dirty pages in the cache
   cache_flush(spl->cc);
}

bool
splinter_node_destroy(splinter_handle *spl,
                      uint64           addr,
                      void            *arg)
{
   page_handle *node = splinter_node_get(spl, addr);
   splinter_node_claim(spl, &node);
   splinter_node_lock(spl, node);
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (pdata->filter.addr != 0) {
         splinter_dec_filter(spl, &pdata->filter);
         //platform_log("dec filter %lu in %lu (%u)\n",
         //      pdata->filter.addr, node->disk_addr,
         //      allocator_get_refcount(spl->al, pdata->filter.addr));
      }
      for (uint16 branch_no = pdata->start_branch;
           branch_no != splinter_end_branch(spl, node);
           branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
         splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
         const char *start_key = splinter_get_pivot(spl, node, pivot_no);
         const char *end_key = splinter_get_pivot(spl, node, pivot_no + 1);
         splinter_zap_branch_range(spl, branch, start_key, end_key,
               PAGE_TYPE_BRANCH);
      }
   }
   uint16 start_filter = splinter_start_sb_filter(spl, node);
   uint16 end_filter = splinter_end_sb_filter(spl, node);
   for (uint16 filter_no = start_filter; filter_no != end_filter; filter_no++) {
      routing_filter *filter = splinter_get_sb_filter(spl, node, filter_no);
      splinter_dec_filter(spl, filter);
      //platform_log("dec filter %lu in %lu (%u)\n",
      //      filter->addr, node->disk_addr,
      //      allocator_get_refcount(spl->al, filter->addr));
   }

   splinter_node_unlock(spl, node);
   splinter_node_unclaim(spl, node);
   splinter_node_unget(spl, &node);
   return TRUE;
}

void
splinter_destroy(splinter_handle *spl)
{
   srq_deinit(&spl->srq);
   splinter_list_remove(&spl->links);

   splinter_deinit(spl);

   splinter_for_each_node(spl, splinter_node_destroy, NULL);

   mini_allocator_zap(spl->cc, NULL, spl->mini.meta_head, NULL, NULL,
         PAGE_TYPE_TRUNK);

   // clear out this splinter table from the meta page.
   allocator_remove_super_addr(spl->al, spl->id);

   if (spl->cfg.use_stats) {
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_histo_destroy(spl->heap_id,
                                spl->stats[i].insert_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                spl->stats[i].update_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                spl->stats[i].delete_latency_histo);
      }
      platform_free(spl->heap_id, spl->stats);
   }
   platform_free(spl->heap_id, spl);
}

void
splinter_dismount(splinter_handle *spl)
{
   srq_deinit(&spl->srq);
   splinter_list_remove(&spl->links);
   splinter_set_super_block(spl, FALSE, TRUE, FALSE);
   splinter_deinit(spl);
   if (spl->cfg.use_stats) {
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_histo_destroy(spl->heap_id,
                                spl->stats[i].insert_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                spl->stats[i].update_latency_histo);
         platform_histo_destroy(spl->heap_id,
                                spl->stats[i].delete_latency_histo);
      }
      platform_free(spl->heap_id, spl->stats);
   }
   platform_free(spl->heap_id, spl);
}

/*
 *-----------------------------------------------------------------------------
 *
 * splinter_perform_task
 *
 *      do a batch of tasks
 *
 *-----------------------------------------------------------------------------
 */

void
splinter_perform_tasks(splinter_handle *spl)
{
   task_perform_all(spl->ts);
   cache_cleanup(spl->cc);
}

/*
 *-----------------------------------------------------------------------------
 *
 * debugging and info functions
 *
 *-----------------------------------------------------------------------------
 */


/*
 * verify_node checks that the node is valid in the following places:
 *    1. values in the trunk header
 *    2. pivots are coherent (in order)
 *    3. check tuple counts (index nodes only, leaves have estimates)
 *    4. bundles are coherent (subbundles are contiguous and non-overlapping)
 *    5. subbundles are coherent (branches are contiguous and non-overlapping)
 *    6. start_frac (resp end_branch) is first (resp last) branch in a subbundle
 */

bool
splinter_verify_node(splinter_handle *spl,
                     page_handle     *node)
{
   bool is_valid = FALSE;
   uint64 addr = node->disk_addr;

   // check values in trunk hdr (currently just num_pivot_keys)
   if (splinter_num_pivot_keys(spl, node) > spl->cfg.max_pivot_keys) {
      platform_error_log("splinter_verify: too many pivots\n");
      platform_error_log("addr: %lu\n", addr);
      goto out;
   }

   // check that pivots are coherent
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      const char *pivot = splinter_get_pivot(spl, node, pivot_no);
      const char *next_pivot = splinter_get_pivot(spl, node, pivot_no + 1);
      if (splinter_key_compare(spl, pivot, next_pivot) >= 0) {
         platform_error_log("splinter_verify: pivots out of order\n");
         platform_error_log("addr: %lu\n", addr);
         goto out;
      }
   }

   // check that pivot generations are < hdr->pivot_generation
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (pdata->generation >= splinter_pivot_generation(spl, node)) {
         platform_error_log("splinter_verify: pivot generation out of bound\n");
         platform_error_log("addr: %lu\n", addr);
         goto out;
      }
   }

   // check that pivot tuple counts are correct
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      uint64 count = 0;
      //uint64 slow_count = 0;
      uint16 pivot_start_branch =
         splinter_pivot_start_branch(spl, node, pivot_no);
      for (uint16 branch_no = pivot_start_branch;
           branch_no != splinter_end_branch(spl, node);
           branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
         //slow_count += splinter_pivot_tuples_in_branch_slow(spl, node,
         //      pivot_no, branch_no);
         count += splinter_pivot_tuples_in_branch(spl, node, pivot_no,
                                                  branch_no);
      }
      //if (count != slow_count) {
      //   platform_error_log("splinter_verify: count != slow_count\n");
      //   platform_error_log("addr: %lu\n", addr);
      //   goto out;
      //}
      if (splinter_pivot_num_tuples(spl, node, pivot_no) != count) {
         platform_error_log("splinter_verify: pivot num tuples incorrect\n");
         platform_error_log("addr: %lu\n", addr);
         goto out;
      }
   }

   // check that pivot branches and bundles are valid
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (!splinter_branch_valid(spl, node, pdata->start_branch)) {
         platform_error_log("splinter_verify: invalid pivot start branch\n");
         platform_error_log("addr: %lu\n", addr);
         goto out;
      }
      if (!splinter_bundle_valid(spl, node, pdata->start_bundle)) {
         platform_error_log("splinter_verify: invalid pivot start bundle\n");
         platform_error_log("addr: %lu\n", addr);
         goto out;
      }
   }

   // check bundles are coherent
   splinter_bundle *last_bundle = NULL;
   for (uint16 bundle_no = splinter_start_bundle(spl, node);
        bundle_no != splinter_end_bundle(spl, node);
        bundle_no = splinter_bundle_no_add(spl, bundle_no, 1)) {
      splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);
      if (bundle_no == splinter_start_bundle(spl, node)) {
         if (splinter_start_subbundle(spl, node) != bundle->start_subbundle) {
            platform_error_log("splinter_verify: start_subbundle mismatch\n");
            platform_error_log("addr: %lu\n", addr);
            goto out;
         }
      } else {
         if (last_bundle->end_subbundle != bundle->start_subbundle) {
            platform_error_log("splinter_verify: "
                  "bundles have mismatched subbundles\n");
            platform_error_log("addr: %lu\n", addr);
            goto out;
         }
      }
      if (bundle_no + 1 == splinter_end_bundle(spl, node)) {
         if (bundle->end_subbundle != splinter_end_subbundle(spl, node)) {
            platform_error_log("splinter_verify: end_subbundle mismatch\n");
            platform_error_log("addr: %lu\n", addr);
            goto out;
         }
      }
      last_bundle = bundle;
   }

   // check subbundles are coherent
   splinter_subbundle *last_sb = NULL;
   for (uint16 sb_no = splinter_start_subbundle(spl, node);
        sb_no != splinter_end_subbundle(spl, node);
        sb_no = splinter_subbundle_no_add(spl, sb_no, 1)) {
      splinter_subbundle *sb = splinter_get_subbundle(spl, node, sb_no);
      if (sb_no == splinter_start_subbundle(spl, node)) {
         if (sb->start_branch != splinter_start_frac_branch(spl, node)) {
            platform_error_log("splinter_verify: start_branch mismatch\n");
            platform_error_log("addr: %lu\n", addr);
            goto out;
         }
      } else {
         if (sb->start_branch != last_sb->end_branch) {
            platform_error_log("splinter_verify: "
                  "subbundles have mismatched branches\n");
            platform_error_log("addr: %lu\n", addr);
            goto out;
         }
      }
      if (sb_no + 1 == splinter_end_subbundle(spl, node)) {
         if (sb->end_branch != splinter_end_branch(spl, node)) {
            platform_error_log("splinter_verify: end_branch mismatch\n");
            platform_error_log("addr: %lu\n", addr);
            goto out;
         }
      }
      last_sb = sb;
   }

   // check that pivot start branches and start bundles are coherent
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (!splinter_bundle_live(spl, node, pdata->start_bundle)) {
         if (1 && pdata->start_branch != splinter_end_branch(spl, node)
               && splinter_bundle_count(spl, node) != 0) {
            platform_error_log("splinter_verify: pivot start bundle doesn't match start branch\n");
            platform_error_log("addr: %lu\n", addr);
            goto out;
         }
      } else {
         splinter_bundle *bundle =
            splinter_get_bundle(spl, node, pdata->start_bundle);
         splinter_subbundle *sb =
            splinter_get_subbundle(spl, node, bundle->start_subbundle);
         if (pdata->start_branch != sb->start_branch) {
            if (!splinter_branch_in_range(spl, pdata->start_branch,
                     splinter_start_branch(spl, node), sb->start_branch)) {
               platform_error_log("splinter_verify: pivot start branch out of order with bundle start branch\n");
               platform_error_log("addr: %lu\n", addr);
               goto out;
            }
            if (pdata->start_bundle != splinter_start_bundle(spl, node)) {
               platform_error_log("splinter_verify: pivot start bundle incoherent with start branch\n");
               platform_error_log("addr: %lu\n", addr);
               goto out;
            }
         }
      }
   }

   if (splinter_height(spl, node) == 0) {
      if (splinter_num_children(spl, node) != 1) {
         platform_error_log("splinter_verify: leaf with multiple children\n");
         platform_error_log("addr: %lu\n", addr);
         goto out;
      }
   }

   is_valid = TRUE;
out:
   if (!is_valid) {
      splinter_print_locked_node(spl, node, PLATFORM_ERR_LOG_HANDLE);
   }
   return is_valid;
}

/*
 * verify_node_with_neighbors checks that the node has:
 * 1. coherent max key with successor's min key
 * 2. coherent pivots with children's min/max keys
 */

bool
splinter_verify_node_with_neighbors(splinter_handle *spl,
                                    page_handle     *node)
{
   bool is_valid = FALSE;
   uint64 addr = node->disk_addr;

   // check node and successor have coherent pivots
   uint64 succ_addr = splinter_next_addr(spl, node);
   if (succ_addr != 0) {
      page_handle *succ = splinter_node_get(spl, succ_addr);
      const char *ube = splinter_max_key(spl, node);
      const char *succ_lbi = splinter_min_key(spl, succ);
      if (splinter_key_compare(spl, ube, succ_lbi) != 0) {
         platform_log("splinter_verify_node_with_neighbors: "
               "mismatched pivots with successor\n");
         platform_log("addr: %lu\n", addr);
         splinter_node_unget(spl, &succ);
         goto out;
      }
      splinter_node_unget(spl, &succ);
   }

   if (splinter_height(spl, node) == 0) {
      is_valid = TRUE;
      goto out;
   }

   // check node and each child have coherent pivots
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 pivot_no = 0; pivot_no != num_children; pivot_no++) {
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      uint64 child_addr = pdata->addr;
      page_handle *child = splinter_node_get(spl, child_addr);

      // check pivot == child min key
      const char *pivot = splinter_get_pivot(spl, node, pivot_no);
      const char *child_min_key = splinter_min_key(spl, child);
      if (splinter_key_compare(spl, pivot, child_min_key) != 0) {
         platform_log("splinter_verify_node_with_neighbors: "
               "mismatched pivot with child min key\n");
         platform_log("0x%016lx%016lx%016lx\n", *((uint64 *)pivot), *(((uint64 *)pivot) + 1), *(((uint64 *)pivot) + 2));
         platform_log("0x%016lx%016lx%016lx\n", *((uint64 *)child_min_key), *(((uint64 *)child_min_key) + 1), *(((uint64 *)child_min_key) + 2));

         platform_log("addr: %lu\n", addr);
         platform_log("child addr: %lu\n", child_addr);
         splinter_node_unget(spl, &child);
         goto out;
      }
      const char *next_pivot = splinter_get_pivot(spl, node, pivot_no + 1);
      const char *child_max_key = splinter_max_key(spl, child);
      if (splinter_key_compare(spl, next_pivot, child_max_key) != 0) {
         platform_log("splinter_verify_node_with_neighbors: "
               "mismatched pivot with child max key\n");
         platform_log("addr: %lu\n", addr);
         platform_log("child addr: %lu\n", child_addr);
         splinter_node_unget(spl, &child);
         goto out;
      }

      splinter_node_unget(spl, &child);
   }

   is_valid = TRUE;
out:
   if (!is_valid) {
      splinter_print_locked_node(spl, node, PLATFORM_DEFAULT_LOG_HANDLE);
   }
   return is_valid;
}

/*
 * wrapper for splinter_for_each_node
 */

bool
splinter_verify_node_and_neighbors(splinter_handle *spl,
                                   uint64           addr,
                                   void            *arg)
{
   page_handle *node = splinter_node_get(spl, addr);
   bool is_valid = splinter_verify_node(spl, node);
   if (!is_valid) {
      goto out;
   }
   is_valid = splinter_verify_node_with_neighbors(spl, node);

out:
   splinter_node_unget(spl, &node);
   return is_valid;
}

/*
 * verify_tree verifies each node with itself and its neighbors
 */

bool
splinter_verify_tree(splinter_handle *spl)
{
   return splinter_for_each_node(spl, splinter_verify_node_and_neighbors, NULL);
}

/*
 * Returns the amount of space used by each level of the tree
 */

bool
splinter_node_space_use(splinter_handle *spl,
                        uint64           addr,
                        void            *arg)
{
   uint64 *bytes_used_on_level = (uint64 *)arg;
   uint64 bytes_used_in_node = 0;
   page_handle *node = splinter_node_get(spl, addr);
   uint16 num_pivot_keys = splinter_num_pivot_keys(spl, node);
   uint16 num_children = splinter_num_children(spl, node);
   for (uint16 branch_no = splinter_start_branch(spl, node);
        branch_no != splinter_end_branch(spl, node);
        branch_no = splinter_branch_no_add(spl, branch_no, 1))
   {
      splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
      char *start_key = NULL;
      char *end_key = NULL;
      for (uint16 pivot_no = 0; pivot_no < num_pivot_keys; pivot_no++) {
         if (1 && pivot_no != num_children
               && splinter_branch_live_for_pivot(spl, node, branch_no, pivot_no))
         {
            if (start_key == NULL) {
               start_key = splinter_get_pivot(spl, node, pivot_no);
            }
         } else {
            if (start_key != NULL) {
               end_key = splinter_get_pivot(spl, node, pivot_no);
               uint64 bytes_used_in_branch_range =
                  btree_space_use_in_range(spl->cc, &spl->cfg.btree_cfg,
                        branch->root_addr, PAGE_TYPE_BRANCH, start_key,
                        end_key);
               bytes_used_in_node += bytes_used_in_branch_range;
            }
            start_key = NULL;
            end_key = NULL;
         }
      }
   }

   uint16 height = splinter_height(spl, node);
   bytes_used_on_level[height] += bytes_used_in_node;
   splinter_node_unget(spl, &node);
   return TRUE;
}

void
splinter_print_space_use(splinter_handle *spl)
{
   uint64 bytes_used_by_level[SPLINTER_MAX_HEIGHT] = { 0 };
   splinter_for_each_node(spl, splinter_node_space_use, bytes_used_by_level);

   platform_log("Space used by level:\n");
   for (uint16 i = 0; i <= splinter_tree_height(spl); i++) {
      platform_log("%u: %8luMiB\n", i, B_TO_MiB(bytes_used_by_level[i]));
   }
   platform_log("\n");
}

void
splinter_print_locked_node(splinter_handle        *spl,
                           page_handle            *node,
                           platform_stream_handle stream)
{
   uint16 height = splinter_height(spl, node);
   platform_log_stream("---------------------------------------------------------------------------------------\n");
   platform_log_stream("|          |     addr      |   next addr  | height |   gen   | pvt gen |              |\n");
   platform_log_stream("|  HEADER  |---------------|--------------|--------|---------|---------|--------------|\n");
   platform_log_stream("|          | %12lu^ | %12lu | %6u | %7lu | %7lu |              |\n",
         node->disk_addr, splinter_next_addr(spl, node), height, splinter_generation(spl, node), splinter_pivot_generation(spl, node));
   platform_log_stream("|-------------------------------------------------------------------------------------|\n");
   platform_log_stream("|                                       PIVOTS                                        |\n");
   platform_log_stream("|-------------------------------------------------------------------------------------|\n");
   platform_log_stream("|         pivot key        |  child addr  |  filter addr | tuple count |  srq |  gen  |\n");
   platform_log_stream("|--------------------------|--------------|--------------|-------------|------|-------|\n");
   for (uint16 pivot_no = 0;
        pivot_no < splinter_num_pivot_keys(spl, node);
        pivot_no++)
   {
      char key_string[128];
      splinter_key_to_string(spl, splinter_get_pivot(spl, node, pivot_no), key_string);
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      if (pivot_no == splinter_num_pivot_keys(spl, node) - 1) {
         platform_log_stream("| %24s | %12s | %12s | %11s | %4s | %5s |\n",
               key_string, "", "", "", "", "");
      } else {
         platform_log_stream("| %24s | %12lu | %12lu | %11lu | %4ld | %5lu |\n",
               key_string, pdata->addr, pdata->filter.addr, pdata->num_tuples,
               pdata->srq_idx, pdata->generation);
      }
   }
   platform_log_stream("|-------------------------------------------------------------------------------------|\n");
   platform_log_stream("|                              BRANCHES AND [SUB]BUNDLES                              |\n");
   platform_log_stream("|-------------------------------------------------------------------------------------|\n");
   platform_log_stream("|   # |  point addr  |  range addr  | filter1 addr | filter2 addr | filter3 addr |    |\n");
   platform_log_stream("|     |    pivot/bundle/subbundle   |  num tuples  |              |              |    |\n");
   platform_log_stream("|-----|--------------|--------------|--------------|--------------|--------------|----|\n");
   uint16 start_branch = splinter_start_branch(spl, node);
   uint16 end_branch = splinter_end_branch(spl, node);
   uint16 start_bundle = splinter_start_bundle(spl, node);
   uint16 end_bundle = splinter_end_bundle(spl, node);
   uint16 start_sb = splinter_start_subbundle(spl, node);
   uint16 end_sb = splinter_end_subbundle(spl, node);
   for (uint16 branch_no = start_branch;
        branch_no != end_branch;
        branch_no = splinter_branch_no_add(spl, branch_no, 1))
   {
      for (uint16 pivot_no = 0;
           pivot_no < splinter_num_children(spl, node);
           pivot_no++)
      {
         if (branch_no == splinter_pivot_start_branch(spl, node, pivot_no)) {
            platform_log_stream("|     |        -- pivot %2u --       |              |              |              |    |\n", pivot_no);
         }
      }
      for (uint16 bundle_no = start_bundle;
           bundle_no != end_bundle;
           bundle_no = splinter_bundle_no_add(spl, bundle_no, 1))
      {
         splinter_bundle *bundle = splinter_get_bundle(spl, node, bundle_no);
         if (branch_no == splinter_bundle_start_branch(spl, node, bundle)) {
            platform_log_stream("|     |       -- bundle %2u --       | %12lu |              |              |    |\n", bundle_no, bundle->num_tuples);
         }
      }
      for (uint16 sb_no = start_sb;
           sb_no != end_sb;
           sb_no = splinter_subbundle_no_add(spl, sb_no, 1))
      {
         splinter_subbundle *sb = splinter_get_subbundle(spl, node, sb_no);
         if (branch_no == sb->start_branch) {
            uint16 filter_count = splinter_subbundle_filter_count(spl, node, sb);
            platform_log_stream("|     |  -- %scomp subbundle %2u --  | %12lu | %12lu | %12lu | %s |\n",
                                sb->state == SB_STATE_COMPACTED ? "" : "un",
                                sb_no,
                                0 < filter_count ? splinter_subbundle_filter(spl, node, sb, 0)->addr : 0,
                                1 < filter_count ? splinter_subbundle_filter(spl, node, sb, 1)->addr : 0,
                                2 < filter_count ? splinter_subbundle_filter(spl, node, sb, 2)->addr : 0,
                                3 < filter_count ? " *" : "  ");
         }
      }

      splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
      platform_log_stream("| %3u | %12lu | %12lu |              |              |              |    |\n", branch_no, branch->root_addr, branch->range_root_addr);
   }
   platform_log_stream("---------------------------------------------------------------------------------------\n");
   platform_log_stream("\n");
}

void
splinter_print_node(splinter_handle       *spl,
                    uint64                 addr,
                    platform_stream_handle stream)
{
   if (!cache_page_valid(spl->cc, addr)) {
      platform_log_stream("*******************\n");
      platform_log_stream("** INVALID NODE \n");
      platform_log_stream("** addr: %lu \n", addr);
      platform_log_stream("-------------------\n");
      return;
   }

   page_handle *node = splinter_node_get(spl, addr);
   splinter_print_locked_node(spl, node, stream);
   cache_unget(spl->cc, node);
}

void
splinter_print_subtree(splinter_handle        *spl,
                       uint64                  addr,
                       platform_stream_handle  stream)
{
   splinter_print_node(spl, addr, stream);
   page_handle *node = splinter_node_get(spl, addr);
   splinter_trunk_hdr *hdr = (splinter_trunk_hdr *)node->data;

   if (hdr->height != 0) {
      for (uint32 i = 0; i < splinter_num_children(spl, node); i++) {
         splinter_pivot_data *data = splinter_get_pivot_data(spl, node, i);
         splinter_print_subtree(spl, data->addr, stream);
      }
   }
   cache_unget(spl->cc, node);
}

void
splinter_print_memtable(splinter_handle        *spl,
                        platform_stream_handle  stream)
{
   uint64 curr_memtable =
      memtable_generation(spl->mt_ctxt) % SPLINTER_NUM_MEMTABLES;
   platform_log_stream("&&&&&&&&&&&&&&&&&&&\n");
   platform_log_stream("&&  MEMTABLES \n");
   platform_log_stream("&&  curr: %lu\n", curr_memtable);
   platform_log_stream("-------------------\n");

   uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
   uint64 mt_gen_end = memtable_generation_retired(spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      memtable *mt = splinter_get_memtable(spl, mt_gen);
      platform_log_stream("%lu: gen %lu ref_count %u state %d\n",
            mt_gen, mt->root_addr,
            allocator_get_refcount(spl->al, mt->root_addr), mt->state);
   }
   platform_log_stream("\n");
}

void
splinter_print(splinter_handle *spl)
{
   platform_open_log_stream();
   splinter_print_memtable(spl, stream);
   splinter_print_subtree(spl, spl->root_addr, stream);
   platform_close_log_stream(PLATFORM_DEFAULT_LOG_HANDLE);
}

void
splinter_print_insertion_stats(splinter_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log("Statistics are not enabled\n");
      return;
   }
   uint64 avg_flush_wait_time, avg_flush_time, num_flushes;
   uint64 avg_compaction_tuples, pack_time_per_tuple, avg_setup_time;
   fraction  avg_leaves_created;
   uint64 avg_filter_tuples, avg_filter_time, filter_time_per_tuple;
   uint32 h, rev_h;
   threadid thr_i;
   page_handle *node = splinter_node_get(spl, spl->root_addr);
   uint32 height = splinter_height(spl, node);
   splinter_node_unget(spl, &node);

   splinter_stats *global;

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
         global->flush_wait_time_ns[h]               += spl->stats[thr_i].flush_wait_time_ns[h];
         global->flush_time_ns[h]                    += spl->stats[thr_i].flush_time_ns[h];
         if (spl->stats[thr_i].flush_time_max_ns[h] >
             global->flush_time_max_ns[h]) {
            global->flush_time_max_ns[h] =
               spl->stats[thr_i].flush_time_max_ns[h];
         }
         global->full_flushes[h]                     += spl->stats[thr_i].full_flushes[h];
         global->count_flushes[h]                    += spl->stats[thr_i].count_flushes[h];

         global->compactions[h]                      += spl->stats[thr_i].compactions[h];
         global->compactions_aborted_flushed[h]      += spl->stats[thr_i].compactions_aborted_flushed[h];
         global->compactions_aborted_leaf_split[h]   += spl->stats[thr_i].compactions_aborted_leaf_split[h];
         global->compactions_discarded_flushed[h]    += spl->stats[thr_i].compactions_discarded_flushed[h];
         global->compactions_discarded_leaf_split[h] += spl->stats[thr_i].compactions_discarded_leaf_split[h];
         global->compactions_empty[h]                += spl->stats[thr_i].compactions_empty[h];
         global->compaction_tuples[h]                += spl->stats[thr_i].compaction_tuples[h];
         if (spl->stats[thr_i].compaction_max_tuples[h] > global->compaction_max_tuples[h]) {
            global->compaction_max_tuples[h] = spl->stats[thr_i].compaction_max_tuples[h];
         }
         global->compaction_time_ns[h]               += spl->stats[thr_i].compaction_time_ns[h];
         global->compaction_time_wasted_ns[h]        += spl->stats[thr_i].compaction_time_wasted_ns[h];
         global->compaction_pack_time_ns[h]          += spl->stats[thr_i].compaction_pack_time_ns[h];
         if (spl->stats[thr_i].compaction_time_max_ns[h] >
             global->compaction_time_max_ns[h]) {
            global->compaction_time_max_ns[h] =
               spl->stats[thr_i].compaction_time_max_ns[h];
         }
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

         global->filters_built[h]                    += spl->stats[thr_i].filters_built[h];
         global->filter_tuples[h]                    += spl->stats[thr_i].filter_tuples[h];
         global->filter_time_ns[h]                   += spl->stats[thr_i].filter_time_ns[h];

         global->space_recs[h]                       += spl->stats[thr_i].space_recs[h];
         global->space_rec_time_ns[h]                += spl->stats[thr_i].space_rec_time_ns[h];
         global->space_rec_tuples_reclaimed[h]       += spl->stats[thr_i].space_rec_tuples_reclaimed[h];
         global->tuples_reclaimed[h]                 += spl->stats[thr_i].tuples_reclaimed[h];
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
      global->root_full_flushes           += spl->stats[thr_i].root_full_flushes;
      global->root_count_flushes          += spl->stats[thr_i].root_count_flushes;
      global->root_flush_time_ns          += spl->stats[thr_i].root_flush_time_ns;
      if (spl->stats[thr_i].root_flush_time_max_ns >
          global->root_flush_time_max_ns) {
         global->root_flush_time_max_ns =
            spl->stats[thr_i].root_flush_time_max_ns;
      }
      global->root_flush_wait_time_ns     += spl->stats[thr_i].root_flush_wait_time_ns;
      global->index_splits                += spl->stats[thr_i].index_splits;

      global->leaf_splits                 += spl->stats[thr_i].leaf_splits;
      global->leaf_splits_leaves_created  += spl->stats[thr_i].leaf_splits_leaves_created;
      global->leaf_split_time_ns          += spl->stats[thr_i].leaf_split_time_ns;
      if (spl->stats[thr_i].leaf_split_max_time_ns >
            global->leaf_split_max_time_ns) {
         global->leaf_split_max_time_ns =
            spl->stats[thr_i].leaf_split_max_time_ns;
      }

      global->root_filters_built          += spl->stats[thr_i].root_filters_built;
      global->root_filter_tuples          += spl->stats[thr_i].root_filter_tuples;
      global->root_filter_time_ns         += spl->stats[thr_i].root_filter_time_ns;
   }

   platform_log("Overall Statistics\n");
   platform_log("------------------------------------------------------------------------------------\n");
   platform_log("| height:            %10u\n", height);
   platform_log("| index nodes:       %10lu\n", global->index_splits + 1);
   platform_log("| leaves:            %10lu\n", global->leaf_splits_leaves_created + 1);
   platform_log("| insertions:        %10lu\n", global->insertions);
   platform_log("| updates:           %10lu\n", global->updates);
   platform_log("| deletions:         %10lu\n", global->deletions);
   platform_log("| completed deletes: %10lu\n", global->discarded_deletes);
   platform_log("------------------------------------------------------------------------------------\n");
   platform_log("| root stalls:       %10lu\n", global->memtable_flush_root_full);
   platform_log("------------------------------------------------------------------------------------\n");
   platform_log("\n");

   platform_log("Latency Histogram Statistics\n");
   platform_histo_print(insert_lat_accum, "Insert Latency Histogram (ns):");
   platform_histo_print(update_lat_accum, "Update Latency Histogram (ns):");
   platform_histo_print(delete_lat_accum, "Delete Latency Histogram (ns):");
   platform_histo_destroy(spl->heap_id, insert_lat_accum);
   platform_histo_destroy(spl->heap_id, update_lat_accum);
   platform_histo_destroy(spl->heap_id, delete_lat_accum);


   platform_log("Flush Statistics\n");
   platform_log("---------------------------------------------------------------------------------------------------------\n");
   platform_log("  height | avg wait time (ns) | avg flush time (ns) | max flush time (ns) | full flushes | count flushes |\n");
   platform_log("---------|--------------------|---------------------|---------------------|--------------|---------------|\n");

   // memtable
   num_flushes = global->memtable_flushes;
   avg_flush_wait_time = num_flushes == 0 ? 0 : global->memtable_flush_wait_time_ns / num_flushes;
   avg_flush_time = num_flushes == 0 ? 0 : global->memtable_flush_time_ns / num_flushes;
   platform_log("memtable | %18lu | %19lu | %19lu | %12lu | %13lu |\n",
                avg_flush_wait_time, avg_flush_time,
                global->memtable_flush_time_max_ns, num_flushes, 0UL);

   // root
   num_flushes = global->root_full_flushes + global->root_count_flushes;
   avg_flush_wait_time = num_flushes == 0 ? 0 : global->root_flush_wait_time_ns / num_flushes;
   avg_flush_time = num_flushes == 0 ? 0 : global->root_flush_time_ns / num_flushes;
   platform_log("    root | %18lu | %19lu | %19lu | %12lu | %13lu |\n",
                avg_flush_wait_time, avg_flush_time,
                global->root_flush_time_max_ns,
                global->root_full_flushes, global->root_count_flushes);

   for (h = 1; h < height; h++) {
      rev_h = height - h;
      num_flushes = global->full_flushes[rev_h] + global->count_flushes[rev_h];
      avg_flush_wait_time = num_flushes == 0 ? 0 : global->flush_wait_time_ns[rev_h] / num_flushes;
      avg_flush_time = num_flushes == 0 ? 0 : global->flush_time_ns[rev_h] / num_flushes;
      platform_log("%8u | %18lu | %19lu | %19lu | %12lu | %13lu |\n",
                   rev_h, avg_flush_wait_time, avg_flush_time,
                   global->flush_time_max_ns[rev_h],
                   global->full_flushes[rev_h], global->count_flushes[rev_h]);
   }
   platform_log("---------------------------------------------------------------------------------------------------------\n");
   platform_log("\n");

   platform_log("Compaction Statistics\n");
   platform_log("------------------------------------------------------------------------------------------------------------------------------------------\n");
   platform_log("  height | compactions | avg setup time (ns) | time / tuple (ns) | avg tuples | max tuples | max time (ns) | empty | aborted | discarded |\n");
   platform_log("---------|-------------|---------------------|-------------------|------------|------------|---------------|-------|---------|-----------|\n");

   avg_setup_time = global->root_compactions == 0 ? 0
      : (global->root_compaction_time_ns - global->root_compaction_pack_time_ns)
            / global->root_compactions;
   avg_compaction_tuples = global->root_compactions == 0 ? 0
      : global->root_compaction_tuples / global->root_compactions;
   pack_time_per_tuple = global->root_compaction_tuples == 0 ? 0
      : global->root_compaction_pack_time_ns / global->root_compaction_tuples;
   platform_log("    root | %11lu | %19lu | %17lu | %10lu | %10lu | %13lu | %5lu | %2lu | %2lu | %3lu | %3lu |\n",
         global->root_compactions, avg_setup_time, pack_time_per_tuple,
         avg_compaction_tuples, global->root_compaction_max_tuples,
         global->root_compaction_time_max_ns, 0UL, 0UL, 0UL, 0UL, 0UL);
   for (h = 1; h <= height; h++) {
      rev_h = height - h;
      avg_setup_time = global->compactions[rev_h] == 0 ? 0
         : (global->compaction_time_ns[rev_h] + global->compaction_time_wasted_ns[rev_h]
               - global->compaction_pack_time_ns[rev_h])
               / global->compactions[rev_h];
      avg_compaction_tuples = global->compactions[rev_h] == 0 ? 0
         : global->compaction_tuples[rev_h] / global->compactions[rev_h];
      pack_time_per_tuple = global->compaction_tuples[rev_h] == 0 ? 0
         : global->compaction_pack_time_ns[rev_h] / global->compaction_tuples[rev_h];
      platform_log("%8u | %11lu | %19lu | %17lu | %10lu | %10lu | %13lu | %5lu | %2lu | %2lu | %3lu | %3lu |\n",
            rev_h, global->compactions[rev_h], avg_setup_time, pack_time_per_tuple,
            avg_compaction_tuples, global->compaction_max_tuples[rev_h],
            global->compaction_time_max_ns[rev_h], global->compactions_empty[rev_h],
            global->compactions_aborted_flushed[rev_h], global->compactions_aborted_leaf_split[rev_h],
            global->compactions_discarded_flushed[rev_h], global->compactions_discarded_leaf_split[rev_h]);
   }
   platform_log("------------------------------------------------------------------------------------------------------------------------------------------\n");
   platform_log("\n");

   if (global->leaf_splits == 0) {
      avg_leaves_created = zero_fraction;
   } else {
      avg_leaves_created = init_fraction(
            global->leaf_splits_leaves_created + global->leaf_splits,
            global->leaf_splits
      );
   }
   uint64 leaf_avg_split_time = global->leaf_splits == 0 ? 0
      : global->leaf_split_time_ns / global->leaf_splits;

   platform_log("Leaf Split Statistics\n");
   platform_log("-------------------------------------------------------------------------------\n");
   platform_log(" leaf splits | avg leaves created | avg split time (ns) | max split time (ns) |\n");
   platform_log("-------------|--------------------|---------------------|---------------------|\n");
   platform_log(" %11lu | "FRACTION_FMT(18, 2)" | %19lu | %19lu\n",
         global->leaf_splits, FRACTION_ARGS(avg_leaves_created),
         leaf_avg_split_time, global->leaf_split_max_time_ns);
   platform_log("------------------------------------------------------------------------------|\n");
   platform_log("\n");

   platform_log("Filter Build Statistics\n");
   platform_log("---------------------------------------------------------------------------------\n");
   platform_log("  height |   built | avg tuples | avg build time (ns) | build_time / tuple (ns) |\n");
   platform_log("---------|---------|------------|---------------------|-------------------------|\n");

   avg_filter_tuples = global->root_filters_built == 0 ? 0 :
      global->root_filter_tuples / global->root_filters_built;
   avg_filter_time = global->root_filters_built == 0 ? 0 :
      global->root_filter_time_ns / global->root_filters_built;
   filter_time_per_tuple = global->root_filter_tuples == 0 ? 0 :
      global->root_filter_time_ns / global->root_filter_tuples;

   platform_log("    root | %7lu | %10lu | %19lu | %23lu |\n",
         global->root_filters_built, avg_filter_tuples,
         avg_filter_time, filter_time_per_tuple);
   for (h = 1; h <= height; h++) {
      rev_h = height - h;
      avg_filter_tuples = global->filters_built[rev_h] == 0 ? 0 :
         global->filter_tuples[rev_h] / global->filters_built[rev_h];
      avg_filter_time = global->filters_built[rev_h] == 0 ? 0 :
         global->filter_time_ns[rev_h] / global->filters_built[rev_h];
      filter_time_per_tuple = global->filter_tuples[rev_h] == 0 ? 0 :
         global->filter_time_ns[rev_h] / global->filter_tuples[rev_h];
      platform_log("%8u | %7lu | %10lu | %19lu | %23lu |\n",
            rev_h, global->filters_built[rev_h], avg_filter_tuples,
            avg_filter_time, filter_time_per_tuple);
   }
   platform_log("--------------------------------------------------------------------------------|\n");
   platform_default_log("\n");

   platform_log("Space Reclamation Statistics\n");
   platform_log("------------------------------------------------------------------------------------\n");
   platform_log("| height | space recs | tuples reclaimed in sr | tuples reclaimed | tuples per rec |\n");
   platform_log("|--------|------------|------------------------|------------------|----------------|\n");

   for (h = 1; h <= height; h++) {
      rev_h = height - h;
      uint64 avg_tuples_per_sr = global->space_recs[rev_h] == 0 ?
         0 : global->space_rec_tuples_reclaimed[rev_h] / global->space_recs[rev_h];
      platform_log("| %6u | %10lu | %22lu | %16lu | %14lu |\n",
            rev_h, global->space_recs[rev_h],
            global->space_rec_tuples_reclaimed[rev_h],
            global->tuples_reclaimed[rev_h], avg_tuples_per_sr);
   }
   platform_log("------------------------------------------------------------------------------------\n");
   task_print_stats(spl->ts);
   platform_default_log("\n");
   platform_free(spl->heap_id, global);
}


void
splinter_print_lookup_stats(splinter_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log("Statistics are not enabled\n");
      return;
   }

   threadid thr_i;
   uint32 h, rev_h;
   uint64 lookups;
   fraction avg_filter_lookups, avg_filter_false_positives, avg_branch_lookups;
   page_handle *node = splinter_node_get(spl, spl->root_addr);
   uint32 height = splinter_height(spl, node);
   splinter_node_unget(spl, &node);

   splinter_stats *global;

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

   platform_log("Overall Statistics\n");
   platform_log("-----------------------------------------------------------------------------------\n");
   platform_log("| height:            %u\n", height);
   platform_log("| lookups:           %lu\n", lookups);
   platform_log("| lookups found:     %lu\n", global->lookups_found);
   platform_log("| lookups not found: %lu\n", global->lookups_not_found);
   platform_log("-----------------------------------------------------------------------------------\n");
   platform_log("\n");

   platform_log("Filter/Branch Statistics\n");
   platform_log("-------------------------------------------------------------------------------------\n");
   platform_log("height   | avg filter lookups | avg false pos | false pos rate | avg branch lookups |\n");
   platform_log("---------|--------------------|---------------|----------------|--------------------|\n");

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
      platform_log("%8u | "FRACTION_FMT(18, 2)" | "FRACTION_FMT(13, 4)" | "
                   FRACTION_FMT(14, 4)" | "FRACTION_FMT(18, 4)"\n",
                   rev_h, FRACTION_ARGS(avg_filter_lookups),
                   FRACTION_ARGS(avg_filter_false_positives),
                   FRACTION_ARGS(false_positives_in_revision),
                   FRACTION_ARGS(avg_branch_lookups));
   }
   platform_log("------------------------------------------------------------------------------------|\n");
   platform_default_log("\n");
   platform_free(spl->heap_id, global);
}


void
splinter_print_lookup(splinter_handle *spl,
                      char            *key)
{
   bool found = FALSE;
   char data[MAX_MESSAGE_SIZE];

   // FIXME: [yfogel 2020-08-27] old existing race regarding root/...
   // look in memtables
   // if we want to point out it's just crappy debug code it needs comments saying that
   uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
   uint64 mt_gen_end = memtable_generation_retired(spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      bool memtable_is_compacted;
      uint64 root_addr = splinter_memtable_root_addr_for_lookup(spl, mt_gen,
            &memtable_is_compacted);

      btree_lookup(spl->cc, &spl->cfg.btree_cfg, root_addr, key, data, &found);
      if (found) {
         char key_str[128];
         char message_str[128];
         splinter_key_to_string(spl, key, key_str);
         splinter_message_to_string(spl, data, message_str);
         platform_log("Key %s found in memtable %lu (gen %lu comp %d) with data %s\n",
                      key_str,
                      root_addr,
                      mt_gen,
                      memtable_is_compacted,
                      message_str);
         btree_print_lookup(spl->cc, &spl->cfg.btree_cfg, root_addr,
                            PAGE_TYPE_MEMTABLE, key);
      }
      found = FALSE;
   }

   page_handle *node = splinter_node_get(spl, spl->root_addr);
   uint16 height = splinter_height(spl, node);
   for (uint16 h = height; h > 0; h--) {
      splinter_print_locked_node(spl, node, PLATFORM_DEFAULT_LOG_HANDLE);
      uint16 pivot_no = splinter_find_pivot(spl, node, key, less_than_or_equal);
      debug_assert(pivot_no < splinter_num_children(spl, node));
      splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, pivot_no);
      splinter_pivot_lookup(spl, node, pdata, key, data, &found);
      if (found) {
         char key_str[128];
         char message_str[128];
         splinter_key_to_string(spl, key, key_str);
         splinter_message_to_string(spl, data, message_str);
         platform_log("Key %s found in node %lu pivot %u with data %s\n",
                      key_str, node->disk_addr, pivot_no, message_str);
         found = FALSE;
      } else {
         for (uint16 branch_no = pdata->start_branch;
              branch_no != splinter_end_branch(spl, node);
              branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
            splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
            splinter_btree_lookup(spl, branch, key, data, &found);
            if (found) {
               char key_str[128];
               char message_str[128];
               splinter_key_to_string(spl, key, key_str);
               splinter_message_to_string(spl, data, message_str);
               platform_log("!! Key %s found in branch %u of node %lu pivot %u "
                            "with data %s\n",
                            key_str, branch_no, node->disk_addr, pivot_no,
                            message_str);
            }
            found = FALSE;
         }
      }
      page_handle *child = splinter_node_get(spl, pdata->addr);
      splinter_node_unget(spl, &node);
      node = child;
   }

   // look in leaf
   splinter_print_locked_node(spl, node, PLATFORM_DEFAULT_LOG_HANDLE);
   splinter_pivot_data *pdata = splinter_get_pivot_data(spl, node, 0);
   splinter_pivot_lookup(spl, node, pdata, key, data, &found);
   if (found) {
      char key_str[128];
      char message_str[128];
      splinter_key_to_string(spl, key, key_str);
      splinter_message_to_string(spl, data, message_str);
      platform_log("Key %s found in node %lu pivot %u with data %s\n",
                   key_str, node->disk_addr, 0, message_str);
      found = FALSE;
   } else {
      for (uint16 branch_no = pdata->start_branch;
           branch_no != splinter_end_branch(spl, node);
           branch_no = splinter_branch_no_add(spl, branch_no, 1)) {
         splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
         splinter_btree_lookup(spl, branch, key, data, &found);
         if (found) {
            char key_str[128];
            char message_str[128];
            splinter_key_to_string(spl, key, key_str);
            splinter_message_to_string(spl, data, message_str);
            platform_log("!! Key %s found in branch %u of node %lu pivot %u "
                  "with data %s\n",
                  key_str, branch_no, node->disk_addr, 0, message_str);
         }
      }
   }
   splinter_node_unget(spl, &node);
}

void
splinter_reset_stats(splinter_handle *spl)
{
   if (spl->cfg.use_stats) {
      memset(spl->stats, 0, MAX_THREADS * sizeof(splinter_stats));
   }
}

uint64
splinter_branch_count_num_tuples(splinter_handle *spl,
                                 page_handle     *node,
                                 uint16           branch_no)
{
   uint16 num_children = splinter_num_children(spl, node);
   uint64 num_tuples = 0;
   for (uint16 pivot_no = 0; pivot_no < num_children; pivot_no++) {
      if (splinter_branch_live_for_pivot(spl, node, branch_no, pivot_no)) {
         num_tuples +=
            splinter_pivot_tuples_in_branch(spl, node, pivot_no, branch_no);
      }
   }
   return num_tuples;
}

uint64
splinter_branch_extent_count(splinter_handle *spl,
                             page_handle     *node,
                             uint16           branch_no)
{
   splinter_branch *branch = splinter_get_branch(spl, node, branch_no);
   return btree_extent_count(spl->cc, &spl->cfg.btree_cfg, branch->root_addr);
}

bool
splinter_node_print_branches(splinter_handle *spl,
                             uint64           addr,
                             void            *arg)
{
   page_handle *node = splinter_node_get(spl, addr);

   platform_log("------------------------------------------------------------------\n");
   platform_log("| node %12lu height %u\n", addr, splinter_height(spl, node));
   platform_log("------------------------------------------------------------------\n");
   uint16 num_pivot_keys = splinter_num_pivot_keys(spl, node);
   platform_log("| pivots:\n");
   for (uint16 pivot_no = 0; pivot_no < num_pivot_keys; pivot_no++) {
      char key_str[128];
      splinter_key_to_string(spl, splinter_get_pivot(spl, node, pivot_no), key_str);
      platform_log("| %u: %s\n", pivot_no, key_str);
   }

   platform_log("--------------------------------------------------------------------\n");
   platform_log("| branch |     addr     |  num tuples  |    space    |  space amp  |\n");
   platform_log("--------------------------------------------------------------------\n");
   uint16 start_branch = splinter_start_branch(spl, node);
   uint16 end_branch = splinter_end_branch(spl, node);
   for (uint16 branch_no = start_branch;
        branch_no != end_branch;
        branch_no = splinter_branch_no_add(spl, branch_no, 1))
   {
      uint64 addr = splinter_get_branch(spl, node, branch_no)->root_addr;
      uint64 num_tuples_in_branch = splinter_branch_count_num_tuples(spl, node, branch_no);
      uint64 kib_in_branch = splinter_branch_extent_count(spl, node, branch_no);
      kib_in_branch *= spl->cfg.extent_size / 1024;
      fraction space_amp = init_fraction(kib_in_branch * 1024,
            num_tuples_in_branch * (spl->cfg.data_cfg->key_size +
               spl->cfg.data_cfg->message_size));
      platform_log("| %6u | %12lu | %12lu | %8luKiB |   "FRACTION_FMT(2,2)"   |\n",
            branch_no, addr, num_tuples_in_branch, kib_in_branch,
            FRACTION_ARGS(space_amp));
   }
   platform_log("------------------------------------------------------------------\n");
   platform_log("\n");
   splinter_node_unget(spl, &node);
   return TRUE;
}

void
splinter_print_branches(splinter_handle *spl)
{
   splinter_for_each_node(spl, splinter_node_print_branches, NULL);
}

bool
splinter_node_print_extent_count(splinter_handle *spl,
                                 uint64           addr,
                                 void            *arg)
{
   page_handle *node = splinter_node_get(spl, addr);

   uint16 start_branch = splinter_start_branch(spl, node);
   uint16 end_branch = splinter_end_branch(spl, node);
   uint64 num_extents = 0;
   for (uint16 branch_no = start_branch;
        branch_no != end_branch;
        branch_no = splinter_branch_no_add(spl, branch_no, 1))
   {
      num_extents += splinter_branch_extent_count(spl, node, branch_no);
   }
   platform_log("%8lu\n", num_extents);
   splinter_node_unget(spl, &node);
   return TRUE;
}

void
splinter_print_extent_counts(splinter_handle *spl)
{
   platform_log("extent counts:\n");
   splinter_for_each_node(spl, splinter_node_print_extent_count, NULL);
}

/*
 *-----------------------------------------------------------------------------
 *
 * splinter_config_init --
 *
 *       Initialize splinter config
 *       This function calls btree_config_init
 *
 *-----------------------------------------------------------------------------
 */

void
splinter_config_init(splinter_config *splinter_cfg,
                     data_config     *data_cfg,
                     log_config      *log_cfg,
                     uint64           memtable_capacity,
                     uint64           fanout,
                     uint64           max_branches_per_node,
                     uint64           btree_rough_count_height,
                     uint64           page_size,
                     uint64           extent_size,
                     uint64           filter_remainder_size,
                     uint64           filter_index_size,
                     uint64           reclaim_threshold,
                     uint64           use_log,
                     uint64           use_stats)
{
   uint64 splinter_pivot_size;
   uint64 bytes_for_branches;
   routing_config *index_filter_cfg = &splinter_cfg->index_filter_cfg;
   routing_config *leaf_filter_cfg = &splinter_cfg->leaf_filter_cfg;

   ZERO_CONTENTS(splinter_cfg);
   // FIXME: [yfogel 2020-07-01] need to change splinter_cfg->data_cfg to a
   //       struct from pointer (parameter to func here is still pointer)
   splinter_cfg->data_cfg = data_cfg;

   // Calculate range data config from point data config.
   // FIXME: [yfogel 2020-07-01] will we want this as a function (for tests?)
   {
      splinter_cfg->range_data_cfg = (data_config) {
         .key_size = data_cfg->key_size,
         .message_size = data_cfg->key_size, // data IS a key
         // C Does not allow us to copy arrays; use a memmove outside
         //.min_key = data_cfg->min_key,
         //.max_key = data_cfg->max_key,
         .key_compare = data_cfg->key_compare,
         .key_to_string = data_cfg->key_to_string,
         .message_to_string = data_cfg->key_to_string, // data IS a key
         // FIXME: [yfogel 2020-07-01] These NULLs should be replaced by
         //     stub implementations that immediate panic.
         //     That will be easier to debug/understand it's intentional
         //     and describing a massive bug rather than a crash you have
         //     to reverse engineer
         .merge_tuples = NULL,
         .merge_tuples_final = NULL,
         .message_class = NULL,
         .clobber_message_with_range_delete = NULL,
      };
      memmove(splinter_cfg->range_data_cfg.min_key,
              data_cfg->min_key, sizeof(data_cfg->min_key));
      memmove(splinter_cfg->range_data_cfg.max_key,
              data_cfg->max_key, sizeof(data_cfg->max_key));
   }

   splinter_cfg->log_cfg = log_cfg;

   splinter_cfg->page_size             = page_size;
   splinter_cfg->extent_size           = extent_size;
   splinter_cfg->fanout                = fanout;
   splinter_cfg->max_branches_per_node = max_branches_per_node;
   splinter_cfg->reclaim_threshold     = reclaim_threshold;
   splinter_cfg->use_log               = use_log;
   splinter_cfg->use_stats             = use_stats;

   splinter_pivot_size = data_cfg->key_size + splinter_pivot_message_size();
   // Setting hard limit and over overprovisioning
   splinter_cfg->max_pivot_keys = splinter_cfg->fanout + 6;
   bytes_for_branches = splinter_cfg->page_size - splinter_trunk_hdr_size()
      - splinter_cfg->max_pivot_keys * splinter_pivot_size;
   splinter_cfg->hard_max_branches_per_node
      = bytes_for_branches / sizeof(splinter_branch) - 1;

   // Initialize point message btree
   btree_config_init(&splinter_cfg->btree_cfg,
                     splinter_cfg->data_cfg,
                     data_type_point,
                     btree_rough_count_height,
                     splinter_cfg->page_size, splinter_cfg->extent_size);
   // Initialize range delete btree
   // FIXME: [yfogel 2020-07-01] replace NULL with panic-immediately
   //        hash function
   btree_config_init(&splinter_cfg->range_btree_cfg,
                     &splinter_cfg->range_data_cfg,
                     data_type_range,
                     btree_rough_count_height,
                     splinter_cfg->page_size, splinter_cfg->extent_size);

   // FIXME: [yfogel 2020-08-27] MUST not be hardcoded for
   //    SPLINTER_NUM_MEMTABLES
   //    tests need to specify and that must happen HERE
   //    so must be plumbed
   memtable_config_init(&splinter_cfg->mt_cfg, &splinter_cfg->btree_cfg,
         SPLINTER_NUM_MEMTABLES, memtable_capacity);

   // Has to be set after btree_config_init is called
   // FIXME: [yfogel 2020-07-01] re-evaluate if this calc needs to take
   //        into account the range delete tree? (2x? 1x? what?)
   splinter_cfg->max_tuples_per_node
      = splinter_cfg->fanout * splinter_cfg->mt_cfg.max_tuples_per_memtable;
   splinter_cfg->target_leaf_tuples = splinter_cfg->max_tuples_per_node / 2;

   // filter config settings
   index_filter_cfg->page_size = page_size;
   leaf_filter_cfg->page_size  = page_size;
   index_filter_cfg->extent_size = extent_size;
   leaf_filter_cfg->extent_size  = extent_size;

   index_filter_cfg->index_size = filter_index_size;
   index_filter_cfg->seed       = 42;
   index_filter_cfg->hash       = splinter_cfg->data_cfg->key_hash;
   index_filter_cfg->data_cfg   = splinter_cfg->data_cfg;
   index_filter_cfg->log_index_size =
      31 - __builtin_clz(index_filter_cfg->index_size);
   memmove(leaf_filter_cfg, index_filter_cfg, sizeof(*leaf_filter_cfg));

   uint64 filter_max_fingerprints = splinter_cfg->max_tuples_per_node;
   uint64 filter_quotient_size = 64 - __builtin_clzll(filter_max_fingerprints);
   uint64 filter_fingerprint_size = filter_remainder_size + filter_quotient_size;
   index_filter_cfg->fingerprint_size = filter_fingerprint_size;
   leaf_filter_cfg->fingerprint_size = filter_fingerprint_size;
   uint64 max_value = splinter_cfg->max_branches_per_node;
   size_t max_value_size = 64 - __builtin_clzll(max_value);
   if (filter_fingerprint_size > 32 - max_value_size) {
      platform_error_log("Fingerprint size %lu too large, max value size is %lu, "
                         "setting to %lu\n",
                         filter_fingerprint_size, max_value_size,
                         32 - max_value_size);
      index_filter_cfg->fingerprint_size = 32 - max_value_size;
      leaf_filter_cfg->fingerprint_size = 32 - max_value_size;
   }

   platform_log("fingerprint_size: %u\n", leaf_filter_cfg->fingerprint_size);

   /*
    * Set filter index size
    *
    * In quick_filter_init() we have this assert:
    *   index / addrs_per_page < cfg->extent_size / cfg->page_size
    * where
    *   - cfg is of type quick_filter_config
    *   - index is less than num_indices, which equals to params.num_buckets /
    *     cfg->index_size. params.num_buckets should be less than
    *     splinter_cfg.max_tuples_per_node
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
   uint64 addrs_per_page = splinter_cfg->page_size / sizeof(uint64);
   uint64 pages_per_extent = splinter_cfg->extent_size /
      splinter_cfg->page_size;
   while (leaf_filter_cfg->index_size <= splinter_cfg->max_tuples_per_node /
       (addrs_per_page * pages_per_extent)) {
      platform_error_log("filter-index-size: %u is too small, "
                         "setting to %u\n",
                         leaf_filter_cfg->index_size, leaf_filter_cfg->index_size * 2);
      leaf_filter_cfg->index_size *= 2;
      leaf_filter_cfg->log_index_size++;
   }
}

size_t
splinter_get_scratch_size()
{
   return sizeof(splinter_task_scratch);
}

