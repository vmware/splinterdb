// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinter.h --
 *
 *     This file contains the interface for SplinterDB.
 */

#ifndef __SPLINTER_H
#define __SPLINTER_H

#include "data.h"
#include "btree.h"
#include "memtable.h"
#include "routing_filter.h"
#include "cache.h"
#include "iterator.h"
#include "merge.h"
#include "allocator.h"
#include "log.h"
#include "srq.h"

#define SPLINTER_MAX_HEIGHT 8
// FIXME: [yfogel 2020-07-02] 72 * 2 > 128
// examin cost of increasing
#define SPLINTER_MAX_TOTAL_DEGREE 256

/*
 *----------------------------------------------------------------------
 *
 * splinter --
 *
 *       TODO: describe the structure here a bit or something
 *
 *----------------------------------------------------------------------
 */

typedef struct splinter_config {
   // robj: if these are redundant, maybe delete them?
   uint64               page_size;               // must match the cache/fs page_size
   uint64               extent_size;             // same

   // parameters
   uint64               fanout;                  // children to trigger split
   uint64               max_pivot_keys;          // hard limit on number of pivot keys
   uint64               max_tuples_per_node;
   uint64               max_branches_per_node;
   uint64               hard_max_branches_per_node;
   uint64               target_leaf_tuples;      // make leaves this big when splitting
   uint64               reclaim_threshold;       // start reclaming space when free space < threshold

   // stats
   bool                 use_stats;

   memtable_config      mt_cfg;
   btree_config         btree_cfg;
   routing_config       index_filter_cfg;
   routing_config       leaf_filter_cfg;

   data_config *data_cfg;

   bool                 use_log;
   log_config          *log_cfg;
} splinter_config;

typedef struct splinter_stats {
   uint64 insertions;
   uint64 updates;
   uint64 deletions;

   platform_histo_handle insert_latency_histo;
   platform_histo_handle update_latency_histo;
   platform_histo_handle delete_latency_histo;

   uint64 flush_wait_time_ns[SPLINTER_MAX_HEIGHT];
   uint64 flush_time_ns[SPLINTER_MAX_HEIGHT];
   uint64 flush_time_max_ns[SPLINTER_MAX_HEIGHT];
   uint64 full_flushes[SPLINTER_MAX_HEIGHT];
   uint64 count_flushes[SPLINTER_MAX_HEIGHT];
   uint64 memtable_flushes;
   uint64 memtable_flush_time_ns;
   uint64 memtable_flush_time_max_ns;
   uint64 memtable_flush_wait_time_ns;
   uint64 memtable_flush_root_full;
   uint64 root_full_flushes;
   uint64 root_count_flushes;
   uint64 root_flush_time_ns;
   uint64 root_flush_time_max_ns;
   uint64 root_flush_wait_time_ns;
   uint64 failed_flushes[SPLINTER_MAX_HEIGHT];
   uint64 root_failed_flushes;
   uint64 memtable_failed_flushes;

   uint64 compactions[SPLINTER_MAX_HEIGHT];
   uint64 compactions_aborted_flushed[SPLINTER_MAX_HEIGHT];
   uint64 compactions_aborted_leaf_split[SPLINTER_MAX_HEIGHT];
   uint64 compactions_discarded_flushed[SPLINTER_MAX_HEIGHT];
   uint64 compactions_discarded_leaf_split[SPLINTER_MAX_HEIGHT];
   uint64 compactions_empty[SPLINTER_MAX_HEIGHT];
   uint64 compaction_tuples[SPLINTER_MAX_HEIGHT];
   uint64 compaction_max_tuples[SPLINTER_MAX_HEIGHT];
   uint64 compaction_time_ns[SPLINTER_MAX_HEIGHT];
   uint64 compaction_time_max_ns[SPLINTER_MAX_HEIGHT];
   uint64 compaction_time_wasted_ns[SPLINTER_MAX_HEIGHT];
   uint64 compaction_pack_time_ns[SPLINTER_MAX_HEIGHT];

   uint64 root_compactions;
   uint64 root_compaction_pack_time_ns;
   uint64 root_compaction_tuples;
   uint64 root_compaction_max_tuples;
   uint64 root_compaction_time_ns;
   uint64 root_compaction_time_max_ns;

   uint64 discarded_deletes;
   uint64 index_splits;
   uint64 leaf_splits;
   uint64 leaf_splits_leaves_created;
   uint64 leaf_split_time_ns;
   uint64 leaf_split_max_time_ns;

   uint64 root_filters_built;
   uint64 root_filter_tuples;
   uint64 root_filter_time_ns;
   uint64 filters_built[SPLINTER_MAX_HEIGHT];
   uint64 filter_tuples[SPLINTER_MAX_HEIGHT];
   uint64 filter_time_ns[SPLINTER_MAX_HEIGHT];

   uint64 lookups_found;
   uint64 lookups_not_found;
   uint64 filter_lookups[SPLINTER_MAX_HEIGHT];
   uint64 branch_lookups[SPLINTER_MAX_HEIGHT];
   uint64 filter_false_positives[SPLINTER_MAX_HEIGHT];
   uint64 filter_negatives[SPLINTER_MAX_HEIGHT];

   uint64 space_recs[SPLINTER_MAX_HEIGHT];
   uint64 space_rec_time_ns[SPLINTER_MAX_HEIGHT];
   uint64 space_rec_tuples_reclaimed[SPLINTER_MAX_HEIGHT];
   uint64 tuples_reclaimed[SPLINTER_MAX_HEIGHT];
} PLATFORM_CACHELINE_ALIGNED splinter_stats;

// splinter refers to btrees as branches
typedef struct splinter_branch {
   // FIXME: [yfogel 2020-07-01] need to rename root_addr to point_root_addr
   //                            delaying the rename to unblock parallelism
   uint64 root_addr; // root address of point btree
} splinter_branch;

typedef struct splinter_handle splinter_handle;
typedef struct splinter_compact_bundle_req splinter_compact_bundle_req;

typedef struct splinter_memtable_args {
   splinter_handle *spl;
   uint64           generation;
} splinter_memtable_args;

typedef struct splinter_compacted_memtable {
   splinter_branch              branch;
   routing_filter               filter;
   timestamp                    wait_start;
   splinter_memtable_args       mt_args;
   splinter_compact_bundle_req *req;
} splinter_compacted_memtable;

struct splinter_handle {
   uint64             root_addr;
   uint64             super_block_idx;
   splinter_config    cfg;
   platform_heap_id   heap_id;

   // space reclamation
   uint64             est_tuples_in_compaction;

   // allocator/cache/log
   allocator         *al;
   cache             *cc;
   log_handle        *log;
   mini_allocator     mini;

   // memtables
   allocator_root_id        id;
   memtable_context  *mt_ctxt;

   // task system
   task_system       *ts;  // ALEX: currently not durable

   // stats
   splinter_stats    *stats;

   // Link inside the splinter list
   List_Links         links;

   /*
    * Per thread task and per splinter table task counter. Used to decide when
    * to run tasks.
    */

   struct {
      uint64 counter;
   } PLATFORM_CACHELINE_ALIGNED task_countup[MAX_THREADS];

   // space rec queue
   srq                srq;

   splinter_compacted_memtable compacted_memtable[/*cfg.mt_cfg.max_memtables*/];
};

typedef struct splinter_range_iterator {
   iterator         super;
   splinter_handle *spl;
   uint64           num_tuples;
   uint64           num_branches;
   uint64           num_memtable_branches;
   uint64           memtable_start_gen;
   uint64           memtable_end_gen;
   bool             compacted[SPLINTER_MAX_TOTAL_DEGREE];
   page_handle     *meta_page[SPLINTER_MAX_TOTAL_DEGREE];
   merge_iterator  *merge_itor;
   bool             has_max_key;
   bool             at_end;
   char             min_key[MAX_KEY_SIZE];
   char             max_key[MAX_KEY_SIZE];
   char             local_max_key[MAX_KEY_SIZE];
   char             rebuild_key[MAX_KEY_SIZE];
   // FIXME: [yfogel 2020-07-01] we are definitely using (up to) double
   // the space in btree_itor (with range deletes)
   // FIXME: [yfogel 2020-07-01] task for during/after range query
   //       increase MAX_TOTAL_DEGREE?? in theory may not be enough
   //       after doubling but probably still fine
   //       only 128 now... but ...
   //       this is probably not fine RIGHT NOW in the worst case.
   //       if we increase significantly (or increase dynamically allcoated)
   //       we probably need to be careful about initialization
   //       to not ahve to pay cost of zeroing massive amounts of data
   btree_iterator   btree_itor[SPLINTER_MAX_TOTAL_DEGREE];
   // FIXME: [yfogel 2020-07-01] this won't need to change
   splinter_branch  branch[SPLINTER_MAX_TOTAL_DEGREE];

   // used for merge iterator construction
   iterator        *itor[SPLINTER_MAX_TOTAL_DEGREE];
} splinter_range_iterator;


// FIXME: [yfogel 2020-08-26] will need some extra states
typedef enum {
   async_state_start,
   async_state_lookup_memtable,
   async_state_get_root_reentrant,
   async_state_trunk_node_lookup,
   async_state_subbundle_lookup,
   async_state_pivot_lookup,
   async_state_filter_lookup_start,
   async_state_filter_lookup_reentrant,
   async_state_btree_lookup_start,
   async_state_btree_lookup_reentrant,
   async_state_next_in_node,
   async_state_trunk_node_done,
   async_state_get_child_trunk_node_reentrant,
   async_state_unget_parent_trunk_node,
   async_state_found_final_answer_early,
   async_state_end
} splinter_async_state;

typedef enum {
   async_lookup_state_pivot,
   async_lookup_state_subbundle,
   async_lookup_state_compacted_subbundle
} splinter_async_lookup_state;

struct splinter_async_ctxt;
struct splinter_pivot_data;
struct splinter_subbundle;

typedef void (*splinter_async_cb)(struct splinter_async_ctxt *ctxt);
typedef struct splinter_async_ctxt {
   splinter_async_cb            cb;            // IN: callback (requeues ctxt
                                              // for dispatch)
   // These fields are internal
   splinter_async_state         prev_state;    // state machine's previous state
   splinter_async_state         state;         // state machine's current state
   page_handle                 *mt_lock_page;  // Memtable lock page
   page_handle                 *trunk_node;    // Current trunk node
   uint16                       height;        // height of trunk_node

   uint16                       sb_no;         // subbundle number (newest)
   uint16                       end_sb_no;     // subbundle number (oldest,
                                               // exclusive
   uint16                       filter_no;     // sb filter no

   splinter_async_lookup_state  lookup_state;  // Can be pivot or
                                               // [compacted] subbundle
   struct splinter_subbundle   *sb;            // Subbundle
   struct splinter_pivot_data  *pdata;         // Pivot data for next trunk node
   routing_filter              *filter;        // Filter for subbundle or pivot
   uint64                       found_values;  // values found in filter
   uint16                       value;         // Current value found in filter

   uint16                       branch_no;     // branch number (newest)
   uint16                       branch_no_end; // branch number end (oldest,
                                               // exclusive)
   bool                         found;         // If found the key
   message_type                 type;          // data_class (if found==TRUE)
   bool                         was_async;     // Did an async IO for trunk ?
   splinter_branch             *branch;        // Current branch
   union {
      routing_async_ctxt        filter_ctxt;    // Filter async context
      btree_async_ctxt          btree_ctxt;    // Btree async context
   };
   cache_async_ctxt             cache_ctxt;    // Async cache context
} splinter_async_ctxt;



// FIXME: [nsarmicanic 2020-07-13] fix comment
// FIXME: robj: can we get rid of this entirely?
/*
 * Tests usually allocate a number of pivot keys.
 * Since we can't use VLAs, it's easier to allocate an array of a struct
 * than to malloc a 2d array which requires a loop of some kind (or math to
 * dereference)
 * Define a struct for a key of max size.
 */
typedef struct {
   char k[MAX_KEY_SIZE];
} key_buffer;


/*
 *----------------------------------------------------------------------
 *
 * splinter API
 *
 *----------------------------------------------------------------------
 */

platform_status
splinter_insert(splinter_handle *spl, char *key, char *data);
platform_status
splinter_lookup(splinter_handle *spl, char *key, char *data, bool *found);
cache_async_result
splinter_lookup_async(splinter_handle *    spl,
                      char *               key,
                      char *               data,
                      bool *               found,
                      splinter_async_ctxt *ctxt);
platform_status
splinter_range_iterator_init(splinter_handle *        spl,
                             splinter_range_iterator *range_itor,
                             char *                   min_key,
                             char *                   max_key,
                             uint64                   num_tuples);
void
splinter_range_iterator_deinit(splinter_range_iterator *range_itor);
platform_status
splinter_range(splinter_handle *spl,
               char *           start_key,
               uint64           num_tuples,
               uint64 *         tuples_returned,
               char *           out);

splinter_handle *
splinter_create(splinter_config * cfg,
                allocator *       al,
                cache *           cc,
                task_system *     ts,
                allocator_root_id id,
                platform_heap_id  hid);
void
splinter_destroy(splinter_handle *spl);
splinter_handle *
splinter_mount(splinter_config * cfg,
               allocator *       al,
               cache *           cc,
               task_system *     ts,
               allocator_root_id id,
               platform_heap_id  hid);
void
splinter_dismount(splinter_handle *spl);

void
splinter_perform_tasks(splinter_handle *spl);

void
splinter_force_flush(splinter_handle *spl);
void
splinter_print_insertion_stats(splinter_handle *spl);
void
splinter_print_lookup_stats(splinter_handle *spl);
void
splinter_reset_stats(splinter_handle *spl);

void
splinter_print(splinter_handle *spl);
void
splinter_print_lookup(splinter_handle *spl, char *key);
void
splinter_print_branches(splinter_handle *spl);
void
splinter_print_extent_counts(splinter_handle *spl);
void
splinter_print_space_use(splinter_handle *spl);
bool
splinter_verify_tree(splinter_handle *spl);

static inline uint64
splinter_key_size(splinter_handle *spl)
{
   return spl->cfg.data_cfg->key_size;
}

static inline uint64
splinter_message_size(splinter_handle *spl)
{
   return spl->cfg.data_cfg->message_size;
}

static inline uint64
splinter_tuple_size(splinter_handle *spl)
{
   return splinter_key_size(spl) + splinter_message_size(spl);
}

static inline int
splinter_key_compare(splinter_handle *spl, const char *key1, const char *key2)
{
   return btree_key_compare(&spl->cfg.btree_cfg, key1, key2);
}

static inline void
splinter_key_to_string(splinter_handle *spl,
                       const char *     key,
                       char             str[static 128])
{
   btree_key_to_string(&spl->cfg.btree_cfg, key, str);
}

static inline void
splinter_message_to_string(splinter_handle *spl,
                           const char *     data,
                           char             str[static 128])
{
   btree_message_to_string(&spl->cfg.btree_cfg, data, str);
}

static inline void
splinter_async_ctxt_init(splinter_async_ctxt *ctxt, splinter_async_cb cb)
{
   ZERO_CONTENTS(ctxt);
   ctxt->state = async_state_start;
   ctxt->cb    = cb;
}

uint64
splinter_pivot_size(splinter_handle *spl);

uint64
splinter_pivot_message_size();

uint64
splinter_trunk_hdr_size();

void
splinter_config_init(splinter_config *splinter_cfg,
                     data_config *    data_cfg,
                     log_config *     log_cfg,
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
                     uint64           use_stats);
size_t
splinter_get_scratch_size();


#endif // __SPLINTER_H
