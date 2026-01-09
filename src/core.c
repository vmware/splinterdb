// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * core.c --
 *
 *     This file contains the implementation for SplinterDB.
 */

#include "core.h"
#include "platform_sleep.h"
#include "platform_time.h"
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
#define CORE_NUM_MEMTABLES (4)

/*
 * For a "small" range query, you don't want to prefetch pages.
 * This is the minimal # of items requested before we turn ON prefetching.
 * (Empirically established through past experiments, for small key-value
 * pairs. So, _may_ be less efficient in general cases. Needs a revisit.)
 */
#define CORE_PREFETCH_MIN (16384)

/* Some randomly chosen Splinter super-block checksum seed. */
#define CORE_SUPER_CSUM_SEED (42)

/*
 * core logging functions.
 *
 * If verbose_logging_enabled is enabled in core_config, these functions print
 * to cfg->log_handle.
 */

static inline bool32
core_verbose_logging_enabled(core_handle *spl)
{
   return spl->cfg.verbose_logging_enabled;
}

static inline platform_log_handle *
core_log_handle(core_handle *spl)
{
   platform_assert(core_verbose_logging_enabled(spl));
   platform_assert(spl->cfg.log_handle != NULL);
   return spl->cfg.log_handle;
}

static inline platform_status
core_open_log_stream_if_enabled(core_handle            *spl,
                                platform_stream_handle *stream)
{
   if (core_verbose_logging_enabled(spl)) {
      return platform_open_log_stream(stream);
   }
   return STATUS_OK;
}

static inline void
core_close_log_stream_if_enabled(core_handle            *spl,
                                 platform_stream_handle *stream)
{
   if (core_verbose_logging_enabled(spl)) {
      platform_assert(stream != NULL);
      platform_close_log_stream(stream, core_log_handle(spl));
   }
}

#define core_log_stream_if_enabled(spl, _stream, message, ...)                 \
   do {                                                                        \
      if (core_verbose_logging_enabled(spl)) {                                 \
         platform_log_stream(                                                  \
            (_stream), "[%3lu] " message, platform_get_tid(), ##__VA_ARGS__);  \
      }                                                                        \
   } while (0)

#define core_default_log_if_enabled(spl, message, ...)                         \
   do {                                                                        \
      if (core_verbose_logging_enabled(spl)) {                                 \
         platform_default_log(message, __VA_ARGS__);                           \
      }                                                                        \
   } while (0)

/*
 *-----------------------------------------------------------------------------
 * Splinter Super Block: Disk-resident structure.
 * Super block lives on page of page type == PAGE_TYPE_SUPERBLOCK.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK core_super_block {
   uint64 root_addr; // Address of the root of the trunk for the instance
                     // referenced by this superblock.
   uint64      log_addr;
   uint64      log_meta_addr;
   uint64      timestamp;
   bool32      checkpointed;
   bool32      unmounted;
   checksum128 checksum;
} core_super_block;

/*
 *-----------------------------------------------------------------------------
 * Super block functions
 *-----------------------------------------------------------------------------
 */
static void
core_set_super_block(core_handle *spl,
                     bool32       is_checkpoint,
                     bool32       is_unmount,
                     bool32       is_create)
{
   uint64            super_addr;
   page_handle      *super_page;
   core_super_block *super;
   uint64            wait = 1;
   platform_status   rc;

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

   super                = (core_super_block *)super_page->data;
   uint64 old_root_addr = super->root_addr;

   trunk_ondisk_node_handle root_handle;
   trunk_init_root_handle(&spl->trunk_context, &root_handle);
   uint64 root_addr = trunk_ondisk_node_handle_addr(&root_handle);
   if (root_addr != 0) {
      super->root_addr = root_addr;
      rc               = trunk_inc_ref(spl->cfg.trunk_node_cfg,
                         spl->heap_id,
                         spl->cc,
                         spl->al,
                         spl->ts,
                         super->root_addr);
      platform_assert_status_ok(rc);

   } else {
      super->root_addr = 0;
   }
   trunk_ondisk_node_handle_deinit(&root_handle);

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
                           sizeof(core_super_block) - sizeof(checksum128),
                           CORE_SUPER_CSUM_SEED);

   cache_mark_dirty(spl->cc, super_page);
   cache_unlock(spl->cc, super_page);
   cache_unclaim(spl->cc, super_page);
   cache_unget(spl->cc, super_page);
   cache_page_sync(spl->cc, super_page, TRUE, PAGE_TYPE_SUPERBLOCK);

   if (old_root_addr != 0 && !is_create) {
      rc = trunk_dec_ref(spl->cfg.trunk_node_cfg,
                         spl->heap_id,
                         spl->cc,
                         spl->al,
                         spl->ts,
                         old_root_addr);
      platform_assert_status_ok(rc);
   }
}

static core_super_block *
core_get_super_block_if_valid(core_handle *spl, page_handle **super_page)
{
   uint64            super_addr;
   core_super_block *super;

   platform_status rc = allocator_get_super_addr(spl->al, spl->id, &super_addr);
   platform_assert_status_ok(rc);
   *super_page = cache_get(spl->cc, super_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   super       = (core_super_block *)(*super_page)->data;

   if (!platform_checksum_is_equal(
          super->checksum,
          platform_checksum128(super,
                               sizeof(core_super_block) - sizeof(checksum128),
                               CORE_SUPER_CSUM_SEED)))
   {
      cache_unget(spl->cc, *super_page);
      *super_page = NULL;
      return NULL;
   }

   return super;
}

static void
core_release_super_block(core_handle *spl, page_handle *super_page)
{
   cache_unget(spl->cc, super_page);
}

/*
 *-----------------------------------------------------------------------------
 * Memtable Functions
 *-----------------------------------------------------------------------------
 */

static memtable *
core_try_get_memtable(core_handle *spl, uint64 generation)
{
   uint64    memtable_idx = generation % CORE_NUM_MEMTABLES;
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
core_get_memtable(core_handle *spl, uint64 generation)
{
   uint64    memtable_idx = generation % CORE_NUM_MEMTABLES;
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

static core_compacted_memtable *
core_get_compacted_memtable(core_handle *spl, uint64 generation)
{
   uint64 memtable_idx = generation % CORE_NUM_MEMTABLES;

   // this call asserts the generation is correct
   memtable *mt = core_get_memtable(spl, generation);
   platform_assert(mt->state != MEMTABLE_STATE_READY);

   return &spl->compacted_memtable[memtable_idx];
}

static inline void
core_memtable_inc_ref(core_handle *spl, uint64 mt_gen)
{
   memtable *mt = core_get_memtable(spl, mt_gen);
   allocator_inc_ref(spl->al, mt->root_addr);
}


static void
core_memtable_dec_ref(core_handle *spl, uint64 generation)
{
   memtable *mt = core_get_memtable(spl, generation);
   memtable_dec_ref_maybe_recycle(spl->mt_ctxt, mt);

   // the branch in the compacted memtable is now in the tree, so don't zap it,
   // we don't try to zero out the cmt because that would introduce a race.
}


/*
 * Wrappers for creating/destroying memtable iterators. Increments/decrements
 * the memtable ref count and cleans up if ref count == 0
 */
static void
core_memtable_iterator_init(core_handle    *spl,
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
                       spl->cfg.btree_cfg,
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
core_memtable_iterator_deinit(core_handle    *spl,
                              btree_iterator *itor,
                              uint64          mt_gen,
                              bool32          dec_ref)
{
   btree_iterator_deinit(itor);
   if (dec_ref) {
      core_memtable_dec_ref(spl, mt_gen);
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
core_memtable_insert(core_handle *spl, key tuple_key, message msg)
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
   memtable *mt = core_get_memtable(spl, generation);
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
core_memtable_compact(core_handle *spl, uint64 generation, const threadid tid)
{
   timestamp comp_start = platform_get_timestamp();

   memtable *mt = core_get_memtable(spl, generation);

   memtable_transition(mt, MEMTABLE_STATE_FINALIZED, MEMTABLE_STATE_COMPACTING);
   mini_release(&mt->mini);

   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   core_branch             *new_branch = &cmt->branch;
   ZERO_CONTENTS(new_branch);

   uint64         memtable_root_addr = mt->root_addr;
   btree_iterator btree_itor;
   iterator      *itor = &btree_itor.super;

   core_memtable_iterator_init(spl,
                               &btree_itor,
                               memtable_root_addr,
                               NEGATIVE_INFINITY_KEY,
                               POSITIVE_INFINITY_KEY,
                               NEGATIVE_INFINITY_KEY,
                               greater_than_or_equal,
                               FALSE,
                               FALSE);
   const routing_config *rfcfg = spl->cfg.trunk_node_cfg->filter_cfg;
   uint64 rflimit = routing_filter_max_fingerprints(spl->cfg.cache_cfg, rfcfg);
   btree_pack_req req;
   btree_pack_req_init(&req,
                       spl->cc,
                       spl->cfg.btree_cfg,
                       itor,
                       rflimit,
                       rfcfg->hash,
                       rfcfg->seed,
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

   platform_assert(req.num_tuples <= rflimit);
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_compaction_pack_time_ns +=
         platform_timestamp_elapsed(pack_start);
      spl->stats[tid].root_compaction_tuples += req.num_tuples;
      if (req.num_tuples > spl->stats[tid].root_compaction_max_tuples) {
         spl->stats[tid].root_compaction_max_tuples = req.num_tuples;
      }
   }
   core_memtable_iterator_deinit(spl, &btree_itor, FALSE, FALSE);

   new_branch->root_addr = req.root_addr;

   platform_assert(req.num_tuples > 0);

   btree_pack_req_deinit(&req, spl->heap_id);
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
core_try_start_incorporate(core_handle *spl, uint64 generation)
{
   bool32 should_start = FALSE;

   memtable_lock_incorporation_lock(spl->mt_ctxt);
   memtable *mt = core_try_get_memtable(spl, generation);
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
core_try_continue_incorporate(core_handle *spl, uint64 next_generation)
{
   bool32 should_continue = FALSE;

   memtable_lock_incorporation_lock(spl->mt_ctxt);
   memtable *mt = core_try_get_memtable(spl, next_generation);
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

static void
core_memtable_incorporate(core_handle   *spl,
                          uint64         generation,
                          const threadid tid)
{
   platform_stream_handle stream;
   platform_status        rc = core_open_log_stream_if_enabled(spl, &stream);
   platform_assert_status_ok(rc);
   core_log_stream_if_enabled(
      spl, &stream, "incorporate memtable gen %lu\n", generation);
   core_log_stream_if_enabled(
      spl, &stream, "----------------------------------------\n");

   // Add the memtable to the new root as a new compacted bundle
   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   uint64                   flush_start;
   if (spl->cfg.use_stats) {
      flush_start = platform_get_timestamp();
   }
   rc = trunk_incorporate_prepare(&spl->trunk_context, cmt->branch.root_addr);
   platform_assert_status_ok(rc);
   btree_dec_ref(
      spl->cc, spl->cfg.btree_cfg, cmt->branch.root_addr, PAGE_TYPE_MEMTABLE);
   if (spl->cfg.use_stats) {
      spl->stats[tid].memtable_flush_wait_time_ns +=
         platform_timestamp_elapsed(cmt->wait_start);
   }

   core_log_stream_if_enabled(
      spl, &stream, "----------------------------------------\n");
   core_log_stream_if_enabled(spl, &stream, "\n");

   /*
    * Lock the lookup lock, blocking lookups.
    * Transition memtable state and increment memtable generation (blocks
    * lookups from accessing the memtable that's being incorporated).
    * And switch to the new root of the trunk.
    */
   memtable_block_lookups(spl->mt_ctxt);
   memtable *mt = core_get_memtable(spl, generation);
   // Normally need to hold incorp_mutex, but debug code and also guaranteed no
   // one is changing gen_to_incorp (we are the only thread that would try)
   debug_assert(generation == memtable_generation_to_incorporate(spl->mt_ctxt));
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATION_ASSIGNED, MEMTABLE_STATE_INCORPORATING);
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATING, MEMTABLE_STATE_INCORPORATED);
   memtable_increment_to_generation_retired(spl->mt_ctxt, generation);
   trunk_incorporate_commit(&spl->trunk_context);
   memtable_unblock_lookups(spl->mt_ctxt);

   trunk_incorporate_cleanup(&spl->trunk_context);

   core_close_log_stream_if_enabled(spl, &stream);

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
core_memtable_flush_internal(core_handle *spl, uint64 generation)
{
   const threadid tid = platform_get_tid();
   // pack and build filter.
   core_memtable_compact(spl, generation, tid);

   // If we are assigned to do so, incorporate the memtable onto the root node.
   if (!core_try_start_incorporate(spl, generation)) {
      goto out;
   }
   do {
      core_memtable_incorporate(spl, generation, tid);
      generation++;
   } while (core_try_continue_incorporate(spl, generation));
out:
   return;
}

static void
core_memtable_flush_internal_virtual(void *arg)
{
   core_memtable_args *mt_args = arg;
   core_memtable_flush_internal(mt_args->spl, mt_args->generation);
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
core_memtable_flush(core_handle *spl, uint64 generation)
{
   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   cmt->mt_args.spl             = spl;
   cmt->mt_args.generation      = generation;
   task_enqueue(spl->ts,
                TASK_TYPE_MEMTABLE,
                core_memtable_flush_internal_virtual,
                &cmt->mt_args,
                FALSE);
}

static void
core_memtable_flush_virtual(void *arg, uint64 generation)
{
   core_handle *spl = arg;
   core_memtable_flush(spl, generation);
}

static inline uint64
core_memtable_root_addr_for_lookup(core_handle *spl,
                                   uint64       generation,
                                   bool32      *is_compacted)
{
   memtable *mt = core_get_memtable(spl, generation);
   platform_assert(memtable_ok_to_lookup(mt));

   if (memtable_ok_to_lookup_compacted(mt)) {
      // lookup in packed tree
      *is_compacted = TRUE;
      core_compacted_memtable *cmt =
         core_get_compacted_memtable(spl, generation);
      return cmt->branch.root_addr;
   } else {
      *is_compacted = FALSE;
      return mt->root_addr;
   }
}

/*
 * core_memtable_lookup
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
core_memtable_lookup(core_handle       *spl,
                     uint64             generation,
                     key                target,
                     merge_accumulator *data)
{
   cache *const        cc  = spl->cc;
   btree_config *const cfg = spl->cfg.btree_cfg;
   bool32              memtable_is_compacted;
   uint64              root_addr = core_memtable_root_addr_for_lookup(
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
 * Branch iterator wrapper functions
 */

static void
core_branch_iterator_init(core_handle    *spl,
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
   btree_config *btree_cfg = spl->cfg.btree_cfg;
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
core_branch_iterator_deinit(core_handle    *spl,
                            btree_iterator *itor,
                            bool32          should_dec_ref)
{
   if (itor->root_addr == 0) {
      return;
   }
   cache        *cc        = spl->cc;
   btree_config *btree_cfg = spl->cfg.btree_cfg;
   btree_iterator_deinit(itor);
   if (should_dec_ref) {
      btree_dec_ref(cc, btree_cfg, itor->root_addr, PAGE_TYPE_BRANCH);
   }
}

/*
 *-----------------------------------------------------------------------------
 * Range functions and iterators
 *
 *      core_node_iterator
 *      core_iterator
 *-----------------------------------------------------------------------------
 */
static void
core_range_iterator_curr(iterator *itor, key *curr_key, message *data);
static bool32
core_range_iterator_can_prev(iterator *itor);
static bool32
core_range_iterator_can_next(iterator *itor);
static platform_status
core_range_iterator_next(iterator *itor);
static platform_status
core_range_iterator_prev(iterator *itor);
void
core_range_iterator_deinit(core_range_iterator *range_itor);

const static iterator_ops core_range_iterator_ops = {
   .curr     = core_range_iterator_curr,
   .can_prev = core_range_iterator_can_prev,
   .can_next = core_range_iterator_can_next,
   .next     = core_range_iterator_next,
   .prev     = core_range_iterator_prev,
};

platform_status
core_range_iterator_init(core_handle         *spl,
                         core_range_iterator *range_itor,
                         key                  min_key,
                         key                  max_key,
                         key                  start_key,
                         comparison           start_type,
                         uint64               num_tuples)
{
   debug_assert(!key_is_null(min_key));
   debug_assert(!key_is_null(max_key));
   debug_assert(!key_is_null(start_key));

   range_itor->spl          = spl;
   range_itor->super.ops    = &core_range_iterator_ops;
   range_itor->num_branches = 0;
   range_itor->num_tuples   = num_tuples;
   range_itor->merge_itor   = NULL;
   range_itor->can_prev     = TRUE;
   range_itor->can_next     = TRUE;

   if (core_key_compare(spl, min_key, start_key) > 0) {
      // in bounds, start at min
      start_key = min_key;
   }
   if (core_key_compare(spl, max_key, start_key) <= 0) {
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
      platform_assert((range_itor->num_branches < CORE_RANGE_ITOR_MAX_BRANCHES),
                      "range_itor->num_branches=%lu should be < "
                      " CORE_RANGE_ITOR_MAX_BRANCHES (%d).",
                      range_itor->num_branches,
                      CORE_RANGE_ITOR_MAX_BRANCHES);
      debug_assert(range_itor->num_branches < ARRAY_SIZE(range_itor->branch));

      bool32 compacted;
      uint64 root_addr =
         core_memtable_root_addr_for_lookup(spl, mt_gen, &compacted);
      range_itor->compacted[range_itor->num_branches] = compacted;
      if (compacted) {
         btree_inc_ref(spl->cc, spl->cfg.btree_cfg, root_addr);
      } else {
         core_memtable_inc_ref(spl, mt_gen);
      }

      range_itor->branch[range_itor->num_branches].addr = root_addr;
      range_itor->branch[range_itor->num_branches].type =
         compacted ? PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;
      range_itor->num_branches++;
   }

   trunk_ondisk_node_handle root_handle;
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
                               CORE_RANGE_ITOR_MAX_BRANCHES,
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
   if (core_key_compare(
          spl, key_buffer_key(&range_itor->local_min_key), min_key)
       <= 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_min_key, min_key);
      platform_assert_status_ok(rc);
   }
   if (core_key_compare(
          spl, key_buffer_key(&range_itor->local_max_key), max_key)
       >= 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_max_key, max_key);
      platform_assert_status_ok(rc);
   }

   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      uint64          branch_no   = range_itor->num_branches - i - 1;
      btree_iterator *btree_itor  = &range_itor->btree_itor[branch_no];
      uint64          branch_addr = range_itor->branch[branch_no].addr;
      if (range_itor->compacted[branch_no]) {
         bool32 do_prefetch =
            range_itor->compacted[branch_no] && num_tuples > CORE_PREFETCH_MIN
               ? TRUE
               : FALSE;
         core_branch_iterator_init(spl,
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
         core_memtable_iterator_init(spl,
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
      if (core_key_compare(spl, local_max, max_key) < 0) {
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(spl,
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
      if (core_key_compare(spl, local_min, min_key) > 0) {
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(spl,
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
core_range_iterator_curr(iterator *itor, key *curr_key, message *data)
{
   debug_assert(itor != NULL);
   core_range_iterator *range_itor = (core_range_iterator *)itor;
   iterator_curr(&range_itor->merge_itor->super, curr_key, data);
}

static platform_status
core_range_iterator_next(iterator *itor)
{
   core_range_iterator *range_itor = (core_range_iterator *)itor;
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
      if (core_key_compare(range_itor->spl, local_max_key, max_key) < 0) {
         uint64 temp_tuples = range_itor->num_tuples;
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(range_itor->spl,
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
core_range_iterator_prev(iterator *itor)
{
   core_range_iterator *range_itor = (core_range_iterator *)itor;
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
      if (core_key_compare(range_itor->spl, local_min_key, min_key) > 0) {
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(range_itor->spl,
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
core_range_iterator_can_prev(iterator *itor)
{
   debug_assert(itor != NULL);
   core_range_iterator *range_itor = (core_range_iterator *)itor;

   return range_itor->can_prev;
}

static bool32
core_range_iterator_can_next(iterator *itor)
{
   debug_assert(itor != NULL);
   core_range_iterator *range_itor = (core_range_iterator *)itor;

   return range_itor->can_next;
}

void
core_range_iterator_deinit(core_range_iterator *range_itor)
{
   core_handle *spl = range_itor->spl;
   if (range_itor->merge_itor != NULL) {
      merge_iterator_destroy(range_itor->spl->heap_id, &range_itor->merge_itor);
      for (uint64 i = 0; i < range_itor->num_branches; i++) {
         btree_iterator *btree_itor = &range_itor->btree_itor[i];
         if (range_itor->compacted[i]) {
            uint64 root_addr = btree_itor->root_addr;
            core_branch_iterator_deinit(spl, btree_itor, FALSE);
            btree_dec_ref(
               spl->cc, spl->cfg.btree_cfg, root_addr, PAGE_TYPE_BRANCH);
         } else {
            uint64 mt_gen = range_itor->memtable_start_gen - i;
            core_memtable_iterator_deinit(spl, btree_itor, mt_gen, FALSE);
            core_memtable_dec_ref(spl, mt_gen);
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
core_insert(core_handle *spl, key tuple_key, message data)
{
   timestamp      ts;
   const threadid tid = platform_get_tid();
   if (spl->cfg.use_stats) {
      ts = platform_get_timestamp();
   }

   if (core_max_key_size(spl) < key_length(tuple_key)) {
      return STATUS_BAD_PARAM;
   }

   if (message_class(data) == MESSAGE_TYPE_DELETE) {
      data = DELETE_MESSAGE;
   }

   platform_status rc = core_memtable_insert(spl, tuple_key, data);
   if (!SUCCESS(rc)) {
      goto out;
   }

   task_perform_one_if_needed(spl->ts, spl->cfg.queue_scale_percent);

   if (spl->cfg.use_stats) {
      switch (message_class(data)) {
         case MESSAGE_TYPE_INSERT:
            spl->stats[tid].insertions++;
            histogram_insert(spl->stats[tid].insert_latency_histo,
                             platform_timestamp_elapsed(ts));
            break;
         case MESSAGE_TYPE_UPDATE:
            spl->stats[tid].updates++;
            histogram_insert(spl->stats[tid].update_latency_histo,
                             platform_timestamp_elapsed(ts));
            break;
         case MESSAGE_TYPE_DELETE:
            spl->stats[tid].deletions++;
            histogram_insert(spl->stats[tid].delete_latency_histo,
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
// core_lookup_async
platform_status
core_lookup(core_handle *spl, key target, merge_accumulator *result)
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
   platform_assert(mt_gen_start - mt_gen_end <= CORE_NUM_MEMTABLES);

   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      platform_status rc;
      rc = core_memtable_lookup(spl, mt_gen, target, result);
      platform_assert_status_ok(rc);
      if (merge_accumulator_is_definitive(result)) {
         memtable_end_lookup(spl->mt_ctxt);
         goto found_final_answer_early;
      }
   }

   trunk_ondisk_node_handle root_handle;
   platform_status          rc;
   rc = trunk_init_root_handle(&spl->trunk_context, &root_handle);
   // release memtable lookup lock before we handle any errors
   memtable_end_lookup(spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      return rc;
   }


   rc = trunk_merge_lookup(
      &spl->trunk_context, &root_handle, target, result, NULL);
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

async_status
core_lookup_async(core_lookup_async_state *state)
{
   async_begin(state, 0);
   // look in memtables

   // 1. get read lock on lookup lock
   //     --- 2. for [mt_no = mt->generation..mt->gen_to_incorp]
   // 2. for gen = mt->generation; mt[gen % ...].gen == gen; gen --;
   //                also handles switch to READY ^^^^^

   merge_accumulator_set_to_null(state->result);

   memtable_begin_lookup(state->spl->mt_ctxt);
   uint64 mt_gen_start = memtable_generation(state->spl->mt_ctxt);
   uint64 mt_gen_end   = memtable_generation_retired(state->spl->mt_ctxt);
   platform_assert(mt_gen_start - mt_gen_end <= CORE_NUM_MEMTABLES);

   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      platform_status rc;
      rc =
         core_memtable_lookup(state->spl, mt_gen, state->target, state->result);
      platform_assert_status_ok(rc);
      if (merge_accumulator_is_definitive(state->result)) {
         memtable_end_lookup(state->spl->mt_ctxt);
         goto found_final_answer_early;
      }
   }

   platform_status rc;
   rc = trunk_init_root_handle(&state->spl->trunk_context, &state->root_handle);
   // release memtable lookup lock before we handle any errors
   memtable_end_lookup(state->spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   async_await_call(state,
                    trunk_merge_lookup_async,
                    &state->trunk_node_state,
                    &state->spl->trunk_context,
                    &state->root_handle,
                    state->target,
                    state->result,
                    NULL,
                    state->callback,
                    state->callback_arg);
   trunk_ondisk_node_handle_deinit(&state->root_handle);
   rc = async_result(&state->trunk_node_state);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   if (!merge_accumulator_is_null(state->result)
       && !merge_accumulator_is_definitive(state->result))
   {
      data_merge_tuples_final(
         state->spl->cfg.data_cfg, state->target, state->result);
   }

found_final_answer_early:

   if (state->spl->cfg.use_stats) {
      threadid tid = platform_get_tid();
      if (!merge_accumulator_is_null(state->result)) {
         state->spl->stats[tid].lookups_found++;
      } else {
         state->spl->stats[tid].lookups_not_found++;
      }
   }

   /* Normalize DELETE messages to return a null merge_accumulator */
   if (!merge_accumulator_is_null(state->result)
       && merge_accumulator_message_class(state->result) == MESSAGE_TYPE_DELETE)
   {
      merge_accumulator_set_to_null(state->result);
   }

   async_return(state, STATUS_OK);
}

platform_status
core_apply_to_range(core_handle   *spl,
                    key            start_key,
                    uint64         num_tuples,
                    tuple_function func,
                    void          *arg)
{
   core_range_iterator *range_itor =
      TYPED_MALLOC(PROCESS_PRIVATE_HEAP_ID, range_itor);
   platform_status rc = core_range_iterator_init(spl,
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
   core_range_iterator_deinit(range_itor);
   platform_free(PROCESS_PRIVATE_HEAP_ID, range_itor);
   return rc;
}


/*
 *-----------------------------------------------------------------------------
 * Create/destroy
 * XXX Fix this api to return platform_status
 *-----------------------------------------------------------------------------
 */
core_handle *
core_create(core_config      *cfg,
            allocator        *al,
            cache            *cc,
            task_system      *ts,
            allocator_root_id id,
            platform_heap_id  hid)
{
   core_handle *spl = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, spl, compacted_memtable, CORE_NUM_MEMTABLES);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   // set up the memtable context
   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   spl->mt_ctxt            = memtable_context_create(
      spl->heap_id, cc, mt_cfg, core_memtable_flush_virtual, spl);

   // set up the log
   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
   }

   // ALEX: For now we assume an init means destroying any present super blocks
   core_set_super_block(spl, FALSE, FALSE, TRUE);

   trunk_context_init(
      &spl->trunk_context, spl->cfg.trunk_node_cfg, hid, cc, al, ts, 0);

   if (spl->cfg.use_stats) {
      spl->stats = TYPED_ARRAY_ZALLOC(spl->heap_id, spl->stats, MAX_THREADS);
      platform_assert(spl->stats);
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_status rc;
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[i].insert_latency_histo);
         platform_assert_status_ok(rc);
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[i].update_latency_histo);
         platform_assert_status_ok(rc);
         rc = histogram_create(spl->heap_id,
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
core_handle *
core_mount(core_config      *cfg,
           allocator        *al,
           cache            *cc,
           task_system      *ts,
           allocator_root_id id,
           platform_heap_id  hid)
{
   core_handle *spl = TYPED_FLEXIBLE_STRUCT_ZALLOC(
      hid, spl, compacted_memtable, CORE_NUM_MEMTABLES);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   // find the unmounted super block
   uint64            root_addr        = 0;
   uint64            latest_timestamp = 0;
   page_handle      *super_page;
   core_super_block *super = core_get_super_block_if_valid(spl, &super_page);
   if (super != NULL) {
      if (super->unmounted && super->timestamp > latest_timestamp) {
         root_addr        = super->root_addr;
         latest_timestamp = super->timestamp;
      }
      core_release_super_block(spl, super_page);
   }

   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   spl->mt_ctxt            = memtable_context_create(
      spl->heap_id, cc, mt_cfg, core_memtable_flush_virtual, spl);

   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
   }

   trunk_context_init(
      &spl->trunk_context, spl->cfg.trunk_node_cfg, hid, cc, al, ts, root_addr);

   core_set_super_block(spl, FALSE, FALSE, FALSE);

   if (spl->cfg.use_stats) {
      spl->stats = TYPED_ARRAY_ZALLOC(spl->heap_id, spl->stats, MAX_THREADS);
      platform_assert(spl->stats);
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         platform_status rc;
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[i].insert_latency_histo);
         platform_assert_status_ok(rc);
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[i].update_latency_histo);
         platform_assert_status_ok(rc);
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[i].delete_latency_histo);
         platform_assert_status_ok(rc);
      }
   }
   return spl;
}

/*
 * This function is only safe to call when all other calls to spl have returned.
 */
void
core_prepare_for_shutdown(core_handle *spl)
{
   // write current memtable to disk
   // (any others must already be flushing/flushed)

   if (!memtable_is_empty(spl->mt_ctxt)) {
      /*
       * memtable_force_finalize is not thread safe. Note also, we do not hold
       * the insert lock or rotate while flushing the memtable.
       */

      uint64 generation = memtable_force_finalize(spl->mt_ctxt);
      core_memtable_flush(spl, generation);
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

   // flush all dirty pages in the cache
   cache_flush(spl->cc);
}

/*
 * Destroy a database such that it cannot be re-opened later
 */
void
core_destroy(core_handle *spl)
{
   core_prepare_for_shutdown(spl);
   trunk_context_deinit(&spl->trunk_context);
   // clear out this splinter table from the meta page.
   allocator_remove_super_addr(spl->al, spl->id);

   if (spl->cfg.use_stats) {
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         histogram_destroy(spl->heap_id, &spl->stats[i].insert_latency_histo);
         histogram_destroy(spl->heap_id, &spl->stats[i].update_latency_histo);
         histogram_destroy(spl->heap_id, &spl->stats[i].delete_latency_histo);
      }
      platform_free(spl->heap_id, spl->stats);
   }
   platform_free(spl->heap_id, spl);
}

/*
 * Close (unmount) a database without destroying it.
 * It can be re-opened later with core_mount().
 */
void
core_unmount(core_handle **spl_in)
{
   core_handle *spl = *spl_in;
   core_prepare_for_shutdown(spl);
   core_set_super_block(spl, FALSE, TRUE, FALSE);
   trunk_context_deinit(&spl->trunk_context);
   if (spl->cfg.use_stats) {
      for (uint64 i = 0; i < MAX_THREADS; i++) {
         histogram_destroy(spl->heap_id, &spl->stats[i].insert_latency_histo);
         histogram_destroy(spl->heap_id, &spl->stats[i].update_latency_histo);
         histogram_destroy(spl->heap_id, &spl->stats[i].delete_latency_histo);
      }
      platform_free(spl->heap_id, spl->stats);
   }
   platform_free(spl->heap_id, spl);
   *spl_in = (core_handle *)NULL;
}

/*
 *-----------------------------------------------------------------------------
 * core_perform_task
 *
 *      do a batch of tasks
 *-----------------------------------------------------------------------------
 */
void
core_perform_tasks(core_handle *spl)
{
   task_perform_all(spl->ts);
   cache_cleanup(spl->cc);
}

/*
 *-----------------------------------------------------------------------------
 * Debugging and info functions
 *-----------------------------------------------------------------------------
 */

void
core_print_space_use(platform_log_handle *log_handle, core_handle *spl)
{
   trunk_print_space_use(log_handle, &spl->trunk_context);
}

/*
 * core_print_super_block()
 *
 * Fetch a super-block for a running Splinter instance, and print its
 * contents.
 */
void
core_print_super_block(platform_log_handle *log_handle, core_handle *spl)
{
   page_handle      *super_page;
   core_super_block *super = core_get_super_block_if_valid(spl, &super_page);
   if (super == NULL) {
      return;
   }

   platform_log(log_handle, "Superblock root_addr=%lu {\n", super->root_addr);
   platform_log(log_handle, "log_meta_addr=%lu\n", super->log_meta_addr);
   platform_log(log_handle,
                "timestamp=%lu, checkpointed=%d, unmounted=%d\n",
                super->timestamp,
                super->checkpointed,
                super->unmounted);
   platform_log(log_handle, "}\n\n");
   core_release_super_block(spl, super_page);
}

// clang-format off
void
core_print_insertion_stats(platform_log_handle *log_handle, core_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   uint64 avg_flush_wait_time, avg_flush_time, num_flushes;
   uint64 avg_compaction_tuples, pack_time_per_tuple, avg_setup_time;
   threadid thr_i;

   core_stats *global;

   global = TYPED_ZALLOC(spl->heap_id, global);
   if (global == NULL) {
      platform_error_log("Out of memory for statistics");
      return;
   }

   histogram_handle insert_lat_accum, update_lat_accum, delete_lat_accum;
   histogram_create(spl->heap_id,
                         LATENCYHISTO_SIZE + 1,
                         latency_histo_buckets,
                         &insert_lat_accum);
   histogram_create(spl->heap_id,
                         LATENCYHISTO_SIZE + 1,
                         latency_histo_buckets,
                         &update_lat_accum);
   histogram_create(spl->heap_id,
                         LATENCYHISTO_SIZE + 1,
                         latency_histo_buckets,
                         &delete_lat_accum);

   for (thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      histogram_merge_in(insert_lat_accum,
                              spl->stats[thr_i].insert_latency_histo);
      histogram_merge_in(update_lat_accum,
                              spl->stats[thr_i].update_latency_histo);
      histogram_merge_in(delete_lat_accum,
                              spl->stats[thr_i].delete_latency_histo);

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
   histogram_print(insert_lat_accum, "Insert Latency Histogram (ns):", log_handle);
   histogram_print(update_lat_accum, "Update Latency Histogram (ns):", log_handle);
   histogram_print(delete_lat_accum, "Delete Latency Histogram (ns):", log_handle);
   histogram_destroy(spl->heap_id, &insert_lat_accum);
   histogram_destroy(spl->heap_id, &update_lat_accum);
   histogram_destroy(spl->heap_id, &delete_lat_accum);


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

   trunk_print_insertion_stats(log_handle, &spl->trunk_context);

   task_print_stats(spl->ts);
   platform_log(log_handle, "\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   cache_print_stats(log_handle, spl->cc);
   platform_log(log_handle, "\n");
   platform_free(spl->heap_id, global);
}

void
core_print_lookup_stats(platform_log_handle *log_handle, core_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   uint64 lookups_found = 0;
   uint64 lookups_not_found = 0;
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      lookups_found     += spl->stats[thr_i].lookups_found;
      lookups_not_found += spl->stats[thr_i].lookups_not_found;
   }
   uint64 lookups = lookups_found + lookups_not_found;

   platform_log(log_handle, "Overall Statistics\n");
   platform_log(log_handle, "-----------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| lookups:           %lu\n", lookups);
   platform_log(log_handle, "| lookups found:     %lu\n", lookups_found);
   platform_log(log_handle, "| lookups not found: %lu\n", lookups_not_found);
   platform_log(log_handle, "-----------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   cache_print_stats(log_handle, spl->cc);
   platform_log(log_handle, "\n");
}
// clang-format on


void
core_print_lookup(core_handle *spl, key target, platform_log_handle *log_handle)
{
   merge_accumulator data;
   merge_accumulator_init(&data, spl->heap_id);

   platform_stream_handle stream;
   platform_open_log_stream(&stream);
   uint64 mt_gen_start = memtable_generation(spl->mt_ctxt);
   uint64 mt_gen_end   = memtable_generation_retired(spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      bool32 memtable_is_compacted;
      uint64 root_addr = core_memtable_root_addr_for_lookup(
         spl, mt_gen, &memtable_is_compacted);
      platform_status rc;

      rc = btree_lookup(spl->cc,
                        spl->cfg.btree_cfg,
                        root_addr,
                        PAGE_TYPE_MEMTABLE,
                        target,
                        &data);
      platform_assert_status_ok(rc);
      if (!merge_accumulator_is_null(&data)) {
         char    key_str[128];
         char    message_str[128];
         message msg = merge_accumulator_to_message(&data);
         core_key_to_string(spl, target, key_str);
         core_message_to_string(spl, msg, message_str);
         platform_log_stream(
            &stream,
            "Key %s found in memtable %lu (gen %lu comp %d) with data %s\n",
            key_str,
            root_addr,
            mt_gen,
            memtable_is_compacted,
            message_str);
         btree_print_lookup(
            spl->cc, spl->cfg.btree_cfg, root_addr, PAGE_TYPE_MEMTABLE, target);
      }
   }

   trunk_ondisk_node_handle handle;
   trunk_init_root_handle(&spl->trunk_context, &handle);
   trunk_merge_lookup(&spl->trunk_context, &handle, target, &data, log_handle);
   trunk_ondisk_node_handle_deinit(&handle);
}

void
core_reset_stats(core_handle *spl)
{
   if (spl->cfg.use_stats) {
      for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
         histogram_destroy(spl->heap_id,
                           &spl->stats[thr_i].insert_latency_histo);
         histogram_destroy(spl->heap_id,
                           &spl->stats[thr_i].update_latency_histo);
         histogram_destroy(spl->heap_id,
                           &spl->stats[thr_i].delete_latency_histo);

         memset(&spl->stats[thr_i], 0, sizeof(spl->stats[thr_i]));

         platform_status rc;
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[thr_i].insert_latency_histo);
         platform_assert_status_ok(rc);
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[thr_i].update_latency_histo);
         platform_assert_status_ok(rc);
         rc = histogram_create(spl->heap_id,
                               LATENCYHISTO_SIZE + 1,
                               latency_histo_buckets,
                               &spl->stats[thr_i].delete_latency_histo);
         platform_assert_status_ok(rc);
      }
   }
}

// basic validation of data_config
static void
core_validate_data_config(const data_config *cfg)
{
   platform_assert(cfg->key_compare != NULL);
}

/*
 *-----------------------------------------------------------------------------
 * core_config_init --
 *
 *       Initialize splinter config
 *       This function calls btree_config_init
 *-----------------------------------------------------------------------------
 */
platform_status
core_config_init(core_config         *core_cfg,
                 cache_config        *cache_cfg,
                 data_config         *data_cfg,
                 btree_config        *btree_cfg,
                 log_config          *log_cfg,
                 trunk_config        *trunk_node_cfg,
                 uint64               queue_scale_percent,
                 bool32               use_log,
                 bool32               use_stats,
                 bool32               verbose_logging,
                 platform_log_handle *log_handle)

{
   core_validate_data_config(data_cfg);

   ZERO_CONTENTS(core_cfg);
   core_cfg->cache_cfg      = cache_cfg;
   core_cfg->data_cfg       = data_cfg;
   core_cfg->btree_cfg      = btree_cfg;
   core_cfg->trunk_node_cfg = trunk_node_cfg;
   core_cfg->log_cfg        = log_cfg;

   core_cfg->queue_scale_percent     = queue_scale_percent;
   core_cfg->use_log                 = use_log;
   core_cfg->use_stats               = use_stats;
   core_cfg->verbose_logging_enabled = verbose_logging;
   core_cfg->log_handle              = log_handle;

   memtable_config_init(&core_cfg->mt_cfg,
                        core_cfg->btree_cfg,
                        CORE_NUM_MEMTABLES,
                        trunk_node_cfg->incorporation_size_kv_bytes);

   // When everything succeeds, return success.
   return STATUS_OK;
}
