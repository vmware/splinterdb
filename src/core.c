// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * core.c --
 *
 *     This file contains the implementation for SplinterDB.
 */

#include "core.h"
#include "shard_log.h" // core constructs the concrete log via shard_log_create()
#include "data_internal.h"
#include "notification.h"
#include "platform_sleep.h"
#include "platform_time.h"
#include "platform_util.h"
#include "prefetch.h"
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
_Static_assert(CORE_NUM_MEMTABLES <= MAX_MEMTABLES,
               "CORE_NUM_MEMTABLES <= MAX_MEMTABLES");

/*
 * Initialize the instance's two locks -- superblock_lock (publication) and
 * checkpoint_state_lock (the checkpoint phase machine) -- plus the checkpoint
 * state they guard.  Transactional: on failure nothing is left initialized, so
 * callers just bail out and must not call core_locks_deinit().
 */
static platform_status
core_locks_init(core_handle *spl)
{
   platform_status rc = platform_mutex_init(
      &spl->superblock_lock, platform_get_module_id(), spl->heap_id);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = platform_mutex_init(
      &spl->checkpoint_state_lock, platform_get_module_id(), spl->heap_id);
   if (!SUCCESS(rc)) {
      platform_status destroy_rc =
         platform_mutex_destroy(&spl->superblock_lock);
      platform_assert_status_ok(destroy_rc);
      return rc;
   }

   ZERO_CONTENTS(&spl->checkpoint); // phase == CORE_CHECKPOINT_IDLE
   spl->last_checkpoint_generation = 0;
   return STATUS_OK;
}

static void
core_locks_deinit(core_handle *spl)
{
   platform_status rc = platform_mutex_destroy(&spl->checkpoint_state_lock);
   platform_assert_status_ok(rc);
   rc = platform_mutex_destroy(&spl->superblock_lock);
   platform_assert_status_ok(rc);
}

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
 * Checkpoint record functions
 *-----------------------------------------------------------------------------
 */
static platform_status
core_checkpoint_capture_cut(core_handle    *spl,
                            trunk_snapshot *snapshot,
                            uint64         *first_unincorporated_generation)
{
   /*
    * Incorporation publishes its generation and root while holding lookup
    * exclusion before taking the trunk root lock.  Take the checkpoint cut
    * in the same order, so a record never combines a pre-incorporation
    * generation with a post-incorporation root (or the converse).
    */
   memtable_block_lookups(&spl->mt_ctxt);
   uint64 retired_generation = memtable_generation_retired(&spl->mt_ctxt);
   platform_status rc = trunk_snapshot_create(&spl->trunk_context, snapshot);
   memtable_unblock_lookups(&spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      return rc;
   }

   /*
    * The first generation not folded into the root -- the exclusive replay
    * bound.  When nothing has been retired, memtable_generation_retired() is
    * UINT64_MAX and this wraps to 0 ("replay from generation 0"), so no
    * sentinel is needed.
    */
   *first_unincorporated_generation = retired_generation + 1;
   return STATUS_OK;
}

/*
 * Translate the log module's log_head into the superblock's descriptor, adding
 * the coverage information the log module does not track: start_generation is
 * the first memtable generation whose entries went to this log.
 */
static superblock_log_head
core_log_to_superblock_log_head(log_head info, uint64 start_generation)
{
   return (superblock_log_head){.addr             = info.addr,
                                .meta_addr        = info.meta_addr,
                                .magic            = info.magic,
                                .start_generation = start_generation};
}

/*
 * Commit the trunk's current COW root as the new durable tree root: capture the
 * root, make its pages durable, snapshot it into the superblock (recording the
 * first unincorporated generation, and dropping the sealed log if this root now
 * covers it), then release the previously published root.  The log slots are
 * the cut protocol's business, so this leaves the live log alone; callers that
 * need to install or discard a log do so with superblock_log_cut() /
 * superblock_discard_logs() before calling this, and the single publish below
 * commits both transitions together.
 *
 * snapshot_tree invalidates the persisted allocation state; a clean unmount
 * revalidates it in a later step.  Used by mkfs, checkpoint completion, a
 * durability checkpoint, and unmount (Part A).  On success the captured
 * reference becomes the durable record's and the previously published root's is
 * released; republishing an unchanged root is just the degenerate case of that,
 * so it needs no special handling.
 *
 * Note this always publishes, even when the root is unchanged: callers stage log
 * transitions into the image beforehand, and the generation bound can advance on
 * its own (an empty generation retires without changing the root), so the root
 * address alone is not a "nothing to do" test.
 */
static platform_status
core_checkpoint_commit_current_root(core_handle *spl)
{
   platform_status        rc;
   trunk_snapshot         snapshot;
   uint64                 first_unincorporated_generation;
   uint64                 old_root_addr = 0;
   superblock_tree_record old_rec;

   /*
    * The snapshot cut, durable record write, and old-root release are one
    * publication transaction; serialize against any concurrent publisher.
    */
   rc = platform_mutex_lock(&spl->superblock_lock);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = core_checkpoint_capture_cut(
      spl, &snapshot, &first_unincorporated_generation);
   if (!SUCCESS(rc)) {
      goto unlock_superblock;
   }

   /*
    * The snapshot reference makes the root stable, but not necessarily
    * durable.  Drain the cache so the pages the record will name are durable
    * before we publish a superblock that points at them.  This can
    * incidentally persist newer log/data pages, but it does not seal or
    * publish a logical durable-log tail; tail sync is a separate operation.
    */
   rc = cache_writeback_dirty(spl->cc);
   if (!SUCCESS(rc)) {
      goto release_snapshot;
   }
   rc = cache_durable_barrier(spl->cc);
   if (!SUCCESS(rc)) {
      goto release_snapshot;
   }

   // The previously published root, retained until the new one is durable.
   superblock_get_tree_record(&spl->superblock, &old_rec);
   old_root_addr = old_rec.root_addr;

   /*
    * Snapshot the new root and make it durable.  snapshot_tree invalidates the
    * persisted allocation map -- the in-memory map now diverges from disk; a
    * clean unmount revalidates it only after persisting the map (Part B).
    */
   superblock_snapshot_tree(
      &spl->superblock, snapshot.root_addr, first_unincorporated_generation);

   rc = superblock_make_durable(&spl->superblock);
   if (!SUCCESS(rc)) {
      /* The old root is still the newest durable one; keep its reference. */
      goto release_snapshot;
   }

   /*
    * The captured reference becomes the record's durable one, and the previously
    * published root's reference is released.  This is uniform even when the root
    * did not change: that root's count is momentarily 2 (the record's plus ours)
    * and the release brings it back to the record's single reference, so a
    * same-root republish needs no special case and cannot grow the count.  The
    * publish barrier already committed the new root to the newer superblock slot,
    * so the old slot is no longer the mount choice and releasing it cannot
    * strand a torn-write fallback.
    */
   snapshot.root_addr = 0; // transferred to the durable record
   if (old_root_addr != 0) {
      trunk_snapshot  old_snapshot = {.root_addr = old_root_addr};
      platform_status release_rc =
         trunk_snapshot_release(&spl->trunk_context, &old_snapshot);
      if (!SUCCESS(release_rc)) {
         platform_error_log("core_checkpoint_commit_current_root: "
                            "trunk_snapshot_release failed for old root addr "
                            "%lu: %s\n",
                            old_root_addr,
                            platform_status_to_string(release_rc));
         if (SUCCESS(rc)) {
            rc = release_rc;
         }
      }
   }

   goto unlock_superblock;

release_snapshot:
{
   platform_status release_rc =
      trunk_snapshot_release(&spl->trunk_context, &snapshot);
   if (SUCCESS(rc) && !SUCCESS(release_rc)) {
      rc = release_rc;
   }
}

unlock_superblock:
{
   platform_status unlock_rc = platform_mutex_unlock(&spl->superblock_lock);
   if (SUCCESS(rc) && !SUCCESS(unlock_rc)) {
      rc = unlock_rc;
   }
}
   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * Incorporation-driven checkpoint (two-log protocol)
 *
 * A checkpoint rotates the log and advances the durable root without stopping
 * the world.  It is driven off memtable rotation and incorporation:
 *
 *   begin       (rotation): pre-create the next live log outside the critical
 *               section, swap it in under the insert lock (so no writer can be
 *               mid-write to the old log), then seal the old log just after.
 *   complete    (incorporation): once the sealed log's generations are folded
 *               into the trunk root, publish the advanced root with the sealed
 *               slot cleared and free the sealed log's extents.
 *
 * See core_checkpoint_state in core.h for the phase machine and its locking.
 *-----------------------------------------------------------------------------
 */

/* Policy: should the next memtable rotation start a checkpoint? */
static bool32
core_should_take_checkpoint(core_handle *spl)
{
   if (spl->cfg.checkpoint_generation_interval == 0) {
      return FALSE;
   }
   uint64 current = spl->mt_ctxt.generation;
   return current - spl->last_checkpoint_generation
          >= spl->cfg.checkpoint_generation_interval;
}

/* The current checkpoint phase.  Read under the state lock. */
static core_checkpoint_phase
core_checkpoint_phase_get(core_handle *spl)
{
   platform_mutex_lock(&spl->checkpoint_state_lock);
   core_checkpoint_phase phase = spl->checkpoint.phase;
   platform_mutex_unlock(&spl->checkpoint_state_lock);
   return phase;
}

/*
 * Begin, step 1 (outside the rotation critical section): if no checkpoint is in
 * progress, and either the caller forces it or the interval policy says so,
 * pre-create the next live log and arm the swap.  Log creation does no disk
 * I/O, but is kept off the insert-blocking path.
 *
 * `force` bypasses only the interval policy, never the use_log precondition:
 * without a log there is nothing to cut, and arming would leave the rotate hook
 * dereferencing a NULL spl->log.
 *
 * Returns whether this call is what armed the checkpoint, so a caller that wants
 * to see its own checkpoint through can tell it did not merely observe someone
 * else's.  Returning FALSE is not an error: only one checkpoint can be in flight
 * at a time, and declining is the normal outcome when one already is.
 */
static bool32
core_checkpoint_begin(core_handle *spl, bool32 force)
{
   if (!spl->cfg.use_log) {
      return FALSE;
   }

   platform_mutex_lock(&spl->checkpoint_state_lock);
   bool32 begin = spl->checkpoint.phase == CORE_CHECKPOINT_IDLE
                  && (force || core_should_take_checkpoint(spl));
   platform_mutex_unlock(&spl->checkpoint_state_lock);
   if (!begin) {
      return FALSE;
   }

   log_handle *next = shard_log_create(
      spl->cc, (shard_log_config *)spl->cfg.log_cfg, spl->heap_id);
   if (next == NULL) {
      platform_error_log(
         "core_checkpoint_begin: shard_log_create failed; skipping\n");
      return FALSE;
   }
   log_head next_head = log_get_head(next);

   bool32 armed = FALSE;
   platform_mutex_lock(&spl->checkpoint_state_lock);
   if (spl->checkpoint.phase == CORE_CHECKPOINT_IDLE) {
      spl->checkpoint.pending_log     = next;
      spl->checkpoint.live_head       = next_head;
      spl->checkpoint.phase           = CORE_CHECKPOINT_PENDING;
      spl->last_checkpoint_generation = spl->mt_ctxt.generation;
      next                            = NULL; // handed off to the checkpoint
      armed                           = TRUE;
   }
   platform_mutex_unlock(&spl->checkpoint_state_lock);

   if (next != NULL) {
      // Lost a race with a concurrent rotation; discard the speculative log.
      log_seal(next);
      log_dec_ref(spl->cc, &next_head);
   }
   return armed;
}

/* The automatic, policy-driven arm, run after every rotation. */
static void
core_checkpoint_maybe_begin(core_handle *spl)
{
   core_checkpoint_begin(spl, FALSE /* force */);
}

/*
 * Begin, step 2 (inside the rotation critical section, insert lock held
 * exclusively): swap the pre-created live log in.  Registered as
 * mt_ctxt.rotate.  Every log writer holds the insert lock shared across its
 * log_write, so once this store retires no writer is mid-write to, or will
 * newly enter, the old log -- making the subsequent seal safe.
 */
static void
core_rotate_log(void *arg, uint64 finalized_generation)
{
   core_handle *spl = arg;

   platform_mutex_lock(&spl->checkpoint_state_lock);
   if (spl->checkpoint.phase == CORE_CHECKPOINT_PENDING) {
      spl->checkpoint.log_to_seal = spl->log;
      spl->checkpoint.sealed_head = log_get_head(spl->log);
      spl->log                    = spl->checkpoint.pending_log;
      spl->checkpoint.pending_log = NULL;
      /*
       * The retiring log received everything up to and including
       * finalized_generation, so the new live log's coverage starts at the next
       * one.  The retiring log's own start generation needs no tracking here:
       * the superblock already records it, and superblock_log_cut() carries it
       * across into the sealed slot.
       */
      spl->checkpoint.live_start_generation = finalized_generation + 1;
      spl->checkpoint.cut_generation        = finalized_generation;
      spl->checkpoint.phase                 = CORE_CHECKPOINT_SEALING;
   }
   platform_mutex_unlock(&spl->checkpoint_state_lock);
}

/*
 * Begin, step 3 (just after the rotation critical section): seal the
 * swapped-out log and publish the cut.  The swap drained and excluded all
 * writers, so sealing is safe.
 *
 * Publishing here is what makes the cut crash-safe.  The rotation moved inserts
 * to the new live log, but the superblock still names the old one, so until
 * this runs a crash would lose everything written to the new log.  Order
 * matters: the sealed log's pages must be durable before the superblock names
 * it as sealed, since recovery replays it as-is.
 */
static void
core_checkpoint_seal_cut(core_handle *spl)
{
   platform_mutex_lock(&spl->checkpoint_state_lock);
   log_handle         *to_seal = NULL;
   superblock_log_head live    = {0};
   if (spl->checkpoint.phase == CORE_CHECKPOINT_SEALING) {
      to_seal                     = spl->checkpoint.log_to_seal;
      spl->checkpoint.log_to_seal = NULL;
      spl->checkpoint.phase       = CORE_CHECKPOINT_INCORPORATING;
      live                        = core_log_to_superblock_log_head(
         spl->checkpoint.live_head, spl->checkpoint.live_start_generation);
   }
   platform_mutex_unlock(&spl->checkpoint_state_lock);

   if (to_seal == NULL) {
      return;
   }
   log_seal(to_seal);

   /*
    * Serialize against any other superblock publisher (the superblock context
    * is not thread safe).  On failure the checkpoint still proceeds: the
    * completion publish will record the correct final state; only this
    * crash-protection window is left uncovered.
    */
   platform_status rc = platform_mutex_lock(&spl->superblock_lock);
   if (!SUCCESS(rc)) {
      platform_error_log("core_checkpoint_seal_cut: lock failed: %s\n",
                         platform_status_to_string(rc));
      return;
   }
   rc = cache_writeback_dirty(spl->cc);
   if (SUCCESS(rc)) {
      rc = cache_durable_barrier(spl->cc);
   }
   if (SUCCESS(rc)) {
      // The image still names the retiring log as live, so the cut moves it
      // into the sealed slot, carrying its recorded start generation with it.
      superblock_log_cut(&spl->superblock, live);
      rc = superblock_make_durable(&spl->superblock);
   }
   if (!SUCCESS(rc)) {
      platform_error_log("core_checkpoint_seal_cut: failed to publish the log "
                         "cut: %s\n",
                         platform_status_to_string(rc));
   }
   platform_status unlock_rc = platform_mutex_unlock(&spl->superblock_lock);
   platform_assert_status_ok(unlock_rc);
}

/*
 * Complete (after an incorporation): if the sealed log's generations are all
 * folded into the trunk root, advance the durable root with the sealed slot
 * cleared (which makes the root and superblock durable) and free the sealed
 * log's extents.  Called from the single-threaded incorporation path.
 */
static platform_status
core_maybe_complete_checkpoint(core_handle *spl)
{
   platform_mutex_lock(&spl->checkpoint_state_lock);
   // The sealed log's cut generation is fully incorporated once it falls below
   // the first unincorporated generation.  When nothing has been retired,
   // memtable_generation_retired() is UINT64_MAX and this wraps to 0, so the
   // comparison is false without a sentinel check.
   uint64 first_unincorporated = memtable_generation_retired(&spl->mt_ctxt) + 1;
   bool32 complete = spl->checkpoint.phase == CORE_CHECKPOINT_INCORPORATING
                     && first_unincorporated > spl->checkpoint.cut_generation;
   log_head sealed = {0};
   if (complete) {
      sealed                = spl->checkpoint.sealed_head;
      spl->checkpoint.phase = CORE_CHECKPOINT_COMPLETING;
   }
   platform_mutex_unlock(&spl->checkpoint_state_lock);

   if (!complete) {
      return STATUS_OK;
   }

   /*
    * The live log is already recorded (core_checkpoint_seal_cut() published the
    * cut), so this only advances the root -- which is what lets snapshot_tree
    * drop the now-covered sealed log.
    */
   platform_status rc = core_checkpoint_commit_current_root(spl);
   if (SUCCESS(rc)) {
      // The sealed log's entries are now durably in the root; free its extents.
      log_dec_ref(spl->cc, &sealed);
   } else {
      platform_error_log("core_maybe_complete_checkpoint: publish failed: %s\n",
                         platform_status_to_string(rc));
   }

   platform_mutex_lock(&spl->checkpoint_state_lock);
   if (SUCCESS(rc)) {
      ZERO_CONTENTS(&spl->checkpoint.sealed_head);
      ZERO_CONTENTS(&spl->checkpoint.live_head);
      spl->checkpoint.cut_generation = 0;
      spl->checkpoint.phase          = CORE_CHECKPOINT_IDLE;
   } else {
      // Leave the sealed slot intact and retry on a later incorporation.
      spl->checkpoint.phase = CORE_CHECKPOINT_INCORPORATING;
   }
   platform_mutex_unlock(&spl->checkpoint_state_lock);
   return rc;
}

/*
 * Release any resources of an in-flight checkpoint during a quiesced shutdown,
 * before the unmount/destroy publish.  No locking: the caller has quiesced all
 * inserts and incorporations.  A completed checkpoint
 * (INCORPORATING/COMPLETING) is normally already reaped by the quiesce drain;
 * the residual cases below are defensive.
 */
static void
core_checkpoint_cleanup_for_shutdown(core_handle *spl)
{
   core_checkpoint_state *cp = &spl->checkpoint;
   switch (cp->phase) {
      case CORE_CHECKPOINT_IDLE:
         break;
      case CORE_CHECKPOINT_PENDING:
         // The next live log was pre-created but never installed; discard it.
         log_seal(cp->pending_log);
         log_dec_ref(spl->cc, &cp->live_head);
         break;
      case CORE_CHECKPOINT_INCORPORATING:
      case CORE_CHECKPOINT_COMPLETING:
         // The sealed log is fully incorporated after quiesce.  The shutdown
         // publish records sealed=none, so just reclaim its extents here
         // (before the map is persisted, so the map reflects the free).
         log_dec_ref(spl->cc, &cp->sealed_head);
         break;
      case CORE_CHECKPOINT_SEALING:
      default:
         platform_assert(
            FALSE, "unexpected checkpoint phase %d at shutdown", cp->phase);
   }
   ZERO_CONTENTS(cp); // phase == CORE_CHECKPOINT_IDLE
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
   memtable *mt           = &spl->mt_ctxt.mt[memtable_idx];
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
   memtable *mt           = &spl->mt_ctxt.mt[memtable_idx];
   platform_assert(mt->generation == generation,
                   "mt->generation=%lu, mt_ctxt->generation=%lu, "
                   "mt_ctxt->generation_retired=%lu, generation=%lu\n",
                   mt->generation,
                   spl->mt_ctxt.generation,
                   spl->mt_ctxt.generation_retired,
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

static bool32
core_memtable_state_has_compacted_branch(memtable_state state)
{
   return state == MEMTABLE_STATE_COMPACTED
          || state == MEMTABLE_STATE_INCORPORATION_ASSIGNED
          || state == MEMTABLE_STATE_INCORPORATION_FAILED;
}

static uint64
core_memtable_compacted_branch_root(core_handle *spl, uint64 generation)
{
   memtable *mt = core_get_memtable(spl, generation);
   if (!core_memtable_state_has_compacted_branch(mt->state)) {
      return 0;
   }

   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   return cmt->branch.root_addr;
}

static void
core_memtable_release_compacted_branch(core_handle *spl, uint64 generation)
{
   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   if (cmt->branch.root_addr == 0) {
      return;
   }

   btree_dec_ref(
      spl->cc, spl->cfg.btree_cfg, cmt->branch.root_addr, PAGE_TYPE_BRANCH);
   cmt->branch.root_addr = 0;
}

static void
core_memtable_mark_incorporation_failed(core_handle    *spl,
                                        uint64          generation,
                                        platform_status status)
{
   memtable_block_lookups(&spl->mt_ctxt);
   memtable *mt = core_get_memtable(spl, generation);
   platform_error_log("Memtable incorporation failed: generation=%lu "
                      "state=%s status=%s memtable_root=%lu\n",
                      generation,
                      memtable_state_string(mt->state),
                      platform_status_to_string(status),
                      mt->root_addr);
   memtable_mark_incorporation_failed(mt, status);
   core_memtable_release_compacted_branch(spl, generation);
   memtable_unblock_lookups(&spl->mt_ctxt);
}

static inline void
core_memtable_inc_ref(core_handle *spl, uint64 root_addr)
{
   memtable_root_inc_ref(&spl->mt_ctxt, root_addr);
}


static void
core_memtable_dec_ref(core_handle *spl, uint64 root_addr)
{
   memtable_root_dec_ref(&spl->mt_ctxt, root_addr);
}


/* Wrappers for creating/destroying memtable btree iterators. */
static platform_status
core_memtable_iterator_init(core_handle    *spl,
                            btree_iterator *itor,
                            uint64          root_addr,
                            comparison      min_key_comparison,
                            key             min_key,
                            comparison      max_key_comparison,
                            key             max_key,
                            comparison      start_key_comparison,
                            key             start_key)
{
   return btree_iterator_init(spl->cc,
                              spl->cfg.btree_cfg,
                              itor,
                              root_addr,
                              PAGE_TYPE_MEMTABLE,
                              min_key_comparison,
                              min_key,
                              max_key_comparison,
                              max_key,
                              start_key_comparison,
                              start_key,
                              FALSE,
                              0,
                              0);
}

static void
core_memtable_iterator_deinit(btree_iterator *itor)
{
   btree_iterator_deinit(itor);
}

/*
 * On success, returns the current memtable with the insert lock held. The
 * caller must release it with memtable_end_insert().
 */
static platform_status
core_begin_memtable_insert(core_handle *spl, uint64 *generation, memtable **mt)
{
   platform_status rc =
      memtable_maybe_rotate_and_begin_insert(&spl->mt_ctxt, generation);
   while (STATUS_IS_EQ(rc, STATUS_BUSY)) {
      // Memtable isn't ready, do a task if available; may be required to
      // incorporate memtable that we're waiting on
      task_perform_one_if_needed(spl->ts, 0);
      rc = memtable_maybe_rotate_and_begin_insert(&spl->mt_ctxt, generation);
   }
   if (!SUCCESS(rc)) {
      return rc;
   }

   // this call is safe because we hold the insert lock
   *mt = core_get_memtable(spl, *generation);
   return STATUS_OK;
}

static platform_status
core_log_insert(core_handle                *spl,
                uint64                      memtable_generation,
                key                         tuple_key,
                message                     msg,
                const btree_insert_results *insert_results)
{
   /* TODO: FIXME: One way we could get stuck in a fetch-and-update is if the
    * insert succeeds but the lookup fails (e.g. due to an I/O error while
    * traversing the trunk).  I think the promise we should make in that case is
    * that we will preserve enough information in the log to enable the user
    * to recover the old value. One way to do this might be to insert a
    * reference to the trunk into the log. */
   if (!spl->cfg.use_log) {
      return STATUS_OK;
   }

   message log_msg =
      merge_accumulator_is_null(&insert_results->msg_blob)
         ? msg
         : merge_accumulator_to_message(&insert_results->msg_blob);
   int log_rc = log_write(spl->log,
                          tuple_key,
                          log_msg,
                          memtable_generation,
                          insert_results->leaf_generation);
   return log_rc == 0 ? STATUS_OK : (platform_status){.r = log_rc};
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

   platform_status rc = core_memtable_iterator_init(spl,
                                                    &btree_itor,
                                                    memtable_root_addr,
                                                    greater_than_or_equal,
                                                    NEGATIVE_INFINITY_KEY,
                                                    less_than,
                                                    POSITIVE_INFINITY_KEY,
                                                    greater_than_or_equal,
                                                    NEGATIVE_INFINITY_KEY);
   platform_assert_status_ok(rc);
   const routing_config *rfcfg = spl->cfg.trunk_node_cfg->filter_cfg;
   uint64 rflimit = routing_filter_max_fingerprints(spl->cfg.cache_cfg, rfcfg);
   btree_pack_req req;
   btree_pack_req_init(&req,
                       spl->cc,
                       spl->cfg.btree_cfg,
                       itor,
                       rflimit,
                       rfcfg->seed,
                       FALSE,
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
   core_memtable_iterator_deinit(&btree_itor);

   /*
    * A forced rotation (see memtable_force_rotation(), used by
    * core_checkpoint() when nothing rotates on its own) can finalize a memtable
    * that received no inserts.  btree_pack() already defines this case: an
    * empty input yields num_tuples == 0 and root_addr == 0, allocating no page.
    * The generation still retires; core_memtable_incorporate() recognizes the
    * missing branch and skips the trunk incorporation.
    */
   new_branch->root_addr = req.root_addr;

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

   memtable_lock_incorporation_lock(&spl->mt_ctxt);
   memtable *mt = core_try_get_memtable(spl, generation);
   if ((mt == NULL)
       || (generation != memtable_generation_to_incorporate(&spl->mt_ctxt)))
   {
      should_start = FALSE;
      goto unlock_incorp_lock;
   }
   should_start = memtable_try_transition(
      mt, MEMTABLE_STATE_COMPACTED, MEMTABLE_STATE_INCORPORATION_ASSIGNED);

unlock_incorp_lock:
   memtable_unlock_incorporation_lock(&spl->mt_ctxt);
   return should_start;
}

static inline bool32
core_try_continue_incorporate(core_handle *spl, uint64 next_generation)
{
   bool32 should_continue = FALSE;

   memtable_lock_incorporation_lock(&spl->mt_ctxt);
   memtable *mt = core_try_get_memtable(spl, next_generation);
   if (mt == NULL) {
      should_continue = FALSE;
      goto unlock_incorp_lock;
   }
   should_continue = memtable_try_transition(
      mt, MEMTABLE_STATE_COMPACTED, MEMTABLE_STATE_INCORPORATION_ASSIGNED);
   memtable_increment_to_generation_to_incorporate(&spl->mt_ctxt,
                                                   next_generation);

unlock_incorp_lock:
   memtable_unlock_incorporation_lock(&spl->mt_ctxt);
   return should_continue;
}

static platform_status
core_memtable_incorporate(core_handle   *spl,
                          uint64         generation,
                          const threadid tid)
{
   platform_stream_handle stream;
   platform_status        rc = core_open_log_stream_if_enabled(spl, &stream);
   if (!SUCCESS(rc)) {
      platform_error_log("core_memtable_incorporate: failed to open log "
                         "stream for generation %lu: %s\n",
                         generation,
                         platform_status_to_string(rc));
      core_memtable_mark_incorporation_failed(spl, generation, rc);
      return rc;
   }
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
   /*
    * A forced rotation can retire a generation that received no inserts, in
    * which case core_memtable_compact() produced no branch (root_addr == 0).
    * There is nothing to fold into the trunk, and the trunk_incorporate_*()
    * calls require a real branch (trunk_incorporate_prepare() asserts
    * branch_addr != 0), so skip them; the generation still retires below.
    */
   bool32 has_branch = (cmt->branch.root_addr != 0);
   if (has_branch) {
      rc =
         trunk_incorporate_prepare(&spl->trunk_context, cmt->branch.root_addr);
      if (!SUCCESS(rc)) {
         platform_error_log("trunk_incorporate_prepare failed: %s\n",
                            platform_status_to_string(rc));
         core_close_log_stream_if_enabled(spl, &stream);
         core_memtable_mark_incorporation_failed(spl, generation, rc);
         return rc;
      }
      btree_dec_ref(
         spl->cc, spl->cfg.btree_cfg, cmt->branch.root_addr, PAGE_TYPE_BRANCH);
      if (spl->cfg.use_stats) {
         spl->stats[tid].memtable_flush_wait_time_ns +=
            platform_timestamp_elapsed(cmt->wait_start);
      }
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
   memtable_block_lookups(&spl->mt_ctxt);
   memtable *mt = core_get_memtable(spl, generation);
   // Normally need to hold incorp_mutex, but debug code and also guaranteed no
   // one is changing gen_to_incorp (we are the only thread that would try)
   debug_assert(generation
                == memtable_generation_to_incorporate(&spl->mt_ctxt));
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATION_ASSIGNED, MEMTABLE_STATE_INCORPORATING);
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATING, MEMTABLE_STATE_INCORPORATED);
   memtable_increment_to_generation_retired(&spl->mt_ctxt, generation);
   if (has_branch) {
      trunk_incorporate_commit(&spl->trunk_context);
   }
   memtable_unblock_lookups(&spl->mt_ctxt);

   if (has_branch) {
      trunk_incorporate_cleanup(&spl->trunk_context);
   }

   core_close_log_stream_if_enabled(spl, &stream);

   memtable_recycle(&spl->mt_ctxt, mt);

   if (spl->cfg.use_stats) {
      const threadid tid = platform_get_tid();
      flush_start        = platform_timestamp_elapsed(flush_start);
      spl->stats[tid].memtable_flush_time_ns += flush_start;
      spl->stats[tid].memtable_flushes++;
      if (flush_start > spl->stats[tid].memtable_flush_time_max_ns) {
         spl->stats[tid].memtable_flush_time_max_ns = flush_start;
      }
   }

   return STATUS_OK;
}

/*
 * Main wrapper function to carry out incorporation of a memtable.
 *
 * If background threads are disabled this function is called inline in the
 * context of the foreground thread.  If background threads are enabled, this
 * function is called in the context of the memtable worker thread.
 */
static platform_status
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
      platform_status rc = core_memtable_incorporate(spl, generation, tid);
      if (!SUCCESS(rc)) {
         return rc;
      }
      generation++;
   } while (core_try_continue_incorporate(spl, generation));

   // A checkpoint's sealed log may now be fully incorporated; complete it.
   core_maybe_complete_checkpoint(spl);
out:
   return STATUS_OK;
}

static void
core_memtable_flush_internal_virtual(task *arg)
{
   core_memtable_args *mt_args = container_of(arg, core_memtable_args, tsk);
   platform_status     rc =
      core_memtable_flush_internal(mt_args->spl, mt_args->generation);
   if (!SUCCESS(rc)) {
      platform_error_log("memtable flush failed: %s\n",
                         platform_status_to_string(rc));
   }
}

/*
 * Function to trigger a memtable incorporation. Called in the context of
 * the foreground doing insertions.
 */
static void
core_memtable_flush(core_handle *spl, uint64 generation)
{
   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   cmt->mt_args.spl             = spl;
   cmt->mt_args.generation      = generation;
   task_enqueue(spl->ts,
                TASK_TYPE_MEMTABLE,
                &cmt->mt_args.tsk,
                core_memtable_flush_internal_virtual,
                FALSE);
}

static void
core_memtable_flush_virtual(void *arg, uint64 generation)
{
   core_handle *spl = arg;

   // Begin, step 3: if this rotation's in-CS hook swapped the live log, seal
   // the old one now that the critical section has been released.
   core_checkpoint_seal_cut(spl);

   core_memtable_flush(spl, generation);

   // Begin, step 1: decide whether the next rotation should start a checkpoint,
   // pre-creating its live log outside any critical section.
   core_checkpoint_maybe_begin(spl);
}

static inline uint64
core_memtable_root_addr_for_lookup(core_handle *spl,
                                   uint64       generation,
                                   bool32      *is_compacted,
                                   bool32      *is_active)
{
   memtable *mt = core_get_memtable(spl, generation);
   platform_assert(memtable_ok_to_lookup(mt));
   if (is_active != NULL) {
      *is_active = mt->state == MEMTABLE_STATE_READY;
   }

   if (memtable_ok_to_lookup_compacted(mt)) {
      // lookup in packed tree
      *is_compacted = TRUE;
      if (is_active != NULL) {
         *is_active = FALSE;
      }
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
core_memtable_lookup(core_handle   *spl,
                     uint64         generation,
                     key            target,
                     lookup_result *result)
{
   cache *const        cc  = spl->cc;
   btree_config *const cfg = spl->cfg.btree_cfg;
   bool32              memtable_is_compacted;
   uint64              root_addr = core_memtable_root_addr_for_lookup(
      spl, generation, &memtable_is_compacted, NULL);
   if (memtable_is_compacted && root_addr == 0) {
      // A forced rotation can retire an empty generation, whose compacted
      // branch has no root page.  It holds no tuples, so there is nothing to
      // search -- equivalent to finding nothing here.
      return STATUS_OK;
   }
   page_type type =
      memtable_is_compacted ? PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;

   return btree_lookup_and_merge(
      cc, cfg, root_addr, type, target, result, NULL);
}

static platform_status
core_lookup_memtables_locked(core_handle   *spl,
                             uint64         mt_gen_start,
                             key            target,
                             lookup_result *result,
                             bool32        *found_final)
{
   uint64 mt_gen_end = memtable_generation_retired(&spl->mt_ctxt);
   platform_assert(mt_gen_start - mt_gen_end <= CORE_NUM_MEMTABLES);

   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      platform_status rc = core_memtable_lookup(spl, mt_gen, target, result);
      platform_assert_status_ok(rc);
      if (!lookup_result_should_continue(result)) {
         *found_final = TRUE;
         return STATUS_OK;
      }
   }

   *found_final = FALSE;
   return STATUS_OK;
}

static platform_status
core_lookup_from_memtable_generation_locked(core_handle   *spl,
                                            uint64         mt_gen_start,
                                            key            target,
                                            lookup_result *result)
{
   bool32                   found_final = FALSE;
   trunk_ondisk_node_handle root_handle;

   platform_status rc;

   if (mt_gen_start != (uint64)-1) {
      rc = core_lookup_memtables_locked(
         spl, mt_gen_start, target, result, &found_final);
      if (found_final) {
         memtable_end_lookup(&spl->mt_ctxt);
         lookup_result_finalize(result, target);
         return STATUS_OK;
      }
   }

   rc = trunk_init_root_handle(&spl->trunk_context, &root_handle);
   memtable_end_lookup(&spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = trunk_merge_lookup(
      &spl->trunk_context, &root_handle, target, result, NULL);
   trunk_ondisk_node_handle_deinit(&root_handle);
   if (!SUCCESS(rc)) {
      return rc;
   }

   lookup_result_finalize(result, target);
   return STATUS_OK;
}

typedef struct core_btree_iterator_init_async_context {
   btree_iterator_async_state state;
   bool32                     ready;
   bool32                     done;
} core_btree_iterator_init_async_context;

static void
core_btree_iterator_init_async_callback(void *arg)
{
   core_btree_iterator_init_async_context *ctxt = arg;
   __atomic_store_n(&ctxt->ready, TRUE, __ATOMIC_RELEASE);
}

static platform_status
core_start_btree_iterator_init_async(
   core_handle                            *spl,
   core_btree_iterator_init_async_context *ctxt,
   btree_iterator                         *itor,
   uint64                                  root_addr,
   page_type                               page_type,
   comparison                              min_key_comparison,
   key                                     min_key,
   comparison                              max_key_comparison,
   key                                     max_key,
   comparison                              start_key_comparison,
   key                                     start_key,
   bool32                                  copy_nodes,
   uint32                                  prefetch_lookahead)
{
   btree_iterator_async_state_init(&ctxt->state,
                                   spl->cc,
                                   spl->cfg.btree_cfg,
                                   itor,
                                   root_addr,
                                   page_type,
                                   min_key_comparison,
                                   min_key,
                                   max_key_comparison,
                                   max_key,
                                   start_key_comparison,
                                   start_key,
                                   copy_nodes,
                                   0,
                                   prefetch_lookahead,
                                   core_btree_iterator_init_async_callback,
                                   ctxt);
   __atomic_store_n(&ctxt->ready, FALSE, __ATOMIC_RELAXED);
   ctxt->done = FALSE;

   if (btree_iterator_init_async(&ctxt->state) == ASYNC_STATUS_DONE) {
      ctxt->done = TRUE;
      return btree_iterator_init_async_result(&ctxt->state);
   }

   return STATUS_OK;
}

static platform_status
core_drain_btree_iterator_init_async(
   cache                                  *cc,
   core_btree_iterator_init_async_context *ctxt,
   uint64                                  num_inits)
{
   platform_status result     = STATUS_OK;
   uint64          done_count = 0;
   for (uint64 i = 0; i < num_inits; i++) {
      if (ctxt[i].done) {
         done_count++;
         platform_status rc = btree_iterator_init_async_result(&ctxt[i].state);
         if (!SUCCESS(rc) && SUCCESS(result)) {
            result = rc;
         }
      }
   }

   while (done_count < num_inits) {
      bool32 made_progress = FALSE;
      for (uint64 i = 0; i < num_inits; i++) {
         if (ctxt[i].done
             || !__atomic_exchange_n(&ctxt[i].ready, FALSE, __ATOMIC_ACQUIRE))
         {
            continue;
         }

         made_progress = TRUE;
         if (btree_iterator_init_async(&ctxt[i].state) == ASYNC_STATUS_DONE) {
            ctxt[i].done = TRUE;
            done_count++;
            platform_status rc =
               btree_iterator_init_async_result(&ctxt[i].state);
            if (!SUCCESS(rc) && SUCCESS(result)) {
               result = rc;
            }
         }
      }

      if (!made_progress) {
         cache_cleanup(cc);
      }
   }

   return result;
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

static inline bool32
core_range_iterator_has_next_leaf(core_range_iterator *range_itor)
{
   key    local_max_key = key_buffer_key(&range_itor->local_max_key);
   key    max_key       = key_buffer_key(&range_itor->max_key);
   int    cmp = core_key_compare(range_itor->spl, local_max_key, max_key);
   bool32 max_is_finite = !key_is_positive_infinity(max_key);

   return cmp < 0
          || (cmp == 0 && max_is_finite && !range_itor->local_max_key_truncated
              && range_itor->max_key_comparison == less_than_or_equal);
}

platform_status
core_range_iterator_init(core_handle         *spl,
                         core_range_iterator *range_itor,
                         comparison           min_key_comparison,
                         key                  min_key,
                         comparison           max_key_comparison,
                         key                  max_key,
                         comparison           start_key_comparison,
                         key                  start_key)
{
   platform_status rc;

   debug_assert(!key_is_null(min_key));
   debug_assert(!key_is_null(max_key));
   debug_assert(!key_is_null(start_key));
   debug_assert(min_key_comparison == greater_than
                || min_key_comparison == greater_than_or_equal);
   debug_assert(max_key_comparison == less_than
                || max_key_comparison == less_than_or_equal);

   range_itor->spl                = spl;
   range_itor->super.ops          = &core_range_iterator_ops;
   range_itor->num_branches       = 0;
   range_itor->merge_itor         = NULL;
   range_itor->can_prev           = TRUE;
   range_itor->can_next           = TRUE;
   range_itor->min_key_comparison = min_key_comparison;
   range_itor->max_key_comparison = max_key_comparison;
   ZERO_ARRAY(range_itor->compacted);
   ZERO_ARRAY(range_itor->btree_itor_initialized);

   key_buffer_init(&range_itor->min_key, PROCESS_PRIVATE_HEAP_ID);
   key_buffer_init(&range_itor->max_key, PROCESS_PRIVATE_HEAP_ID);
   key_buffer_init(&range_itor->local_min_key, PROCESS_PRIVATE_HEAP_ID);
   key_buffer_init(&range_itor->local_max_key, PROCESS_PRIVATE_HEAP_ID);

   bool32 forward_start = comparison_is_forward(start_key_comparison);
   int    min_start_cmp = core_key_compare(spl, min_key, start_key);
   if (min_start_cmp > 0) {
      start_key            = min_key;
      start_key_comparison = forward_start
                                ? min_key_comparison
                                : comparison_invert(min_key_comparison);
   } else if (min_start_cmp == 0) {
      if (forward_start && min_key_comparison == greater_than) {
         start_key_comparison = greater_than;
      } else if (!forward_start && min_key_comparison == greater_than) {
         start_key_comparison = less_than_or_equal;
      }
   }
   int max_start_cmp = core_key_compare(spl, max_key, start_key);
   if (max_start_cmp < 0) {
      start_key            = max_key;
      start_key_comparison = forward_start
                                ? comparison_invert(max_key_comparison)
                                : max_key_comparison;
   } else if (max_start_cmp == 0) {
      if (!forward_start && max_key_comparison == less_than) {
         start_key_comparison = less_than;
      } else if (forward_start && max_key_comparison == less_than) {
         start_key_comparison = greater_than_or_equal;
      }
   }

   // copy over global min and max
   rc = key_buffer_copy_key(&range_itor->min_key, min_key);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }
   rc = key_buffer_copy_key(&range_itor->max_key, max_key);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   // grab the lookup lock
   memtable_begin_lookup(&spl->mt_ctxt);

   // memtables
   ZERO_ARRAY(range_itor->branch);
   // Note this iteration is in descending generation order
   range_itor->memtable_start_gen = memtable_generation(&spl->mt_ctxt);
   range_itor->memtable_end_gen   = memtable_generation_retired(&spl->mt_ctxt);
   range_itor->num_memtable_branches =
      range_itor->memtable_start_gen - range_itor->memtable_end_gen;
   bool32 first_memtable_copy_nodes = FALSE;
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
      bool32 active;
      uint64 root_addr =
         core_memtable_root_addr_for_lookup(spl, mt_gen, &compacted, &active);
      // Only READY memtables can be modified while this iterator is live.
      // Determined before the empty-generation skip below so that skipping
      // cannot affect which generation is treated as the first one.
      if (range_itor->num_branches == 0) {
         first_memtable_copy_nodes = active;
      } else {
         debug_assert(!active);
      }
      if (compacted && root_addr == 0) {
         // A forced rotation can retire an empty generation, whose compacted
         // branch has no root page.  It contributes no tuples, so there is
         // nothing to merge from it.
         continue;
      }
      range_itor->compacted[range_itor->num_branches] = compacted;
      if (compacted) {
         btree_inc_ref(spl->cc, spl->cfg.btree_cfg, root_addr);
      } else {
         core_memtable_inc_ref(spl, root_addr);
      }

      range_itor->branch[range_itor->num_branches].addr = root_addr;
      range_itor->branch[range_itor->num_branches].type =
         compacted ? PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;
      range_itor->num_branches++;
   }

   trunk_ondisk_node_handle root_handle;
   rc = trunk_init_root_handle(&spl->trunk_context, &root_handle);
   memtable_end_lookup(&spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   uint64 old_num_branches = range_itor->num_branches;
   rc                      = trunk_collect_branches(&spl->trunk_context,
                               &root_handle,
                               start_key,
                               start_key_comparison,
                               CORE_RANGE_ITOR_MAX_BRANCHES,
                               &range_itor->num_branches,
                               range_itor->branch,
                               &range_itor->local_min_key,
                               &range_itor->local_max_key);
   trunk_ondisk_node_handle_deinit(&root_handle);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   for (uint64 i = old_num_branches; i < range_itor->num_branches; i++) {
      range_itor->compacted[i] = TRUE;
   }

   range_itor->local_min_key_comparison = greater_than_or_equal;
   range_itor->local_max_key_comparison = less_than;
   range_itor->local_max_key_truncated  = FALSE;

   // have a leaf, use to establish local bounds
   if (core_key_compare(
          spl, key_buffer_key(&range_itor->local_min_key), min_key)
       <= 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_min_key, min_key);
      range_itor->local_min_key_comparison = min_key_comparison;
      if (!SUCCESS(rc)) {
         core_range_iterator_deinit(range_itor);
         return rc;
      }
   }
   if (core_key_compare(
          spl, key_buffer_key(&range_itor->local_max_key), max_key)
       > 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_max_key, max_key);
      range_itor->local_max_key_comparison = max_key_comparison;
      range_itor->local_max_key_truncated  = TRUE;
      if (!SUCCESS(rc)) {
         core_range_iterator_deinit(range_itor);
         return rc;
      }
   }

   core_btree_iterator_init_async_context *init_ctxt = NULL;
   if (range_itor->num_branches != 0) {
      /*
       * Async cache-load waiters embedded in these contexts can be released by
       * another process when the clockcache is shared.  The callback only marks
       * ctxt->ready, so keep the context itself in the Splinter heap; the
       * owning process remains responsible for resuming the iterator state.
       */
      init_ctxt =
         TYPED_ARRAY_ZALLOC(spl->heap_id, init_ctxt, range_itor->num_branches);
   }
   if (range_itor->num_branches != 0 && init_ctxt == NULL) {
      core_range_iterator_deinit(range_itor);
      return STATUS_NO_MEMORY;
   }

   // Deep extent-prefetch for the scan: count compacted branches and give each
   // a soft share of the prefetch budget.
   uint64 n_prefetch_branches = 0;
   for (uint64 branch_no = 0; branch_no < range_itor->num_branches; branch_no++)
   {
      if (range_itor->compacted[branch_no]) {
         n_prefetch_branches++;
      }
   }
   uint32 deep_lookahead =
      prefetch_budget_to_extent_lookahead(cache_extent_size(spl->cc),
                                          spl->cfg.prefetch_budget,
                                          n_prefetch_branches);

   uint64 started_inits = 0;
   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      uint64          branch_no          = range_itor->num_branches - i - 1;
      btree_iterator *btree_itor         = &range_itor->btree_itor[branch_no];
      uint64          branch_addr        = range_itor->branch[branch_no].addr;
      page_type       page_type          = range_itor->branch[branch_no].type;
      uint32          prefetch_lookahead = 0;
      if (range_itor->compacted[branch_no]) {
         prefetch_lookahead = deep_lookahead;
      }
      rc = core_start_btree_iterator_init_async(
         spl,
         &init_ctxt[i],
         btree_itor,
         branch_addr,
         page_type,
         range_itor->local_min_key_comparison,
         key_buffer_key(&range_itor->local_min_key),
         range_itor->local_max_key_comparison,
         key_buffer_key(&range_itor->local_max_key),
         start_key_comparison,
         start_key,
         branch_no == 0 ? first_memtable_copy_nodes : FALSE,
         prefetch_lookahead);
      started_inits++;
      if (!SUCCESS(rc)) {
         break;
      }
      range_itor->itor[i] = &btree_itor->super;
   }

   platform_status drain_rc =
      core_drain_btree_iterator_init_async(spl->cc, init_ctxt, started_inits);
   if (SUCCESS(rc)) {
      rc = drain_rc;
   }
   for (uint64 i = 0; i < started_inits; i++) {
      if (init_ctxt[i].done) {
         uint64 branch_no = range_itor->num_branches - i - 1;
         range_itor->btree_itor_initialized[branch_no] = TRUE;
      }
   }
   if (init_ctxt != NULL) {
      platform_free(spl->heap_id, init_ctxt);
   }
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   rc = merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                              spl->cfg.data_cfg,
                              range_itor->num_branches,
                              range_itor->itor,
                              MERGE_FULL,
                              greater_than <= start_key_comparison,
                              &range_itor->merge_itor);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   bool32 in_range = iterator_can_curr(&range_itor->merge_itor->super);

   /*
    * if the merge itor is already exhausted, and there are more keys in the
    * db/range, move to prev/next leaf
    */
   if (!in_range && start_key_comparison >= greater_than) {
      if (core_range_iterator_has_next_leaf(range_itor)) {
         key        local_max = key_buffer_key(&range_itor->local_max_key);
         key_buffer local_max_buffer;
         rc = key_buffer_init_from_key(
            &local_max_buffer, PROCESS_PRIVATE_HEAP_ID, local_max);
         core_range_iterator_deinit(range_itor);
         if (!SUCCESS(rc)) {
            return rc;
         }
         local_max = key_buffer_key(&local_max_buffer);
         rc        = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       greater_than_or_equal,
                                       local_max);
         key_buffer_deinit(&local_max_buffer);
         if (!SUCCESS(rc)) {
            return rc;
         }

      } else {
         range_itor->can_next = FALSE;
         range_itor->can_prev =
            iterator_can_prev(&range_itor->merge_itor->super);
      }
   }
   if (!in_range && start_key_comparison <= less_than_or_equal) {
      key local_min = key_buffer_key(&range_itor->local_min_key);
      if (core_key_compare(spl, local_min, min_key) > 0) {
         key_buffer local_min_buffer;
         rc = key_buffer_init_from_key(
            &local_min_buffer, PROCESS_PRIVATE_HEAP_ID, local_min);
         core_range_iterator_deinit(range_itor);
         if (!SUCCESS(rc)) {
            return rc;
         }
         local_min = key_buffer_key(&local_min_buffer);
         rc        = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       less_than,
                                       local_min);
         key_buffer_deinit(&local_min_buffer);
         if (!SUCCESS(rc)) {
            return rc;
         }

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
   range_itor->can_prev = TRUE;
   range_itor->can_next = iterator_can_next(&range_itor->merge_itor->super);
   if (!range_itor->can_next) {
      KEY_CREATE_LOCAL_COPY(rc,
                            min_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            max_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            local_max_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->local_max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }

      // if there is more data to get, rebuild the iterator for next leaf
      if (core_range_iterator_has_next_leaf(range_itor)) {
         core_handle *spl                = range_itor->spl;
         comparison   min_key_comparison = range_itor->min_key_comparison;
         comparison   max_key_comparison = range_itor->max_key_comparison;
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       greater_than_or_equal,
                                       local_max_key);
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
   range_itor->can_next = TRUE;
   range_itor->can_prev = iterator_can_prev(&range_itor->merge_itor->super);
   if (!range_itor->can_prev) {
      KEY_CREATE_LOCAL_COPY(rc,
                            min_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            max_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            local_min_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->local_min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }

      // if there is more data to get, rebuild the iterator for prev leaf
      if (core_key_compare(range_itor->spl, local_min_key, min_key) > 0) {
         core_handle *spl                = range_itor->spl;
         comparison   min_key_comparison = range_itor->min_key_comparison;
         comparison   max_key_comparison = range_itor->max_key_comparison;
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       less_than,
                                       local_min_key);
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
      merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &range_itor->merge_itor);
   }
   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      btree_iterator *btree_itor = &range_itor->btree_itor[i];
      if (range_itor->btree_itor_initialized[i]) {
         btree_iterator_deinit(btree_itor);
         range_itor->btree_itor_initialized[i] = FALSE;
      }
      if (range_itor->compacted[i]) {
         btree_dec_ref(spl->cc,
                       spl->cfg.btree_cfg,
                       range_itor->branch[i].addr,
                       PAGE_TYPE_BRANCH);
      } else {
         core_memtable_dec_ref(spl, range_itor->branch[i].addr);
      }
   }
   key_buffer_deinit(&range_itor->min_key);
   key_buffer_deinit(&range_itor->max_key);
   key_buffer_deinit(&range_itor->local_min_key);
   key_buffer_deinit(&range_itor->local_max_key);
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
core_insert(core_handle   *spl,
            key            tuple_key,
            message        data,
            lookup_result *old_result)
{
   timestamp      ts;
   const threadid tid = platform_get_tid();
   if (spl->cfg.use_stats) {
      ts = platform_get_timestamp();
   }

   if (message_class(data) == MESSAGE_TYPE_DELETE) {
      data = DELETE_MESSAGE;
   }

   if (old_result != NULL) {
      lookup_result_reset(old_result);
   }

   uint64          generation;
   memtable       *mt = NULL;
   platform_status rc = core_begin_memtable_insert(spl, &generation, &mt);
   if (!SUCCESS(rc)) {
      goto out;
   }

   btree_insert_results insert_results;
   btree_insert_results_init(&insert_results, old_result);
   rc = memtable_insert(&spl->mt_ctxt,
                        mt,
                        PROCESS_PRIVATE_HEAP_ID,
                        tuple_key,
                        data,
                        &insert_results);
   if (!SUCCESS(rc)) {
      goto end_insert;
   }

   rc = core_log_insert(spl, generation, tuple_key, data, &insert_results);
   if (!SUCCESS(rc)) {
      goto end_insert;
   }

   if (old_result != NULL) {
      if (lookup_result_should_continue(old_result)) {
         memtable_begin_lookup(&spl->mt_ctxt);
         memtable_end_insert(&spl->mt_ctxt);
         // Passing generation - 1 is allowed here
         rc = core_lookup_from_memtable_generation_locked(
            spl, generation - 1, tuple_key, old_result);
         if (!SUCCESS(rc)) {
            goto deinit_insert_results;
         }
      } else {
         memtable_end_insert(&spl->mt_ctxt);
         lookup_result_finalize(old_result, tuple_key);
      }
   } else {
      memtable_end_insert(&spl->mt_ctxt);
   }

deinit_insert_results:
   btree_insert_results_deinit(&insert_results);

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

end_insert:
   btree_insert_results_deinit(&insert_results);
   memtable_end_insert(&spl->mt_ctxt);
   return rc;
}

platform_status
core_optimize(core_handle             *spl,
              key                      minkey,
              key                      maxkey,
              bool32                   full_leaf_compactions,
              splinterdb_notification *notification)
{
   if (key_is_null(minkey) || key_is_null(maxkey)) {
      return STATUS_BAD_PARAM;
   }

   int cmp = data_key_compare(spl->cfg.data_cfg, minkey, maxkey);
   if (cmp > 0) {
      return STATUS_BAD_PARAM;
   }
   if (cmp == 0) {
      splinterdb_notification_complete(notification, STATUS_OK);
      return STATUS_OK;
   }

   return trunk_optimize(
      &spl->trunk_context, minkey, maxkey, full_leaf_compactions, notification);
}

// If any change is made in here, please make similar change in
// core_lookup_async
platform_status
core_lookup(core_handle *spl, key target, lookup_result *result)
{
   lookup_result_reset(result);

   memtable_begin_lookup(&spl->mt_ctxt);
   uint64          mt_gen_start = memtable_generation(&spl->mt_ctxt);
   platform_status rc           = core_lookup_from_memtable_generation_locked(
      spl, mt_gen_start, target, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (spl->cfg.use_stats) {
      threadid tid = platform_get_tid();
      if (lookup_result_found(result)) {
         spl->stats[tid].lookups_found++;
      } else {
         spl->stats[tid].lookups_not_found++;
      }
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

   lookup_result_reset(state->result);

   memtable_begin_lookup(&state->spl->mt_ctxt);
   uint64          mt_gen_start = memtable_generation(&state->spl->mt_ctxt);
   bool32          found_final;
   platform_status rc = core_lookup_memtables_locked(
      state->spl, mt_gen_start, state->target, state->result, &found_final);
   platform_assert_status_ok(rc);
   if (found_final) {
      memtable_end_lookup(&state->spl->mt_ctxt);
      goto found_final_answer;
   }

   rc = trunk_init_root_handle(&state->spl->trunk_context, &state->root_handle);
   // release memtable lookup lock before we handle any errors
   memtable_end_lookup(&state->spl->mt_ctxt);
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

found_final_answer:

   lookup_result_finalize(state->result, state->target);

   if (state->spl->cfg.use_stats) {
      threadid tid = platform_get_tid();
      if (lookup_result_found(state->result)) {
         state->spl->stats[tid].lookups_found++;
      } else {
         state->spl->stats[tid].lookups_not_found++;
      }
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
   if (range_itor == NULL) {
      platform_error_log("core_apply_to_range: failed to allocate range "
                         "iterator for %lu tuples\n",
                         num_tuples);
      return STATUS_NO_MEMORY;
   }

   platform_status rc = core_range_iterator_init(spl,
                                                 range_itor,
                                                 greater_than_or_equal,
                                                 start_key,
                                                 less_than,
                                                 POSITIVE_INFINITY_KEY,
                                                 greater_than_or_equal,
                                                 start_key);
   if (!SUCCESS(rc)) {
      platform_error_log("core_apply_to_range: range iterator init failed: "
                         "%s\n",
                         platform_status_to_string(rc));
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
         platform_error_log("core_apply_to_range: iterator_next failed: %s\n",
                            platform_status_to_string(rc));
         goto destroy_range_itor;
      }
   }

destroy_range_itor:
   core_range_iterator_deinit(range_itor);
   platform_free(PROCESS_PRIVATE_HEAP_ID, range_itor);
   return rc;
}

static void
core_stats_destroy(platform_heap_id heap_id, core_stats *stats)
{
   if (stats == NULL) {
      return;
   }

   for (uint64 i = 0; i < MAX_THREADS; i++) {
      histogram_destroy(heap_id, stats[i].insert_latency_histo);
      histogram_destroy(heap_id, stats[i].update_latency_histo);
      histogram_destroy(heap_id, stats[i].delete_latency_histo);
   }
   platform_free(heap_id, stats);
}

static core_stats *
core_stats_create(platform_heap_id heap_id)
{
   core_stats *stats = TYPED_ARRAY_ZALLOC(heap_id, stats, MAX_THREADS);
   if (stats == NULL) {
      platform_error_log("core_stats_create: failed to allocate stats array "
                         "for %u threads\n",
                         MAX_THREADS);
      return NULL;
   }

   for (uint64 i = 0; i < MAX_THREADS; i++) {
      stats[i].insert_latency_histo = histogram_create(
         heap_id, LATENCYHISTO_SIZE + 1, latency_histo_buckets);
      if (stats[i].insert_latency_histo == NULL) {
         platform_error_log("core_stats_create: failed to allocate insert "
                            "latency histogram for thread %lu\n",
                            i);
         goto cleanup;
      }
      stats[i].update_latency_histo = histogram_create(
         heap_id, LATENCYHISTO_SIZE + 1, latency_histo_buckets);
      if (stats[i].update_latency_histo == NULL) {
         platform_error_log("core_stats_create: failed to allocate update "
                            "latency histogram for thread %lu\n",
                            i);
         goto cleanup;
      }
      stats[i].delete_latency_histo = histogram_create(
         heap_id, LATENCYHISTO_SIZE + 1, latency_histo_buckets);
      if (stats[i].delete_latency_histo == NULL) {
         platform_error_log("core_stats_create: failed to allocate delete "
                            "latency histogram for thread %lu\n",
                            i);
         goto cleanup;
      }
   }

   return stats;

cleanup:
   core_stats_destroy(heap_id, stats);
   return NULL;
}

static platform_status
core_create_stats(core_handle *spl)
{
   if (!spl->cfg.use_stats) {
      return STATUS_OK;
   }

   spl->stats = core_stats_create(spl->heap_id);
   return spl->stats == NULL ? STATUS_NO_MEMORY : STATUS_OK;
}

static void
core_destroy_stats(core_handle *spl)
{
   core_stats_destroy(spl->heap_id, spl->stats);
   spl->stats = NULL;
}


/* Format the disk and mount the database */
platform_status
core_mkfs(core_handle      *spl,
          core_config      *cfg,
          allocator        *al,
          cache            *cc,
          io_handle        *io,
          task_system      *ts,
          allocator_root_id id,
          platform_heap_id  hid)
{
   ZERO_CONTENTS(spl);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   platform_status rc = core_locks_init(spl);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "core_mkfs: lock initialization failed: %s\n",
         platform_status_to_string(rc));
      return rc;
   }

   // Fresh superblock: geometry, empty tree table, allocation state invalid.
   allocator_config *allocator_cfg = allocator_get_config(al);
   rc = superblock_context_init(&spl->superblock, io, allocator_cfg, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: superblock_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_locks;
   }
   rc = superblock_format(&spl->superblock, allocator_cfg);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: superblock_format failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_superblock;
   }

   // set up the memtable context
   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   rc                      = memtable_context_init(&spl->mt_ctxt,
                              spl->heap_id,
                              cc,
                              mt_cfg,
                              core_rotate_log,
                              core_memtable_flush_virtual,
                              spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: memtable_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_superblock;
   }

   // set up the log
   if (spl->cfg.use_log) {
      spl->log = shard_log_create(
         cc, (shard_log_config *)spl->cfg.log_cfg, spl->heap_id);
      if (spl->log == NULL) {
         platform_error_log("core_mkfs: shard_log_create failed\n");
         rc = STATUS_NO_MEMORY;
         goto deinit_memtable_context;
      }
   }

   rc = trunk_context_init(&spl->trunk_context,
                           spl->cfg.trunk_node_cfg,
                           hid,
                           cc,
                           al,
                           ts,
                           (trunk_snapshot){.root_addr = 0});
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: trunk_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_log;
   }

   rc = core_create_stats(spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: core_create_stats failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_trunk_context;
   }

   /*
    * Establish the initial (empty) tree record and install this instance's log,
    * then publish both durably in one write.  The formatted image has no live
    * log, so the cut leaves the sealed slot empty; the log covers generations
    * from 0.
    */
   if (spl->cfg.use_log) {
      superblock_log_cut(
         &spl->superblock,
         core_log_to_superblock_log_head(log_get_head(spl->log), 0));
   }
   rc = core_checkpoint_commit_current_root(spl);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "core_mkfs: core_checkpoint_commit_current_root failed: %s\n",
         platform_status_to_string(rc));
      goto deinit_stats;
   }
   return STATUS_OK;

deinit_stats:
   core_destroy_stats(spl);
deinit_trunk_context:
   trunk_context_deinit(&spl->trunk_context);
deinit_log:
   if (spl->cfg.use_log) {
      platform_free(spl->heap_id, spl->log);
      spl->log = NULL;
   }
deinit_memtable_context:
   memtable_context_deinit(&spl->mt_ctxt);
deinit_superblock:
   superblock_context_deinit(&spl->superblock);
deinit_locks:
   core_locks_deinit(spl);
   return rc;
}

/*
 * Open (mount) an existing splinter database
 */
platform_status
core_mount(core_handle      *spl,
           core_config      *cfg,
           allocator        *al,
           cache            *cc,
           io_handle        *io,
           task_system      *ts,
           allocator_root_id id,
           platform_heap_id  hid)
{
   ZERO_CONTENTS(spl);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   platform_status rc = core_locks_init(spl);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "core_mount: lock initialization failed: %s\n",
         platform_status_to_string(rc));
      return rc;
   }

   // Read the superblock (newest valid A/B copy; validates geometry).
   allocator_config *allocator_cfg = allocator_get_config(al);
   rc = superblock_context_init(&spl->superblock, io, allocator_cfg, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: superblock_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_locks;
   }
   rc = superblock_mount(&spl->superblock, allocator_cfg);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: superblock_mount failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_superblock;
   }

   superblock_tree_record rec;
   superblock_get_tree_record(&spl->superblock, &rec);

   /*
    * Preserve the historical clean-only mount rule for this first format
    * slice: crash recovery (log replay + allocator rebuild) is not wired yet,
    * so only a clean at-rest instance -- one whose allocation state is still
    * valid -- may supply the root.  A valid allocation state is published only
    * at the end of a clean unmount, so it is the single at-rest signal (no
    * separate per-tree clean flag is needed; see superblock.h).
    */
   bool32 rebuild = !superblock_allocation_state_valid(&spl->superblock);
   if (rebuild) {
      platform_error_log("core_mount: root id %lu requires crash recovery\n",
                         spl->id);
      rc = STATUS_INVALID_STATE;
      goto deinit_superblock;
   }

   /*
    * Load the trusted refcount map (rebuild == FALSE on this path).  This must
    * precede trunk_snapshot_create_from_addr(), which increments the root's
    * refcount in the now-loaded map.
    */
   rc = allocator_load_refcounts(al);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: allocator_load_refcounts failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_superblock;
   }

   uint64 root_addr = rec.root_addr;
   // The record already stores the first unincorporated generation, which is
   // exactly where the memtable resumes (0 for a fresh, never-incorporated db).
   uint64 resume_generation = rec.first_unincorporated_generation;

   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   rc                      = memtable_context_init_at_generation(&spl->mt_ctxt,
                                            spl->heap_id,
                                            cc,
                                            mt_cfg,
                                            core_rotate_log,
                                            core_memtable_flush_virtual,
                                            spl,
                                            resume_generation);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: memtable_context_init_at_generation "
                         "failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_superblock;
   }

   if (spl->cfg.use_log) {
      spl->log = shard_log_create(
         cc, (shard_log_config *)spl->cfg.log_cfg, spl->heap_id);
      if (spl->log == NULL) {
         platform_error_log("core_mount: shard_log_create failed\n");
         rc = STATUS_NO_MEMORY;
         goto deinit_memtable_context;
      }
   }

   trunk_snapshot root_snapshot;
   rc = trunk_snapshot_create_from_addr(al, root_addr, &root_snapshot);
   if (!SUCCESS(rc)) {
      platform_error_log(
         "core_mount: trunk_snapshot_create_from_addr failed: %s\n",
         platform_status_to_string(rc));
      goto deinit_log;
   }

   rc = trunk_context_init(&spl->trunk_context,
                           spl->cfg.trunk_node_cfg,
                           hid,
                           cc,
                           al,
                           ts,
                           root_snapshot);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: trunk_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_log;
   }

   rc = core_create_stats(spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: core_create_stats failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_trunk_context;
   }

   /*
    * Mark dirty: cut this session's fresh live log and invalidate the persisted
    * allocation state, before any allocation diverges the in-memory map from
    * disk.  A crash after this forces the next mount into recovery instead of
    * silently reverting to this now-stale root.  The root is unchanged; a clean
    * mount has no prior live log, so the sealed slot stays empty.  This
    * session's log receives generations from the resume generation onward.
    */
   superblock_log_cut(&spl->superblock,
                      spl->cfg.use_log
                         ? core_log_to_superblock_log_head(
                              log_get_head(spl->log), resume_generation)
                         : (superblock_log_head){0});
   rc = superblock_make_durable(&spl->superblock);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: mark-dirty superblock_make_durable "
                         "failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_stats;
   }
   return STATUS_OK;

deinit_stats:
   core_destroy_stats(spl);
deinit_trunk_context:
   trunk_context_deinit(&spl->trunk_context);
deinit_log:
   if (spl->cfg.use_log) {
      platform_free(spl->heap_id, spl->log);
      spl->log = NULL;
   }
deinit_memtable_context:
   memtable_context_deinit(&spl->mt_ctxt);
deinit_superblock:
   superblock_context_deinit(&spl->superblock);
deinit_locks:
   core_locks_deinit(spl);
   return rc;
}

static bool32
core_report_unincorporated_memtables(core_handle *spl)
{
   bool32 found_unincorporated = FALSE;
   uint64 start_generation     = memtable_generation_retired(&spl->mt_ctxt) + 1;
   uint64 end_generation       = memtable_generation(&spl->mt_ctxt);

   for (uint64 generation = start_generation; generation < end_generation;
        generation++)
   {
      memtable *mt = core_try_get_memtable(spl, generation);
      if (mt == NULL || mt->state == MEMTABLE_STATE_READY) {
         continue;
      }

      found_unincorporated = TRUE;
      uint64 compacted_root =
         core_memtable_compacted_branch_root(spl, generation);
      if (mt->state == MEMTABLE_STATE_INCORPORATION_FAILED) {
         platform_error_log("Shutdown found memtable from failed "
                            "incorporation: generation=%lu state=%s "
                            "status=%s memtable_root=%lu "
                            "compacted_root=%lu\n",
                            generation,
                            memtable_state_string(mt->state),
                            platform_status_to_string(mt->incorporation_status),
                            mt->root_addr,
                            compacted_root);
      } else {
         platform_error_log("Shutdown found unincorporated memtable: "
                            "generation=%lu state=%s memtable_root=%lu "
                            "compacted_root=%lu\n",
                            generation,
                            memtable_state_string(mt->state),
                            mt->root_addr,
                            compacted_root);
      }

      if (compacted_root != 0) {
         core_memtable_release_compacted_branch(spl, generation);
      }
   }

   return found_unincorporated;
}

/*
 * This function is only safe to call when all other calls to spl have returned.
 * It intentionally leaves the memtable and log contexts live: the clean
 * checkpoint record needs both after final incorporation has quiesced.
 */
static void
core_quiesce_for_shutdown(core_handle *spl)
{
   // write current memtable to disk
   // (any others must already be flushing/flushed)

   if (!memtable_is_empty(&spl->mt_ctxt)) {
      /*
       * memtable_force_rotation is not thread safe.  It dispatches the flush
       * itself (via the process callback), which also resolves any checkpoint
       * log cut its rotate hook just made.  That callback may arm a fresh
       * checkpoint; core_checkpoint_cleanup_for_shutdown() discards it.
       */
      memtable_force_rotation(&spl->mt_ctxt);
   }

   // finish any outstanding tasks and destroy task system for this table.
   platform_status rc = task_perform_until_quiescent(spl->ts);
   platform_assert_status_ok(rc);

   core_report_unincorporated_memtables(spl);
}

/*
 * Seal the live log at shutdown -- finalizes its pages and frees the handle --
 * and return its identity so the caller can free the extents with log_dec_ref()
 * once the cache is flushed.  A clean shutdown has folded everything into the
 * durable root, so the live log is fully incorporated and discardable.  Returns
 * an empty descriptor when logging is disabled.
 */
static log_head
core_seal_live_log(core_handle *spl)
{
   if (!spl->cfg.use_log || spl->log == NULL) {
      return (log_head){0};
   }
   log_head info = log_get_head(spl->log);
   log_seal(spl->log);
   spl->log = NULL;
   return info;
}

/*
 * Take a checkpoint: make every modification that completed before this call
 * durable in the trunk root, reclaiming the retired log's space, and block
 * until that is done.  Safe to call on a running system with concurrent
 * inserts.
 *
 * Durability reduces to a generation bound.  Inserts land in the currently
 * active memtable generation and generations only advance, so everything
 * already inserted is in a generation at or below the one active at entry --
 * the target. Once the target is incorporated, all of it is in the trunk's COW
 * root, and committing that root makes it durable.
 *
 * Reclaiming log space needs a log cut, which happens only when a rotation
 * finds a checkpoint armed.  So we arm one up front and then see it through:
 * its completion frees the retired log's extents.  This is what lets an
 * application turn the interval policy off and manage log space entirely
 * through this call. Arming is best effort -- only one checkpoint can be in
 * flight, so if one already is we ride it out and leave reclamation to it.
 * Durability does not depend on any of this.
 *
 * Both halves wait on a rotation: the target cannot be incorporated until
 * something finalizes it, and the cut cannot happen without one either.  Insert
 * traffic normally provides it; if none arrives within rotation_timeout_ns we
 * force one.  Forcing is safe even with an empty memtable -- that generation
 * retires with no branch at all (see core_memtable_compact()) -- but note that
 * a caller polling a quiescent database will force a rotation per call,
 * spending a generation and a log extent each time.  A longer timeout lets real
 * traffic drive the cut instead.
 */
platform_status
core_checkpoint(core_handle *spl, uint64 rotation_timeout_ns)
{
   uint64 target = memtable_generation(&spl->mt_ctxt);

   bool32 armed = core_checkpoint_begin(spl, TRUE /* force */);

   uint64    wait     = 100;
   timestamp deadline = platform_get_timestamp();
   while (TRUE) {
      bool32 incorporated =
         memtable_generation_retired(&spl->mt_ctxt) + 1 > target;
      core_checkpoint_phase phase = core_checkpoint_phase_get(spl);
      /*
       * Done once the target is durable-able and, if we started a checkpoint,
       * it has completed (which is what freed the retired log).  We only wait
       * on a checkpoint we armed ourselves, so an unrelated one cannot hold us
       * up.
       */
      if (incorporated && (!armed || phase == CORE_CHECKPOINT_IDLE)) {
         break;
      }

      /*
       * Force a rotation if either half is still waiting on one: the target is
       * still the active generation, or our checkpoint has yet to cut.  Once
       * neither holds, the remaining work is just draining flushes.
       */
      bool32 needs_rotation = memtable_generation(&spl->mt_ctxt) == target
                              || (armed && phase == CORE_CHECKPOINT_PENDING);
      if (needs_rotation
          && rotation_timeout_ns <= platform_timestamp_elapsed(deadline))
      {
         memtable_force_rotation(&spl->mt_ctxt); // dispatches the flush itself
         deadline = platform_get_timestamp();
      }

      task_perform_one_if_needed(spl->ts, 0);
      platform_sleep_ns(wait);
      wait = wait > 2048 ? wait : 2 * wait;
   }

   /*
    * Advance the durable root.  The log slots are left exactly as the cut
    * protocol published them, so this cannot orphan a log; snapshot_tree only
    * drops the sealed log if this root already covers it.  A completed
    * checkpoint has already committed an equivalent root -- republishing is
    * harmless -- and this is still required when nothing was armed (or logging
    * is off).
    */
   return core_checkpoint_commit_current_root(spl);
}

/*
 * Close (unmount) a database without destroying it.
 * It can be re-opened later with core_mount().
 */
platform_status
core_unmount(core_handle *spl)
{
   platform_status rc;

   /*
    * Quiescing leaves the memtable and log contexts live so publication can
    * atomically capture the retired generation and root.  Teardown is safe
    * regardless of publication success.
    */
   core_quiesce_for_shutdown(spl);

   // Reclaim any in-flight checkpoint's logs before the unmount publish.
   core_checkpoint_cleanup_for_shutdown(spl);

   /*
    * The clean-unmount root incorporates everything, so the live log is fully
    * folded and discardable.  Seal it (frees the handle) now; free its extents
    * after the cache flush below.
    */
   log_head live_log = core_seal_live_log(spl);

   /*
    * Part A: publish the clean-unmount root with both log slots cleared (no
    * live or sealed log at rest -- their extents are freed just below, so no
    * reference to them may survive).  Both transitions go out in the single
    * publish the commit performs.  Allocation state stays invalid here; it
    * becomes valid only in Part B, after the map is persisted.
    */
   superblock_discard_logs(&spl->superblock);
   rc = core_checkpoint_commit_current_root(spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_unmount: failed to publish unmount root: %s\n",
                         platform_status_to_string(rc));
   }

   // Keep this after publication above: it supplies the generation cut.
   memtable_context_deinit(&spl->mt_ctxt);

   // Flush all dirty pages.  The live log has already been sealed (above); free
   // its extents now that the cache is flushed, before the map is persisted
   // (Part B) so the persisted map reflects the free.
   cache_flush(spl->cc);
   log_dec_ref(spl->cc, &live_log);
   /*
    * Release the context's live root reference before persisting the map, so
    * the persisted refcounts reflect exactly the durable record's single
    * reference to the root.
    */
   trunk_context_deinit(&spl->trunk_context);

   /*
    * Part B: only after a clean Part A publish, persist the refcount map and
    * republish a valid allocation state pointing at it.  The map is made
    * durable before the "map is trustworthy" flag, and that flag becomes
    * durable only after the root it must agree with (Part A).  On a Part A
    * failure we leave the allocation state invalid so the next open rebuilds.
    */
   if (SUCCESS(rc)) {
      uint64          map_addr;
      platform_status prc = allocator_persist(spl->al, &map_addr);
      if (SUCCESS(prc)) {
         superblock_snapshot_allocator(&spl->superblock, map_addr);
         prc = superblock_make_durable(&spl->superblock);
      }
      if (!SUCCESS(prc)) {
         platform_error_log("core_unmount: failed to publish clean allocation "
                            "state: %s\n",
                            platform_status_to_string(prc));
         rc = prc;
      }
   }

   superblock_context_deinit(&spl->superblock);
   core_destroy_stats(spl);
   core_locks_deinit(spl);
   return rc;
}

/*
 * Destroy a database such that it cannot be re-opened later
 */
void
core_destroy(core_handle *spl)
{
   core_quiesce_for_shutdown(spl);

   // Reclaim any in-flight checkpoint's logs before teardown.
   core_checkpoint_cleanup_for_shutdown(spl);

   /*
    * Release the reference the published tree record holds on its root before
    * tearing down the context (which releases the context's own live root
    * reference).  Together these free the whole tree.  Release must precede
    * trunk_context_deinit(): trunk_snapshot_release() needs the live context.
    */
   superblock_tree_record rec;
   superblock_get_tree_record(&spl->superblock, &rec);
   if (rec.root_addr != 0) {
      trunk_snapshot  old_snapshot = {.root_addr = rec.root_addr};
      platform_status rc =
         trunk_snapshot_release(&spl->trunk_context, &old_snapshot);
      if (!SUCCESS(rc)) {
         platform_error_log(
            "core_destroy: failed to release root addr %lu: %s\n",
            rec.root_addr,
            platform_status_to_string(rc));
      }
   }

   // Discard the live log too: seal (frees the handle), then free its extents
   // after the cache flush.
   log_head live_log = core_seal_live_log(spl);
   memtable_context_deinit(&spl->mt_ctxt);
   cache_flush(spl->cc);
   log_dec_ref(spl->cc, &live_log);
   trunk_context_deinit(&spl->trunk_context);

   /*
    * A destroyed instance must not be reopened.  The mount-time mark-dirty
    * already left the on-disk allocation state invalid, so we deliberately do
    * not persist the map or publish a valid state here: the freed root and its
    * subtree stay unreachable, and the next mount rejects the device
    * (allocation state invalid) rather than trusting a now-freed root.
    */
   superblock_context_deinit(&spl->superblock);
   core_destroy_stats(spl);
   core_locks_deinit(spl);
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
 * Print this instance's superblock tree record and the persisted allocation
 * state.
 */
void
core_print_super_block(platform_log_handle *log_handle, core_handle *spl)
{
   superblock_tree_record rec;
   superblock_get_tree_record(&spl->superblock, &rec);

   platform_log(log_handle,
                "Superblock tree record root_id=%lu {\n"
                "  root_addr=%lu first_unincorporated_generation=%lu\n"
                "  live_log:   meta_addr=%lu addr=%lu magic=%lu\n"
                "  sealed_log: meta_addr=%lu addr=%lu magic=%lu\n"
                "  allocation_state: %s (addr=%lu)\n"
                "}\n\n",
                spl->id,
                rec.root_addr,
                rec.first_unincorporated_generation,
                rec.live_log.meta_addr,
                rec.live_log.addr,
                rec.live_log.magic,
                rec.sealed_log.meta_addr,
                rec.sealed_log.addr,
                rec.sealed_log.magic,
                superblock_allocation_state_valid(&spl->superblock) ? "valid"
                                                                    : "invalid",
                superblock_allocation_state_addr(&spl->superblock));
}

// clang-format off
void
core_print_insertion_stats(platform_log_handle *log_handle, const core_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   uint64 avg_flush_wait_time, avg_flush_time, num_flushes;
   uint64 avg_compaction_tuples, pack_time_per_tuple, avg_setup_time;
   threadid thr_i;

   core_stats *global;

   global = TYPED_ZALLOC(PROCESS_PRIVATE_HEAP_ID, global);
   if (global == NULL) {
      platform_error_log("Out of memory for statistics");
      return;
   }

   histogram *insert_lat_accum;
   histogram *update_lat_accum;
   histogram *delete_lat_accum;
   insert_lat_accum =
      histogram_create(PROCESS_PRIVATE_HEAP_ID,
                       LATENCYHISTO_SIZE + 1,
                       latency_histo_buckets);
   update_lat_accum =
      histogram_create(PROCESS_PRIVATE_HEAP_ID,
                       LATENCYHISTO_SIZE + 1,
                       latency_histo_buckets);
   delete_lat_accum =
      histogram_create(PROCESS_PRIVATE_HEAP_ID,
                       LATENCYHISTO_SIZE + 1,
                       latency_histo_buckets);
   if (insert_lat_accum == NULL || update_lat_accum == NULL
       || delete_lat_accum == NULL)
   {
      platform_error_log("Out of memory for statistics\n");
      histogram_destroy(PROCESS_PRIVATE_HEAP_ID, insert_lat_accum);
      histogram_destroy(PROCESS_PRIVATE_HEAP_ID, update_lat_accum);
      histogram_destroy(PROCESS_PRIVATE_HEAP_ID, delete_lat_accum);
      platform_free(PROCESS_PRIVATE_HEAP_ID, global);
      return;
   }

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
   histogram_destroy(PROCESS_PRIVATE_HEAP_ID, insert_lat_accum);
   histogram_destroy(PROCESS_PRIVATE_HEAP_ID, update_lat_accum);
   histogram_destroy(PROCESS_PRIVATE_HEAP_ID, delete_lat_accum);


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
   platform_free(PROCESS_PRIVATE_HEAP_ID, global);
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
   lookup_result lookup;
   lookup_result_init(
      &lookup, spl->cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   platform_stream_handle stream;
   platform_open_log_stream(&stream);
   uint64 mt_gen_start = memtable_generation(&spl->mt_ctxt);
   uint64 mt_gen_end   = memtable_generation_retired(&spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      bool32 memtable_is_compacted;
      uint64 root_addr = core_memtable_root_addr_for_lookup(
         spl, mt_gen, &memtable_is_compacted, NULL);
      if (memtable_is_compacted && root_addr == 0) {
         continue; // empty generation: no branch to look up in
      }
      platform_status rc;

      rc = btree_lookup(spl->cc,
                        spl->cfg.btree_cfg,
                        root_addr,
                        PAGE_TYPE_MEMTABLE,
                        target,
                        &lookup);
      platform_assert_status_ok(rc);
      if (lookup_result_found(&lookup)) {
         char    key_str[128];
         char    message_str[128];
         message msg =
            merge_accumulator_to_message(lookup_result_accumulator(&lookup));
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
   trunk_merge_lookup(
      &spl->trunk_context, &handle, target, &lookup, log_handle);
   trunk_ondisk_node_handle_deinit(&handle);
   lookup_result_deinit(&lookup);
}

void
core_reset_stats(core_handle *spl)
{
   if (spl->cfg.use_stats) {
      core_stats *new_stats = core_stats_create(spl->heap_id);
      if (new_stats == NULL) {
         platform_error_log("core_reset_stats: failed to reset stats: %s\n",
                            platform_status_to_string(STATUS_NO_MEMORY));
         return;
      }

      core_destroy_stats(spl);
      spl->stats = new_stats;
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
                 uint64               prefetch_budget,
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
   core_cfg->prefetch_budget         = prefetch_budget;
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
