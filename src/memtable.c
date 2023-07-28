// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *-----------------------------------------------------------------------------
 * memtable.c --
 *
 *     This file contains the implementation for the splinter memtable.
 *-----------------------------------------------------------------------------
 */

#include "platform.h"
#include "memtable.h"

#include "poison.h"

#define MEMTABLE_COUNT_GRANULARITY 128

#define MEMTABLE_INSERT_LOCK_IDX 0
#define MEMTABLE_LOOKUP_LOCK_IDX 1

bool32
memtable_is_full(const memtable_config *cfg, memtable *mt)
{
   return cfg->max_extents_per_memtable <= mini_num_extents(&mt->mini);
}

bool32
memtable_is_empty(memtable_context *ctxt)
{
   return ctxt->is_empty;
}

static inline void
memtable_mark_empty(memtable_context *ctxt)
{
   ctxt->is_empty = TRUE;
}

static inline void
memtable_process(memtable_context *ctxt, uint64 generation)
{
   ctxt->process(ctxt->process_ctxt, generation);
}

static inline void
memtable_begin_insert(memtable_context *ctxt)
{
   platform_batch_rwlock_get(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
}

void
memtable_end_insert(memtable_context *ctxt)
{
   platform_batch_rwlock_unget(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
}

static inline bool32
memtable_try_begin_insert_rotation(memtable_context *ctxt)
{
   if (!platform_batch_rwlock_try_claim(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX))
   {
      return FALSE;
   }
   platform_batch_rwlock_lock(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
   return TRUE;
}

static inline void
memtable_end_insert_rotation(memtable_context *ctxt)
{
   platform_batch_rwlock_unlock(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
   platform_batch_rwlock_unclaim(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
}

static inline void
memtable_begin_raw_rotation(memtable_context *ctxt)
{
   platform_batch_rwlock_get(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
   platform_batch_rwlock_claim_loop(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
   platform_batch_rwlock_lock(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
}

static inline void
memtable_end_raw_rotation(memtable_context *ctxt)
{
   platform_batch_rwlock_full_unlock(ctxt->rwlock, MEMTABLE_INSERT_LOCK_IDX);
}

void
memtable_begin_lookup(memtable_context *ctxt)
{
   platform_batch_rwlock_get(ctxt->rwlock, MEMTABLE_LOOKUP_LOCK_IDX);
}

void
memtable_end_lookup(memtable_context *ctxt)
{
   platform_batch_rwlock_unget(ctxt->rwlock, MEMTABLE_LOOKUP_LOCK_IDX);
}

void
memtable_block_lookups(memtable_context *ctxt)
{
   platform_batch_rwlock_get(ctxt->rwlock, MEMTABLE_LOOKUP_LOCK_IDX);
   platform_batch_rwlock_claim_loop(ctxt->rwlock, MEMTABLE_LOOKUP_LOCK_IDX);
   platform_batch_rwlock_lock(ctxt->rwlock, MEMTABLE_LOOKUP_LOCK_IDX);
}

void
memtable_unblock_lookups(memtable_context *ctxt)
{
   platform_batch_rwlock_full_unlock(ctxt->rwlock, MEMTABLE_LOOKUP_LOCK_IDX);
}


platform_status
memtable_maybe_rotate_and_begin_insert(memtable_context *ctxt,
                                       uint64           *generation)
{
   uint64 wait = 100;
   while (TRUE) {
      memtable_begin_insert(ctxt);
      uint64    current_generation = ctxt->generation;
      uint64    current_mt_no = current_generation % ctxt->cfg.max_memtables;
      memtable *current_mt    = &ctxt->mt[current_mt_no];
      if (current_mt->state != MEMTABLE_STATE_READY) {
         // The next memtable is not ready yet, back off and wait.
         memtable_end_insert(ctxt);
         platform_sleep_ns(wait);
         wait = wait > 2048 ? wait : 2 * wait;
         continue;
      }
      wait = 100;

      if (memtable_is_full(&ctxt->cfg, current_mt)) {
         // If the current memtable is full, try to retire it

         uint64    next_generation = current_generation + 1;
         uint64    next_mt_no      = next_generation % ctxt->cfg.max_memtables;
         memtable *next_mt         = &ctxt->mt[next_mt_no];
         if (next_mt->state != MEMTABLE_STATE_READY) {
            memtable_end_insert(ctxt);
            return STATUS_BUSY;
         }

         if (memtable_try_begin_insert_rotation(ctxt)) {
            // We successfully got the lock, so we do the finalization
            memtable_transition(
               current_mt, MEMTABLE_STATE_READY, MEMTABLE_STATE_FINALIZED);

            // Safe to increment non-atomically because we have a lock on
            // the insert lock
            ctxt->generation++;
            platform_assert(ctxt->generation - ctxt->generation_retired
                               <= ctxt->cfg.max_memtables,
                            "ctxt->generation: %lu, "
                            "ctxt->generation_retired: %lu, "
                            "current_generation: %lu\n",
                            ctxt->generation,
                            ctxt->generation_retired,
                            current_generation);
            platform_assert(current_generation + 1 == ctxt->generation,
                            "ctxt->generation: %lu, "
                            "ctxt->generation_retired: %lu, "
                            "current_generation: %lu\n",
                            ctxt->generation,
                            ctxt->generation_retired,
                            current_generation);

            memtable_mark_empty(ctxt);
            memtable_end_insert_rotation(ctxt);
            memtable_end_insert(ctxt);
            memtable_process(ctxt, current_generation);
         } else {
            memtable_end_insert(ctxt);
            platform_sleep_ns(wait);
            wait = wait > 2048 ? wait : 2 * wait;
         }
         continue;
      }
      *generation = current_generation;
      return STATUS_OK;
   }
}

/*
 *-----------------------------------------------------------------------------
 * Increments the distributed tuple counter.  Must hold a read lock on
 * insert_lock.
 *
 * Add to local num_tuple counter. If at granularity, then increment the
 * global counter.
 *
 * There is no race because thread_num_tuples is never accessed by another
 * thread, it is only used by the current thread to determine when to change
 * the global counter.
 *
 * each thread pads num_tuples by MEMTABLE_COUNT_GRANULARITY whenever it
 * adds a tuple which takes its local count to
 * k * MEMTABLE_COUNT_GRANULARITY + 1. Therefore, num_tuples is an upper
 * bound except that each thread may still add 1 more tuple.
 *-----------------------------------------------------------------------------
 */
static inline void
memtable_add_tuple(memtable_context *ctxt)
{
   ctxt->is_empty = FALSE;
}

platform_status
memtable_insert(memtable_context *ctxt,
                memtable         *mt,
                platform_heap_id  heap_id,
                key               tuple_key,
                message           msg,
                uint64           *leaf_generation)
{
   const threadid tid = platform_get_tid();
   bool32         was_unique;

   platform_status rc = btree_insert(ctxt->cc,
                                     ctxt->cfg.btree_cfg,
                                     heap_id,
                                     &ctxt->scratch[tid],
                                     mt->root_addr,
                                     &mt->mini,
                                     tuple_key,
                                     msg,
                                     leaf_generation,
                                     &was_unique);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (was_unique) {
      memtable_add_tuple(ctxt);
   }

   return rc;
}

/*
 * if there are no outstanding refs, then destroy and reinit memtable and
 * transition to READY
 */
bool32
memtable_dec_ref_maybe_recycle(memtable_context *ctxt, memtable *mt)
{
   cache *cc = ctxt->cc;

   bool32 freed = btree_dec_ref(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
   if (freed) {
      platform_assert(mt->state == MEMTABLE_STATE_INCORPORATED);
      mt->root_addr = btree_create(cc, mt->cfg, &mt->mini, PAGE_TYPE_MEMTABLE);
      memtable_lock_incorporation_lock(ctxt);
      mt->generation += ctxt->cfg.max_memtables;
      memtable_unlock_incorporation_lock(ctxt);
      memtable_transition(
         mt, MEMTABLE_STATE_INCORPORATED, MEMTABLE_STATE_READY);
   }
   return freed;
}

uint64
memtable_force_finalize(memtable_context *ctxt)
{
   memtable_begin_raw_rotation(ctxt);

   uint64    generation = ctxt->generation;
   uint64    mt_no      = generation % ctxt->cfg.max_memtables;
   memtable *mt         = &ctxt->mt[mt_no];
   memtable_transition(mt, MEMTABLE_STATE_READY, MEMTABLE_STATE_FINALIZED);
   uint64 current_generation = ctxt->generation++;
   platform_assert(ctxt->generation - ctxt->generation_retired
                   <= ctxt->cfg.max_memtables);
   memtable_mark_empty(ctxt);

   memtable_end_raw_rotation(ctxt);
   return current_generation;
}

void
memtable_init(memtable *mt, cache *cc, memtable_config *cfg, uint64 generation)
{
   ZERO_CONTENTS(mt);
   mt->cfg       = cfg->btree_cfg;
   mt->root_addr = btree_create(cc, mt->cfg, &mt->mini, PAGE_TYPE_MEMTABLE);
   mt->state     = MEMTABLE_STATE_READY;
   platform_assert(generation < UINT64_MAX);
   mt->generation = generation;
}

void
memtable_deinit(cache *cc, memtable *mt)
{
   mini_release(&mt->mini, NULL_KEY);
   debug_only bool32 freed =
      btree_dec_ref(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
   debug_assert(freed);
}

memtable_context *
memtable_context_create(platform_heap_id hid,
                        cache           *cc,
                        memtable_config *cfg,
                        process_fn       process,
                        void            *process_ctxt)
{
   memtable_context *ctxt =
      TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, ctxt, mt, cfg->max_memtables);
   ctxt->cc = cc;
   memmove(&ctxt->cfg, cfg, sizeof(ctxt->cfg));

   platform_mutex_init(
      &ctxt->incorporation_mutex, platform_get_module_id(), hid);
   ctxt->rwlock = TYPED_MALLOC(hid, ctxt->rwlock);
   platform_batch_rwlock_init(ctxt->rwlock);

   for (uint64 mt_no = 0; mt_no < cfg->max_memtables; mt_no++) {
      uint64 generation = mt_no;
      memtable_init(&ctxt->mt[mt_no], cc, cfg, generation);
   }

   ctxt->generation                = 0;
   ctxt->generation_to_incorporate = 0;
   ctxt->generation_retired        = (uint64)-1;

   ctxt->is_empty = TRUE;

   ctxt->process      = process;
   ctxt->process_ctxt = process_ctxt;

   return ctxt;
}

void
memtable_context_destroy(platform_heap_id hid, memtable_context *ctxt)
{
   cache *cc = ctxt->cc;
   for (uint64 mt_no = 0; mt_no < ctxt->cfg.max_memtables; mt_no++) {
      memtable_deinit(cc, &ctxt->mt[mt_no]);
   }

   platform_mutex_destroy(&ctxt->incorporation_mutex);
   platform_free(hid, ctxt->rwlock);

   platform_free(hid, ctxt);
}

void
memtable_config_init(memtable_config *cfg,
                     btree_config    *btree_cfg,
                     uint64           max_memtables,
                     uint64           memtable_capacity)
{
   ZERO_CONTENTS(cfg);
   cfg->btree_cfg     = btree_cfg;
   cfg->max_memtables = max_memtables;
   cfg->max_extents_per_memtable =
      MEMTABLE_SPACE_OVERHEAD_FACTOR * memtable_capacity
      / cache_config_extent_size(btree_cfg->cache_cfg);
}
