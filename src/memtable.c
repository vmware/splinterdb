// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * memtable.c --
 *
 *     This file contains the implementation for the splinter memtable.
 */

#include "platform.h"
#include "memtable.h"

#include "poison.h"

#define MEMTABLE_COUNT_GRANULARITY 128

bool
memtable_is_full(memtable_context *ctxt)
{
   const threadid tid = platform_get_tid();
   uint64 thread_num_tuples_overcount =
      (
       MEMTABLE_COUNT_GRANULARITY -
      (ctxt->thread_num_tuples[tid].v % MEMTABLE_COUNT_GRANULARITY)
      ) % MEMTABLE_COUNT_GRANULARITY;
   uint64 upper_bound_num_tuples =
      ctxt->num_tuples - thread_num_tuples_overcount + MAX_THREADS;
   return upper_bound_num_tuples >= ctxt->cfg.max_tuples_per_memtable;
}

bool
memtable_is_empty(memtable_context *ctxt)
{
   return ctxt->num_tuples == 0;
}

static inline void
memtable_clear_num_tuples(memtable_context *ctxt)
{
   ZERO_ARRAY(ctxt->thread_num_tuples);
   ctxt->num_tuples = 0;
}

static inline void
memtable_process(memtable_context *ctxt,
                 uint64            generation)
{
   ctxt->process(ctxt->process_ctxt, generation);
}


platform_status
memtable_maybe_rotate_and_get_insert_lock(memtable_context  *ctxt,
                                          uint64            *generation,
                                          page_handle      **lock_page)
{
   cache *cc = ctxt->cc;
   uint64 lock_addr = ctxt->insert_lock_addr;
   uint64 wait = 100;
   while (TRUE) {
      *lock_page = cache_get(cc, lock_addr, TRUE, PAGE_TYPE_LOCK_NO_DATA);
      *generation = ctxt->generation;
      // FIXME: [aconway 2020-09-02] memtable_try_get, then replace the state
      // check with a mt != NULL check, and the state check should become an
      // assert
      uint64 mt_no = *generation % ctxt->cfg.max_memtables;
      memtable *mt = &ctxt->mt[mt_no];
      if (mt->state != MEMTABLE_STATE_READY) {
          // The next memtable is not ready yet, back off and wait.
         cache_unget(cc, *lock_page);
         platform_sleep(wait);
         wait = wait > 2048 ? wait: 2 * wait;
         continue;
      }

      if (memtable_is_full(ctxt)) {
         // If the current memtable is full, try to retire it.
         if (cache_claim(cc, *lock_page)) {
            // We successfully got the claim, so we do the finalization
            cache_lock(cc, *lock_page);
            memtable_transition(mt,
                                MEMTABLE_STATE_READY, MEMTABLE_STATE_FINALIZED);

            uint64 process_generation = ctxt->generation++;
            memtable_clear_num_tuples(ctxt);
            cache_unlock(cc, *lock_page);
            cache_unclaim(cc, *lock_page);
            cache_unget(cc, *lock_page);
            // FIXME: [aconway 2020-09-01] process
            memtable_process(ctxt, process_generation);
         } else {
            cache_unget(cc, *lock_page);
            platform_sleep(wait);
            wait *= 2;
         }
         continue;
      }
      *generation = ctxt->generation;
      return STATUS_OK;
   }
}

/*
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
 */
static inline void
memtable_add_tuple(memtable_context *ctxt)
{
   const threadid tid = platform_get_tid();
   ctxt->thread_num_tuples[tid].v++;
   if (ctxt->thread_num_tuples[tid].v % MEMTABLE_COUNT_GRANULARITY == 1) {
      __sync_fetch_and_add(&ctxt->num_tuples, MEMTABLE_COUNT_GRANULARITY);
   }
}

platform_status
memtable_insert(memtable_context *ctxt,
                memtable         *mt,
                const char       *key,
                const char       *data,
                uint64           *leaf_generation)
{
   bool was_unique;
   platform_status rc = btree_insert(ctxt->cc, ctxt->cfg.btree_cfg,
         ctxt->scratch, mt->root_addr, &mt->mini, key, data, leaf_generation,
         &was_unique);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (was_unique) {
      memtable_add_tuple(ctxt);
   }

   return rc;
}

void
memtable_unget_insert_lock(memtable_context *ctxt,
                           page_handle      *lock_page)
{
   cache_unget(ctxt->cc, lock_page);
}

page_handle *
memtable_get_lookup_lock(memtable_context *ctxt)
{
   return cache_get(ctxt->cc, ctxt->lookup_lock_addr, TRUE,
                    PAGE_TYPE_LOCK_NO_DATA);
}

// FIXME: [aconway 2020-09-01] Not used by current algorithm? Delete?
page_handle *
memtable_try_get_lookup_lock(memtable_context *ctxt)
{
   return cache_get(ctxt->cc, ctxt->lookup_lock_addr, FALSE,
                    PAGE_TYPE_LOCK_NO_DATA);
}

void
memtable_unget_lookup_lock(memtable_context *ctxt,
                           page_handle      *lock_page)
{
   cache_unget(ctxt->cc, lock_page);
}

page_handle *
memtable_uncontended_get_claim_lock_lookup_lock(memtable_context *ctxt)
{
   page_handle *lock_page = memtable_get_lookup_lock(ctxt);
   cache *cc = ctxt->cc;
   bool claimed = cache_claim(cc, lock_page);
   platform_assert(claimed);
   cache_lock(cc, lock_page);
   return lock_page;
}

void
memtable_unlock_unclaim_unget_lookup_lock(memtable_context *ctxt,
                                          page_handle      *lock_page)
{
   cache *cc = ctxt->cc;
   cache_unlock(cc, lock_page);
   cache_unclaim(cc, lock_page);
   cache_unget(cc, lock_page);
}

/*
 * if there are no outstanding refs, then destroy and reinit memtable and
 * transition to READY
 */
bool
memtable_dec_ref_maybe_recycle(memtable_context *ctxt,
                               memtable         *mt)
{
   cache *cc = ctxt->cc;
   bool should_zap =
      btree_should_zap_dec_ref(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
   if (should_zap) {
      platform_assert(mt->state == MEMTABLE_STATE_INCORPORATED);
      btree_zap(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
      mt->root_addr = btree_init(cc, mt->cfg, &mt->mini, FALSE);
      memtable_lock_incorporation_lock(ctxt);
      mt->generation += ctxt->cfg.max_memtables;
      // FIXME: [aconway 2020-09-02] assert that memtable_get(...,
      // mt->generation) == mt
      memtable_unlock_incorporation_lock(ctxt);
      memtable_transition(mt, MEMTABLE_STATE_INCORPORATED,
                              MEMTABLE_STATE_READY);
   }
   return should_zap;
}

uint64
memtable_force_finalize(memtable_context *ctxt)
{
   uint64 lock_addr = ctxt->insert_lock_addr;
   cache *cc = ctxt->cc;
   page_handle *lock_page =
      cache_get(cc, lock_addr, TRUE, PAGE_TYPE_LOCK_NO_DATA);
   uint64 wait = 100;
   while (!cache_claim(cc, lock_page)) {
      cache_unget(cc, lock_page);
      platform_sleep(wait);
      wait *= 2;
      lock_page = cache_get(cc, lock_addr, TRUE, PAGE_TYPE_LOCK_NO_DATA);
   }
   cache_lock(cc, lock_page);

   uint64 generation = ctxt->generation;
   uint64 mt_no = generation % ctxt->cfg.max_memtables;
   memtable *mt = &ctxt->mt[mt_no];
   memtable_transition(mt, MEMTABLE_STATE_READY, MEMTABLE_STATE_FINALIZED);
   uint64 process_generation = ctxt->generation++;
   memtable_clear_num_tuples(ctxt);

   cache_unlock(cc, lock_page);
   cache_unclaim(cc, lock_page);
   cache_unget(cc, lock_page);

   return process_generation;
}

void
memtable_init(memtable         *mt,
              cache            *cc,
              memtable_config  *cfg,
              uint64            generation)
{
   ZERO_CONTENTS(mt);
   mt->cfg = cfg->btree_cfg;
   mt->root_addr = btree_init(cc, mt->cfg, &mt->mini, FALSE);
   mt->state = MEMTABLE_STATE_READY;
   platform_assert(generation < UINT64_MAX);
   mt->generation = generation;

   // FIXME: [aconway 2020-08-24] Add tracing?
   //TRACESplinter(1, SPLINTERTraceMemtableCreate, cfg->max_tuples_per_tree);
}

void
memtable_deinit(cache            *cc,
                memtable         *mt)
{
   __attribute__ ((unused)) bool should_zap =
      btree_should_zap_dec_ref(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
   debug_assert(should_zap);
   btree_zap(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
}

memtable_context *
memtable_context_create(platform_heap_id  hid,
                        cache            *cc,
                        memtable_config  *cfg,
                        process_fn        process,
                        void             *process_ctxt)
{
   memtable_context *ctxt =
      TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, ctxt, mt, cfg->max_memtables);
   ctxt->cc = cc;
   memmove(&ctxt->cfg, cfg, sizeof(ctxt->cfg));

   uint64          base_addr;
   allocator *     al = cache_allocator(cc);
   platform_status rc = allocator_alloc_extent(al, &base_addr);
   platform_assert_status_ok(rc);

   ctxt->insert_lock_addr = base_addr;
   ctxt->lookup_lock_addr = base_addr + cache_page_size(cc);

   page_handle *lock_page =
      cache_alloc(cc, ctxt->insert_lock_addr, PAGE_TYPE_LOCK_NO_DATA);
   cache_unlock(cc, lock_page);
   cache_unclaim(cc, lock_page);
   cache_unget(cc, lock_page);

   lock_page = cache_alloc(cc, ctxt->lookup_lock_addr, PAGE_TYPE_LOCK_NO_DATA);
   cache_unlock(cc, lock_page);
   cache_unclaim(cc, lock_page);
   cache_unget(cc, lock_page);

   platform_spinlock_init(
      &ctxt->incorporation_lock, platform_get_module_id(), hid);

   for (uint64 mt_no = 0; mt_no < cfg->max_memtables; mt_no++) {
      uint64 generation = mt_no;
      memtable_init(&ctxt->mt[mt_no], cc, cfg, generation);
   }

   // FIXME: [aconway 2020-09-02] These either need to start at 1 (0 for
   // retired) or retired should always do a -1 somewhere.
   ctxt->generation = 0;
   ctxt->generation_to_incorporate = 0;
   ctxt->generation_retired = (uint64)-1;

   ctxt->process = process;
   ctxt->process_ctxt = process_ctxt;

   // FIXME: [aconway 2020-08-22] Do we want/need TRACESplinter here?

   return ctxt;
}

void
memtable_context_destroy(platform_heap_id  hid,
                         memtable_context *ctxt)
{
   cache *cc = ctxt->cc;
   for (uint64 mt_no = 0; mt_no < ctxt->cfg.max_memtables; mt_no++) {
      memtable_deinit(cc, &ctxt->mt[mt_no]);
   }

   platform_spinlock_destroy(&ctxt->incorporation_lock);

   /*
    * lookup lock and insert lock share extents but not pages.
    * this deallocs both.
    */
   cache_dealloc(cc, ctxt->insert_lock_addr, PAGE_TYPE_LOCK_NO_DATA);
   platform_free(hid, ctxt);
}

void
memtable_config_init(memtable_config *cfg,
                     btree_config    *btree_cfg,
                     uint64           max_memtables,
                     uint64           memtable_capacity)
{
   ZERO_CONTENTS(cfg);
   cfg->btree_cfg = btree_cfg;
   cfg->max_memtables = max_memtables;

   data_config *data_cfg = btree_cfg->data_cfg;
   uint64 packed_tuple_size = data_cfg->key_size + data_cfg->message_size;
   cfg->max_tuples_per_memtable = memtable_capacity / packed_tuple_size;
}
