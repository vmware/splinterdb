// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * memtable.h --
 *
 *     This file contains the interface for the splinter memtable.
 */

#pragma once

#include "platform.h"
#include "task.h"
#include "cache.h"
#include "btree.h"

#define MEMTABLE_SPACE_OVERHEAD_FACTOR (2)

typedef enum memtable_state {
   MEMTABLE_STATE_INVALID = 0,
   MEMTABLE_STATE_READY, // if it's the correct one, go ahead and insert
   MEMTABLE_STATE_FINALIZED,
   MEMTABLE_STATE_COMPACTED,
   MEMTABLE_STATE_COMPACTING,
   MEMTABLE_STATE_INCORPORATION_ASSIGNED,
   MEMTABLE_STATE_INCORPORATING,
   MEMTABLE_STATE_INCORPORATED,
   NUM_MEMTABLE_STATES,
} memtable_state;

typedef struct memtable {
   volatile memtable_state state;
   uint64                  generation;
   uint64                  root_addr;
   mini_allocator          mini;
   btree_config           *cfg;
} PLATFORM_CACHELINE_ALIGNED memtable;

static inline bool
memtable_try_transition(memtable      *mt,
                        memtable_state old_state,
                        memtable_state new_state)
{
   switch (old_state) {
      case MEMTABLE_STATE_READY:
         debug_assert(new_state == MEMTABLE_STATE_FINALIZED);
         break;
      case MEMTABLE_STATE_FINALIZED:
         debug_assert(new_state == MEMTABLE_STATE_COMPACTING);
         break;
      case MEMTABLE_STATE_COMPACTING:
         debug_assert(new_state == MEMTABLE_STATE_COMPACTED);
         break;
      case MEMTABLE_STATE_COMPACTED:
         // A compacted memtable can transition to ASSIGNED when it goes
         // through the normal path where the compacting thread incorporates it
         // or it can transition straight to INCORPORATING if it is finishes
         // before the prior memtable and is incorporated by that thread.
         debug_assert(new_state == MEMTABLE_STATE_INCORPORATION_ASSIGNED);
         break;
      case MEMTABLE_STATE_INCORPORATION_ASSIGNED:
         // This occurs after the lookup lock has been acquired in
         // incorporate_memtable
         debug_assert(new_state == MEMTABLE_STATE_INCORPORATING);
         break;
      case MEMTABLE_STATE_INCORPORATING:
         // This transition happens when incorporation has completed
         debug_assert(new_state == MEMTABLE_STATE_INCORPORATED);
         break;
      case MEMTABLE_STATE_INCORPORATED:
         debug_assert(new_state == MEMTABLE_STATE_READY);
         break;
      default:
         debug_assert(0);
   }

   memtable_state actual_old_state =
      __sync_val_compare_and_swap(&mt->state, old_state, new_state);
   if (actual_old_state != old_state) {
      switch (old_state) {
         case MEMTABLE_STATE_COMPACTED:
            debug_assert(actual_old_state
                         != MEMTABLE_STATE_INCORPORATION_ASSIGNED);
            debug_assert(actual_old_state != MEMTABLE_STATE_INCORPORATING);
            break;
         default:
            platform_assert(0);
      }
   }
   return actual_old_state == old_state;
}

static inline void
memtable_transition(memtable      *mt,
                    memtable_state old_state,
                    memtable_state new_state)
{
   bool success = memtable_try_transition(mt, old_state, new_state);
   platform_assert(success);
}

typedef void (*process_fn)(void *arg, uint64 generation);

typedef struct memtable_config {
   uint64        max_extents_per_memtable;
   uint64        max_memtables;
   btree_config *btree_cfg;
} memtable_config;

typedef struct memtable_context {
   cache          *cc;
   memtable_config cfg;
   task_system    *ts;

   process_fn process;
   void      *process_ctxt;

   // Protected by insert_lock. Can read without lock. Must get read lock to
   // freeze and write lock to modify.
   uint64          insert_lock_addr;
   volatile uint64 generation;

   // Protected by incorporation_lock. Must hold to read or modify.
   platform_spinlock incorporation_lock;
   volatile uint64   generation_to_incorporate;

   // Protected by the lookup lock. Must hold read lock to read and write lock
   // to modify.
   uint64          lookup_lock_addr;
   volatile uint64 generation_retired;

   bool   is_empty;
   size_t mt_ctxt_size; // # of bytes of memory allocated to this struct

   // Effectively thread local, no locking at all:
   btree_scratch scratch[MAX_THREADS];

   memtable mt[];
} memtable_context;

platform_status
memtable_maybe_rotate_and_get_insert_lock(memtable_context *ctxt,
                                          uint64           *generation,
                                          page_handle     **lock_page);

void
memtable_unget_insert_lock(memtable_context *ctxt, page_handle *lock_page);

platform_status
memtable_insert(memtable_context *ctxt,
                memtable         *mt,
                platform_heap_id  heap_id,
                key               tuple_key,
                message           msg,
                uint64           *generation);

page_handle *
memtable_get_lookup_lock(memtable_context *ctxt);

void
memtable_unget_lookup_lock(memtable_context *ctxt, page_handle *lock_page);

page_handle *
memtable_uncontended_get_claim_lock_lookup_lock(memtable_context *ctxt);

void
memtable_unlock_unclaim_unget_lookup_lock(memtable_context *ctxt,
                                          page_handle      *lock_page);

bool
memtable_dec_ref_maybe_recycle(memtable_context *ctxt, memtable *mt);

uint64
memtable_force_finalize(memtable_context *ctxt);

void
memtable_init(memtable *mt, cache *cc, memtable_config *cfg, uint64 generation);

void
memtable_deinit(cache *cc, memtable *mt);

memtable_context *
memtable_context_create(platform_heap_id hid,
                        cache           *cc,
                        memtable_config *cfg,
                        process_fn       process,
                        void            *process_ctxt);

void
memtable_context_destroy(platform_heap_id hid, memtable_context *ctxt);

void
memtable_config_init(memtable_config *cfg,
                     btree_config    *btree_cfg,
                     uint64           max_memtables,
                     uint64           memtable_capacity);

static inline uint64
memtable_root_addr(memtable *mt)
{
   return mt->root_addr;
}

static inline uint64
memtable_generation(memtable_context *ctxt)
{
   return ctxt->generation;
}

static inline uint64
memtable_generation_to_incorporate(memtable_context *ctxt)
{
   return ctxt->generation_to_incorporate;
}

static inline uint64
memtable_generation_retired(memtable_context *ctxt)
{
   return ctxt->generation_retired;
}

/*
 * Must hold write lock on insert_lock
 */
static inline void
memtable_increment_to_generation_to_incorporate(memtable_context *ctxt,
                                                uint64            generation)
{
   platform_assert(ctxt->generation_to_incorporate + 1 == generation);
   // protected by mutex, so don't need atomics
   ctxt->generation_to_incorporate++;
}

/*
 * Must hold write lock on lookup_lock
 */
static inline void
memtable_increment_to_generation_retired(memtable_context *ctxt,
                                         uint64            generation)
{
   platform_assert(ctxt->generation_retired + 1 == generation);
   // protected by lookup_lock, so don't need atomics
   ctxt->generation_retired++;
}

static inline void
memtable_lock_incorporation_lock(memtable_context *ctxt)
{
   platform_spin_lock(&ctxt->incorporation_lock);
}

static inline void
memtable_unlock_incorporation_lock(memtable_context *ctxt)
{
   platform_spin_unlock(&ctxt->incorporation_lock);
}

static inline void
memtable_zap(cache *cc, memtable *mt)
{
   btree_dec_ref(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
}

static inline bool
memtable_ok_to_lookup(memtable *mt)
{
   return mt->state != MEMTABLE_STATE_INCORPORATING
          && mt->state != MEMTABLE_STATE_INCORPORATED;
}

static inline bool
memtable_ok_to_lookup_compacted(memtable *mt)
{
   return mt->state == MEMTABLE_STATE_COMPACTED
          || mt->state == MEMTABLE_STATE_INCORPORATION_ASSIGNED;
}

bool
memtable_is_empty(memtable_context *mt_ctxt);

static inline bool
memtable_verify(cache *cc, memtable *mt)
{
   return btree_verify_tree(cc, mt->cfg, mt->root_addr, PAGE_TYPE_MEMTABLE);
}

static inline void
memtable_print(platform_log_handle *log_handle, cache *cc, memtable *mt)
{
   btree_print_memtable_tree(log_handle, cc, mt->cfg, mt->root_addr);
}

static inline void
memtable_print_stats(platform_log_handle *log_handle, cache *cc, memtable *mt)
{
   btree_print_tree_stats(log_handle, cc, mt->cfg, mt->root_addr);
}
