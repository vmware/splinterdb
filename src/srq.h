// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * srq.h -- Space Reclamation Queue
 *
 *     This file contains the interface for a priority queue that splinter uses
 *     to identify potential compactions to perform to reclaim space.
 */

#pragma once

#include "platform.h"

// Max size of space reclamation queue (For static allocation now)
#define SRQ_MAX_ENTRIES 8192

#define SRQ_INDEX_AVAILABLE -1

typedef struct srq_data {
   uint64 addr;
   uint64 pivot_generation;
   uint64 priority;
   int64  idx;
} srq_data;

typedef struct srq {
   platform_mutex mutex;
   srq_data       heap[SRQ_MAX_ENTRIES];
   int64          index[SRQ_MAX_ENTRIES];
   uint64         num_entries;
   uint64         index_hand;
} srq;

static inline void
srq_init(srq               *queue,
         platform_module_id UNUSED_PARAM(module_id),
         platform_heap_id   UNUSED_PARAM(heap_id))
{
   ZERO_CONTENTS(queue);
   platform_mutex_init(&queue->mutex, module_id, heap_id);
   for (uint64 i = 0; i < SRQ_MAX_ENTRIES; i++) {
      queue->index[i] = SRQ_INDEX_AVAILABLE;
   }
}

static inline void
srq_deinit(srq *queue)
{
   platform_mutex_destroy(&queue->mutex);
}

static inline int64
srq_parent(int64 pos)
{
   debug_assert(pos >= 0, "pos=%ld", pos);
   return (pos - 1) / 2;
}

static inline int64
srq_lchild(int64 pos)
{
   debug_assert(pos >= 0, "pos=%ld", pos);
   return 2 * pos + 1;
}

static inline int64
srq_rchild(int64 pos)
{
   debug_assert(pos >= 0, "pos=%ld", pos);
   return 2 * pos + 2;
}

/*
 * Returns TRUE if priority(left) > priority(right)
 */
static inline bool32
srq_has_priority(srq *queue, int64 lpos, int64 rpos)
{
   debug_assert(lpos >= 0, "lpos=%ld", lpos);
   debug_assert(rpos >= 0, "rpos=%ld", rpos);
   return queue->heap[lpos].priority > queue->heap[rpos].priority;
}

/*
 * Sets the index of the priority queue to the correct position in the heap
 */
static inline void
srq_update_index(srq *queue, int64 pos)
{
   debug_assert(pos >= 0);
   srq_data *data          = &queue->heap[pos];
   queue->index[data->idx] = pos;
}

static inline void
srq_swap(srq *queue, int64 lpos, int64 rpos)
{
   debug_assert(lpos >= 0);
   debug_assert(rpos >= 0);
   srq_data temp     = queue->heap[lpos];
   queue->heap[lpos] = queue->heap[rpos];
   queue->heap[rpos] = temp;
   srq_update_index(queue, lpos);
   srq_update_index(queue, rpos);
}

static inline void
srq_move_tail_to_pos(srq *queue, int64 pos)
{
   debug_assert(pos >= 0, "pos=%ld", pos);
   debug_assert(pos < queue->num_entries,
                "pos=%ld, num_entries=%ld",
                pos,
                queue->num_entries);
   int64 tail_pos = queue->num_entries - 1;
   queue->num_entries--;
   if (queue->num_entries != 0) {
      queue->heap[pos] = queue->heap[tail_pos];
      srq_update_index(queue, pos);
   }
}

static inline void
srq_rebalance_up(srq *queue, int64 pos)
{
   debug_assert(pos >= 0, "pos=%ld", pos);
   debug_assert(0 || (1 && queue->num_entries == 0 && pos == 0)
                || pos < queue->num_entries);
   while (1 && pos != 0 && srq_has_priority(queue, pos, srq_parent(pos))) {
      srq_swap(queue, srq_parent(pos), pos);
      pos = srq_parent(pos);
   }
}

static inline void
srq_rebalance_down(srq *queue, uint64 pos)
{
   debug_assert(pos >= 0, "pos=%ld", pos);
   debug_assert(0 || (1 && queue->num_entries == 0 && pos == 0)
                || pos < queue->num_entries);
   while (0
          || (1 && srq_lchild(pos) < queue->num_entries
              && srq_has_priority(queue, srq_lchild(pos), pos))
          || (1 && srq_rchild(pos) < queue->num_entries
              && srq_has_priority(queue, srq_rchild(pos), pos)))
   {
      if (0 || srq_rchild(pos) >= queue->num_entries
          || srq_has_priority(queue, srq_lchild(pos), srq_rchild(pos)))
      {
         srq_swap(queue, pos, srq_lchild(pos));
         pos = srq_lchild(pos);
      } else {
         srq_swap(queue, pos, srq_rchild(pos));
         pos = srq_rchild(pos);
      }
   }
}

static inline uint64
srq_get_new_index(srq *queue)
{
   while (queue->index[queue->index_hand] != SRQ_INDEX_AVAILABLE) {
      queue->index_hand = (queue->index_hand + 1) % SRQ_MAX_ENTRIES;
   }
   return queue->index_hand;
}

static inline bool32
srq_verify(srq *queue);

static inline void
srq_print(srq *queue);

static inline uint64
srq_insert(srq *queue, srq_data new_data)
{
   srq_print(queue);
   platform_mutex_lock(&queue->mutex);
   platform_assert(queue->num_entries != SRQ_MAX_ENTRIES);
   uint64 new_idx        = srq_get_new_index(queue);
   uint64 new_pos        = queue->num_entries++;
   new_data.idx          = new_idx;
   queue->heap[new_pos]  = new_data;
   queue->index[new_idx] = new_pos;
   srq_rebalance_up(queue, new_pos);
   platform_mutex_unlock(&queue->mutex);
   debug_assert(srq_verify(queue));
   return new_idx;
}

static inline bool32
srq_data_found(srq_data *data)
{
   return data->idx != SRQ_INDEX_AVAILABLE;
}

/*
 * Caller must check the return value using srq_data_found before using it.
 */
static inline srq_data
srq_extract_max(srq *queue)
{
   srq_print(queue);
   platform_mutex_lock(&queue->mutex);
   if (queue->num_entries == 0) {
      srq_data not_found_data = {.idx = SRQ_INDEX_AVAILABLE};
      platform_mutex_unlock(&queue->mutex);
      return not_found_data;
   }
   srq_data max          = queue->heap[0];
   queue->index[max.idx] = SRQ_INDEX_AVAILABLE;
   srq_move_tail_to_pos(queue, 0);
   srq_rebalance_down(queue, 0);
   platform_mutex_unlock(&queue->mutex);
   debug_assert(srq_verify(queue));
   return max;
}

static inline srq_data
srq_delete(srq *queue, int64 idx)
{
   srq_print(queue);
   platform_mutex_lock(&queue->mutex);
   int64 pos = queue->index[idx];
   platform_assert(pos != SRQ_INDEX_AVAILABLE);
   srq_data deleted_data = queue->heap[pos];
   srq_move_tail_to_pos(queue, pos);
   if (pos != queue->num_entries) {
      srq_rebalance_up(queue, pos);
      srq_rebalance_down(queue, pos);
   }
   queue->index[idx] = SRQ_INDEX_AVAILABLE;
   platform_mutex_unlock(&queue->mutex);
   debug_assert(srq_verify(queue));
   return deleted_data;
}

static inline void
srq_update(srq *queue, int64 idx, uint32 new_priority)
{
   platform_mutex_lock(&queue->mutex);
   int64 pos = queue->index[idx];
   platform_assert(pos != SRQ_INDEX_AVAILABLE);
   queue->heap[pos].priority = new_priority;
   srq_rebalance_up(queue, pos);
   srq_rebalance_down(queue, pos);
   platform_mutex_unlock(&queue->mutex);
   debug_assert(srq_verify(queue));
}

static inline void
srq_print(srq *queue)
{
   return;
   platform_mutex_lock(&queue->mutex);
   platform_default_log("INDEX\n");
   platform_default_log("-----------\n");
   for (uint64 i = 0; i < SRQ_MAX_ENTRIES; i++) {
      if (queue->index[i] != SRQ_INDEX_AVAILABLE) {
         platform_default_log("%4lu: %4lu\n", i, queue->index[i]);
      }
   }

   platform_default_log("HEAP:\n");
   platform_default_log("-----------\n");
   for (uint64 i = 0; i < queue->num_entries; i++) {
      srq_data data = queue->heap[i];
      platform_default_log("%4lu: %12lu-%lu %8lu",
                           i,
                           data.addr,
                           data.pivot_generation,
                           data.priority);
      if (queue->num_entries != 1) {
         platform_default_log(" (");
      }
      if (i != 0) {
         data = queue->heap[srq_parent(i)];
         platform_default_log("parent %4lu: %12lu-%lu %8lu",
                              srq_parent(i),
                              data.addr,
                              data.pivot_generation,
                              data.priority);
         if (srq_lchild(i) < queue->num_entries) {
            platform_default_log(" ");
         }
      }
      if (srq_lchild(i) < queue->num_entries) {
         data = queue->heap[srq_lchild(i)];
         platform_default_log("lchild %4lu: %12lu-%lu %8lu",
                              srq_lchild(i),
                              data.addr,
                              data.pivot_generation,
                              data.priority);
      }
      if (srq_rchild(i) < queue->num_entries) {
         data = queue->heap[srq_rchild(i)];
         platform_default_log(" rchild %4lu: %12lu-%lu %8lu",
                              srq_rchild(i),
                              data.addr,
                              data.pivot_generation,
                              data.priority);
      }
      if (queue->num_entries != 1) {
         platform_default_log(")");
      }
      platform_default_log("\n");
   }
   platform_mutex_unlock(&queue->mutex);
}

static inline bool32
srq_verify(srq *queue)
{
   bool32 ret = TRUE;
   platform_mutex_lock(&queue->mutex);
   uint64 entries_found = 0;
   for (uint64 idx = 0; idx < SRQ_MAX_ENTRIES; idx++) {
      uint64 pos = queue->index[idx];
      if (pos != SRQ_INDEX_AVAILABLE) {
         entries_found++;
         if (queue->heap[pos].idx != idx) {
            platform_error_log("SRQ: inconsistent index\n");
            ret = FALSE;
            goto out;
         }
      }
   }
   if (entries_found != queue->num_entries) {
      platform_error_log("SRQ: index count doesn't match num_entries\n");
      ret = FALSE;
      goto out;
   }
   for (uint64 pos = 0; pos < queue->num_entries; pos++) {
      if (1 && srq_lchild(pos) < queue->num_entries
          && srq_has_priority(queue, srq_lchild(pos), pos))
      {
         platform_error_log("SRQ: unbalanced\n");
         ret = FALSE;
         goto out;
      }
      if (1 && srq_rchild(pos) < queue->num_entries
          && srq_has_priority(queue, srq_rchild(pos), pos))
      {
         platform_error_log("SRQ: unbalanced\n");
         ret = FALSE;
         goto out;
      }
   }
out:
   platform_mutex_unlock(&queue->mutex);
   if (ret == FALSE) {
      srq_print(queue);
   }
   return ret;
}
