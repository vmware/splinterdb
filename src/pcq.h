// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * pcq.h --
 *
 *     Multi-producer, single-consumer queue. The queue has a fixed
 *     number of maximum elements.
 */

#pragma once

#include "platform.h"

typedef struct {
   uint32               num_elems;
   cache_aligned_uint32 tail; // Producers enqueue to here
   cache_aligned_uint32 head; // Consumer dequeues from here
   size_t               size; // of memory allocate to this struct
   void                *elems[];
} pcq;


/*
 * Allocate a PCQ. Returns NULL on out of memory.
 */
static inline pcq *
pcq_alloc(platform_heap_id hid, size_t num_elems)
{
   pcq *q;

   platform_memfrag memfrag_q;
   q = TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, q, elems, num_elems);
   if (q != NULL) {
      q->num_elems = num_elems;
      q->size      = memfrag_size(&memfrag_q);
   }

   return q;
}

/*
 * Count of elements in a PCQ. A non-zero count does not guarantee a
 * subsequent dequeue will succeed. It could fail because the enqueuer
 * had advanced the tail but hasn't yet filled in the elem.
 */
static inline uint32
pcq_count(const pcq *q)
{
   return q->tail.v - q->head.v;
}

// Return TRUE if PCQ is empty, FALSE otherwise
static inline bool
pcq_is_empty(const pcq *q)
{
   return q->head.v == q->tail.v;
}

// Return TRUE if PCQ is full, FALSE otherwise
static inline bool
pcq_is_full(const pcq *q)
{
   return pcq_count(q) == q->num_elems;
}

// Deallocate a PCQ, and NULL out input handle
static inline void
pcq_free(platform_heap_id hid, pcq **q)
{
   platform_memfrag  memfrag;
   platform_memfrag *mf = &memfrag;
   memfrag_init_size(mf, *q, (*q)->size);
   platform_free(hid, mf);
   *q = NULL;
}

// Enqueue an elem to a PCQ. Element must not be NULL
static inline void
pcq_enqueue(pcq *q, void *elem)
{
   // NULL element is used to detect empty elements. Can't enqueue them.
   debug_assert(elem != NULL);
   uint32 old = __sync_fetch_and_add(&q->tail.v, 1);
   /*
    * Check if tail bumped into head. None of our callers enqueue more
    * than num_elems. But if any user needed this, we could return
    * STATUS_NO_SPACE here.
    */
   debug_assert(old - q->head.v <= q->num_elems);
   old %= q->num_elems;
   debug_assert(q->elems[old] == NULL);
   /*
    * An element may remain NULL after tail is advanced until here. Dequeue
    * detects this, and does not dequeue a NULL element.
    */
   q->elems[old] = elem;
}

/*
 * Dequeue an elem from a PCQ. Returns STATUS_NOT_FOUND if PCQ is empty,
 * STATUS_OK otherwise. This assumes a single consumer, so MT safety is
 * not done here.
 */
static inline platform_status
pcq_dequeue(pcq   *q,    // IN
            void **elem) // OUT
{
   if (pcq_is_empty(q)) {
      return STATUS_NOT_FOUND;
   }
   uint32 head = q->head.v % q->num_elems;
   ;
   /*
    * Enqueue has advanced the tail, but not filled in the element
    * yet. There might other elements up in the queue that are ready
    * and filled in by other threads, but we can't dequeue out of
    * order. So just indicate to caller that queue is empty.
    */
   if (q->elems[head] == NULL) {
      return STATUS_NOT_FOUND;
   }
   *elem = q->elems[head];
   debug_assert(*elem != NULL);
   q->elems[head] = NULL;
   debug_assert(!pcq_is_empty(q));
   q->head.v++;

   return STATUS_OK;
}
