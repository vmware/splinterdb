#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <xxhash.h>
#include "platform_units.h"
#include "shmalloc.h"

#define IS_ALIGNED(ptr, alignment)                                             \
   ((((uintptr_t)(ptr)) & ((uintptr_t)(alignment) - 1)) == 0)
#define ALIGN_UP(value, alignment)                                             \
   (((uintptr_t)(value) + (uintptr_t)(alignment) - 1)                          \
    & ~((uintptr_t)(alignment) - 1))
#define ALIGN_DOWN(value, alignment)                                           \
   (((uintptr_t)(value)) & ~((uintptr_t)(alignment) - 1))

typedef enum chunk_state {
   CHUNK_STATE_UNUSED    = 0,
   CHUNK_STATE_FREE      = 1,
   CHUNK_STATE_ALLOCATED = 2,
} chunk_state;

typedef struct chunk {
   uint64_t      size : 55;
   uint64_t      requested_alignment : 6;
   uint64_t      hash_index : 1;
   chunk_state   state : 2;
   void         *ptr;
   struct chunk *phys_pred;
   void         *phys_next;
   struct chunk *size_pred;
   struct chunk *size_next;
} chunk;

#define NUM_SIZE_CLASSES (48)

/* The chunk table uses power-of-two-choice hashing.  The fingerprint array is
 * used to speed up lookups.*/

#define CHUNK_TABLE_BUCKET_SIZE (64)
_Static_assert(CHUNK_TABLE_BUCKET_SIZE <= 256, "CHUNK_TABLE_BUCKET_SIZE < 256");

typedef struct chunk_table_bucket {
   uint8_t occupancy;
   uint8_t fingerprints[CHUNK_TABLE_BUCKET_SIZE];
   chunk   chunks[CHUNK_TABLE_BUCKET_SIZE];
} chunk_table_bucket;

_Static_assert(CHUNK_TABLE_BUCKET_SIZE
                  < (1ULL << 8 * sizeof((chunk_table_bucket *)NULL)->occupancy),
               "CHUNK_TABLE_BUCKET_SIZE is too large for the occupancy field");

static const uint64_t chunk_table_hash_seeds[2] = {0x1234567890abcdefULL,
                                                   0xabcdef1234567890ULL};

typedef struct shmallocator {
   pthread_spinlock_t lock;
   void              *end;
   void              *max_allocated_end;
   uint64_t           chunk_table_num_buckets;
   chunk             *free[NUM_SIZE_CLASSES];
   chunk_table_bucket chunk_table[];
} shmallocator;

typedef struct hash_info {
   uint64_t bucket_idx;
   uint8_t  fingerprint;
} hash_info;

static hash_info
pointer_hash(shmallocator *shm, void *ptr, uint64_t seed)
{
   hash_info info;
   uint64_t  hash   = XXH64(&ptr, sizeof(void *), chunk_table_hash_seeds[seed]);
   info.bucket_idx  = (hash >> 8) % shm->chunk_table_num_buckets;
   info.fingerprint = (uint8_t)(hash & 0xFF);
   if (info.fingerprint == 0) {
      info.fingerprint = 1;
   }
   return info;
}

static chunk *
chunk_table_alloc_entry(shmallocator *shm, void *ptr)
{
   uint64_t  chosen_bucket_idx = 0;
   hash_info chosen_bucket_info;
   uint8_t   chosen_bucket_occupancy = UINT8_MAX;
   for (uint64_t i = 0; i < 2; i++) {
      hash_info           info   = pointer_hash(shm, ptr, i);
      chunk_table_bucket *bucket = &shm->chunk_table[info.bucket_idx];
      if (bucket->occupancy < chosen_bucket_occupancy) {
         chosen_bucket_idx       = i;
         chosen_bucket_info      = info;
         chosen_bucket_occupancy = bucket->occupancy;
      }
   }

   if (chosen_bucket_occupancy == CHUNK_TABLE_BUCKET_SIZE) {
      return NULL;
   }

   chunk_table_bucket *bucket =
      &shm->chunk_table[chosen_bucket_info.bucket_idx];
   assert(bucket->occupancy < CHUNK_TABLE_BUCKET_SIZE);
   for (uint64_t j = 0; j < CHUNK_TABLE_BUCKET_SIZE; j++) {
      if (bucket->fingerprints[j] == 0) {
         assert(bucket->chunks[j].state == CHUNK_STATE_UNUSED);
         bucket->fingerprints[j]      = chosen_bucket_info.fingerprint;
         bucket->chunks[j].hash_index = chosen_bucket_idx;
         bucket->occupancy++;
         return &bucket->chunks[j];
      }
   }
   assert(0);
}

static void
chunk_table_free_entry(shmallocator *shm, chunk *chnk)
{
   hash_info           info   = pointer_hash(shm, chnk->ptr, chnk->hash_index);
   chunk_table_bucket *bucket = &shm->chunk_table[info.bucket_idx];
   assert(&bucket->chunks[0] <= chnk
          && chnk < &bucket->chunks[CHUNK_TABLE_BUCKET_SIZE]);
   uint64_t j = chnk - &bucket->chunks[0];
   assert(bucket->fingerprints[j] == info.fingerprint);
   memset(chnk, 0, sizeof(chunk));
   chnk->state             = CHUNK_STATE_UNUSED;
   bucket->fingerprints[j] = 0;
   bucket->occupancy--;
}

static chunk *
chunk_table_get_entry(shmallocator *shm, void *ptr)
{
   for (uint64_t i = 0; i < 2; i++) {
      hash_info           info   = pointer_hash(shm, ptr, i);
      chunk_table_bucket *bucket = &shm->chunk_table[info.bucket_idx];
      for (uint64_t j = 0; j < CHUNK_TABLE_BUCKET_SIZE; j++) {
         if (bucket->fingerprints[j] == info.fingerprint
             && bucket->chunks[j].ptr == ptr)
         {
            return &bucket->chunks[j];
         }
      }
   }
   return NULL;
}

static chunk *
chunk_get_physical_successor(shmallocator *shm, chunk *chnk)
{
   return chnk->phys_next;
}

static uint64_t
log_size(uint64_t size)
{
   assert(size > 0);
   return 63 - __builtin_clzll(size);
}

static void
free_list_add(shmallocator *shm, chunk *chnk)
{
   uint64_t lsize = log_size(chnk->size);
   assert(lsize < NUM_SIZE_CLASSES);
   assert(chnk->state == CHUNK_STATE_FREE);
   assert(chnk->size_pred == NULL);
   assert(chnk->size_next == NULL);
   chnk->size_pred = NULL;
   chnk->size_next = shm->free[lsize];
   if (chnk->size_next) {
      chnk->size_next->size_pred = chnk;
   }
   shm->free[lsize] = chnk;
}

static chunk *
free_list_alloc(shmallocator *shm, size_t size)
{
   uint64_t lsize = 1 + log_size(size);
   assert(lsize < NUM_SIZE_CLASSES);
   while (lsize < NUM_SIZE_CLASSES && !shm->free[lsize]) {
      lsize++;
   }
   if (lsize >= NUM_SIZE_CLASSES) {
      return NULL;
   }
   chunk *chnk      = shm->free[lsize];
   shm->free[lsize] = chnk->size_next;
   if (chnk->size_next) {
      chnk->size_next->size_pred = NULL;
   }
   chnk->size_pred = NULL;
   chnk->size_next = NULL;
   return chnk;
}

static void
free_list_remove(shmallocator *shm, chunk *chnk)
{
   assert(chnk->state == CHUNK_STATE_FREE);
   if (chnk->size_pred) {
      chnk->size_pred->size_next = chnk->size_next;
   } else {
      uint64_t lsize = log_size(chnk->size);
      assert(lsize < NUM_SIZE_CLASSES);
      assert(shm->free[lsize] == chnk);
      shm->free[lsize] = chnk->size_next;
   }
   if (chnk->size_next) {
      chnk->size_next->size_pred = chnk->size_pred;
   }
   chnk->size_pred = NULL;
   chnk->size_next = NULL;
}

static chunk *
chunk_remove_tail(shmallocator *shm, chunk *chnk, size_t size)
{
   assert(chnk->state == CHUNK_STATE_ALLOCATED);

   chunk *psucc = chunk_get_physical_successor(shm, chnk);

   chunk *new_chunk = chunk_table_alloc_entry(shm, chnk->ptr + size);
   if (!new_chunk) {
      return NULL;
   }

   new_chunk->size      = chnk->size - size;
   new_chunk->state     = CHUNK_STATE_FREE;
   new_chunk->ptr       = chnk->ptr + size;
   new_chunk->phys_pred = chnk;
   new_chunk->phys_next = psucc;

   if (psucc) {
      psucc->phys_pred = new_chunk;
   }

   chnk->size      = size;
   chnk->phys_next = new_chunk;

   return new_chunk;
}

static void
chunks_merge(shmallocator *shm, chunk *chnk, chunk *psucc)
{
   assert(psucc->state == CHUNK_STATE_FREE);

   chunk *ppsucc = chunk_get_physical_successor(shm, psucc);

   chnk->size += psucc->size;
   chnk->phys_next = psucc->phys_next;
   chunk_table_free_entry(shm, psucc);
   if (ppsucc) {
      ppsucc->phys_pred = chnk;
   }
}

static void
free_list_is_free(shmallocator *shm)
{
   uint64_t free_list_count = 0;
   for (uint64_t i = 0; i < NUM_SIZE_CLASSES; i++) {
      chunk *chnk = shm->free[i];
      while (chnk) {
         assert(chnk->state == CHUNK_STATE_FREE);
         free_list_count++;
         chnk = chnk->size_next;
      }
   }

   uint64_t chunk_table_count = 0;
   for (uint64_t i = 0; i < shm->chunk_table_num_buckets; i++) {
      chunk_table_bucket *bucket = &shm->chunk_table[i];
      for (uint64_t j = 0; j < CHUNK_TABLE_BUCKET_SIZE; j++) {
         chunk *chnk = &bucket->chunks[j];
         if (chnk->state == CHUNK_STATE_FREE) {
            chunk_table_count++;
         }
      }

      assert(free_list_count == chunk_table_count);
   }
}

static void
inuse_chunks_are_gettable(shmallocator *shm)
{
   for (uint64_t i = 0; i < shm->chunk_table_num_buckets; i++) {
      chunk_table_bucket *bucket = &shm->chunk_table[i];
      for (uint64_t j = 0; j < CHUNK_TABLE_BUCKET_SIZE; j++) {
         chunk *chnk = &bucket->chunks[j];
         if (chnk->state == CHUNK_STATE_ALLOCATED
             || chnk->state == CHUNK_STATE_FREE)
         {
            assert(chnk->ptr != NULL);
            assert(chunk_table_get_entry(shm, chnk->ptr) == chnk);
         }
      }
   }
}

static void
available_chunks_are_zeroed(shmallocator *shm)
{
   for (uint64_t i = 0; i < shm->chunk_table_num_buckets; i++) {
      chunk_table_bucket *bucket = &shm->chunk_table[i];
      for (uint64_t j = 0; j < CHUNK_TABLE_BUCKET_SIZE; j++) {
         chunk *chnk = &bucket->chunks[j];
         if (chnk->state == CHUNK_STATE_UNUSED) {
            assert(chnk->ptr == NULL);
            assert(chnk->size == 0);
            assert(chnk->phys_pred == NULL);
            assert(chnk->size_pred == NULL);
            assert(chnk->size_next == NULL);
         }
      }
   }
}

static void
allocated_chunks_have_null_size_pred_and_size_next(shmallocator *shm)
{
   for (uint64_t i = 0; i < shm->chunk_table_num_buckets; i++) {
      chunk_table_bucket *bucket = &shm->chunk_table[i];
      for (uint64_t j = 0; j < CHUNK_TABLE_BUCKET_SIZE; j++) {
         chunk *chnk = &bucket->chunks[j];
         if (chnk->state == CHUNK_STATE_ALLOCATED) {
            assert(chnk->size_pred == NULL);
            assert(chnk->size_next == NULL);
         }
      }
   }
}

static void
physical_ordering_is_consistent(shmallocator *shm)
{
   void  *ptr  = &shm->chunk_table[shm->chunk_table_num_buckets];
   chunk *chnk = chunk_table_get_entry(shm, ptr);
   assert(chnk);
   while (chnk) {
      assert(chnk->ptr == ptr);
      ptr              = chnk->ptr + chnk->size;
      chunk *next_chnk = chunk_get_physical_successor(shm, chnk);
      if (next_chnk) {
         assert(next_chnk->phys_pred == chnk);
      }
      chnk = next_chnk;
   }
}

static void
no_adjacent_free_chunks(shmallocator *shm)
{
   void  *ptr  = &shm->chunk_table[shm->chunk_table_num_buckets];
   chunk *chnk = chunk_table_get_entry(shm, ptr);
   assert(chnk);
   while (chnk) {
      assert(chnk->state == CHUNK_STATE_FREE
             || chnk->state == CHUNK_STATE_ALLOCATED);
      assert(chnk->ptr == ptr);
      ptr              = chnk->ptr + chnk->size;
      chunk *next_chnk = chunk_get_physical_successor(shm, chnk);
      if (next_chnk) {
         assert(next_chnk->state == CHUNK_STATE_ALLOCATED
                || chnk->state == CHUNK_STATE_ALLOCATED);
      }

      chnk = next_chnk;
   }
}

static void
no_overlapping_chunks(shmallocator *shm)
{
   uint64_t physical_count = 0;
   void    *ptr            = &shm->chunk_table[shm->chunk_table_num_buckets];
   chunk   *chnk           = chunk_table_get_entry(shm, ptr);
   assert(chnk);
   while (chnk) {
      physical_count++;
      ptr  = chnk->ptr + chnk->size;
      chnk = chunk_get_physical_successor(shm, chnk);
   }

   uint64_t chunk_table_count = 0;
   for (uint64_t i = 0; i < shm->chunk_table_num_buckets; i++) {
      chunk_table_bucket *bucket = &shm->chunk_table[i];
      for (uint64_t j = 0; j < CHUNK_TABLE_BUCKET_SIZE; j++) {
         chunk *chnk = &bucket->chunks[j];
         if (chnk->state == CHUNK_STATE_ALLOCATED
             || chnk->state == CHUNK_STATE_FREE)
         {
            chunk_table_count++;
         }
      }
   }
   assert(physical_count == chunk_table_count);
}

static void
all_invariants(shmallocator *shm)
{
   return;
   free_list_is_free(shm);
   inuse_chunks_are_gettable(shm);
   available_chunks_are_zeroed(shm);
   allocated_chunks_have_null_size_pred_and_size_next(shm);
   physical_ordering_is_consistent(shm);
   no_adjacent_free_chunks(shm);
   no_overlapping_chunks(shm);
}

int
shmallocator_init(shmallocator *shm, uint64_t max_allocations, size_t size)
{
   /* There chunk table holds all the free and allocated chunks.  There may be
    * up to one free chunk between each pair of allocated chunks.  So we need to
    * double the number of slots in the chunk table.  Furthermore, we need to
    * allocate a few extra slots to be able to resolve collisions.  So, all
    * total, we allocate 2.4x as many slots as the max number of allocations.*/
   uint64_t chunk_table_num_buckets =
      (24 * max_allocations / 10 + CHUNK_TABLE_BUCKET_SIZE - 1)
      / CHUNK_TABLE_BUCKET_SIZE;


   uint64_t shmsize = sizeof(shmallocator)
                      + chunk_table_num_buckets * sizeof(chunk_table_bucket);
   if (size < shmsize) {
      return -1;
   }

   memset(shm, 0, shmsize);
   shm->chunk_table_num_buckets = chunk_table_num_buckets;
   shm->end                     = ((void *)shm) + size;

   void  *ptr      = &shm->chunk_table[chunk_table_num_buckets];
   chunk *chnk     = chunk_table_alloc_entry(shm, ptr);
   chnk->size      = size - shmsize;
   chnk->state     = CHUNK_STATE_FREE;
   chnk->ptr       = ptr;
   chnk->phys_pred = NULL;
   chnk->size_pred = NULL;
   chnk->size_next = NULL;
   free_list_add(shm, chnk);
   pthread_spin_init(&shm->lock, PTHREAD_PROCESS_SHARED);
   all_invariants(shm);
   return 0;
}

void
shmallocator_deinit(shmallocator *shm)
{
   // printf("shmallocator_deinit: max needed MiBs: %lu (%lu%%)\n",
   //        B_TO_MiB(shm->max_allocated_end - (void *)shm),
   //        (uint64_t)(shm->max_allocated_end - (void *)shm) * 100
   //           / shmallocator_size(shm));
   pthread_spin_destroy(&shm->lock);
}

size_t
shmallocator_size(shmallocator *shm)
{
   return shm->end - (void *)shm;
}

void *
shmalloc(shmallocator *shm, size_t alignment, size_t size)
{
   if (alignment < 16) {
      alignment = 16;
   }
   uint64_t requested_alignment = log_size(alignment);
   if (requested_alignment > 63) {
      return NULL;
   }
   if (alignment != (1ULL << requested_alignment)) {
      return NULL;
   }

   if (size == 0) {
      size = 1;
   }

   pthread_spin_lock(&shm->lock);

   all_invariants(shm);

   chunk *chnk = free_list_alloc(shm, size + alignment - 1);
   if (!chnk) {
      all_invariants(shm);
      pthread_spin_unlock(&shm->lock);
      return NULL;
   }
   chnk->state = CHUNK_STATE_ALLOCATED;

   if (!IS_ALIGNED(chnk->ptr, alignment)) {
      void  *aligned_ptr = (void *)ALIGN_UP(chnk->ptr, alignment);
      size_t padding     = aligned_ptr - chnk->ptr;
      chunk *new_chunk   = chunk_remove_tail(shm, chnk, padding);
      chnk->state        = CHUNK_STATE_FREE;
      free_list_add(shm, chnk);
      if (!new_chunk) {
         all_invariants(shm);
         pthread_spin_unlock(&shm->lock);
         return NULL;
      }
      chnk        = new_chunk;
      chnk->state = CHUNK_STATE_ALLOCATED;
   }

   if (size < chnk->size) {
      chunk *new_chunk = chunk_remove_tail(shm, chnk, size);
      if (new_chunk) {
         free_list_add(shm, new_chunk);
      }
   }

   chnk->requested_alignment = requested_alignment;

   if (chnk->ptr + size > shm->max_allocated_end) {
      shm->max_allocated_end = chnk->ptr + size;
   }

   all_invariants(shm);
   pthread_spin_unlock(&shm->lock);
   assert((void *)&shm->chunk_table[shm->chunk_table_num_buckets] <= chnk->ptr);
   assert(size <= chnk->size);
   assert(chnk->ptr + size <= shm->end);
   assert(IS_ALIGNED(chnk->ptr, alignment));
   return chnk->ptr;
}

void
shfree(shmallocator *shm, void *ptr)
{
   pthread_spin_lock(&shm->lock);
   all_invariants(shm);
   chunk *chnk = chunk_table_get_entry(shm, ptr);
   if (!chnk) {
      all_invariants(shm);
      pthread_spin_unlock(&shm->lock);
      assert(0);
   }
   assert(chnk->state == CHUNK_STATE_ALLOCATED);
   assert(chnk->ptr == ptr);
   chnk->state  = CHUNK_STATE_FREE;
   chunk *ppred = chnk->phys_pred;
   if (ppred && ppred->state == CHUNK_STATE_FREE) {
      free_list_remove(shm, ppred);
      chunks_merge(shm, ppred, chnk);
      chnk = ppred;
   }
   chunk *psucc = chunk_get_physical_successor(shm, chnk);
   if (psucc && psucc->state == CHUNK_STATE_FREE) {
      free_list_remove(shm, psucc);
      chunks_merge(shm, chnk, psucc);
   }
   free_list_add(shm, chnk);

   all_invariants(shm);
   pthread_spin_unlock(&shm->lock);
}

void *
shrealloc(shmallocator *shm, void *ptr, size_t size)
{
   if (ptr == NULL) {
      return shmalloc(shm, 0, size);
   }
   if (size == 0) {
      shfree(shm, ptr);
      return NULL;
   }

   pthread_spin_lock(&shm->lock);
   all_invariants(shm);
   chunk *chnk = chunk_table_get_entry(shm, ptr);
   if (!chnk) {
      pthread_spin_unlock(&shm->lock);
      assert(0);
   }
   if (size < chnk->size / 2) {
      chunk *new_chunk = chunk_remove_tail(shm, chnk, size);
      if (new_chunk) {
         free_list_add(shm, new_chunk);
      }
      all_invariants(shm);
      pthread_spin_unlock(&shm->lock);
      return chnk->ptr;
   } else if (size > chnk->size) {
      chunk *next_chunk = chunk_get_physical_successor(shm, chnk);
      if (next_chunk && next_chunk->state == CHUNK_STATE_FREE
          && size <= chnk->size + next_chunk->size)
      {
         free_list_remove(shm, next_chunk);
         chunks_merge(shm, chnk, next_chunk);
         if (size < chnk->size / 2) {
            chunk *new_chunk = chunk_remove_tail(shm, chnk, size);
            if (new_chunk) {
               free_list_add(shm, new_chunk);
            }
         }
         all_invariants(shm);
         pthread_spin_unlock(&shm->lock);
         return chnk->ptr;
      } else {
         pthread_spin_unlock(&shm->lock);
         if (size < 2 * chnk->size) {
            size = 2 * chnk->size;
         }
         void *pnew = shmalloc(shm, 1ULL << chnk->requested_alignment, size);
         if (pnew) {
            memcpy(pnew, ptr, chnk->size);
            shfree(shm, ptr);
         }
         return pnew;
      }
   } else {
      all_invariants(shm);
      pthread_spin_unlock(&shm->lock);
      return ptr;
   }
}
