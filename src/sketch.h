#pragma once

#include "platform.h"

#include <stdatomic.h>

typedef struct sketch {
   atomic_ulong **freq;
   uint64         num_hashes;
   uint64         hash_size;
} sketch;

static inline uint64
hash(slice key, int seed, uint64 size)
{
   return platform_hash64(slice_data(key), slice_length(key), seed) % size;
}

static inline void
count_min_sketch_init(sketch *s, uint64 num_hashes, uint64 hash_size)
{
   s->num_hashes = num_hashes;
   s->hash_size  = hash_size;
   s->freq       = TYPED_ARRAY_ZALLOC(0, s->freq, s->num_hashes);
   for (uint64 i = 0; i < s->num_hashes; ++i) {
      s->freq[i] = TYPED_ARRAY_ZALLOC(0, s->freq[i], s->hash_size);
   }
}

static inline void
count_min_sketch_deinit(sketch *s)
{
   for (uint64 i = 0; i < s->num_hashes; i++) {
      platform_free(0, s->freq[i]);
   }
   platform_free(0, s->freq);
}

static inline void
count_min_sketch_add(sketch *s, slice key, uint64 count)
{
   for (int i = 0; i < s->num_hashes; i++) {
      atomic_fetch_add_explicit(
         &s->freq[i][hash(key, i, s->hash_size)], count, memory_order_relaxed);
   }
}

static inline void
count_min_sketch_max_insert(sketch *s, slice key, uint64 value)
{
   for (int i = 0; i < s->num_hashes; i++) {
      uint64 h             = hash(key, i, s->hash_size);
      uint64 current_value = s->freq[i][h];
      while (current_value < value) {
         if (atomic_compare_exchange_strong_explicit(&s->freq[i][h],
                                                     &current_value,
                                                     value,
                                                     memory_order_relaxed,
                                                     memory_order_relaxed))
         {
            break;
         }
      }
   }
}

static inline uint64
count_min_sketch_query(sketch *s, slice key)
{
   uint64 min_count = s->freq[0][hash(key, 0, s->hash_size)];
   for (int i = 1; i < s->num_hashes; i++) {
      min_count = MIN(min_count, s->freq[i][hash(key, i, s->hash_size)]);
   }
   return min_count;
}

static inline void
count_min_sketch_print(sketch *s)
{
   for (int i = 0; i < s->num_hashes; i++) {
      for (int j = 0; j < s->hash_size; j++) {
         platform_error_log("%lu ", s->freq[i][j]);
      }
      platform_error_log("\n");
   }
}
