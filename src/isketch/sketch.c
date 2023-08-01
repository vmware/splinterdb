#if !defined(_GNU_SOURCE)
#   define _GNU_SOURCE
#endif

#include "sketch.h"
#include "hashutil.h"
#include <sys/mman.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <immintrin.h>

void
sketch_init(sketch_config *config, sketch *sktch)
{
   assert(sktch);

   sktch->config = config;

   uint64_t table_size = config->rows * config->cols * sizeof(sketch_item);

   sktch->table =
      (sketch_item *)mmap(NULL,
                          table_size,
                          PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                          0,
                          0);
   if (!sktch->table) {
      perror("table malloc failed");
      exit(1);
   }

   memset(sktch->table, 0, table_size);

   sktch->hashes =
      (unsigned int *)mmap(NULL,
                           config->rows * sizeof(unsigned int),
                           PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                           0,
                           0);
   if (!sktch->hashes) {
      perror("hashes malloc failed");
      exit(1);
   }

   for (uint64_t row = 0; row < config->rows; ++row) {
      sktch->hashes[row] = 0xc0ffee + row;
   }
}

void
sketch_deinit(sketch *sktch)
{
   munmap(sktch->table,
          sktch->config->rows * sktch->config->cols * sizeof(sketch_item));
   munmap(sktch->hashes, sktch->config->rows * sizeof(unsigned int));
}

static inline uint64_t
get_index_in_row(sketch *sktch, KeyType key, uint64_t row)
{
   bool     should_compute_hash = sktch->config->cols > 1;
   uint64_t col =
      should_compute_hash
         ? MurmurHash64A_inline((const void *)key, KEY_SIZE, sktch->hashes[row])
              % sktch->config->cols
         : 0;
   return row * sktch->config->cols + col;
}

#if USE_SKETCH_ITEM_LATCH
static inline void
lock(bool *lck)
{
   while (__atomic_test_and_set(lck, __ATOMIC_ACQUIRE)) {
      _mm_pause();
   }
}

static inline void
unlock(bool *lck)
{
   __atomic_clear(lck, __ATOMIC_RELEASE);
}

static inline void
lock_all(sketch *sktch, KeyType key)
{
   uint64_t index;
   for (uint64_t row = 0; row < sktch->config->rows; ++row) {
      index = get_index_in_row(sktch, key, row);
      lock(&sktch->table[index].latch);
   }
}

static inline void
unlock_all(sketch *sktch, KeyType key)
{
   uint64_t index;
   for (uint64_t row = 0; row < sktch->config->rows; ++row) {
      index = get_index_in_row(sktch, key, row);
      unlock(&sktch->table[index].latch);
   }
}
#endif

inline void
sketch_insert(sketch *sktch, KeyType key, ValueType value)
{
   uint64_t row = 0;
   uint64_t index;

   if (sktch->config->rows == 1) {
      index = get_index_in_row(sktch, key, row);
      ValueType current_value =
         __atomic_load_n(&sktch->table[index].value, __ATOMIC_SEQ_CST);
      ValueType max_value;
      while (current_value < value) {
         max_value = current_value;
         sktch->config->insert_value_fn(&max_value, value);
         bool is_success =
            __atomic_compare_exchange_n(&sktch->table[index].value,
                                        &current_value,
                                        max_value,
                                        true,
                                        __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST);
         if (is_success) {
            break;
         }
      }
      return;
   }

#if USE_SKETCH_ITEM_LATCH
   lock_all(sktch, key);
   for (row = 0; row < sktch->config->rows; ++row) {
      index = get_index_in_row(sktch, key, row);
      sktch->config->insert_value_fn(&sktch->table[index].value, value);
   }
   unlock_all(sktch, key);
#else
   ValueType current_value, max_value;
   for (row = 0; row < sktch->config->rows; ++row) {
      index = get_index_in_row(sktch, key, row);
      current_value =
         __atomic_load_n(&sktch->table[index].value, __ATOMIC_SEQ_CST);
      while (current_value < value) {
         max_value = current_value;
         sktch->config->insert_value_fn(&max_value, value);
         bool is_success =
            __atomic_compare_exchange_n(&sktch->table[index].value,
                                        &current_value,
                                        max_value,
                                        true,
                                        __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST);
         if (is_success) {
            break;
         }
      }
   }
#endif
}

inline ValueType
sketch_get(sketch *sktch, KeyType key)
{
   uint64_t row   = 0;
   uint64_t index = get_index_in_row(sktch, key, row);
   if (sktch->config->rows == 1) {
      return __atomic_load_n(&sktch->table[index].value, __ATOMIC_SEQ_CST);
   }

#if USE_SKETCH_ITEM_LATCH
   lock_all(sktch, key);
   ValueType value = sktch->table[index].value;
   for (row = 1; row < sktch->config->rows; ++row) {
      index = get_index_in_row(sktch, key, row);
      if (sktch->config->less_than_fn(sktch->table[index].value, value)) {
         value = sktch->table[index].value;
      }
   }
   unlock_all(sktch, key);
#else
   ValueType value =
      __atomic_load_n(&sktch->table[index].value, __ATOMIC_SEQ_CST);
   for (row = 1; row < sktch->config->rows; ++row) {
      index = get_index_in_row(sktch, key, row);
      sktch->config->get_value_fn(
         __atomic_load_n(&sktch->table[index].value, __ATOMIC_SEQ_CST), &value);
   }
#endif
   return value;
}
