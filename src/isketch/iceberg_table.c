#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <pthread.h>
#include <immintrin.h>
#include <tmmintrin.h>
#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <math.h>

#include "iceberg_precompute.h"
#include "iceberg_table.h"

#define likely(x)   __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

/* #define RESIZE_THRESHOLD 0.96 */
#define RESIZE_THRESHOLD 0.85 // For YCSB*/

uint64_t seed[5] = {12351327692179052ll,
                    23246347347385899ll,
                    35236262354132235ll,
                    13604702930934770ll,
                    57439820692984798ll};

static inline uint64_t
nonzero_fprint(uint64_t hash)
{
   return hash & ((1 << FPRINT_BITS) - 2) ? hash : hash | 2;
}

static inline uint64_t
lv1_hash(slice key)
{
   return nonzero_fprint(
      platform_hash64(slice_data(key), slice_length(key), seed[0]));
}

static inline uint64_t
lv2_hash(slice key, uint8_t i)
{
   return nonzero_fprint(
      platform_hash64(slice_data(key), slice_length(key), seed[i + 1]));
}

static inline uint8_t
word_select(uint64_t val, int rank)
{
   val = _pdep_u64(one[rank], val);
   return _tzcnt_u64(val);
}

uint64_t
lv1_balls(iceberg_table *table)
{
   pc_sync(&table->metadata.lv1_balls);
   return *(table->metadata.lv1_balls.global_counter);
}

static inline uint64_t
lv1_balls_aprox(iceberg_table *table)
{
   return *(table->metadata.lv1_balls.global_counter);
}

uint64_t
lv2_balls(iceberg_table *table)
{
   pc_sync(&table->metadata.lv2_balls);
   return *(table->metadata.lv2_balls.global_counter);
}

static inline uint64_t
lv2_balls_aprox(iceberg_table *table)
{
   return *(table->metadata.lv2_balls.global_counter);
}

uint64_t
lv3_balls(iceberg_table *table)
{
   pc_sync(&table->metadata.lv3_balls);
   return *(table->metadata.lv3_balls.global_counter);
}

static inline uint64_t
lv3_balls_aprox(iceberg_table *table)
{
   return *(table->metadata.lv3_balls.global_counter);
}

uint64_t
tot_balls(iceberg_table *table)
{
   return lv1_balls(table) + lv2_balls(table) + lv3_balls(table);
}

uint64_t
tot_balls_aprox(iceberg_table *table)
{
   return lv1_balls_aprox(table) + lv2_balls_aprox(table)
          + lv3_balls_aprox(table);
}

static inline uint64_t
total_capacity(iceberg_table *table)
{
   return lv3_balls(table)
          + table->metadata.nblocks
               * ((1 << SLOT_BITS) + C_LV2 + MAX_LG_LG_N / D_CHOICES);
}

#ifdef ENABLE_RESIZE
static inline uint64_t
total_capacity_aprox(iceberg_table *table)
{
   return lv3_balls_aprox(table)
          + table->metadata.nblocks
               * ((1 << SLOT_BITS) + C_LV2 + MAX_LG_LG_N / D_CHOICES);
}
#endif

inline double
iceberg_load_factor(iceberg_table *table)
{
   return (double)tot_balls(table) / (double)total_capacity(table);
}

#ifdef ENABLE_RESIZE
static inline double
iceberg_load_factor_aprox(iceberg_table *table)
{
   return (double)tot_balls_aprox(table) / (double)total_capacity_aprox(table);
}
#endif

static inline bool
iceberg_key_compare(const data_config *spl_config, slice a, slice b)
{
   return data_key_compare(
      spl_config, key_create_from_slice(a), key_create_from_slice(b));
}


#ifdef ENABLE_RESIZE
bool
need_resize(iceberg_table *table)
{
   double lf = iceberg_load_factor_aprox(table);
   if (lf >= RESIZE_THRESHOLD)
      return true;
   return false;
}
#endif

static inline void
get_index_offset(uint64_t  init_log,
                 uint64_t  index,
                 uint64_t *bindex,
                 uint64_t *boffset)
{
   uint64_t shf = index >> init_log;
   *bindex      = 64 - _lzcnt_u64(shf);
   uint64_t adj = 1ULL << *bindex;
   adj          = adj >> 1;
   adj          = adj << init_log;
   *boffset     = index - adj;
}

static inline void
split_hash(uint64_t          hash,
           uint8_t          *fprint,
           uint64_t         *index,
           iceberg_metadata *metadata)
{
   *fprint = hash & ((1 << FPRINT_BITS) - 1);
   *index  = (hash >> FPRINT_BITS) & ((1 << metadata->block_bits) - 1);
}

#define LOCK_MASK   1ULL
#define UNLOCK_MASK ~1ULL

static inline void
lock_block(uint64_t *metadata)
{
#ifdef ENABLE_BLOCK_LOCKING
   uint64_t *data = metadata + 7;
   while ((__sync_fetch_and_or(data, LOCK_MASK) & 1) != 0) {
      _mm_pause();
   }
#endif
}

static inline void
unlock_block(uint64_t *metadata)
{
#ifdef ENABLE_BLOCK_LOCKING
   uint64_t *data = metadata + 7;
   *data          = *data & UNLOCK_MASK;
#endif
}

static inline uint32_t
slot_mask_32(uint8_t *metadata, uint8_t fprint)
{
   __m256i bcast = _mm256_set1_epi8(fprint);
   __m256i block = _mm256_loadu_si256((const __m256i *)(metadata));
#if defined __AVX512BW__ && defined __AVX512VL__
   return _mm256_cmp_epi8_mask(bcast, block, _MM_CMPINT_EQ);
#else
   __m256i cmp = _mm256_cmpeq_epi8(bcast, block);
   return _mm256_movemask_epi8(cmp);
#endif
}

#if defined __AVX512F__ && defined __AVX512BW__
static inline uint64_t
slot_mask_64(uint8_t *metadata, uint8_t fprint)
{
   __m512i mask  = _mm512_loadu_si512((const __m512i *)(broadcast_mask));
   __m512i bcast = _mm512_set1_epi8(fprint);
   bcast         = _mm512_or_epi64(bcast, mask);
   __m512i block = _mm512_loadu_si512((const __m512i *)(metadata));
   block         = _mm512_or_epi64(block, mask);
   return _mm512_cmp_epi8_mask(bcast, block, _MM_CMPINT_EQ);
}
#else  /* ! (defined __AVX512F__ && defined __AVX512BW__) */
static inline uint32_t
slot_mask_64_half(__m256i fprint, __m256i md, __m256i mask)
{
   __m256i masked_fp = _mm256_or_si256(fprint, mask);
   __m256i masked_md = _mm256_or_si256(md, mask);
   __m256i cmp = _mm256_cmpeq_epi8(masked_md, masked_fp);
   return _mm256_movemask_epi8(cmp);
}

static inline uint64_t
slot_mask_64(uint8_t *metadata, uint8_t fp)
{
   __m256i fprint = _mm256_set1_epi8(fp);

   __m256i md1 = _mm256_loadu_si256((const __m256i *)(metadata));
   __m256i mask1 = _mm256_loadu_si256((const __m256i *)(broadcast_mask));
   uint64_t result1 = slot_mask_64_half(fprint, md1, mask1);

   __m256i md2 = _mm256_loadu_si256((const __m256i *)(&metadata[32]));
   __m256i mask2 = _mm256_loadu_si256((const __m256i *)(&broadcast_mask[32]));
   uint64_t result2 = slot_mask_64_half(fprint, md2, mask2);

   return ((uint64_t)result2 << 32) | result1;
}
#endif /* ! (defined __AVX512F__ && defined __AVX512BW__) */


static inline void __attribute__((unused))
atomic_write_128(slice key, ValueType val, uint64_t *slot)
{
   kv_pair t = {.key = key, .val = val};
   __m128d a = _mm_load_pd((double *)&t);
   _mm_store_pd((double *)slot, a);
}

static uint64_t
iceberg_block_load(iceberg_table *table, uint64_t index, uint8_t level)
{
   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
   if (level == 1) {
      __mmask64 mask64 =
         slot_mask_64(table->metadata.lv1_md[bindex][boffset].block_md, 0);
      return (1ULL << SLOT_BITS) - __builtin_popcountll(mask64);
   } else if (level == 2) {
      __mmask32 mask32 =
         slot_mask_32(table->metadata.lv2_md[bindex][boffset].block_md, 0)
         & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
      return (C_LV2 + MAX_LG_LG_N / D_CHOICES) - __builtin_popcountll(mask32);
   } else
      return table->metadata.lv3_sizes[bindex][boffset];
}

static uint64_t __attribute__((unused)) iceberg_table_load(iceberg_table *table)
{
   uint64_t total = 0;

   for (uint8_t i = 1; i <= 3; ++i) {
      for (uint64_t j = 0; j < table->metadata.nblocks; ++j) {
         total += iceberg_block_load(table, j, i);
      }
   }

   return total;
}

static double __attribute__((unused))
iceberg_block_load_factor(iceberg_table *table, uint64_t index, uint8_t level)
{
   if (level == 1)
      return iceberg_block_load(table, index, level)
             / (double)(1ULL << SLOT_BITS);
   else if (level == 2)
      return iceberg_block_load(table, index, level)
             / (double)(C_LV2 + MAX_LG_LG_N / D_CHOICES);
   else
      return iceberg_block_load(table, index, level);
}

static inline size_t __attribute__((unused)) round_up(size_t n, size_t k)
{
   size_t rem = n % k;
   if (rem == 0) {
      return n;
   }
   n += k - rem;
   return n;
}

void
transform_sketch_value_default(ValueType *hash_table_value,
                               ValueType  sketch_value)
{
   bool      should_retry;
   ValueType expected;
   do {
      should_retry = TRUE;
      expected     = __atomic_load_n(hash_table_value, __ATOMIC_RELAXED);
      should_retry = !__atomic_compare_exchange_n(hash_table_value,
                                                  &expected,
                                                  sketch_value,
                                                  TRUE,
                                                  __ATOMIC_RELAXED,
                                                  __ATOMIC_RELAXED);
   } while (should_retry);
}

void
iceberg_config_default_init(iceberg_config *config)
{
   config->log_slots               = 0;
   config->merge_value_from_sketch = NULL;
   config->transform_sketch_value  = &transform_sketch_value_default;
   config->post_remove             = NULL;
}

int
iceberg_init(iceberg_table     *table,
             iceberg_config    *config,
             const data_config *spl_data_config)
{
   memset(table, 0, sizeof(*table));

   memcpy(&table->config, config, sizeof(table->config));

   const uint64_t log_slots = config->log_slots;

   uint64_t total_blocks = 1 << (log_slots - SLOT_BITS);
   uint64_t total_size_in_bytes =
      (sizeof(iceberg_lv1_block) + sizeof(iceberg_lv2_block)
       + sizeof(iceberg_lv1_block_md) + sizeof(iceberg_lv2_block_md))
      * total_blocks;

   assert(table);

#if defined(HUGE_TLB)
   int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB;
#else
   int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
#endif

   size_t level1_size = sizeof(iceberg_lv1_block) * total_blocks;
   // table->level1 = (iceberg_lv1_block *)malloc(level1_size);
   table->level1[0] = (iceberg_lv1_block *)mmap(
      NULL, level1_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->level1[0]) {
      perror("level1 malloc failed");
      exit(1);
   }
   size_t level2_size = sizeof(iceberg_lv2_block) * total_blocks;
   // table->level2 = (iceberg_lv2_block *)malloc(level2_size);
   table->level2[0] = (iceberg_lv2_block *)mmap(
      NULL, level2_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->level2[0]) {
      perror("level2 malloc failed");
      exit(1);
   }
   size_t level3_size = sizeof(iceberg_lv3_list) * total_blocks;
   table->level3[0]   = (iceberg_lv3_list *)mmap(
      NULL, level3_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->level3[0]) {
      perror("level3 malloc failed");
      exit(1);
   }

   table->metadata.total_size_in_bytes = total_size_in_bytes;
   table->metadata.nslots              = 1 << log_slots;
   table->metadata.nblocks             = total_blocks;
   table->metadata.block_bits          = log_slots - SLOT_BITS;
   table->metadata.init_size           = total_blocks;
   table->metadata.log_init_size       = log2(total_blocks);
   table->metadata.nblocks_parts[0]    = total_blocks;

   uint32_t procs = get_nprocs();
   pc_init(&table->metadata.lv1_balls, &table->metadata.lv1_ctr, procs, 1000);
   pc_init(&table->metadata.lv2_balls, &table->metadata.lv2_ctr, procs, 1000);
   pc_init(&table->metadata.lv3_balls, &table->metadata.lv3_ctr, procs, 1000);

   size_t lv1_md_size = sizeof(iceberg_lv1_block_md) * total_blocks + 64;
   // table->metadata.lv1_md = (iceberg_lv1_block_md
   // *)malloc(sizeof(iceberg_lv1_block_md) * total_blocks);
   table->metadata.lv1_md[0] = (iceberg_lv1_block_md *)mmap(
      NULL, lv1_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->metadata.lv1_md[0]) {
      perror("lv1_md malloc failed");
      exit(1);
   }
   // table->metadata.lv2_md = (iceberg_lv2_block_md
   // *)malloc(sizeof(iceberg_lv2_block_md) * total_blocks);
   size_t lv2_md_size        = sizeof(iceberg_lv2_block_md) * total_blocks + 32;
   table->metadata.lv2_md[0] = (iceberg_lv2_block_md *)mmap(
      NULL, lv2_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->metadata.lv2_md[0]) {
      perror("lv2_md malloc failed");
      exit(1);
   }
   table->metadata.lv3_sizes[0] =
      (uint64_t *)mmap(NULL,
                       sizeof(uint64_t) * total_blocks,
                       PROT_READ | PROT_WRITE,
                       mmap_flags,
                       0,
                       0);
   if (!table->metadata.lv3_sizes[0]) {
      perror("lv3_sizes malloc failed");
      exit(1);
   }
   table->metadata.lv3_locks[0] =
      (uint8_t *)mmap(NULL,
                      sizeof(uint8_t) * total_blocks,
                      PROT_READ | PROT_WRITE,
                      mmap_flags,
                      0,
                      0);
   if (!table->metadata.lv3_locks[0]) {
      perror("lv3_locks malloc failed");
      exit(1);
   }

#ifdef ENABLE_RESIZE
   table->metadata.resize_cnt     = 0;
   table->metadata.lv1_resize_ctr = total_blocks;
   table->metadata.lv2_resize_ctr = total_blocks;
   table->metadata.lv3_resize_ctr = total_blocks;

   // create one marker for 8 blocks.
   size_t resize_marker_size            = sizeof(uint8_t) * total_blocks / 8;
   table->metadata.lv1_resize_marker[0] = (uint8_t *)mmap(
      NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->metadata.lv1_resize_marker[0]) {
      perror("level1 resize ctr malloc failed");
      exit(1);
   }
   table->metadata.lv2_resize_marker[0] = (uint8_t *)mmap(
      NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->metadata.lv2_resize_marker[0]) {
      perror("level2 resize ctr malloc failed");
      exit(1);
   }
   table->metadata.lv3_resize_marker[0] = (uint8_t *)mmap(
      NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (!table->metadata.lv3_resize_marker[0]) {
      perror("level3 resize ctr malloc failed");
      exit(1);
   }
   memset(table->metadata.lv1_resize_marker[0], 1, resize_marker_size);
   memset(table->metadata.lv2_resize_marker[0], 1, resize_marker_size);
   memset(table->metadata.lv3_resize_marker[0], 1, resize_marker_size);

   table->metadata.marker_sizes[0] = resize_marker_size;
   table->metadata.lock            = 0;
#endif

   for (uint64_t i = 0; i < total_blocks; ++i) {
      for (uint64_t j = 0; j < (1 << SLOT_BITS); ++j) {
         table->level1[0][i].slots[j].key      = NULL_SLICE;
         table->level1[0][i].slots[j].refcount = 0;
      }

      for (uint64_t j = 0; j < C_LV2 + MAX_LG_LG_N / D_CHOICES; ++j) {
         table->level2[0][i].slots[j].key      = NULL_SLICE;
         table->level2[0][i].slots[j].refcount = 0;
      }
      table->level3[0]->head = NULL;
   }

   memset((char *)table->metadata.lv1_md[0], 0, lv1_md_size);
   memset((char *)table->metadata.lv2_md[0], 0, lv2_md_size);
   memset(table->metadata.lv3_sizes[0], 0, total_blocks * sizeof(uint64_t));
   memset(table->metadata.lv3_locks[0], 0, total_blocks * sizeof(uint8_t));

   table->spl_data_config = spl_data_config;

   return 0;
}

int
iceberg_init_with_sketch(iceberg_table     *table,
                         iceberg_config    *config,
                         const data_config *spl_data_config,
                         sketch_config     *sktch_config)
{
   iceberg_init(table, config, spl_data_config);

   table->sktch = TYPED_ZALLOC(0, table->sktch);
   sketch_init(sktch_config, table->sktch);

   return 0;
}

#ifdef ENABLE_RESIZE
static inline bool
is_lv1_resize_active(iceberg_table *table)
{
   uint64_t half_mark = table->metadata.nblocks >> 1;
   uint64_t lv1_ctr =
      __atomic_load_n(&table->metadata.lv1_resize_ctr, __ATOMIC_SEQ_CST);
   return lv1_ctr < half_mark;
}

static inline bool
is_lv2_resize_active(iceberg_table *table)
{
   uint64_t half_mark = table->metadata.nblocks >> 1;
   uint64_t lv2_ctr =
      __atomic_load_n(&table->metadata.lv2_resize_ctr, __ATOMIC_SEQ_CST);
   return lv2_ctr < half_mark;
}

static inline bool
is_lv3_resize_active(iceberg_table *table)
{
   uint64_t half_mark = table->metadata.nblocks >> 1;
   uint64_t lv3_ctr =
      __atomic_load_n(&table->metadata.lv3_resize_ctr, __ATOMIC_SEQ_CST);
   return lv3_ctr < half_mark;
}

static bool
is_resize_active(iceberg_table *table)
{
   return is_lv3_resize_active(table) || is_lv2_resize_active(table)
          || is_lv1_resize_active(table);
}

static bool
iceberg_setup_resize(iceberg_table *table)
{
   // grab write lock
   if (!lock(&table->metadata.lock, TRY_ONCE_LOCK))
      return false;

   if (unlikely(!need_resize(table))) {
      unlock(&table->metadata.lock);
      return false;
   }
   if (is_resize_active(table)) {
      // finish the current resize
      iceberg_end(table);
      unlock(&table->metadata.lock);
      return false;
   }

   printf("Setting up resize\n");
   iceberg_print_state(table);

#   if defined(HUGE_TLB)
   int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB;
#   else
   int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
#   endif

   // compute new sizes
   uint64_t cur_blocks = table->metadata.nblocks;
   uint64_t resize_cnt = table->metadata.resize_cnt + 1;

   // Allocate new table and metadata
   // alloc level1
   size_t level1_size        = sizeof(iceberg_lv1_block) * cur_blocks;
   table->level1[resize_cnt] = (iceberg_lv1_block *)mmap(
      NULL, level1_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->level1[resize_cnt] == (void *)-1) {
      perror("level1 resize failed");
      exit(1);
   }

   // alloc level2
   size_t level2_size        = sizeof(iceberg_lv2_block) * cur_blocks;
   table->level2[resize_cnt] = (iceberg_lv2_block *)mmap(
      NULL, level2_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->level2[resize_cnt] == (void *)-1) {
      perror("level2 resize failed");
      exit(1);
   }

   // alloc level3
   size_t level3_size        = sizeof(iceberg_lv3_list) * cur_blocks;
   table->level3[resize_cnt] = (iceberg_lv3_list *)mmap(
      NULL, level3_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->level3[resize_cnt] == (void *)-1) {
      perror("level3 resize failed");
      exit(1);
   }

   // alloc level1 metadata
   size_t lv1_md_size = sizeof(iceberg_lv1_block_md) * cur_blocks + 64;
   table->metadata.lv1_md[resize_cnt] = (iceberg_lv1_block_md *)mmap(
      NULL, lv1_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->metadata.lv1_md[resize_cnt] == (void *)-1) {
      perror("lv1_md resize failed");
      exit(1);
   }

   // alloc level2 metadata
   size_t lv2_md_size = sizeof(iceberg_lv2_block_md) * cur_blocks + 32;
   table->metadata.lv2_md[resize_cnt] = (iceberg_lv2_block_md *)mmap(
      NULL, lv2_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->metadata.lv2_md[resize_cnt] == (void *)-1) {
      perror("lv2_md resize failed");
      exit(1);
   }

   // alloc level3 metadata (sizes, locks)
   size_t lv3_sizes_size                 = sizeof(uint64_t) * cur_blocks;
   table->metadata.lv3_sizes[resize_cnt] = (uint64_t *)mmap(
      NULL, lv3_sizes_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->metadata.lv3_sizes[resize_cnt] == (void *)-1) {
      perror("lv3_sizes resize failed");
      exit(1);
   }

   size_t lv3_locks_size                 = sizeof(uint8_t) * cur_blocks;
   table->metadata.lv3_locks[resize_cnt] = (uint8_t *)mmap(
      NULL, lv3_locks_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->metadata.lv3_locks[resize_cnt] == (void *)-1) {
      perror("lv3_locks remap failed");
      exit(1);
   }
   memset(
      table->metadata.lv3_locks[resize_cnt], 0, cur_blocks * sizeof(uint8_t));

   // alloc resize markers
   // resize_marker_size
   size_t resize_marker_size = sizeof(uint8_t) * cur_blocks / 8;
   table->metadata.lv1_resize_marker[resize_cnt] = (uint8_t *)mmap(
      NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->metadata.lv1_resize_marker[resize_cnt] == (void *)-1) {
      perror("level1 resize failed");
      exit(1);
   }

   table->metadata.lv2_resize_marker[resize_cnt] = (uint8_t *)mmap(
      NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->metadata.lv2_resize_marker[resize_cnt] == (void *)-1) {
      perror("level1 resize failed");
      exit(1);
   }

   table->metadata.lv3_resize_marker[resize_cnt] = (uint8_t *)mmap(
      NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
   if (table->metadata.lv3_resize_marker[resize_cnt] == (void *)-1) {
      perror("level1 resize failed");
      exit(1);
   }

   table->metadata.marker_sizes[resize_cnt] = resize_marker_size;
   // resetting the resize markers.
   for (uint64_t i = 0; i <= resize_cnt; ++i) {
      memset(table->metadata.lv1_resize_marker[i],
             0,
             table->metadata.marker_sizes[i]);
      memset(table->metadata.lv2_resize_marker[i],
             0,
             table->metadata.marker_sizes[i]);
      memset(table->metadata.lv3_resize_marker[i],
             0,
             table->metadata.marker_sizes[i]);
   }

   uint64_t total_blocks = table->metadata.nblocks * 2;
   uint64_t total_size_in_bytes =
      (sizeof(iceberg_lv1_block) + sizeof(iceberg_lv2_block)
       + sizeof(iceberg_lv1_block_md) + sizeof(iceberg_lv2_block_md))
      * total_blocks;

   // increment resize cnt
   table->metadata.resize_cnt += 1;

   // update metadata
   table->metadata.total_size_in_bytes = total_size_in_bytes;
   table->metadata.nslots *= 2;
   table->metadata.nblocks = total_blocks;
   table->metadata.block_bits += 1;
   table->metadata.nblocks_parts[resize_cnt] = total_blocks;

   // reset the block ctr
   table->metadata.lv1_resize_ctr = 0;
   table->metadata.lv2_resize_ctr = 0;
   table->metadata.lv3_resize_ctr = 0;

   /*printf("Setting up finished\n");*/
   unlock(&table->metadata.lock);
   return true;
}

static bool
iceberg_lv1_move_block(iceberg_table *table, uint64_t bnum, threadid thread_id);
static bool
iceberg_lv2_move_block(iceberg_table *table, uint64_t bnum, threadid thread_id);
static bool
iceberg_lv3_move_block(iceberg_table *table, uint64_t bnum, threadid thread_id);

// finish moving blocks that are left during the last resize.
void
iceberg_end(iceberg_table *table)
{
   if (is_lv1_resize_active(table)) {
      for (uint64_t j = 0; j < table->metadata.nblocks / 8; ++j) {
         uint64_t chunk_idx = j;
         uint64_t mindex, moffset;
         get_index_offset(
            table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
         // if fixing is needed set the marker
         if (!__sync_lock_test_and_set(
                &table->metadata.lv1_resize_marker[mindex][moffset], 1))
         {
            for (uint8_t i = 0; i < 8; ++i) {
               uint64_t idx = chunk_idx * 8 + i;
               iceberg_lv1_move_block(table, idx, 0);
            }
            // set the marker for the dest block
            uint64_t dest_chunk_idx =
               chunk_idx + table->metadata.nblocks / 8 / 2;
            uint64_t mindex, moffset;
            get_index_offset(table->metadata.log_init_size - 3,
                             dest_chunk_idx,
                             &mindex,
                             &moffset);
            __sync_lock_test_and_set(
               &table->metadata.lv1_resize_marker[mindex][moffset], 1);
         }
      }
   }
   if (is_lv2_resize_active(table)) {
      for (uint64_t j = 0; j < table->metadata.nblocks / 8; ++j) {
         uint64_t chunk_idx = j;
         uint64_t mindex, moffset;
         get_index_offset(
            table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
         // if fixing is needed set the marker
         if (!__sync_lock_test_and_set(
                &table->metadata.lv2_resize_marker[mindex][moffset], 1))
         {
            for (uint8_t i = 0; i < 8; ++i) {
               uint64_t idx = chunk_idx * 8 + i;
               iceberg_lv2_move_block(table, idx, 0);
            }
            // set the marker for the dest block
            uint64_t dest_chunk_idx =
               chunk_idx + table->metadata.nblocks / 8 / 2;
            uint64_t mindex, moffset;
            get_index_offset(table->metadata.log_init_size - 3,
                             dest_chunk_idx,
                             &mindex,
                             &moffset);
            __sync_lock_test_and_set(
               &table->metadata.lv2_resize_marker[mindex][moffset], 1);
         }
      }
   }
   if (is_lv3_resize_active(table)) {
      for (uint64_t j = 0; j < table->metadata.nblocks / 8; ++j) {
         uint64_t chunk_idx = j;
         uint64_t mindex, moffset;
         get_index_offset(
            table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
         // if fixing is needed set the marker
         if (!__sync_lock_test_and_set(
                &table->metadata.lv3_resize_marker[mindex][moffset], 1))
         {
            for (uint8_t i = 0; i < 8; ++i) {
               uint64_t idx = chunk_idx * 8 + i;
               iceberg_lv3_move_block(table, idx, 0);
            }
            // set the marker for the dest block
            uint64_t dest_chunk_idx =
               chunk_idx + table->metadata.nblocks / 8 / 2;
            uint64_t mindex, moffset;
            get_index_offset(table->metadata.log_init_size - 3,
                             dest_chunk_idx,
                             &mindex,
                             &moffset);
            __sync_lock_test_and_set(
               &table->metadata.lv3_resize_marker[mindex][moffset], 1);
         }
      }
   }

   /*printf("Final resize done.\n");*/
   /*printf("Final resize done. Table load: %ld\n",
    * iceberg_table_load(table));*/
}
#endif

static inline bool
iceberg_lv3_insert(iceberg_table *table,
                   slice          key,
                   ValueType      value,
                   uint64_t       refcount,
                   uint64_t       lv3_index,
                   threadid       thread_id)
{

#ifdef ENABLE_RESIZE
   if (unlikely(lv3_index < (table->metadata.nblocks >> 1)
                && is_lv3_resize_active(table)))
   {
      uint64_t chunk_idx = lv3_index / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      // if fixing is needed set the marker
      if (!__sync_lock_test_and_set(
             &table->metadata.lv3_resize_marker[mindex][moffset], 1))
      {
         for (uint8_t i = 0; i < 8; ++i) {
            uint64_t idx = chunk_idx * 8 + i;
            /*printf("LV3 Before: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 3));*/
            iceberg_lv3_move_block(table, idx, thread_id);
            /*printf("LV3 After: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 3));*/
         }
         // set the marker for the dest block
         uint64_t dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
         uint64_t mindex, moffset;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         __sync_lock_test_and_set(
            &table->metadata.lv3_resize_marker[mindex][moffset], 1);
      }
   }
#endif

   uint64_t bindex, boffset;
   get_index_offset(
      table->metadata.log_init_size, lv3_index, &bindex, &boffset);
   iceberg_metadata *metadata = &table->metadata;
   iceberg_lv3_list *lists    = table->level3[bindex];

   while (__sync_lock_test_and_set(metadata->lv3_locks[bindex] + boffset, 1))
      ;

   // iceberg_lv3_node *new_node =
   //    (iceberg_lv3_node *)malloc(sizeof(iceberg_lv3_node));
   iceberg_lv3_node *new_node;
   posix_memalign((void **)&new_node, 64, sizeof(iceberg_lv3_node));

   new_node->kv.key      = key;
   new_node->kv.val      = value;
   new_node->kv.refcount = refcount;

   // printf("tid %d %p %s %s insert refcount: %d\n", thread_id, (void *)table,
   // __func__, key, value.refcount);

   new_node->next_node = lists[boffset].head;
   lists[boffset].head = new_node;

   metadata->lv3_sizes[bindex][boffset]++;
   pc_add(&metadata->lv3_balls, 1, thread_id);
   metadata->lv3_locks[bindex][boffset] = 0;

   return true;
}

static inline bool
iceberg_lv2_insert_internal(iceberg_table *table,
                            slice          key,
                            ValueType      value,
                            uint64_t       refcount,
                            uint8_t        fprint,
                            uint64_t       index,
                            threadid       thread_id)
{
   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);

   iceberg_metadata  *metadata = &table->metadata;
   iceberg_lv2_block *blocks   = table->level2[bindex];

start:;
   __mmask32 md_mask =
      slot_mask_32(metadata->lv2_md[bindex][boffset].block_md, 0)
      & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
   uint8_t popct = __builtin_popcountll(md_mask);

   if (unlikely(!popct))
      return false;

   uint8_t start = 0;
   /*for(uint8_t i = start; i < start + popct; ++i) {*/

   uint8_t slot = word_select(md_mask, start);

   if (__sync_bool_compare_and_swap(
          metadata->lv2_md[bindex][boffset].block_md + slot, 0, 1))
   {
      pc_add(&metadata->lv2_balls, 1, thread_id);
      blocks[boffset].slots[slot].key      = key;
      blocks[boffset].slots[slot].val      = value;
      blocks[boffset].slots[slot].refcount = refcount;
      // ValueType value_with_refcount = {.value    = value.value,
      //                                  .refcount = value.refcount};
      // // printf("tid %d %p %s %s before update refcount: %d\n", thread_id,
      // (void *)table, __func__, key,
      // blocks[boffset].slots[slot].val.refcount);

      // atomic_write_128(
      //    key, value_with_refcount, (uint64_t *)&blocks[boffset].slots[slot]);

      // printf("tid %d %p %s %s after update refcount: %d\n", thread_id, (void
      // *)table, __func__, key, blocks[boffset].slots[slot].val.refcount);

      metadata->lv2_md[bindex][boffset].block_md[slot] = fprint;
      return true;
   }
   goto start;
   /*}*/

   return false;
}

static inline bool
iceberg_lv2_insert(iceberg_table *table,
                   slice          key,
                   ValueType      value,
                   uint64_t       refcount,
                   uint64_t       lv3_index,
                   threadid       thread_id)
{

   iceberg_metadata *metadata = &table->metadata;

   if (metadata->lv2_ctr == (int64_t)(C_LV2 * metadata->nblocks))
      return iceberg_lv3_insert(
         table, key, value, refcount, lv3_index, thread_id);

   uint8_t  fprint1, fprint2;
   uint64_t index1, index2;

   split_hash(lv2_hash(key, 0), &fprint1, &index1, metadata);
   split_hash(lv2_hash(key, 1), &fprint2, &index2, metadata);

   uint64_t bindex1, boffset1, bindex2, boffset2;
   get_index_offset(table->metadata.log_init_size, index1, &bindex1, &boffset1);
   get_index_offset(table->metadata.log_init_size, index2, &bindex2, &boffset2);

   __mmask32 md_mask1 =
      slot_mask_32(metadata->lv2_md[bindex1][boffset1].block_md, 0)
      & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
   __mmask32 md_mask2 =
      slot_mask_32(metadata->lv2_md[bindex2][boffset2].block_md, 0)
      & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);

   uint8_t popct1 = __builtin_popcountll(md_mask1);
   uint8_t popct2 = __builtin_popcountll(md_mask2);

   if (popct2 > popct1) {
      fprint1  = fprint2;
      index1   = index2;
      bindex1  = bindex2;
      boffset1 = boffset2;
      md_mask1 = md_mask2;
      popct1   = popct2;
   }

#ifdef ENABLE_RESIZE
   // move blocks if resize is active and not already moved.
   if (unlikely(index1 < (table->metadata.nblocks >> 1)
                && is_lv2_resize_active(table)))
   {
      uint64_t chunk_idx = index1 / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      // if fixing is needed set the marker
      if (!__sync_lock_test_and_set(
             &table->metadata.lv2_resize_marker[mindex][moffset], 1))
      {
         for (uint8_t i = 0; i < 8; ++i) {
            uint64_t idx = chunk_idx * 8 + i;
            /*printf("LV2 Before: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 2));*/
            iceberg_lv2_move_block(table, idx, thread_id);
            /*printf("LV2 After: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 2));*/
         }
         // set the marker for the dest block
         uint64_t dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
         uint64_t mindex, moffset;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         __sync_lock_test_and_set(
            &table->metadata.lv2_resize_marker[mindex][moffset], 1);
      }
   }
#endif

   if (iceberg_lv2_insert_internal(
          table, key, value, refcount, fprint1, index1, thread_id))
      return true;

   return iceberg_lv3_insert(table, key, value, refcount, lv3_index, thread_id);
}

static bool
iceberg_insert_internal(iceberg_table *table,
                        slice          key,
                        ValueType      value,
                        uint64_t       refcount,
                        uint8_t        fprint,
                        uint64_t       bindex,
                        uint64_t       boffset,
                        threadid       thread_id)
{
   iceberg_metadata  *metadata = &table->metadata;
   iceberg_lv1_block *blocks   = table->level1[bindex];
start:;
   __mmask64 md_mask =
      slot_mask_64(metadata->lv1_md[bindex][boffset].block_md, 0);

   uint8_t popct = __builtin_popcountll(md_mask);

   if (unlikely(!popct))
      return false;

   uint8_t start = 0;
   uint8_t slot  = word_select(md_mask, start);

   /*if(__sync_bool_compare_and_swap(metadata->lv1_md[bindex][boffset].block_md
    * + slot, 0, 1)) {*/
   pc_add(&metadata->lv1_balls, 1, thread_id);
   blocks[boffset].slots[slot].key      = key;
   blocks[boffset].slots[slot].val      = value;
   blocks[boffset].slots[slot].refcount = refcount;
   // ValueType value_with_refcount = {.value    = value.value,
   //                                  .refcount = value.refcount};
   // // printf("tid %d %p %s %s before update refcount: %d\n", thread_id, (void
   // *)table, __func__, key, blocks[boffset].slots[slot].val.refcount);

   // atomic_write_128(
   //    key, value_with_refcount, (uint64_t *)&blocks[boffset].slots[slot]);
   // printf("tid %d %p %s %s after update refcount: %d\n", thread_id, (void
   // *)table, __func__, key, blocks[boffset].slots[slot].val.refcount);

   metadata->lv1_md[bindex][boffset].block_md[slot] = fprint;
   return true;
   /*}*/
   goto start;
   /*}*/

   return false;
}


static bool
iceberg_get_value_internal(iceberg_table *table,
                           slice          key,
                           kv_pair      **kv,
                           threadid       thread_id,
                           bool           should_lock,
                           bool           should_lookup_sketch);

static bool
iceberg_put_or_insert(iceberg_table *table,
                      slice         *key,
                      ValueType    **value,
                      threadid       thread_id,
                      bool           increase_refcount,
                      bool           overwrite_value)
{
#ifdef ENABLE_RESIZE
   if (unlikely(need_resize(table))) {
      iceberg_setup_resize(table);
   }
#endif

   iceberg_metadata *metadata = &table->metadata;
   uint8_t           fprint;
   uint64_t          index;

   split_hash(lv1_hash(*key), &fprint, &index, metadata);

#ifdef ENABLE_RESIZE
   // move blocks if resize is active and not already moved.
   if (unlikely(index < (table->metadata.nblocks >> 1)
                && is_lv1_resize_active(table)))
   {
      uint64_t chunk_idx = index / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      // if fixing is needed set the marker
      if (!__sync_lock_test_and_set(
             &table->metadata.lv1_resize_marker[mindex][moffset], 1))
      {
         for (uint8_t i = 0; i < 8; ++i) {
            uint64_t idx = chunk_idx * 8 + i;
            /*printf("LV1 Before: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 1));*/
            iceberg_lv1_move_block(table, idx, thread_id);
            /*printf("LV1 After: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 1));*/
         }
         // set the marker for the dest block
         uint64_t dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
         uint64_t mindex, moffset;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         __sync_lock_test_and_set(
            &table->metadata.lv1_resize_marker[mindex][moffset], 1);
      }
   }
#endif
   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);

   // struct timespec before_lock, after_lock, unlock;
   // clock_gettime(CLOCK_MONOTONIC, &before_lock);

   lock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);

   // clock_gettime(CLOCK_MONOTONIC, &after_lock);
   // printf("tid %d: %s before_lock - after_lock: %lu ns\n", thread_id,
   // __func__, (after_lock.tv_sec - before_lock.tv_sec) * 1000000000 +
   // (after_lock.tv_nsec - before_lock.tv_nsec));

   kv_pair *kv = NULL;
   if (unlikely(iceberg_get_value_internal(
          table, *key, &kv, thread_id, false, false)))
   {
      // printf("tid %d %p %s %s previous refcount: %lu\n", thread_id, (void
      // *)table, __func__, key, kv->refcount);

      if (increase_refcount) {
         kv->refcount++;
      }
      if (overwrite_value) {
         kv->val = **value;
      }

      *key   = kv->key;
      *value = &kv->val;

      // printf("tid %lu %p %s %s refcount: %lu (val1: %lu, val2: %lu)\n",
      // thread_id, (void *)table,
      //       __func__, (char *)slice_data(*key), kv->refcount - 1,
      //       *((uint64 *)&kv->val), *((uint64 *)&kv->val + 8));

      // clock_gettime(CLOCK_MONOTONIC, &unlock);
      // printf("tid %d: %s after_lock ~ unlock:  %lu ns\n", thread_id,
      // __func__, (unlock.tv_sec - after_lock.tv_sec) * 1000000000 +
      // (unlock.tv_nsec - after_lock.tv_nsec)); printf("tid %d: %s before_lock
      // ~ unlock:  %lu ns\n", thread_id, __func__, (unlock.tv_sec -
      // before_lock.tv_sec) * 1000000000 + (unlock.tv_nsec -
      // before_lock.tv_nsec));

      /*printf("Found!\n");*/
      unlock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
      return true && overwrite_value;
   }

   const uint64_t refcount = 1;

   // Copy the key and insert it to the table.
   char *key_copy_buffer;
   key_copy_buffer = TYPED_ARRAY_ZALLOC(0, key_copy_buffer, slice_length(*key));
   *key            = slice_copy_contents(key_copy_buffer, *key);

   bool ret = iceberg_insert_internal(
      table, *key, **value, refcount, fprint, bindex, boffset, thread_id);
   if (!ret)
      ret =
         iceberg_lv2_insert(table, *key, **value, refcount, index, thread_id);

   if (ret) {
      iceberg_get_value_internal(table, *key, &kv, thread_id, false, false);
      // If the sketch is enabled, get the value from the sketch to
      // the new item and set the max.
      if (table->sktch && !overwrite_value) {
         platform_assert(table->config.merge_value_from_sketch);
         table->config.merge_value_from_sketch(&kv->val,
                                               sketch_get(table->sktch, *key));
      }
      *value = &kv->val;
   }

   // If it fails to insert the key, free the key memory and return
   // the key as NULL.
   if (!ret) {
      platform_free(0, key_copy_buffer);
      *key = NULL_SLICE;
   }

   // clock_gettime(CLOCK_MONOTONIC, &unlock);
   // printf("tid %d: %s new_item after_lock ~ unlock:  %lu ns\n", thread_id,
   // __func__, (unlock.tv_sec - after_lock.tv_sec) * 1000000000 +
   // (unlock.tv_nsec - after_lock.tv_nsec)); printf("tid %d: %s new_item
   // before_lock ~ unlock:  %lu ns\n", thread_id, __func__, (unlock.tv_sec -
   // before_lock.tv_sec) * 1000000000 + (unlock.tv_nsec -
   // before_lock.tv_nsec));

   // printf("tid %lu %p %s %s refcount: %lu (val1: %lu, val2: %lu)\n",
   // thread_id, (void *)table,
   //          __func__, (char *)slice_data(*key), kv->refcount - 1,
   //          *((uint64 *)&kv->val), *((uint64 *)&kv->val + 8));
   unlock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
   return ret;
}

__attribute__((always_inline)) bool
iceberg_insert(iceberg_table *table,
               slice         *key,
               ValueType      value,
               threadid       thread_id)
{
   // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);
   ValueType *value_ptr = &value;
   return iceberg_put_or_insert(table, key, &value_ptr, thread_id, true, false);
}

__attribute__((always_inline)) bool
iceberg_insert_without_increasing_refcount(iceberg_table *table,
                                           slice         *key,
                                           ValueType      value,
                                           threadid       thread_id)
{
   // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);
   ValueType *value_ptr = &value;
   return iceberg_put_or_insert(
      table, key, &value_ptr, thread_id, false, false);
}

__attribute__((always_inline)) bool
iceberg_insert_and_get(iceberg_table *table,
                       slice         *key,
                       ValueType    **value,
                       threadid       thread_id)
{
   // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);
   return iceberg_put_or_insert(table, key, value, thread_id, true, false);
}

__attribute__((always_inline)) bool
iceberg_insert_and_get_without_increasing_refcount(iceberg_table *table,
                                                   slice         *key,
                                                   ValueType    **value,
                                                   threadid       thread_id)
{
   // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);
   return iceberg_put_or_insert(table, key, value, thread_id, false, false);
}

// __attribute__((always_inline)) bool
// iceberg_insert_and_get(iceberg_table *table,
//                slice        key,
//                ValueType      value,
//                threadid        thread_id)
// {
//    // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);
//    return iceberg_put_or_insert(table, key, value, thread_id, true, false);
// }

// __attribute__((always_inline)) bool
// iceberg_insert_and_get_without_increasing_refcount(iceberg_table *table,
//                                            slice        key,
//                                            ValueType      value,
//                                            threadid        thread_id)
// {
//    // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);
//    return iceberg_put_or_insert(table, key, value, thread_id, false, false);
// }

bool
iceberg_update(iceberg_table *table,
               slice         *key,
               ValueType      value,
               threadid       thread_id)
{

   iceberg_metadata *metadata = &table->metadata;
   uint8_t           fprint;
   uint64_t          index;

   split_hash(lv1_hash(*key), &fprint, &index, metadata);

   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);

   lock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);

   kv_pair *kv;
   if (likely(iceberg_get_value_internal(
          table, *key, &kv, thread_id, false, false)))
   {
      kv->val = value;
      *key    = kv->key;
      /*printf("Found!\n");*/
      unlock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
      return true;
   }

   *key = NULL_SLICE;

   unlock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
   return false;
}

__attribute__((always_inline)) bool
iceberg_put(iceberg_table *table,
            slice         *key,
            ValueType      value,
            threadid       thread_id)
{
   ValueType *value_ptr = &value;
   return iceberg_put_or_insert(table, key, &value_ptr, thread_id, true, true);
}

static inline void
iceberg_lv3_node_deinit(iceberg_lv3_node *node)
{
   void *ptr = (void *)slice_data(node->kv.key);
   platform_free(0, ptr);
   ptr = (void *)node;
   platform_free(0, ptr);
}

static inline bool
iceberg_lv3_remove_internal(iceberg_table *table,
                            slice          key,
                            uint64_t       lv3_index,
                            bool           delete_item,
                            bool           force_remove,
                            threadid       thread_id)
{
   uint64_t bindex, boffset;
   get_index_offset(
      table->metadata.log_init_size, lv3_index, &bindex, &boffset);

   iceberg_metadata *metadata = &table->metadata;
   iceberg_lv3_list *lists    = table->level3[bindex];

   if (metadata->lv3_sizes[bindex][boffset] == 0) {
      return false;
   }

   while (__sync_lock_test_and_set(metadata->lv3_locks[bindex] + boffset, 1))
      ;

   iceberg_lv3_node *head = lists[boffset].head;

   bool ret = false;

   if (iceberg_key_compare(table->spl_data_config, head->kv.key, key) == 0) {
      // printf("tid %d %p %s %s previous head refcount: %d\n", thread_id, (void
      // *)table, __func__, key, head->val.refcount);
      if (force_remove || (delete_item && head->kv.refcount == 1)) {
         // If it has a sketch, insert the removed value to it.
         if (table->sktch) {
            sketch_insert(table->sktch, head->kv.key, head->kv.val);
         }
         if (table->config.post_remove) {
            table->config.post_remove(&head->kv.val);
         }

         head->kv.refcount          = 0;
         iceberg_lv3_node *old_head = lists[boffset].head;
         lists[boffset].head        = lists[boffset].head->next_node;
         iceberg_lv3_node_deinit(old_head);
         metadata->lv3_sizes[bindex][boffset]--;
         pc_add(&metadata->lv3_balls, -1, thread_id);
         ret = true;
      } else if (head->kv.refcount == 0) {
         ;
      } else {
         head->kv.refcount--;
      }
      // printf("tid %d %p %s %s after head refcount: %d\n", thread_id, (void
      // *)table, __func__, key, head->val.refcount);

      metadata->lv3_locks[bindex][boffset] = 0;
      return ret;
   }

   iceberg_lv3_node *current_node = head;

   for (uint64_t i = 0; i < metadata->lv3_sizes[bindex][boffset] - 1; ++i) {
      iceberg_lv3_node *next_node = current_node->next_node;

      if (iceberg_key_compare(table->spl_data_config, next_node->kv.key, key)
          == 0) {
         // printf("tid %d %p %s %s before next_node refcount: %d\n", thread_id,
         // (void *)table, __func__, key, next_node->val.refcount);

         if (force_remove || (delete_item && next_node->kv.refcount == 1)) {
            // If it has a sketch, insert the removed value to it.
            if (table->sktch) {
               sketch_insert(
                  table->sktch, next_node->kv.key, next_node->kv.val);
            }
            if (table->config.post_remove) {
               table->config.post_remove(&next_node->kv.val);
            }

            key                        = next_node->kv.key;
            next_node->kv.refcount     = 0;
            iceberg_lv3_node *old_node = current_node->next_node;
            current_node->next_node    = current_node->next_node->next_node;
            iceberg_lv3_node_deinit(old_node);
            metadata->lv3_sizes[bindex][boffset]--;
            pc_add(&metadata->lv3_balls, -1, thread_id);
            ret = true;
         } else if (next_node->kv.refcount == 0) {
            ;
         } else {
            next_node->kv.refcount--;
         }
         // printf("tid %d %p %s %s after next_node refcount: %d\n", thread_id,
         // (void *)table, __func__, key, next_node->val.refcount);
         metadata->lv3_locks[bindex][boffset] = 0;

         return ret;
      }

      current_node = next_node;
   }

   metadata->lv3_locks[bindex][boffset] = 0;
   return false;
}

static inline bool
iceberg_lv3_remove(iceberg_table *table,
                   slice          key,
                   uint64_t       lv3_index,
                   bool           delete_item,
                   bool           force_remove,
                   threadid       thread_id)
{

   bool ret = iceberg_lv3_remove_internal(
      table, key, lv3_index, delete_item, force_remove, thread_id);

   if (ret)
      return true;

#ifdef ENABLE_RESIZE
   // check if there's an active resize and block isn't fixed yet
   if (unlikely(is_lv3_resize_active(table)
                && lv3_index >= (table->metadata.nblocks >> 1)))
   {
      uint64_t mask      = ~(1ULL << (table->metadata.block_bits - 1));
      uint64_t old_index = lv3_index & mask;
      uint64_t chunk_idx = old_index / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      if (__atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset],
                          __ATOMIC_SEQ_CST)
          == 0)
      { // not fixed yet
         return iceberg_lv3_remove_internal(
            table, key, old_index, delete_item, force_remove, thread_id);
      } else {
         // wait for the old block to be fixed
         uint64_t dest_chunk_idx = lv3_index / 8;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         while (
            __atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset],
                            __ATOMIC_SEQ_CST)
            == 0)
            ;
      }
   }
#endif

   return false;
}

static inline bool
iceberg_lv2_remove(iceberg_table *table,
                   slice          key,
                   ValueType     *value,
                   uint64_t       lv3_index,
                   bool           delete_item,
                   bool           force_remove,
                   threadid       thread_id)
{
   iceberg_metadata *metadata = &table->metadata;

   for (int i = 0; i < D_CHOICES; ++i) {
      uint8_t  fprint;
      uint64_t index;

      split_hash(lv2_hash(key, i), &fprint, &index, metadata);

      uint64_t bindex, boffset;
      get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
      iceberg_lv2_block *blocks = table->level2[bindex];

#ifdef ENABLE_RESIZE
      // check if there's an active resize and block isn't fixed yet
      if (unlikely(is_lv2_resize_active(table)
                   && index >= (table->metadata.nblocks >> 1)))
      {
         uint64_t mask      = ~(1ULL << (table->metadata.block_bits - 1));
         uint64_t old_index = index & mask;
         uint64_t chunk_idx = old_index / 8;
         uint64_t mindex, moffset;
         get_index_offset(
            table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
         if (__atomic_load_n(
                &table->metadata.lv2_resize_marker[mindex][moffset],
                __ATOMIC_SEQ_CST)
             == 0)
         { // not fixed yet
            uint64_t old_bindex, old_boffset;
            get_index_offset(table->metadata.log_init_size,
                             old_index,
                             &old_bindex,
                             &old_boffset);
            __mmask32 md_mask =
               slot_mask_32(metadata->lv2_md[old_bindex][old_boffset].block_md,
                            fprint)
               & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
            int                popct  = __builtin_popcount(md_mask);
            iceberg_lv2_block *blocks = table->level2[old_bindex];

            bool ret = false;
            for (int i = 0; i < popct; ++i) {
               uint8_t slot = word_select(md_mask, i);

               if (iceberg_key_compare(table->spl_data_config,
                                       blocks[old_boffset].slots[slot].key,
                                       key)
                   == 0)
               {
                  // printf("tid %d %p %s %s Found in the old index refcount:
                  // %d\n", thread_id, (void *)table, __func__, key,
                  // blocks[old_boffset].slots[slot].val.refcount);

                  if (value) {
                     *value = blocks[old_boffset].slots[slot].val;
                  }

                  if (force_remove
                      || (delete_item
                          && blocks[old_boffset].slots[slot].refcount == 1))
                  {
                     // If it has a sketch, insert the removed value to it.
                     if (table->sktch) {
                        sketch_insert(table->sktch,
                                      blocks[old_boffset].slots[slot].key,
                                      blocks[old_boffset].slots[slot].val);
                     }
                     if (table->config.post_remove) {
                        table->config.post_remove(
                           &blocks[old_boffset].slots[slot].val);
                     }

                     metadata->lv2_md[old_bindex][old_boffset].block_md[slot] =
                        0;
                     void *ptr =
                        slice_data(blocks[old_boffset].slots[slot].key);
                     platform_free(0, ptr);
                     blocks[old_boffset].slots[slot].key      = NULL_SLICE;
                     blocks[old_boffset].slots[slot].refcount = 0;
                     pc_add(&metadata->lv2_balls, -1, thread_id);
                     ret = true;
                  } else if (blocks[old_boffset].slots[slot].refcount == 0) {
                     return false;
                  } else {
                     blocks[old_boffset].slots[slot].refcount--;
                  }

                  return ret;
               }
            }
         } else {
            // wait for the old block to be fixed
            uint64_t dest_chunk_idx = index / 8;
            get_index_offset(table->metadata.log_init_size - 3,
                             dest_chunk_idx,
                             &mindex,
                             &moffset);
            while (__atomic_load_n(
                      &table->metadata.lv2_resize_marker[mindex][moffset],
                      __ATOMIC_SEQ_CST)
                   == 0)
               ;
         }
      }
#endif

      __mmask32 md_mask =
         slot_mask_32(metadata->lv2_md[bindex][boffset].block_md, fprint)
         & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
      int popct = __builtin_popcount(md_mask);

      bool ret = false;

      for (int i = 0; i < popct; ++i) {
         uint8_t slot = word_select(md_mask, i);

         if (iceberg_key_compare(
                table->spl_data_config, blocks[boffset].slots[slot].key, key)
             == 0)
         {

            // printf("tid %d %p %s %s Found refcount: %d\n", thread_id, (void
            // *)table, __func__, key,
            // blocks[boffset].slots[slot].val.refcount);

            if (value) {
               *value = blocks[boffset].slots[slot].val;
            }

            if (force_remove
                || (delete_item && blocks[boffset].slots[slot].refcount == 1)) {
               // If it has a sketch, insert the removed value to it.
               if (table->sktch) {
                  sketch_insert(table->sktch,
                                blocks[boffset].slots[slot].key,
                                blocks[boffset].slots[slot].val);
               }
               if (table->config.post_remove) {
                  table->config.post_remove(&blocks[boffset].slots[slot].val);
               }
               metadata->lv2_md[bindex][boffset].block_md[slot] = 0;
               void *ptr = (void *)slice_data(blocks[boffset].slots[slot].key);
               platform_free(0, ptr);
               blocks[boffset].slots[slot].key      = NULL_SLICE;
               blocks[boffset].slots[slot].refcount = 0;
               pc_add(&metadata->lv2_balls, -1, thread_id);
               ret = true;
            } else if (blocks[boffset].slots[slot].refcount == 0) {
               return false;
            } else {
               blocks[boffset].slots[slot].refcount--;
            }

            return ret;
         }
      }
   }

   return iceberg_lv3_remove(
      table, key, lv3_index, delete_item, force_remove, thread_id);
}

static inline bool
iceberg_get_and_remove_with_force(iceberg_table *table,
                                  slice          key,
                                  ValueType     *value,
                                  bool           delete_item,
                                  bool           force_remove,
                                  threadid       thread_id)
{
   iceberg_metadata *metadata = &table->metadata;
   uint8_t           fprint;
   uint64_t          index;

   split_hash(lv1_hash(key), &fprint, &index, metadata);

   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
   iceberg_lv1_block *blocks = table->level1[bindex];
   // printf("tid %d %p %s %s started\n", thread_id, (void *)table, __func__,
   // key);

#ifdef ENABLE_RESIZE
   // check if there's an active resize and block isn't fixed yet
   if (unlikely(is_lv1_resize_active(table)
                && index >= (table->metadata.nblocks >> 1)))
   {
      uint64_t mask      = ~(1ULL << (table->metadata.block_bits - 1));
      uint64_t old_index = index & mask;
      uint64_t chunk_idx = old_index / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      if (__atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset],
                          __ATOMIC_SEQ_CST)
          == 0)
      { // not fixed yet
         uint64_t old_bindex, old_boffset;
         get_index_offset(table->metadata.log_init_size,
                          old_index,
                          &old_bindex,
                          &old_boffset);

         // lock_block(
         //    (uint64_t *)&metadata->lv1_md[old_bindex][old_boffset].block_md);

         __mmask64 md_mask = slot_mask_64(
            metadata->lv1_md[old_bindex][old_boffset].block_md, fprint);
         uint8_t popct = __builtin_popcountll(md_mask);

         iceberg_lv1_block *blocks = table->level1[old_bindex];

         bool ret = false;
         for (int i = 0; i < popct; ++i) {
            uint8_t slot = word_select(md_mask, i);

            if (iceberg_key_compare(table->spl_data_config,
                                    blocks[old_boffset].slots[slot].key,
                                    key)
                == 0)
            {
               if (value) {
                  *value = blocks[old_boffset].slots[slot].val;
               }

               // printf("tid %d %p %s %s Found in old index refcount: %d\n",
               // thread_id, (void *)table, __func__, key,
               // blocks[old_boffset].slots[slot].val.refcount);


               if (force_remove
                   || (delete_item
                       && blocks[old_boffset].slots[slot].refcount == 1))
               {
                  // If it has a sketch, insert the removed value to it.
                  if (table->sktch) {
                     sketch_insert(table->sktch,
                                   blocks[old_boffset].slots[slot].key,
                                   blocks[old_boffset].slots[slot].val);
                  }
                  if (table->config.post_remove) {
                     table->config.post_remove(
                        &blocks[old_boffset].slots[slot].val);
                  }
                  metadata->lv1_md[old_bindex][old_boffset].block_md[slot] = 0;
                  void *ptr = slice_data(blocks[old_boffset].slots[slot].key);
                  platform_free(0, ptr);
                  blocks[old_boffset].slots[slot].key      = NULL_SLICE;
                  blocks[old_boffset].slots[slot].refcount = 0;
                  pc_add(&metadata->lv1_balls, -1, thread_id);
                  ret = true;
               } else if (blocks[old_boffset].slots[slot].refcount == 0) {
                  // unlock_block(
                  //    (uint64_t *)&metadata->lv1_md[old_bindex][old_boffset]
                  //       .block_md);
                  return false;
               } else {
                  blocks[old_boffset].slots[slot].refcount--;
               }
               // unlock_block(
               //    (uint64_t *)&metadata->lv1_md[old_bindex][old_boffset]
               //       .block_md);
               return ret;
            }
         }
      } else {
         // wait for the old block to be fixed
         uint64_t dest_chunk_idx = index / 8;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         while (
            __atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset],
                            __ATOMIC_SEQ_CST)
            == 0)
            ;
      }
   }
#endif

   lock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
   // printf("tid %d %p %s %s lock acquired\n", thread_id, (void *)table,
   // __func__, key);

   __mmask64 md_mask =
      slot_mask_64(metadata->lv1_md[bindex][boffset].block_md, fprint);
   uint8_t popct = __builtin_popcountll(md_mask);

   bool ret = false;

   for (int i = 0; i < popct; ++i) {
      uint8_t slot = word_select(md_mask, i);

      if (iceberg_key_compare(
             table->spl_data_config, blocks[boffset].slots[slot].key, key)
          == 0)
      {

         // printf("tid %lu %p %s %s boffset=%lu slot=%d\n", thread_id, (void
         // *)table,
         // __func__, (char *)slice_data(key), boffset, slot);

         // printf("tid %d %p %s %s before decreasing %lu\n", thread_id, (void
         // *)table, __func__, key, blocks[boffset].slots[slot].refcount);

         // printf("tid %lu %p %s %s refcount: %lu (val1: %lu, val2: %lu)\n",
         // thread_id, (void *)table,
         //    __func__, (char *)slice_data(blocks[boffset].slots[slot].key),
         //    blocks[boffset].slots[slot].refcount - 1,
         //    *((uint64 *)&blocks[boffset].slots[slot].val), *((uint64
         //    *)&blocks[boffset].slots[slot].val + 8));

         if (value) {
            *value = blocks[boffset].slots[slot].val;
         }

         if (force_remove
             || (delete_item && blocks[boffset].slots[slot].refcount == 1)) {
            // If it has a sketch, insert the removed value to it.
            if (table->sktch) {
               sketch_insert(table->sktch,
                             blocks[boffset].slots[slot].key,
                             blocks[boffset].slots[slot].val);
            }
            if (table->config.post_remove) {
               table->config.post_remove(&blocks[boffset].slots[slot].val);
            }
            metadata->lv1_md[bindex][boffset].block_md[slot] = 0;
            void *ptr = (void *)slice_data(blocks[boffset].slots[slot].key);
            platform_free(0, ptr);
            blocks[boffset].slots[slot].key      = NULL_SLICE;
            blocks[boffset].slots[slot].refcount = 0;
            pc_add(&metadata->lv1_balls, -1, thread_id);
            ret = true;
         } else if (blocks[boffset].slots[slot].refcount == 0) {
            unlock_block(
               (uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
            return false;
         } else {
            blocks[boffset].slots[slot].refcount--;
         }
         // printf("tid %lu %p %s %s %s %lu\n", thread_id, (void *)table,
         // __func__, (char *)slice_data(key), ret ? "DELETED" : "REFCOUNT
         // DECREASED", blocks[boffset].slots[slot].refcount);

         unlock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
         return ret;
      }
   }

   ret = iceberg_lv2_remove(
      table, key, value, index, delete_item, force_remove, thread_id);

   unlock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
   return ret;
}

__attribute__((always_inline)) bool
iceberg_remove(iceberg_table *table, slice key, threadid thread_id)
{
   // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);

   return iceberg_get_and_remove_with_force(
      table, key, NULL, true, false, thread_id);
}

__attribute__((always_inline)) bool
iceberg_get_and_remove(iceberg_table *table,
                       slice          key,
                       ValueType     *value,
                       threadid       thread_id)
{
   // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);

   return iceberg_get_and_remove_with_force(
      table, key, value, true, false, thread_id);
}

__attribute__((always_inline)) bool
iceberg_force_remove(iceberg_table *table, slice key, threadid thread_id)
{
   // printf("tid %d %p %s %s\n", thread_id, (void *)table, __func__, key);

   return iceberg_get_and_remove_with_force(
      table, key, NULL, true, true, thread_id);
}

__attribute__((always_inline)) bool
iceberg_decrease_refcount(iceberg_table *table, slice key, threadid thread_id)
{
   return iceberg_get_and_remove_with_force(
      table, key, NULL, false, false, thread_id);
}


static bool
iceberg_lv3_get_value_internal(iceberg_table *table,
                               slice          key,
                               kv_pair      **kv,
                               uint64_t       lv3_index)
{
   // printf("tid x %p %s %s\n", (void *)table, __func__, key);


   uint64_t bindex, boffset;
   get_index_offset(
      table->metadata.log_init_size, lv3_index, &bindex, &boffset);

   iceberg_metadata *metadata = &table->metadata;
   iceberg_lv3_list *lists    = table->level3[bindex];

   if (likely(!metadata->lv3_sizes[bindex][boffset]))
      return false;

   while (__sync_lock_test_and_set(metadata->lv3_locks[bindex] + boffset, 1))
      ;

   iceberg_lv3_node *current_node = lists[boffset].head;

   for (uint64_t i = 0; i < metadata->lv3_sizes[bindex][boffset]; ++i) {
      if (iceberg_key_compare(table->spl_data_config, current_node->kv.key, key)
          == 0) {
         *kv                                  = &current_node->kv;
         metadata->lv3_locks[bindex][boffset] = 0;
         return true;
      }
      current_node = current_node->next_node;
   }

   metadata->lv3_locks[bindex][boffset] = 0;

   return false;
}

static inline bool
iceberg_lv3_get_value(iceberg_table *table,
                      slice          key,
                      kv_pair      **kv,
                      uint64_t       lv3_index)
{
   // printf("tid x %p %s %s\n", (void *)table, __func__, key);


#ifdef ENABLE_RESIZE
   // check if there's an active resize and block isn't fixed yet
   if (unlikely(is_lv3_resize_active(table)
                && lv3_index >= (table->metadata.nblocks >> 1)))
   {
      uint64_t mask      = ~(1ULL << (table->metadata.block_bits - 1));
      uint64_t old_index = lv3_index & mask;
      uint64_t chunk_idx = old_index / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      if (__atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset],
                          __ATOMIC_SEQ_CST)
          == 0)
      { // not fixed yet
         return iceberg_lv3_get_value_internal(table, key, kv, old_index);
      } else {
         // wait for the old block to be fixed
         uint64_t dest_chunk_idx = lv3_index / 8;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         while (
            __atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset],
                            __ATOMIC_SEQ_CST)
            == 0)
            ;
      }
   }
#endif

   return iceberg_lv3_get_value_internal(table, key, kv, lv3_index);
}


static bool
iceberg_lv2_get_value(iceberg_table *table,
                      slice          key,
                      kv_pair      **kv,
                      uint64_t       lv3_index)
{
   // printf("tid x %p %s %s\n", (void *)table, __func__, key);


   iceberg_metadata *metadata = &table->metadata;

   for (uint8_t i = 0; i < D_CHOICES; ++i) {
      uint8_t  fprint;
      uint64_t index;

      split_hash(lv2_hash(key, i), &fprint, &index, metadata);

      uint64_t bindex, boffset;
      get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
      iceberg_lv2_block *blocks = table->level2[bindex];

#ifdef ENABLE_RESIZE
      // check if there's an active resize and block isn't fixed yet
      if (unlikely(is_lv2_resize_active(table)
                   && index >= (table->metadata.nblocks >> 1)))
      {
         uint64_t mask      = ~(1ULL << (table->metadata.block_bits - 1));
         uint64_t old_index = index & mask;
         uint64_t chunk_idx = old_index / 8;
         uint64_t mindex, moffset;
         get_index_offset(
            table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
         if (__atomic_load_n(
                &table->metadata.lv2_resize_marker[mindex][moffset],
                __ATOMIC_SEQ_CST)
             == 0)
         { // not fixed yet
            uint64_t old_bindex, old_boffset;
            get_index_offset(table->metadata.log_init_size,
                             old_index,
                             &old_bindex,
                             &old_boffset);
            __mmask32 md_mask =
               slot_mask_32(metadata->lv2_md[old_bindex][old_boffset].block_md,
                            fprint)
               & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);

            iceberg_lv2_block *blocks = table->level2[old_bindex];
            while (md_mask != 0) {
               int slot = __builtin_ctz(md_mask);
               md_mask  = md_mask & ~(1U << slot);

               if (iceberg_key_compare(table->spl_data_config,
                                       blocks[old_boffset].slots[slot].key,
                                       key)
                   == 0)
               {
                  *kv = &blocks[old_boffset].slots[slot];
                  return true;
               }
            }
         } else {
            // wait for the old block to be fixed
            uint64_t dest_chunk_idx = index / 8;
            get_index_offset(table->metadata.log_init_size - 3,
                             dest_chunk_idx,
                             &mindex,
                             &moffset);
            while (__atomic_load_n(
                      &table->metadata.lv2_resize_marker[mindex][moffset],
                      __ATOMIC_SEQ_CST)
                   == 0)
               ;
         }
      }
#endif

      __mmask32 md_mask =
         slot_mask_32(metadata->lv2_md[bindex][boffset].block_md, fprint)
         & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);

      while (md_mask != 0) {
         int slot = __builtin_ctz(md_mask);
         md_mask  = md_mask & ~(1U << slot);

         if (iceberg_key_compare(
                table->spl_data_config, blocks[boffset].slots[slot].key, key)
             == 0)
         {
            *kv = &blocks[boffset].slots[slot];
            return true;
         }
      }
   }

   return iceberg_lv3_get_value(table, key, kv, lv3_index);
}

static bool
iceberg_put_nolock(iceberg_table *table,
                   slice          key,
                   ValueType      value,
                   threadid       thread_id)
{
#ifdef ENABLE_RESIZE
   if (unlikely(need_resize(table))) {
      iceberg_setup_resize(table);
   }
#endif

   iceberg_metadata *metadata = &table->metadata;
   uint8_t           fprint;
   uint64_t          index;

   split_hash(lv1_hash(key), &fprint, &index, metadata);

#ifdef ENABLE_RESIZE
   // move blocks if resize is active and not already moved.
   if (unlikely(index < (table->metadata.nblocks >> 1)
                && is_lv1_resize_active(table)))
   {
      uint64_t chunk_idx = index / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      // if fixing is needed set the marker
      if (!__sync_lock_test_and_set(
             &table->metadata.lv1_resize_marker[mindex][moffset], 1))
      {
         for (uint8_t i = 0; i < 8; ++i) {
            uint64_t idx = chunk_idx * 8 + i;
            /*printf("LV1 Before: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 1));*/
            iceberg_lv1_move_block(table, idx, thread_id);
            /*printf("LV1 After: Moving block: %ld load: %f\n", idx,
             * iceberg_block_load(table, idx, 1));*/
         }
         // set the marker for the dest block
         uint64_t dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
         uint64_t mindex, moffset;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         __sync_lock_test_and_set(
            &table->metadata.lv1_resize_marker[mindex][moffset], 1);
      }
   }
#endif
   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);

   const uint64_t refcount = 1;

   bool ret = iceberg_insert_internal(
      table, key, value, refcount, fprint, bindex, boffset, thread_id);
   if (!ret)
      ret = iceberg_lv2_insert(table, key, value, refcount, index, thread_id);

   return ret;
}

static bool
iceberg_get_value_internal(iceberg_table                   *table,
                           slice                            key,
                           kv_pair                        **kv,
                           __attribute__((unused)) threadid thread_id,
                           bool                             should_lock,
                           bool should_lookup_sketch)
{
   iceberg_metadata *metadata = &table->metadata;

   uint8_t  fprint;
   uint64_t index;

   split_hash(lv1_hash(key), &fprint, &index, metadata);

#ifdef ENABLE_RESIZE
   // check if there's an active resize and block isn't fixed yet
   if (unlikely(is_lv1_resize_active(table)
                && index >= (table->metadata.nblocks >> 1)))
   {
      uint64_t mask      = ~(1ULL << (table->metadata.block_bits - 1));
      uint64_t old_index = index & mask;
      uint64_t chunk_idx = old_index / 8;
      uint64_t mindex, moffset;
      get_index_offset(
         table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      if (__atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset],
                          __ATOMIC_SEQ_CST)
          == 0)
      { // not fixed yet
         uint64_t old_bindex, old_boffset;
         get_index_offset(table->metadata.log_init_size,
                          old_index,
                          &old_bindex,
                          &old_boffset);

         // if (should_lock) {
         //    lock_block(
         //       (uint64_t
         //       *)&metadata->lv1_md[old_bindex][old_boffset].block_md);
         // }

         __mmask64 md_mask = slot_mask_64(
            metadata->lv1_md[old_bindex][old_boffset].block_md, fprint);

         iceberg_lv1_block *blocks = table->level1[old_bindex];
         while (md_mask != 0) {
            int slot = __builtin_ctzll(md_mask);
            md_mask  = md_mask & ~(1ULL << slot);

            if (iceberg_key_compare(table->spl_data_config,
                                    blocks[old_boffset].slots[slot].key,
                                    key)
                == 0)
            {
               *kv = &blocks[old_boffset].slots[slot];
               // printf("tid %d %p %s %s refcount: %d Found in old lv1\n",
               // thread_id, (void *)table, __func__, key,
               // blocks[old_boffset].slots[slot].val.refcount);

               // if (should_lock) {
               //    unlock_block(
               //       (uint64_t *)&metadata->lv1_md[old_bindex][old_boffset]
               //          .block_md);
               // }
               return true;
            }
         }

         // if (should_lock) {
         //    unlock_block(
         //       (uint64_t
         //       *)&metadata->lv1_md[old_bindex][old_boffset].block_md);
         // }
      } else {
         // wait for the old block to be fixed
         uint64_t dest_chunk_idx = index / 8;
         get_index_offset(table->metadata.log_init_size - 3,
                          dest_chunk_idx,
                          &mindex,
                          &moffset);
         while (
            __atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset],
                            __ATOMIC_SEQ_CST)
            == 0)
            ;
      }
   }
#endif

   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
   iceberg_lv1_block *blocks = table->level1[bindex];

   if (should_lock) {
      lock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
   }
   __mmask64 md_mask =
      slot_mask_64(metadata->lv1_md[bindex][boffset].block_md, fprint);

   while (md_mask != 0) {
      int slot = __builtin_ctzll(md_mask);
      md_mask  = md_mask & ~(1ULL << slot);

      if (iceberg_key_compare(
             table->spl_data_config, blocks[boffset].slots[slot].key, key)
          == 0)
      {
         // printf("tid %lu %p %s %s boffset=%lu slot=%d\n", thread_id, (void
         // *)table,
         // __func__, (char *)slice_data(key), boffset, slot);

         // printf("tid %lu %p %s %s refcount: %lu Found in lv1\n", thread_id,
         // (void *)table, __func__, (char *)slice_data(key),
         // blocks[boffset].slots[slot].refcount);

         *kv = &blocks[boffset].slots[slot];

         // printf("tid %d %p %s %s refcount: %d Found in lv1\n", thread_id,
         // (void *)table, __func__, key,
         // blocks[boffset].slots[slot].val.refcount);

         if (should_lock) {
            unlock_block(
               (uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
         }
         return true;
      }
   }

   bool ret = iceberg_lv2_get_value(table, key, kv, index);

   // printf("tid %lu %p %s %s %s\n", thread_id, (void *)table, __func__, (char
   // *)slice_data(key) , ret ? "Found in lv2" : "Not found");

   // If there is a sketch and the key is not found, insert the key
   // and the value which is from the sketch into the table. Then,
   // it should return true.
   if (should_lookup_sketch && table->sktch) {
      if (!ret) {
         ValueType value_from_sketch = sketch_get(table->sktch, key);
         ValueType transformed_item;
         table->config.transform_sketch_value(&transformed_item,
                                              value_from_sketch);
         iceberg_put_nolock(table, key, transformed_item, thread_id);
         ret =
            iceberg_get_value_internal(table, key, kv, thread_id, false, false);
      }
   }

   if (should_lock) {
      unlock_block((uint64_t *)&metadata->lv1_md[bindex][boffset].block_md);
   }
   return ret;
}

__attribute__((always_inline)) bool
iceberg_get_value(iceberg_table *table,
                  slice          key,
                  ValueType    **value,
                  threadid       thread_id)
{
   // printf("tid %lu %p %s %s\n", thread_id, (void *)table, __func__, (char
   // *)slice_data(key));
   kv_pair *kv = NULL;
   bool     found =
      iceberg_get_value_internal(table, key, &kv, thread_id, true, true);
   *value = &kv->val;
   return found;
}

#ifdef ENABLE_RESIZE
static bool
iceberg_nuke_key(iceberg_table *table,
                 uint64_t       level,
                 uint64_t       index,
                 uint64_t       slot,
                 threadid       thread_id)
{
   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
   iceberg_metadata *metadata = &table->metadata;

   if (level == 1) {
      iceberg_lv1_block *blocks                        = table->level1[bindex];
      metadata->lv1_md[bindex][boffset].block_md[slot] = 0;
      blocks[boffset].slots[slot].key                  = NULL_SLICE;
      blocks[boffset].slots[slot].refcount             = 0;
      pc_add(&metadata->lv1_balls, -1, thread_id);
   } else if (level == 2) {
      iceberg_lv2_block *blocks                        = table->level2[bindex];
      metadata->lv2_md[bindex][boffset].block_md[slot] = 0;
      blocks[boffset].slots[slot].key                  = NULL_SLICE;
      blocks[boffset].slots[slot].refcount             = 0;
      pc_add(&metadata->lv2_balls, -1, thread_id);
   }

   return true;
}

static bool
iceberg_lv1_move_block(iceberg_table *table, uint64_t bnum, threadid thread_id)
{
   // printf("tid %d %p %s %lu\n", thread_id, (void *)table, __func__, bnum);

   // grab a block
   uint64_t bctr =
      __atomic_fetch_add(&table->metadata.lv1_resize_ctr, 1, __ATOMIC_SEQ_CST);
   if (bctr >= (table->metadata.nblocks >> 1))
      return true;

   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, bnum, &bindex, &boffset);
   // relocate items in level1
   for (uint64_t j = 0; j < (1 << SLOT_BITS); ++j) {
      slice key = table->level1[bindex][boffset].slots[j].key;
      if (slice_is_null(key)) {
         continue;
      }
      ValueType value    = table->level1[bindex][boffset].slots[j].val;
      uint64_t  refcount = table->level1[bindex][boffset].slots[j].refcount;
      uint8_t   fprint;
      uint64_t  index;

      split_hash(lv1_hash(key), &fprint, &index, &table->metadata);

      // move to new location
      if (index != bnum) {
         uint64_t local_bindex, local_boffset;
         get_index_offset(table->metadata.log_init_size,
                          index,
                          &local_bindex,
                          &local_boffset);
         if (!iceberg_insert_internal(table,
                                      key,
                                      value,
                                      refcount,
                                      fprint,
                                      local_bindex,
                                      local_boffset,
                                      thread_id))
         {
            printf("Failed insert during resize lv1\n");
            exit(0);
         }
         if (!iceberg_nuke_key(table, 1, bnum, j, thread_id)) {
            printf("Failed remove during resize lv1. key: %s, block: %ld\n",
                   key,
                   bnum);
            exit(0);
         }
         // ValueType *val;
         // if (!iceberg_get_value(table, key, &val, thread_id)) {
         //  printf("Key not found during resize lv1: %ld\n", key);
         // exit(0);
         // }
      }
   }

   return false;
}

static bool
iceberg_lv2_move_block(iceberg_table *table, uint64_t bnum, threadid thread_id)
{
   // printf("tid %d %p %s %lu\n", thread_id, (void *)table, __func__, bnum);


   // grab a block
   uint64_t bctr =
      __atomic_fetch_add(&table->metadata.lv2_resize_ctr, 1, __ATOMIC_SEQ_CST);
   if (bctr >= (table->metadata.nblocks >> 1))
      return true;

   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, bnum, &bindex, &boffset);
   uint64_t mask = ~(1ULL << (table->metadata.block_bits - 1));
   // relocate items in level2
   for (uint64_t j = 0; j < C_LV2 + MAX_LG_LG_N / D_CHOICES; ++j) {
      slice key = table->level2[bindex][boffset].slots[j].key;
      if (slice_is_null(key)) {
         continue;
      }
      ValueType value    = table->level2[bindex][boffset].slots[j].val;
      uint64_t  refcount = table->level2[bindex][boffset].slots[j].refcount;
      uint8_t   fprint;
      uint64_t  index;

      split_hash(lv1_hash(key), &fprint, &index, &table->metadata);

      for (int i = 0; i < D_CHOICES; ++i) {
         uint8_t  l2fprint;
         uint64_t l2index;

         split_hash(lv2_hash(key, i), &l2fprint, &l2index, &table->metadata);

         // move to new location
         if ((l2index & mask) == bnum && l2index != bnum) {
            if (!iceberg_lv2_insert_internal(
                   table, key, value, refcount, l2fprint, l2index, thread_id))
            {
               if (!iceberg_lv2_insert(
                      table, key, value, refcount, index, thread_id)) {
                  printf("Failed insert during resize lv2\n");
                  exit(0);
               }
            }
            if (!iceberg_nuke_key(table, 2, bnum, j, thread_id)) {
               printf("Failed remove during resize lv2\n");
               exit(0);
            }
            break;
            // ValueType *val;
            // if (!iceberg_get_value(table, key, &val, thread_id)) {
            // printf("Key not found during resize lv2: %ld\n", key);
            // exit(0);
            // }
         }
      }
   }

   return false;
}

static bool
iceberg_lv3_move_block(iceberg_table *table, uint64_t bnum, threadid thread_id)
{
   // printf("tid %d %p %s %lu\n", thread_id, (void *)table, __func__, bnum);


   // grab a block
   uint64_t bctr =
      __atomic_fetch_add(&table->metadata.lv3_resize_ctr, 1, __ATOMIC_SEQ_CST);
   if (bctr >= (table->metadata.nblocks >> 1))
      return true;

   uint64_t bindex, boffset;
   get_index_offset(table->metadata.log_init_size, bnum, &bindex, &boffset);
   // relocate items in level3
   if (unlikely(table->metadata.lv3_sizes[bindex][boffset])) {
      iceberg_lv3_node *current_node = table->level3[bindex][boffset].head;
      while (current_node != NULL) {
         kv_pair current_node_kv = current_node->kv;

         uint8_t  fprint;
         uint64_t index;

         split_hash(
            lv1_hash(current_node_kv.key), &fprint, &index, &table->metadata);
         // move to new location
         if (index != bnum) {
            if (!iceberg_lv3_insert(table,
                                    current_node_kv.key,
                                    current_node_kv.val,
                                    current_node_kv.refcount,
                                    index,
                                    thread_id))
            {
               printf("Failed insert during resize lv3\n");
               exit(0);
            }
            if (!iceberg_lv3_remove(
                   table, current_node_kv.key, bnum, true, true, thread_id))
            {
               printf("Failed remove during resize lv3: %s\n",
                      current_node_kv.key);
               exit(0);
            }
            // ValueType *val;
            // if (!iceberg_get_value(table, key, &val, thread_id)) {
            // printf("Key not found during resize lv3: %ld\n", key);
            // exit(0);
            //}
         }
         current_node = current_node->next_node;
      }
   }

   return false;
}
#endif

void
iceberg_print_state(iceberg_table *table)
{
   printf("Current stats: \n");

   printf("Load factor: %f\n", iceberg_load_factor(table));
#ifdef ENABLE_RESIZE
   printf("Load factor_aprox: %f\n", iceberg_load_factor_aprox(table));
#endif
   printf("Number level 1 inserts: %ld\n", lv1_balls(table));
   printf("Number level 2 inserts: %ld\n", lv2_balls(table));
   printf("Number level 3 inserts: %ld\n", lv3_balls(table));
   printf("Total inserts: %ld\n", tot_balls(table));
}
