#include "iceberg_table.h"

const static uint64 one[128] = {
  1ULL << 0, 1ULL << 1, 1ULL << 2, 1ULL << 3, 1ULL << 4, 1ULL << 5, 1ULL << 6, 1ULL << 7, 1ULL << 8, 1ULL << 9,
  1ULL << 10, 1ULL << 11, 1ULL << 12, 1ULL << 13, 1ULL << 14, 1ULL << 15, 1ULL << 16, 1ULL << 17, 1ULL << 18, 1ULL << 19,
  1ULL << 20, 1ULL << 21, 1ULL << 22, 1ULL << 23, 1ULL << 24, 1ULL << 25, 1ULL << 26, 1ULL << 27, 1ULL << 28, 1ULL << 29,
  1ULL << 30, 1ULL << 31, 1ULL << 32, 1ULL << 33, 1ULL << 34, 1ULL << 35, 1ULL << 36, 1ULL << 37, 1ULL << 38, 1ULL << 39,
  1ULL << 40, 1ULL << 41, 1ULL << 42, 1ULL << 43, 1ULL << 44, 1ULL << 45, 1ULL << 46, 1ULL << 47, 1ULL << 48, 1ULL << 49,
  1ULL << 50, 1ULL << 51, 1ULL << 52, 1ULL << 53, 1ULL << 54, 1ULL << 55, 1ULL << 56, 1ULL << 57, 1ULL << 58, 1ULL << 59,
  1ULL << 60, 1ULL << 61, 1ULL << 62, 1ULL << 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

const static uint64 pdep_table[128] = {
  ~(1ULL << 0), ~(1ULL << 1), ~(1ULL << 2), ~(1ULL << 3), ~(1ULL << 4), ~(1ULL << 5), ~(1ULL << 6), ~(1ULL << 7),
  ~(1ULL << 8), ~(1ULL << 9), ~(1ULL << 10), ~(1ULL << 11), ~(1ULL << 12), ~(1ULL << 13), ~(1ULL << 14), ~(1ULL << 15),
  ~(1ULL << 16), ~(1ULL << 17), ~(1ULL << 18), ~(1ULL << 19), ~(1ULL << 20), ~(1ULL << 21), ~(1ULL << 22), ~(1ULL << 23),
  ~(1ULL << 24), ~(1ULL << 25), ~(1ULL << 26), ~(1ULL << 27), ~(1ULL << 28), ~(1ULL << 29), ~(1ULL << 30), ~(1ULL << 31),
  ~(1ULL << 32), ~(1ULL << 33), ~(1ULL << 34), ~(1ULL << 35), ~(1ULL << 36), ~(1ULL << 37), ~(1ULL << 38), ~(1ULL << 39),
  ~(1ULL << 40), ~(1ULL << 41), ~(1ULL << 42), ~(1ULL << 43), ~(1ULL << 44), ~(1ULL << 45), ~(1ULL << 46), ~(1ULL << 47),
  ~(1ULL << 48), ~(1ULL << 49), ~(1ULL << 50), ~(1ULL << 51), ~(1ULL << 52), ~(1ULL << 53), ~(1ULL << 54), ~(1ULL << 55),
  ~(1ULL << 56), ~(1ULL << 57), ~(1ULL << 58), ~(1ULL << 59), ~(1ULL << 60), ~(1ULL << 61), ~(1ULL << 62), ~(1ULL << 63),
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL,
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL,
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL,
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL,
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL,
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL,
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL,
  ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL, ~0ULL
};

const static uint8 broadcast_mask[64] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

#define FPRINT_BITS 8
#define RESIZE_THRESHOLD 0.96
/*#define RESIZE_THRESHOLD 0.85 // For YCSB*/

uint64 seed[5] = { 12351327692179052ll, 23246347347385899ll, 35236262354132235ll, 13604702930934770ll, 57439820692984798ll };

static inline uint64 nonzero_fprint(uint64 hash) {
  return hash & ((1 << FPRINT_BITS) - 2) ? hash : hash | 2;
}

static inline uint64 lv1_hash(KeyType key) {
  return nonzero_fprint(platform_hash64(&key, sizeof key, seed[0]));
}

static inline uint64 lv2_hash(KeyType key, uint8 i) {
  return nonzero_fprint(platform_hash64(&key, sizeof key, seed[i + 1]));
}

static inline uint8 word_select(uint64 val, int rank) {
  val = _pdep_u64(one[rank], val);
  return _tzcnt_u64(val);
}

static uint64 lv1_balls(iceberg_table * table) {
  return table->metadata.lv1_ctr;
}

static inline uint64 lv1_balls_aprox(iceberg_table * table) {
  return table->metadata.lv1_ctr;
}

static uint64 lv2_balls(iceberg_table * table) {
  return table->metadata.lv2_ctr;
}

static inline uint64 lv2_balls_aprox(iceberg_table * table) {
  return table->metadata.lv2_ctr;
}

static uint64 lv3_balls(iceberg_table * table) {
  return table->metadata.lv3_ctr;
}

static inline uint64 lv3_balls_aprox(iceberg_table * table) {
  return table->metadata.lv3_ctr;
}

static uint64 tot_balls(iceberg_table * table) {
  return lv1_balls(table) + lv2_balls(table) + lv3_balls(table);
}

static uint64 tot_balls_aprox(iceberg_table * table) {
  return lv1_balls_aprox(table) + lv2_balls_aprox(table) + lv3_balls_aprox(table);
}

static inline uint64 total_capacity(iceberg_table * table) {
  return lv3_balls(table) + table->metadata.nblocks * ((1 << SLOT_BITS) + C_LV2 + MAX_LG_LG_N / D_CHOICES);
}

static inline uint64 total_capacity_aprox(iceberg_table * table) {
  return lv3_balls_aprox(table) + table->metadata.nblocks * ((1 << SLOT_BITS) + C_LV2 + MAX_LG_LG_N / D_CHOICES);
}

static inline double iceberg_load_factor(iceberg_table * table) {
  return (double)tot_balls(table) / (double)total_capacity(table);
}

static inline double iceberg_load_factor_aprox(iceberg_table * table) {
  return (double)tot_balls_aprox(table) / (double)total_capacity_aprox(table);
}

#ifdef ENABLE_RESIZE
static bool need_resize(iceberg_table * table) {
  double lf = iceberg_load_factor_aprox(table);
  if (lf >= RESIZE_THRESHOLD)
    return TRUE;
  return FALSE;
}
#endif

static inline void get_index_offset(uint64 init_log, uint64 index, uint64 *bindex, uint64 *boffset) {
  uint64 shf = index >> init_log;
  *bindex = 64 - _lzcnt_u64(shf);
  uint64 adj = 1ULL << *bindex;
  adj = adj >> 1;
  adj = adj << init_log;
  *boffset = index - adj;
}

static inline void split_hash(uint64 hash, uint8 *fprint, uint64 *index, iceberg_metadata * metadata) {	
  *fprint = hash & ((1ULL << FPRINT_BITS) - 1);
  *index = (hash >> FPRINT_BITS) & ((1ULL << metadata->block_bits) - 1);
}

#define LOCK_MASK 1ULL
#define UNLOCK_MASK ~1ULL

static inline void lock_block(uint64 * metadata)
{
#ifdef ENABLE_BLOCK_LOCKING
  uint64 *data = metadata + 7;
  while ((__sync_fetch_and_or(data, LOCK_MASK) & 1) != 0) { _mm_pause(); }
#endif
}

static inline void unlock_block(uint64 * metadata)
{
#ifdef ENABLE_BLOCK_LOCKING
  uint64 *data = metadata + 7;
   *data = *data & UNLOCK_MASK;
#endif
}

static inline uint32 slot_mask_32(uint8 * metadata, uint8 fprint) {
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
static inline uint64 slot_mask_64(uint8 * metadata, uint8 fprint) {
  __m512i mask = _mm512_loadu_si512((const __m512i *)(broadcast_mask));
  __m512i bcast = _mm512_set1_epi8(fprint);
  bcast = _mm512_or_epi64(bcast, mask);
  __m512i block = _mm512_loadu_si512((const __m512i *)(metadata));
  block = _mm512_or_epi64(block, mask);
  return _mm512_cmp_epi8_mask(bcast, block, _MM_CMPINT_EQ);
}
#else /* ! (defined __AVX512F__ && defined __AVX512BW__) */
static inline uint32 slot_mask_64_half(__m256i fprint, __m256i md, __m256i mask)
{
  __m256i masked_fp = _mm256_or_si256(fprint, mask);
  __m256i masked_md = _mm256_or_si256(md, mask);
  __m256i cmp       = _mm256_cmpeq_epi8(masked_md, masked_fp);
  return _mm256_movemask_epi8(cmp);
}

static inline uint64 slot_mask_64(uint8 * metadata, uint8 fp) {
  __m256i fprint   = _mm256_set1_epi8(fp);

  __m256i  md1     = _mm256_loadu_si256((const __m256i *)(metadata));
  __m256i  mask1   = _mm256_loadu_si256((const __m256i *)(broadcast_mask));
  uint64 result1 = slot_mask_64_half(fprint, md1, mask1);

  __m256i  md2     = _mm256_loadu_si256((const __m256i *)(&metadata[32]));
  __m256i  mask2   = _mm256_loadu_si256((const __m256i *)(&broadcast_mask[32]));
  uint64 result2 = slot_mask_64_half(fprint, md2, mask2);

  return ((uint64)result2 << 32) | result1;
}
#endif /* ! (defined __AVX512F__ && defined __AVX512BW__) */


static inline void atomic_write_128(uint64 key, uint64 val, uint64 *slot) {
  uint64 arr[2] = {key, val};
  __m128d a =  _mm_load_pd((double *)arr);
  _mm_store_pd ((double*)slot, a);
}

static uint64 iceberg_block_load(iceberg_table * table, uint64 index, uint8 level) {
  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
  if (level == 1) {
    __mmask64 mask64 = slot_mask_64(table->metadata.lv1_md[bindex][boffset].block_md, 0);
    return (1ULL << SLOT_BITS) - __builtin_popcountll(mask64);
  } else if (level == 2) {
    __mmask32 mask32 = slot_mask_32(table->metadata.lv2_md[bindex][boffset].block_md, 0) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
    return (C_LV2 + MAX_LG_LG_N / D_CHOICES) - __builtin_popcountll(mask32);
  } else
    return table->metadata.lv3_sizes[bindex][boffset];
}

static uint64 iceberg_table_load(iceberg_table * table) {
  uint64 total = 0;

  for (uint8 i = 1; i <= 3; ++i) {
    for (uint64 j = 0; j < table->metadata.nblocks; ++j) {
      total += iceberg_block_load(table, j, i); 
    }
  }

  return total;
}

static double iceberg_block_load_factor(iceberg_table * table, uint64 index, uint8 level) {
  if (level == 1)
    return iceberg_block_load(table, index, level) / (double)(1ULL << SLOT_BITS);
  else if (level == 2)
    return iceberg_block_load(table, index, level) / (double)(C_LV2 + MAX_LG_LG_N / D_CHOICES);
  else
    return iceberg_block_load(table, index, level);
}

static inline size_t round_up(size_t n, size_t k) {
  size_t rem = n % k;
  if (rem == 0) {
    return n;
  }
  n += k - rem;
  return n;
}

int iceberg_init(iceberg_table *table, uint64 log_slots) {
  memset(table, 0, sizeof(*table));

  uint64 total_blocks = 1 << (log_slots - SLOT_BITS);
  uint64 total_size_in_bytes = (sizeof(iceberg_lv1_block) + sizeof(iceberg_lv2_block) + sizeof(iceberg_lv1_block_md) + sizeof(iceberg_lv2_block_md)) * total_blocks;

  assert(table);

#if defined(HUGE_TLB)
  int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB;
#else
  int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
#endif

  table->level1[0] = (iceberg_lv1_block *)mmap(NULL, level1_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->level1[0]) {
    perror("level1 malloc failed");
    exit(1);
  }
  size_t level2_size = sizeof(iceberg_lv2_block) * total_blocks;
  //table->level2 = (iceberg_lv2_block *)malloc(level2_size);
  table->level2[0] = (iceberg_lv2_block *)mmap(NULL, level2_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->level2[0]) {
    perror("level2 malloc failed");
    exit(1);
  }
  size_t level3_size = sizeof(iceberg_lv3_list) * total_blocks;
  table->level3[0] = (iceberg_lv3_list *)mmap(NULL, level3_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->level3[0]) {
    perror("level3 malloc failed");
    exit(1);
  }

  table->metadata.total_size_in_bytes = total_size_in_bytes;
  table->metadata.nslots = 1 << log_slots;
  table->metadata.nblocks = total_blocks;
  table->metadata.block_bits = log_slots - SLOT_BITS;
  table->metadata.init_size = total_blocks;
  table->metadata.log_init_size = log2(total_blocks);
  table->metadata.nblocks_parts[0] = total_blocks;

  size_t lv1_md_size = sizeof(iceberg_lv1_block_md) * total_blocks + 64;
  //table->metadata.lv1_md = (iceberg_lv1_block_md *)malloc(sizeof(iceberg_lv1_block_md) * total_blocks);
  table->metadata.lv1_md[0] = (iceberg_lv1_block_md *)mmap(NULL, lv1_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->metadata.lv1_md[0]) {
    perror("lv1_md malloc failed");
    exit(1);
  }
  //table->metadata.lv2_md = (iceberg_lv2_block_md *)malloc(sizeof(iceberg_lv2_block_md) * total_blocks);
  size_t lv2_md_size = sizeof(iceberg_lv2_block_md) * total_blocks + 32;
  table->metadata.lv2_md[0] = (iceberg_lv2_block_md *)mmap(NULL, lv2_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->metadata.lv2_md[0]) {
    perror("lv2_md malloc failed");
    exit(1);
  }
  table->metadata.lv3_sizes[0] = (uint64 *)mmap(NULL, sizeof(uint64) * total_blocks, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->metadata.lv3_sizes[0]) {
    perror("lv3_sizes malloc failed");
    exit(1);
  }
  table->metadata.lv3_locks[0] = (uint8 *)mmap(NULL, sizeof(uint8) * total_blocks, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->metadata.lv3_locks[0]) {
    perror("lv3_locks malloc failed");
    exit(1);
  }

#ifdef ENABLE_RESIZE
  table->metadata.resize_cnt = 0;
  table->metadata.lv1_resize_ctr = total_blocks;
  table->metadata.lv2_resize_ctr = total_blocks;
  table->metadata.lv3_resize_ctr = total_blocks;

  // create one marker for 8 blocks.
  size_t resize_marker_size = sizeof(uint8) * total_blocks / 8;
  table->metadata.lv1_resize_marker[0] = (uint8 *)mmap(NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->metadata.lv1_resize_marker[0]) {
    perror("level1 resize ctr malloc failed");
    exit(1);
  }
  table->metadata.lv2_resize_marker[0] = (uint8 *)mmap(NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->metadata.lv2_resize_marker[0]) {
    perror("level2 resize ctr malloc failed");
    exit(1);
  }
  table->metadata.lv3_resize_marker[0] = (uint8 *)mmap(NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (!table->metadata.lv3_resize_marker[0]) {
    perror("level3 resize ctr malloc failed");
    exit(1);
  }
  memset(table->metadata.lv1_resize_marker[0], 1, resize_marker_size);
  memset(table->metadata.lv2_resize_marker[0], 1, resize_marker_size);
  memset(table->metadata.lv3_resize_marker[0], 1, resize_marker_size);

  table->metadata.marker_sizes[0] = resize_marker_size;
  table->metadata.lock = 0;
#endif

  for (uint64 i = 0; i < total_blocks; ++i) {
    for (uint64 j = 0; j < (1 << SLOT_BITS); ++j) {
      table->level1[0][i].slots[j].key = table->level1[0][i].slots[j].val = 0;
    }

    for (uint64 j = 0; j < C_LV2 + MAX_LG_LG_N / D_CHOICES; ++j) {
      table->level2[0][i].slots[j].key = table->level2[0][i].slots[j].val = 0;
    }
    table->level3[0]->head = NULL;
  }

  memset((char *)table->metadata.lv1_md[0], 0, lv1_md_size);
  memset((char *)table->metadata.lv2_md[0], 0, lv2_md_size);
  memset(table->metadata.lv3_sizes[0], 0, total_blocks * sizeof(uint64));
  memset(table->metadata.lv3_locks[0], 0, total_blocks * sizeof(uint8));

  return 0;
}

#ifdef ENABLE_RESIZE
static inline bool is_lv1_resize_active(iceberg_table * table) {
  uint64 half_mark = table->metadata.nblocks >> 1;
  uint64 lv1_ctr = __atomic_load_n(&table->metadata.lv1_resize_ctr, __ATOMIC_SEQ_CST);
  return lv1_ctr < half_mark;
}

static inline bool is_lv2_resize_active(iceberg_table * table) {
  uint64 half_mark = table->metadata.nblocks >> 1;
  uint64 lv2_ctr = __atomic_load_n(&table->metadata.lv2_resize_ctr, __ATOMIC_SEQ_CST);
  return lv2_ctr < half_mark;
}

static inline bool is_lv3_resize_active(iceberg_table * table) {
  uint64 half_mark = table->metadata.nblocks >> 1;
  uint64 lv3_ctr = __atomic_load_n(&table->metadata.lv3_resize_ctr, __ATOMIC_SEQ_CST);
  return lv3_ctr < half_mark;
}

static bool is_resize_active(iceberg_table * table) {
  return is_lv3_resize_active(table) || is_lv2_resize_active(table) || is_lv1_resize_active(table); 
}

/**
 * Try to acquire a lock once and return even if the lock is busy.
 */
static bool lock(volatile int *var) {
    return !__sync_lock_test_and_set(var, 1);
}

static void unlock(volatile int *var) {
  __sync_lock_release(var);
  return;
}

static bool iceberg_setup_resize(iceberg_table * table) {
  // grab write lock
  if (!lock(&table->metadata.lock))
    return FALSE;

  if (UNLIKELY(!need_resize(table))) {
    unlock(&table->metadata.lock);
    return FALSE;
  }
  if (is_resize_active(table)) {
    // finish the current resize
    iceberg_end(table);
    unlock(&table->metadata.lock);
    return FALSE;
  }

#if defined(HUGE_TLB)
  int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB;
#else
  int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
#endif

  // compute new sizes
  uint64 cur_blocks = table->metadata.nblocks;
  uint64 resize_cnt = table->metadata.resize_cnt + 1;

  // Allocate new table and metadata
  // alloc level1
  size_t level1_size = sizeof(iceberg_lv1_block) * cur_blocks;
  table->level1[resize_cnt] = (iceberg_lv1_block *)mmap(NULL, level1_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->level1[resize_cnt] == (void *)-1) {
    perror("level1 resize failed");
    exit(1);
  }

  // alloc level2
  size_t level2_size = sizeof(iceberg_lv2_block) * cur_blocks;
  table->level2[resize_cnt] = (iceberg_lv2_block *)mmap(NULL, level2_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->level2[resize_cnt] == (void *)-1) {
    perror("level2 resize failed");
    exit(1);
  }

  // alloc level3
  size_t level3_size = sizeof(iceberg_lv3_list) * cur_blocks;
  table->level3[resize_cnt] = (iceberg_lv3_list *)mmap(NULL, level3_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->level3[resize_cnt] == (void *)-1) {
    perror("level3 resize failed");
    exit(1);
  }

  // alloc level1 metadata
  size_t lv1_md_size = sizeof(iceberg_lv1_block_md) * cur_blocks + 64;
  table->metadata.lv1_md[resize_cnt] = (iceberg_lv1_block_md *)mmap(NULL, lv1_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->metadata.lv1_md[resize_cnt] == (void *)-1) {
    perror("lv1_md resize failed");
    exit(1);
  }

  // alloc level2 metadata
  size_t lv2_md_size = sizeof(iceberg_lv2_block_md) * cur_blocks + 32;
  table->metadata.lv2_md[resize_cnt] = (iceberg_lv2_block_md *)mmap(NULL, lv2_md_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->metadata.lv2_md[resize_cnt] == (void *)-1) {
    perror("lv2_md resize failed");
    exit(1);
  }

  // alloc level3 metadata (sizes, locks)
  size_t lv3_sizes_size = sizeof(uint64) * cur_blocks;
  table->metadata.lv3_sizes[resize_cnt] = (uint64 *)mmap(NULL, lv3_sizes_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->metadata.lv3_sizes[resize_cnt] == (void *)-1) {
    perror("lv3_sizes resize failed");
    exit(1);
  }

  size_t lv3_locks_size = sizeof(uint8) * cur_blocks;
  table->metadata.lv3_locks[resize_cnt] = (uint8 *)mmap(NULL, lv3_locks_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->metadata.lv3_locks[resize_cnt] == (void *)-1) {
    perror("lv3_locks remap failed");
    exit(1);
  }
  memset(table->metadata.lv3_locks[resize_cnt], 0, cur_blocks * sizeof(uint8));

  // alloc resize markers
  // resize_marker_size
  size_t resize_marker_size = sizeof(uint8) * cur_blocks / 8;
  table->metadata.lv1_resize_marker[resize_cnt] = (uint8 *)mmap(NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->metadata.lv1_resize_marker[resize_cnt] == (void *)-1) {
    perror("level1 resize failed");
    exit(1);
  }

  table->metadata.lv2_resize_marker[resize_cnt] = (uint8 *)mmap(NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->metadata.lv2_resize_marker[resize_cnt] == (void *)-1) {
    perror("level1 resize failed");
    exit(1);
  }

  table->metadata.lv3_resize_marker[resize_cnt] = (uint8 *)mmap(NULL, resize_marker_size, PROT_READ | PROT_WRITE, mmap_flags, 0, 0);
  if (table->metadata.lv3_resize_marker[resize_cnt] == (void *)-1) {
    perror("level1 resize failed");
    exit(1);
  }

  table->metadata.marker_sizes[resize_cnt] = resize_marker_size;
  // resetting the resize markers.
  for (uint64 i = 0;  i <= resize_cnt; ++i) {
    memset(table->metadata.lv1_resize_marker[i], 0, table->metadata.marker_sizes[i]);
    memset(table->metadata.lv2_resize_marker[i], 0, table->metadata.marker_sizes[i]);
    memset(table->metadata.lv3_resize_marker[i], 0, table->metadata.marker_sizes[i]);
  }

  uint64 total_blocks = table->metadata.nblocks * 2;
  uint64 total_size_in_bytes = (sizeof(iceberg_lv1_block) + sizeof(iceberg_lv2_block) + sizeof(iceberg_lv1_block_md) + sizeof(iceberg_lv2_block_md)) * total_blocks;

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

  unlock(&table->metadata.lock);
  return TRUE;
}

static bool iceberg_lv1_move_block(iceberg_table * table, uint64 bnum, uint8 thread_id);
static bool iceberg_lv2_move_block(iceberg_table * table, uint64 bnum, uint8 thread_id);
static bool iceberg_lv3_move_block(iceberg_table * table, uint64 bnum, uint8 thread_id);

// finish moving blocks that are left during the last resize.
void iceberg_end(iceberg_table * table) {
  if (is_lv1_resize_active(table)) {
    for (uint64 j = 0; j < table->metadata.nblocks / 8; ++j) {
      uint64 chunk_idx = j;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      // if fixing is needed set the marker
      if (!__sync_lock_test_and_set(&table->metadata.lv1_resize_marker[mindex][moffset], 1)) {
        for (uint8 i = 0; i < 8; ++i) {
          uint64 idx = chunk_idx * 8 + i;
          iceberg_lv1_move_block(table, idx, 0);
        }
        // set the marker for the dest block
        uint64 dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
        uint64 mindex, moffset;
        get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
        __sync_lock_test_and_set(&table->metadata.lv1_resize_marker[mindex][moffset], 1);
      }
    }
  }
  if (is_lv2_resize_active(table)) {
    for (uint64 j = 0; j < table->metadata.nblocks / 8; ++j) {
      uint64 chunk_idx = j;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      // if fixing is needed set the marker
      if (!__sync_lock_test_and_set(&table->metadata.lv2_resize_marker[mindex][moffset], 1)) {
        for (uint8 i = 0; i < 8; ++i) {
          uint64 idx = chunk_idx * 8 + i;
          iceberg_lv2_move_block(table, idx, 0);
        }
        // set the marker for the dest block
        uint64 dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
        uint64 mindex, moffset;
        get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
        __sync_lock_test_and_set(&table->metadata.lv2_resize_marker[mindex][moffset], 1);
      }
    }
  }
  if (is_lv3_resize_active(table)) {
    for (uint64 j = 0; j < table->metadata.nblocks / 8; ++j) {
      uint64 chunk_idx = j;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      // if fixing is needed set the marker
      if (!__sync_lock_test_and_set(&table->metadata.lv3_resize_marker[mindex][moffset], 1)) {
        for (uint8 i = 0; i < 8; ++i) {
          uint64 idx = chunk_idx * 8 + i;
          iceberg_lv3_move_block(table, idx, 0);
        }
        // set the marker for the dest block
        uint64 dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
        uint64 mindex, moffset;
        get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
        __sync_lock_test_and_set(&table->metadata.lv3_resize_marker[mindex][moffset], 1);
      }
    }
  }

  /*printf("Final resize done.\n");*/
  /*printf("Final resize done. Table load: %ld\n", iceberg_table_load(table));*/
}
#endif

static inline bool iceberg_lv3_insert(iceberg_table * table, KeyType key, ValueType value, uint64 lv3_index, uint8 thread_id) {

#ifdef ENABLE_RESIZE
  if (UNLIKELY(lv3_index < (table->metadata.nblocks >> 1) && is_lv3_resize_active(table))) {
    uint64 chunk_idx = lv3_index / 8;
    uint64 mindex, moffset;
    get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
    // if fixing is needed set the marker
    if (!__sync_lock_test_and_set(&table->metadata.lv3_resize_marker[mindex][moffset], 1)) {
      for (uint8 i = 0; i < 8; ++i) {
        uint64 idx = chunk_idx * 8 + i;
        /*printf("LV3 Before: Moving block: %ld load: %f\n", idx, iceberg_block_load(table, idx, 3));*/
        iceberg_lv3_move_block(table, idx, thread_id);
        /*printf("LV3 After: Moving block: %ld load: %f\n", idx, iceberg_block_load(table, idx, 3));*/
      }
      // set the marker for the dest block
      uint64 dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
      __sync_lock_test_and_set(&table->metadata.lv3_resize_marker[mindex][moffset], 1);
    }
  }
#endif

  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, lv3_index, &bindex, &boffset);
  iceberg_metadata * metadata = &table->metadata;
  iceberg_lv3_list * lists = table->level3[bindex];

  while(__sync_lock_test_and_set(metadata->lv3_locks[bindex] + boffset, 1));

#if PMEM
  iceberg_lv3_node * level3_nodes = table->level3_nodes;
  iceberg_lv3_node *new_node = NULL;
  ptrdiff_t new_node_idx = -1;
  ptrdiff_t start = lv3_index % NUM_LEVEL3_NODES;
  for (ptrdiff_t i = start; i != NUM_LEVEL3_NODES + start; i++) {
    ptrdiff_t j = i % NUM_LEVEL3_NODES;
    if (__sync_bool_compare_and_swap(&level3_nodes[j].in_use, 0, 1)) {
      new_node = &level3_nodes[j];
      new_node_idx = j;
      break;
    }
  }
  if (new_node == NULL) {
    metadata->lv3_locks[bindex][boffset] = 0;
    return FALSE;
  }
#else
  iceberg_lv3_node * new_node = (iceberg_lv3_node *)malloc(sizeof(iceberg_lv3_node));
#endif

  new_node->key = key;
  new_node->val = value;
#if PMEM
  new_node->next_idx = lists[boffset].head_idx;
  pmem_persist(new_node, sizeof(*new_node));
  lists[boffset].head_idx = new_node_idx;
  pmem_persist(&lists[boffset], sizeof(lists[boffset]));
#else
  new_node->next_node = lists[boffset].head;
  lists[boffset].head = new_node;
#endif

  metadata->lv3_sizes[bindex][boffset]++;
  pc_add(&metadata->lv3_balls, 1, thread_id);
  metadata->lv3_locks[bindex][boffset] = 0;

  return TRUE;
}

static inline bool iceberg_lv2_insert_internal(iceberg_table * table, KeyType key, ValueType value, uint8 fprint, uint64 index, uint8 thread_id) {
  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);

  iceberg_metadata * metadata = &table->metadata;
  iceberg_lv2_block * blocks = table->level2[bindex];

start: ;
  __mmask32 md_mask = slot_mask_32(metadata->lv2_md[bindex][boffset].block_md, 0) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
  uint8 popct = __builtin_popcountll(md_mask);

  if (UNLIKELY(!popct))
    return FALSE;

#if PMEM
  uint8 slot_choice = get_slot_choice(key);
  uint8 start = popct == 0 ? 0 : slot_choice % popct;
#else
  uint8 start = 0;
#endif
  /*for(uint8 i = start; i < start + popct; ++i) {*/
#if PMEM
    uint8 slot = word_select(md_mask, start % popct);
#else
    uint8 slot = word_select(md_mask, start);
#endif

    if(__sync_bool_compare_and_swap(metadata->lv2_md[bindex][boffset].block_md + slot, 0, 1)) {
      pc_add(&metadata->lv2_balls, 1, thread_id);
      /*blocks[boffset].slots[slot].key = key;*/
      /*blocks[boffset].slots[slot].val = value;*/
      atomic_write_128(key, value, (uint64*)&blocks[boffset].slots[slot]);
#if PMEM
      pmem_persist(&blocks[boffset].slots[slot], sizeof(kv_pair));
#endif
      metadata->lv2_md[bindex][boffset].block_md[slot] = fprint;
      return TRUE;
    }
    goto start;
  /*}*/

  return FALSE;
}

static inline bool iceberg_lv2_insert(iceberg_table * table, KeyType key, ValueType value, uint64 lv3_index, uint8 thread_id) {

  iceberg_metadata * metadata = &table->metadata;

  if (metadata->lv2_ctr == (int64_t)(C_LV2 * metadata->nblocks))
    return iceberg_lv3_insert(table, key, value, lv3_index, thread_id);

  uint8 fprint1, fprint2;
  uint64 index1, index2;

  split_hash(lv2_hash(key, 0), &fprint1, &index1, metadata);
  split_hash(lv2_hash(key, 1), &fprint2, &index2, metadata);

  uint64 bindex1, boffset1, bindex2, boffset2;
  get_index_offset(table->metadata.log_init_size, index1, &bindex1, &boffset1);
  get_index_offset(table->metadata.log_init_size, index2, &bindex2, &boffset2);

  __mmask32 md_mask1 = slot_mask_32(metadata->lv2_md[bindex1][boffset1].block_md, 0) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
  __mmask32 md_mask2 = slot_mask_32(metadata->lv2_md[bindex2][boffset2].block_md, 0) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);

  uint8 popct1 = __builtin_popcountll(md_mask1);
  uint8 popct2 = __builtin_popcountll(md_mask2);

  if(popct2 > popct1) {
    fprint1 = fprint2;
    index1 = index2;
    bindex1 = bindex2;
    boffset1 = boffset2;
    md_mask1 = md_mask2;
    popct1 = popct2;
  }

#ifdef ENABLE_RESIZE
  // move blocks if resize is active and not already moved.
  if (UNLIKELY(index1 < (table->metadata.nblocks >> 1) && is_lv2_resize_active(table))) {
    uint64 chunk_idx = index1 / 8;
    uint64 mindex, moffset;
    get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
    // if fixing is needed set the marker
    if (!__sync_lock_test_and_set(&table->metadata.lv2_resize_marker[mindex][moffset], 1)) {
      for (uint8 i = 0; i < 8; ++i) {
        uint64 idx = chunk_idx * 8 + i;
        /*printf("LV2 Before: Moving block: %ld load: %f\n", idx, iceberg_block_load(table, idx, 2));*/
        iceberg_lv2_move_block(table, idx, thread_id);
        /*printf("LV2 After: Moving block: %ld load: %f\n", idx, iceberg_block_load(table, idx, 2));*/
      }
      // set the marker for the dest block
      uint64 dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
      __sync_lock_test_and_set(&table->metadata.lv2_resize_marker[mindex][moffset], 1);
    }
  }
#endif

  if (iceberg_lv2_insert_internal(table, key, value, fprint1, index1, thread_id))
    return TRUE;

  return iceberg_lv3_insert(table, key, value, lv3_index, thread_id);
}

static bool iceberg_insert_internal(iceberg_table * table, KeyType key, ValueType value, uint8 fprint, uint64 bindex, uint64 boffset, uint8 thread_id) {
  iceberg_metadata * metadata = &table->metadata;
  iceberg_lv1_block * blocks = table->level1[bindex];	
start: ;
  __mmask64 md_mask = slot_mask_64(metadata->lv1_md[bindex][boffset].block_md, 0);

  uint8 popct = __builtin_popcountll(md_mask);

  if (UNLIKELY(!popct))
    return FALSE;

#if PMEM
  uint8 slot_choice = get_slot_choice(key);
  uint8 start = popct == 0 ? 0 : slot_choice % popct;
#else
  uint8 start = 0;
#endif
  /*for(uint8 i = start; i < start + popct; ++i) {*/
#if PMEM
    uint8 slot = word_select(md_mask, start % popct);
#else
    uint8 slot = word_select(md_mask, start);
#endif

    /*if(__sync_bool_compare_and_swap(metadata->lv1_md[bindex][boffset].block_md + slot, 0, 1)) {*/
      pc_add(&metadata->lv1_balls, 1, thread_id);
      /*blocks[boffset].slots[slot].key = key;*/
      /*blocks[boffset].slots[slot].val = value;*/
      atomic_write_128(key, value, (uint64*)&blocks[boffset].slots[slot]);
#if PMEM
      pmem_persist(&blocks[boffset].slots[slot], sizeof(kv_pair));
#endif
      metadata->lv1_md[bindex][boffset].block_md[slot] = fprint;
      return TRUE;
    /*}*/
  goto start;
  /*}*/

  return FALSE;
}

__attribute__ ((always_inline)) inline bool iceberg_insert(iceberg_table * table, KeyType key, ValueType value, uint8 thread_id) {
#ifdef ENABLE_RESIZE
  if (UNLIKELY(need_resize(table))) {
    iceberg_setup_resize(table);
  }
#endif

  iceberg_metadata * metadata = &table->metadata;
  uint8 fprint;
  uint64 index;

  split_hash(lv1_hash(key), &fprint, &index, metadata);

#ifdef ENABLE_RESIZE
  // move blocks if resize is active and not already moved.
  if (UNLIKELY(index < (table->metadata.nblocks >> 1) && is_lv1_resize_active(table))) {
    uint64 chunk_idx = index / 8;
    uint64 mindex, moffset;
    get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
    // if fixing is needed set the marker
    if (!__sync_lock_test_and_set(&table->metadata.lv1_resize_marker[mindex][moffset], 1)) {
      for (uint8 i = 0; i < 8; ++i) {
        uint64 idx = chunk_idx * 8 + i;
        /*printf("LV1 Before: Moving block: %ld load: %f\n", idx, iceberg_block_load(table, idx, 1));*/
        iceberg_lv1_move_block(table, idx, thread_id);
        /*printf("LV1 After: Moving block: %ld load: %f\n", idx, iceberg_block_load(table, idx, 1));*/
      }
      // set the marker for the dest block
      uint64 dest_chunk_idx = chunk_idx + table->metadata.nblocks / 8 / 2;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
      __sync_lock_test_and_set(&table->metadata.lv1_resize_marker[mindex][moffset], 1);
    }
  }
#endif
  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);

  lock_block(&metadata->lv1_md[bindex][boffset].block_md);
  ValueType v;
  if (UNLIKELY(iceberg_get_value(table, key, &v, thread_id))) {
    /*printf("Found!\n");*/
    unlock_block(&metadata->lv1_md[bindex][boffset].block_md);
    return TRUE;
  }

  bool ret = iceberg_insert_internal(table, key, value, fprint, bindex, boffset, thread_id);
  if (!ret)
    ret = iceberg_lv2_insert(table, key, value, index, thread_id);

  unlock_block(&metadata->lv1_md[bindex][boffset].block_md);
  return ret;
}

static inline bool iceberg_lv3_remove_internal(iceberg_table * table, KeyType key, uint64 lv3_index, uint8 thread_id) {
  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, lv3_index, &bindex, &boffset);

  iceberg_metadata * metadata = &table->metadata;
  iceberg_lv3_list * lists = table->level3[bindex];

  while(__sync_lock_test_and_set(metadata->lv3_locks[bindex] + boffset, 1));

  if(metadata->lv3_sizes[bindex][boffset] == 0) return FALSE;

#if PMEM
  iceberg_lv3_node * lv3_nodes = table->level3_nodes;
  assert(lists[boffset].head_idx != -1);
  iceberg_lv3_node *head = &lv3_nodes[lists[boffset].head_idx];
#else
  iceberg_lv3_node *head = lists[boffset].head;
#endif

  if(head->key == key) {
#if PMEM
    lists[boffset].head_idx = head->next_idx;
    pmem_memset_persist(head, 0, sizeof(*head));
#else    
    iceberg_lv3_node * old_head = lists[boffset].head;
    lists[boffset].head = lists[boffset].head->next_node;
    free(old_head);
#endif

    metadata->lv3_sizes[bindex][boffset]--;
    pc_add(&metadata->lv3_balls, -1, thread_id);
    metadata->lv3_locks[bindex][boffset] = 0;

    return TRUE;
  }

  iceberg_lv3_node * current_node = head;

  for(uint64 i = 0; i < metadata->lv3_sizes[bindex][boffset] - 1; ++i) {
#if PMEM
    assert(current_node->next_idx != -1);
    iceberg_lv3_node *next_node = &lv3_nodes[current_node->next_idx];
#else
    iceberg_lv3_node *next_node = current_node->next_node;
#endif

    if(next_node->key == key) {
#if PMEM
      current_node->next_idx = next_node->next_idx;
      pmem_memset_persist(next_node, 0, sizeof(*next_node));
#else
      iceberg_lv3_node * old_node = current_node->next_node;
      current_node->next_node = current_node->next_node->next_node;
      free(old_node);
#endif

      metadata->lv3_sizes[bindex][boffset]--;
      pc_add(&metadata->lv3_balls, -1, thread_id);
      metadata->lv3_locks[bindex][boffset] = 0;

      return TRUE;
    }

    current_node = next_node;
  }

  metadata->lv3_locks[bindex][boffset] = 0;
  return FALSE;
}

static inline bool iceberg_lv3_remove(iceberg_table * table, KeyType key, uint64 lv3_index, uint8 thread_id) {

  bool ret = iceberg_lv3_remove_internal(table, key, lv3_index, thread_id);

  if (ret)
    return TRUE;

#ifdef ENABLE_RESIZE
  // check if there's an active resize and block isn't fixed yet
  if (UNLIKELY(is_lv3_resize_active(table) && lv3_index >= (table->metadata.nblocks >> 1))) {
    uint64 mask = ~(1ULL << (table->metadata.block_bits - 1));
    uint64 old_index = lv3_index & mask;
    uint64 chunk_idx = old_index / 8;
    uint64 mindex, moffset;
    get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
    if (__atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0) { // not fixed yet
      return iceberg_lv3_remove_internal(table, key, old_index, thread_id);
    } else {
      // wait for the old block to be fixed
      uint64 dest_chunk_idx = lv3_index / 8;
      get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
      while (__atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0)
        ;
    }
  }
#endif

  return FALSE;
}

static inline bool iceberg_lv2_remove(iceberg_table * table, KeyType key, uint64 lv3_index, uint8 thread_id) {
  iceberg_metadata * metadata = &table->metadata;

  for(int i = 0; i < D_CHOICES; ++i) {
    uint8 fprint;
    uint64 index;

    split_hash(lv2_hash(key, i), &fprint, &index, metadata);

    uint64 bindex, boffset;
    get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
    iceberg_lv2_block * blocks = table->level2[bindex];

#ifdef ENABLE_RESIZE
    // check if there's an active resize and block isn't fixed yet
    if (UNLIKELY(is_lv2_resize_active(table) && index >= (table->metadata.nblocks >> 1))) {
      uint64 mask = ~(1ULL << (table->metadata.block_bits - 1));
      uint64 old_index = index & mask;
      uint64 chunk_idx = old_index / 8;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      if (__atomic_load_n(&table->metadata.lv2_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0) { // not fixed yet
        uint64 old_bindex, old_boffset;
        get_index_offset(table->metadata.log_init_size - 3, old_index, &old_bindex, &old_boffset);
        __mmask32 md_mask = slot_mask_32(metadata->lv2_md[old_bindex][old_boffset].block_md, fprint) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
        uint8 popct = __builtin_popcount(md_mask);
        iceberg_lv2_block * blocks = table->level2[old_bindex];
        for(uint8 i = 0; i < popct; ++i) {
          uint8 slot = word_select(md_mask, i);

          if (blocks[old_boffset].slots[slot].key == key) {
            metadata->lv2_md[old_bindex][old_boffset].block_md[slot] = 0;
            blocks[old_boffset].slots[slot].key = blocks[old_boffset].slots[slot].val = 0;
#if PMEM
            pmem_persist(&blocks[old_boffset].slots[slot], sizeof(kv_pair));
#endif
            pc_add(&metadata->lv2_balls, -1, thread_id);
            return TRUE;
          }
        }
      } else {
        // wait for the old block to be fixed
        uint64 dest_chunk_idx = index / 8;
        get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
        while (__atomic_load_n(&table->metadata.lv2_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0)
          ;
      }
    }
#endif

    __mmask32 md_mask = slot_mask_32(metadata->lv2_md[bindex][boffset].block_md, fprint) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);
    uint8 popct = __builtin_popcount(md_mask);

    for(uint8 i = 0; i < popct; ++i) {
      uint8 slot = word_select(md_mask, i);

      if (blocks[boffset].slots[slot].key == key) {
        metadata->lv2_md[bindex][boffset].block_md[slot] = 0;
        blocks[boffset].slots[slot].key = blocks[boffset].slots[slot].val = 0;
#if PMEM
        pmem_persist(&blocks[boffset].slots[slot], sizeof(kv_pair));
#endif
        pc_add(&metadata->lv2_balls, -1, thread_id);
        return TRUE;
      }
    }
  }

  return iceberg_lv3_remove(table, key, lv3_index, thread_id);
}

bool iceberg_remove(iceberg_table * table, KeyType key, uint8 thread_id) {
  iceberg_metadata * metadata = &table->metadata;
  uint8 fprint;
  uint64 index;

  split_hash(lv1_hash(key), &fprint, &index, metadata);

  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
  iceberg_lv1_block * blocks = table->level1[bindex];

#ifdef ENABLE_RESIZE
  // check if there's an active resize and block isn't fixed yet
  if (UNLIKELY(is_lv1_resize_active(table) && index >= (table->metadata.nblocks >> 1))) {
    uint64 mask = ~(1ULL << (table->metadata.block_bits - 1));
    uint64 old_index = index & mask;
    uint64 chunk_idx = old_index / 8;
    uint64 mindex, moffset;
    get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
    if (__atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0) { // not fixed yet
      uint64 old_bindex, old_boffset;
      get_index_offset(table->metadata.log_init_size, old_index, &old_bindex, &old_boffset);
      __mmask64 md_mask = slot_mask_64(metadata->lv1_md[old_bindex][old_boffset].block_md, fprint);
      uint8 popct = __builtin_popcountll(md_mask);

      iceberg_lv1_block * blocks = table->level1[old_bindex];
      for(uint8 i = 0; i < popct; ++i) {
        uint8 slot = word_select(md_mask, i);

        if (blocks[old_index].slots[slot].key == key) {
          metadata->lv1_md[old_bindex][old_boffset].block_md[slot] = 0;
          blocks[old_boffset].slots[slot].key = blocks[old_boffset].slots[slot].val = 0;
#if PMEM
          pmem_persist(&blocks[old_boffset].slots[slot], sizeof(kv_pair));
#endif
          pc_add(&metadata->lv1_balls, -1, thread_id);
          return TRUE;
        }
      }
    } else {
      // wait for the old block to be fixed
      uint64 dest_chunk_idx = index / 8;
      get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
      while (__atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0)
        ;
    }
  }
#endif

  lock_block(&metadata->lv1_md[bindex][boffset].block_md);
  __mmask64 md_mask = slot_mask_64(metadata->lv1_md[bindex][boffset].block_md, fprint);
  uint8 popct = __builtin_popcountll(md_mask);

  for(uint8 i = 0; i < popct; ++i) {
    uint8 slot = word_select(md_mask, i);

    if (blocks[boffset].slots[slot].key == key) {
      metadata->lv1_md[bindex][boffset].block_md[slot] = 0;
      blocks[boffset].slots[slot].key = blocks[boffset].slots[slot].val = 0;
#if PMEM
      pmem_persist(&blocks[boffset].slots[slot], sizeof(kv_pair));
#endif
      pc_add(&metadata->lv1_balls, -1, thread_id);
      unlock_block(&metadata->lv1_md[bindex][boffset].block_md);
      return TRUE;
    }
  }

  bool ret = iceberg_lv2_remove(table, key, index, thread_id);

  unlock_block(&metadata->lv1_md[bindex][boffset].block_md);
  return ret;
}

static inline bool iceberg_lv3_get_value_internal(iceberg_table * table, KeyType key, ValueType *value, uint64 lv3_index) {
  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, lv3_index, &bindex, &boffset);

  iceberg_metadata * metadata = &table->metadata;
  iceberg_lv3_list * lists = table->level3[bindex];

  if(LIKELY(!metadata->lv3_sizes[bindex][boffset]))
    return FALSE;

  while(__sync_lock_test_and_set(metadata->lv3_locks[bindex] + boffset, 1));

#if PMEM
  iceberg_lv3_node * lv3_nodes = table->level3_nodes;
  assert(lists[boffset].head_idx != -1);
  iceberg_lv3_node * current_node = &lv3_nodes[lists[boffset].head_idx];
#else
  iceberg_lv3_node * current_node = lists[boffset].head;
#endif

  for(uint8 i = 0; i < metadata->lv3_sizes[bindex][boffset]; ++i) {
    if(current_node->key == key) {
      *value = current_node->val;
      metadata->lv3_locks[bindex][boffset] = 0;
      return TRUE;
    }
#if PMEM
    current_node = &lv3_nodes[current_node->next_idx];
#else
    current_node = current_node->next_node;
#endif
  }

  metadata->lv3_locks[bindex][boffset] = 0;

  return FALSE;
}

static inline bool iceberg_lv3_get_value(iceberg_table * table, KeyType key, ValueType *value, uint64 lv3_index) {
#ifdef ENABLE_RESIZE
  // check if there's an active resize and block isn't fixed yet
  if (UNLIKELY(is_lv3_resize_active(table) && lv3_index >= (table->metadata.nblocks >> 1))) {
    uint64 mask = ~(1ULL << (table->metadata.block_bits - 1));
    uint64 old_index = lv3_index & mask;
    uint64 chunk_idx = old_index / 8;
    uint64 mindex, moffset;
    get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
    if (__atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0) { // not fixed yet
      return iceberg_lv3_get_value_internal(table, key, value, old_index);
    } else {
      // wait for the old block to be fixed
      uint64 dest_chunk_idx = lv3_index / 8;
      get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
      while (__atomic_load_n(&table->metadata.lv3_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0)
        ;
    }
  }
#endif

  return iceberg_lv3_get_value_internal(table, key, value, lv3_index);
}

static inline bool iceberg_lv2_get_value(iceberg_table * table, KeyType key, ValueType *value, uint64 lv3_index) {

  iceberg_metadata * metadata = &table->metadata;

  for(uint8 i = 0; i < D_CHOICES; ++i) {
    uint8 fprint;
    uint64 index;

    split_hash(lv2_hash(key, i), &fprint, &index, metadata);

    uint64 bindex, boffset;
    get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
    iceberg_lv2_block * blocks = table->level2[bindex];

#ifdef ENABLE_RESIZE
    // check if there's an active resize and block isn't fixed yet
    if (UNLIKELY(is_lv2_resize_active(table) && index >= (table->metadata.nblocks >> 1))) {
      uint64 mask = ~(1ULL << (table->metadata.block_bits - 1));
      uint64 old_index = index & mask;
      uint64 chunk_idx = old_index / 8;
      uint64 mindex, moffset;
      get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
      if (__atomic_load_n(&table->metadata.lv2_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0) { // not fixed yet
        uint64 old_bindex, old_boffset;
        get_index_offset(table->metadata.log_init_size, old_index, &old_bindex, &old_boffset);
        __mmask32 md_mask = slot_mask_32(metadata->lv2_md[old_bindex][old_boffset].block_md, fprint) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);

        iceberg_lv2_block * blocks = table->level2[old_bindex];
        while (md_mask != 0) {
          int slot = __builtin_ctz(md_mask);
          md_mask = md_mask & ~(1U << slot);

          if (blocks[old_boffset].slots[slot].key == key) {
            *value = blocks[old_boffset].slots[slot].val;
            return TRUE;
          }
        }
      } else {
        // wait for the old block to be fixed
        uint64 dest_chunk_idx = index / 8;
        get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
        while (__atomic_load_n(&table->metadata.lv2_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0)
          ;
      }
    }
#endif

    __mmask32 md_mask = slot_mask_32(metadata->lv2_md[bindex][boffset].block_md, fprint) & ((1 << (C_LV2 + MAX_LG_LG_N / D_CHOICES)) - 1);

    while (md_mask != 0) {
      int slot = __builtin_ctz(md_mask);
      md_mask = md_mask & ~(1U << slot);

      if (blocks[boffset].slots[slot].key == key) {
        *value = blocks[boffset].slots[slot].val;
        return TRUE;
      }
    }

  }

  return iceberg_lv3_get_value(table, key, value, lv3_index);
}

__attribute__ ((always_inline)) inline bool iceberg_get_value(iceberg_table * table, KeyType key, ValueType *value, uint8 thread_id) {
  iceberg_metadata * metadata = &table->metadata;

  uint8 fprint;
  uint64 index;

  split_hash(lv1_hash(key), &fprint, &index, metadata);

  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
  iceberg_lv1_block * blocks = table->level1[bindex];

#ifdef ENABLE_RESIZE
  // check if there's an active resize and block isn't fixed yet
  if (UNLIKELY(is_lv1_resize_active(table) && index >= (table->metadata.nblocks >> 1))) {
    uint64 mask = ~(1ULL << (table->metadata.block_bits - 1));
    uint64 old_index = index & mask;
    uint64 chunk_idx = old_index / 8;
    uint64 mindex, moffset;
    get_index_offset(table->metadata.log_init_size - 3, chunk_idx, &mindex, &moffset);
    if (__atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0) { // not fixed yet
      uint64 old_bindex, old_boffset;
      get_index_offset(table->metadata.log_init_size, old_index, &old_bindex, &old_boffset);
      __mmask64 md_mask = slot_mask_64(metadata->lv1_md[old_bindex][old_boffset].block_md, fprint);

      iceberg_lv1_block * blocks = table->level1[old_bindex];
      while (md_mask != 0) {
        int slot = __builtin_ctzll(md_mask);
        md_mask = md_mask & ~(1ULL << slot);

        if (blocks[old_boffset].slots[slot].key == key) {
          *value = blocks[old_boffset].slots[slot].val;
          return TRUE;
        }
      }
    } else {
      // wait for the old block to be fixed
      uint64 dest_chunk_idx = index / 8;
      get_index_offset(table->metadata.log_init_size - 3, dest_chunk_idx, &mindex, &moffset);
      while (__atomic_load_n(&table->metadata.lv1_resize_marker[mindex][moffset], __ATOMIC_SEQ_CST) == 0)
        ;
    }
  }
#endif

  __mmask64 md_mask = slot_mask_64(metadata->lv1_md[bindex][boffset].block_md, fprint);

  while (md_mask != 0) {
    int slot = __builtin_ctzll(md_mask);
    md_mask = md_mask & ~(1ULL << slot);

    if (blocks[boffset].slots[slot].key == key) {
      *value = blocks[boffset].slots[slot].val;
      return TRUE;
    }
  }

  bool ret = iceberg_lv2_get_value(table, key, value, index);

  /*unlock_block(&metadata->lv1_md[bindex][boffset].block_md);*/
  return ret;
}

#ifdef ENABLE_RESIZE
static bool iceberg_nuke_key(iceberg_table * table, uint64 level, uint64 index, uint64 slot, uint64 thread_id) {
  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, index, &bindex, &boffset);
  iceberg_metadata * metadata = &table->metadata;

  if (level == 1) {
    iceberg_lv1_block * blocks = table->level1[bindex];
    metadata->lv1_md[bindex][boffset].block_md[slot] = 0;
    blocks[boffset].slots[slot].key = blocks[boffset].slots[slot].val = 0;
#if PMEM
    pmem_persist(&blocks[boffset].slots[slot], sizeof(kv_pair));
#endif
    pc_add(&metadata->lv1_balls, -1, thread_id);
  } else if (level == 2) {
    iceberg_lv2_block * blocks = table->level2[bindex];
    metadata->lv2_md[bindex][boffset].block_md[slot] = 0;
    blocks[boffset].slots[slot].key = blocks[boffset].slots[slot].val = 0;
#if PMEM
    pmem_persist(&blocks[boffset].slots[slot], sizeof(kv_pair));
#endif
    pc_add(&metadata->lv2_balls, -1, thread_id);
  }

  return TRUE;
}

static bool iceberg_lv1_move_block(iceberg_table * table, uint64 bnum, uint8 thread_id) {
  // grab a block 
  uint64 bctr = __atomic_fetch_add(&table->metadata.lv1_resize_ctr, 1, __ATOMIC_SEQ_CST);
  if (bctr >= (table->metadata.nblocks >> 1))
    return TRUE;

  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, bnum, &bindex, &boffset);
  // relocate items in level1
  for (uint64 j = 0; j < (1 << SLOT_BITS); ++j) {
    KeyType key = table->level1[bindex][boffset].slots[j].key;
    if (key == 0)
      continue;
    ValueType value = table->level1[bindex][boffset].slots[j].val;
    uint8 fprint;
    uint64 index;

    split_hash(lv1_hash(key), &fprint, &index, &table->metadata);

    // move to new location
    if (index != bnum) {
      uint64 local_bindex, local_boffset;
      get_index_offset(table->metadata.log_init_size,index, &local_bindex, &local_boffset);
      if (!iceberg_insert_internal(table, key, value, fprint, local_bindex, local_boffset, thread_id)) {
        printf("Failed insert during resize lv1\n");
        exit(0);
      }
      if (!iceberg_nuke_key(table, 1, bnum, j, thread_id)) {
        printf("Failed remove during resize lv1. key: %" PRIu64 ", block: %ld\n", key, bnum);
        exit(0);
      }
      //ValueType *val;
      //if (!iceberg_get_value(table, key, &val, thread_id)) {
      // printf("Key not found during resize lv1: %ld\n", key);
      //exit(0);
      //}
    }
  }

  return FALSE;
}

static bool iceberg_lv2_move_block(iceberg_table * table, uint64 bnum, uint8 thread_id) {
  // grab a block 
  uint64 bctr = __atomic_fetch_add(&table->metadata.lv2_resize_ctr, 1, __ATOMIC_SEQ_CST);
  if (bctr >= (table->metadata.nblocks >> 1))
    return TRUE;

  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, bnum, &bindex, &boffset);
  uint64 mask = ~(1ULL << (table->metadata.block_bits - 1));
  // relocate items in level2
  for (uint64 j = 0; j < C_LV2 + MAX_LG_LG_N / D_CHOICES; ++j) {
    KeyType key = table->level2[bindex][boffset].slots[j].key;
    if (key == 0)
      continue;
    ValueType value = table->level2[bindex][boffset].slots[j].val;
    uint8 fprint;
    uint64 index;

    split_hash(lv1_hash(key), &fprint, &index, &table->metadata);

    for(int i = 0; i < D_CHOICES; ++i) {
      uint8 l2fprint;
      uint64 l2index;

      split_hash(lv2_hash(key, i), &l2fprint, &l2index, &table->metadata);

      // move to new location
      if ((l2index & mask) == bnum && l2index != bnum) {
        if (!iceberg_lv2_insert_internal(table, key, value, l2fprint, l2index, thread_id)) {
          if (!iceberg_lv2_insert(table, key, value, index, thread_id)) {
            printf("Failed insert during resize lv2\n");
            exit(0);
          }
        }
        if (!iceberg_nuke_key(table, 2, bnum, j, thread_id)) {
          printf("Failed remove during resize lv2\n");
          exit(0);
        }
        break;
        //ValueType *val;
        //if (!iceberg_get_value(table, key, &val, thread_id)) {
        //printf("Key not found during resize lv2: %ld\n", key);
        //exit(0);
        //}
      }
    }
  }

  return FALSE;
}

static bool iceberg_lv3_move_block(iceberg_table * table, uint64 bnum, uint8 thread_id) {
  // grab a block 
  uint64 bctr = __atomic_fetch_add(&table->metadata.lv3_resize_ctr, 1, __ATOMIC_SEQ_CST);
  if (bctr >= (table->metadata.nblocks >> 1))
    return TRUE;

  uint64 bindex, boffset;
  get_index_offset(table->metadata.log_init_size, bnum, &bindex, &boffset);
  // relocate items in level3
  if(UNLIKELY(table->metadata.lv3_sizes[bindex][boffset])) {
#if PMEM
    iceberg_lv3_list * lists = table->level3[bindex];
    iceberg_lv3_node * lv3_nodes = table->level3_nodes;
    iceberg_lv3_node * current_node = &lv3_nodes[lists[boffset].head_idx];
#else
    iceberg_lv3_node * current_node = table->level3[bindex][boffset].head;
#endif

    while (current_node != NULL) {
      KeyType key = current_node->key;
      ValueType value = current_node->val;

      uint8 fprint;
      uint64 index;

      split_hash(lv1_hash(key), &fprint, &index, &table->metadata);
      // move to new location
      if (index != bnum) {
        if (!iceberg_lv3_insert(table, key, value, index, thread_id)) {
          printf("Failed insert during resize lv3\n");
          exit(0);
        }
        if (!iceberg_lv3_remove(table, key, bnum, thread_id)) {
          printf("Failed remove during resize lv3: %" PRIu64 "\n", key);
          exit(0);
        }
        // ValueType *val;
        //if (!iceberg_get_value(table, key, &val, thread_id)) {
        // printf("Key not found during resize lv3: %ld\n", key);
        //exit(0);
        //}
      }
#if PMEM      
      if (current_node->next_idx != -1) {
        current_node = &lv3_nodes[current_node->next_idx];
      } else {
        current_node = NULL;
      }
#else
      current_node = current_node->next_node;
#endif
    }
  }

  return FALSE;
}
#endif
