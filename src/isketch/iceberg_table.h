#ifndef _POTC_TABLE_H_
#define _POTC_TABLE_H_

#include "lock.h"
#include "types.h"
#include "sketch.h"

#ifdef __cplusplus
#   define __restrict__
extern "C" {
#endif

#define SLOT_BITS   6
#define FPRINT_BITS 8
#define D_CHOICES   2
#define MAX_LG_LG_N 4
#define C_LV2       6
#define MAX_RESIZES 32

// typedef struct __attribute__((__packed__)) kv_pair {
typedef struct kv_pair {
   KeyType   key;
   ValueType val;
   uint64_t  refcount;
} kv_pair;

typedef struct __attribute__((__packed__)) iceberg_lv1_block {
   kv_pair slots[1 << SLOT_BITS];
} iceberg_lv1_block;

typedef struct __attribute__((__packed__)) iceberg_lv1_block_md {
   uint8_t block_md[1 << SLOT_BITS];
} iceberg_lv1_block_md;

typedef struct __attribute__((__packed__)) iceberg_lv2_block {
   kv_pair slots[C_LV2 + MAX_LG_LG_N / D_CHOICES];
} iceberg_lv2_block;

typedef struct __attribute__((__packed__)) iceberg_lv2_block_md {
   uint8_t block_md[C_LV2 + MAX_LG_LG_N / D_CHOICES];
} iceberg_lv2_block_md;

typedef struct iceberg_lv3_node {
   kv_pair kv;
#ifdef PMEM
   bool      in_use;
   ptrdiff_t next_idx;
#else
   struct iceberg_lv3_node *next_node;
#endif
} iceberg_lv3_node __attribute__((aligned(64)));

typedef struct iceberg_lv3_list {
#ifdef PMEM
   ptrdiff_t head_idx;
#else
   iceberg_lv3_node        *head;
#endif
} iceberg_lv3_list;

typedef struct iceberg_metadata {
   uint64_t              total_size_in_bytes;
   uint64_t              nblocks;
   uint64_t              nslots;
   uint64_t              block_bits;
   uint64_t              init_size;
   uint64_t              log_init_size;
   int64_t               lv1_ctr;
   int64_t               lv2_ctr;
   int64_t               lv3_ctr;
   pc_t                  lv1_balls;
   pc_t                  lv2_balls;
   pc_t                  lv3_balls;
   iceberg_lv1_block_md *lv1_md[MAX_RESIZES];
   iceberg_lv2_block_md *lv2_md[MAX_RESIZES];
   uint64_t             *lv3_sizes[MAX_RESIZES];
   uint8_t              *lv3_locks[MAX_RESIZES];
   uint64_t              nblocks_parts[MAX_RESIZES];
#ifdef ENABLE_RESIZE
   volatile int lock;
   uint64_t     resize_cnt;
   uint64_t     marker_sizes[MAX_RESIZES];
   uint64_t     lv1_resize_ctr;
   uint64_t     lv2_resize_ctr;
   uint64_t     lv3_resize_ctr;
   uint8_t     *lv1_resize_marker[MAX_RESIZES];
   uint8_t     *lv2_resize_marker[MAX_RESIZES];
   uint8_t     *lv3_resize_marker[MAX_RESIZES];
#endif
} iceberg_metadata;

typedef struct iceberg_table {
   iceberg_metadata metadata;
   /* Only things that are persisted on PMEM */
   iceberg_lv1_block *level1[MAX_RESIZES];
   iceberg_lv2_block *level2[MAX_RESIZES];
   iceberg_lv3_list  *level3[MAX_RESIZES];
#ifdef PMEM
   iceberg_lv3_node *level3_nodes;
#endif
   sketch *sktch;
} iceberg_table;

uint64_t
lv1_balls(iceberg_table *table);
uint64_t
lv2_balls(iceberg_table *table);
uint64_t
lv3_balls(iceberg_table *table);
uint64_t
tot_balls(iceberg_table *table);

int
iceberg_init(iceberg_table *table, uint64_t log_slots);
int
iceberg_init_with_sketch(iceberg_table *table,
                         uint64_t       log_slots,
                         uint64_t       rows,
                         uint64_t       cols);

double
iceberg_load_factor(iceberg_table *table);

/**
 *
 * If there exists a key in the hash table, it increases its refcount
 * by one without updating the value. Otherwise, the value with the
 * refcount of 1 is inserted. In this case, if the sketch is enabled,
 * it set the bigger value between the new value and that from the
 * sketch.
 *
 * @return If a new item is inserted, it returns true. Otherwise,
 * returns false.
 *
 */
bool
iceberg_insert(iceberg_table *table,
               KeyType        key,
               ValueType      value,
               uint8_t        thread_id);

/**
 *
 * If there exists a key in the hash table, it increases its refcount
 * by one without updating the value. Otherwise, the value with the
 * refcount of 1 is inserted. In this case, if the sketch is enabled,
 * it set the bigger value between the new value and that from the
 * sketch.
 *
 */
bool
iceberg_insert_without_increasing_refcount(iceberg_table *table,
                                           KeyType        key,
                                           ValueType      value,
                                           uint8_t        thread_id);

/**
 * The following functions do the same thing as the above functions,
 * but it returns the pointer of the value.
 */
bool
iceberg_insert_and_get(iceberg_table *table,
                       KeyType        key,
                       ValueType    **value,
                       uint8_t        thread_id);

bool
iceberg_insert_and_get_without_increasing_refcount(iceberg_table *table,
                                                   KeyType        key,
                                                   ValueType    **value,
                                                   uint8_t        thread_id);

/**
 *
 * If there exists a key in the hash table, it just overwrites the value without
 * increasing the refcount. Otherwise, it does nothing.
 *
 */
bool
iceberg_update(iceberg_table *table,
               KeyType        key,
               ValueType      value,
               uint8_t        thread_id);

/**
 *
 * If there exists a key in the hash table, it increases its refcount
 * by one and overwrites the value.
 *
 */
bool
iceberg_put(iceberg_table *table,
            KeyType        key,
            ValueType      value,
            uint8_t        thread_id);

bool
iceberg_remove(iceberg_table *table, KeyType key, uint8_t thread_id);


/*
 * Gets the key and the copy of the value before removing
 */
bool
iceberg_get_and_remove(iceberg_table *table,
                       KeyType       *key,
                       ValueType     *value,
                       uint8_t        thread_id);

bool
iceberg_force_remove(iceberg_table *table, KeyType key, uint8_t thread_id);

bool
iceberg_decrease_refcount(iceberg_table *table, KeyType key, uint8_t thread_id);

bool
iceberg_get_value(iceberg_table *table,
                  KeyType        key,
                  ValueType    **value,
                  uint8_t        thread_id);

#ifdef PMEM
uint64_t
iceberg_dismount(iceberg_table *table);
int
iceberg_mount(iceberg_table *table, uint64_t log_slots, uint64_t resize_cnt);
#endif

#ifdef ENABLE_RESIZE
void
iceberg_end(iceberg_table *table);
#endif

void
iceberg_print_state(iceberg_table *table);

#ifdef __cplusplus
}
#endif

#endif // _POTC_TABLE_H_
