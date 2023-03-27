#pragma once

#include "platform.h"

#define SLOT_BITS 6
#define FPRINT_BITS 8
#define D_CHOICES 2
#define MAX_LG_LG_N 4
#define C_LV2 6
#define MAX_RESIZES 8

  typedef uint64 KeyType;
  typedef uint64 ValueType;

  typedef struct __attribute__ ((__packed__)) kv_pair {
    KeyType key;
    ValueType val;
  } kv_pair;

  typedef struct __attribute__ ((__packed__)) iceberg_lv1_block {
    kv_pair slots[1 << SLOT_BITS];
  } iceberg_lv1_block;

  typedef struct __attribute__ ((__packed__)) iceberg_lv1_block_md {
    uint8 block_md[1 << SLOT_BITS];
  } iceberg_lv1_block_md;

  typedef struct __attribute__ ((__packed__)) iceberg_lv2_block {
    kv_pair slots[C_LV2 + MAX_LG_LG_N / D_CHOICES];
  } iceberg_lv2_block;

  typedef struct __attribute__ ((__packed__)) iceberg_lv2_block_md {
    uint8 block_md[C_LV2 + MAX_LG_LG_N / D_CHOICES];
  } iceberg_lv2_block_md;

  typedef struct iceberg_lv3_node {
    KeyType key;
    ValueType val;
    struct iceberg_lv3_node * next_node;
  } iceberg_lv3_node;

  typedef struct iceberg_lv3_list {
    iceberg_lv3_node * head;
  } iceberg_lv3_list;

  typedef struct iceberg_metadata {
    uint64 total_size_in_bytes;
    uint64 nblocks;
    uint64 nslots;
    uint64 block_bits;
    uint64 init_size;
    uint64 log_init_size;
    int64 lv1_ctr;
    int64 lv2_ctr;
    int64 lv3_ctr;
    iceberg_lv1_block_md * lv1_md[MAX_RESIZES];
    iceberg_lv2_block_md * lv2_md[MAX_RESIZES];
    buffer_handle lv1_md_bh[MAX_RESIZES];
    buffer_handle lv2_md_bh[MAX_RESIZES];
    uint64 * lv3_sizes[MAX_RESIZES];
    uint8 * lv3_locks[MAX_RESIZES];
    buffer_handle lv3_sizes_bh[MAX_RESIZES];
    buffer_handle lv3_locks_bh[MAX_RESIZES];
    uint64 nblocks_parts[MAX_RESIZES];
#ifdef ENABLE_RESIZE
    volatile int lock;
    uint64 resize_cnt;
    uint64 marker_sizes[MAX_RESIZES];
    uint64 lv1_resize_ctr;
    uint64 lv2_resize_ctr;
    uint64 lv3_resize_ctr;
    uint8 * lv1_resize_marker[MAX_RESIZES];
    uint8 * lv2_resize_marker[MAX_RESIZES];
    uint8 * lv3_resize_marker[MAX_RESIZES];
    buffer_handle lv1_resize_marker_bh[MAX_RESIZES];
    buffer_handle lv2_resize_marker_bh[MAX_RESIZES];
    buffer_handle lv3_resize_marker_bh[MAX_RESIZES];
#endif
  } iceberg_metadata;

  typedef struct iceberg_table {
    iceberg_metadata metadata;
    /* Only things that are persisted on PMEM */
    iceberg_lv1_block * level1[MAX_RESIZES];
    iceberg_lv2_block * level2[MAX_RESIZES];
    iceberg_lv3_list * level3[MAX_RESIZES];
    buffer_handle lv1_block_bh[MAX_RESIZES];
    buffer_handle lv2_block_bh[MAX_RESIZES];
    buffer_handle lv3_block_bh[MAX_RESIZES];

  } iceberg_table;

  int iceberg_init(iceberg_table *table, uint64 log_slots);

  void iceberg_deinit(iceberg_table * table);
  
  // TRUE = inserted, FALSE = insert insuccessful.
  bool iceberg_insert(iceberg_table * table, KeyType key, ValueType value);

  bool iceberg_remove(iceberg_table * table, KeyType key);

  // Only perform the remove if the value at the key is the value being passed in. TRUE if did perform the remove, FALSE otherwise.
  bool iceberg_remove_value(iceberg_table * table, KeyType key, ValueType value);

  bool iceberg_get_value(iceberg_table * table, KeyType key, ValueType * value);

#ifdef ENABLE_RESIZE
  void iceberg_end(iceberg_table * table);
#endif
