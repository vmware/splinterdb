// Copyright 2021-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * btree_private.h --
 *
 * This file contains the private interfaces for dynamic b-trees/memtables.
 * These definitions are provided here so that they can be shared by the
 * source and test modules.
 */
#pragma once

#include "splinterdb/data.h"
#include "util.h"
#include "btree.h"

/*
 * Typedefs for disk-resident index / offset / sizes of pieces of BTree row
 * formats. These appear in other disk-resident structures defined below.
 */
typedef uint16 table_index; //  So we can make this bigger for bigger nodes.
typedef uint16 node_offset; //  So we can make this bigger for bigger nodes.
typedef node_offset table_entry;

/*
 * *************************************************************************
 * BTree Node headers: Disk-resident structure:
 * Stored on pages of Page Type == PAGE_TYPE_MEMTABLE, PAGE_TYPE_BRANCH
 * See btree.c for a description of the layout of this page format.
 * The byte offset of the k'th entry from the start of the page is given by
 * the offsets[k]'th value.
 * *************************************************************************
 */
struct ONDISK btree_hdr {
   /*
    * Memtables store literal previous/next node addresses. Branches compute
    * same-extent neighbors from the page address; every branch page in an
    * extent stores the adjacent previous/next extent base addresses for its
    * level. FIRST_IN_LEVEL and LAST_IN_LEVEL stop logical neighbor computation
    * at level edges.
    */
   uint64 prev_addr;
   uint64 next_addr;
   union {
      // Branch: mini_allocator meta page that lists this node's extent.
      uint64 meta_page_addr;
      // Memtable: generation used to detect stale copied nodes.
      uint64 generation;
   };
   uint8       height;
   uint8       flags;
   node_offset next_entry;
   table_index num_entries;
   table_entry offsets[];
};

#define BTREE_HDR_FIRST_IN_LEVEL (1 << 0)
#define BTREE_HDR_LAST_IN_LEVEL  (1 << 1)

/*
 * *************************************************************************
 * BTree Node index entries: Disk-resident structure
 * *************************************************************************
 */
typedef struct ONDISK index_entry {
   btree_pivot_data pivot_data;
   ondisk_key       pivot;
} index_entry;

/*
 * *************************************************************************
 * BTree Node leaf entries: Disk-resident structure
 * *************************************************************************
 */
typedef ondisk_tuple leaf_entry;

typedef struct leaf_incorporate_spec {
   key   tuple_key;
   int64 idx;
   enum {
      ENTRY_DID_NOT_EXIST,
      ENTRY_STILL_EXISTS,
      ENTRY_HAS_BEEN_REMOVED
   } old_entry_state;
   bool32 use_new_message;
   union {
      /* use_new_message is the tag on this union. */
      message new_message;
      // modified_message is either merged or blobified or both...
      merge_accumulator modified_message;
   } msg;
} leaf_incorporate_spec;

platform_status
btree_create_leaf_incorporate_spec(const btree_config    *cfg,
                                   cache                 *cc,
                                   mini_allocator        *mini,
                                   platform_heap_id       heap_id,
                                   btree_hdr             *hdr,
                                   key                    tuple_key,
                                   message                message,
                                   leaf_incorporate_spec *spec);

bool32
btree_try_perform_leaf_incorporate_spec(const btree_config          *cfg,
                                        btree_hdr                   *hdr,
                                        const leaf_incorporate_spec *spec,
                                        uint64 *generation);

/*
 * This structure is intended to capture all the decisions in a leaf split.
 * That way, we can have a single function that defines the entire policy,
 * separate from the code that executes the policy (possibly as several steps
 * for concurrency reasons).
 */
typedef struct leaf_splitting_plan {
   uint64 split_idx; // keys with idx < split_idx go left
   bool32
      insertion_goes_left; // does the key to be inserted go to the left child
} leaf_splitting_plan;

/*
 * *************************************************************************
 * External function prototypes: Declare these first, as some inline static
 * functions defined below may call these extern functions.
 * *************************************************************************
 */
bool32
btree_set_index_entry(const btree_config *cfg,
                      btree_hdr          *hdr,
                      table_index         k,
                      key                 new_pivot_key,
                      uint64              new_addr,
                      btree_pivot_stats   stats);

bool32
btree_set_leaf_entry(const btree_config *cfg,
                     btree_hdr          *hdr,
                     table_index         k,
                     key                 new_key,
                     message             new_message);

void
btree_defragment_leaf(const btree_config    *cfg, // IN
                      btree_scratch         *scratch,
                      btree_hdr             *hdr,
                      leaf_incorporate_spec *spec); // IN

void
btree_defragment_index(const btree_config *cfg, // IN
                       btree_scratch      *scratch,
                       btree_hdr          *hdr); // IN

int64
btree_find_pivot(const btree_config *cfg,
                 const btree_hdr    *hdr,
                 key                 target,
                 bool32             *found);

leaf_splitting_plan
btree_build_leaf_splitting_plan(const btree_config          *cfg, // IN
                                const btree_hdr             *hdr,
                                const leaf_incorporate_spec *spec); // IN

void
destroy_leaf_incorporate_spec(leaf_incorporate_spec *spec);

/*
 * ***********************************************************
 * Inline accessor functions for different private structure.
 * ***********************************************************
 */
static inline uint64
btree_page_size(const btree_config *cfg)
{
   return cache_config_page_size(cfg->cache_cfg);
}

static inline uint64
btree_extent_size(const btree_config *cfg)
{
   return cache_config_extent_size(cfg->cache_cfg);
}

static inline bool32
btree_page_is_first_in_extent(const btree_config *cfg, uint64 addr)
{
   return addr % btree_extent_size(cfg) == 0;
}

static inline bool32
btree_page_is_last_in_extent(const btree_config *cfg, uint64 addr)
{
   return (addr + btree_page_size(cfg)) % btree_extent_size(cfg) == 0;
}

static inline void
btree_hdr_set_first_in_level(btree_hdr *hdr)
{
   hdr->flags |= BTREE_HDR_FIRST_IN_LEVEL;
}

static inline void
btree_hdr_set_last_in_level(btree_hdr *hdr)
{
   hdr->flags |= BTREE_HDR_LAST_IN_LEVEL;
}

static inline bool32
btree_hdr_is_first_in_level(const btree_hdr *hdr)
{
   return hdr->flags & BTREE_HDR_FIRST_IN_LEVEL;
}

static inline bool32
btree_hdr_is_last_in_level(const btree_hdr *hdr)
{
   return hdr->flags & BTREE_HDR_LAST_IN_LEVEL;
}

static inline uint64
btree_hdr_next_addr(const btree_config *cfg,
                    const btree_hdr    *hdr,
                    uint64              addr,
                    page_type           type)
{
   if (type == PAGE_TYPE_MEMTABLE) {
      return hdr->next_addr;
   }
   platform_assert(type == PAGE_TYPE_BRANCH);

   if (btree_hdr_is_last_in_level(hdr)) {
      return 0;
   }
   if (!btree_page_is_last_in_extent(cfg, addr)) {
      return addr + btree_page_size(cfg);
   }
   debug_assert(hdr->next_addr != 0);
   return hdr->next_addr;
}

static inline uint64
btree_hdr_prev_addr(const btree_config *cfg,
                    const btree_hdr    *hdr,
                    uint64              addr,
                    page_type           type)
{
   if (type == PAGE_TYPE_MEMTABLE) {
      return hdr->prev_addr;
   }
   platform_assert(type == PAGE_TYPE_BRANCH);

   if (btree_hdr_is_first_in_level(hdr)) {
      return 0;
   }
   if (!btree_page_is_first_in_extent(cfg, addr)) {
      return addr - btree_page_size(cfg);
   }
   debug_assert(hdr->prev_addr != 0);
   return hdr->prev_addr + btree_extent_size(cfg) - btree_page_size(cfg);
}

static inline uint64
btree_hdr_next_extent_addr(const btree_config *cfg,
                           const btree_hdr    *hdr,
                           uint64              addr,
                           page_type           type)
{
   (void)cfg;
   (void)addr;

   if (type != PAGE_TYPE_BRANCH || btree_hdr_is_last_in_level(hdr)
       || hdr->next_addr == 0)
   {
      return 0;
   }
   return hdr->next_addr;
}

static inline uint64
btree_hdr_prev_extent_addr(const btree_config *cfg,
                           const btree_hdr    *hdr,
                           uint64              addr,
                           page_type           type)
{
   (void)cfg;
   (void)addr;

   if (type != PAGE_TYPE_BRANCH || btree_hdr_is_first_in_level(hdr)
       || hdr->prev_addr == 0)
   {
      return 0;
   }
   return hdr->prev_addr;
}

static inline void
btree_init_hdr(const btree_config *cfg, btree_hdr *hdr)
{
   ZERO_CONTENTS(hdr);
   hdr->next_entry = btree_page_size(cfg);
}

static inline uint64
sizeof_index_entry(const index_entry *entry)
{
   return sizeof(*entry) + sizeof_ondisk_key_data(&entry->pivot);
}

static inline uint64
sizeof_leaf_entry(const leaf_entry *entry)
{
   return sizeof(*entry) + sizeof_ondisk_tuple_data(entry);
}

static inline key
index_entry_key(const index_entry *entry)
{
   return ondisk_key_to_key(&entry->pivot);
}

static inline uint64
index_entry_child_addr(const index_entry *entry)
{
   return entry->pivot_data.child_addr;
}

static inline key
leaf_entry_key(leaf_entry *entry)
{
   return ondisk_tuple_key(entry);
}

static inline message
leaf_entry_message(cache *cc, leaf_entry *entry)
{
   return ondisk_tuple_message(cc, entry);
}

static inline message_type
leaf_entry_message_type(leaf_entry *entry)
{
   return entry->flags & ONDISK_MESSAGE_TYPE_MASK;
}

static inline leaf_entry *
btree_get_leaf_entry(const btree_config *cfg,
                     const btree_hdr    *hdr,
                     table_index         k)
{
   /* Ensure that the kth entry's header is after the end of the table and
    * before the end of the page.
    */
   debug_assert(diff_ptr(hdr, &hdr->offsets[hdr->num_entries])
                <= hdr->offsets[k]);
   debug_code(uint64 bt_page_size = btree_page_size(cfg));
   debug_assert(hdr->offsets[k] + sizeof(leaf_entry) <= bt_page_size);
   leaf_entry *entry =
      (leaf_entry *)const_pointer_byte_offset(hdr, hdr->offsets[k]);
   debug_assert(hdr->offsets[k] + sizeof_leaf_entry(entry) <= bt_page_size);
   return entry;
}

static inline key
btree_get_tuple_key(const btree_config *cfg,
                    const btree_hdr    *hdr,
                    table_index         k)
{
   return leaf_entry_key(btree_get_leaf_entry(cfg, hdr, k));
}

static inline message
btree_get_tuple_message(const btree_config *cfg,
                        cache              *cc,
                        const btree_hdr    *hdr,
                        table_index         k)
{
   return leaf_entry_message(cc, btree_get_leaf_entry(cfg, hdr, k));
}

static inline message_type
btree_get_tuple_message_type(const btree_config *cfg,
                             const btree_hdr    *hdr,
                             table_index         k)
{
   return leaf_entry_message_type(btree_get_leaf_entry(cfg, hdr, k));
}


/*
 * Return a ptr to the k'th index_entry on the BTree page.
 * Validates addresses, in debug mode.
 */
static inline index_entry *
btree_get_index_entry(const btree_config *cfg,
                      const btree_hdr    *hdr,
                      table_index         k)
{
   /* Ensure that the kth entry's header is after the end of the table and
    * before the end of the page.
    */
   debug_assert(diff_ptr(hdr, &hdr->offsets[hdr->num_entries])
                <= hdr->offsets[k]);
   debug_code(uint64 bt_page_size = btree_page_size(cfg));
   debug_assert((hdr->offsets[k] + sizeof(index_entry) <= bt_page_size),
                "k=%d, offsets[k]=%d, sizeof(index_entry)=%lu"
                ", btree_page_size=%lu.",
                k,
                hdr->offsets[k],
                sizeof(index_entry),
                bt_page_size);

   index_entry *entry =
      (index_entry *)const_pointer_byte_offset(hdr, hdr->offsets[k]);

   /* Now ensure that the entire entry fits in the page. */
   debug_assert((hdr->offsets[k] + sizeof_index_entry(entry) <= bt_page_size),
                "Offsets entry at index k=%d does not fit in the page."
                " offsets[k]=%d, sizeof_index_entry()=%lu"
                ", btree_page_size=%lu.",
                k,
                hdr->offsets[k],
                sizeof_index_entry(entry),
                bt_page_size);
   return entry;
}

static inline key
btree_get_pivot(const btree_config *cfg, const btree_hdr *hdr, table_index k)
{
   return index_entry_key(btree_get_index_entry(cfg, hdr, k));
}

static inline uint64
btree_get_child_addr(const btree_config *cfg,
                     const btree_hdr    *hdr,
                     table_index         k)
{
   return index_entry_child_addr(btree_get_index_entry(cfg, hdr, k));
}
