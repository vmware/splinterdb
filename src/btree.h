// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * btree.h --
 *
 *     This file contains the public interfaces for dynamic b-trees/memtables.
 */

#pragma once

#include "mini_allocator.h"
#include "iterator.h"
#include "util.h"

/*
 * Max height of the BTree. This is somewhat of an arbitrary limit to size
 * the maximum storage that can be tracked by a BTree. This constant affects
 * the size of the BTree depending on the key-size, fanout etc. For default
 * 4 KiB pages, with an avg row-size of ~512 bytes, we can store roughly
 * 6-7 rows / page; round it off to 8. With max of 8 levels, that's about
 * ( 8 ** 8) * 4KiB of storage ~= 64 GiB. This is expected to be plenty big.
 *
 * This limit is also related to the batching done by the mini-allocator.
 * Finally, this is limited for convenience to allow for static allocation
 * of some nested arrays sized by this value.
 */
#define BTREE_MAX_HEIGHT (8)

/*
 * Mini-allocator uses separate batches for each height of the BTree.
 * Therefore, the max # of mini-batches that the mini-allocator can track
 * is limited by the max height of the BTree.
 */
_Static_assert(BTREE_MAX_HEIGHT == MINI_MAX_BATCHES,
               "BTREE_MAX_HEIGHT has to be == MINI_MAX_BATCHES");

/*
 * Acceptable upper-bound on amount of space to waste when deciding whether
 * to do pre-emptive splits. Pre-emptive splitting is when we may split a
 * BTree child node in anticipation that a subsequent split of a grand-child
 * node may cause this child node to have to split. Pre-emptive splitting
 * requires that we leave enough free space in each child node for at least
 * one key + one pivot data. In such cases, we are willing to 'waste' this
 * much of space on the child node when splitting it.
 *
 * In other words, this limit anticpates that a split of a grand-child node
 * may result in an insertion of a key of this size to the child node. We,
 * therefore, may pre-emptively split the child to provision for this much of
 * available space to absorb inserts from the split of a grand-child.
 *
 * (This limit is indirectly 'disk-resident' as it affects the node's layout.)
 */
#define MAX_INLINE_KEY_SIZE(page_size) ((page_size) / 8) // Bytes

/*
 * Size of messages are limited so that a single split will always enable an
 * index insertion to succeed.
 */
#define MAX_INLINE_MESSAGE_SIZE(page_size) (35 * (page_size) / 100) // Bytes

/*
 * Used in-memory to allocate scratch buffer space for BTree splits &
 * defragmentation.
 */
#define MAX_PAGE_SIZE (1ULL << 16) // Bytes

/*
 *----------------------------------------------------------------------
 * Dynamic btree --
 *
 *       Each node in the btree is initially referred to with a
 * btree_node. This object abstracts away the packing of nodes
 * into pages. Afterwards, the node can be directly manipulated via the
 * btree_hdr.
 *----------------------------------------------------------------------
 */
typedef struct btree_config {
   cache_config *cache_cfg;
   data_config  *data_cfg;
   uint64        rough_count_height;
} btree_config;

typedef struct ONDISK btree_hdr btree_hdr;

typedef struct btree_node {
   uint64       addr;
   page_handle *page;
   btree_hdr   *hdr;
} btree_node;

typedef struct {
   char merged_data[MAX_INLINE_MESSAGE_SIZE(MAX_PAGE_SIZE)];
} scratch_btree_add_tuple;

typedef struct {
   char scratch_node[MAX_PAGE_SIZE];
} scratch_btree_defragment_node;

typedef struct { // Note: not a union
   scratch_btree_add_tuple       add_tuple;
   scratch_btree_defragment_node defragment_node;
} PLATFORM_CACHELINE_ALIGNED btree_scratch;

/*
 * *************************************************************************
 * BTree pivot data: Disk-resident structure
 *
 * Metadata for a pivot of an internal BTree node. Returned from an iterator
 * of height > 0 in order to track amount of data stored in sub-trees, given
 * by stuff like # of key/value pairs, # of bytes stored in the tree.
 *
 * Iterators at (height > 0) return this struct as a value for each pivot.
 * *************************************************************************
 */
typedef struct ONDISK btree_pivot_stats {
   uint32 num_kvs;
   uint32 key_bytes;
   uint32 message_bytes;
} btree_pivot_stats;

typedef struct ONDISK btree_pivot_data {
   uint64            child_addr;
   btree_pivot_stats stats;
} btree_pivot_data;

/*
 * A BTree iterator:
 */
typedef struct btree_iterator {
   iterator      super;
   cache        *cc;
   btree_config *cfg;
   bool32        do_prefetch;
   uint32        height;
   page_type     page_type;
   key           min_key;
   key           max_key;

   uint64     root_addr;
   btree_node curr;
   int64      idx;
   int64      curr_min_idx;
   uint64     end_addr;
   uint64     end_idx;
   uint64     end_generation;
} btree_iterator;

typedef struct btree_pack_req {
   // inputs to the pack
   cache        *cc;
   btree_config *cfg;
   iterator     *itor; // the itor which is being packed
   uint64        max_tuples;
   hash_fn       hash; // hash function used for calculating filter_hash
   unsigned int  seed; // seed used for calculating filter_hash
   uint32       *fingerprint_arr; // IN/OUT: hashes of the keys in the tree

   // internal data
   uint16            height;
   btree_node        edge[BTREE_MAX_HEIGHT][MAX_PAGES_PER_EXTENT];
   btree_pivot_stats edge_stats[BTREE_MAX_HEIGHT][MAX_PAGES_PER_EXTENT];
   uint32            num_edges[BTREE_MAX_HEIGHT];

   mini_allocator mini;

   // output of the compaction
   uint64 root_addr;     // root address of the output tree
   uint64 num_tuples;    // no. of tuples in the output tree
   uint64 key_bytes;     // total size of keys in tuples of the output tree
   uint64 message_bytes; // total size of msgs in tuples of the output tree
} btree_pack_req;

struct btree_async_ctxt;
typedef void (*btree_async_cb)(struct btree_async_ctxt *ctxt);

// States for the btree async lookup.
typedef enum {
   btree_async_state_invalid = 0,
   btree_async_state_start,
   btree_async_state_get_node, // re-entrant state
   btree_async_state_get_index_complete,
   btree_async_state_get_leaf_complete
} btree_async_state;

// Context of a bree async lookup request
typedef struct btree_async_ctxt {
   /*
    * When async lookup returns async_io_started, it uses this callback to
    * inform the upper layer that the page needed by async btree lookup
    * has been loaded into the cache, and the upper layer should re-enqueue
    * the async btree lookup for dispatch.
    */
   btree_async_cb cb;
   // Internal fields
   cache_async_ctxt *cache_ctxt; // cache ctxt for async get
   btree_async_state prev_state; // Previous state
   btree_async_state state;      // Current state
   bool32            was_async;  // Was the last cache_get async ?
   btree_node        node;       // Current node
   uint64            child_addr; // Child disk address
} btree_async_ctxt;

platform_status
btree_insert(cache              *cc,         // IN
             const btree_config *cfg,        // IN
             platform_heap_id    heap_id,    // IN
             btree_scratch      *scratch,    // IN
             uint64              root_addr,  // IN
             mini_allocator     *mini,       // IN
             key                 tuple_key,  // IN
             message             data,       // IN
             uint64             *generation, // OUT
             bool32             *was_unique);            // OUT

/*
 *-----------------------------------------------------------------------------
 * btree_ctxt_init --
 *
 *      Initialize the async context used by an async btree lookup request.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static inline void
btree_ctxt_init(btree_async_ctxt *ctxt,       // OUT
                cache_async_ctxt *cache_ctxt, // IN
                btree_async_cb    cb)            // IN
{
   ctxt->state      = btree_async_state_start;
   ctxt->cb         = cb;
   ctxt->cache_ctxt = cache_ctxt;
}

uint64
btree_create(cache              *cc,
             const btree_config *cfg,
             mini_allocator     *mini,
             page_type           type);

void
btree_inc_ref_range(cache              *cc,
                    const btree_config *cfg,
                    uint64              root_addr,
                    key                 start_key,
                    key                 end_key);

bool32
btree_dec_ref_range(cache              *cc,
                    const btree_config *cfg,
                    uint64              root_addr,
                    key                 start_key,
                    key                 end_key);

bool32
btree_dec_ref(cache              *cc,
              const btree_config *cfg,
              uint64              root_addr,
              page_type           type);

void
btree_block_dec_ref(cache *cc, btree_config *cfg, uint64 root_addr);

void
btree_unblock_dec_ref(cache *cc, btree_config *cfg, uint64 root_addr);

void
btree_node_unget(cache *cc, const btree_config *cfg, btree_node *node);
platform_status
btree_lookup(cache             *cc,
             btree_config      *cfg,
             uint64             root_addr,
             page_type          type,
             key                target,
             merge_accumulator *result);

static inline bool32
btree_found(merge_accumulator *result)
{
   return !merge_accumulator_is_null(result);
}

platform_status
btree_lookup_and_merge(cache             *cc,
                       btree_config      *cfg,
                       uint64             root_addr,
                       page_type          type,
                       key                target,
                       merge_accumulator *data,
                       bool32            *local_found);

cache_async_result
btree_lookup_async(cache             *cc,
                   btree_config      *cfg,
                   uint64             root_addr,
                   key                target,
                   merge_accumulator *result,
                   btree_async_ctxt  *ctxt);

cache_async_result
btree_lookup_and_merge_async(cache             *cc,          // IN
                             btree_config      *cfg,         // IN
                             uint64             root_addr,   // IN
                             key                target,      // IN
                             merge_accumulator *data,        // OUT
                             bool32            *local_found, // OUT
                             btree_async_ctxt  *ctxt);        // IN

void
btree_iterator_init(cache          *cc,
                    btree_config   *cfg,
                    btree_iterator *itor,
                    uint64          root_addr,
                    page_type       page_type,
                    key             min_key,
                    key             max_key,
                    key             start_key,
                    comparison      start_type,
                    bool32          do_prefetch,
                    uint32          height);

void
btree_iterator_deinit(btree_iterator *itor);

static inline void
btree_pack_req_init(btree_pack_req  *req,
                    cache           *cc,
                    btree_config    *cfg,
                    iterator        *itor,
                    uint64           max_tuples,
                    hash_fn          hash,
                    unsigned int     seed,
                    platform_heap_id hid)
{
   memset(req, 0, sizeof(*req));
   req->cc         = cc;
   req->cfg        = cfg;
   req->itor       = itor;
   req->max_tuples = max_tuples;
   req->hash       = hash;
   req->seed       = seed;
   if (hash != NULL && max_tuples > 0) {
      req->fingerprint_arr =
         TYPED_ARRAY_MALLOC(hid, req->fingerprint_arr, max_tuples);
   }
}

static inline void
btree_pack_req_deinit(btree_pack_req *req, platform_heap_id hid)
{
   if (req->fingerprint_arr) {
      platform_free(hid, req->fingerprint_arr);
   }
}

platform_status
btree_pack(btree_pack_req *req);

void
btree_count_in_range(cache             *cc,
                     btree_config      *cfg,
                     uint64             root_addr,
                     key                min_key,
                     key                max_key,
                     btree_pivot_stats *stats);

void
btree_count_in_range_by_iterator(cache             *cc,
                                 btree_config      *cfg,
                                 uint64             root_addr,
                                 key                min_key,
                                 key                max_key,
                                 btree_pivot_stats *stats);

uint64
btree_rough_count(cache        *cc,
                  btree_config *cfg,
                  uint64        root_addr,
                  key           min_key,
                  key           max_key);

void
btree_print_memtable_tree(platform_log_handle *log_handle,
                          cache               *cc,
                          btree_config        *cfg,
                          uint64               addr);

void
btree_print_tree(platform_log_handle *log_handle,
                 cache               *cc,
                 btree_config        *cfg,
                 uint64               addr,
                 page_type            type);

void
btree_print_locked_node(platform_log_handle *log_handle,
                        btree_config        *cfg,
                        uint64               addr,
                        btree_hdr           *hdr,
                        page_type            type);

void
btree_print_node(platform_log_handle *log_handle,
                 cache               *cc,
                 btree_config        *cfg,
                 btree_node          *node,
                 page_type            type);

void
btree_print_tree_stats(platform_log_handle *log_handle,
                       cache               *cc,
                       btree_config        *cfg,
                       uint64               addr);

void
btree_print_lookup(cache        *cc,
                   btree_config *cfg,
                   uint64        root_addr,
                   page_type     type,
                   key           target);

bool32
btree_verify_tree(cache *cc, btree_config *cfg, uint64 addr, page_type type);

uint64
btree_extent_count(cache *cc, btree_config *cfg, uint64 root_addr);

uint64
btree_space_use_in_range(cache        *cc,
                         btree_config *cfg,
                         uint64        root_addr,
                         page_type     type,
                         key           start_key,
                         key           end_key);

void
btree_config_init(btree_config *btree_cfg,
                  cache_config *cache_cfg,
                  data_config  *data_cfg,
                  uint64        rough_count_height);

// robj: I propose making all the following functions private to
// btree.c

static inline int
btree_key_compare(const btree_config *cfg, key key1, key key2)
{
   return data_key_compare(cfg->data_cfg, key1, key2);
}

static inline void
btree_key_to_string(btree_config *cfg, key k, char str[static 128])
{
   return data_key_to_string(cfg->data_cfg, k, str, 128);
}

static inline void
btree_message_to_string(btree_config *cfg, message data, char str[static 128])
{
   return data_message_to_string(cfg->data_cfg, data, str, 128);
}
