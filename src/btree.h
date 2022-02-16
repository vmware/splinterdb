// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * btree.h --
 *
 *     This file contains the public interfaces for dynamic b-trees/memtables.
 */

#ifndef __BTREE_H__
#define __BTREE_H__

#include "mini_allocator.h"
#include "iterator.h"
#include "util.h"

#define VARIABLE_LENGTH_BTREE_MAX_HEIGHT (8)
#define MAX_INLINE_KEY_SIZE              (512)
#define MAX_INLINE_MESSAGE_SIZE          (2048)
#define MAX_NODE_SIZE                    (1ULL << 16)

extern char         trace_key[24];
extern page_handle *trace_page;


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

typedef struct PACKED btree_hdr btree_hdr;

typedef struct btree_node {
   uint64       addr;
   page_handle *page;
   btree_hdr   *hdr;
} btree_node;

typedef struct {
   char merged_data[MAX_INLINE_MESSAGE_SIZE];
} scratch_btree_add_tuple;

typedef struct {
   char scratch_node[MAX_NODE_SIZE];
} scratch_btree_defragment_node;

typedef struct { // Note: not a union
   scratch_btree_add_tuple       add_tuple;
   scratch_btree_defragment_node defragment_node;
} PLATFORM_CACHELINE_ALIGNED btree_scratch;

typedef struct PACKED btree_pivot_data {
   uint64 child_addr;
   uint32 num_kvs_in_tree;
   uint32 key_bytes_in_tree;
   uint32 message_bytes_in_tree;
} btree_pivot_data;

typedef struct btree_iterator {
   iterator      super;
   cache        *cc;
   btree_config *cfg;
   bool          do_prefetch;
   uint32        height;
   page_type     page_type;
   slice         min_key;
   slice         max_key;

   uint64     root_addr;
   btree_node curr;
   uint64     idx;
   uint64     end_addr;
   uint64     end_idx;
   uint64     end_generation;

   // Variables used for debug only
   debug_code(bool debug_is_packed);
   debug_code(char debug_prev_key[MAX_KEY_SIZE]);
   debug_code(char debug_prev_end_key[MAX_KEY_SIZE]);
} btree_iterator;

typedef struct btree_pack_req {
   // inputs to the pack
   cache        *cc;
   btree_config *cfg;
   iterator     *itor;       // the itor which is being packed
   uint64        max_tuples; // max tuples for the tree
   hash_fn       hash;       // hash function used for calculating filter_hash
   unsigned int  seed;       // seed used for calculating filter_hash

   // internal data
   uint64         next_extent;
   uint16         height;
   btree_node     edge[VARIABLE_LENGTH_BTREE_MAX_HEIGHT];
   mini_allocator mini;

   // output of the compaction
   uint64  root_addr;       // root address of the output tree
   uint64  num_tuples;      // no. of tuples in the output tree
   uint64  key_bytes;       // total size of keys in tuples of the output tree
   uint64  message_bytes;   // total size of msgs in tuples of the output tree
   uint32 *fingerprint_arr; // hashes of the keys in the tree
} btree_pack_req;

struct btree_async_ctxt;
typedef void (*btree_async_cb)(struct btree_async_ctxt *ctxt);

// States for the btree async lookup.
typedef enum {
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
   bool              was_async;  // Was the last cache_get async ?
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
             slice               key,        // IN
             slice               data,       // IN
             uint64             *generation, // OUT
             bool               *was_unique);              // OUT

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
                    const slice         start_key,
                    const slice         end_key);

bool
btree_dec_ref_range(cache              *cc,
                    const btree_config *cfg,
                    uint64              root_addr,
                    const slice         start_key,
                    const slice         end_key,
                    page_type           type);

bool
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
btree_lookup(cache           *cc,
             btree_config    *cfg,
             uint64           root_addr,
             page_type        type,
             slice            key,
             writable_buffer *result);

static inline bool
btree_found(writable_buffer *result)
{
   return !writable_buffer_is_null(result);
}

platform_status
btree_lookup_and_merge(cache           *cc,
                       btree_config    *cfg,
                       uint64           root_addr,
                       page_type        type,
                       slice            key,
                       writable_buffer *data,
                       bool            *local_found);

cache_async_result
btree_lookup_async(cache            *cc,
                   btree_config     *cfg,
                   uint64            root_addr,
                   slice             key,
                   writable_buffer  *result,
                   btree_async_ctxt *ctxt);

cache_async_result
btree_lookup_and_merge_async(cache            *cc,          // IN
                             btree_config     *cfg,         // IN
                             uint64            root_addr,   // IN
                             const slice       key,         // IN
                             writable_buffer  *data,        // OUT
                             bool             *local_found, // OUT
                             btree_async_ctxt *ctxt);       // IN

void
btree_iterator_init(cache          *cc,
                    btree_config   *cfg,
                    btree_iterator *iterator,
                    uint64          root_addr,
                    page_type       page_type,
                    slice           min_key,
                    slice           max_key,
                    bool            do_prefetch,
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
btree_count_in_range(cache        *cc,
                     btree_config *cfg,
                     uint64        root_addr,
                     const slice   min_key,
                     const slice   max_key,
                     uint32       *kv_rank,
                     uint32       *key_bytes_rank,
                     uint32       *message_bytes_rank);

void
btree_count_in_range_by_iterator(cache        *cc,
                                 btree_config *cfg,
                                 uint64        root_addr,
                                 const slice   min_key,
                                 const slice   max_key,
                                 uint32       *kv_rank,
                                 uint32       *key_bytes_rank,
                                 uint32       *message_bytes_rank);

uint64
btree_rough_count(cache        *cc,
                  btree_config *cfg,
                  uint64        root_addr,
                  slice         min_key,
                  slice         max_key);

void
btree_print_tree(cache *cc, btree_config *cfg, uint64 addr);

void
btree_print_locked_node(btree_config          *cfg,
                        uint64                 addr,
                        btree_hdr             *hdr,
                        platform_stream_handle stream);

void
btree_print_node(cache                 *cc,
                 btree_config          *cfg,
                 btree_node            *node,
                 platform_stream_handle stream);

void
btree_print_tree_stats(cache *cc, btree_config *cfg, uint64 addr);

void
btree_print_lookup(cache        *cc,
                   btree_config *cfg,
                   uint64        root_addr,
                   page_type     type,
                   slice         key);

bool
btree_verify_tree(cache *cc, btree_config *cfg, uint64 addr, page_type type);

uint64
btree_extent_count(cache *cc, btree_config *cfg, uint64 root_addr);

uint64
btree_space_use_in_range(cache        *cc,
                         btree_config *cfg,
                         uint64        root_addr,
                         page_type     type,
                         slice         start_key,
                         slice         end_key);

void
btree_config_init(btree_config *btree_cfg,
                  cache_config *cache_cfg,
                  data_config  *data_cfg,
                  uint64        rough_count_height);

// robj: I propose making all the following functions private to
// btree.c

static inline char *
btree_min_key(btree_config *cfg)
{
   platform_assert(0); // Need to kill data_cfg->min_key
   return cfg->data_cfg->min_key;
}

static inline int
btree_key_compare(const btree_config *cfg, slice key1, slice key2)
{
   return data_key_compare(cfg->data_cfg, key1, key2);
}

static inline void
btree_key_to_string(btree_config *cfg, slice key, char str[static 128])
{
   return data_key_to_string(cfg->data_cfg, key, str, 128);
}

static inline void
btree_message_to_string(btree_config *cfg, slice data, char str[static 128])
{
   return data_message_to_string(cfg->data_cfg, data, str, 128);
}

#endif // __BTREE_H__
