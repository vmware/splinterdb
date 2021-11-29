/* **********************************************************
 * Copyright 2018-2020 VMware, Inc.  All rights reserved. -- VMware Confidential
 * **********************************************************/

/*
 * variable_length_btree.h --
 *
 *     This file contains the interface for dynamic b-trees/memtables.
 */

#ifndef __VARIABLE_LENGTH_BTREE_H
#define __VARIABLE_LENGTH_BTREE_H

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
 *
 * dynamic btree --
 *
 *       Each node in the btree is initially referred to with a
 *variable_length_btree_node. This object abstracts away the packing of nodes
 *into pages. Afterwards, the node can be directly manipulated via the
 *variable_length_btree_hdr.
 *
 *----------------------------------------------------------------------
 */

typedef struct variable_length_btree_config {
   uint64       page_size;   // must match the cache/fs page_size
   uint64       extent_size; // same
   uint64       rough_count_height;
   data_config *data_cfg;
} variable_length_btree_config;

typedef struct PACKED variable_length_btree_hdr variable_length_btree_hdr;

typedef struct variable_length_btree_node {
   uint64                     addr;
   page_handle *              page;
   variable_length_btree_hdr *hdr;
} variable_length_btree_node;

typedef struct {
   char merged_data[MAX_INLINE_MESSAGE_SIZE];
} scratch_variable_length_btree_add_tuple;

typedef struct {
   char scratch_node[MAX_NODE_SIZE];
} scratch_variable_length_btree_defragment_node;

typedef struct { // Note: not a union
   scratch_variable_length_btree_add_tuple       add_tuple;
   scratch_variable_length_btree_defragment_node defragment_node;
} PLATFORM_CACHELINE_ALIGNED variable_length_btree_scratch;

typedef struct variable_length_btree_iterator {
   iterator                      super;
   cache *                       cc;
   variable_length_btree_config *cfg;
   bool                          do_prefetch;
   uint32                        height;
   page_type                     page_type;
   slice                         max_key;

   uint64                     root_addr;
   variable_length_btree_node curr;
   uint64                     idx;
   uint64                     end_addr;
   uint64                     end_idx;
   uint64                     end_generation;

   // Variables used for debug only
   debug_code(bool debug_is_packed);
   debug_code(char debug_prev_key[MAX_KEY_SIZE]);
   debug_code(char debug_prev_end_key[MAX_KEY_SIZE]);
} variable_length_btree_iterator;

typedef struct variable_length_btree_pack_req {
   // inputs to the pack
   cache *                       cc;
   variable_length_btree_config *cfg;
   iterator *                    itor;       // the itor which is being packed
   uint64                        max_tuples; // max tuples for the tree
   hash_fn      hash; // hash function used for calculating filter_hash
   unsigned int seed; // seed used for calculating filter_hash

   // internal data
   uint64                     next_extent;
   uint16                     height;
   variable_length_btree_node edge[VARIABLE_LENGTH_BTREE_MAX_HEIGHT];
   mini_allocator             mini;

   // output of the compaction
   uint64  root_addr;       // root address of the output tree
   uint64  num_tuples;      // no. of tuples in the output tree
   uint64  key_bytes;       // total size of keys in tuples of the output tree
   uint64  message_bytes;   // total size of msgs in tuples of the output tree
   uint32 *fingerprint_arr; // hashes of the keys in the tree
} variable_length_btree_pack_req;

struct variable_length_btree_async_ctxt;
typedef void (*variable_length_btree_async_cb)(
   struct variable_length_btree_async_ctxt *ctxt);

// States for the btree async lookup.
typedef enum {
   variable_length_btree_async_state_start,
   variable_length_btree_async_state_get_node, // re-entrant state
   variable_length_btree_async_state_get_index_complete,
   variable_length_btree_async_state_get_leaf_complete
} variable_length_btree_async_state;

// Context of a bree async lookup request
typedef struct variable_length_btree_async_ctxt {
   /*
    * When async lookup returns async_io_started, it uses this callback to
    * inform the upper layer that the page needed by async btree lookup
    * has been loaded into the cache, and the upper layer should re-enqueue
    * the async btree lookup for dispatch.
    */
   variable_length_btree_async_cb cb;
   // Internal fields
   cache_async_ctxt *                cache_ctxt; // cache ctxt for async get
   variable_length_btree_async_state prev_state; // Previous state
   variable_length_btree_async_state state;      // Current state
   bool                       was_async;  // Was the last cache_get async ?
   variable_length_btree_node node;       // Current node
   uint64                     child_addr; // Child disk address
} variable_length_btree_async_ctxt;

platform_status
variable_length_btree_insert(cache *                             cc,      // IN
                             const variable_length_btree_config *cfg,     // IN
                             variable_length_btree_scratch *     scratch, // IN
                             uint64          root_addr,                   // IN
                             mini_allocator *mini,                        // IN
                             slice           key,                         // IN
                             slice           data,                        // IN
                             uint64 *        generation,                  // OUT
                             bool *          was_unique);                           // OUT

/*
 *-----------------------------------------------------------------------------
 *
 * btree_ctxt_init --
 *
 *      Initialize the async context used by an async btree lookup request.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

static inline void
variable_length_btree_ctxt_init(variable_length_btree_async_ctxt *ctxt, // OUT
                                cache_async_ctxt *             cache_ctxt, // IN
                                variable_length_btree_async_cb cb)         // IN
{
   ctxt->state      = variable_length_btree_async_state_start;
   ctxt->cb         = cb;
   ctxt->cache_ctxt = cache_ctxt;
}

uint64
variable_length_btree_init(cache *                             cc,
                           const variable_length_btree_config *cfg,
                           mini_allocator *                    mini,
                           page_type                           type);

bool
variable_length_btree_should_zap_dec_ref(
   cache *                             cc,
   const variable_length_btree_config *cfg,
   uint64                              root_addr,
   page_type                           type);

void
variable_length_btree_inc_range(cache *                             cc,
                                const variable_length_btree_config *cfg,
                                uint64                              root_addr,
                                const slice                         start_key,
                                const slice                         end_key);

bool
variable_length_btree_zap_range(cache *                             cc,
                                const variable_length_btree_config *cfg,
                                uint64                              root_addr,
                                const slice                         start_key,
                                const slice                         end_key,
                                page_type                           type);

bool
variable_length_btree_zap(cache *                             cc,
                          const variable_length_btree_config *cfg,
                          uint64                              root_addr,
                          page_type                           type);

page_handle *
variable_length_btree_blind_inc(cache *                       cc,
                                variable_length_btree_config *cfg,
                                uint64                        root_addr,
                                page_type                     type);

void
variable_length_btree_blind_zap(cache *                             cc,
                                const variable_length_btree_config *cfg,
                                page_handle *                       meta_page,
                                page_type                           type);

void
variable_length_btree_lookup_with_ref(cache *                       cc,
                                      variable_length_btree_config *cfg,
                                      uint64                        root_addr,
                                      page_type                     type,
                                      slice                         key,
                                      variable_length_btree_node *  node,
                                      slice *                       data,
                                      bool *                        found);

cache_async_result
variable_length_btree_lookup_async_with_ref(
   cache *                           cc,
   variable_length_btree_config *    cfg,
   uint64                            root_addr,
   slice                             key,
   variable_length_btree_node *      node,
   slice *                           data,
   bool *                            found,
   variable_length_btree_async_ctxt *ctxt);

void
variable_length_btree_node_unget(cache *                             cc,
                                 const variable_length_btree_config *cfg,
                                 variable_length_btree_node *        node);
void
variable_length_btree_lookup(cache *                       cc,
                             variable_length_btree_config *cfg,
                             uint64                        root_addr,
                             slice                         key,
                             uint64 *                      data_out_len,
                             void *                        data_out,
                             bool *                        found);

cache_async_result
variable_length_btree_lookup_async(cache *                       cc,
                                   variable_length_btree_config *cfg,
                                   uint64                        root_addr,
                                   slice                         key,
                                   uint64 *                      data_out_len,
                                   void *                        data_out,
                                   bool *                        found,
                                   variable_length_btree_async_ctxt *ctxt);

void
variable_length_btree_iterator_init(cache *                         cc,
                                    variable_length_btree_config *  cfg,
                                    variable_length_btree_iterator *iterator,
                                    uint64                          root_addr,
                                    page_type                       page_type,
                                    slice                           min_key,
                                    slice                           max_key,
                                    bool                            do_prefetch,
                                    uint32                          height);

void
variable_length_btree_iterator_deinit(variable_length_btree_iterator *itor);

static inline void
variable_length_btree_pack_req_init(variable_length_btree_pack_req *req,
                                    cache *                         cc,
                                    variable_length_btree_config *  cfg,
                                    iterator *                      itor,
                                    uint64                          max_tuples,
                                    hash_fn                         hash,
                                    unsigned int                    seed,
                                    platform_heap_id                hid)
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
variable_length_btree_pack_req_deinit(variable_length_btree_pack_req *req,
                                      platform_heap_id                hid)
{
   if (req->fingerprint_arr) {
      platform_free(hid, req->fingerprint_arr);
   }
}

platform_status
variable_length_btree_pack(variable_length_btree_pack_req *req);

void
variable_length_btree_count_in_range(cache *                       cc,
                                     variable_length_btree_config *cfg,
                                     uint64                        root_addr,
                                     const slice                   min_key,
                                     const slice                   max_key,
                                     uint32 *                      kv_rank,
                                     uint32 *key_bytes_rank,
                                     uint32 *message_bytes_rank);

void
variable_length_btree_count_in_range_by_iterator(
   cache *                       cc,
   variable_length_btree_config *cfg,
   uint64                        root_addr,
   const slice                   min_key,
   const slice                   max_key,
   uint32 *                      kv_rank,
   uint32 *                      key_bytes_rank,
   uint32 *                      message_bytes_rank);

uint64
variable_length_btree_rough_count(cache *                       cc,
                                  variable_length_btree_config *cfg,
                                  uint64                        root_addr,
                                  slice                         min_key,
                                  slice                         max_key);

void
variable_length_btree_print_tree(cache *                       cc,
                                 variable_length_btree_config *cfg,
                                 uint64                        addr);

void
variable_length_btree_print_locked_node(variable_length_btree_config *cfg,
                                        uint64                        addr,
                                        variable_length_btree_hdr *   hdr,
                                        platform_stream_handle        stream);

void
variable_length_btree_print_node(cache *                       cc,
                                 variable_length_btree_config *cfg,
                                 variable_length_btree_node *  node,
                                 platform_stream_handle        stream);

void
variable_length_btree_print_tree_stats(cache *                       cc,
                                       variable_length_btree_config *cfg,
                                       uint64                        addr);

void
variable_length_btree_print_lookup(cache *                       cc,
                                   variable_length_btree_config *cfg,
                                   uint64                        root_addr,
                                   page_type                     type,
                                   slice                         key);

bool
variable_length_btree_verify_tree(cache *                       cc,
                                  variable_length_btree_config *cfg,
                                  uint64                        addr,
                                  page_type                     type);

uint64
variable_length_btree_extent_count(cache *                       cc,
                                   variable_length_btree_config *cfg,
                                   uint64                        root_addr);

uint64
variable_length_btree_space_use_in_range(cache *                       cc,
                                         variable_length_btree_config *cfg,
                                         uint64    root_addr,
                                         page_type type,
                                         slice     start_key,
                                         slice     end_key);

void
variable_length_btree_config_init(
   variable_length_btree_config *variable_length_btree_cfg,
   data_config *                 data_cfg,
   uint64                        rough_count_height,
   uint64                        page_size,
   uint64                        extent_size);

// robj: I propose making all the following functions private to
// variable_length_btree.c

static inline char *
variable_length_btree_min_key(variable_length_btree_config *cfg)
{
   platform_assert(0); // Need to kill data_cfg->min_key
   return cfg->data_cfg->min_key;
}

static inline int
variable_length_btree_key_compare(const variable_length_btree_config *cfg,
                                  slice                               key1,
                                  slice                               key2)
{
   return data_key_compare(cfg->data_cfg, key1, key2);
}

static inline void
variable_length_btree_key_to_string(variable_length_btree_config *cfg,
                                    slice                         key,
                                    char str[static 128])
{
   return data_key_to_string(cfg->data_cfg, key, str, 128);
}

static inline void
variable_length_btree_message_to_string(variable_length_btree_config *cfg,
                                        slice                         data,
                                        char str[static 128])
{
   return data_message_to_string(cfg->data_cfg, data, str, 128);
}

#endif // __VARIABLE_LENGTH_BTREE_H
