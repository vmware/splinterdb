/* **********************************************************
 * Copyright 2018-2020 VMware, Inc.  All rights reserved. -- VMware Confidential
 * **********************************************************/

/*
 * dynamic_btree.h --
 *
 *     This file contains the interface for dynamic b-trees/memtables.
 */

#ifndef __BTREE_H
#define __BTREE_H

#include "mini_allocator.h"
#include "iterator.h"
#include "util.h"

#define MAX_INLINE_MESSAGE_SIZE (2048)
#define MAX_NODE_SIZE (1ULL << 16)

extern char trace_key[24];
extern page_handle *trace_page;


/*
 *----------------------------------------------------------------------
 *
 * dynamic btree --
 *
 *       Each node in the btree is initially referred to with a dynamic_btree_node.
 *       This object abstracts away the packing of nodes into pages.
 *       Afterwards, the node can be directly manipulated via the dynamic_btree_hdr.
 *
 *----------------------------------------------------------------------
 */

typedef struct dynamic_btree_config {
   uint64       page_size;           // must match the cache/fs page_size
   uint64       extent_size;         // same
   uint64       rough_count_height;
   data_config *data_cfg;
} dynamic_btree_config;

typedef struct PACKED dynamic_btree_hdr dynamic_btree_hdr;

typedef struct dynamic_btree_node {
   uint64 addr;
   page_handle *page;
   dynamic_btree_hdr *hdr;
} dynamic_btree_node;

typedef struct {
   char merged_data[MAX_INLINE_MESSAGE_SIZE];
} scratch_dynamic_btree_add_tuple;

typedef struct {
   char scratch_node[MAX_NODE_SIZE];
} scratch_dynamic_btree_defragment_node;

typedef union {
   scratch_dynamic_btree_add_tuple add_tuple;
   scratch_dynamic_btree_defragment_node defragment_node;
} PLATFORM_CACHELINE_ALIGNED dynamic_btree_scratch;

typedef struct dynamic_btree_iterator {
   iterator              super;
   cache                *cc;
   dynamic_btree_config *cfg;
   uint64                root_addr;
   dynamic_btree_node    curr, end;
   uint16                idx, end_idx;
   uint16                start_idx;
   uint64                start_addr;
   bool                  do_prefetch;
   bool                  is_live;
   uint32                height;
   page_type             page_type;
   slice                 min_key;
   slice                 max_key;
   slice                 curr_key;
   slice                 curr_data;
   bool                  at_end;
   bool                  empty_itor;

   // Variables used for debug only
   debug_code(bool debug_is_packed);
   debug_code(char debug_prev_key[MAX_KEY_SIZE]);
   debug_code(char debug_prev_end_key[MAX_KEY_SIZE]);
} dynamic_btree_iterator;

typedef struct dynamic_btree_pack_req {
   // inputs to the pack
   cache        *cc;
   dynamic_btree_config *cfg;

   // the itor which is being packed
   iterator     *itor;

   uint64        max_tuples;  // max tuples for the tree
   hash_fn       hash;        // hash function used for calculating filter_hash
   unsigned int  seed;        // seed used for calculating filter_hash

   // output of the compaction
   uint64        root_addr;   // root address of the output tree
   uint64        num_tuples;  // no. of tuples in the output tree
   uint32       *fingerprint_arr; // hashes of the keys in the tree
} dynamic_btree_pack_req;

typedef struct dynamic_branch_pack_req {
   dynamic_btree_pack_req point_req;
   dynamic_btree_pack_req range_req;
} dynamic_branch_pack_req;

struct dynamic_btree_async_ctxt;
typedef void (*dynamic_btree_async_cb)(struct dynamic_btree_async_ctxt *ctxt);

// States for the btree async lookup.
typedef enum {
   dynamic_btree_async_state_start,
   dynamic_btree_async_state_get_node,      // re-entrant state
   dynamic_btree_async_state_get_index_complete,
   dynamic_btree_async_state_get_leaf_complete
} dynamic_btree_async_state;

// Context of a bree async lookup request
typedef struct dynamic_btree_async_ctxt {
   /*
    * When async lookup returns async_io_started, it uses this callback to
    * inform the upper layer that the page needed by async btree lookup
    * has been loaded into the cache, and the upper layer should re-enqueue
    * the async btree lookup for dispatch.
    */
   dynamic_btree_async_cb     cb;
   // Internal fields
   cache_async_ctxt          *cache_ctxt; // cache ctxt for async get
   dynamic_btree_async_state  prev_state; // Previous state
   dynamic_btree_async_state  state; // Current state
   bool                       was_async; // Was the last cache_get async ?
   dynamic_btree_node         node; // Current node
   uint64                     child_addr; // Child disk address
} dynamic_btree_async_ctxt;

platform_status
dynamic_btree_insert(cache              *cc, // IN
             const dynamic_btree_config *cfg, // IN
             dynamic_btree_scratch      *scratch, // IN
             uint64                      root_addr, // IN
             mini_allocator             *mini, // IN
             slice                       key, // IN
             slice                       data, // IN
             uint64                     *generation, // OUT
             bool                       *was_unique); // OUT

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
dynamic_btree_ctxt_init(dynamic_btree_async_ctxt *ctxt, // OUT
                cache_async_ctxt                 *cache_ctxt, // IN
                dynamic_btree_async_cb            cb) // IN
{
   ctxt->state      = dynamic_btree_async_state_start;
   ctxt->cb         = cb;
   ctxt->cache_ctxt = cache_ctxt;
}

uint64
dynamic_btree_init(cache              *cc,
           const dynamic_btree_config *cfg,
           mini_allocator             *mini,
           page_type                   type);

bool
dynamic_btree_should_zap_dec_ref(cache              *cc,
                         const dynamic_btree_config *cfg,
                         uint64              root_addr,
                         page_type           type);

void
dynamic_btree_inc_range(cache              *cc,
                const dynamic_btree_config *cfg,
                uint64                      root_addr,
                const slice                 start_key,
                const slice                 end_key);

bool
dynamic_btree_zap_range(cache              *cc,
                const dynamic_btree_config *cfg,
                uint64                      root_addr,
                const slice                 start_key,
                const slice                 end_key,
                page_type                   type);

bool
dynamic_btree_zap(cache              *cc,
          const dynamic_btree_config *cfg,
          uint64              root_addr,
          page_type           type);

page_handle *
dynamic_btree_blind_inc(cache        *cc,
                dynamic_btree_config *cfg,
                uint64        root_addr,
                page_type     type);

void
dynamic_btree_blind_zap(cache              *cc,
                const dynamic_btree_config *cfg,
                page_handle        *meta_page,
                page_type           type);

void
dynamic_btree_lookup_with_ref(cache        *cc,
                      dynamic_btree_config *cfg,
                      uint64                root_addr,
                      page_type             type,
                      slice                 key,
                      dynamic_btree_node   *node,
                      slice                *data,
                      bool                 *found);

cache_async_result
dynamic_btree_lookup_async_with_ref(cache                    *cc,
                                    dynamic_btree_config     *cfg,
                                    uint64                    root_addr,
                                    slice                     key,
                                    dynamic_btree_node       *node,
                                    slice                    *data,
                                    bool                     *found,
                                    dynamic_btree_async_ctxt *ctxt);

void
dynamic_btree_node_unget(cache              *cc,
                 const dynamic_btree_config *cfg,
                 dynamic_btree_node         *node);
void
dynamic_btree_lookup(cache        *cc,
             dynamic_btree_config *cfg,
             uint64        root_addr,
             slice        key,
             slice       *data,
             bool         *found);

cache_async_result
dynamic_btree_lookup_async(cache            *cc,
                   dynamic_btree_config     *cfg,
                   uint64            root_addr,
                   slice            key,
                   slice           *data,
                   bool             *found,
                   dynamic_btree_async_ctxt *ctxt);

void
dynamic_btree_iterator_init(cache          *cc,
                    dynamic_btree_config   *cfg,
                    dynamic_btree_iterator *iterator,
                    uint64          root_addr,
                    page_type       page_type,
                    slice          min_key,
                    slice          max_key,
                    bool            do_prefetch,
                    bool            is_live,
                    uint32          height);

void
dynamic_btree_iterator_deinit(dynamic_btree_iterator *itor);

static inline void
dynamic_btree_pack_req_init(dynamic_btree_pack_req   *req,
                    cache            *cc,
                    dynamic_btree_config     *cfg,
                    iterator         *itor,
                    uint64            max_tuples,
                    hash_fn           hash,
                    unsigned int      seed,
                    platform_heap_id  hid)
{
   memset(req, 0, sizeof(*req));
   req->cc = cc;
   req->cfg = cfg;
   req->itor = itor;
   req->max_tuples = max_tuples;
   req->hash = hash;
   req->seed = seed;
   if (hash != NULL && max_tuples > 0) {
      req->fingerprint_arr = TYPED_ARRAY_MALLOC(hid, req->fingerprint_arr,
                                                max_tuples);
   }
}

static inline void
dynamic_btree_pack_req_deinit(dynamic_btree_pack_req *req, platform_heap_id hid)
{
   if (req->fingerprint_arr) {
      platform_free(hid, req->fingerprint_arr);
   }
}

platform_status
dynamic_btree_pack(dynamic_btree_pack_req *req);

platform_status
dynamic_branch_pack(platform_heap_id hid, dynamic_branch_pack_req *req);

uint64
dynamic_btree_count_in_range(cache        *cc,
                     dynamic_btree_config *cfg,
                     uint64        root_addr,
                     slice        min_key,
                     slice        max_key);

uint64
dynamic_btree_count_in_range_by_iterator(cache        *cc,
                                 dynamic_btree_config *cfg,
                                 uint64        root_addr,
                                 slice        min_key,
                                 slice        max_key);

uint64
dynamic_btree_rough_count(cache        *cc,
                  dynamic_btree_config *cfg,
                  uint64        root_addr,
                  slice        min_key,
                  slice        max_key);

void
dynamic_btree_print_tree(cache *cc,
                 dynamic_btree_config *cfg,
                 uint64 addr);

void
dynamic_btree_print_locked_node(cache                 *cc,
                        dynamic_btree_config          *cfg,
                        dynamic_btree_node            *node,
                        platform_stream_handle stream);

void
dynamic_btree_print_node(cache                 *cc,
                 dynamic_btree_config          *cfg,
                 dynamic_btree_node            *node,
                 platform_stream_handle stream);

void
dynamic_btree_print_tree_stats(cache *cc,
                       dynamic_btree_config *cfg,
                       uint64 addr);

void
dynamic_btree_print_lookup(cache        *cc,
                   dynamic_btree_config *cfg,
                   uint64        root_addr,
                   page_type     type,
                   slice        key);

bool
dynamic_btree_verify_tree(cache *cc,
                  dynamic_btree_config *cfg,
                  uint64 addr,
                  page_type type);

uint64
dynamic_btree_extent_count(cache        *cc,
                   dynamic_btree_config *cfg,
                   uint64        root_addr);

uint64
dynamic_btree_space_use_in_range(cache        *cc,
                         dynamic_btree_config *cfg,
                         uint64        root_addr,
                         page_type     type,
                         slice        start_key,
                         slice        end_key);

void
dynamic_btree_config_init(dynamic_btree_config *dynamic_btree_cfg,
                  data_config  *data_cfg,
                  uint64        rough_count_height,
                  uint64        page_size,
                  uint64        extent_size);

// robj: I propose making all the following functions private to dynamic_btree.c

static inline char *
dynamic_btree_min_key(dynamic_btree_config *cfg)
{
   platform_assert(0); // Need to kill data_cfg->min_key
   return cfg->data_cfg->min_key;
}

static inline int
dynamic_btree_key_compare(const dynamic_btree_config *cfg,
                  slice              key1,
                  slice              key2)
{
  platform_assert(0); // Need to update data interface to handle slices
  //return data_key_compare(cfg->data_cfg, key1, key2);
}

static inline void
dynamic_btree_key_to_string(dynamic_btree_config *cfg,
                    slice        key,
                    char          str[static 128])
{
  platform_assert(0); // Need to update data interface to handle slices
  //return data_key_to_string(cfg->data_cfg, key, str, 128);
}

static inline void
dynamic_btree_message_to_string(dynamic_btree_config *cfg,
                     slice           data,
                     char             str[static 128])
{
  platform_assert(0); // Need to update data interface to handle slices
  //return data_message_to_string(cfg->data_cfg, data, str, 128);
}

#endif // __DYNAMIC_BTREE_H
