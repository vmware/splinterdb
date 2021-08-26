// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * btree.h --
 *
 *     This file contains the interface for b-trees/packets.
 */

#ifndef __BTREE_H
#define __BTREE_H

#include "mini_allocator.h"
#include "iterator.h"
#include "util.h"

#define BTREE_MAX_HEIGHT 8
#define MAX_UNPACKED_IDX (1<<8)

extern char trace_key[24];
extern page_handle *trace_page;


/*
 *----------------------------------------------------------------------
 *
 * btree --
 *
 *       B-trees come in two flavors, dynamic and static.
 *
 *       Dynamic btrees must be referred to with a btree_dynamic_handle, which
 *       enables insertion
 *
 *       Static btrees can be referred to with just the root address.
 *
 *       Each node in the btree is initially referred to with a btree_node.
 *       This object abstracts away the packing of nodes into pages.
 *       Afterwards, the node can be directly manipulated via the btree_hdr.
 *
 *----------------------------------------------------------------------
 */

typedef struct btree_config {
   uint64       page_size;           // must match the cache/fs page_size
   uint64       extent_size;         // same

   uint16       tuples_per_leaf;     // must not overflow node_size
   uint16       tuples_per_packed_leaf;
   uint16       pivots_per_index;    // same
   uint16       pivots_per_packed_index;    // same
   uint64       rough_count_height;

   data_config *data_cfg;
} btree_config;

typedef struct PACKED btree_hdr btree_hdr;

typedef struct btree_node {
   uint64 addr;
   page_handle *page;
   btree_hdr *hdr;
} btree_node;

typedef struct {
   char old_data[MAX_MESSAGE_SIZE];
} scratch_btree_add_tuple;

typedef union {
   scratch_btree_add_tuple add_tuple;
} PLATFORM_CACHELINE_ALIGNED btree_scratch;

typedef struct btree_iterator {
   iterator super;
   cache *cc;
   btree_config *cfg;
   uint64 root_addr;
   btree_node curr, end;
   uint16 idx, end_idx;
   uint16 start_idx;
   uint64 start_addr;
   bool do_prefetch;
   bool is_live;
   uint32 height;
   page_type page_type;
   const char *min_key;
   const char *max_key;
   char *curr_key;
   char *curr_data;
   bool at_end;
   bool empty_itor;

   // Variables used for debug only
   debug_code(bool debug_is_packed);
   debug_code(char debug_prev_key[MAX_KEY_SIZE]);
   debug_code(char debug_prev_end_key[MAX_KEY_SIZE]);
} btree_iterator;

typedef struct btree_pack_req {
   // inputs to the pack
   cache        *cc;
   btree_config *cfg;

   // the itor which is being packed
   iterator     *itor;

   uint64        max_tuples;  // max tuples for the tree
   hash_fn       hash;        // hash function used for calculating filter_hash
   unsigned int  seed;        // seed used for calculating filter_hash

   // output of the compaction
   uint64        root_addr;   // root address of the output tree
   uint64        num_tuples;  // no. of tuples in the output tree
   uint32       *fingerprint_arr; // hashes of the keys in the tree
} btree_pack_req;

struct btree_async_ctxt;
typedef void (*btree_async_cb)(struct btree_async_ctxt *ctxt);

// States for the btree async lookup.
typedef enum {
   btree_async_state_start,
   btree_async_state_get_node,      // re-entrant state
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
   btree_async_cb          cb;
   // Internal fields
   cache_async_ctxt       *cache_ctxt;   // cache ctxt for async get
   btree_async_state       prev_state;   // Previous state
   btree_async_state       state;        // Current state
   bool                    was_async;    // Was the last cache_get async ?
   btree_node              node;         // Current node
   uint64                  child_addr;   // Child disk address
} btree_async_ctxt;

platform_status
btree_insert(cache          *cc,          // IN
             btree_config   *cfg,         // IN
             btree_scratch  *scratch,     // IN
             uint64          root_addr,   // IN
             mini_allocator *mini,        // IN
             const char     *key,         // IN
             const char     *data,        // IN
             uint64         *generation,  // OUT
             bool           *was_unique); // OUT

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
btree_ctxt_init(btree_async_ctxt *ctxt,       // OUT
                cache_async_ctxt *cache_ctxt, // IN
                btree_async_cb    cb)         // IN
{
   ctxt->state      = btree_async_state_start;
   ctxt->cb         = cb;
   ctxt->cache_ctxt = cache_ctxt;
}

void
btree_lookup_with_ref(cache         *cc,
                      btree_config  *cfg,
                      uint64         root_addr,
                      btree_node    *node,
                      page_type      type,
                      const char    *key,
                      char         **data,
                      bool          *found);

cache_async_result
btree_lookup_async_with_ref(cache *cc,
                            btree_config *cfg,
                            uint64 root_addr,
                            btree_node *node,
                            char *key,
                            char **data,
                            bool *found,
                            btree_async_ctxt *ctxt);

void
btree_node_unget(cache *cc,
                 btree_config *cfg,
                 btree_node *node);
void
btree_lookup(cache        *cc,
             btree_config *cfg,
             uint64        root_addr,
             const         char *key,
             char         *data,
             bool         *found);

cache_async_result
btree_lookup_async(cache *cc,
                   btree_config *cfg,
                   uint64 root_addr,
                   char *key,
                   char *data,
                   bool *found,
                   btree_async_ctxt *ctxt);

uint64
btree_init(cache          *cc,
           btree_config   *cfg,
           mini_allocator *mini,
           bool            is_packed);

bool
btree_zap_range(cache        *cc,
                btree_config *cfg,
                uint64        root_addr,
                const char   *start_key,
                const char   *end_key,
                page_type     type);

bool
btree_zap(cache        *cc,
          btree_config *cfg,
          uint64        root_addr,
          page_type     type);

bool
btree_should_zap_dec_ref(cache        *cc,
                         btree_config *cfg,
                         uint64        root_addr,
                         page_type     type);

void
btree_inc_range(cache        *cc,
                btree_config *cfg,
                uint64        root_addr,
                const char   *start_key,
                const char   *end_key);

page_handle *
btree_blind_inc(cache        *cc,
                btree_config *cfg,
                uint64        root_addr,
                page_type     type);

void
btree_blind_zap(cache        *cc,
                btree_config *cfg,
                page_handle  *meta_page,
                page_type     type);

void
btree_iterator_init(cache *         cc,
                    btree_config *  cfg,
                    btree_iterator *iterator,
                    uint64          root_addr,
                    page_type       page_type,
                    const char *    min_key,
                    const char *    max_key,
                    bool            do_prefetch,
                    bool            is_live,
                    uint32          height);

void
btree_iterator_deinit(btree_iterator *itor);

static inline void
btree_pack_req_init(btree_pack_req   *req,
                    cache            *cc,
                    btree_config     *cfg,
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
btree_pack_req_deinit(btree_pack_req *req, platform_heap_id hid)
{
   if (req->fingerprint_arr) {
      platform_free(hid, req->fingerprint_arr);
   }
}

platform_status
btree_pack(btree_pack_req *req);

uint64
btree_count_in_range(cache        *cc,
                     btree_config *cfg,
                     uint64        root_addr,
                     const char   *min_key,
                     const char   *max_key);

uint64
btree_count_in_range_by_iterator(cache        *cc,
                                 btree_config *cfg,
                                 uint64        root_addr,
                                 const char   *min_key,
                                 const char   *max_key);

uint64
btree_rough_count(cache        *cc,
                  btree_config *cfg,
                  uint64        root_addr,
                  char         *min_key,
                  char         *max_key);

void
btree_print_tree(cache *cc,
                 btree_config *cfg,
                 uint64 addr);

void
btree_print_locked_node(cache                 *cc,
                        btree_config          *cfg,
                        btree_node            *node,
                        platform_stream_handle stream);

void
btree_print_node(cache                 *cc,
                 btree_config          *cfg,
                 btree_node            *node,
                 platform_stream_handle stream);

void
btree_print_tree_stats(cache *cc,
                       btree_config *cfg,
                       uint64 addr);

void
btree_print_lookup(cache *cc,
                   btree_config *cfg,
                   uint64 root_addr,
                   page_type type,
                   char *key);

bool
btree_verify_tree(cache *cc,
                  btree_config *cfg,
                  uint64 addr,
                  page_type type);

uint64
btree_extent_count(cache        *cc,
                   btree_config *cfg,
                   uint64        root_addr);

uint64
btree_space_use_in_range(cache        *cc,
                         btree_config *cfg,
                         uint64        root_addr,
                         page_type     type,
                         const char   *start_key,
                         const char   *end_key);

void
btree_config_init(btree_config *btree_cfg,
                  data_config  *data_cfg,
                  uint64        rough_count_height,
                  uint64        page_size,
                  uint64        extent_size);

// robj: I propose making all the following functions private to btree.c

static inline uint64
btree_key_size(btree_config *cfg)
{
   return cfg->data_cfg->key_size;
}

static inline uint64
btree_message_size(btree_config *cfg)
{
   return cfg->data_cfg->message_size;
}

static inline uint64
btree_tuple_size(btree_config *cfg)
{
   return btree_key_size(cfg) + btree_message_size(cfg);
}

static inline char *
btree_min_key(btree_config *cfg)
{
   return cfg->data_cfg->min_key;
}

static inline int
btree_key_compare(btree_config *cfg,
                  const char   *key1,
                  const char   *key2)
{
  return data_key_compare(cfg->data_cfg, key1, key2);
}

static inline void
btree_key_to_string(btree_config *cfg,
                    const char *key,
                    char str[static 128])
{
  return data_key_to_string(cfg->data_cfg, key, str, 128);
}

static inline void
btree_message_to_string(btree_config *cfg,
                     const char *data,
                     char str[static 128])
{
  return data_message_to_string(cfg->data_cfg, data, str, 128);
}

#endif // __BTREE_H
