// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk_node.c --
 *
 *     This file contains the implementation SplinterDB trunk nodes.
 */

#include "trunk_node.h"
#include "poison.h"


typedef struct ONDISK branch_ref {
   uint64 addr;
} branch_ref;

typedef struct ONDISK maplet_ref {
   uint64 addr;
} maplet_ref;

/*
 * Bundles are used to represent groups of branches that have not yet
 * been incorporated into the per-pivot filters.
 */
typedef enum bundle_state {
   BUNDLE_STATE_ROUTED,
   BUNDLE_STATE_COMPACTED
} bundle_state;

typedef struct ONDISK routed_bundle {
   maplet_ref maplet;
   uint16     num_branches;
   branch_ref branches[];
} routed_bundle;

/*
 * In a compacted bundle, there is one branch per child of the node.
 * Furthermore, all the maplets should be treated as simply filters.
 */
typedef struct ONDISK compacted_bundle {
   uint64     num_maplets;
   maplet_ref maplets[];
} compacted_bundle;

typedef struct ONDISK inflight_bundle {
   bundle_state state;
   union {
      routed_bundle    ubundle;
      compacted_bundle cbundle;
   } u;
} inflight_bundle;

typedef struct ONDISK pivot {
   uint64     child_addr;
   uint64     inflight_bundle_start;
   ondisk_key key;
} pivot;

#if 0

/*
 * Node layout:
 * - header
 * - pivot offsets table (array at end of header struct)
 * - pivots (note each pivot is variable size due to the key)
 * - whole branch array
 * - bundles
 */
typedef struct ONDISK node_hdr {
   uint16 height;
   uint64 num_whole_branches;
   uint64 next_bundle_offset;
   uint64 num_pivots;
   uint64 num_pages;
   uint64 page_addrs[];
} node_hdr;

/*
 * Basic accessor functions
 */

static inline uint64
sizeof_pivot(const pivot *pvt)
{
   return sizeof(pivot) + sizeof_ondisk_key_data(&pvt->key);
}

static inline uint64
pivot_size(key pivot_key)
{
   return sizeof(pivot) + ondisk_key_required_data_capacity(pivot_key);
}

static inline const const pivot *
get_pivot(const node_hdr *hdr, uint64 i)
{
   debug_assert(i < hdr->num_pivots);
   return (const pivot *)(((const char *)hdr) + hdr->pivot_offsets[i]);
}

static inline const branch_ref *
get_whole_branch_table(const node_hdr *hdr)
{
   const pivot *last_pivot = get_pivot(hdr, hdr->num_pivots - 1);
   return (const branch_ref *)(((const char *)last_pivot)
                               + sizeof_pivot(last_pivot));
}

static inline branch_ref
get_whole_branch(const node_hdr *hdr, uint64 i)
{
   const branch_ref *table = get_whole_branch_table(hdr);
   debug_assert(i < hdr->num_whole_branches);
   return table[i];
}

static inline uint64
sizeof_bundle(const bundle *bndl)
{
   return sizeof(bundle) + bndl->num_branches * sizeof(branch_ref);
}

static inline uint64
bundle_size(uint64 num_branches)
{
   return sizeof(bundle) + num_branches * sizeof(branch_ref);
}

static inline const bundle *
first_bundle(const node_hdr *hdr)
{
   const branch_ref *table = get_whole_branch_table(hdr);
   return (const bundle *)&table[hdr->num_whole_branches];
}

static inline const bundle *
bundle_by_offset(const node_hdr *hdr, uint64 offset)
{
   return (const bundle *)(((const char *)hdr) + offset);
}

static inline const const bundle *
next_bundle(const bundle *bndl)
{
   return (const bundle *)(((const char *)bndl) + sizeof_bundle(bndl));
}

static inline bool32
is_valid_bundle(const node_hdr *hdr, uint64 page_size, const bundle *bndl)
{
   uint64 bndl_offset = ((char *)bndl) - ((char *)hdr);
   return bndl_offset < hdr->next_bundle_offset
          && bndl_offset + sizeof_bundle(bndl) <= page_size;
}

/*
 * Some simple constructors
 */

static inline void
init_branch_ref(branch_ref *branch, uint64 addr)
{
   branch->addr = addr;
}

static inline void
init_maplet_ref(maplet_ref *maplet, uint64 addr)
{
   maplet->addr = addr;
}

/*
 * Bundle operations
 */

static inline bool32
append_singleton_bundle(node_hdr *hdr,
                        uint64    page_size,
                        uint64    branch_addr,
                        uint64    maplet_addr)
{
   if (hdr->next_bundle_offset + bundle_size(1) <= page_size) {
      bundle *dest = (bundle *)bundle_by_offset(hdr, hdr->next_bundle_offset);
      init_maplet_ref(&dest->maplet, maplet_addr);
      init_branch_ref(&dest->branches[0], branch_addr);
      dest->num_branches = 1;
      return TRUE;
   }
   return FALSE;
}

static inline bool32
append_bundle(node_hdr *hdr, uint64 page_size, const bundle *src)
{
   if (hdr->next_bundle_offset + sizeof_bundle(src) <= page_size) {
      bundle *dest = (bundle *)bundle_by_offset(hdr, hdr->next_bundle_offset);
      memcpy(dest, src, sizeof_bundle(src));
      return TRUE;
   }
   return FALSE;
}

static inline void
convert_first_bundle_to_whole_branch(node_hdr *hdr, )
#endif
