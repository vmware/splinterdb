// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/splinterdb.h"
#include "data_internal.h"
#include "vector.h"

typedef struct iterator iterator;

typedef void (*iterator_curr_fn)(iterator *itor, key *curr_key, message *msg);
typedef bool32 (*iterator_bound_fn)(iterator *itor);
typedef platform_status (*iterator_step_fn)(iterator *itor);
typedef platform_status (*iterator_seek_fn)(iterator  *itor,
                                            comparison seek_type,
                                            key        seek_key);
typedef void (*iterator_print_fn)(iterator *itor);

/*
 * Initialization and the positioning operations (next, prev, and seek) must
 * perform all work that can fail before returning success. In particular,
 * curr, can_prev, and can_next are observation-only: implementations must not
 * perform I/O, allocate memory, or discover a deferred error in those calls.
 */
typedef struct iterator_ops {
   /* Callers should not modify data pointed to by *key or *data */
   iterator_curr_fn  curr;
   iterator_bound_fn can_prev;
   iterator_bound_fn can_next;
   iterator_step_fn  next;
   iterator_step_fn  prev;
   iterator_seek_fn  seek;
   iterator_print_fn print;
} iterator_ops;

// To sub-class iterator, make an iterator your first field
struct iterator {
   const iterator_ops *ops;
};

typedef VECTOR(iterator *) iterator_vector;

static inline bool32
comparison_is_forward(comparison c)
{
   return c == greater_than || c == greater_than_or_equal;
}

static inline comparison
comparison_invert(comparison c)
{
   switch (c) {
      case less_than:
         return greater_than_or_equal;
      case less_than_or_equal:
         return greater_than;
      case greater_than_or_equal:
         return less_than;
      case greater_than:
         return less_than_or_equal;
      default:
         platform_assert(FALSE, "Invalid comparison %d", c);
   }
}

/*
 * Range specification for iterator constructors. These mirror the public
 * splinterdb_lower_limit / splinterdb_upper_limit / splinterdb_start_position
 * types, but hold internal keys, so they can name infinities. Constructors
 * take them in an options struct filled in with designated initializers. The
 * zero value of each is the default: a limit whose key is NULL_KEY is
 * unbounded (and its comparison is ignored), and the zero start position is
 * "at or after the smallest key".
 */
typedef struct key_lower_limit {
   comparison cmp; // greater_than or greater_than_or_equal
   key        k;
} key_lower_limit;

typedef struct key_upper_limit {
   comparison cmp; // less_than or less_than_or_equal
   key        k;
} key_upper_limit;

/*
 * The start is a direction and an inclusivity rather than a comparison, so
 * that the zero value (forward, inclusive) is the default.
 */
typedef struct key_start_position {
   key    k;
   bool32 backward;  // scan toward smaller keys
   bool32 exclusive; // skip a key equal to k
} key_start_position;

static inline key_lower_limit
key_greater_than(key k)
{
   return (key_lower_limit){.cmp = greater_than, .k = k};
}
static inline key_lower_limit
key_greater_than_or_equal(key k)
{
   return (key_lower_limit){.cmp = greater_than_or_equal, .k = k};
}
static inline key_lower_limit
make_key_lower_limit(comparison cmp, key k)
{
   return (key_lower_limit){.cmp = cmp, .k = k};
}

static inline key_upper_limit
key_less_than(key k)
{
   return (key_upper_limit){.cmp = less_than, .k = k};
}
static inline key_upper_limit
key_less_than_or_equal(key k)
{
   return (key_upper_limit){.cmp = less_than_or_equal, .k = k};
}
static inline key_upper_limit
make_key_upper_limit(comparison cmp, key k)
{
   return (key_upper_limit){.cmp = cmp, .k = k};
}

static inline key_start_position
key_start_at_or_after(key k)
{
   return (key_start_position){.k = k};
}
static inline key_start_position
key_start_after(key k)
{
   return (key_start_position){.k = k, .exclusive = TRUE};
}
static inline key_start_position
key_start_at_or_before(key k)
{
   return (key_start_position){.k = k, .backward = TRUE};
}
static inline key_start_position
key_start_before(key k)
{
   return (key_start_position){.k = k, .backward = TRUE, .exclusive = TRUE};
}
static inline key_start_position
make_key_start_position(comparison cmp, key k)
{
   return (key_start_position){.k        = k,
                               .backward = !comparison_is_forward(cmp),
                               .exclusive =
                                  cmp == greater_than || cmp == less_than};
}

static inline comparison
key_start_position_comparison(key_start_position start)
{
   if (start.backward) {
      return start.exclusive ? less_than : less_than_or_equal;
   } else {
      return start.exclusive ? greater_than : greater_than_or_equal;
   }
}

// A range specification with every default filled in: infinite keys for
// unbounded sides, and the start key chosen by direction.
typedef struct resolved_key_range {
   comparison min_cmp;
   key        min_key;
   comparison max_cmp;
   key        max_key;
   comparison start_cmp;
   key        start_key;
} resolved_key_range;

static inline resolved_key_range
resolve_key_range(key_lower_limit    lower,
                  key_upper_limit    upper,
                  key_start_position start)
{
   resolved_key_range r;
   bool32             lower_set = !key_is_null(lower.k);
   bool32             upper_set = !key_is_null(upper.k);
   r.min_cmp                    = lower_set ? lower.cmp : greater_than_or_equal;
   r.min_key                    = lower_set ? lower.k : NEGATIVE_INFINITY_KEY;
   r.max_cmp                    = upper_set ? upper.cmp : less_than;
   r.max_key                    = upper_set ? upper.k : POSITIVE_INFINITY_KEY;
   r.start_cmp                  = key_start_position_comparison(start);
   if (!key_is_null(start.k)) {
      r.start_key = start.k;
   } else {
      r.start_key =
         start.backward ? POSITIVE_INFINITY_KEY : NEGATIVE_INFINITY_KEY;
   }
   return r;
}

// It is safe to call curr whenever iterator_in_range() returns true
// otherwise the behavior of iterator_curr is undefined
static inline void
iterator_curr(iterator *itor, key *curr_key, message *msg)
{
   itor->ops->curr(itor, curr_key, msg);
}

static inline bool32
iterator_can_prev(iterator *itor)
{
   return itor->ops->can_prev(itor);
}

static inline bool32
iterator_can_next(iterator *itor)
{
   return itor->ops->can_next(itor);
}

static inline bool32
iterator_can_curr(iterator *itor)
{
   return itor->ops->can_next(itor) && itor->ops->can_prev(itor);
}

static inline platform_status
iterator_next(iterator *itor)
{
   return itor->ops->next(itor);
}

static inline platform_status
iterator_prev(iterator *itor)
{
   return itor->ops->prev(itor);
}

static inline platform_status
iterator_seek(iterator *itor, comparison seek_type, key seek_key)
{
   return itor->ops->seek(itor, seek_type, seek_key);
}

static inline void
iterator_print(iterator *itor)
{
   return itor->ops->print(itor);
}
