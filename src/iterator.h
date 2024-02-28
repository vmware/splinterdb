// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "data_internal.h"
#include "util.h"

typedef struct iterator iterator;

// for seek
typedef enum comparison {
   less_than,
   less_than_or_equal,
   greater_than,
   greater_than_or_equal,
} comparison;

typedef void (*iterator_curr_fn)(iterator *itor, key *curr_key, message *msg);
typedef bool32 (*iterator_bound_fn)(iterator *itor);
typedef platform_status (*iterator_step_fn)(iterator *itor);
typedef platform_status (*iterator_seek_fn)(iterator  *itor,
                                            key        seek_key,
                                            comparison from_above);
typedef void (*iterator_print_fn)(iterator *itor);

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
iterator_seek(iterator *itor, key seek_key, comparison seek_type)
{
   return itor->ops->seek(itor, seek_key, seek_type);
}

static inline void
iterator_print(iterator *itor)
{
   return itor->ops->print(itor);
}
