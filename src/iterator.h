// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "data_internal.h"
#include "util.h"

typedef struct iterator iterator;

typedef void (*iterator_get_curr_fn)(iterator *itor,
                                     key      *curr_key,
                                     message  *msg);
typedef platform_status (*iterator_at_end_fn)(iterator *itor, bool *at_end);
typedef platform_status (*iterator_advance_fn)(iterator *itor);
typedef void (*iterator_print_fn)(iterator *itor);

typedef struct iterator_ops {
   /* Callers should not modify data pointed to by *key or *data */
   iterator_get_curr_fn get_curr;
   iterator_at_end_fn   at_end;
   iterator_advance_fn  advance;
   iterator_print_fn    print;
} iterator_ops;

// To sub-class iterator, make an iterator your first field
struct iterator {
   const iterator_ops *ops;
};

static inline void
iterator_get_curr(iterator *itor, key *curr_key, message *msg)
{
   itor->ops->get_curr(itor, curr_key, msg);
}

static inline platform_status
iterator_at_end(iterator *itor, bool *at_end)
{
   return itor->ops->at_end(itor, at_end);
}

static inline platform_status
iterator_advance(iterator *itor)
{
   return itor->ops->advance(itor);
}

static inline void
iterator_print(iterator *itor)
{
   return itor->ops->print(itor);
}
