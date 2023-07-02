// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "data_internal.h"
#include "util.h"

typedef struct iterator iterator;

typedef void (*iterator_curr_fn)(iterator *itor, key *curr_key, message *msg);
typedef bool (*iterator_valid_fn)(iterator *itor);
typedef platform_status (*iterator_step_fn)(iterator *itor);
typedef void (*iterator_print_fn)(iterator *itor);

typedef struct iterator_ops {
   /* Callers should not modify data pointed to by *key or *data */
   iterator_curr_fn  curr;
   iterator_valid_fn valid;
   iterator_step_fn  next;
   iterator_print_fn print;
} iterator_ops;

// To sub-class iterator, make an iterator your first field
struct iterator {
   const iterator_ops *ops;
};

static inline void
iterator_curr(iterator *itor, key *curr_key, message *msg)
{
   itor->ops->curr(itor, curr_key, msg);
}

static inline bool
iterator_valid(iterator *itor)
{
   return itor->ops->valid(itor);
}

static inline platform_status
iterator_next(iterator *itor)
{
   return itor->ops->next(itor);
}

static inline void
iterator_print(iterator *itor)
{
   return itor->ops->print(itor);
}
