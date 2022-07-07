#ifndef _SIMPLE_SET_H
#define _SIMPLE_SET_H

#include "splinterdb/public_platform.h"
#include "mvcc_data.h"

#define SIMPLE_SET_SIZE_LIMIT 1024

typedef struct simple_set_node {
   transaction_op_meta *meta;
} simple_set_node;


typedef struct simple_set {
   simple_set_node nodes[SIMPLE_SET_SIZE_LIMIT];
   uint64          size;
   uint64          seed;
} simple_set;

void
simple_set_init(simple_set *set);

int
simple_set_insert(simple_set *set, transaction_op_meta *meta);

int
simple_set_delete(simple_set *set, transaction_op_meta *meta);

bool
simple_set_is_overlap(simple_set *s1, simple_set *s2);

typedef struct simple_set_iter {
   simple_set *set;
   uint64      i;
} simple_set_iter;

simple_set_iter
simple_set_first(simple_set *set);

simple_set_iter
simple_set_iter_next(simple_set_iter it);

bool
simple_set_iter_is_valid(simple_set_iter it);

transaction_op_meta *
simple_set_iter_data(simple_set_iter it);

#endif // _SIMPLE_SET_H
