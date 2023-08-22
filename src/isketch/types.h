#pragma once

#ifdef __cplusplus
#   define __restrict__
extern "C" {
#endif

#include "splinterdb/public_util.h"
#include "splinterdb/data.h"
#include "data_internal.h"

typedef unsigned __int128 ValueType;

typedef struct kv_pair {
   slice     key;
   ValueType val;
   uint64_t  refcount;
} kv_pair;

#ifdef __cplusplus
}
#endif