#pragma once

#ifdef __cplusplus
#   define __restrict__
extern "C" {
#endif

#include "types.h"
#include "sketch_config.h"

#define USE_SKETCH_ITEM_LATCH 0

typedef struct sketch_item {
   ValueType value;
#if USE_SKETCH_ITEM_LATCH
   bool latch;
#endif
} sketch_item;

typedef struct sketch {
   sketch_config *config;
   sketch_item   *table;
   unsigned int  *hashes;
} sketch;

void
sketch_init(sketch_config *config, sketch *sktch);

void
sketch_deinit(sketch *sktch);

void
sketch_insert(sketch *sktch, slice key, ValueType value);
ValueType
sketch_get(sketch *sktch, slice key);

#ifndef MAX
#   define MAX(x, y) ((x) > (y) ? (x) : (y))
#endif
#ifndef MIN
#   define MIN(x, y) ((x) < (y) ? (x) : (y))
#endif

#ifdef __cplusplus
}
#endif
