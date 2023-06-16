#ifndef _SKETCH_H_
#define _SKETCH_H_

#ifdef __cplusplus
#   define __restrict__
extern "C" {
#endif

#include "types.h"

#define USE_SKETCH_ITEM_LATCH 0

typedef struct sketch_item {
   ValueType value;
#if USE_SKETCH_ITEM_LATCH
   bool latch;
#endif
} sketch_item;

typedef struct sketch {
   uint64_t      rows;
   uint64_t      cols;
   sketch_item  *table;
   unsigned int *hashes;
} sketch;

void
sketch_init(uint64_t rows, uint64_t cols, sketch *sktch);
void
sketch_deinit(sketch *sktch);

void
sketch_insert(sketch *sktch, KeyType key, ValueType value);
ValueType
sketch_get(sketch *sktch, KeyType key);

#ifndef MAX
#   define MAX(x, y) ((x) > (y) ? (x) : (y))
#endif
#ifndef MIN
#   define MIN(x, y) ((x) < (y) ? (x) : (y))
#endif

#ifdef __cplusplus
}
#endif

#endif