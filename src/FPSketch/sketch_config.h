#pragma once

#include "types.h"

typedef void(sketch_insert_value_fn)(ValueType *current_value,
                                     ValueType  new_value)
   __attribute__((nonnull(1)));
typedef void(sketch_get_value_fn)(ValueType current_value, ValueType *new_value)
   __attribute__((nonnull(2)));

typedef struct sketch_config {
   uint64_t                rows;
   uint64_t                cols;
   sketch_insert_value_fn *insert_value_fn;
   sketch_get_value_fn    *get_value_fn;
} sketch_config;

void
sketch_config_default_init(sketch_config *config);
