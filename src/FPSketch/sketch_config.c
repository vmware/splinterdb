#include "sketch_config.h"

void
insert_value_fn_default(ValueType *current_value, ValueType new_value)
{
   *current_value = (*current_value > new_value) ? *current_value : new_value;
}

void
get_value_fn_default(ValueType left_value, ValueType *right_value)
{
   *right_value = (left_value < *right_value) ? left_value : *right_value;
}

void
sketch_config_default_init(sketch_config *config)
{
   config->rows            = 1;
   config->cols            = 1;
   config->insert_value_fn = &insert_value_fn_default;
   config->get_value_fn    = &get_value_fn_default;
}