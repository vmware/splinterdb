#include "transaction_data_config.h"
#include <string.h>

// TODO: implement this
static int
merge_tictoc_tuple(const data_config *cfg,
                   slice              key,         // IN
                   message            old_message, // IN
                   merge_accumulator *new_message) // IN/OUT
{
   return 0;
}

// TODO: implement this
static int
merge_tictoc_tuple_final(const data_config *cfg,
                         slice              key,
                         merge_accumulator *oldest_message)
{
   return 0;
}

static transaction_data_config template_cfg = {
   .super = {.merge_tuples       = merge_tictoc_tuple,
             .merge_tuples_final = merge_tictoc_tuple_final},
};

void
transaction_data_config_init(data_config             *in_cfg, // IN
                             transaction_data_config *out_cfg // OUT
)
{
   memcpy(out_cfg, &template_cfg, sizeof(template_cfg));
   out_cfg->application_data_config = in_cfg;
}
