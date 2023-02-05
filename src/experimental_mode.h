#ifndef EXPERIMENTAL_MODE_H
#define EXPERIMENTAL_MODE_H

#include "platform.h"

#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS 0
#define EXPERIMENTAL_MODE_SILO          0
#if EXPERIMENTAL_MODE_SILO == 1
#   undef EXPERIMENTAL_MODE_KEEP_ALL_KEYS
#   define EXPERIMENTAL_MODE_KEEP_ALL_KEYS 1
#endif

static inline void
print_current_experimental_modes()
{
   // This function is used for only research experiments
   platform_set_log_streams(stdout, stderr);

   platform_default_log("EXPERIMENTAL_MODE_KEEP_ALL_KEYS: %d\n",
                        EXPERIMENTAL_MODE_KEEP_ALL_KEYS);
   platform_default_log("EXPERIMENTAL_MODE_SILO: %d\n", EXPERIMENTAL_MODE_SILO);
}

#endif
