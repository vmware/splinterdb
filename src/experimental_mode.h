#pragma once

#include "platform.h"

#define EXPERIMENTAL_MODE_TICTOC_DISK       0
#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS     0
#define EXPERIMENTAL_MODE_SILO              0
#define EXPERIMENTAL_MODE_BYPASS_SPLINTERDB 0

#if EXPERIMENTAL_MODE_SILO
#   undef EXPERIMENTAL_MODE_KEEP_ALL_KEYS
#   define EXPERIMENTAL_MODE_KEEP_ALL_KEYS 1
#endif
#if EXPERIMENTAL_MODE_KEEP_ALL_KEYS
#   undef EXPERIMENTAL_MODE_TICTOC_DISK
#   define EXPERIMENTAL_MODE_TICTOC_DISK 0
#endif
#if EXPERIMENTAL_MODE_TICTOC_DISK
#   undef EXPERIMENTAL_MODE_KEEP_ALL_KEYS
#   undef EXPERIMENTAL_MODE_SILO
#   undef EXPERIMENTAL_MODE_BYPASS_SPLINTERDB
#   define EXPERIMENTAL_MODE_KEEP_ALL_KEYS     0
#   define EXPERIMENTAL_MODE_SILO              0
#   define EXPERIMENTAL_MODE_BYPASS_SPLINTERDB 0
typedef uint32 txn_timestamp;
#else
typedef uint64 txn_timestamp;
#endif

static inline void
print_current_experimental_modes()
{
   // This function is used for only research experiments
   platform_set_log_streams(stdout, stderr);
   platform_default_log("EXPERIMENTAL_MODE_TICTOC_DISK: %d\n",
                        EXPERIMENTAL_MODE_TICTOC_DISK);
   platform_default_log("EXPERIMENTAL_MODE_KEEP_ALL_KEYS: %d\n",
                        EXPERIMENTAL_MODE_KEEP_ALL_KEYS);
   platform_default_log("EXPERIMENTAL_MODE_SILO: %d\n", EXPERIMENTAL_MODE_SILO);
   platform_default_log("EXPERIMENTAL_MODE_BYPASS_SPLINTERDB: %d\n",
                        EXPERIMENTAL_MODE_BYPASS_SPLINTERDB);
}
