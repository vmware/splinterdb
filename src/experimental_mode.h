#pragma once

#include "platform.h"

#define EXPERIMENTAL_MODE_TICTOC_DISK 1
#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS     0
#define EXPERIMENTAL_MODE_SKETCH            0
#define EXPERIMENTAL_MODE_SILO              0
#define EXPERIMENTAL_MODE_BYPASS_SPLINTERDB 0
#define EXPERIMENTAL_MODE_ATOMIC_WORD       0

#if EXPERIMENTAL_MODE_TICTOC_DISK
typedef uint32 txn_timestamp;
#else
typedef uint64 txn_timestamp;
#endif

static inline void
check_experimental_mode_is_valid()
{
   if (EXPERIMENTAL_MODE_TICTOC_DISK) {
      platform_assert(EXPERIMENTAL_MODE_KEEP_ALL_KEYS == 0);
      platform_assert(EXPERIMENTAL_MODE_SKETCH == 0);
      platform_assert(EXPERIMENTAL_MODE_SILO == 0);
      platform_assert(EXPERIMENTAL_MODE_BYPASS_SPLINTERDB == 0);
      platform_assert(EXPERIMENTAL_MODE_ATOMIC_WORD == 0);
   }

   if (EXPERIMENTAL_MODE_SILO) {
      platform_assert(EXPERIMENTAL_MODE_KEEP_ALL_KEYS == 1);
   }

   if (EXPERIMENTAL_MODE_KEEP_ALL_KEYS) {
      platform_assert(EXPERIMENTAL_MODE_TICTOC_DISK == 0);
      platform_assert(EXPERIMENTAL_MODE_SKETCH == 0);
   }

   if (EXPERIMENTAL_MODE_SKETCH) {
      platform_assert(EXPERIMENTAL_MODE_TICTOC_DISK == 0);
      platform_assert(EXPERIMENTAL_MODE_KEEP_ALL_KEYS == 0);
      platform_assert(EXPERIMENTAL_MODE_SILO == 0);
   }

   if (EXPERIMENTAL_MODE_BYPASS_SPLINTERDB) {
      platform_assert(EXPERIMENTAL_MODE_TICTOC_DISK == 0);
   }

   if (EXPERIMENTAL_MODE_ATOMIC_WORD) {
      platform_assert(EXPERIMENTAL_MODE_TICTOC_DISK == 0);
   }
}

static inline void
print_current_experimental_modes()
{
   // This function is used for only research experiments
   platform_set_log_streams(stdout, stderr);
   platform_default_log("EXPERIMENTAL_MODE_TICTOC_DISK: %d\n",
                        EXPERIMENTAL_MODE_TICTOC_DISK);
   platform_default_log("EXPERIMENTAL_MODE_KEEP_ALL_KEYS: %d\n",
                        EXPERIMENTAL_MODE_KEEP_ALL_KEYS);
   platform_default_log("EXPERIMENTAL_MODE_SKETCH: %d\n",
                        EXPERIMENTAL_MODE_SKETCH);
   platform_default_log("EXPERIMENTAL_MODE_SILO: %d\n", EXPERIMENTAL_MODE_SILO);
   platform_default_log("EXPERIMENTAL_MODE_BYPASS_SPLINTERDB: %d\n",
                        EXPERIMENTAL_MODE_BYPASS_SPLINTERDB);
   platform_default_log("EXPERIMENTAL_MODE_ATOMIC_WORD: %d\n",
                        EXPERIMENTAL_MODE_ATOMIC_WORD);
}
