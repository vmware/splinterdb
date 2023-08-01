#pragma once

#include "platform.h"

#define EXPERIMENTAL_MODE_TICTOC            0
#define EXPERIMENTAL_MODE_STO               0
#define EXPERIMENTAL_MODE_MEMORY            0
#define EXPERIMENTAL_MODE_DISK              0
#define EXPERIMENTAL_MODE_COUNTER           0
#define EXPERIMENTAL_MODE_SKETCH            0
#define EXPERIMENTAL_MODE_SILO              0
#define EXPERIMENTAL_MODE_BYPASS_SPLINTERDB 0

#if EXPERIMENTAL_MODE_TICTOC_DISK
typedef uint32 txn_timestamp;
#else
typedef uint128 txn_timestamp;
#endif

static inline void
check_experimental_mode_is_valid()
{
   platform_assert(EXPERIMENTAL_MODE_TICTOC + EXPERIMENTAL_MODE_STO == 1);
   platform_assert(EXPERIMENTAL_MODE_MEMORY + EXPERIMENTAL_MODE_DISK
                      + EXPERIMENTAL_MODE_COUNTER + EXPERIMENTAL_MODE_SKETCH
                   == 1);
}

static inline void
print_current_experimental_modes()
{
   // This function is used for only research experiments
   platform_set_log_streams(stdout, stderr);

   if (EXPERIMENTAL_MODE_TICTOC) {
      platform_default_log("EXPERIMENTAL_MODE_TICTOC\n");
   }
   if (EXPERIMENTAL_MODE_STO) {
      platform_default_log("EXPERIMENTAL_MODE_STO\n");
   }
   if (EXPERIMENTAL_MODE_MEMORY) {
      platform_default_log("EXPERIMENTAL_MODE_MEMORY\n");
   }
   if (EXPERIMENTAL_MODE_DISK) {
      platform_default_log("EXPERIMENTAL_MODE_DISK\n");
   }
   if (EXPERIMENTAL_MODE_COUNTER) {
      platform_default_log("EXPERIMENTAL_MODE_COUNTER\n");
   }
   if (EXPERIMENTAL_MODE_SKETCH) {
      platform_default_log("EXPERIMENTAL_MODE_SKETCH\n");
   }
   if (EXPERIMENTAL_MODE_SILO) {
      platform_default_log("EXPERIMENTAL_MODE_SILO\n");
   }
   if (EXPERIMENTAL_MODE_BYPASS_SPLINTERDB) {
      platform_default_log("EXPERIMENTAL_MODE_BYPASS_SPLINTERDB\n");
   }
}
