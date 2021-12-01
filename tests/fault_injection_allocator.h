#include "rc_allocator.h"
#include "random.h"

typedef struct fault_injection_allocator {
   allocator  super;
   allocator *base;

   /* Configuration */
   uint64 failure_probability;
   uint64 burst_size;

   /* State */
   random_state rs;
   uint64       current_burst_size;
} fault_injection_allocator;

MUST_CHECK_RESULT platform_status
fault_injection_allocator_init(fault_injection_allocator *al,
                               allocator *                base,
                               uint64                     failure_probability,
                               uint64                     burst_size,
                               uint64                     seed);
