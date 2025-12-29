#include "platform_buffer.h"
#include "platform_log.h"
#include <sys/mman.h>

bool32 platform_use_hugetlb = FALSE;
bool32 platform_use_mlock   = FALSE;

/*
 * Certain modules, e.g. the buffer cache, need a very large buffer which
 * may not be serviceable by the heap. Create the requested buffer using
 * mmap() and initialize the input 'bh' to track this memory allocation.
 */
platform_status
platform_buffer_init(buffer_handle *bh, size_t length)
{
   platform_status rc = STATUS_NO_MEMORY;

   int prot = PROT_READ | PROT_WRITE;

   // Technically, for threaded execution model, MAP_PRIVATE is sufficient.
   // And we only need to create this mmap()'ed buffer in MAP_SHARED for
   // process-execution mode. But, at this stage, we don't know apriori if
   // we will be using SplinterDB in a multi-process execution environment.
   // So, always create this in SHARED mode. This still works for multiple
   // threads.
   int flags = MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE;
   if (platform_use_hugetlb) {
      flags |= MAP_HUGETLB;
   }

   bh->addr = mmap(NULL, length, prot, flags, -1, 0);
   if (bh->addr == MAP_FAILED) {
      platform_error_log(
         "mmap (%lu bytes) failed with error: %s\n", length, strerror(errno));
      goto error;
   }

   if (platform_use_mlock) {
      int mlock_rv = mlock(bh->addr, length);
      if (mlock_rv != 0) {
         platform_error_log("mlock (%lu bytes) failed with error: %s\n",
                            length,
                            strerror(errno));
         // Save off rc from mlock()-failure, so we return that as status
         rc = CONST_STATUS(errno);

         int rv = munmap(bh->addr, length);
         if (rv != 0) {
            // We are losing rc from this failure; can't return 2 statuses
            platform_error_log("munmap %p (%lu bytes) failed with error: %s\n",
                               bh->addr,
                               length,
                               strerror(errno));
         }
         goto error;
      }
   }
   bh->length = length;
   return STATUS_OK;

error:
   // Reset, in case mmap() or mlock() failed.
   bh->addr = NULL;
   return rc;
}

void *
platform_buffer_getaddr(const buffer_handle *bh)
{
   return bh->addr;
}

/*
 * platform_buffer_deinit() - Deinit the buffer handle, which involves
 * unmapping the memory for the large buffer created using mmap().
 * This is expected to be called on a 'bh' that has been successfully
 * initialized, thru a prior platform_buffer_init() call.
 */
platform_status
platform_buffer_deinit(buffer_handle *bh)
{
   int ret;
   ret = munmap(bh->addr, bh->length);
   if (ret) {
      return CONST_STATUS(errno);
   }

   bh->addr   = NULL;
   bh->length = 0;
   return STATUS_OK;
}
