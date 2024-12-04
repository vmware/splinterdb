#include "platform.h"
#include "async.h"

void
async_call_sync_callback_function(void *arg)
{
   bool32 *ready = (bool32 *)arg;
   *ready        = TRUE;
}
