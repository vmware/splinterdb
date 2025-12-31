#include "platform_io.h"
#include "laio.h"

io_handle *
io_handle_create(io_config *cfg, platform_heap_id hid)
{
   return laio_handle_create(cfg, hid);
}

void
io_handle_destroy(io_handle *ioh)
{
   laio_handle_destroy(ioh);
}

platform_status
io_config_valid(io_config *cfg)
{
   return laio_config_valid(cfg);
}
