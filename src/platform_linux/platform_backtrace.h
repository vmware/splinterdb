#pragma once
#include <execinfo.h>

static inline int
platform_backtrace(void **buffer, int size)
{
   return backtrace(buffer, size);
}

