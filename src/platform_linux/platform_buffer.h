#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_status.h"

extern bool32 platform_use_hugetlb;
extern bool32 platform_use_mlock;

// Buffer handle
typedef struct {
   void  *addr;
   size_t length;
} buffer_handle;

platform_status
platform_buffer_init(buffer_handle *bh, size_t length);

void *
platform_buffer_getaddr(const buffer_handle *bh);

platform_status
platform_buffer_deinit(buffer_handle *bh);
