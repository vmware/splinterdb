// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __PLATFORM_SHMEM_H__
#define __PLATFORM_SHMEM_H__

#include <sys/types.h>
#include <sys/shm.h>

platform_status platform_shmget(size_t size);

#endif   // __PLATFORM_SHMEM_H__
