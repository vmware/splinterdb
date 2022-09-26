// Copyright 2018-2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * shmem.c --
 *
 * This file contains the implementation for managing shared memory created
 * for use by SplinterDB and all its innards.
 */

#include "platform.h"
#include "shmem.h"

platform_status
platform_shmget(size_t size)
{
    platform_default_log("Created shared memory of size %lu bytes.\n", size);
    return STATUS_OK;
}
