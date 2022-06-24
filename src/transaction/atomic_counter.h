// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _ATOMIC_COUNTER_H_
#define _ATOMIC_COUNTER_H_

#include "splinterdb/platform_linux/public_platform.h"

typedef struct atomic_counter {
   uint64 num;
} atomic_counter;

void
atomic_counter_init(atomic_counter *counter);

void
atomic_counter_deinit(atomic_counter *counter);

uint64
atomic_counter_get_next(atomic_counter *counter);

#endif // _ATOMIC_COUNTER_H_
