// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * limits.h --
 *
 *     This file contains constants and functions the pertain to
 *     limits of keys and messages supported by the Key Value Store.
 */

#ifndef __LIMITS_H__
#define __LIMITS_H__

#define MAX_KEY_SIZE     100

// We get assertion from BTree code if this limit is 2000.
// We get value-too-large error if this is 1800, when FDB seemingly inserts
// values beyond 1800 bytes. 1900 seems to work fine.
#define MAX_MESSAGE_SIZE 1900

#endif // __LIMITS_H__
