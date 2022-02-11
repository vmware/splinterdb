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

#define MAX_KEY_SIZE     24  /* bytes */
#define MAX_MESSAGE_SIZE 128 /* bytes */
#define MAX_KEY_STR_LEN  128 /* bytes */

/*
 * Define limits for default values for these sizes. For an application that
 * just wants to kick the tyres on SplinterDB, we provide a
 * default_data_config{} structure which uses these limits.
 */
#define DEFAULT_KEY_SIZE     24  /* bytes */
#define DEFAULT_MESSAGE_SIZE 200 /* bytes */

#endif // __LIMITS_H__
