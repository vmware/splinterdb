// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinterdb_private.h --
 *
 * This file contains the private interfaces implemented in splinterdb.c.
 * These definitions are provided here so that they can be shared by the
 * source and test modules.
 */
#ifndef __SPLINTERDB_PRIVATE_H__
#define __SPLINTERDB_PRIVATE_H__

#include "splinterdb/splinterdb.h"

bool
validate_key_in_range(const splinterdb *kvs, slice key);

#endif // __SPLINTERDB_PRIVATE_H__
