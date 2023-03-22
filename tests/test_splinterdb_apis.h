// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * test_splinterdb_apis.h --
 *
 * Header file with extern prototypes for testing-hooks provided in diff source
 * files.
 * -----------------------------------------------------------------------------
 */
#pragma once

#include "splinterdb/splinterdb.h"

// External APIs provided -ONLY- for use as a testing hook.
void
splinterdb_cache_flush(const splinterdb *kvs);
