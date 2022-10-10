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

void *
splinterdb_get_heap_handle(const splinterdb *kvs);

const void *
splinterdb_get_task_system_handle(const splinterdb *kvs);

const void *
splinterdb_get_io_handle(const splinterdb *kvs);

const void *
splinterdb_get_allocator_handle(const splinterdb *kvs);

const void *
splinterdb_get_cache_handle(const splinterdb *kvs);

const void *
splinterdb_get_trunk_handle(const splinterdb *kvs);

const void *
splinterdb_get_memtable_context_handle(const splinterdb *kvs);

void
platform_enable_tracing_large_frags();

void
platform_disable_tracing_large_frags();
