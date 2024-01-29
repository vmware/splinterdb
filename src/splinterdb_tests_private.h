// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinterdb_tests_private.h -
 *
 * Header file with extern prototypes for testing-hooks provided in diff source
 * files.
 * -----------------------------------------------------------------------------
 */
#pragma once

#include "splinterdb/splinterdb.h"
#include "task.h"
#include "allocator.h"
#include "cache.h"
#include "trunk.h"

// External APIs provided -ONLY- for use as a testing hook.
void
splinterdb_cache_flush(const splinterdb *kvs);

platform_heap_id
splinterdb_get_heap_id(const splinterdb *kvs);

const task_system *
splinterdb_get_task_system_handle(const splinterdb *kvs);

const platform_io_handle *
splinterdb_get_io_handle(const splinterdb *kvs);

const allocator *
splinterdb_get_allocator_handle(const splinterdb *kvs);

const cache *
splinterdb_get_cache_handle(const splinterdb *kvs);

const trunk_handle *
splinterdb_get_trunk_handle(const splinterdb *kvs);

const memtable_context *
splinterdb_get_memtable_context_handle(const splinterdb *kvs);
