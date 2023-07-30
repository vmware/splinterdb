
/*
 * splinterdb_internal.h --
 *
 *    Private struct declarations for splinterdb that we'd like
 *    to share among multiple files, but not share with users.
 */

#ifndef SPLINTERDB_SPLINTERDB_INTERNAL_H_
#define SPLINTERDB_SPLINTERDB_INTERNAL_H_

#include "trunk.h"
#include "clockcache.h"
#include "rc_allocator.h"
#include "shard_log.h"

typedef struct splinterdb {
   task_system         *task_sys;
   io_config            io_cfg;
   platform_io_handle   io_handle;
   allocator_config     allocator_cfg;
   rc_allocator         allocator_handle;
   clockcache_config    cache_cfg;
   clockcache           cache_handle;
   shard_log_config     log_cfg;
   task_system_config   task_cfg;
   allocator_root_id    trunk_id;
   trunk_config         trunk_cfg;
   trunk_handle        *spl;
   platform_heap_handle heap_handle; // for platform_buffer_create
   platform_heap_id     heap_id;
   data_config         *data_cfg;
} splinterdb;

struct splinterdb_iterator {
   trunk_range_iterator sri;
   platform_status      last_rc;
   const splinterdb    *parent;
};

#endif // SPLINTERDB_SPLINTERDB_INTERNAL_H_
