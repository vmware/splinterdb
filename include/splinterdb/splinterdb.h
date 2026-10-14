// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinterdb.h --
 *
 *     The public API for SplinterDB
 *
 *     A data_config must be provided at the time of create/open.
 *     See default_data_config.h for a basic reference implementation.
 *
 * Threads are registered with SplinterDB automatically on first use and are
 * deregistered automatically when they exit.
 */

#pragma once

#include "data.h"

#ifdef __cplusplus
extern "C" {
#endif

// Get a version string for this build of SplinterDB
// Currently a git tag
const char *
splinterdb_get_version();

/*
 * ****************************************************************************
 * Configuration options for SplinterDB:
 *
 * Physical configuration things such as file name, cache & disk-size,
 * extent- and page-size are specified here. Application-specific data
 * configuration is also provided through this struct. Additionally,
 * user can select whether to use malloc()/free()-based memory allocation
 * for all structures (default), or choose to setup a shared segment
 * which will be used for shared structures.
 *
 */
typedef struct splinterdb_config {
   // required configuration
   const char *filename;
   uint64      cache_size;

   // Required for splinterdb_create().  For splinterdb_open(), zero means read
   // the value from the on-disk superblock; nonzero values are checked against
   // the superblock.
   uint64 disk_size;

   // data_config is a required field that defines how your data should be
   // read and written in SplinterDB.  See data.h for details.
   // For a simple reference implementation, see default_data_config.h
   data_config *data_cfg;

   // optional advanced config below
   // if unset, defaults will be used
   void *heap_handle;
   void *heap_id;

   // Shared memory support
   uint64 shmem_size;
   _Bool  use_shmem; // Default is FALSE.

   // For splinterdb_open(), zero means read the value from the on-disk
   // superblock; nonzero values are checked against the superblock.
   uint64 page_size;
   uint64 extent_size;

   // io
   int    io_flags;
   uint32 io_perms;
   uint64 io_async_queue_depth;

   // cache
   _Bool       cache_use_stats;
   const char *cache_logfile;

   // task system
   // Background threads configuration:
   //
   // - Memtable bg-threads work on Memtables tasks which are short but latency
   //   sensitive. A rule of thumb is to allocate around 1 memtable bg-thread
   //   for every 10 threads performing insertions. Too few memtable threads
   //   can cause some insertions to have high latency.
   // - Normal bg-threads work on task such as compacting branches in the trunk
   //   and building filters. These tasks take longer and are less latency
   //   sensitive. A rule of thumb is to allocate 1-2 normal background
   //   threads for every thread performing insertions. Too few "normal"
   //   background threads can cause disk I/O bandwidth to go underutilized.
   uint64 num_memtable_bg_threads;
   uint64 num_normal_bg_threads;

   // Maximum encoded blob descriptor bytes, including its header. Zero selects
   // 512. This is a runtime encoding policy; existing descriptors are decoded
   // independently of this setting. Must be at least 114 and no greater than
   // the B-tree inline message limit (35% of page_size).
   uint64 blob_max_descriptor_size;

   // btree
   uint64 btree_rough_count_height;

   // filter
   // Zero selects the default. Explicit hash widths must be at most 32 bits;
   // log-index widths must be less than 32 and no larger than the effective
   // hash width.
   uint64 filter_hash_size;
   uint64 filter_log_index_size;

   // log
   _Bool use_log;

   // Best-effort automatic durability threshold, in pages. Zero selects
   // 1024 pages; UINT64_MAX disables it. Each new staging page and
   // each blob's logical bytes (rounded to pages) count toward the threshold.
   // Crossing it submits a fire-and-forget durable barrier after publishing
   // the update. Concurrent writers and large blobs may overshoot. Failures
   // are logged; they do not turn a visible update into a failed insert.
   // Background task scheduling may enlist foreground threads to do the work.
   uint64 log_group_size_pages;

   // Automatic checkpoints: once the write-ahead log has grown by this many
   // bytes, SplinterDB arms a checkpoint.  The next natural memtable rotation
   // cuts the log, after which the checkpoint folds the logged updates into the
   // durable tree and reclaims that log's space.  This bounds both how much log
   // a crash has to replay and how much space the log occupies.
   //
   // The trigger is sized in log bytes rather than in updates because the two
   // are independent: a workload that repeatedly overwrites the same keys grows
   // the log with every write while barely growing the tree.
   //
   // Zero selects a default of one cache's worth of log, on the reasoning that
   // replaying much more log than the cache can hold has little to gain.  To
   // effectively disable automatic checkpoints -- for an application that calls
   // for them itself, and accepts that the log grows until it does -- set this
   // very large (UINT64_MAX).
   uint64 checkpoint_log_size_bytes;

   // Once an automatic checkpoint is armed, allow the live log to grow by this
   // many additional bytes while waiting for a natural memtable rotation.  If
   // the memtable has not rotated by then, SplinterDB forces a rotation.  Zero
   // selects a default of twice the memtable capacity; UINT64_MAX effectively
   // disables forced rotation while retaining the soft checkpoint trigger.
   uint64 checkpoint_log_grace_bytes;

   // splinter
   uint64 memtable_capacity;
   uint64 fanout;
   uint64 use_stats;
   uint64 reclaim_threshold;

   // The following parameter governs when foreground threads
   // performing an update to the database will perform queued
   // background tasks.  When a foreground thread performs a
   // background task, the latency of that update can be very large,
   // because some background tasks can take many milliseconds to
   // execute.  However, if foreground threads never perform
   // background tasks, then queues of background tasks may grow
   // unboundedly if there are not enough background threads, and this
   // may cause some processes, such as memtable rotation, to stall
   // updates to the database.

   // When queue_scale_percent is 0, then foreground threads will
   // perform a background task whenever one is available.  This will
   // result in high tail latencies for database updates, but will
   // ensure that background task queues are always short.

   // When queue_scale_percent is UINT64_MAX, then foreground threads
   // will never perform background tasks unless there are no background
   // threads allocated to that task group.  This will ensure that
   // foreground tasks have low latency, but requires that you
   // configure enough background threads to keep up with arriving
   // background tasks.  Thus you should use this option only if you
   // know how many background threads you need for each task type.

   // The default value of 100 says that foreground threads will begin
   // performing background tasks if there are more queued tasks than
   // there are background threads to serve them. This heuristic
   // allows you to configure the number of background threads as you
   // see fit, and the system will do its best to execute tasks on the
   // provided background threads, but will perform tasks on
   // foreground threads if needed.

   // Increasing this value (e.g. to 200, 300, etc), will cause more
   // work to take place on background threads, but task queues may
   // grow longer, causing some other parts of the system to stall.
   // Decreasing this value (e.g. to 50, 25, 10, etc) will cause more
   // work to be performed on foreground threads, increasing tail
   // latencies.
   uint64 queue_scale_percent;

   // Soft byte budget for extent read-ahead in range scans and compactions,
   // divided across the branches being merged. Roughly the storage's
   // bandwidth-delay product (bandwidth x latency); raise it for higher-latency
   // devices such as networked/cloud volumes. Zero selects a default suited to
   // local SSDs.
   uint64 prefetch_budget;
} splinterdb_config;

#define SPLINTERDB_COST_STATS_MAX_MEMTABLES 8

typedef struct splinterdb_cost_stats {
   uint64 target_fanout;
   uint64 trunk_height;
   uint32 nmemtables;
   uint8  memtable_heights[SPLINTERDB_COST_STATS_MAX_MEMTABLES];
} splinterdb_cost_stats;

///////////////////////////////////////
// Lifecycle
///////////////////////////////////////

// Opaque handle to an opened instance of SplinterDB
typedef struct splinterdb splinterdb;

// Create a new SplinterDB instance, erasing any existing file or block device.
//
// The library will allocate and own the memory for splinterdb
// and will free it on splinterdb_close().
//
// It is ok for the caller to stack-allocate cfg, since the cfg object itself
// is not retained. The returned splinterdb object records an effective copy
// for splinterdb_get_config(). cfg->data_cfg will be referenced by the
// returned splinterdb object, so it must live at least as long as the
// splinterdb.
int
splinterdb_create(const splinterdb_config *cfg, splinterdb **kvs);

// Open an existing splinterdb from a file/device on disk.
//
// disk_size, page_size, and extent_size are read from the on-disk superblock
// when left unset. If they are set, they must match the superblock.
//
// The library will allocate and own the memory for splinterdb
// and will free it on splinterdb_close().
//
// It is ok for the caller to stack-allocate cfg, since the cfg object itself
// is not retained. The returned splinterdb object records an effective copy
// for splinterdb_get_config(). cfg->data_cfg will be referenced by the
// returned splinterdb object, so it must live at least as long as the
// splinterdb.
int
splinterdb_open(const splinterdb_config *cfg, splinterdb **kvs);

// Close a splinterdb
//
// A completed close makes all acknowledged data recoverable and releases all
// resources.
//
// STATUS_OK means all acknowledged data is recoverable.  Recovery may still
// need to replay a durable log or rebuild allocator state; that is not a close
// failure.
//
// Without force, any error means shutdown was refused before destructive
// teardown.  The database remains open and *kvs is unchanged, so the caller can
// retry or investigate.
//
// With force, teardown always completes.  An error means data preservation
// could not be guaranteed; it does not prove that data was actually lost.
//
// force | return code | database closed? | meaning
// ---------------------------------------------------------------
// FALSE | STATUS_OK   | YES              | acknowledged data is recoverable
// FALSE | any error   | NO               | shutdown was refused
// TRUE  | STATUS_OK   | YES              | acknowledged data is recoverable
// TRUE  | any error   | YES              | preservation cannot be guaranteed
//
// After STATUS_OK or any forced close, *kvs is freed and set to NULL.
int
splinterdb_close(splinterdb **kvs, bool32 force);

// Return the effective config for a running SplinterDB instance, including
// defaults applied during create/open.
//
// The returned pointer is owned by kvs and remains valid until
// splinterdb_close(). The caller must not modify it.
const splinterdb_config *
splinterdb_get_config(const splinterdb *kvs);

// Fill in cost-model statistics for a running SplinterDB instance.
//
// Returns 0 on success, otherwise an errno-style error code.
int
splinterdb_get_cost_stats(splinterdb *kvs, splinterdb_cost_stats *stats);


////////////////////////////////////
// Notifications
////////////////////////////////////

// Size of opaque data required to hold a notification.
#define SPLINTERDB_NOTIFICATION_BUFSIZE (32 * sizeof(void *))

typedef struct splinterdb_notification {
   char opaque[SPLINTERDB_NOTIFICATION_BUFSIZE];
} __attribute__((__aligned__(8))) splinterdb_notification;

// Operation notification arguments select blocking, fire-and-forget, or tracked
// asynchronous completion. These sentinels are operation arguments, not
// objects: never pass them to notification init, deinit, poll, wait, or
// user_data.
#define SPLINTERDB_NOTIFICATION_BLOCKING        NULL
#define SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET ((splinterdb_notification *)1)

// A real notification requests asynchronous completion. The caller owns its
// storage and must keep it alive until completion. After completion (or
// rejected submission), call deinit before reusing or destroying it. If
// completion may happen in another process, allocate the notification in shared
// memory.
void
splinterdb_notification_init_polling(splinterdb_notification *notification,
                                     void                    *user_data);

void
splinterdb_notification_deinit(splinterdb_notification *notification);

_Bool
splinterdb_notification_poll(const splinterdb_notification *notification,
                             int                           *status);

// Drive queued work until completion. A wait error can leave the operation
// pending: poll to confirm completion before destroying the notification.
int
splinterdb_notification_wait(splinterdb              *kvs,
                             splinterdb_notification *notification);

void *
splinterdb_notification_user_data(const splinterdb_notification *notification);


////////////////////////////////////
// Lookups
////////////////////////////////////

typedef uint64 splinterdb_lookup_flags;

#define SPLINTERDB_LOOKUP_VALUE       ((splinterdb_lookup_flags)0)
#define SPLINTERDB_LOOKUP_MIGHT_EXIST ((splinterdb_lookup_flags)1 << 0)

// Size of opaque data required to hold a lookup result
#define SPLINTERDB_LOOKUP_BUFSIZE (20 * sizeof(void *))

// A lookup result is stored and parsed from here
//
// Once initialized, a splinterdb_lookup_result may be used for multiple
// lookups. It is not safe to use from multiple threads.
typedef struct splinterdb_lookup_result {
   char opaque[SPLINTERDB_LOOKUP_BUFSIZE];
} __attribute__((__aligned__(8))) splinterdb_lookup_result;

// Initialize a lookup result object.
//
// flags may be SPLINTERDB_LOOKUP_VALUE or SPLINTERDB_LOOKUP_MIGHT_EXIST.
//
// When SPLINTERDB_LOOKUP_MIGHT_EXIST is set, splinterdb_lookup_found() reports
// true if the key might exist, and false if the key definitely does not exist.
// splinterdb_lookup_result_value() will return EINVAL for such results.
//
// If buffer is NULL, then the library will allocate and manage memory.
//
// If the caller provides a buffer, that will be used, unless a lookup
// requires a larger buffer, at which time the library will allocate.
// Regardless, the library will never free a buffer supplied by the application.
//
// After this function returns, the caller must ensure that
// 1. *result is only used in conjunction with the kvs
//    Attempting to use one lookup_result with multiple instances of splinterdb
//    will cause problems
// 2. The lifetime of *result must not exceed the lifetime of kvs
//    The result should be deinit'ed before calling splinterdb_close on kvs
//
// While the current version of SplinterDB does not rely on Rule 2, future
// versions may store pointers to Splinter's own memory in the lookup_result.
void
splinterdb_lookup_result_init(const splinterdb         *kvs,        // IN
                              splinterdb_lookup_result *result,     // IN/OUT
                              splinterdb_lookup_flags   flags,      // IN
                              uint64                    buffer_len, // IN
                              char                     *buffer      // IN
);

// Release any resources used by result
//
// This will never free a buffer passed to splinterdb_lookup_result_init
void
splinterdb_lookup_result_deinit(splinterdb_lookup_result *result); // IN

// Returns true if the result was found.
//
// For existence-only lookups, this returns true when the key exists or might
// exist.
_Bool
splinterdb_lookup_found(const splinterdb_lookup_result *result); // IN

// Decode the value from a found result
//
// Do not modify the memory pointed at by *value
int
splinterdb_lookup_result_value(const splinterdb_lookup_result *result, // IN
                               slice                          *value   // OUT
);

// Lookup the message for a given key
//
// result must have first been initialized using splinterdb_lookup_result_init
int
splinterdb_lookup(splinterdb               *kvs,   // IN
                  slice                     key,   // IN
                  splinterdb_lookup_result *result // IN/OUT
);


/////////////////////////////////
// Updates
/////////////////////////////////

// A successful update is visible to subsequent operations, but is not
// necessarily durable. Use splinterdb_durable_barrier() to establish crash
// durability for a prefix of updates.
//
// Operations requested with old_result may return EAGAIN if memtable rotations
// outrun the bounded old-value catch-up, or if a skipped memtable is
// incorporated before catch-up can increment its reference count. In that
// case the update has not been applied.

// Insert a key and value.  Overwrites any previous value associated with the
// key.
//
// If old_result is non-NULL, it must have been initialized using
// splinterdb_lookup_result_init() and will receive the value previously
// associated with key.
int
splinterdb_insert(splinterdb               *kvsb,
                  slice                     key,
                  slice                     value,
                  splinterdb_lookup_result *old_result);

// Delete a given key and any associated value / messages
int
splinterdb_delete(splinterdb               *kvsb,
                  slice                     key,
                  splinterdb_lookup_result *old_result);

// Update the value associated with key.
int
splinterdb_update(splinterdb               *kvsb,
                  slice                     key,
                  slice                     delta,
                  splinterdb_lookup_result *old_result);

// Optimize already-incorporated trunk data in [min_key, max_key). A null
// min_key or max_key means the range is unbounded on that side. If
// full_leaf_compactions is true, enqueue full compactions for leaves in the
// range after flushing. NULL (SPLINTERDB_NOTIFICATION_BLOCKING) waits and
// returns completion or wait status. On a wait error, accepted work may still
// finish later; the library retains its internal completion state as needed.
// SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET returns without waiting and discards
// completion status. A real notification receives completion asynchronously.
// For either nonblocking mode, a nonzero return means submission failed.
int
splinterdb_optimize(splinterdb              *kvs,
                    slice                    min_key,
                    slice                    max_key,
                    _Bool                    full_leaf_compactions,
                    splinterdb_notification *notification);

/////////////////////////////////
// Durability
/////////////////////////////////

// Establish a durability barrier covering updates that linearized before this
// call began, including every successful insert, update, or delete that
// returned before the call. Updates may proceed concurrently; an overlapping
// update may or may not be covered.
//
// NULL (SPLINTERDB_NOTIFICATION_BLOCKING) waits and returns the durability
// status directly. Zero establishes the guarantee; a nonzero result means it
// was not established, although some or all updates may already be durable.
// This blocking path requires no notification allocation or task submission.
//
// A real notification requests asynchronous completion; the guarantee holds
// when it completes successfully. SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET
// submits without retaining a completion result. In either nonblocking mode,
// zero means accepted, not durable; a nonzero return means no work was accepted
// and any notification remains idle. Keep accepted notification storage alive
// until completion.
//
// Nonblocking launch allocates memory and takes internal locks but performs no
// device I/O. Completion runs in the normal task queue. With no normal workers,
// use notification_wait() or another operation that performs task work;
// polling alone does not progress the barrier. Close drains accepted barriers.
//
// This establishes durability only. With the WAL enabled, it does not promise
// a checkpoint, log reclamation, a clean cache, or recovery without log replay.
// Without the WAL, the guarantee requires checkpointing the tree and may be
// substantially more expensive.
int
splinterdb_durable_barrier(splinterdb              *kvs,
                           splinterdb_notification *notification);

/*
Iterator API (range query)

This API is modeled after the RocksDB Iterator and is intended to allow drop-in
replacement for most scenarios.

This documentation is heavily inspired by
  https://github.com/facebook/rocksdb/wiki/Iterator

The biggest difference is that RocksDB's Iterator exposes a Seek(key) method,
while SplinterDB expects the starting key to be provided at the time the
iterator is initialized.

Similar to RocksDB, if there is no error, then status()==0.  If status() != 0,
then valid() == false.  In other words, valid()==true implies status()== 0,
which means it is safe to proceed with other operations without checking
status().

On the other hand, if valid() == false, then there are two possibilities:
(1) We reached the end of the data without error. In this case, status() == 0;
(2) there is an error. In this case status() != 0;

Be sure to check status() whenever valid() is false.

Iterators must be cleaned up via deinit before closing the database. An iterator
may move between threads using ordinary synchronization; only one thread may
access it at a time. The creating thread need not remain registered or alive.

Known issue: a live iterator may block inserts and deletes from the same thread.


Sample application code:

   splinterdb_iterator* it;
   int rc = splinterdb_iterator_init(kvs, &it);
   if (rc != 0) { ... handle error ... }

   slice key, value;

   for(; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      splinterdb_iterator_get_current(it, &key, &value);
      // do something with key and value...
   }

   // loop exit may mean error, or just that we've reached the end of the range
   rc = splinterdb_iterator_status(it);
   if (rc != 0) { ... handle error ... }

   // Release resources acquired by the iterator
   // If you skip this, other operations, including close(), may hang.
   splinterdb_iterator_deinit(it);
*/

typedef struct splinterdb_iterator splinterdb_iterator;

// should the iterator start at the first key that is <, <=, >, or >= than the
// start_key?
typedef enum comparison {
   less_than,
   less_than_or_equal,
   greater_than,
   greater_than_or_equal,
} comparison;


// A predicate over stored keys. The key slice is borrowed for this call only.
// The predicate may run repeatedly, including for keys that were deleted, and
// before values are merged. It must return the same result for keys that
// compare equal and remain stable for the iterator's lifetime. The context is
// borrowed until iterator destruction and must be usable by any thread using
// the iterator.
typedef bool (*splinterdb_key_predicate)(void *context, slice key);

// A limit on the range an iterator returns. Build these with the helpers
// below (splinterdb_greater_than, splinterdb_less_than_or_equal, ...) rather
// than by hand. The zero value means "no limit". A limit whose key is
// NULL_SLICE also means "no limit".
typedef struct splinterdb_lower_limit {
   bool       present;
   comparison cmp; // greater_than or greater_than_or_equal
   slice      key;
} splinterdb_lower_limit;

typedef struct splinterdb_upper_limit {
   bool       present;
   comparison cmp; // less_than or less_than_or_equal
   slice      key;
} splinterdb_upper_limit;

// Where an iterator starts, and therefore which way it initially travels. The
// zero value means "at or after the smallest key", i.e. a forward scan of the
// whole range.
typedef struct splinterdb_start_position {
   bool       present;
   comparison cmp;
   slice      key;
} splinterdb_start_position;

// Lower limits: only return keys > key, or >= key.
static inline splinterdb_lower_limit
splinterdb_greater_than(slice key)
{
   return (splinterdb_lower_limit){true, greater_than, key};
}
static inline splinterdb_lower_limit
splinterdb_greater_than_or_equal(slice key)
{
   return (splinterdb_lower_limit){true, greater_than_or_equal, key};
}
// Generic form, for when the comparison is only known at run time. Passing a
// comparison other than greater_than/greater_than_or_equal makes
// splinterdb_iterator_init fail with EINVAL.
static inline splinterdb_lower_limit
splinterdb_make_lower_limit(comparison cmp, slice key)
{
   return (splinterdb_lower_limit){true, cmp, key};
}

// Upper limits: only return keys < key, or <= key.
static inline splinterdb_upper_limit
splinterdb_less_than(slice key)
{
   return (splinterdb_upper_limit){true, less_than, key};
}
static inline splinterdb_upper_limit
splinterdb_less_than_or_equal(slice key)
{
   return (splinterdb_upper_limit){true, less_than_or_equal, key};
}
// Generic form. Passing a comparison other than less_than/less_than_or_equal
// makes splinterdb_iterator_init fail with EINVAL.
static inline splinterdb_upper_limit
splinterdb_make_upper_limit(comparison cmp, slice key)
{
   return (splinterdb_upper_limit){true, cmp, key};
}

// Start positions. The iterator starts at the closest key satisfying the
// relation, and the relation also sets the initial direction (the first two
// scan forward, the last two backward). If key is NULL_SLICE, the iterator
// starts at the first key in the limits for the forward forms, and the last
// key in the limits for the backward forms.
static inline splinterdb_start_position
splinterdb_start_at_or_after(slice key)
{
   return (splinterdb_start_position){true, greater_than_or_equal, key};
}
static inline splinterdb_start_position
splinterdb_start_after(slice key)
{
   return (splinterdb_start_position){true, greater_than, key};
}
static inline splinterdb_start_position
splinterdb_start_at_or_before(slice key)
{
   return (splinterdb_start_position){true, less_than_or_equal, key};
}
static inline splinterdb_start_position
splinterdb_start_before(slice key)
{
   return (splinterdb_start_position){true, less_than, key};
}
// Generic form, for when the comparison is only known at run time.
static inline splinterdb_start_position
splinterdb_make_start_position(comparison cmp, slice key)
{
   return (splinterdb_start_position){true, cmp, key};
}

// Optional parameters for splinterdb_iterator_init. A zero-initialized options
// struct (apart from struct_size) requests an ordinary unbounded, unfiltered
// forward scan. Don't build one by hand; use the splinterdb_iterator_init
// macro below, which sets struct_size and lets you name just the fields you
// need.
typedef struct splinterdb_iterator_init_options {
   // Set by the splinterdb_iterator_init macro. Lets future versions add
   // fields without breaking callers built against this one.
   size_t struct_size;

   // Range limits. Default: unbounded.
   splinterdb_lower_limit lower_limit;
   splinterdb_upper_limit upper_limit;

   // Where to start, and in which direction. Default: at or after the smallest
   // key in the limits.
   splinterdb_start_position start_position;

   // If non-NULL, the iterator returns only keys accepted by predicate.
   // A predicate without a routing_filter_key scans all branches in the range.
   splinterdb_key_predicate predicate;
   void                    *predicate_context;

   // Optional branch pruning for filtered scans. For EVERY seed, the
   // application must guarantee that every key accepted by predicate has the
   // same data_config.key_hash value as routing_filter_key. Violating this
   // contract can omit results or parts of their update histories. The routing
   // key uses the query-key representation accepted by key_hash and is copied
   // by initialization; its original storage need not remain alive.
   // NULL_SLICE (the default) disables pruning. A routing_filter_key without a
   // predicate is invalid and returns EINVAL.
   slice routing_filter_key;
} splinterdb_iterator_init_options;

// Initialize a new iterator.
//
// Filtering runs before message merging; it does not change snapshot
// isolation. Bounds, starting position, direction and errors follow the
// ordinary iterator contract. The predicate is not a value predicate and must
// not mutate the DB.
int
splinterdb_iterator_init_full(
   splinterdb                             *kvs,    // IN
   splinterdb_iterator                   **iter,   // OUT
   const splinterdb_iterator_init_options *options // IN
);

// Convenience wrapper for splinterdb_iterator_init_full. After the required
// arguments, optionally list any of the fields of
// splinterdb_iterator_init_options as designated initializers:
//
//   // Forward scan of everything
//   splinterdb_iterator_init(kvs, &it);
//
//   // Forward scan of [lo, hi), starting at the first key >= start
//   splinterdb_iterator_init(kvs, &it,
//                            .lower_limit =
//                            splinterdb_greater_than_or_equal(lo), .upper_limit
//                            = splinterdb_less_than(hi), .start_position =
//                            splinterdb_start_at_or_after(start));
//
//   // Backward scan from the end
//   splinterdb_iterator_init(kvs, &it,
//                            .start_position =
//                            splinterdb_start_at_or_before(NULL_SLICE));
//
//   splinterdb_iterator_init(kvs, &it,
//                            .predicate = my_pred,
//                            .predicate_context = &ctx);
//
// Note that commas inside field values must be parenthesized as usual for
// macro arguments.
#define splinterdb_iterator_init(kvs, iter, ...)                               \
   splinterdb_iterator_init_full(                                              \
      (kvs),                                                                   \
      (iter),                                                                  \
      &(const splinterdb_iterator_init_options){                               \
         .struct_size = sizeof(splinterdb_iterator_init_options),              \
         __VA_ARGS__})

// Deinitialize an iterator
//
// Failing to do this may cause hangs.
void
splinterdb_iterator_deinit(splinterdb_iterator *iter);

// Checks that the iterator status is OK (no errors) and that get_current()
// will succeed. If false, there are two possibilities:
// 1. Iterator is out of bounds.  In this case, status() == 0
// 2. Iterator has encountered an error.  In this case, status() != 0
_Bool
splinterdb_iterator_valid(splinterdb_iterator *iter);

/*
 * splinterdb_iterator_can_next --
 * splinterdb_iterator_can_prev --
 *
 * Knowing the iterator is invalid does not provide enough information to
 * determine if next and prev are safe operations.
 *
 * These functions provide granular information on which operations are safe.
 * splinterdb_iterator_can_next == TRUE <-> splinterdb_iterator_next is safe
 * splinterdb_iterator_can_prev == TRUE <-> splinterdb_iterator_prev is safe
 */
_Bool
splinterdb_iterator_can_prev(splinterdb_iterator *iter);

_Bool
splinterdb_iterator_can_next(splinterdb_iterator *iter);

// Moves the iterator to the previous item.
// Precondition for calling: splinterdb_iterator_can_prev() returns TRUE
// Any error will cause valid() == false and be visible with status()
void
splinterdb_iterator_prev(splinterdb_iterator *iter);

// Moves the iterator to the next item.
// Precondition for calling: splinterdb_iterator_can_next() returns TRUE
// Any error will cause valid() == false and be visible with status()
void
splinterdb_iterator_next(splinterdb_iterator *iter);

// Sets *key and *value to the locations of the current item
// Callers must not modify that memory pointed to by the slice
//
// If valid() == false, then behavior is undefined.
// Always check valid() before calling this function.
void
splinterdb_iterator_get_current(splinterdb_iterator *iter, // IN
                                slice               *key,  // OUT
                                slice               *value // OUT
);

// Returns an error encountered from iteration, or 0 if successful.
//
// End-of-range is not an error
int
splinterdb_iterator_status(const splinterdb_iterator *iter);

/*
 * Statistics Printing
 *
 * Must set the use_stats config option.
 *
 * Prints insertion or lookup statistics. Both print cache statistics.
 * Statistics printing is a best-effort snapshot: updates may proceed while
 * statistics are being read, so fields in one report may reflect different
 * points in time. Statistics must not be used for synchronization or database
 * correctness decisions.
 *
 * Reset statistics clears all statistics, including cache statistics.
 */
void
splinterdb_stats_print_insertion(const splinterdb *kvs);

void
splinterdb_stats_print_lookup(splinterdb *kvs);

void
splinterdb_stats_reset(splinterdb *kvs);

#ifdef __cplusplus
}
#endif
