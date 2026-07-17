// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * core.c --
 *
 *     This file contains the implementation for SplinterDB.
 */

#include "core.h"
#include "data_internal.h"
#include "notification.h"
#include "platform_sleep.h"
#include "platform_time.h"
#include "platform_util.h"
#include "prefetch.h"
#include "poison.h"

#define LATENCYHISTO_SIZE 15

static const int64 latency_histo_buckets[LATENCYHISTO_SIZE] = {
   1,          // 1   ns
   10,         // 10  ns
   100,        // 100 ns
   500,        // 500 ns
   1000,       // 1   us
   5000,       // 5   us
   10000,      // 10  us
   100000,     // 100 us
   500000,     // 500 us
   1000000,    // 1   ms
   5000000,    // 5   ms
   10000000,   // 10  ms
   100000000,  // 100 ms
   1000000000, // 1   s
   10000000000 // 10  s
};

/*
 * At any time, one Memtable is "active" for inserts / updates.
 * At any time, the most # of Memtables that can be active or in one of these
 * states, such as, compaction, incorporation, reclamation, is given by this
 * limit.
 */
#define CORE_NUM_MEMTABLES (4)
_Static_assert(CORE_NUM_MEMTABLES <= MAX_MEMTABLES,
               "CORE_NUM_MEMTABLES <= MAX_MEMTABLES");

/* Checkpoint metadata has independently checksummed directory and records. */
#define CORE_CHECKPOINT_DIRECTORY_CSUM_SEED (42)
#define CORE_CHECKPOINT_RECORD_CSUM_SEED    (43)

#define CORE_CHECKPOINT_FORMAT_VERSION (2)
#define CORE_CHECKPOINT_RECORD_COUNT   (2)

#define CORE_CHECKPOINT_DIRECTORY_MAGIC (0x534442434B505444ULL) // SDBCKPTD
#define CORE_CHECKPOINT_RECORD_MAGIC    (0x534442434B505452ULL) // SDBCKPTR

static platform_status
core_checkpoint_lock_init(core_handle *spl)
{
   platform_status rc = platform_mutex_init(&spl->checkpoint_lock,
                                            platform_get_module_id(),
                                            spl->heap_id);
   if (SUCCESS(rc)) {
      spl->checkpoint_lock_initialized = TRUE;
   }
   return rc;
}

static void
core_checkpoint_lock_deinit(core_handle *spl)
{
   if (!spl->checkpoint_lock_initialized) {
      return;
   }
   platform_status rc = platform_mutex_destroy(&spl->checkpoint_lock);
   platform_assert_status_ok(rc);
   spl->checkpoint_lock_initialized = FALSE;
}

/*
 * core logging functions.
 *
 * If verbose_logging_enabled is enabled in core_config, these functions print
 * to cfg->log_handle.
 */

static inline bool32
core_verbose_logging_enabled(core_handle *spl)
{
   return spl->cfg.verbose_logging_enabled;
}

static inline platform_log_handle *
core_log_handle(core_handle *spl)
{
   platform_assert(core_verbose_logging_enabled(spl));
   platform_assert(spl->cfg.log_handle != NULL);
   return spl->cfg.log_handle;
}

static inline platform_status
core_open_log_stream_if_enabled(core_handle            *spl,
                                platform_stream_handle *stream)
{
   if (core_verbose_logging_enabled(spl)) {
      return platform_open_log_stream(stream);
   }
   return STATUS_OK;
}

static inline void
core_close_log_stream_if_enabled(core_handle            *spl,
                                 platform_stream_handle *stream)
{
   if (core_verbose_logging_enabled(spl)) {
      platform_assert(stream != NULL);
      platform_close_log_stream(stream, core_log_handle(spl));
   }
}

#define core_log_stream_if_enabled(spl, _stream, message, ...)                 \
   do {                                                                        \
      if (core_verbose_logging_enabled(spl)) {                                 \
         platform_log_stream(                                                  \
            (_stream), "[%3lu] " message, platform_get_tid(), ##__VA_ARGS__);  \
      }                                                                        \
   } while (0)

#define core_default_log_if_enabled(spl, message, ...)                         \
   do {                                                                        \
      if (core_verbose_logging_enabled(spl)) {                                 \
         platform_default_log(message, __VA_ARGS__);                           \
      }                                                                        \
   } while (0)

/*
 *-----------------------------------------------------------------------------
 * Checkpoint metadata: disk-resident structures.
 *
 * allocator_get_super_addr() identifies a fixed page in the allocator's
 * bootstrap extent.  That page is an immutable directory, written exactly
 * once when the table is created.  The directory names two independently
 * allocated record extents.  Checkpoint publication alternates between their
 * first pages, leaving one formerly valid record untouched if a new write is
 * torn.
 *
 * This is intentionally a new on-disk format.  Do not interpret a legacy
 * core_super_block as a directory: doing so would turn arbitrary old fields
 * into allocator-owned addresses.  Existing databases must be migrated or
 * reformatted before using this checkpoint metadata format.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK core_checkpoint_directory {
   uint64      magic;
   uint64      format_version;
   uint64      table_id;
   uint64      record_addr[CORE_CHECKPOINT_RECORD_COUNT];
   checksum128 checksum;
} core_checkpoint_directory;

typedef struct ONDISK core_checkpoint_record {
   /*
    * The highest memtable generation incorporated in root_addr.  The boolean
    * keeps the fresh-database case distinct from generation zero.
    */
   uint64 incorporated_generation;
   bool32 has_incorporated_generation;
   uint64 root_addr;
   uint64 timestamp;
   uint64 sequence;
   uint64 table_id;
   uint32 record_slot;
   bool32 checkpointed;
   bool32 unmounted;
   uint64 magic;
   uint64 format_version;
   checksum128 checksum;
} core_checkpoint_record;

typedef struct core_checkpoint_records {
   core_checkpoint_record record[CORE_CHECKPOINT_RECORD_COUNT];
   bool32                 valid[CORE_CHECKPOINT_RECORD_COUNT];
   bool32                 have_newest;
   uint64                 newest_slot;
   bool32                 have_newest_unmounted;
   uint64                 newest_unmounted_slot;
} core_checkpoint_records;

static checksum128
core_checkpoint_directory_checksum(const core_checkpoint_directory *directory)
{
   return platform_checksum128(directory,
                               offsetof(core_checkpoint_directory, checksum),
                               CORE_CHECKPOINT_DIRECTORY_CSUM_SEED);
}

static checksum128
core_checkpoint_record_checksum(const core_checkpoint_record *record)
{
   return platform_checksum128(record,
                               offsetof(core_checkpoint_record, checksum),
                               CORE_CHECKPOINT_RECORD_CSUM_SEED);
}

static bool32
core_checkpoint_record_addr_is_valid(core_handle *spl, uint64 addr)
{
   allocator_config *allocator_cfg = allocator_get_config(spl->al);
   uint64            page_size     = cache_page_size(spl->cc);

   return addr != 0 && addr % allocator_cfg->io_cfg->extent_size == 0
          && addr < allocator_cfg->capacity
          && page_size <= allocator_cfg->capacity - addr;
}

static bool32
core_checkpoint_directory_is_valid(core_handle                        *spl,
                                   const core_checkpoint_directory *directory)
{
   if (directory->magic != CORE_CHECKPOINT_DIRECTORY_MAGIC
       || directory->format_version != CORE_CHECKPOINT_FORMAT_VERSION
       || directory->table_id != spl->id
       || !platform_checksum_is_equal(
          directory->checksum, core_checkpoint_directory_checksum(directory)))
   {
      return FALSE;
   }

   uint64 record0 = directory->record_addr[0];
   uint64 record1 = directory->record_addr[1];
   allocator_config *allocator_cfg = allocator_get_config(spl->al);
   return core_checkpoint_record_addr_is_valid(spl, record0)
          && core_checkpoint_record_addr_is_valid(spl, record1)
          && record0 != record1
          && !allocator_config_pages_share_extent(allocator_cfg, record0, record1);
}

static bool32
core_checkpoint_record_is_valid(core_handle                     *spl,
                                const core_checkpoint_record *record,
                                uint64                           record_slot)
{
   return record->magic == CORE_CHECKPOINT_RECORD_MAGIC
          && record->format_version == CORE_CHECKPOINT_FORMAT_VERSION
          && record->table_id == spl->id && record->record_slot == record_slot
          && record->sequence != 0
          && (record->has_incorporated_generation == FALSE
              || record->has_incorporated_generation == TRUE)
          && (record->has_incorporated_generation
                 ? record->incorporated_generation < UINT64_MAX
                 : record->incorporated_generation == 0)
          && platform_checksum_is_equal(
             record->checksum, core_checkpoint_record_checksum(record));
}

static void
core_write_checkpoint_page(core_handle *spl,
                           uint64       page_addr,
                           const void  *contents,
                           uint64       contents_size)
{
   page_handle *page =
      cache_get(spl->cc, page_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   uint64 wait = 1;
   while (!cache_try_claim(spl->cc, page)) {
      cache_unget(spl->cc, page);
      platform_sleep_ns(wait);
      wait = wait > 1024 ? wait : 2 * wait;
      page = cache_get(spl->cc, page_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   }
   cache_lock(spl->cc, page);
   platform_assert(contents_size <= cache_page_size(spl->cc));
   memset(page->data, 0, cache_page_size(spl->cc));
   memcpy(page->data, contents, contents_size);
   cache_unlock(spl->cc, page);
   cache_unclaim(spl->cc, page);
   cache_page_writeback(spl->cc, page, TRUE, PAGE_TYPE_SUPERBLOCK);
   cache_unget(spl->cc, page);
}

static void
core_initialize_checkpoint_record_page(core_handle *spl, uint64 page_addr)
{
   page_handle *page = cache_alloc(spl->cc, page_addr, PAGE_TYPE_SUPERBLOCK);
   platform_assert(page != NULL);
   memset(page->data, 0, cache_page_size(spl->cc));
   cache_unlock(spl->cc, page);
   cache_unclaim(spl->cc, page);
   cache_page_writeback(spl->cc, page, TRUE, PAGE_TYPE_SUPERBLOCK);
   cache_unget(spl->cc, page);
}

static platform_status
core_create_checkpoint_directory(core_handle                 *spl,
                                 core_checkpoint_directory *directory)
{
   uint64          directory_addr;
   platform_status rc =
      allocator_alloc_super_addr(spl->al, spl->id, &directory_addr);
   if (!SUCCESS(rc)) {
      platform_error_log("core_create_checkpoint_directory: failed to allocate "
                         "directory address for root id %lu: %s\n",
                         spl->id,
                         platform_status_to_string(rc));
      return rc;
   }

   ZERO_CONTENTS(directory);
   directory->magic          = CORE_CHECKPOINT_DIRECTORY_MAGIC;
   directory->format_version = CORE_CHECKPOINT_FORMAT_VERSION;
   directory->table_id       = spl->id;

   for (uint64 slot = 0; slot < CORE_CHECKPOINT_RECORD_COUNT; slot++) {
      uint64 record_addr;
      rc = allocator_alloc(spl->al, &record_addr, PAGE_TYPE_SUPERBLOCK);
      if (!SUCCESS(rc)) {
         platform_error_log("core_create_checkpoint_directory: failed to "
                            "allocate record extent %lu: %s\n",
                            slot,
                            platform_status_to_string(rc));
         return rc;
      }
      directory->record_addr[slot] = record_addr;
      core_initialize_checkpoint_record_page(spl, record_addr);
   }

   directory->checksum = core_checkpoint_directory_checksum(directory);
   core_write_checkpoint_page(
      spl, directory_addr, directory, sizeof(*directory));

   /*
    * Submit the newly initialized record and directory pages before the
    * durable barrier. allocator_alloc() changes its refcount map only in
    * memory; crash recovery deliberately rebuilds that map instead of relying
    * on this publication. allocator_alloc_super_addr() does write the raw
    * bootstrap mapping through the shared backing I/O handle, which the
    * following durable barrier fdatasyncs with the directory page.
    */
   rc = cache_writeback_fence(spl->cc);
   if (!SUCCESS(rc)) {
      return rc;
   }
   return cache_durable_barrier(spl->cc);
}

static platform_status
core_get_checkpoint_directory(core_handle                 *spl,
                              core_checkpoint_directory *directory)
{
   uint64          directory_addr;
   platform_status rc =
      allocator_get_super_addr(spl->al, spl->id, &directory_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }

   page_handle *page =
      cache_get(spl->cc, directory_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   memcpy(directory, page->data, sizeof(*directory));
   cache_unget(spl->cc, page);

   if (!core_checkpoint_directory_is_valid(spl, directory)) {
      platform_error_log("core_get_checkpoint_directory: no compatible "
                         "checkpoint directory for root id %lu\n",
                         spl->id);
      return STATUS_BAD_PARAM;
   }
   return STATUS_OK;
}

static platform_status
core_load_checkpoint_records(core_handle                       *spl,
                             const core_checkpoint_directory *directory,
                             core_checkpoint_records          *records)
{
   ZERO_CONTENTS(records);
   for (uint64 slot = 0; slot < CORE_CHECKPOINT_RECORD_COUNT; slot++) {
      page_handle *page = cache_get(spl->cc,
                                    directory->record_addr[slot],
                                    TRUE,
                                    PAGE_TYPE_SUPERBLOCK);
      memcpy(&records->record[slot], page->data, sizeof(records->record[slot]));
      cache_unget(spl->cc, page);

      records->valid[slot] = core_checkpoint_record_is_valid(
         spl, &records->record[slot], slot);
      if (!records->valid[slot]) {
         continue;
      }

      if (!records->have_newest
          || records->record[records->newest_slot].sequence
                < records->record[slot].sequence)
      {
         records->have_newest = TRUE;
         records->newest_slot = slot;
      }
      if (records->record[slot].unmounted
          && (!records->have_newest_unmounted
              || records->record[records->newest_unmounted_slot].sequence
                    < records->record[slot].sequence))
      {
         records->have_newest_unmounted = TRUE;
         records->newest_unmounted_slot = slot;
      }
   }

   if (records->valid[0] && records->valid[1]
       && records->record[0].sequence == records->record[1].sequence)
   {
      platform_error_log("core_load_checkpoint_records: duplicate record "
                         "sequence %lu for root id %lu\n",
                         records->record[0].sequence,
                         spl->id);
      return STATUS_BAD_PARAM;
   }
   return STATUS_OK;
}

static void
core_destroy_checkpoint_record_extent(core_handle *spl, uint64 record_addr)
{
   refcount ref =
      allocator_dec_ref(spl->al, record_addr, PAGE_TYPE_SUPERBLOCK);
   if (ref != AL_NO_REFS) {
      platform_error_log("core_destroy_checkpoint_record_extent: record extent "
                         "%lu has unexpected refcount %u\n",
                         record_addr,
                         ref);
      return;
   }

   cache_extent_discard(spl->cc, record_addr, PAGE_TYPE_SUPERBLOCK);
   ref = allocator_dec_ref(spl->al, record_addr, PAGE_TYPE_SUPERBLOCK);
   platform_assert(ref == AL_FREE);
}

static void
core_destroy_checkpoint_storage(core_handle *spl)
{
   core_checkpoint_directory directory;
   platform_status rc = core_get_checkpoint_directory(spl, &directory);
   if (!SUCCESS(rc)) {
      platform_error_log("core_destroy_checkpoint_storage: unable to load "
                         "checkpoint directory for root id %lu: %s\n",
                         spl->id,
                         platform_status_to_string(rc));
      return;
   }

   core_checkpoint_records records;
   rc = core_load_checkpoint_records(spl, &directory, &records);
   if (!SUCCESS(rc)) {
      platform_error_log("core_destroy_checkpoint_storage: unable to load "
                         "checkpoint records for root id %lu: %s\n",
                         spl->id,
                         platform_status_to_string(rc));
      return;
   }

   /*
    * Both valid slots own independent root references. Keeping the older
    * record live makes it a real fallback if the next record write is torn;
    * its reference is released only when that slot is successfully
    * overwritten. This is clean destruction, so release both record owners.
    */
   for (uint64 slot = 0; slot < CORE_CHECKPOINT_RECORD_COUNT; slot++) {
      if (!records.valid[slot] || records.record[slot].root_addr == 0) {
         continue;
      }
      rc = trunk_dec_ref(spl->cfg.trunk_node_cfg,
                         PROCESS_PRIVATE_HEAP_ID,
                         spl->cc,
                         spl->al,
                         spl->ts,
                         records.record[slot].root_addr);
      if (!SUCCESS(rc)) {
         platform_error_log("core_destroy_checkpoint_storage: failed to "
                            "release record %lu root %lu: %s\n",
                            slot,
                            records.record[slot].root_addr,
                            platform_status_to_string(rc));
      }
   }

   for (uint64 slot = 0; slot < CORE_CHECKPOINT_RECORD_COUNT; slot++) {
      core_destroy_checkpoint_record_extent(spl, directory.record_addr[slot]);
   }
}

/*
 *-----------------------------------------------------------------------------
 * Checkpoint record functions
 *-----------------------------------------------------------------------------
 */
static platform_status
core_capture_checkpoint_cut(core_handle    *spl,
                            trunk_snapshot *snapshot,
                            bool32         *has_incorporated_generation,
                            uint64         *incorporated_generation)
{
   /*
    * Incorporation publishes its generation and root while holding lookup
    * exclusion before taking the trunk root lock.  Take the checkpoint cut
    * in the same order, so a record never combines a pre-incorporation
    * generation with a post-incorporation root (or the converse).
    */
   memtable_block_lookups(&spl->mt_ctxt);
   uint64 retired_generation = memtable_generation_retired(&spl->mt_ctxt);
   platform_status rc = trunk_snapshot_acquire(&spl->trunk_context, snapshot);
   memtable_unblock_lookups(&spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      return rc;
   }

   *has_incorporated_generation = retired_generation != UINT64_MAX;
   *incorporated_generation = *has_incorporated_generation
                                 ? retired_generation
                                 : 0;
   return STATUS_OK;
}

static platform_status
core_publish_checkpoint_record(core_handle *spl,
                               bool32       is_checkpoint,
                               bool32       is_unmount,
                               bool32       is_create)
{
   uint64          old_root_addr;
   platform_status rc;
   trunk_snapshot snapshot;
   bool32         has_incorporated_generation;
   uint64         incorporated_generation;
   core_checkpoint_directory directory;
   core_checkpoint_records   records;
   uint64                    target_slot;
   core_checkpoint_record    record;

   /*
    * The snapshot, target-slot selection, durable record write, and old-slot
    * release are one publication transaction. In particular, two concurrent
    * publishers must never choose the same target slot or release the same
    * former record owner.
    */
   rc = platform_mutex_lock(&spl->checkpoint_lock);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = core_capture_checkpoint_cut(spl,
                                    &snapshot,
                                    &has_incorporated_generation,
                                    &incorporated_generation);
   if (!SUCCESS(rc)) {
      goto unlock_checkpoint;
   }

   /*
    * The snapshot reference makes the root stable, but not necessarily
    * durable. Drain only the cache intervals that existed at this cut before
    * making a record that can name the root durable. This can incidentally
    * persist newer log/data pages, but it does not seal or publish a logical
    * durable-log tail; tail sync is a separate operation.
    */
   rc = trunk_make_durable(&spl->trunk_context);
   if (!SUCCESS(rc)) {
      goto release_snapshot;
   }

   if (is_create) {
      rc = core_create_checkpoint_directory(spl, &directory);
   } else {
      rc = core_get_checkpoint_directory(spl, &directory);
   }
   if (!SUCCESS(rc)) {
      platform_error_log("core_publish_checkpoint_record: failed to %s "
                         "checkpoint directory for root id %lu: %s\n",
                         is_create ? "create" : "load",
                         spl->id,
                         platform_status_to_string(rc));
      goto release_snapshot;
   }

   rc = core_load_checkpoint_records(spl, &directory, &records);
   if (!SUCCESS(rc)) {
      goto release_snapshot;
   }

   if (records.have_newest
       && records.record[records.newest_slot].sequence == UINT64_MAX)
   {
      rc = STATUS_LIMIT_EXCEEDED;
      goto release_snapshot;
   }
   target_slot = records.have_newest ? records.newest_slot ^ 1 : 0;
   /*
    * Each valid record owns its root reference. This publication overwrites
    * target_slot, so retain that slot's old root until the replacement page
    * is durable, then release only the overwritten owner. The newest record
    * remains independently live as the torn-write fallback.
    */
   old_root_addr = records.valid[target_slot]
                      ? records.record[target_slot].root_addr
                      : 0;

   ZERO_CONTENTS(&record);
   record.incorporated_generation     = incorporated_generation;
   record.has_incorporated_generation = has_incorporated_generation;
   record.root_addr                    = snapshot.root_addr;
   record.timestamp                    = platform_get_real_time();
   record.sequence = records.have_newest
                        ? records.record[records.newest_slot].sequence + 1
                        : 1;
   record.table_id                     = spl->id;
   record.record_slot                  = target_slot;
   record.checkpointed                 = is_checkpoint;
   record.unmounted                    = is_unmount;
   record.magic                        = CORE_CHECKPOINT_RECORD_MAGIC;
   record.format_version               = CORE_CHECKPOINT_FORMAT_VERSION;

   record.checksum = core_checkpoint_record_checksum(&record);

   core_write_checkpoint_page(spl,
                              directory.record_addr[target_slot],
                              &record,
                              sizeof(record));
   /* The record now owns this reference, even if the barrier reports failure. */
   snapshot.root_addr = 0;

   rc = cache_durable_barrier(spl->cc);
   if (!SUCCESS(rc)) {
      /* The new record may be durable, so retain its transferred root ref. */
      goto unlock_checkpoint;
   }

   if (old_root_addr != 0) {
      rc = trunk_dec_ref(spl->cfg.trunk_node_cfg,
                         PROCESS_PRIVATE_HEAP_ID,
                         spl->cc,
                         spl->al,
                         spl->ts,
                         old_root_addr);
      if (!SUCCESS(rc)) {
         platform_error_log("core_publish_checkpoint_record: trunk_dec_ref "
                            "failed for old root addr %lu: %s\n",
                            old_root_addr,
                            platform_status_to_string(rc));
         goto unlock_checkpoint;
      }
   }

   rc = STATUS_OK;
   goto unlock_checkpoint;

release_snapshot:
   {
      platform_status release_rc =
         trunk_snapshot_release(&spl->trunk_context, &snapshot);
      if (SUCCESS(rc) && !SUCCESS(release_rc)) {
         rc = release_rc;
      }
   }

unlock_checkpoint:
   {
      platform_status unlock_rc = platform_mutex_unlock(&spl->checkpoint_lock);
      if (SUCCESS(rc) && !SUCCESS(unlock_rc)) {
         rc = unlock_rc;
      }
   }
   return rc;
}

/*
 *-----------------------------------------------------------------------------
 * Memtable Functions
 *-----------------------------------------------------------------------------
 */

static memtable *
core_try_get_memtable(core_handle *spl, uint64 generation)
{
   uint64    memtable_idx = generation % CORE_NUM_MEMTABLES;
   memtable *mt           = &spl->mt_ctxt.mt[memtable_idx];
   if (mt->generation != generation) {
      mt = NULL;
   }
   return mt;
}

/*
 * returns the memtable with generation number generation. Caller must ensure
 * that there exists a memtable with the appropriate generation.
 */
static memtable *
core_get_memtable(core_handle *spl, uint64 generation)
{
   uint64    memtable_idx = generation % CORE_NUM_MEMTABLES;
   memtable *mt           = &spl->mt_ctxt.mt[memtable_idx];
   platform_assert(mt->generation == generation,
                   "mt->generation=%lu, mt_ctxt->generation=%lu, "
                   "mt_ctxt->generation_retired=%lu, generation=%lu\n",
                   mt->generation,
                   spl->mt_ctxt.generation,
                   spl->mt_ctxt.generation_retired,
                   generation);
   return mt;
}

static core_compacted_memtable *
core_get_compacted_memtable(core_handle *spl, uint64 generation)
{
   uint64 memtable_idx = generation % CORE_NUM_MEMTABLES;

   // this call asserts the generation is correct
   memtable *mt = core_get_memtable(spl, generation);
   platform_assert(mt->state != MEMTABLE_STATE_READY);

   return &spl->compacted_memtable[memtable_idx];
}

static bool32
core_memtable_state_has_compacted_branch(memtable_state state)
{
   return state == MEMTABLE_STATE_COMPACTED
          || state == MEMTABLE_STATE_INCORPORATION_ASSIGNED
          || state == MEMTABLE_STATE_INCORPORATION_FAILED;
}

static uint64
core_memtable_compacted_branch_root(core_handle *spl, uint64 generation)
{
   memtable *mt = core_get_memtable(spl, generation);
   if (!core_memtable_state_has_compacted_branch(mt->state)) {
      return 0;
   }

   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   return cmt->branch.root_addr;
}

static void
core_memtable_release_compacted_branch(core_handle *spl, uint64 generation)
{
   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   if (cmt->branch.root_addr == 0) {
      return;
   }

   btree_dec_ref(
      spl->cc, spl->cfg.btree_cfg, cmt->branch.root_addr, PAGE_TYPE_BRANCH);
   cmt->branch.root_addr = 0;
}

static void
core_memtable_mark_incorporation_failed(core_handle    *spl,
                                        uint64          generation,
                                        platform_status status)
{
   memtable_block_lookups(&spl->mt_ctxt);
   memtable *mt = core_get_memtable(spl, generation);
   platform_error_log("Memtable incorporation failed: generation=%lu "
                      "state=%s status=%s memtable_root=%lu\n",
                      generation,
                      memtable_state_string(mt->state),
                      platform_status_to_string(status),
                      mt->root_addr);
   memtable_mark_incorporation_failed(mt, status);
   core_memtable_release_compacted_branch(spl, generation);
   memtable_unblock_lookups(&spl->mt_ctxt);
}

static inline void
core_memtable_inc_ref(core_handle *spl, uint64 root_addr)
{
   memtable_root_inc_ref(&spl->mt_ctxt, root_addr);
}


static void
core_memtable_dec_ref(core_handle *spl, uint64 root_addr)
{
   memtable_root_dec_ref(&spl->mt_ctxt, root_addr);
}


/* Wrappers for creating/destroying memtable btree iterators. */
static platform_status
core_memtable_iterator_init(core_handle    *spl,
                            btree_iterator *itor,
                            uint64          root_addr,
                            comparison      min_key_comparison,
                            key             min_key,
                            comparison      max_key_comparison,
                            key             max_key,
                            comparison      start_key_comparison,
                            key             start_key)
{
   return btree_iterator_init(spl->cc,
                              spl->cfg.btree_cfg,
                              itor,
                              root_addr,
                              PAGE_TYPE_MEMTABLE,
                              min_key_comparison,
                              min_key,
                              max_key_comparison,
                              max_key,
                              start_key_comparison,
                              start_key,
                              FALSE,
                              0,
                              0);
}

static void
core_memtable_iterator_deinit(btree_iterator *itor)
{
   btree_iterator_deinit(itor);
}

/*
 * On success, returns the current memtable with the insert lock held. The
 * caller must release it with memtable_end_insert().
 */
static platform_status
core_begin_memtable_insert(core_handle *spl, uint64 *generation, memtable **mt)
{
   platform_status rc =
      memtable_maybe_rotate_and_begin_insert(&spl->mt_ctxt, generation);
   while (STATUS_IS_EQ(rc, STATUS_BUSY)) {
      // Memtable isn't ready, do a task if available; may be required to
      // incorporate memtable that we're waiting on
      task_perform_one_if_needed(spl->ts, 0);
      rc = memtable_maybe_rotate_and_begin_insert(&spl->mt_ctxt, generation);
   }
   if (!SUCCESS(rc)) {
      return rc;
   }

   // this call is safe because we hold the insert lock
   *mt = core_get_memtable(spl, *generation);
   return STATUS_OK;
}

static platform_status
core_log_insert(core_handle                *spl,
                uint64                      memtable_generation,
                key                         tuple_key,
                message                     msg,
                const btree_insert_results *insert_results)
{
   /* TODO: FIXME: One way we could get stuck in a fetch-and-update is if the
    * insert succeeds but the lookup fails (e.g. due to an I/O error while
    * traversing the trunk).  I think the promise we should make in that case is
    * that we will preserve enough information in the log to enable the user
    * to recover the old value. One way to do this might be to insert a
    * reference to the trunk into the log. */
   if (!spl->cfg.use_log) {
      return STATUS_OK;
   }

   message log_msg =
      merge_accumulator_is_null(&insert_results->msg_blob)
         ? msg
         : merge_accumulator_to_message(&insert_results->msg_blob);
   int log_rc = log_write(spl->log,
                          tuple_key,
                          log_msg,
                          memtable_generation,
                          insert_results->leaf_generation);
   return log_rc == 0 ? STATUS_OK : (platform_status){.r = log_rc};
}

/*
 * Compacts the memtable with generation generation and builds its filter.
 * Returns a pointer to the memtable.
 */
static memtable *
core_memtable_compact(core_handle *spl, uint64 generation, const threadid tid)
{
   timestamp comp_start = platform_get_timestamp();

   memtable *mt = core_get_memtable(spl, generation);

   memtable_transition(mt, MEMTABLE_STATE_FINALIZED, MEMTABLE_STATE_COMPACTING);
   mini_release(&mt->mini);

   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   core_branch             *new_branch = &cmt->branch;
   ZERO_CONTENTS(new_branch);

   uint64         memtable_root_addr = mt->root_addr;
   btree_iterator btree_itor;
   iterator      *itor = &btree_itor.super;

   platform_status rc = core_memtable_iterator_init(spl,
                                                    &btree_itor,
                                                    memtable_root_addr,
                                                    greater_than_or_equal,
                                                    NEGATIVE_INFINITY_KEY,
                                                    less_than,
                                                    POSITIVE_INFINITY_KEY,
                                                    greater_than_or_equal,
                                                    NEGATIVE_INFINITY_KEY);
   platform_assert_status_ok(rc);
   const routing_config *rfcfg = spl->cfg.trunk_node_cfg->filter_cfg;
   uint64 rflimit = routing_filter_max_fingerprints(spl->cfg.cache_cfg, rfcfg);
   btree_pack_req req;
   btree_pack_req_init(&req,
                       spl->cc,
                       spl->cfg.btree_cfg,
                       itor,
                       rflimit,
                       rfcfg->seed,
                       FALSE,
                       spl->heap_id);
   uint64 pack_start;
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_compactions++;
      pack_start = platform_get_timestamp();
   }

   platform_status pack_status = btree_pack(&req);
   platform_assert(SUCCESS(pack_status),
                   "platform_status of btree_pack: %d\n",
                   pack_status.r);

   platform_assert(req.num_tuples <= rflimit);
   if (spl->cfg.use_stats) {
      spl->stats[tid].root_compaction_pack_time_ns +=
         platform_timestamp_elapsed(pack_start);
      spl->stats[tid].root_compaction_tuples += req.num_tuples;
      if (req.num_tuples > spl->stats[tid].root_compaction_max_tuples) {
         spl->stats[tid].root_compaction_max_tuples = req.num_tuples;
      }
   }
   core_memtable_iterator_deinit(&btree_itor);

   new_branch->root_addr = req.root_addr;

   platform_assert(req.num_tuples > 0);

   btree_pack_req_deinit(&req, spl->heap_id);
   if (spl->cfg.use_stats) {
      uint64 comp_time = platform_timestamp_elapsed(comp_start);
      spl->stats[tid].root_compaction_time_ns += comp_time;
      if (comp_start > spl->stats[tid].root_compaction_time_max_ns) {
         spl->stats[tid].root_compaction_time_max_ns = comp_time;
      }
      cmt->wait_start = platform_get_timestamp();
   }

   memtable_transition(mt, MEMTABLE_STATE_COMPACTING, MEMTABLE_STATE_COMPACTED);
   return mt;
}

/*
 * Cases:
 * 1. memtable set to COMP before try_continue tries to set it to incorp
 *       try_continue will successfully assign itself to incorp the memtable
 * 2. memtable set to COMP after try_continue tries to set it to incorp
 *       should_wait will be set to generation, so try_start will incorp
 */
static inline bool32
core_try_start_incorporate(core_handle *spl, uint64 generation)
{
   bool32 should_start = FALSE;

   memtable_lock_incorporation_lock(&spl->mt_ctxt);
   memtable *mt = core_try_get_memtable(spl, generation);
   if ((mt == NULL)
       || (generation != memtable_generation_to_incorporate(&spl->mt_ctxt)))
   {
      should_start = FALSE;
      goto unlock_incorp_lock;
   }
   should_start = memtable_try_transition(
      mt, MEMTABLE_STATE_COMPACTED, MEMTABLE_STATE_INCORPORATION_ASSIGNED);

unlock_incorp_lock:
   memtable_unlock_incorporation_lock(&spl->mt_ctxt);
   return should_start;
}

static inline bool32
core_try_continue_incorporate(core_handle *spl, uint64 next_generation)
{
   bool32 should_continue = FALSE;

   memtable_lock_incorporation_lock(&spl->mt_ctxt);
   memtable *mt = core_try_get_memtable(spl, next_generation);
   if (mt == NULL) {
      should_continue = FALSE;
      goto unlock_incorp_lock;
   }
   should_continue = memtable_try_transition(
      mt, MEMTABLE_STATE_COMPACTED, MEMTABLE_STATE_INCORPORATION_ASSIGNED);
   memtable_increment_to_generation_to_incorporate(&spl->mt_ctxt,
                                                   next_generation);

unlock_incorp_lock:
   memtable_unlock_incorporation_lock(&spl->mt_ctxt);
   return should_continue;
}

static platform_status
core_memtable_incorporate(core_handle   *spl,
                          uint64         generation,
                          const threadid tid)
{
   platform_stream_handle stream;
   platform_status        rc = core_open_log_stream_if_enabled(spl, &stream);
   if (!SUCCESS(rc)) {
      platform_error_log("core_memtable_incorporate: failed to open log "
                         "stream for generation %lu: %s\n",
                         generation,
                         platform_status_to_string(rc));
      core_memtable_mark_incorporation_failed(spl, generation, rc);
      return rc;
   }
   core_log_stream_if_enabled(
      spl, &stream, "incorporate memtable gen %lu\n", generation);
   core_log_stream_if_enabled(
      spl, &stream, "----------------------------------------\n");

   // Add the memtable to the new root as a new compacted bundle
   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   uint64                   flush_start;
   if (spl->cfg.use_stats) {
      flush_start = platform_get_timestamp();
   }
   rc = trunk_incorporate_prepare(&spl->trunk_context, cmt->branch.root_addr);
   if (!SUCCESS(rc)) {
      platform_error_log("trunk_incorporate_prepare failed: %s\n",
                         platform_status_to_string(rc));
      core_close_log_stream_if_enabled(spl, &stream);
      core_memtable_mark_incorporation_failed(spl, generation, rc);
      return rc;
   }
   btree_dec_ref(
      spl->cc, spl->cfg.btree_cfg, cmt->branch.root_addr, PAGE_TYPE_BRANCH);
   if (spl->cfg.use_stats) {
      spl->stats[tid].memtable_flush_wait_time_ns +=
         platform_timestamp_elapsed(cmt->wait_start);
   }

   core_log_stream_if_enabled(
      spl, &stream, "----------------------------------------\n");
   core_log_stream_if_enabled(spl, &stream, "\n");

   /*
    * Lock the lookup lock, blocking lookups.
    * Transition memtable state and increment memtable generation (blocks
    * lookups from accessing the memtable that's being incorporated).
    * And switch to the new root of the trunk.
    */
   memtable_block_lookups(&spl->mt_ctxt);
   memtable *mt = core_get_memtable(spl, generation);
   // Normally need to hold incorp_mutex, but debug code and also guaranteed no
   // one is changing gen_to_incorp (we are the only thread that would try)
   debug_assert(generation
                == memtable_generation_to_incorporate(&spl->mt_ctxt));
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATION_ASSIGNED, MEMTABLE_STATE_INCORPORATING);
   memtable_transition(
      mt, MEMTABLE_STATE_INCORPORATING, MEMTABLE_STATE_INCORPORATED);
   memtable_increment_to_generation_retired(&spl->mt_ctxt, generation);
   trunk_incorporate_commit(&spl->trunk_context);
   memtable_unblock_lookups(&spl->mt_ctxt);

   trunk_incorporate_cleanup(&spl->trunk_context);

   core_close_log_stream_if_enabled(spl, &stream);

   memtable_recycle(&spl->mt_ctxt, mt);

   if (spl->cfg.use_stats) {
      const threadid tid = platform_get_tid();
      flush_start        = platform_timestamp_elapsed(flush_start);
      spl->stats[tid].memtable_flush_time_ns += flush_start;
      spl->stats[tid].memtable_flushes++;
      if (flush_start > spl->stats[tid].memtable_flush_time_max_ns) {
         spl->stats[tid].memtable_flush_time_max_ns = flush_start;
      }
   }

   return STATUS_OK;
}

/*
 * Main wrapper function to carry out incorporation of a memtable.
 *
 * If background threads are disabled this function is called inline in the
 * context of the foreground thread.  If background threads are enabled, this
 * function is called in the context of the memtable worker thread.
 */
static platform_status
core_memtable_flush_internal(core_handle *spl, uint64 generation)
{
   const threadid tid = platform_get_tid();
   // pack and build filter.
   core_memtable_compact(spl, generation, tid);

   // If we are assigned to do so, incorporate the memtable onto the root node.
   if (!core_try_start_incorporate(spl, generation)) {
      goto out;
   }
   do {
      platform_status rc = core_memtable_incorporate(spl, generation, tid);
      if (!SUCCESS(rc)) {
         return rc;
      }
      generation++;
   } while (core_try_continue_incorporate(spl, generation));
out:
   return STATUS_OK;
}

static void
core_memtable_flush_internal_virtual(task *arg)
{
   core_memtable_args *mt_args = container_of(arg, core_memtable_args, tsk);
   platform_status     rc =
      core_memtable_flush_internal(mt_args->spl, mt_args->generation);
   if (!SUCCESS(rc)) {
      platform_error_log("memtable flush failed: %s\n",
                         platform_status_to_string(rc));
   }
}

/*
 * Function to trigger a memtable incorporation. Called in the context of
 * the foreground doing insertions.
 * If background threads are not enabled, this function does the entire memtable
 * incorporation inline.
 * If background threads are enabled, this function just queues up the task to
 * carry out the incorporation, swaps the curr_memtable pointer, claims the
 * root and returns.
 */
static void
core_memtable_flush(core_handle *spl, uint64 generation)
{
   core_compacted_memtable *cmt = core_get_compacted_memtable(spl, generation);
   cmt->mt_args.spl             = spl;
   cmt->mt_args.generation      = generation;
   task_enqueue(spl->ts,
                TASK_TYPE_MEMTABLE,
                &cmt->mt_args.tsk,
                core_memtable_flush_internal_virtual,
                FALSE);
}

static void
core_memtable_flush_virtual(void *arg, uint64 generation)
{
   core_handle *spl = arg;
   core_memtable_flush(spl, generation);
}

static inline uint64
core_memtable_root_addr_for_lookup(core_handle *spl,
                                   uint64       generation,
                                   bool32      *is_compacted,
                                   bool32      *is_active)
{
   memtable *mt = core_get_memtable(spl, generation);
   platform_assert(memtable_ok_to_lookup(mt));
   if (is_active != NULL) {
      *is_active = mt->state == MEMTABLE_STATE_READY;
   }

   if (memtable_ok_to_lookup_compacted(mt)) {
      // lookup in packed tree
      *is_compacted = TRUE;
      if (is_active != NULL) {
         *is_active = FALSE;
      }
      core_compacted_memtable *cmt =
         core_get_compacted_memtable(spl, generation);
      return cmt->branch.root_addr;
   } else {
      *is_compacted = FALSE;
      return mt->root_addr;
   }
}

/*
 * core_memtable_lookup
 *
 * Pre-conditions:
 *    If *found
 *       `data` has the most recent answer.
 *       the current memtable is older than the most recent answer
 *
 * Post-conditions:
 *    if *found, the data can be found in `data`.
 */
static platform_status
core_memtable_lookup(core_handle   *spl,
                     uint64         generation,
                     key            target,
                     lookup_result *result)
{
   cache *const        cc  = spl->cc;
   btree_config *const cfg = spl->cfg.btree_cfg;
   bool32              memtable_is_compacted;
   uint64              root_addr = core_memtable_root_addr_for_lookup(
      spl, generation, &memtable_is_compacted, NULL);
   page_type type =
      memtable_is_compacted ? PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;

   return btree_lookup_and_merge(
      cc, cfg, root_addr, type, target, result, NULL);
}

static platform_status
core_lookup_memtables_locked(core_handle   *spl,
                             uint64         mt_gen_start,
                             key            target,
                             lookup_result *result,
                             bool32        *found_final)
{
   uint64 mt_gen_end = memtable_generation_retired(&spl->mt_ctxt);
   platform_assert(mt_gen_start - mt_gen_end <= CORE_NUM_MEMTABLES);

   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      platform_status rc = core_memtable_lookup(spl, mt_gen, target, result);
      platform_assert_status_ok(rc);
      if (!lookup_result_should_continue(result)) {
         *found_final = TRUE;
         return STATUS_OK;
      }
   }

   *found_final = FALSE;
   return STATUS_OK;
}

static platform_status
core_lookup_from_memtable_generation_locked(core_handle   *spl,
                                            uint64         mt_gen_start,
                                            key            target,
                                            lookup_result *result)
{
   bool32                   found_final = FALSE;
   trunk_ondisk_node_handle root_handle;

   platform_status rc;

   if (mt_gen_start != (uint64)-1) {
      rc = core_lookup_memtables_locked(
         spl, mt_gen_start, target, result, &found_final);
      if (found_final) {
         memtable_end_lookup(&spl->mt_ctxt);
         lookup_result_finalize(result, target);
         return STATUS_OK;
      }
   }

   rc = trunk_init_root_handle(&spl->trunk_context, &root_handle);
   memtable_end_lookup(&spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      return rc;
   }

   rc = trunk_merge_lookup(
      &spl->trunk_context, &root_handle, target, result, NULL);
   trunk_ondisk_node_handle_deinit(&root_handle);
   if (!SUCCESS(rc)) {
      return rc;
   }

   lookup_result_finalize(result, target);
   return STATUS_OK;
}

typedef struct core_btree_iterator_init_async_context {
   btree_iterator_async_state state;
   bool32                     ready;
   bool32                     done;
} core_btree_iterator_init_async_context;

static void
core_btree_iterator_init_async_callback(void *arg)
{
   core_btree_iterator_init_async_context *ctxt = arg;
   __atomic_store_n(&ctxt->ready, TRUE, __ATOMIC_RELEASE);
}

static platform_status
core_start_btree_iterator_init_async(
   core_handle                            *spl,
   core_btree_iterator_init_async_context *ctxt,
   btree_iterator                         *itor,
   uint64                                  root_addr,
   page_type                               page_type,
   comparison                              min_key_comparison,
   key                                     min_key,
   comparison                              max_key_comparison,
   key                                     max_key,
   comparison                              start_key_comparison,
   key                                     start_key,
   bool32                                  copy_nodes,
   uint32                                  prefetch_lookahead)
{
   btree_iterator_async_state_init(&ctxt->state,
                                   spl->cc,
                                   spl->cfg.btree_cfg,
                                   itor,
                                   root_addr,
                                   page_type,
                                   min_key_comparison,
                                   min_key,
                                   max_key_comparison,
                                   max_key,
                                   start_key_comparison,
                                   start_key,
                                   copy_nodes,
                                   0,
                                   prefetch_lookahead,
                                   core_btree_iterator_init_async_callback,
                                   ctxt);
   __atomic_store_n(&ctxt->ready, FALSE, __ATOMIC_RELAXED);
   ctxt->done = FALSE;

   if (btree_iterator_init_async(&ctxt->state) == ASYNC_STATUS_DONE) {
      ctxt->done = TRUE;
      return btree_iterator_init_async_result(&ctxt->state);
   }

   return STATUS_OK;
}

static platform_status
core_drain_btree_iterator_init_async(
   cache                                  *cc,
   core_btree_iterator_init_async_context *ctxt,
   uint64                                  num_inits)
{
   platform_status result     = STATUS_OK;
   uint64          done_count = 0;
   for (uint64 i = 0; i < num_inits; i++) {
      if (ctxt[i].done) {
         done_count++;
         platform_status rc = btree_iterator_init_async_result(&ctxt[i].state);
         if (!SUCCESS(rc) && SUCCESS(result)) {
            result = rc;
         }
      }
   }

   while (done_count < num_inits) {
      bool32 made_progress = FALSE;
      for (uint64 i = 0; i < num_inits; i++) {
         if (ctxt[i].done
             || !__atomic_exchange_n(&ctxt[i].ready, FALSE, __ATOMIC_ACQUIRE))
         {
            continue;
         }

         made_progress = TRUE;
         if (btree_iterator_init_async(&ctxt[i].state) == ASYNC_STATUS_DONE) {
            ctxt[i].done = TRUE;
            done_count++;
            platform_status rc =
               btree_iterator_init_async_result(&ctxt[i].state);
            if (!SUCCESS(rc) && SUCCESS(result)) {
               result = rc;
            }
         }
      }

      if (!made_progress) {
         cache_cleanup(cc);
      }
   }

   return result;
}

/*
 *-----------------------------------------------------------------------------
 * Range functions and iterators
 *
 *      core_node_iterator
 *      core_iterator
 *-----------------------------------------------------------------------------
 */
static void
core_range_iterator_curr(iterator *itor, key *curr_key, message *data);
static bool32
core_range_iterator_can_prev(iterator *itor);
static bool32
core_range_iterator_can_next(iterator *itor);
static platform_status
core_range_iterator_next(iterator *itor);
static platform_status
core_range_iterator_prev(iterator *itor);
void
core_range_iterator_deinit(core_range_iterator *range_itor);

const static iterator_ops core_range_iterator_ops = {
   .curr     = core_range_iterator_curr,
   .can_prev = core_range_iterator_can_prev,
   .can_next = core_range_iterator_can_next,
   .next     = core_range_iterator_next,
   .prev     = core_range_iterator_prev,
};

static inline bool32
core_range_iterator_has_next_leaf(core_range_iterator *range_itor)
{
   key    local_max_key = key_buffer_key(&range_itor->local_max_key);
   key    max_key       = key_buffer_key(&range_itor->max_key);
   int    cmp = core_key_compare(range_itor->spl, local_max_key, max_key);
   bool32 max_is_finite = !key_is_positive_infinity(max_key);

   return cmp < 0
          || (cmp == 0 && max_is_finite && !range_itor->local_max_key_truncated
              && range_itor->max_key_comparison == less_than_or_equal);
}

platform_status
core_range_iterator_init(core_handle         *spl,
                         core_range_iterator *range_itor,
                         comparison           min_key_comparison,
                         key                  min_key,
                         comparison           max_key_comparison,
                         key                  max_key,
                         comparison           start_key_comparison,
                         key                  start_key)
{
   platform_status rc;

   debug_assert(!key_is_null(min_key));
   debug_assert(!key_is_null(max_key));
   debug_assert(!key_is_null(start_key));
   debug_assert(min_key_comparison == greater_than
                || min_key_comparison == greater_than_or_equal);
   debug_assert(max_key_comparison == less_than
                || max_key_comparison == less_than_or_equal);

   range_itor->spl                = spl;
   range_itor->super.ops          = &core_range_iterator_ops;
   range_itor->num_branches       = 0;
   range_itor->merge_itor         = NULL;
   range_itor->can_prev           = TRUE;
   range_itor->can_next           = TRUE;
   range_itor->min_key_comparison = min_key_comparison;
   range_itor->max_key_comparison = max_key_comparison;
   ZERO_ARRAY(range_itor->compacted);
   ZERO_ARRAY(range_itor->btree_itor_initialized);

   key_buffer_init(&range_itor->min_key, PROCESS_PRIVATE_HEAP_ID);
   key_buffer_init(&range_itor->max_key, PROCESS_PRIVATE_HEAP_ID);
   key_buffer_init(&range_itor->local_min_key, PROCESS_PRIVATE_HEAP_ID);
   key_buffer_init(&range_itor->local_max_key, PROCESS_PRIVATE_HEAP_ID);

   bool32 forward_start = comparison_is_forward(start_key_comparison);
   int    min_start_cmp = core_key_compare(spl, min_key, start_key);
   if (min_start_cmp > 0) {
      start_key            = min_key;
      start_key_comparison = forward_start
                                ? min_key_comparison
                                : comparison_invert(min_key_comparison);
   } else if (min_start_cmp == 0) {
      if (forward_start && min_key_comparison == greater_than) {
         start_key_comparison = greater_than;
      } else if (!forward_start && min_key_comparison == greater_than) {
         start_key_comparison = less_than_or_equal;
      }
   }
   int max_start_cmp = core_key_compare(spl, max_key, start_key);
   if (max_start_cmp < 0) {
      start_key            = max_key;
      start_key_comparison = forward_start
                                ? comparison_invert(max_key_comparison)
                                : max_key_comparison;
   } else if (max_start_cmp == 0) {
      if (!forward_start && max_key_comparison == less_than) {
         start_key_comparison = less_than;
      } else if (forward_start && max_key_comparison == less_than) {
         start_key_comparison = greater_than_or_equal;
      }
   }

   // copy over global min and max
   rc = key_buffer_copy_key(&range_itor->min_key, min_key);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }
   rc = key_buffer_copy_key(&range_itor->max_key, max_key);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   // grab the lookup lock
   memtable_begin_lookup(&spl->mt_ctxt);

   // memtables
   ZERO_ARRAY(range_itor->branch);
   // Note this iteration is in descending generation order
   range_itor->memtable_start_gen = memtable_generation(&spl->mt_ctxt);
   range_itor->memtable_end_gen   = memtable_generation_retired(&spl->mt_ctxt);
   range_itor->num_memtable_branches =
      range_itor->memtable_start_gen - range_itor->memtable_end_gen;
   bool32 first_memtable_copy_nodes = FALSE;
   for (uint64 mt_gen = range_itor->memtable_start_gen;
        mt_gen != range_itor->memtable_end_gen;
        mt_gen--)
   {
      platform_assert((range_itor->num_branches < CORE_RANGE_ITOR_MAX_BRANCHES),
                      "range_itor->num_branches=%lu should be < "
                      " CORE_RANGE_ITOR_MAX_BRANCHES (%d).",
                      range_itor->num_branches,
                      CORE_RANGE_ITOR_MAX_BRANCHES);
      debug_assert(range_itor->num_branches < ARRAY_SIZE(range_itor->branch));

      bool32 compacted;
      bool32 active;
      uint64 root_addr =
         core_memtable_root_addr_for_lookup(spl, mt_gen, &compacted, &active);
      range_itor->compacted[range_itor->num_branches] = compacted;
      // Only READY memtables can be modified while this iterator is live.
      if (range_itor->num_branches == 0) {
         first_memtable_copy_nodes = active;
      } else {
         debug_assert(!active);
      }
      if (compacted) {
         btree_inc_ref(spl->cc, spl->cfg.btree_cfg, root_addr);
      } else {
         core_memtable_inc_ref(spl, root_addr);
      }

      range_itor->branch[range_itor->num_branches].addr = root_addr;
      range_itor->branch[range_itor->num_branches].type =
         compacted ? PAGE_TYPE_BRANCH : PAGE_TYPE_MEMTABLE;
      range_itor->num_branches++;
   }

   trunk_ondisk_node_handle root_handle;
   rc = trunk_init_root_handle(&spl->trunk_context, &root_handle);
   memtable_end_lookup(&spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   uint64 old_num_branches = range_itor->num_branches;
   rc                      = trunk_collect_branches(&spl->trunk_context,
                               &root_handle,
                               start_key,
                               start_key_comparison,
                               CORE_RANGE_ITOR_MAX_BRANCHES,
                               &range_itor->num_branches,
                               range_itor->branch,
                               &range_itor->local_min_key,
                               &range_itor->local_max_key);
   trunk_ondisk_node_handle_deinit(&root_handle);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   for (uint64 i = old_num_branches; i < range_itor->num_branches; i++) {
      range_itor->compacted[i] = TRUE;
   }

   range_itor->local_min_key_comparison = greater_than_or_equal;
   range_itor->local_max_key_comparison = less_than;
   range_itor->local_max_key_truncated  = FALSE;

   // have a leaf, use to establish local bounds
   if (core_key_compare(
          spl, key_buffer_key(&range_itor->local_min_key), min_key)
       <= 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_min_key, min_key);
      range_itor->local_min_key_comparison = min_key_comparison;
      if (!SUCCESS(rc)) {
         core_range_iterator_deinit(range_itor);
         return rc;
      }
   }
   if (core_key_compare(
          spl, key_buffer_key(&range_itor->local_max_key), max_key)
       > 0)
   {
      rc = key_buffer_copy_key(&range_itor->local_max_key, max_key);
      range_itor->local_max_key_comparison = max_key_comparison;
      range_itor->local_max_key_truncated  = TRUE;
      if (!SUCCESS(rc)) {
         core_range_iterator_deinit(range_itor);
         return rc;
      }
   }

   core_btree_iterator_init_async_context *init_ctxt = NULL;
   if (range_itor->num_branches != 0) {
      /*
       * Async cache-load waiters embedded in these contexts can be released by
       * another process when the clockcache is shared.  The callback only marks
       * ctxt->ready, so keep the context itself in the Splinter heap; the
       * owning process remains responsible for resuming the iterator state.
       */
      init_ctxt =
         TYPED_ARRAY_ZALLOC(spl->heap_id, init_ctxt, range_itor->num_branches);
   }
   if (range_itor->num_branches != 0 && init_ctxt == NULL) {
      core_range_iterator_deinit(range_itor);
      return STATUS_NO_MEMORY;
   }

   // Deep extent-prefetch for the scan: count compacted branches and give each
   // a soft share of the prefetch budget.
   uint64 n_prefetch_branches = 0;
   for (uint64 branch_no = 0; branch_no < range_itor->num_branches; branch_no++)
   {
      if (range_itor->compacted[branch_no]) {
         n_prefetch_branches++;
      }
   }
   uint32 deep_lookahead =
      prefetch_budget_to_extent_lookahead(cache_extent_size(spl->cc),
                                          spl->cfg.prefetch_budget,
                                          n_prefetch_branches);

   uint64 started_inits = 0;
   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      uint64          branch_no          = range_itor->num_branches - i - 1;
      btree_iterator *btree_itor         = &range_itor->btree_itor[branch_no];
      uint64          branch_addr        = range_itor->branch[branch_no].addr;
      page_type       page_type          = range_itor->branch[branch_no].type;
      uint32          prefetch_lookahead = 0;
      if (range_itor->compacted[branch_no]) {
         prefetch_lookahead = deep_lookahead;
      }
      rc = core_start_btree_iterator_init_async(
         spl,
         &init_ctxt[i],
         btree_itor,
         branch_addr,
         page_type,
         range_itor->local_min_key_comparison,
         key_buffer_key(&range_itor->local_min_key),
         range_itor->local_max_key_comparison,
         key_buffer_key(&range_itor->local_max_key),
         start_key_comparison,
         start_key,
         branch_no == 0 ? first_memtable_copy_nodes : FALSE,
         prefetch_lookahead);
      started_inits++;
      if (!SUCCESS(rc)) {
         break;
      }
      range_itor->itor[i] = &btree_itor->super;
   }

   platform_status drain_rc =
      core_drain_btree_iterator_init_async(spl->cc, init_ctxt, started_inits);
   if (SUCCESS(rc)) {
      rc = drain_rc;
   }
   for (uint64 i = 0; i < started_inits; i++) {
      if (init_ctxt[i].done) {
         uint64 branch_no = range_itor->num_branches - i - 1;
         range_itor->btree_itor_initialized[branch_no] = TRUE;
      }
   }
   if (init_ctxt != NULL) {
      platform_free(spl->heap_id, init_ctxt);
   }
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   rc = merge_iterator_create(PROCESS_PRIVATE_HEAP_ID,
                              spl->cfg.data_cfg,
                              range_itor->num_branches,
                              range_itor->itor,
                              MERGE_FULL,
                              greater_than <= start_key_comparison,
                              &range_itor->merge_itor);
   if (!SUCCESS(rc)) {
      core_range_iterator_deinit(range_itor);
      return rc;
   }

   bool32 in_range = iterator_can_curr(&range_itor->merge_itor->super);

   /*
    * if the merge itor is already exhausted, and there are more keys in the
    * db/range, move to prev/next leaf
    */
   if (!in_range && start_key_comparison >= greater_than) {
      if (core_range_iterator_has_next_leaf(range_itor)) {
         key        local_max = key_buffer_key(&range_itor->local_max_key);
         key_buffer local_max_buffer;
         rc = key_buffer_init_from_key(
            &local_max_buffer, PROCESS_PRIVATE_HEAP_ID, local_max);
         core_range_iterator_deinit(range_itor);
         if (!SUCCESS(rc)) {
            return rc;
         }
         local_max = key_buffer_key(&local_max_buffer);
         rc        = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       greater_than_or_equal,
                                       local_max);
         key_buffer_deinit(&local_max_buffer);
         if (!SUCCESS(rc)) {
            return rc;
         }

      } else {
         range_itor->can_next = FALSE;
         range_itor->can_prev =
            iterator_can_prev(&range_itor->merge_itor->super);
      }
   }
   if (!in_range && start_key_comparison <= less_than_or_equal) {
      key local_min = key_buffer_key(&range_itor->local_min_key);
      if (core_key_compare(spl, local_min, min_key) > 0) {
         key_buffer local_min_buffer;
         rc = key_buffer_init_from_key(
            &local_min_buffer, PROCESS_PRIVATE_HEAP_ID, local_min);
         core_range_iterator_deinit(range_itor);
         if (!SUCCESS(rc)) {
            return rc;
         }
         local_min = key_buffer_key(&local_min_buffer);
         rc        = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       less_than,
                                       local_min);
         key_buffer_deinit(&local_min_buffer);
         if (!SUCCESS(rc)) {
            return rc;
         }

      } else {
         range_itor->can_prev = FALSE;
         range_itor->can_next =
            iterator_can_next(&range_itor->merge_itor->super);
      }
   }
   return rc;
}

static void
core_range_iterator_curr(iterator *itor, key *curr_key, message *data)
{
   debug_assert(itor != NULL);
   core_range_iterator *range_itor = (core_range_iterator *)itor;
   iterator_curr(&range_itor->merge_itor->super, curr_key, data);
}

static platform_status
core_range_iterator_next(iterator *itor)
{
   core_range_iterator *range_itor = (core_range_iterator *)itor;
   debug_assert(range_itor != NULL);
   platform_assert(range_itor->can_next);

   platform_status rc = iterator_next(&range_itor->merge_itor->super);
   if (!SUCCESS(rc)) {
      return rc;
   }
   range_itor->can_prev = TRUE;
   range_itor->can_next = iterator_can_next(&range_itor->merge_itor->super);
   if (!range_itor->can_next) {
      KEY_CREATE_LOCAL_COPY(rc,
                            min_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            max_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            local_max_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->local_max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }

      // if there is more data to get, rebuild the iterator for next leaf
      if (core_range_iterator_has_next_leaf(range_itor)) {
         core_handle *spl                = range_itor->spl;
         comparison   min_key_comparison = range_itor->min_key_comparison;
         comparison   max_key_comparison = range_itor->max_key_comparison;
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       greater_than_or_equal,
                                       local_max_key);
         if (!SUCCESS(rc)) {
            return rc;
         }
         debug_assert(range_itor->can_next
                      == iterator_can_next(&range_itor->merge_itor->super));
      }
   }

   return STATUS_OK;
}

static platform_status
core_range_iterator_prev(iterator *itor)
{
   core_range_iterator *range_itor = (core_range_iterator *)itor;
   debug_assert(itor != NULL);
   platform_assert(range_itor->can_prev);

   platform_status rc = iterator_prev(&range_itor->merge_itor->super);
   if (!SUCCESS(rc)) {
      return rc;
   }
   range_itor->can_next = TRUE;
   range_itor->can_prev = iterator_can_prev(&range_itor->merge_itor->super);
   if (!range_itor->can_prev) {
      KEY_CREATE_LOCAL_COPY(rc,
                            min_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            max_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->max_key));
      if (!SUCCESS(rc)) {
         return rc;
      }
      KEY_CREATE_LOCAL_COPY(rc,
                            local_min_key,
                            PROCESS_PRIVATE_HEAP_ID,
                            key_buffer_key(&range_itor->local_min_key));
      if (!SUCCESS(rc)) {
         return rc;
      }

      // if there is more data to get, rebuild the iterator for prev leaf
      if (core_key_compare(range_itor->spl, local_min_key, min_key) > 0) {
         core_handle *spl                = range_itor->spl;
         comparison   min_key_comparison = range_itor->min_key_comparison;
         comparison   max_key_comparison = range_itor->max_key_comparison;
         core_range_iterator_deinit(range_itor);
         rc = core_range_iterator_init(spl,
                                       range_itor,
                                       min_key_comparison,
                                       min_key,
                                       max_key_comparison,
                                       max_key,
                                       less_than,
                                       local_min_key);
         if (!SUCCESS(rc)) {
            return rc;
         }
         debug_assert(range_itor->can_prev
                      == iterator_can_prev(&range_itor->merge_itor->super));
      }
   }

   return STATUS_OK;
}

static bool32
core_range_iterator_can_prev(iterator *itor)
{
   debug_assert(itor != NULL);
   core_range_iterator *range_itor = (core_range_iterator *)itor;

   return range_itor->can_prev;
}

static bool32
core_range_iterator_can_next(iterator *itor)
{
   debug_assert(itor != NULL);
   core_range_iterator *range_itor = (core_range_iterator *)itor;

   return range_itor->can_next;
}

void
core_range_iterator_deinit(core_range_iterator *range_itor)
{
   core_handle *spl = range_itor->spl;
   if (range_itor->merge_itor != NULL) {
      merge_iterator_destroy(PROCESS_PRIVATE_HEAP_ID, &range_itor->merge_itor);
   }
   for (uint64 i = 0; i < range_itor->num_branches; i++) {
      btree_iterator *btree_itor = &range_itor->btree_itor[i];
      if (range_itor->btree_itor_initialized[i]) {
         btree_iterator_deinit(btree_itor);
         range_itor->btree_itor_initialized[i] = FALSE;
      }
      if (range_itor->compacted[i]) {
         btree_dec_ref(spl->cc,
                       spl->cfg.btree_cfg,
                       range_itor->branch[i].addr,
                       PAGE_TYPE_BRANCH);
      } else {
         core_memtable_dec_ref(spl, range_itor->branch[i].addr);
      }
   }
   key_buffer_deinit(&range_itor->min_key);
   key_buffer_deinit(&range_itor->max_key);
   key_buffer_deinit(&range_itor->local_min_key);
   key_buffer_deinit(&range_itor->local_max_key);
}

/*
 *-----------------------------------------------------------------------------
 * Main Splinter API functions
 *
 *      insert
 *      lookup
 *      range
 *-----------------------------------------------------------------------------
 */

platform_status
core_insert(core_handle   *spl,
            key            tuple_key,
            message        data,
            lookup_result *old_result)
{
   timestamp      ts;
   const threadid tid = platform_get_tid();
   if (spl->cfg.use_stats) {
      ts = platform_get_timestamp();
   }

   if (message_class(data) == MESSAGE_TYPE_DELETE) {
      data = DELETE_MESSAGE;
   }

   if (old_result != NULL) {
      lookup_result_reset(old_result);
   }

   uint64          generation;
   memtable       *mt = NULL;
   platform_status rc = core_begin_memtable_insert(spl, &generation, &mt);
   if (!SUCCESS(rc)) {
      goto out;
   }

   btree_insert_results insert_results;
   btree_insert_results_init(&insert_results, old_result);
   rc = memtable_insert(&spl->mt_ctxt,
                        mt,
                        PROCESS_PRIVATE_HEAP_ID,
                        tuple_key,
                        data,
                        &insert_results);
   if (!SUCCESS(rc)) {
      goto end_insert;
   }

   rc = core_log_insert(spl, generation, tuple_key, data, &insert_results);
   if (!SUCCESS(rc)) {
      goto end_insert;
   }

   if (old_result != NULL) {
      if (lookup_result_should_continue(old_result)) {
         memtable_begin_lookup(&spl->mt_ctxt);
         memtable_end_insert(&spl->mt_ctxt);
         // Passing generation - 1 is allowed here
         rc = core_lookup_from_memtable_generation_locked(
            spl, generation - 1, tuple_key, old_result);
         if (!SUCCESS(rc)) {
            goto deinit_insert_results;
         }
      } else {
         memtable_end_insert(&spl->mt_ctxt);
         lookup_result_finalize(old_result, tuple_key);
      }
   } else {
      memtable_end_insert(&spl->mt_ctxt);
   }

deinit_insert_results:
   btree_insert_results_deinit(&insert_results);

   task_perform_one_if_needed(spl->ts, spl->cfg.queue_scale_percent);

   if (spl->cfg.use_stats) {
      switch (message_class(data)) {
         case MESSAGE_TYPE_INSERT:
            spl->stats[tid].insertions++;
            histogram_insert(spl->stats[tid].insert_latency_histo,
                             platform_timestamp_elapsed(ts));
            break;
         case MESSAGE_TYPE_UPDATE:
            spl->stats[tid].updates++;
            histogram_insert(spl->stats[tid].update_latency_histo,
                             platform_timestamp_elapsed(ts));
            break;
         case MESSAGE_TYPE_DELETE:
            spl->stats[tid].deletions++;
            histogram_insert(spl->stats[tid].delete_latency_histo,
                             platform_timestamp_elapsed(ts));
            break;
         default:
            platform_assert(0);
      }
   }

out:
   return rc;

end_insert:
   btree_insert_results_deinit(&insert_results);
   memtable_end_insert(&spl->mt_ctxt);
   return rc;
}

platform_status
core_optimize(core_handle             *spl,
              key                      minkey,
              key                      maxkey,
              bool32                   full_leaf_compactions,
              splinterdb_notification *notification)
{
   if (key_is_null(minkey) || key_is_null(maxkey)) {
      return STATUS_BAD_PARAM;
   }

   int cmp = data_key_compare(spl->cfg.data_cfg, minkey, maxkey);
   if (cmp > 0) {
      return STATUS_BAD_PARAM;
   }
   if (cmp == 0) {
      splinterdb_notification_complete(notification, STATUS_OK);
      return STATUS_OK;
   }

   return trunk_optimize(
      &spl->trunk_context, minkey, maxkey, full_leaf_compactions, notification);
}

// If any change is made in here, please make similar change in
// core_lookup_async
platform_status
core_lookup(core_handle *spl, key target, lookup_result *result)
{
   lookup_result_reset(result);

   memtable_begin_lookup(&spl->mt_ctxt);
   uint64          mt_gen_start = memtable_generation(&spl->mt_ctxt);
   platform_status rc           = core_lookup_from_memtable_generation_locked(
      spl, mt_gen_start, target, result);
   if (!SUCCESS(rc)) {
      return rc;
   }

   if (spl->cfg.use_stats) {
      threadid tid = platform_get_tid();
      if (lookup_result_found(result)) {
         spl->stats[tid].lookups_found++;
      } else {
         spl->stats[tid].lookups_not_found++;
      }
   }


   return STATUS_OK;
}

async_status
core_lookup_async(core_lookup_async_state *state)
{
   async_begin(state, 0);
   // look in memtables

   // 1. get read lock on lookup lock
   //     --- 2. for [mt_no = mt->generation..mt->gen_to_incorp]
   // 2. for gen = mt->generation; mt[gen % ...].gen == gen; gen --;
   //                also handles switch to READY ^^^^^

   lookup_result_reset(state->result);

   memtable_begin_lookup(&state->spl->mt_ctxt);
   uint64          mt_gen_start = memtable_generation(&state->spl->mt_ctxt);
   bool32          found_final;
   platform_status rc = core_lookup_memtables_locked(
      state->spl, mt_gen_start, state->target, state->result, &found_final);
   platform_assert_status_ok(rc);
   if (found_final) {
      memtable_end_lookup(&state->spl->mt_ctxt);
      goto found_final_answer;
   }

   rc = trunk_init_root_handle(&state->spl->trunk_context, &state->root_handle);
   // release memtable lookup lock before we handle any errors
   memtable_end_lookup(&state->spl->mt_ctxt);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   async_await_call(state,
                    trunk_merge_lookup_async,
                    &state->trunk_node_state,
                    &state->spl->trunk_context,
                    &state->root_handle,
                    state->target,
                    state->result,
                    NULL,
                    state->callback,
                    state->callback_arg);
   trunk_ondisk_node_handle_deinit(&state->root_handle);
   rc = async_result(&state->trunk_node_state);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

found_final_answer:

   lookup_result_finalize(state->result, state->target);

   if (state->spl->cfg.use_stats) {
      threadid tid = platform_get_tid();
      if (lookup_result_found(state->result)) {
         state->spl->stats[tid].lookups_found++;
      } else {
         state->spl->stats[tid].lookups_not_found++;
      }
   }


   async_return(state, STATUS_OK);
}

platform_status
core_apply_to_range(core_handle   *spl,
                    key            start_key,
                    uint64         num_tuples,
                    tuple_function func,
                    void          *arg)
{
   core_range_iterator *range_itor =
      TYPED_MALLOC(PROCESS_PRIVATE_HEAP_ID, range_itor);
   if (range_itor == NULL) {
      platform_error_log("core_apply_to_range: failed to allocate range "
                         "iterator for %lu tuples\n",
                         num_tuples);
      return STATUS_NO_MEMORY;
   }

   platform_status rc = core_range_iterator_init(spl,
                                                 range_itor,
                                                 greater_than_or_equal,
                                                 start_key,
                                                 less_than,
                                                 POSITIVE_INFINITY_KEY,
                                                 greater_than_or_equal,
                                                 start_key);
   if (!SUCCESS(rc)) {
      platform_error_log("core_apply_to_range: range iterator init failed: "
                         "%s\n",
                         platform_status_to_string(rc));
      goto destroy_range_itor;
   }

   for (int i = 0; i < num_tuples && iterator_can_next(&range_itor->super); i++)
   {
      key     curr_key;
      message data;
      iterator_curr(&range_itor->super, &curr_key, &data);
      func(curr_key, data, arg);
      rc = iterator_next(&range_itor->super);
      if (!SUCCESS(rc)) {
         platform_error_log("core_apply_to_range: iterator_next failed: %s\n",
                            platform_status_to_string(rc));
         goto destroy_range_itor;
      }
   }

destroy_range_itor:
   core_range_iterator_deinit(range_itor);
   platform_free(PROCESS_PRIVATE_HEAP_ID, range_itor);
   return rc;
}

static void
core_stats_destroy(platform_heap_id heap_id, core_stats *stats)
{
   if (stats == NULL) {
      return;
   }

   for (uint64 i = 0; i < MAX_THREADS; i++) {
      histogram_destroy(heap_id, stats[i].insert_latency_histo);
      histogram_destroy(heap_id, stats[i].update_latency_histo);
      histogram_destroy(heap_id, stats[i].delete_latency_histo);
   }
   platform_free(heap_id, stats);
}

static core_stats *
core_stats_create(platform_heap_id heap_id)
{
   core_stats *stats = TYPED_ARRAY_ZALLOC(heap_id, stats, MAX_THREADS);
   if (stats == NULL) {
      platform_error_log("core_stats_create: failed to allocate stats array "
                         "for %u threads\n",
                         MAX_THREADS);
      return NULL;
   }

   for (uint64 i = 0; i < MAX_THREADS; i++) {
      stats[i].insert_latency_histo = histogram_create(
         heap_id, LATENCYHISTO_SIZE + 1, latency_histo_buckets);
      if (stats[i].insert_latency_histo == NULL) {
         platform_error_log("core_stats_create: failed to allocate insert "
                            "latency histogram for thread %lu\n",
                            i);
         goto cleanup;
      }
      stats[i].update_latency_histo = histogram_create(
         heap_id, LATENCYHISTO_SIZE + 1, latency_histo_buckets);
      if (stats[i].update_latency_histo == NULL) {
         platform_error_log("core_stats_create: failed to allocate update "
                            "latency histogram for thread %lu\n",
                            i);
         goto cleanup;
      }
      stats[i].delete_latency_histo = histogram_create(
         heap_id, LATENCYHISTO_SIZE + 1, latency_histo_buckets);
      if (stats[i].delete_latency_histo == NULL) {
         platform_error_log("core_stats_create: failed to allocate delete "
                            "latency histogram for thread %lu\n",
                            i);
         goto cleanup;
      }
   }

   return stats;

cleanup:
   core_stats_destroy(heap_id, stats);
   return NULL;
}

static platform_status
core_create_stats(core_handle *spl)
{
   if (!spl->cfg.use_stats) {
      return STATUS_OK;
   }

   spl->stats = core_stats_create(spl->heap_id);
   return spl->stats == NULL ? STATUS_NO_MEMORY : STATUS_OK;
}

static void
core_destroy_stats(core_handle *spl)
{
   core_stats_destroy(spl->heap_id, spl->stats);
   spl->stats = NULL;
}


/* Format the disk and mount the database */
platform_status
core_mkfs(core_handle      *spl,
          core_config      *cfg,
          allocator        *al,
          cache            *cc,
          task_system      *ts,
          allocator_root_id id,
          platform_heap_id  hid)
{
   ZERO_CONTENTS(spl);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   platform_status rc = core_checkpoint_lock_init(spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: checkpoint lock initialization failed: %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   // set up the memtable context
   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   rc = memtable_context_init(&spl->mt_ctxt,
                              spl->heap_id,
                              cc,
                              mt_cfg,
                              core_memtable_flush_virtual,
                              spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: memtable_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_checkpoint_lock;
   }

   // set up the log
   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
      if (spl->log == NULL) {
         platform_error_log("core_mkfs: log_create failed\n");
         rc = STATUS_NO_MEMORY;
         goto deinit_memtable_context;
      }
   }

   rc = trunk_context_init(
      &spl->trunk_context, spl->cfg.trunk_node_cfg, hid, cc, al, ts, 0);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: trunk_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_log;
   }

   rc = core_create_stats(spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: core_create_stats failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_trunk_context;
   }

   rc = core_publish_checkpoint_record(spl, FALSE, FALSE, TRUE);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mkfs: core_publish_checkpoint_record failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_stats;
   }
   return STATUS_OK;

deinit_stats:
   core_destroy_stats(spl);
deinit_trunk_context:
   trunk_context_deinit(&spl->trunk_context);
deinit_log:
   if (spl->cfg.use_log) {
      platform_free(spl->heap_id, spl->log);
      spl->log = NULL;
   }
deinit_memtable_context:
   memtable_context_deinit(&spl->mt_ctxt);
deinit_checkpoint_lock:
   core_checkpoint_lock_deinit(spl);
   return rc;
}

/*
 * Open (mount) an existing splinter database
 */
platform_status
core_mount(core_handle      *spl,
           core_config      *cfg,
           allocator        *al,
           cache            *cc,
           task_system      *ts,
           allocator_root_id id,
           platform_heap_id  hid)
{
   ZERO_CONTENTS(spl);
   memmove(&spl->cfg, cfg, sizeof(*cfg));

   spl->al = al;
   spl->cc = cc;
   debug_assert(id != INVALID_ALLOCATOR_ROOT_ID);
   spl->id      = id;
   spl->heap_id = hid;
   spl->ts      = ts;

   platform_status rc = core_checkpoint_lock_init(spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: checkpoint lock initialization failed: %s\n",
                         platform_status_to_string(rc));
      return rc;
   }

   /*
    * Preserve the historical clean-only mount rule for this first format
    * slice: an interrupted run is not replayed yet, so only an explicitly
    * unmounted record supplies the root.  We still validate both records and
    * choose the newest clean one by sequence rather than wall-clock time.
    */
   uint64                    root_addr = 0;
   bool32                    has_incorporated_generation = FALSE;
   uint64                    incorporated_generation     = 0;
   core_checkpoint_directory directory;
   core_checkpoint_records   records;
   rc = core_get_checkpoint_directory(spl, &directory);
   if (!SUCCESS(rc)) {
      goto deinit_checkpoint_lock;
   }
   rc = core_load_checkpoint_records(spl, &directory, &records);
   if (!SUCCESS(rc)) {
      goto deinit_checkpoint_lock;
   }
   if (!records.have_newest) {
      platform_error_log("core_mount: checkpoint directory for root id %lu "
                         "has no valid records\n",
                         spl->id);
      rc = STATUS_BAD_PARAM;
      goto deinit_checkpoint_lock;
   }
   const core_checkpoint_record *record =
      &records.record[records.newest_slot];
   if (!record->unmounted) {
      /*
       * This is an interrupted run. An older clean record is only an A/B
       * torn-write fallback, not permission to silently discard the newer
       * checkpoint and its log suffix. Do not overwrite its metadata before
       * log replay and allocator reconstruction are wired.
       */
      platform_error_log("core_mount: root id %lu requires crash recovery\n",
                         spl->id);
      rc = STATUS_INVALID_STATE;
      goto deinit_checkpoint_lock;
   }
   root_addr                    = record->root_addr;
   has_incorporated_generation = record->has_incorporated_generation;
   incorporated_generation     = record->incorporated_generation;

   memtable_config *mt_cfg = &spl->cfg.mt_cfg;
   rc = memtable_context_init_at_generation(
      &spl->mt_ctxt,
      spl->heap_id,
      cc,
      mt_cfg,
      core_memtable_flush_virtual,
      spl,
      has_incorporated_generation ? incorporated_generation + 1 : 0);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: memtable_context_init_at_generation "
                         "failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_checkpoint_lock;
   }

   if (spl->cfg.use_log) {
      spl->log = log_create(cc, spl->cfg.log_cfg, spl->heap_id);
      if (spl->log == NULL) {
         platform_error_log("core_mount: log_create failed\n");
         rc = STATUS_NO_MEMORY;
         goto deinit_memtable_context;
      }
   }

   rc = trunk_context_init(
      &spl->trunk_context, spl->cfg.trunk_node_cfg, hid, cc, al, ts, root_addr);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: trunk_context_init failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_log;
   }

   rc = core_create_stats(spl);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: core_create_stats failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_trunk_context;
   }

   rc = core_publish_checkpoint_record(spl, FALSE, FALSE, FALSE);
   if (!SUCCESS(rc)) {
      platform_error_log("core_mount: core_publish_checkpoint_record failed: %s\n",
                         platform_status_to_string(rc));
      goto deinit_stats;
   }
   return STATUS_OK;

deinit_stats:
   core_destroy_stats(spl);
deinit_trunk_context:
   trunk_context_deinit(&spl->trunk_context);
deinit_log:
   if (spl->cfg.use_log) {
      platform_free(spl->heap_id, spl->log);
      spl->log = NULL;
   }
deinit_memtable_context:
   memtable_context_deinit(&spl->mt_ctxt);
deinit_checkpoint_lock:
   core_checkpoint_lock_deinit(spl);
   return rc;
}

static bool32
core_report_unincorporated_memtables(core_handle *spl)
{
   bool32 found_unincorporated = FALSE;
   uint64 start_generation     = memtable_generation_retired(&spl->mt_ctxt) + 1;
   uint64 end_generation       = memtable_generation(&spl->mt_ctxt);

   for (uint64 generation = start_generation; generation < end_generation;
        generation++)
   {
      memtable *mt = core_try_get_memtable(spl, generation);
      if (mt == NULL || mt->state == MEMTABLE_STATE_READY) {
         continue;
      }

      found_unincorporated = TRUE;
      uint64 compacted_root =
         core_memtable_compacted_branch_root(spl, generation);
      if (mt->state == MEMTABLE_STATE_INCORPORATION_FAILED) {
         platform_error_log("Shutdown found memtable from failed "
                            "incorporation: generation=%lu state=%s "
                            "status=%s memtable_root=%lu "
                            "compacted_root=%lu\n",
                            generation,
                            memtable_state_string(mt->state),
                            platform_status_to_string(mt->incorporation_status),
                            mt->root_addr,
                            compacted_root);
      } else {
         platform_error_log("Shutdown found unincorporated memtable: "
                            "generation=%lu state=%s memtable_root=%lu "
                            "compacted_root=%lu\n",
                            generation,
                            memtable_state_string(mt->state),
                            mt->root_addr,
                            compacted_root);
      }

      if (compacted_root != 0) {
         core_memtable_release_compacted_branch(spl, generation);
      }
   }

   return found_unincorporated;
}

/*
 * This function is only safe to call when all other calls to spl have returned.
 * It intentionally leaves the memtable and log contexts live: the clean
 * checkpoint record needs both after final incorporation has quiesced.
 */
static void
core_quiesce_for_shutdown(core_handle *spl)
{
   // write current memtable to disk
   // (any others must already be flushing/flushed)

   if (!memtable_is_empty(&spl->mt_ctxt)) {
      /*
       * memtable_force_finalize is not thread safe. Note also, we do not hold
       * the insert lock or rotate while flushing the memtable.
       */

      uint64 generation = memtable_force_finalize(&spl->mt_ctxt);
      core_memtable_flush(spl, generation);
   }

   // finish any outstanding tasks and destroy task system for this table.
   platform_status rc = task_perform_until_quiescent(spl->ts);
   platform_assert_status_ok(rc);

   core_report_unincorporated_memtables(spl);
}

static void
core_teardown_after_shutdown(core_handle *spl)
{
   // Keep this after checkpoint publication: it supplies the generation cut.
   memtable_context_deinit(&spl->mt_ctxt);

   // Keep the log alive through clean-record publication. A later explicit
   // tail-sync protocol will own its immutable log metadata separately.
   if (spl->cfg.use_log) {
      platform_free(spl->heap_id, spl->log);
      spl->log = NULL;
   }

   // flush all dirty pages in the cache
   cache_flush(spl->cc);
}

/*
 * Close (unmount) a database without destroying it.
 * It can be re-opened later with core_mount().
 */
platform_status
core_unmount(core_handle *spl)
{
   platform_status rc;

   /*
    * Quiescing leaves the memtable and log contexts live so publication can
    * atomically capture the retired generation and root, then record log
    * metadata. Teardown is safe regardless of publication success.
    */
   core_quiesce_for_shutdown(spl);
   rc = core_publish_checkpoint_record(spl, FALSE, TRUE, FALSE);
   if (!SUCCESS(rc)) {
      platform_error_log("core_unmount: failed to publish checkpoint record: %s\n",
                         platform_status_to_string(rc));
   }
   core_teardown_after_shutdown(spl);
   trunk_context_deinit(&spl->trunk_context);
   core_destroy_stats(spl);
   core_checkpoint_lock_deinit(spl);
   return rc;
}

/*
 * Destroy a database such that it cannot be re-opened later
 */
void
core_destroy(core_handle *spl)
{
   core_quiesce_for_shutdown(spl);
   core_teardown_after_shutdown(spl);
   /* Records own trunk references and their two dedicated record extents. */
   core_destroy_checkpoint_storage(spl);
   trunk_context_deinit(&spl->trunk_context);
   // clear out this splinter table from the meta page.
   allocator_remove_super_addr(spl->al, spl->id);

   core_destroy_stats(spl);
   core_checkpoint_lock_deinit(spl);
}


/*
 *-----------------------------------------------------------------------------
 * core_perform_task
 *
 *      do a batch of tasks
 *-----------------------------------------------------------------------------
 */
void
core_perform_tasks(core_handle *spl)
{
   task_perform_all(spl->ts);
   cache_cleanup(spl->cc);
}

/*
 *-----------------------------------------------------------------------------
 * Debugging and info functions
 *-----------------------------------------------------------------------------
 */

void
core_print_space_use(platform_log_handle *log_handle, core_handle *spl)
{
   trunk_print_space_use(log_handle, &spl->trunk_context);
}

/*
 * core_print_super_block()
 *
 * Print the fixed checkpoint directory and both independently written record
 * slots for a running Splinter instance.
 */
void
core_print_super_block(platform_log_handle *log_handle, core_handle *spl)
{
   core_checkpoint_directory directory;
   platform_status rc = core_get_checkpoint_directory(spl, &directory);
   if (!SUCCESS(rc)) {
      platform_log(log_handle,
                   "No compatible checkpoint directory for root id %lu\n",
                   spl->id);
      return;
   }

   core_checkpoint_records records;
   rc = core_load_checkpoint_records(spl, &directory, &records);
   if (!SUCCESS(rc)) {
      platform_log(log_handle,
                   "Unable to load checkpoint records for root id %lu: %s\n",
                   spl->id,
                   platform_status_to_string(rc));
      return;
   }

   platform_log(log_handle,
                "Checkpoint directory root_id=%lu record_addr=[%lu, %lu] {\n",
                directory.table_id,
                directory.record_addr[0],
                directory.record_addr[1]);
   for (uint64 slot = 0; slot < CORE_CHECKPOINT_RECORD_COUNT; slot++) {
      if (!records.valid[slot]) {
         platform_log(log_handle, "  record[%lu]: invalid\n", slot);
         continue;
      }
      core_checkpoint_record *record = &records.record[slot];
      platform_log(log_handle,
                   "  record[%lu]: sequence=%lu root_addr=%lu "
                   "has_incorporated_generation=%d "
                   "incorporated_generation=%lu "
                   "timestamp=%lu "
                   "checkpointed=%d unmounted=%d\n",
                   slot,
                   record->sequence,
                   record->root_addr,
                   record->has_incorporated_generation,
                   record->incorporated_generation,
                   record->timestamp,
                   record->checkpointed,
                   record->unmounted);
   }
   platform_log(log_handle, "}\n\n");
}

// clang-format off
void
core_print_insertion_stats(platform_log_handle *log_handle, const core_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   uint64 avg_flush_wait_time, avg_flush_time, num_flushes;
   uint64 avg_compaction_tuples, pack_time_per_tuple, avg_setup_time;
   threadid thr_i;

   core_stats *global;

   global = TYPED_ZALLOC(PROCESS_PRIVATE_HEAP_ID, global);
   if (global == NULL) {
      platform_error_log("Out of memory for statistics");
      return;
   }

   histogram *insert_lat_accum;
   histogram *update_lat_accum;
   histogram *delete_lat_accum;
   insert_lat_accum =
      histogram_create(PROCESS_PRIVATE_HEAP_ID,
                       LATENCYHISTO_SIZE + 1,
                       latency_histo_buckets);
   update_lat_accum =
      histogram_create(PROCESS_PRIVATE_HEAP_ID,
                       LATENCYHISTO_SIZE + 1,
                       latency_histo_buckets);
   delete_lat_accum =
      histogram_create(PROCESS_PRIVATE_HEAP_ID,
                       LATENCYHISTO_SIZE + 1,
                       latency_histo_buckets);
   if (insert_lat_accum == NULL || update_lat_accum == NULL
       || delete_lat_accum == NULL)
   {
      platform_error_log("Out of memory for statistics\n");
      histogram_destroy(PROCESS_PRIVATE_HEAP_ID, insert_lat_accum);
      histogram_destroy(PROCESS_PRIVATE_HEAP_ID, update_lat_accum);
      histogram_destroy(PROCESS_PRIVATE_HEAP_ID, delete_lat_accum);
      platform_free(PROCESS_PRIVATE_HEAP_ID, global);
      return;
   }

   for (thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      histogram_merge_in(insert_lat_accum,
                              spl->stats[thr_i].insert_latency_histo);
      histogram_merge_in(update_lat_accum,
                              spl->stats[thr_i].update_latency_histo);
      histogram_merge_in(delete_lat_accum,
                              spl->stats[thr_i].delete_latency_histo);

          global->root_compactions                    += spl->stats[thr_i].root_compactions;
          global->root_compaction_pack_time_ns        += spl->stats[thr_i].root_compaction_pack_time_ns;
          global->root_compaction_tuples              += spl->stats[thr_i].root_compaction_tuples;
          if (spl->stats[thr_i].root_compaction_max_tuples >
               global->root_compaction_max_tuples) {
             global->root_compaction_max_tuples =
               spl->stats[thr_i].root_compaction_max_tuples;
          }
          global->root_compaction_time_ns             += spl->stats[thr_i].root_compaction_time_ns;
          if (spl->stats[thr_i].root_compaction_time_max_ns >
               global->root_compaction_time_max_ns) {
             global->root_compaction_time_max_ns =
               spl->stats[thr_i].root_compaction_time_max_ns;
          }

      global->insertions                  += spl->stats[thr_i].insertions;
      global->updates                     += spl->stats[thr_i].updates;
      global->deletions                   += spl->stats[thr_i].deletions;
      global->discarded_deletes           += spl->stats[thr_i].discarded_deletes;

      global->memtable_flushes            += spl->stats[thr_i].memtable_flushes;
      global->memtable_flush_wait_time_ns += spl->stats[thr_i].memtable_flush_wait_time_ns;
      global->memtable_flush_time_ns      += spl->stats[thr_i].memtable_flush_time_ns;
      if (spl->stats[thr_i].memtable_flush_time_max_ns >
          global->memtable_flush_time_max_ns) {
         global->memtable_flush_time_max_ns =
            spl->stats[thr_i].memtable_flush_time_max_ns;
      }
      global->memtable_flush_root_full    += spl->stats[thr_i].memtable_flush_root_full;
   }

   platform_log(log_handle, "Overall Statistics\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| insertions:        %10lu\n", global->insertions);
   platform_log(log_handle, "| updates:           %10lu\n", global->updates);
   platform_log(log_handle, "| deletions:         %10lu\n", global->deletions);
   platform_log(log_handle, "| completed deletes: %10lu\n", global->discarded_deletes);
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| root stalls:       %10lu\n", global->memtable_flush_root_full);
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");

   platform_log(log_handle, "Latency Histogram Statistics\n");
   histogram_print(insert_lat_accum, "Insert Latency Histogram (ns):", log_handle);
   histogram_print(update_lat_accum, "Update Latency Histogram (ns):", log_handle);
   histogram_print(delete_lat_accum, "Delete Latency Histogram (ns):", log_handle);
   histogram_destroy(PROCESS_PRIVATE_HEAP_ID, insert_lat_accum);
   histogram_destroy(PROCESS_PRIVATE_HEAP_ID, update_lat_accum);
   histogram_destroy(PROCESS_PRIVATE_HEAP_ID, delete_lat_accum);


   platform_log(log_handle, "Flush Statistics\n");
   platform_log(log_handle, "---------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "  height | avg wait time (ns) | avg flush time (ns) | max flush time (ns) | full flushes | count flushes |\n");
   platform_log(log_handle, "---------|--------------------|---------------------|---------------------|--------------|---------------|\n");

   // memtable
   num_flushes = global->memtable_flushes;
   avg_flush_wait_time = num_flushes == 0 ? 0 : global->memtable_flush_wait_time_ns / num_flushes;
   avg_flush_time = num_flushes == 0 ? 0 : global->memtable_flush_time_ns / num_flushes;
   platform_log(log_handle, "memtable | %18lu | %19lu | %19lu | %12lu | %13lu |\n",
                avg_flush_wait_time, avg_flush_time,
                global->memtable_flush_time_max_ns, num_flushes, 0UL);

   platform_log(log_handle, "---------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");

   platform_log(log_handle, "Compaction Statistics\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "  height | compactions | avg setup time (ns) | time / tuple (ns) | avg tuples | max tuples | max time (ns) | empty | aborted | discarded |\n");
   platform_log(log_handle, "---------|-------------|---------------------|-------------------|------------|------------|---------------|-------|---------|-----------|\n");

   avg_setup_time = global->root_compactions == 0 ? 0
      : (global->root_compaction_time_ns - global->root_compaction_pack_time_ns)
            / global->root_compactions;
   avg_compaction_tuples = global->root_compactions == 0 ? 0
      : global->root_compaction_tuples / global->root_compactions;
   pack_time_per_tuple = global->root_compaction_tuples == 0 ? 0
      : global->root_compaction_pack_time_ns / global->root_compaction_tuples;
   platform_log(log_handle, "    root | %11lu | %19lu | %17lu | %10lu | %10lu | %13lu | %5lu | %2lu | %2lu | %3lu | %3lu |\n",
         global->root_compactions, avg_setup_time, pack_time_per_tuple,
         avg_compaction_tuples, global->root_compaction_max_tuples,
         global->root_compaction_time_max_ns, 0UL, 0UL, 0UL, 0UL, 0UL);
   platform_log(log_handle, "------------------------------------------------------------------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");

   platform_log(log_handle, "Filter Build Statistics\n");
   platform_log(log_handle, "---------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| height |   built | avg tuples | avg build time (ns) | build_time / tuple (ns) |\n");
   platform_log(log_handle, "---------|---------|------------|---------------------|-------------------------|\n");

   trunk_print_insertion_stats(log_handle, &spl->trunk_context);

   task_print_stats(spl->ts);
   platform_log(log_handle, "\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   cache_print_stats(log_handle, spl->cc);
   platform_log(log_handle, "\n");
   platform_free(PROCESS_PRIVATE_HEAP_ID, global);
}

void
core_print_lookup_stats(platform_log_handle *log_handle, core_handle *spl)
{
   if (!spl->cfg.use_stats) {
      platform_log(log_handle, "Statistics are not enabled\n");
      return;
   }

   uint64 lookups_found = 0;
   uint64 lookups_not_found = 0;
   for (threadid thr_i = 0; thr_i < MAX_THREADS; thr_i++) {
      lookups_found     += spl->stats[thr_i].lookups_found;
      lookups_not_found += spl->stats[thr_i].lookups_not_found;
   }
   uint64 lookups = lookups_found + lookups_not_found;

   platform_log(log_handle, "Overall Statistics\n");
   platform_log(log_handle, "-----------------------------------------------------------------------------------\n");
   platform_log(log_handle, "| lookups:           %lu\n", lookups);
   platform_log(log_handle, "| lookups found:     %lu\n", lookups_found);
   platform_log(log_handle, "| lookups not found: %lu\n", lookups_not_found);
   platform_log(log_handle, "-----------------------------------------------------------------------------------\n");
   platform_log(log_handle, "\n");
   platform_log(log_handle, "------------------------------------------------------------------------------------\n");
   cache_print_stats(log_handle, spl->cc);
   platform_log(log_handle, "\n");
}
// clang-format on


void
core_print_lookup(core_handle *spl, key target, platform_log_handle *log_handle)
{
   lookup_result lookup;
   lookup_result_init(
      &lookup, spl->cfg.data_cfg, SPLINTERDB_LOOKUP_VALUE, 0, NULL);

   platform_stream_handle stream;
   platform_open_log_stream(&stream);
   uint64 mt_gen_start = memtable_generation(&spl->mt_ctxt);
   uint64 mt_gen_end   = memtable_generation_retired(&spl->mt_ctxt);
   for (uint64 mt_gen = mt_gen_start; mt_gen != mt_gen_end; mt_gen--) {
      bool32 memtable_is_compacted;
      uint64 root_addr = core_memtable_root_addr_for_lookup(
         spl, mt_gen, &memtable_is_compacted, NULL);
      platform_status rc;

      rc = btree_lookup(spl->cc,
                        spl->cfg.btree_cfg,
                        root_addr,
                        PAGE_TYPE_MEMTABLE,
                        target,
                        &lookup);
      platform_assert_status_ok(rc);
      if (lookup_result_found(&lookup)) {
         char    key_str[128];
         char    message_str[128];
         message msg =
            merge_accumulator_to_message(lookup_result_accumulator(&lookup));
         core_key_to_string(spl, target, key_str);
         core_message_to_string(spl, msg, message_str);
         platform_log_stream(
            &stream,
            "Key %s found in memtable %lu (gen %lu comp %d) with data %s\n",
            key_str,
            root_addr,
            mt_gen,
            memtable_is_compacted,
            message_str);
         btree_print_lookup(
            spl->cc, spl->cfg.btree_cfg, root_addr, PAGE_TYPE_MEMTABLE, target);
      }
   }

   trunk_ondisk_node_handle handle;
   trunk_init_root_handle(&spl->trunk_context, &handle);
   trunk_merge_lookup(
      &spl->trunk_context, &handle, target, &lookup, log_handle);
   trunk_ondisk_node_handle_deinit(&handle);
   lookup_result_deinit(&lookup);
}

void
core_reset_stats(core_handle *spl)
{
   if (spl->cfg.use_stats) {
      core_stats *new_stats = core_stats_create(spl->heap_id);
      if (new_stats == NULL) {
         platform_error_log("core_reset_stats: failed to reset stats: %s\n",
                            platform_status_to_string(STATUS_NO_MEMORY));
         return;
      }

      core_destroy_stats(spl);
      spl->stats = new_stats;
   }
}

// basic validation of data_config
static void
core_validate_data_config(const data_config *cfg)
{
   platform_assert(cfg->key_compare != NULL);
}

/*
 *-----------------------------------------------------------------------------
 * core_config_init --
 *
 *       Initialize splinter config
 *       This function calls btree_config_init
 *-----------------------------------------------------------------------------
 */
platform_status
core_config_init(core_config         *core_cfg,
                 cache_config        *cache_cfg,
                 data_config         *data_cfg,
                 btree_config        *btree_cfg,
                 log_config          *log_cfg,
                 trunk_config        *trunk_node_cfg,
                 uint64               queue_scale_percent,
                 uint64               prefetch_budget,
                 bool32               use_log,
                 bool32               use_stats,
                 bool32               verbose_logging,
                 platform_log_handle *log_handle)

{
   core_validate_data_config(data_cfg);

   ZERO_CONTENTS(core_cfg);
   core_cfg->cache_cfg      = cache_cfg;
   core_cfg->data_cfg       = data_cfg;
   core_cfg->btree_cfg      = btree_cfg;
   core_cfg->trunk_node_cfg = trunk_node_cfg;
   core_cfg->log_cfg        = log_cfg;

   core_cfg->queue_scale_percent     = queue_scale_percent;
   core_cfg->prefetch_budget         = prefetch_budget;
   core_cfg->use_log                 = use_log;
   core_cfg->use_stats               = use_stats;
   core_cfg->verbose_logging_enabled = verbose_logging;
   core_cfg->log_handle              = log_handle;

   memtable_config_init(&core_cfg->mt_cfg,
                        core_cfg->btree_cfg,
                        CORE_NUM_MEMTABLES,
                        trunk_node_cfg->incorporation_size_kv_bytes);

   // When everything succeeds, return success.
   return STATUS_OK;
}
