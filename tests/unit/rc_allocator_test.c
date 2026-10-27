// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "ctest.h"
#include "platform_typed_alloc.h"
#include "rc_allocator.h"
#include "rc_allocator_disk.h"
#include "unit_tests.h"

/* Sparse backing avoids allocating the logical 2.4 GiB device in memory. */
#define SNAPSHOT_TEST_EXTENTS      20001
#define SNAPSHOT_TEST_LARGE_COUNTS 9000

typedef struct snapshot_test_io {
   io_handle super;
   int       fd;
   uint64    reads;
   uint64    writes;
   uint64    barriers;
   uint64    fail_read;
   uint64    fail_write;
   uint64    fail_barrier;
} snapshot_test_io;

static platform_status
snapshot_test_read(io_handle *handle, void *buffer, uint64 bytes, uint64 addr)
{
   snapshot_test_io *io = (snapshot_test_io *)handle;
   if (++io->reads == io->fail_read) {
      return STATUS_IO_ERROR;
   }
   uint64 done = 0;
   while (done < bytes) {
      ssize_t result =
         pread(io->fd, (char *)buffer + done, bytes - done, addr + done);
      if (result < 0 && errno == EINTR) {
         continue;
      }
      if (result <= 0) {
         return STATUS_IO_ERROR;
      }
      done += result;
   }
   return STATUS_OK;
}

static platform_status
snapshot_test_write(io_handle *handle, void *buffer, uint64 bytes, uint64 addr)
{
   snapshot_test_io *io = (snapshot_test_io *)handle;
   if (++io->writes == io->fail_write) {
      return STATUS_IO_ERROR;
   }
   uint64 done = 0;
   while (done < bytes) {
      ssize_t result =
         pwrite(io->fd, (char *)buffer + done, bytes - done, addr + done);
      if (result < 0 && errno == EINTR) {
         continue;
      }
      if (result <= 0) {
         return STATUS_IO_ERROR;
      }
      done += result;
   }
   return STATUS_OK;
}

static platform_status
snapshot_test_range(io_handle *handle,
                    uint64     addr,
                    uint64     bytes,
                    bool32    *readable)
{
   snapshot_test_io *io = (snapshot_test_io *)handle;
   struct stat       status;
   if (fstat(io->fd, &status) != 0 || bytes > UINT64_MAX - addr) {
      return STATUS_IO_ERROR;
   }
   *readable = addr + bytes <= (uint64)status.st_size;
   return STATUS_OK;
}

static platform_status
snapshot_test_barrier(io_handle *handle)
{
   snapshot_test_io *io = (snapshot_test_io *)handle;
   return ++io->barriers == io->fail_barrier ? STATUS_IO_ERROR : STATUS_OK;
}

static const io_ops snapshot_test_io_ops = {
   .read              = snapshot_test_read,
   .write             = snapshot_test_write,
   .range_is_readable = snapshot_test_range,
   .durable_barrier   = snapshot_test_barrier,
};

CTEST_DATA(rc_allocator)
{
   platform_heap_id heap_id;
   io_config        io_cfg;
   allocator_config cfg;
   snapshot_test_io io;
   rc_allocator    *source;
   rc_allocator    *loaded;
   bool32           loaded_attached;
   uint64           reserved;
};

CTEST_SETUP(rc_allocator)
{
   ASSERT_EQUAL(0, platform_register_thread());
   bool use_shmem = FALSE;
   for (int arg = 1; arg < Ctest_argc; arg++) {
      use_shmem |= strcmp(Ctest_argv[arg], "--use-shmem") == 0;
   }
   ASSERT_TRUE(SUCCESS(platform_heap_create(
      platform_get_module_id(), 32 * Mega, use_shmem, &data->heap_id)));
   ZERO_CONTENTS(&data->io);
   data->io.super.ops = &snapshot_test_io_ops;
   char path[]        = "/tmp/splinter-rc-snapshot-XXXXXX";
   data->io.fd        = mkstemp(path);
   ASSERT_TRUE(data->io.fd >= 0);
   ASSERT_EQUAL(0, unlink(path));
   ZERO_CONTENTS(&data->io_cfg);
   data->io_cfg.page_size         = IO_DEFAULT_PAGE_SIZE;
   data->io_cfg.extent_size       = IO_DEFAULT_EXTENT_SIZE;
   data->io_cfg.flags             = O_RDWR;
   data->io_cfg.perms             = 0600;
   data->io_cfg.kernel_queue_size = IO_DEFAULT_KERNEL_QUEUE_SIZE;
   allocator_config_init(&data->cfg,
                         &data->io_cfg,
                         SNAPSHOT_TEST_EXTENTS
                            * (uint64)IO_DEFAULT_EXTENT_SIZE);
   ASSERT_EQUAL(0, ftruncate(data->io.fd, data->cfg.capacity));
   data->source = TYPED_ZALLOC(data->heap_id, data->source);
   data->loaded = TYPED_ZALLOC(data->heap_id, data->loaded);
   ASSERT_NOT_NULL(data->source);
   ASSERT_NOT_NULL(data->loaded);
   data->loaded_attached = FALSE;
   ASSERT_TRUE(SUCCESS(rc_allocator_init(data->source,
                                         &data->cfg,
                                         &data->io.super,
                                         data->heap_id,
                                         platform_get_module_id())));
   data->reserved = allocator_in_use(&data->source->super);
   ASSERT_EQUAL(2, data->reserved);
}

CTEST_TEARDOWN(rc_allocator)
{
   if (data->loaded_attached) {
      rc_allocator_deinit(data->loaded);
   }
   rc_allocator_deinit(data->source);
   platform_free(data->heap_id, data->source);
   platform_free(data->heap_id, data->loaded);
   ASSERT_EQUAL(0, close(data->io.fd));
   platform_heap_destroy(&data->heap_id);
   platform_deregister_thread();
}

static void
snapshot_set_count(rc_allocator *al, uint64 extent, refcount count)
{
#if SPLINTER_RC_COMPACT
   ASSERT_TRUE(SUCCESS(rc_refcount_set(&al->compact, extent, count)));
#else
   __atomic_store_n(&al->ref_count[extent], count, __ATOMIC_RELAXED);
#endif
}

static uint64
snapshot_allocate(rc_allocator *al, refcount count)
{
   uint64 addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_alloc(&al->super, &addr, PAGE_TYPE_MISC)));
   snapshot_set_count(al, addr / al->cfg->io_cfg->extent_size, count);
   return addr;
}

static void snapshot_remount(struct CTEST_IMPL_DATA_SNAME(rc_allocator) * data)
{
   if (data->loaded_attached) {
      rc_allocator_deinit(data->loaded);
   }
   ASSERT_TRUE(SUCCESS(rc_allocator_mount(data->loaded,
                                          &data->cfg,
                                          &data->io.super,
                                          data->heap_id,
                                          platform_get_module_id())));
   data->loaded_attached = TRUE;
   ASSERT_FALSE(data->loaded->map_is_valid);
}

static void
snapshot_compare_maps(rc_allocator *first, rc_allocator *second)
{
   for (uint64 extent = 0; extent < first->cfg->extent_capacity; extent++) {
      uint64 addr = extent * first->cfg->io_cfg->extent_size;
      ASSERT_EQUAL_U(allocator_get_refcount(&first->super, addr),
                     allocator_get_refcount(&second->super, addr));
   }
   ASSERT_EQUAL_U(allocator_in_use(&first->super),
                  allocator_in_use(&second->super));
}

static void
snapshot_rebuild_only(struct CTEST_IMPL_DATA_SNAME(rc_allocator) * data,
                      rc_allocator *al,
                      uint64        user_addr)
{
   ASSERT_TRUE(SUCCESS(allocator_recovery_begin(&al->super)));
   ASSERT_FALSE(al->map_is_valid);
   ASSERT_EQUAL_U(data->reserved, allocator_in_use(&al->super));
   ASSERT_TRUE(SUCCESS(allocator_recovery_record_reference(
      &al->super, user_addr, PAGE_TYPE_MISC)));
   allocator_recovery_finish(&al->super);
   ASSERT_TRUE(al->map_is_valid);
   ASSERT_EQUAL_U(data->reserved + 1, allocator_in_use(&al->super));
   ASSERT_EQUAL_U(AL_ONE_REF, allocator_get_refcount(&al->super, user_addr));
}

CTEST2(rc_allocator, empty_overflow_roundtrip)
{
   uint64 addresses[14];
   for (refcount count = 1; count < 15; count++) {
      addresses[count - 1] = snapshot_allocate(data->source, count);
   }
   uint64 state_addr = UINT64_MAX;
   uint64 before     = allocator_in_use(&data->source->super);
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   ASSERT_EQUAL_U(data->io_cfg.extent_size, state_addr);
   ASSERT_EQUAL_U(before, allocator_in_use(&data->source->super));
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   snapshot_compare_maps(data->source, data->loaded);
   ASSERT_TRUE(SUCCESS(rc_allocator_release_snapshot(data->loaded)));
   ASSERT_EQUAL_U(before, allocator_in_use(&data->loaded->super));
   for (refcount count = 1; count < 15; count++) {
      ASSERT_EQUAL_U(
         count,
         allocator_get_refcount(&data->loaded->super, addresses[count - 1]));
   }
}

CTEST2(rc_allocator, wide_counts_multiple_overflow_extents_and_release)
{
   const refcount counts[] = {
      15, (uint64)UINT32_MAX + 1, UINT64_C(1) << 63, UINT64_MAX};
   for (uint64 i = 0; i < SNAPSHOT_TEST_LARGE_COUNTS; i++) {
      snapshot_allocate(data->source, counts[i % ARRAY_SIZE(counts)]);
   }
   uint64 before     = allocator_in_use(&data->source->super);
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   uint64 chain_count = allocator_in_use(&data->source->super) - before;
   ASSERT_TRUE(chain_count > 1);
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   snapshot_compare_maps(data->source, data->loaded);
   ASSERT_EQUAL_U(before + chain_count, allocator_in_use(&data->loaded->super));
   uint64 unpublished = UINT64_MAX;
   uint64 writes      = data->io.writes;
   ASSERT_TRUE(STATUS_IS_EQ(
      STATUS_BUSY, allocator_persist(&data->loaded->super, &unpublished)));
   ASSERT_EQUAL_U(UINT64_MAX, unpublished);
   ASSERT_EQUAL_U(writes, data->io.writes);
   /* A release preflight must not free a prefix when a later node is shared. */
   uint64 shared_node = data->loaded->snapshot_extents[chain_count - 1];
   ASSERT_EQUAL_U(3, allocator_inc_ref(&data->loaded->super, shared_node));
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_INVALID_STATE,
                            rc_allocator_release_snapshot(data->loaded)));
   ASSERT_EQUAL_U(before + chain_count, allocator_in_use(&data->loaded->super));
   ASSERT_EQUAL_U(AL_ONE_REF,
                  allocator_get_refcount(&data->loaded->super,
                                         data->loaded->snapshot_extents[0]));
   ASSERT_EQUAL_U(
      AL_ONE_REF,
      allocator_dec_ref(&data->loaded->super, shared_node, PAGE_TYPE_MISC));
   /* The caller has durably invalidated the superblock before invoking this. */
   ASSERT_TRUE(SUCCESS(rc_allocator_release_snapshot(data->loaded)));
   ASSERT_EQUAL_U(before, allocator_in_use(&data->loaded->super));
   ASSERT_TRUE(SUCCESS(rc_allocator_release_snapshot(data->loaded)));
   ASSERT_EQUAL_U(before, allocator_in_use(&data->loaded->super));
   for (uint64 i = 0; i < SNAPSHOT_TEST_LARGE_COUNTS; i++) {
      uint64 addr = (data->reserved + i) * data->io_cfg.extent_size;
      ASSERT_EQUAL_U(counts[i % ARRAY_SIZE(counts)],
                     allocator_get_refcount(&data->loaded->super, addr));
   }
   snapshot_rebuild_only(
      data, data->source, data->reserved * data->io_cfg.extent_size);
   snapshot_rebuild_only(
      data, data->loaded, data->reserved * data->io_cfg.extent_size);
   snapshot_compare_maps(data->source, data->loaded);
}

CTEST2(rc_allocator, write_and_barrier_failure_do_not_publish_or_leak)
{
   uint64 addr = snapshot_allocate(data->source, UINT64_MAX);
   for (uint64 i = 1; i < SNAPSHOT_TEST_LARGE_COUNTS; i++) {
      snapshot_allocate(data->source, 15);
   }
   uint64 expected_in_use = allocator_in_use(&data->source->super);
   /* Successful writes establish how many distinct write failure positions to
    * exercise; releasing simulates a subsequent durable dirty publication. */
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   uint64 write_count = data->io.writes;
   ASSERT_TRUE(write_count >= 2);
   ASSERT_TRUE(SUCCESS(rc_allocator_release_snapshot(data->source)));
   ASSERT_EQUAL_U(expected_in_use, allocator_in_use(&data->source->super));
   for (uint64 failure = 1; failure <= write_count; failure++) {
      data->io.writes     = 0;
      data->io.fail_write = failure;
      state_addr          = UINT64_MAX;
      ASSERT_TRUE(
         STATUS_IS_EQ(STATUS_IO_ERROR,
                      allocator_persist(&data->source->super, &state_addr)));
      ASSERT_EQUAL_U(UINT64_MAX, state_addr);
      ASSERT_EQUAL_U(expected_in_use, allocator_in_use(&data->source->super));
      ASSERT_EQUAL_U(UINT64_MAX,
                     allocator_get_refcount(&data->source->super, addr));
   }
   data->io.fail_write   = 0;
   data->io.barriers     = 0;
   data->io.fail_barrier = 1;
   state_addr            = UINT64_MAX;
   ASSERT_TRUE(STATUS_IS_EQ(
      STATUS_IO_ERROR, allocator_persist(&data->source->super, &state_addr)));
   ASSERT_EQUAL_U(UINT64_MAX, state_addr);
   ASSERT_EQUAL_U(expected_in_use, allocator_in_use(&data->source->super));
   data->io.fail_barrier = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   snapshot_compare_maps(data->source, data->loaded);
}

CTEST2(rc_allocator, out_of_space_does_not_publish_or_change_counts)
{
   /* Leave one free extent: an overflowing snapshot needs three, so a writer
    * that allocates incrementally must roll back the allocated prefix. */
   for (uint64 extent = data->reserved; extent + 1 < data->cfg.extent_capacity;
        extent++)
   {
      snapshot_allocate(data->source, 15);
   }
   uint64 before     = allocator_in_use(&data->source->super);
   uint64 state_addr = UINT64_MAX;
   ASSERT_FALSE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   ASSERT_EQUAL_U(UINT64_MAX, state_addr);
   ASSERT_EQUAL_U(before, allocator_in_use(&data->source->super));
   uint64 last_addr =
      (data->cfg.extent_capacity - 1) * data->io_cfg.extent_size;
   ASSERT_EQUAL_U(AL_FREE,
                  allocator_get_refcount(&data->source->super, last_addr));
   ASSERT_EQUAL_U(last_addr, snapshot_allocate(data->source, 15));
   before++;
   ASSERT_FALSE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   ASSERT_EQUAL_U(UINT64_MAX, state_addr);
   ASSERT_EQUAL_U(before, allocator_in_use(&data->source->super));
   for (uint64 extent = data->reserved; extent < data->cfg.extent_capacity;
        extent++)
   {
      ASSERT_EQUAL_U(15,
                     allocator_get_refcount(&data->source->super,
                                            extent * data->io_cfg.extent_size));
   }
   snapshot_rebuild_only(
      data, data->source, data->reserved * data->io_cfg.extent_size);
}

CTEST2(rc_allocator, io_read_failure_leaves_map_invalid_and_rebuildable)
{
   uint64 addr       = snapshot_allocate(data->source, (uint64)UINT32_MAX + 7);
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   data->io.reads = 0;
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   uint64 read_count = data->io.reads;
   ASSERT_TRUE(read_count >= 2);
   for (uint64 failure = 1; failure <= read_count; failure++) {
      snapshot_remount(data);
      data->io.reads     = 0;
      data->io.fail_read = failure;
      ASSERT_FALSE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
      ASSERT_FALSE(data->loaded->map_is_valid);
      data->io.fail_read = 0;
      snapshot_rebuild_only(data, data->loaded, addr);
   }
}


/* Keep complete disk blocks to recompute their checksums after structural
 * mutations. These cases must reach semantic checks, not merely checksum
 * rejection. Field offsets intentionally follow the documented wire format. */
typedef struct snapshot_test_image {
   disk_geometry            geometry;
   rc_allocator_disk_layout layout;
   uint8                   *header;
   uint8                   *tags;
   uint8                   *nodes;
   uint64                  *addresses;
   uint64                   node_count;
} snapshot_test_image;

static snapshot_test_image snapshot_capture(
   struct CTEST_IMPL_DATA_SNAME(rc_allocator) * data)
{
   snapshot_test_image image = {
      .geometry = {data->cfg.capacity,
                   data->io_cfg.page_size,
                   data->io_cfg.extent_size},
   };
   ASSERT_TRUE(SUCCESS(
      rc_allocator_disk_compute_layout(&image.geometry, &image.layout)));
   image.header = malloc(image.geometry.page_size);
   image.tags   = malloc(image.layout.tag_disk_bytes);
   ASSERT_NOT_NULL(image.header);
   ASSERT_NOT_NULL(image.tags);
   ASSERT_TRUE(SUCCESS(snapshot_test_read(&data->io.super,
                                          image.header,
                                          image.geometry.page_size,
                                          image.geometry.extent_size)));
   ASSERT_TRUE(SUCCESS(snapshot_test_read(&data->io.super,
                                          image.tags,
                                          image.layout.tag_disk_bytes,
                                          image.geometry.extent_size
                                             + image.geometry.page_size)));
   image.node_count = rc_allocator_disk_get_u64(image.header + 72);
   ASSERT_TRUE(image.node_count > 0);
   image.nodes     = malloc(image.node_count * image.geometry.extent_size);
   image.addresses = malloc(image.node_count * sizeof(*image.addresses));
   ASSERT_NOT_NULL(image.nodes);
   ASSERT_NOT_NULL(image.addresses);
   uint64 addr = rc_allocator_disk_get_u64(image.header + 80);
   for (uint64 node = 0; node < image.node_count; node++) {
      ASSERT_TRUE(addr != 0);
      image.addresses[node] = addr;
      uint8 *block          = image.nodes + node * image.geometry.extent_size;
      ASSERT_TRUE(SUCCESS(snapshot_test_read(
         &data->io.super, block, image.geometry.extent_size, addr)));
      addr = rc_allocator_disk_get_u64(block + 32);
   }
   ASSERT_EQUAL_U(0, addr);
   return image;
}

static void
snapshot_image_deinit(snapshot_test_image *image)
{
   free(image->header);
   free(image->tags);
   free(image->nodes);
   free(image->addresses);
}

static void
snapshot_image_seal(snapshot_test_image *image)
{
   for (uint64 node = 0; node < image->node_count; node++) {
      rc_allocator_disk_seal_block(image->nodes
                                      + node * image->geometry.extent_size,
                                   image->geometry.extent_size);
   }
   checksum128 tags  = XXH3_128bits(image->tags, image->layout.tag_bytes);
   checksum128 chain = XXH3_128bits(
      image->nodes, image->node_count * image->geometry.extent_size);
   rc_allocator_disk_put_u64(image->header + 88, tags.low64);
   rc_allocator_disk_put_u64(image->header + 96, tags.high64);
   rc_allocator_disk_put_u64(image->header + 104, chain.low64);
   rc_allocator_disk_put_u64(image->header + 112, chain.high64);
   rc_allocator_disk_seal_block(image->header, image->geometry.page_size);
}

static void
snapshot_image_write(struct CTEST_IMPL_DATA_SNAME(rc_allocator) * data,
                     const snapshot_test_image *image)
{
   ASSERT_TRUE(SUCCESS(snapshot_test_write(&data->io.super,
                                           image->header,
                                           image->geometry.page_size,
                                           image->geometry.extent_size)));
   ASSERT_TRUE(SUCCESS(snapshot_test_write(&data->io.super,
                                           image->tags,
                                           image->layout.tag_disk_bytes,
                                           image->geometry.extent_size
                                              + image->geometry.page_size)));
   for (uint64 node = 0; node < image->node_count; node++) {
      ASSERT_TRUE(SUCCESS(
         snapshot_test_write(&data->io.super,
                             image->nodes + node * image->geometry.extent_size,
                             image->geometry.extent_size,
                             image->addresses[node])));
   }
}

static void
snapshot_image_set_tag(snapshot_test_image *image, uint64 extent, uint8 tag)
{
   uint8 shift = (extent % 2) * 4;
   image->tags[extent / 2] =
      (image->tags[extent / 2] & ~(15 << shift)) | tag << shift;
}

CTEST2(rc_allocator, checksums_and_structural_corruption_rejected)
{
   for (uint64 i = 0; i < SNAPSHOT_TEST_LARGE_COUNTS; i++) {
      snapshot_allocate(data->source, 15 + i);
   }
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   snapshot_test_image pristine = snapshot_capture(data);
   ASSERT_TRUE(pristine.node_count > 1);
   for (unsigned corruption = 0; corruption < 17; corruption++) {
      snapshot_test_image image = snapshot_capture(data);
      uint8              *last_node =
         image.nodes + (image.node_count - 1) * image.geometry.extent_size;
      uint64 last_records = rc_allocator_disk_get_u64(last_node + 40);
      uint8 *last_pair    = last_node + RC_ALLOCATOR_DISK_NODE_HEADER_BYTES
                         + (last_records - 1) * RC_ALLOCATOR_DISK_RECORD_BYTES;
      bool32 reseal = TRUE;
      switch (corruption) {
         case 0: /* Header checksum. */
            image.header[0] ^= 1;
            reseal = FALSE;
            break;
         case 1: /* Packed-tag aggregate checksum. */
            image.tags[data->reserved / 2] ^= 1;
            reseal = FALSE;
            break;
         case 2: /* Overflow-block checksum. */
            image.nodes[0] ^= 1;
            reseal = FALSE;
            break;
         case 3: /* A cycle must be rejected even when blocks are resealed. */
            rc_allocator_disk_put_u64(image.nodes + 32, image.addresses[0]);
            break;
         case 4: /* Escape record with an inline count. */
            rc_allocator_disk_put_u64(image.nodes + 64 + 8, 14);
            break;
         case 5: /* Duplicate sorted extent identity. */
            rc_allocator_disk_put_u64(
               image.nodes + 64 + 16,
               rc_allocator_disk_get_u64(image.nodes + 64));
            break;
         case 6: /* Escape record whose array tag is not an escape. */
            snapshot_image_set_tag(&image, data->reserved, AL_ONE_REF);
            break;
         case 7: /* Escape tag whose corresponding record is missing. */
            rc_allocator_disk_put_u64(
               image.header + 64,
               rc_allocator_disk_get_u64(image.header + 64) - 1);
            rc_allocator_disk_put_u64(last_node + 40, last_records - 1);
            memset(last_pair, 0, RC_ALLOCATOR_DISK_RECORD_BYTES);
            break;
         case 8: /* The fixed reserved prefix must remain allocated. */
            snapshot_image_set_tag(&image, 0, AL_FREE);
            break;
         case 9: /* Overflow-chain storage cannot itself be multiply owned. */
            snapshot_image_set_tag(
               &image, image.addresses[0] / image.geometry.extent_size, 3);
            break;
         case 10: /* Odd capacity leaves an unused high nibble. */
            image.tags[image.layout.tag_bytes - 1] |= 0xf0;
            break;
         case 11: /* Tag I/O padding is not part of the aggregate hash. */
            image.tags[image.layout.tag_bytes] = 1;
            break;
         case 12: /* Unused header bytes. */
            image.header[RC_ALLOCATOR_DISK_HEADER_BYTES] = 1;
            break;
         case 13: /* Unused record bytes in the final overflow block. */
            last_pair[RC_ALLOCATOR_DISK_RECORD_BYTES] = 1;
            break;
         case 14: /* Sequence numbers constrain linked traversal. */
            rc_allocator_disk_put_u64(image.nodes + 24, 1);
            break;
         case 15: /* The allocator cannot load a different device geometry. */
            rc_allocator_disk_put_u64(image.header + 32,
                                      image.layout.extent_count + 2);
            break;
         case 16: /* Old format version with a valid header checksum. */
            rc_allocator_disk_put_u64(image.header + 24, 2);
            break;
      }
      if (reseal) {
         snapshot_image_seal(&image);
      }
      snapshot_image_write(data, &image);
      snapshot_remount(data);
      platform_status rc = allocator_load_refcounts(&data->loaded->super);
      ASSERT_TRUE(STATUS_IS_EQ(STATUS_INVALID_STATE, rc),
                  "corruption case %u was not rejected",
                  corruption);
      ASSERT_FALSE(data->loaded->map_is_valid);
      snapshot_rebuild_only(
         data, data->loaded, data->reserved * data->io_cfg.extent_size);
      snapshot_image_deinit(&image);
      snapshot_image_write(data, &pristine);
   }
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   snapshot_compare_maps(data->source, data->loaded);
   snapshot_image_deinit(&pristine);
}

CTEST2(rc_allocator, truncated_snapshot_rejected)
{
   for (uint64 i = 0; i < SNAPSHOT_TEST_LARGE_COUNTS; i++) {
      snapshot_allocate(data->source, (uint64)UINT32_MAX + i);
   }
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   snapshot_test_image image = snapshot_capture(data);
   uint64              end =
      image.addresses[image.node_count - 1] + image.geometry.extent_size;
   ASSERT_EQUAL(0, ftruncate(data->io.fd, end - 1));
   snapshot_remount(data);
   ASSERT_FALSE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   ASSERT_FALSE(data->loaded->map_is_valid);
   snapshot_rebuild_only(
      data, data->loaded, data->reserved * data->io_cfg.extent_size);
   snapshot_image_deinit(&image);
}


#if SPLINTER_DEBUG && SPLINTER_RC_COMPACT
CTEST2(rc_allocator, overflow_import_oom_leaves_map_invalid)
{
   uint64 addr       = snapshot_allocate(data->source, UINT64_MAX);
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   data->loaded->compact.test_allocations_before_failure = 0;
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_NO_MEMORY,
                            allocator_load_refcounts(&data->loaded->super)));
   ASSERT_FALSE(data->loaded->map_is_valid);
   ASSERT_EQUAL_U(0, data->loaded->snapshot_extent_count);
   data->loaded->compact.test_allocations_before_failure = -1;
   snapshot_rebuild_only(data, data->loaded, addr);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   snapshot_compare_maps(data->source, data->loaded);
}
#endif

CTEST2(rc_allocator, tag_stream_crosses_chunk_and_reserved_extent_boundaries)
{
   rc_allocator_deinit(data->source);
   const uint64 extents = 2 * RC_ALLOCATOR_DISK_CHUNK_BYTES + 33;
   allocator_config_init(
      &data->cfg, &data->io_cfg, extents * data->io_cfg.extent_size);
   ASSERT_EQUAL(0, ftruncate(data->io.fd, data->cfg.capacity));
   ASSERT_TRUE(SUCCESS(rc_allocator_init(data->source,
                                         &data->cfg,
                                         &data->io.super,
                                         data->heap_id,
                                         platform_get_module_id())));
   data->reserved = allocator_in_use(&data->source->super);
   ASSERT_TRUE(data->reserved > 2);
   uint64   indices[] = {2 * RC_ALLOCATOR_DISK_CHUNK_BYTES - 1,
                         2 * RC_ALLOCATOR_DISK_CHUNK_BYTES,
                         extents - 1};
   refcount counts[]  = {14, 15, UINT64_MAX};
   ASSERT_TRUE(SUCCESS(allocator_recovery_begin(&data->source->super)));
   for (uint64 i = 0; i < ARRAY_SIZE(indices); i++) {
      ASSERT_TRUE(SUCCESS(allocator_recovery_record_reference(
         &data->source->super,
         indices[i] * data->io_cfg.extent_size,
         PAGE_TYPE_MISC)));
   }
   allocator_recovery_finish(&data->source->super);
   for (uint64 i = 0; i < ARRAY_SIZE(indices); i++) {
      snapshot_set_count(data->source, indices[i], counts[i]);
   }
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   snapshot_compare_maps(data->source, data->loaded);
   ASSERT_TRUE(SUCCESS(rc_allocator_release_snapshot(data->loaded)));
   ASSERT_EQUAL_U(data->reserved + ARRAY_SIZE(indices),
                  allocator_in_use(&data->loaded->super));
}


#if PLATFORM_MEMORY_FAULT_INJECTION
CTEST2(rc_allocator, persist_allocation_oom_preserves_live_map)
#else
CTEST2_SKIP(rc_allocator, persist_allocation_oom_preserves_live_map)
#endif
{
   uint64 addr   = snapshot_allocate(data->source, UINT64_MAX);
   uint64 before = allocator_in_use(&data->source->super);
   platform_memory_fault_config saved;
   platform_memory_fault_config_get(&saved);
   /* The writer allocates its tag buffer, node buffer and chain vector before
    * claiming any snapshot extent. Fail each allocation independently. */
   for (uint64 fail_at = 1; fail_at <= 3; fail_at++) {
      platform_memory_fault_config fault = saved;
      fault.mode                         = PLATFORM_MEMORY_FAULT_RANGE;
      fault.range_start                  = fail_at;
      fault.range_count                  = 1;
      fault.max_failures                 = 1;
      fault.verbose                      = TRUE;
      platform_memory_fault_config_set(&fault);
      platform_memory_fault_reset_counters();
      platform_memory_fault_enable();
      uint64          state_addr = UINT64_MAX;
      platform_status status =
         allocator_persist(&data->source->super, &state_addr);
      platform_memory_fault_counters counters =
         platform_memory_fault_get_counters();
      /* Always disarm injection before any test assertion can exit the case. */
      platform_memory_fault_disable();
      platform_memory_fault_config_set(&saved);
      ASSERT_EQUAL_U(1, counters.failure_count);
      ASSERT_TRUE(counters.alloc_count >= fail_at);
      ASSERT_TRUE(STATUS_IS_EQ(STATUS_NO_MEMORY, status));
      ASSERT_EQUAL_U(UINT64_MAX, state_addr);
      ASSERT_TRUE(data->source->map_is_valid);
      ASSERT_EQUAL_U(before, allocator_in_use(&data->source->super));
      ASSERT_NULL(data->source->snapshot_extents);
      ASSERT_EQUAL_U(0, data->source->snapshot_extent_count);
      ASSERT_EQUAL_U(0, data->source->snapshot_extent_capacity);
      ASSERT_EQUAL_U(0, data->io.writes);
      ASSERT_EQUAL_U(0, data->io.barriers);
      for (uint64 extent = 0; extent < data->cfg.extent_capacity; extent++) {
         refcount expected     = extent < data->reserved ? AL_ONE_REF : AL_FREE;
         uint64   current_addr = extent * data->io_cfg.extent_size;
         if (current_addr == addr) {
            expected = UINT64_MAX;
         }
         ASSERT_EQUAL_U(
            expected,
            allocator_get_refcount(&data->source->super, current_addr));
      }
   }
   uint64 state_addr = 0;
   ASSERT_TRUE(SUCCESS(allocator_persist(&data->source->super, &state_addr)));
   snapshot_remount(data);
   ASSERT_TRUE(SUCCESS(allocator_load_refcounts(&data->loaded->super)));
   snapshot_compare_maps(data->source, data->loaded);
}
