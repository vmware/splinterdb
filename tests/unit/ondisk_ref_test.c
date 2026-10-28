// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "ctest.h"
#include "lookup_result.h"
#include "ondisk_ref.h"

typedef struct test_owner {
   uint64          refs;
   uint64          inc_calls;
   uint64          dec_calls;
   uint64         *clock;
   uint64          last_inc;
   uint64          last_dec;
   platform_status failure;
} test_owner;

static platform_status
owner_inc(const ondisk_ref *ref)
{
   test_owner *owner = (test_owner *)ref->arg;
   owner->inc_calls++;
   if (!SUCCESS(owner->failure)) {
      return owner->failure;
   }
   owner->refs++;
   if (owner->clock != NULL) {
      owner->last_inc = ++*owner->clock;
   }
   return STATUS_OK;
}

static void
owner_dec(const ondisk_ref *ref)
{
   test_owner *owner = (test_owner *)ref->arg;
   ASSERT_TRUE(owner->refs != 0);
   owner->refs--;
   owner->dec_calls++;
   if (owner->clock != NULL) {
      owner->last_dec = ++*owner->clock;
   }
}

static void
owner_adopt(ondisk_ref *ref, test_owner *owner)
{
   owner->refs++;
   ondisk_ref_init_adopt(
      ref, NULL, 4096, PAGE_TYPE_BRANCH, owner, owner_inc, owner_dec);
}

CTEST(ondisk_ref, failed_init_and_copy_are_null)
{
   const platform_status failures[] = {STATUS_NO_MEMORY, STATUS_LIMIT_EXCEEDED};
   for (uint64 i = 0; i < ARRAY_SIZE(failures); i++) {
      test_owner owner = {.failure = failures[i]};
      ondisk_ref ref;
      ASSERT_TRUE(STATUS_IS_EQ(
         failures[i],
         ondisk_ref_init(
            &ref, NULL, 4096, PAGE_TYPE_BRANCH, &owner, owner_inc, owner_dec)));
      ASSERT_TRUE(ondisk_ref_is_null(&ref));
      ondisk_ref_deinit(&ref);
      ASSERT_EQUAL(0, owner.refs);
      ASSERT_EQUAL(0, owner.dec_calls);

      ondisk_ref source;
      owner_adopt(&source, &owner);
      ASSERT_TRUE(STATUS_IS_EQ(failures[i], ondisk_ref_copy(&ref, &source)));
      ASSERT_TRUE(ondisk_ref_is_null(&ref));
      ASSERT_FALSE(ondisk_ref_is_null(&source));
      ASSERT_EQUAL(1, owner.refs);
      ondisk_ref_deinit(&ref);
      owner.failure = STATUS_OK;
      ASSERT_TRUE(SUCCESS(ondisk_ref_copy(&ref, &source)));
      ASSERT_EQUAL(2, owner.refs);
      ondisk_ref_deinit(&source);
      ondisk_ref_deinit(&ref);
      ASSERT_EQUAL(0, owner.refs);
      ASSERT_EQUAL(2, owner.dec_calls);
   }
}

CTEST(ondisk_ref,
      replace_preserves_owner_on_failure_and_acquires_before_release)
{
   uint64     clock = 0;
   test_owner old   = {.clock = &clock};
   test_owner next  = {.clock = &clock, .failure = STATUS_NO_MEMORY};
   ondisk_ref dst, src;
   owner_adopt(&dst, &old);
   owner_adopt(&src, &next);
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_NO_MEMORY, ondisk_ref_replace(&dst, &src)));
   ASSERT_TRUE(dst.arg == &old);
   ASSERT_TRUE(src.arg == &next);
   ASSERT_EQUAL(1, old.refs);
   ASSERT_EQUAL(1, next.refs);
   ASSERT_EQUAL(0, old.dec_calls);

   // Self-replacement requires neither a retain nor a release, even if the
   // callback would fail. Replacing with null releases exactly once.
   ASSERT_TRUE(SUCCESS(ondisk_ref_replace(&src, &src)));
   ASSERT_EQUAL(1, next.inc_calls);
   next.failure = STATUS_OK;
   ASSERT_TRUE(SUCCESS(ondisk_ref_replace(&dst, &src)));
   ASSERT_TRUE(dst.arg == &next);
   ASSERT_TRUE(next.last_inc < old.last_dec);
   ASSERT_EQUAL(0, old.refs);
   ASSERT_EQUAL(2, next.refs);
   ASSERT_TRUE(SUCCESS(ondisk_ref_replace(&dst, NULL)));
   ASSERT_TRUE(ondisk_ref_is_null(&dst));
   ASSERT_EQUAL(1, next.refs);
   ondisk_ref_deinit(&src);
   ASSERT_EQUAL(0, next.refs);
}

CTEST(ondisk_ref, move_consumes_source_without_retain)
{
   test_owner old = {0}, next = {.failure = STATUS_NO_MEMORY};
   ondisk_ref dst, src;
   owner_adopt(&dst, &old);
   owner_adopt(&src, &next);
   ondisk_ref_move(&dst, &src);
   ASSERT_TRUE(ondisk_ref_is_null(&src));
   ASSERT_TRUE(dst.arg == &next);
   ASSERT_EQUAL(0, old.refs);
   ASSERT_EQUAL(1, next.refs);
   ASSERT_EQUAL(0, next.inc_calls);
   ondisk_ref_move(&dst, &dst);
   ASSERT_EQUAL(1, next.refs);
   ondisk_ref_deinit(&src);
   ondisk_ref_deinit(&dst);
   ASSERT_EQUAL(1, old.dec_calls);
   ASSERT_EQUAL(1, next.dec_calls);
}

CTEST(ondisk_ref, lookup_stages_ownership_before_publishing_blob)
{
   test_owner owner = {.failure = STATUS_NO_MEMORY};
   ondisk_ref source;
   owner_adopt(&source, &owner);
   char          descriptor[32] = "borrowed descriptor";
   char          result_buffer[32];
   lookup_result result;
   lookup_result_init(&result,
                      NULL,
                      SPLINTERDB_LOOKUP_VALUE,
                      sizeof(result_buffer),
                      result_buffer);
   // This path only copies a descriptor; no cache or blob I/O is involved.
   message msg =
      message_create_internal(MESSAGE_TYPE_INSERT,
                              (cache *)&owner,
                              slice_create(sizeof(descriptor), descriptor));
   ASSERT_TRUE(STATUS_IS_EQ(
      STATUS_NO_MEMORY, lookup_result_update(&result, NULL_KEY, msg, &source)));
   ASSERT_TRUE(merge_accumulator_is_null(&result.value));
   ASSERT_TRUE(ondisk_ref_is_null(&result.blob_ref));
   ASSERT_EQUAL(1, owner.refs);
   ASSERT_EQUAL(0, owner.dec_calls);

   owner.failure = STATUS_OK;
   ASSERT_TRUE(SUCCESS(lookup_result_update(&result, NULL_KEY, msg, &source)));
   ASSERT_EQUAL(2, owner.refs);
   ASSERT_EQUAL(2, owner.inc_calls); // One failed acquire, one successful.
   ASSERT_TRUE(result.blob_ref.arg == &owner);
   ASSERT_TRUE(message_is_blob(merge_accumulator_to_message(&result.value)));
   ASSERT_EQUAL(0,
                memcmp(descriptor,
                       merge_accumulator_data(&result.value),
                       sizeof(descriptor)));
   lookup_result_deinit(&result);
   ondisk_ref_deinit(&source);
   ASSERT_EQUAL(0, owner.refs);
   ASSERT_EQUAL(2, owner.dec_calls);
}

#if PLATFORM_MEMORY_FAULT_INJECTION
CTEST(ondisk_ref, failed_message_copy_releases_staged_owner)
{
   ASSERT_EQUAL(0, platform_register_thread());
   test_owner owner = {0};
   ondisk_ref source;
   owner_adopt(&source, &owner);
   char          descriptor[128] = "borrowed descriptor";
   lookup_result result;
   lookup_result_init(&result, NULL, SPLINTERDB_LOOKUP_VALUE, 0, NULL);
   message msg =
      message_create_internal(MESSAGE_TYPE_INSERT,
                              (cache *)&owner,
                              slice_create(sizeof(descriptor), descriptor));
   platform_memory_fault_config saved;
   platform_memory_fault_config_get(&saved);
   platform_memory_fault_config fault = saved;
   fault.mode                         = PLATFORM_MEMORY_FAULT_RANGE;
   fault.range_start                  = 1;
   fault.range_count                  = 1;
   fault.max_failures                 = 1;
   platform_memory_fault_config_set(&fault);
   platform_memory_fault_enable();
   platform_status rc = lookup_result_update(&result, NULL_KEY, msg, &source);
   platform_memory_fault_counters counters =
      platform_memory_fault_get_counters();
   platform_memory_fault_disable();
   platform_memory_fault_config_set(&saved);
   ASSERT_TRUE(STATUS_IS_EQ(STATUS_NO_MEMORY, rc));
   ASSERT_EQUAL(1, counters.failure_count);
   ASSERT_TRUE(merge_accumulator_is_null(&result.value));
   ASSERT_TRUE(ondisk_ref_is_null(&result.blob_ref));
   ASSERT_EQUAL(1, owner.inc_calls);
   ASSERT_EQUAL(1, owner.dec_calls);
   ASSERT_EQUAL(1, owner.refs);
   ASSERT_TRUE(SUCCESS(lookup_result_update(&result, NULL_KEY, msg, &source)));
   lookup_result_deinit(&result);
   ondisk_ref_deinit(&source);
   ASSERT_EQUAL(0, owner.refs);
   ASSERT_EQUAL(3, owner.dec_calls);
   platform_deregister_thread();
}
#endif
