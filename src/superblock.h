// Copyright 2018-2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * super_block.h --
 *
 *     This file contains a basic superblock implementation.
 */

#ifndef __SUPERBLOCK_H
#define __SUPERBLOCK_H

/*
 *-----------------------------------------------------------------------------
 * Splinter Super Block: Disk-resident structure.
 * Super block lives on page of page type == PAGE_TYPE_SUPERBLOCK.
 *-----------------------------------------------------------------------------
 */
typedef struct ONDISK trunk_super_block {
   uint64 root_addr; // Address of the root of the trunk for the instance
                     // referenced by this superblock.
   uint64      meta_tail;
   uint64      log_addr;
   uint64      log_meta_addr;
   uint64      timestamp;
   bool        checkpointed;
   bool        unmounted;
   checksum128 checksum;
} trunk_super_block;

void
trunk_set_super_block(trunk_handle *spl,
                      bool          is_checkpoint,
                      bool          is_unmount,
                      bool          is_create)
{
   uint64             super_addr;
   page_handle       *super_page;
   trunk_super_block *super;
   uint64             wait = 1;
   platform_status    rc;

   if (is_create) {
      rc = allocator_alloc_super_addr(spl->al, spl->id, &super_addr);
   } else {
      rc = allocator_get_super_addr(spl->al, spl->id, &super_addr);
   }
   platform_assert_status_ok(rc);
   super_page = cache_get(spl->cc, super_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   while (!cache_claim(spl->cc, super_page)) {
      platform_sleep(wait);
      wait *= 2;
   }
   wait = 1;
   cache_lock(spl->cc, super_page);

   super            = (trunk_super_block *)super_page->data;
   super->root_addr = spl->root_addr;
   super->meta_tail = mini_meta_tail(&spl->mini);
   if (spl->cfg.use_log) {
      super->log_addr      = log_addr(spl->log);
      super->log_meta_addr = log_meta_addr(spl->log);
   }
   super->timestamp    = platform_get_real_time();
   super->checkpointed = is_checkpoint;
   super->unmounted    = is_unmount;
   super->checksum =
      platform_checksum128(super,
                           sizeof(trunk_super_block) - sizeof(checksum128),
                           TRUNK_SUPER_CSUM_SEED);

   cache_mark_dirty(spl->cc, super_page);
   cache_unlock(spl->cc, super_page);
   cache_unclaim(spl->cc, super_page);
   cache_unget(spl->cc, super_page);
   cache_page_sync(spl->cc, super_page, TRUE, PAGE_TYPE_SUPERBLOCK);
}

trunk_super_block *
trunk_get_super_block_if_valid(trunk_handle *spl, page_handle **super_page)
{
   uint64             super_addr;
   trunk_super_block *super;

   platform_status rc = allocator_get_super_addr(spl->al, spl->id, &super_addr);
   platform_assert_status_ok(rc);
   *super_page = cache_get(spl->cc, super_addr, TRUE, PAGE_TYPE_SUPERBLOCK);
   super       = (trunk_super_block *)(*super_page)->data;

   if (!platform_checksum_is_equal(
          super->checksum,
          platform_checksum128(super,
                               sizeof(trunk_super_block) - sizeof(checksum128),
                               TRUNK_SUPER_CSUM_SEED)))
   {
      cache_unget(spl->cc, *super_page);
      *super_page = NULL;
      return NULL;
   }

   return super;
}

void
trunk_release_super_block(trunk_handle *spl, page_handle *super_page)
{
   cache_unget(spl->cc, super_page);
}

#endif // __SUPERBLOCK_H
