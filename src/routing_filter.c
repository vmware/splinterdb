// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *----------------------------------------------------------------------
 * routing_filter.c --
 *
 *     This file contains the implementation for a routing filter
 *----------------------------------------------------------------------
 */
#include <unistd.h>
#include "platform.h"
#include "routing_filter.h"
#include "PackedArray.h"
#include "mini_allocator.h"
#include "iterator.h"
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <math.h>

#include "poison.h"

#define ROUTING_FPS_PER_PAGE 4096

/*
 *----------------------------------------------------------------------
 * routing_hdr: Disk-resident structure.
 *
 *       This header encodes the bucket counts for all buckets covered by a
 *       single index. Appears on pages of page type == PAGE_TYPE_FILTER.
 *----------------------------------------------------------------------
 */
typedef struct ONDISK routing_hdr {
   uint16 num_remainders;
   char   encoding[];
} routing_hdr;

/*
 *----------------------------------------------------------------------
 * RadixSort --
 *
 *      A fast integer sort based on https://stackoverflow.com/a/44792724
 *----------------------------------------------------------------------
 */

// A 4x256 matrix is used for RadixSort
#define MATRIX_ROWS sizeof(uint32)
#define MATRIX_COLS (UINT8_MAX + 1)

// XXX Change arguments to struct
static uint32 *
RadixSort(uint32 *pData,
          uint32  mBuf[static MATRIX_ROWS * MATRIX_COLS],
          uint32 *pTemp,
          uint32  count,
          uint32  fp_size,
          uint32  value_size)
{
   uint32 *mIndex[MATRIX_ROWS]; // index matrix
   uint32 *pDst, *pSrc, *pTmp;
   uint32  i, j, m, n;
   uint32  u;
   uint32  fpover = value_size % 8;
   if (fp_size == 0) {
      fp_size = 1;
   }
   uint32 rounds = (fp_size + fpover - 1) / 8 + 1;
   uint8  c;
   uint32 fpshift = value_size / 8;
   value_size     = value_size / 8 * 8;

   for (i = 0; i < MATRIX_ROWS; i++) {
      mIndex[i] = &mBuf[i * MATRIX_COLS];
      for (ptrdiff_t j = 0; j < MATRIX_COLS; j++) {
         platform_assert(mIndex[i][j] == 0);
      }
   }
   for (i = 0; i < count; i++) { // generate histograms
      u = pData[i] >> value_size;
      for (j = 0; j < rounds; j++) {
         c = ((uint8 *)&u)[j];
         mIndex[j][c]++;
         debug_assert(mIndex[j][c] <= count);
      }
   }

   for (j = 0; j < rounds; j++) { // convert to indices
      n = 0;
      for (i = 0; i < MATRIX_COLS; i++) {
         m            = mIndex[j][i];
         mIndex[j][i] = n;
         platform_assert(mIndex[j][i] <= count);
         n += m;
      }
   }

   pDst = pTemp; // radix sort
   pSrc = pData;
   for (j = 0; j < rounds; j++) {
      for (i = 0; i < count; i++) {
         u = pSrc[i];
         c = ((uint8 *)&u)[j + fpshift];
         platform_assert((mIndex[j][c] < count),
                         "OS-pid=%d, thread-ID=%lu, i=%u, j=%u, c=%d"
                         ", mIndex[j][c]=%d, count=%u\n",
                         getpid(),
                         platform_get_tid(),
                         i,
                         j,
                         c,
                         mIndex[j][c],
                         count);
         pDst[mIndex[j][c]++] = u;
      }
      pTmp = pSrc;
      pSrc = pDst;
      pDst = pTmp;
   }

   return (pSrc);
}


/*
 *----------------------------------------------------------------------
 *
 * Utility functions
 *
 *----------------------------------------------------------------------
 */

debug_only static inline void
routing_set_bit(uint64 *data, uint64 bitnum)
{
   *(data + bitnum / 64) |= (1ULL << (bitnum % 64));
}

static inline void
routing_unset_bit(uint64 *data, uint64 bitnum)
{
   *(data + bitnum / 64) &= ~((1ULL << (bitnum % 64)));
}

static inline uint32
routing_get_bucket(uint32 fp, size_t remainder_and_value_size)
{
   return fp >> remainder_and_value_size;
}

static inline uint32
routing_get_index(uint32 fp, size_t index_remainder_and_value_size)
{
   return index_remainder_and_value_size == 32
             ? 0
             : fp >> index_remainder_and_value_size;
}

static inline void
routing_filter_get_remainder_and_value(routing_config *cfg,
                                       uint32         *data,
                                       uint32          pos,
                                       uint32         *remainder_and_value,
                                       size_t          remainder_value_size)
{
   *remainder_and_value = PackedArray_get(data, pos, remainder_value_size);
}

static inline routing_hdr *
routing_get_header(cache          *cc,
                   routing_config *cfg,
                   uint64          filter_addr,
                   uint64          index,
                   page_handle   **filter_page)
{
   uint64 addrs_per_page =
      cache_config_page_size(cfg->cache_cfg) / sizeof(uint64);
   debug_assert(index / addrs_per_page < 32);
   uint64 index_addr =
      filter_addr
      + cache_config_page_size(cfg->cache_cfg) * (index / addrs_per_page);
   page_handle *index_page = cache_get(cc, index_addr, TRUE, PAGE_TYPE_FILTER);
   uint64 hdr_raw_addr = ((uint64 *)index_page->data)[index % addrs_per_page];
   platform_assert(hdr_raw_addr != 0);
   uint64 header_addr =
      hdr_raw_addr - (hdr_raw_addr % cache_config_page_size(cfg->cache_cfg));
   *filter_page            = cache_get(cc, header_addr, TRUE, PAGE_TYPE_FILTER);
   uint64       header_off = hdr_raw_addr - header_addr;
   routing_hdr *hdr        = (routing_hdr *)((*filter_page)->data + header_off);
   cache_unget(cc, index_page);
   return hdr;
}

static inline void
routing_unget_header(cache *cc, page_handle *header_page)
{
   cache_unget(cc, header_page);
}

static inline uint64
routing_header_length(routing_config *cfg, routing_hdr *hdr)
{
   uint64 metamessage_size =
      (hdr->num_remainders + cfg->index_size - 1) / 8 + 4;
   return metamessage_size + sizeof(routing_hdr);
}

static inline void
routing_unlock_and_unget_page(cache *cc, page_handle *page)
{
   cache_unlock(cc, page);
   cache_unclaim(cc, page);
   cache_unget(cc, page);
}

/*
 *----------------------------------------------------------------------
 * routing_get_bucket_bounds
 *
 *      parses the encoding to return the start and end indices for the
 *      bucket_offset
 *----------------------------------------------------------------------
 */
static inline void
routing_get_bucket_bounds(char   *encoding,
                          uint64  len,
                          uint64  bucket_offset,
                          uint64 *start,
                          uint64 *end)
{
   uint32 word          = 0;
   uint32 encoding_word = 0;
   uint64 bucket        = 0;
   uint64 bucket_pop    = 0;
   uint64 bit_offset    = 0;

   if (bucket_offset == 0) {
      *start        = 0;
      word          = 0;
      encoding_word = *((uint32 *)encoding + word);
      while (encoding_word == 0) {
         word++;
         encoding_word = *((uint32 *)encoding + word);
      }

      // ffs returns the index + 1 ALEX: I think that's what we want though.
      bit_offset = __builtin_ffs(encoding_word) - 1;
      *end       = 32 * word + bit_offset;
   } else {
      bucket_pop = __builtin_popcount(*((uint32 *)encoding));
      while (4 * word < len && bucket + bucket_pop < bucket_offset) {
         bucket += bucket_pop;
         word++;
         bucket_pop = __builtin_popcount(*((uint32 *)encoding + word));
      }

      encoding_word = *((uint32 *)encoding + word);
      while (bucket < bucket_offset - 1) {
         encoding_word &= encoding_word - 1;
         bucket++;
      }
      bit_offset = __builtin_ffs(encoding_word) - 1;
      *start     = 32 * word + bit_offset - bucket_offset + 1;

      encoding_word &= encoding_word - 1;
      while (encoding_word == 0) {
         word++;
         encoding_word = *((uint32 *)encoding + word);
      }
      bit_offset = __builtin_ffs(encoding_word) - 1; // ffs returns index + 1
      *end       = 32 * word + bit_offset - bucket_offset;
   }
}

void
routing_get_bucket_counts(routing_config *cfg, routing_hdr *hdr, uint32 *count)
{
   uint64  start = 0;
   uint64  end;
   uint64  i;
   uint64 *word_cursor = (uint64 *)hdr->encoding;
   uint64  word        = *(word_cursor++);

   memset(count, 0, cfg->index_size * sizeof(uint32));

   for (i = 0; i < cfg->index_size; i++) {
      while (word == 0) {
         count[i] += 64 - start;
         start = 0;
         word  = *(word_cursor++);
      }
      end = __builtin_ffsll(word) - 1;
      debug_assert(end - start < 1000);
      word &= word - 1;
      count[i] += end - start;
      start = end + 1;
   }
}

/*
 *----------------------------------------------------------------------
 *
 * unroll
 *
 *        Converts a routing filter into a fingerprint array
 *
 *----------------------------------------------------------------------
 */

/*
 *----------------------------------------------------------------------
 *
 * MAIN API
 *
 *----------------------------------------------------------------------
 */

/*
 *----------------------------------------------------------------------
 * routing_filter_add
 *
 *      Adds the fingerprints in fp_arr with value value to the
 *      routing filter at old_filter_addr and returns the result in
 *      filter_addr.
 *
 *      meta_head should be passed to routing_filter_zap
 *----------------------------------------------------------------------
 */
platform_status
routing_filter_add(cache           *cc,
                   routing_config  *cfg,
                   platform_heap_id hid,
                   routing_filter  *old_filter,
                   routing_filter  *filter,
                   uint32          *new_fp_arr,
                   uint64           num_new_fp,
                   uint16           value)
{
   ZERO_CONTENTS(filter);

   // old filter
   uint32 old_log_num_buckets          = 0;
   uint32 old_num_indices              = 1;
   uint32 old_remainder_size           = 0;
   size_t old_value_size               = 0;
   uint32 old_value_mask               = 0;
   size_t old_remainder_and_value_size = 0;
   if (old_filter->addr != 0) {
      mini_unkeyed_prefetch(cc, PAGE_TYPE_FILTER, old_filter->meta_head);
      old_log_num_buckets = 31 - __builtin_clz(old_filter->num_fingerprints);
      if (old_log_num_buckets < cfg->log_index_size) {
         old_log_num_buckets = cfg->log_index_size;
      }
      uint32 old_log_num_indices = old_log_num_buckets - cfg->log_index_size;
      old_num_indices            = 1UL << old_log_num_indices;
      old_remainder_size         = cfg->fingerprint_size - old_log_num_buckets;
      old_value_size             = old_filter->value_size;
      old_value_mask             = (1UL << old_value_size) - 1;
      old_remainder_and_value_size = old_value_size + old_remainder_size;
      platform_assert(cfg->fingerprint_size + old_value_size <= 32);
   }

   // compute parameters
   filter->num_fingerprints = num_new_fp + old_filter->num_fingerprints;
   filter->num_unique       = 0;

   uint32 log_num_buckets = 31 - __builtin_clz(filter->num_fingerprints);
   if (log_num_buckets < cfg->log_index_size) {
      log_num_buckets = cfg->log_index_size;
   }
   uint32 log_num_indices = log_num_buckets - cfg->log_index_size;
   uint32 num_indices     = 1UL << log_num_indices;
   debug_assert(num_indices > 0);
   uint32 remainder_size           = cfg->fingerprint_size - log_num_buckets;
   size_t value_size               = value == 0 ? 0 : 32 - __builtin_clz(value);
   filter->value_size              = value_size;
   size_t remainder_and_value_size = value_size + remainder_size;
   uint32 remainder_and_value_mask = (1UL << remainder_and_value_size) - 1;
   size_t index_remainder_and_value_size =
      remainder_and_value_size + cfg->log_index_size;
   uint32 new_indices_per_old_index = num_indices / old_num_indices;
   platform_assert(cfg->fingerprint_size + value_size <= 32);

   // for convenience
   uint64 page_size        = cache_config_page_size(cfg->cache_cfg);
   uint64 extent_size      = cache_config_extent_size(cfg->cache_cfg);
   uint64 pages_per_extent = cache_config_pages_per_extent(cfg->cache_cfg);
   uint64 index_size       = cfg->index_size;

   /*
    * temp and fingerprint_arr are used to radix sort the fps
    * index_count is # fps in each index
    */
   uint32 *index_count;
   uint32 *old_count;
   uint32 *matrix;
   uint32 *fp_buffer;
   uint32 *old_fp_buffer;
   uint64 *encoding_buffer;
   size_t  temp_buffer_count = num_new_fp +               // temp
                              num_indices +               // index_count
                              index_size +                // old_count
                              MATRIX_ROWS * MATRIX_COLS + // matrix
                              ROUTING_FPS_PER_PAGE +      // fp_buffer
                              ROUTING_FPS_PER_PAGE +      // old_fp_buffer
                              ROUTING_FPS_PER_PAGE / 32;  // encoding_buffer
   debug_assert(temp_buffer_count < 100000000);
   uint32 *temp = TYPED_ARRAY_ZALLOC_MF(
      PROCESS_PRIVATE_HEAP_ID, temp, temp_buffer_count, NULL);

   if (temp == NULL) {
      return STATUS_NO_MEMORY;
   }
   index_count     = temp + num_new_fp;
   old_count       = index_count + num_indices;
   matrix          = old_count + cfg->index_size;
   fp_buffer       = matrix + MATRIX_ROWS * MATRIX_COLS;
   old_fp_buffer   = fp_buffer + ROUTING_FPS_PER_PAGE;
   encoding_buffer = (uint64 *)(old_fp_buffer + ROUTING_FPS_PER_PAGE);
   memset(encoding_buffer, 0xff, ROUTING_FPS_PER_PAGE / 32 * sizeof(uint32));

   // we use a mini_allocator to obtain pages
   allocator      *al = cache_get_allocator(cc);
   uint64          meta_head;
   platform_status rc = allocator_alloc(al, &meta_head, PAGE_TYPE_FILTER);
   platform_assert_status_ok(rc);
   filter->meta_head = meta_head;
   // filters use an unkeyed mini allocator
   mini_allocator mini;
   mini_init(&mini, cc, NULL, filter->meta_head, 0, 1, PAGE_TYPE_FILTER, FALSE);

   // set up the index pages
   uint64       addrs_per_page = page_size / sizeof(uint64);
   page_handle *index_page[MAX_PAGES_PER_EXTENT];
   uint64       index_addr = mini_alloc(&mini, 0, NULL_KEY, NULL);
   platform_assert(index_addr % extent_size == 0);
   index_page[0] = cache_alloc(cc, index_addr, PAGE_TYPE_FILTER);
   for (uint64 i = 1; i < pages_per_extent; i++) {
      uint64 next_index_addr = mini_alloc(&mini, 0, NULL_KEY, NULL);
      platform_assert(next_index_addr == index_addr + i * page_size);
      index_page[i] = cache_alloc(cc, next_index_addr, PAGE_TYPE_FILTER);
   }
   filter->addr = index_addr;

   // we write to the filter with the filter cursor
   uint64       addr          = mini_alloc(&mini, 0, NULL_KEY, NULL);
   page_handle *filter_page   = cache_alloc(cc, addr, PAGE_TYPE_FILTER);
   char        *filter_cursor = filter_page->data;
   uint64       bytes_remaining_on_page = page_size;

   for (uint32 new_fp_no = 0; new_fp_no < num_new_fp; new_fp_no++) {
      new_fp_arr[new_fp_no] >>= 32 - cfg->fingerprint_size;
      new_fp_arr[new_fp_no] <<= value_size;
      new_fp_arr[new_fp_no] |= value;
   }

   uint32 *fp_arr = RadixSort(
      new_fp_arr, matrix, temp, num_new_fp, cfg->fingerprint_size, value_size);

   uint32 dst_fp_no         = 0;
   uint64 num_new_unique_fp = num_new_fp;
   for (uint32 src_fp_no = 0; src_fp_no != num_new_fp; src_fp_no++) {
      debug_assert(src_fp_no >= dst_fp_no);
      fp_arr[dst_fp_no] = fp_arr[src_fp_no];
      if (dst_fp_no == 0 || fp_arr[dst_fp_no] != fp_arr[dst_fp_no - 1]) {
         dst_fp_no++;
      } else {
         debug_assert(num_new_unique_fp != 0);
         num_new_unique_fp--;
      }
   }

   uint32 fp_no = 0;
   for (uint32 index_no = 0; index_no < num_indices; index_no++) {
      uint64 index_start = fp_no;
      for (; fp_no < num_new_unique_fp
             && routing_get_index(fp_arr[fp_no], index_remainder_and_value_size)
                   == index_no;
           fp_no++)
      {}
      index_count[index_no] = fp_no - index_start;
   }

   fp_no = 0;
   for (uint32 old_index_no = 0; old_index_no < old_num_indices; old_index_no++)
   {
      // process metadata from old filter
      char        *old_block_start;
      uint16       old_index_count = 0;
      page_handle *old_filter_node;
      if (old_filter->addr != 0) {
         routing_hdr *old_hdr = routing_get_header(
            cc, cfg, old_filter->addr, old_index_no, &old_filter_node);
         uint16 header_length = routing_header_length(cfg, old_hdr);
         old_block_start      = (char *)old_hdr + header_length;
         old_index_count      = old_hdr->num_remainders;
         routing_get_bucket_counts(cfg, old_hdr, old_count);
         // routing_filter_print_encoding(cfg, old_hdr);
      }

      uint32 *old_src_fp         = old_fp_buffer;
      uint32 *dst_fp             = fp_buffer;
      uint32  index_bucket_start = old_index_no * index_size;
      if (old_index_count != 0) {
         PackedArray_unpack((uint32 *)old_block_start,
                            0,
                            old_src_fp,
                            old_index_count,
                            old_remainder_and_value_size);
         uint32 old_fp_no = 0;
         for (uint32 bucket_off = 0; bucket_off < index_size; bucket_off++) {
            uint32 bucket = index_bucket_start + bucket_off;
            for (uint32 i = 0; i < old_count[bucket_off]; i++) {
               old_src_fp[old_fp_no++] |= bucket
                                          << old_remainder_and_value_size;
            }
         }
         debug_assert((old_fp_no == old_index_count),
                      "old_fp_no=%u, old_index_count=%u\n",
                      old_fp_no,
                      old_index_count);

         if (old_value_size != value_size) {
            for (old_fp_no = 0; old_fp_no < old_index_count; old_fp_no++) {
               uint32 old_value = old_src_fp[old_fp_no] & old_value_mask;
               old_src_fp[old_fp_no] -= old_value;
               old_src_fp[old_fp_no] <<= (value_size - old_value_size);
               old_src_fp[old_fp_no] |= old_value;
            }
         }
      }
      uint32 old_fps_added = 0;
      for (uint32 index_off = 0; index_off < new_indices_per_old_index;
           index_off++) {
         uint32 *new_src_fp = &fp_arr[fp_no];
         uint32 index_no = old_index_no * new_indices_per_old_index + index_off;
         uint32 last_bucket = index_no * index_size;
         uint32 fps_added = 0, new_fps_added = 0;
         uint32 end_bucket      = (index_no + 1) * index_size;
         uint32 new_index_count = index_count[index_no];
         uint64 header_bit      = 0;
         // platform_default_log("index 0x%x start 0x%x end 0x%x\n", index_no,
         // last_bucket, end_bucket);
         uint32 last_fp_added = UINT32_MAX;
         while (new_fps_added < new_index_count
                || old_fps_added < old_index_count) {
            uint32 fp;
            bool   is_old = ((new_fps_added == new_index_count)
                           || ((old_fps_added != old_index_count)
                               && (old_src_fp[old_fps_added]
                                   <= new_src_fp[new_fps_added])));
            if (is_old) {
               fp = old_src_fp[old_fps_added++];
            } else {
               fp = new_src_fp[new_fps_added++];
            }
            if (last_fp_added >> value_size != fp >> value_size) {
               filter->num_unique++;
            }
            uint32 bucket = routing_get_bucket(fp, remainder_and_value_size);
            // if (fp >> value_size == 0x4a11feb) {
            //    if (is_old) {
            //       platform_default_log("old %4u 0x%08x bucket 0x%x\n",
            //       old_fps_added, fp, bucket);
            //    } else {
            //       platform_default_log("new %4u 0x%08x bucket 0x%x\n",
            //       new_fps_added, fp, bucket);
            //    }
            // }
            if (bucket >= end_bucket) {
               debug_assert(is_old);
               debug_assert(old_fps_added != 0);
               old_fps_added--;
               break;
            }
            debug_assert(bucket >= last_bucket);
            header_bit += bucket - last_bucket;
            last_bucket = bucket;
            routing_unset_bit(encoding_buffer, header_bit++);
            last_fp_added       = fp;
            dst_fp[fps_added++] = fp & remainder_and_value_mask;
         }

         uint32 remainder_block_size =
            (fps_added * remainder_and_value_size - 1) / 8 + 4;
         uint64 encoding_size = (fps_added + index_size - 1) / 8 + 4;
         uint32 header_size   = encoding_size + sizeof(routing_hdr);
         if (header_size + remainder_block_size > bytes_remaining_on_page) {
            routing_unlock_and_unget_page(cc, filter_page);
            addr        = mini_alloc(&mini, 0, NULL_KEY, NULL);
            filter_page = cache_alloc(cc, addr, PAGE_TYPE_FILTER);

            bytes_remaining_on_page = page_size;
            filter_cursor           = filter_page->data;
         }

         // Set the index_no
         // ALEX: for now the indices must fit in a single extent
         platform_assert((index_no / addrs_per_page < pages_per_extent),
                         "index_no=%u, addrs_per_page=%lu"
                         ", (index_no / addrs_per_page)=%lu"
                         ", pages_per_extent=%lu",
                         index_no,
                         addrs_per_page,
                         (index_no / addrs_per_page),
                         pages_per_extent);

         uint64  index_page_no = index_no / addrs_per_page;
         uint64  index_offset  = index_no % addrs_per_page;
         uint64 *index_cursor  = (uint64 *)(index_page[index_page_no]->data);
         index_cursor += index_offset;
         uint64 filter_page_offset = filter_cursor - filter_page->data;
         *index_cursor             = addr + filter_page_offset;

         routing_hdr *hdr    = (routing_hdr *)filter_cursor;
         hdr->num_remainders = fps_added;
         memmove(hdr->encoding, encoding_buffer, encoding_size);
         memset(encoding_buffer, 0xff, encoding_size);
         filter_cursor += header_size;
         if (fps_added != 0) {
            PackedArray_pack((uint32 *)filter_cursor,
                             0,
                             fp_buffer,
                             fps_added,
                             remainder_and_value_size);
         }
         fp_no += index_count[index_no];
         filter_cursor += remainder_block_size;
         debug_assert(bytes_remaining_on_page
                      >= header_size + remainder_block_size);
         bytes_remaining_on_page -= header_size + remainder_block_size;
      }
      if (old_filter->addr != 0) {
         routing_unget_header(cc, old_filter_node);
      }
   }
   debug_assert(fp_no == num_new_unique_fp);
   routing_unlock_and_unget_page(cc, filter_page);

   for (uint64 i = 0; i < pages_per_extent; i++) {
      routing_unlock_and_unget_page(cc, index_page[i]);
   }

   mini_release(&mini, NULL_KEY);

   platform_free(PROCESS_PRIVATE_HEAP_ID, temp);

   return STATUS_OK;
}

void
routing_filter_prefetch(cache          *cc,
                        routing_config *cfg,
                        routing_filter *filter,
                        uint64          num_indices)
{
   uint64 last_extent_addr = 0;
   uint64 page_size        = cache_config_page_size(cfg->cache_cfg);
   uint64 addrs_per_page   = page_size / sizeof(uint64);
   uint64 num_index_pages  = (num_indices - 1) / addrs_per_page + 1;
   uint64 index_no         = 0;

   for (uint64 index_page_no = 0; index_page_no < num_index_pages;
        index_page_no++) {
      uint64       index_addr = filter->addr + (page_size * index_page_no);
      page_handle *index_page =
         cache_get(cc, index_addr, TRUE, PAGE_TYPE_FILTER);
      platform_assert(index_no < num_indices);

      uint64 max_index_no;
      if (index_page_no == num_index_pages - 1) {
         max_index_no = num_indices % addrs_per_page;
         if (max_index_no == 0) {
            max_index_no = addrs_per_page;
         }
      } else {
         max_index_no = addrs_per_page;
      }
      for (index_no = 0; index_no < max_index_no; index_no++) {
         uint64 hdr_raw_addr =
            ((uint64 *)index_page->data)[index_no % addrs_per_page];
         uint64 extent_addr =
            hdr_raw_addr
            - (hdr_raw_addr % cache_config_extent_size(cfg->cache_cfg));
         if (extent_addr != last_extent_addr) {
            cache_prefetch(cc, extent_addr, PAGE_TYPE_FILTER);
            last_extent_addr = extent_addr;
         }
      }
      cache_unget(cc, index_page);
   }
}

uint32
routing_filter_estimate_unique_fp(cache           *cc,
                                  routing_config  *cfg,
                                  platform_heap_id hid,
                                  routing_filter  *filter,
                                  uint64           num_filters)
{
   uint32 total_num_fp = 0;
   for (uint64 i = 0; i != num_filters; i++) {
      total_num_fp += filter[i].num_fingerprints;
   }
   uint32  buffer_size = total_num_fp / 12;
   uint32  alloc_size  = buffer_size + cfg->index_size;
   size_t  size        = (alloc_size * sizeof(uint32));
   fp_hdr  local;
   uint32 *fp_arr = fingerprint_init(&local, hid, size);
   if (!fp_arr) {
      platform_error_log("Initialization of fingerprint for %lu tuples"
                         " failed, likely due to insufficient memory.",
                         size);
      return 0;
   }

   uint32 *count = fingerprint_nth(&local, buffer_size);

   uint32 src_fp_no             = 0;
   uint32 dst_fp_no             = 0;
   uint32 fp_start[MAX_FILTERS] = {0};
   for (uint64 i = 0; i != num_filters; i++) {
      if (filter[i].addr == 0) {
         fp_start[i + 1] = dst_fp_no;
         continue;
      }
      uint32 log_num_buckets = 31 - __builtin_clz(filter[i].num_fingerprints);
      if (log_num_buckets < cfg->log_index_size) {
         log_num_buckets = cfg->log_index_size;
      }
      uint32 log_num_indices          = log_num_buckets - cfg->log_index_size;
      uint32 num_indices              = 1UL << log_num_indices;
      uint32 remainder_size           = cfg->fingerprint_size - log_num_buckets;
      uint32 value_size               = filter[i].value_size;
      uint32 remainder_and_value_size = value_size + remainder_size;
      platform_assert(cfg->fingerprint_size + value_size <= 32);
      uint32 index_size = cfg->index_size;

      if (num_indices >= 16) {
         // the filter is too small forget it
         platform_assert(num_indices % 16 == 0);
         num_indices /= 16;

         routing_filter_prefetch(cc, cfg, &filter[i], num_indices);

         for (uint32 index_no = 0; index_no < num_indices; index_no++) {
            // process metadata
            char        *block_start;
            uint16       index_count = 0;
            page_handle *filter_node;
            routing_hdr *hdr = routing_get_header(
               cc, cfg, filter[i].addr, index_no, &filter_node);
            uint16 header_length = routing_header_length(cfg, hdr);
            block_start          = (char *)hdr + header_length;
            index_count          = hdr->num_remainders;
            routing_get_bucket_counts(cfg, hdr, count);
            // routing_filter_print_encoding(cfg, hdr);

            uint32  index_bucket_start = index_no * index_size;
            uint32 *src_fp             = &fp_arr[src_fp_no];
            platform_assert((src_fp_no + index_count <= buffer_size),
                            "src_fp_no=%u, index_count=%u, buffer_size=%u\n",
                            src_fp_no,
                            index_count,
                            buffer_size);
            if (index_count != 0) {
               debug_only uint32 index_start = src_fp_no;
               PackedArray_unpack((uint32 *)block_start,
                                  0,
                                  src_fp,
                                  index_count,
                                  remainder_and_value_size);
               uint32 last_fp = UINT32_MAX;
               for (uint32 bucket_off = 0; bucket_off < index_size;
                    bucket_off++) {
                  uint32 bucket = index_bucket_start + bucket_off;
                  for (uint32 i = 0; i < count[bucket_off]; i++) {
                     fp_arr[src_fp_no] |= bucket << remainder_and_value_size;
                     fp_arr[src_fp_no] >>= value_size;
                     if (fp_arr[src_fp_no] == last_fp) {
                        src_fp_no++;
                     } else {
                        last_fp             = fp_arr[src_fp_no];
                        fp_arr[dst_fp_no++] = fp_arr[src_fp_no++];
                        platform_assert(dst_fp_no <= buffer_size);
                     }
                  }
               }
               debug_assert(src_fp_no - index_start == index_count);
            }
            routing_unget_header(cc, filter_node);
         }
      }
      fp_start[i + 1] = dst_fp_no;
   }

   // platform_default_log("num fp %u\n", fp_start[num_filters - 1]);

   uint32 idx[MAX_FILTERS] = {0};
   memmove(idx, fp_start, MAX_FILTERS * sizeof(uint32));
   uint32 num_unique = 0;
   for (uint64 i = 0; i < num_filters; i++) {
      debug_assert(fp_start[i] <= fp_start[i + 1]);
   }
   while (TRUE) {
      uint32 min_fp = UINT32_MAX;
      for (uint64 i = 0; i < num_filters; i++) {
         if (idx[i] != fp_start[i + 1] && fp_arr[idx[i]] < min_fp) {
            min_fp = fp_arr[idx[i]];
         }
      }

      if (min_fp == UINT32_MAX) {
         break;
      }
      // platform_default_log("0x%08x:", min_fp);

      for (uint64 i = 0; i < num_filters; i++) {
         if (idx[i] != fp_start[i + 1] && fp_arr[idx[i]] == min_fp) {
            // platform_default_log(" %lu-%u", i, idx[i]);
            idx[i]++;
         }
      }
      // platform_default_log("\n");
      num_unique++;
   }

   fingerprint_deinit(hid, &local);
   return num_unique * 16;
}

/*
 *----------------------------------------------------------------------
 * routing_filter_lookup
 *
 *      Looks for key in the filter and returns whether it was found, it's
 *      value goes in found_values.
 *
 *      IMPORTANT: If there are multiple matching values, this function returns
 *      them in the reverse order.
 *----------------------------------------------------------------------
 */
platform_status
routing_filter_lookup(cache          *cc,
                      routing_config *cfg,
                      routing_filter *filter,
                      key             target,
                      uint64         *found_values)
{
   debug_assert(key_is_user_key(target));

   if (filter->addr == 0) {
      *found_values = 0;
      return STATUS_OK;
   }

   hash_fn hash       = cfg->hash;
   uint64  seed       = cfg->seed;
   uint64  index_size = cfg->index_size;

   uint32 fp = hash(key_data(target), key_length(target), seed);
   fp >>= 32 - cfg->fingerprint_size;
   size_t value_size      = filter->value_size;
   uint32 log_num_buckets = 31 - __builtin_clz(filter->num_fingerprints);
   if (log_num_buckets < cfg->log_index_size) {
      log_num_buckets = cfg->log_index_size;
   }
   uint32 remainder_size           = cfg->fingerprint_size - log_num_buckets;
   size_t remainder_and_value_size = remainder_size + value_size;
   uint32 bucket =
      routing_get_bucket(fp << value_size, remainder_and_value_size);
   uint32 bucket_off = bucket % index_size;
   size_t index_remainder_and_value_size =
      remainder_size + value_size + cfg->log_index_size;
   uint32 remainder_mask = (1UL << remainder_size) - 1;
   uint32 index =
      routing_get_index(fp << value_size, index_remainder_and_value_size);
   uint32 remainder = fp & remainder_mask;

   page_handle *filter_node;
   routing_hdr *hdr =
      routing_get_header(cc, cfg, filter->addr, index, &filter_node);
   uint64 encoding_size = (hdr->num_remainders + index_size - 1) / 8 + 4;
   uint64 header_length = encoding_size + sizeof(routing_hdr);

   uint64 start, end;
   routing_get_bucket_bounds(
      hdr->encoding, header_length, bucket_off, &start, &end);
   char *remainder_block_start = (char *)hdr + header_length;

   // platform_default_log("routing_filter_lookup: "
   //      "index 0x%lx bucket 0x%lx (0x%lx) remainder 0x%x start %lu end
   //      %lu\n", index, bucket, bucket % index_size, remainder, start, end);

   if (start == end) {
      routing_unget_header(cc, filter_node);
      *found_values = 0;
      return STATUS_OK;
   }

   uint64 found_values_int = 0;
   for (uint32 i = 0; i < end - start; i++) {
      uint32 pos = end - i - 1;
      uint32 found_remainder_and_value;
      routing_filter_get_remainder_and_value(cfg,
                                             (uint32 *)remainder_block_start,
                                             pos,
                                             &found_remainder_and_value,
                                             remainder_and_value_size);
      uint32 found_remainder = found_remainder_and_value >> value_size;
      if (found_remainder == remainder) {
         uint32 value_mask  = (1UL << value_size) - 1;
         uint16 found_value = found_remainder_and_value & value_mask;
         platform_assert(found_value < 64);
         found_values_int |= (1UL << found_value);
      }
   }

   routing_unget_header(cc, filter_node);
   *found_values = found_values_int;
   return STATUS_OK;
}

/*
 *-----------------------------------------------------------------------------
 * routing_async_set_state --
 *
 *      Set the state of the async filter lookup state machine.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static inline void
routing_async_set_state(routing_async_ctxt *ctxt, routing_async_state new_state)
{
   ctxt->prev_state = ctxt->state;
   ctxt->state      = new_state;
}


/*
 *-----------------------------------------------------------------------------
 * routing_filter_async_callback --
 *
 *      Callback that's called when the async cache get loads a page into
 *      the cache. This function moves the async filter lookup state machine's
 *      state ahead, and calls the upper layer callback that'll re-enqueue
 *      the filter lookup for dispatch.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
static void
routing_filter_async_callback(cache_async_ctxt *cache_ctxt)
{
   routing_async_ctxt *ctxt = cache_ctxt->cbdata;

   platform_assert(SUCCESS(cache_ctxt->status));
   platform_assert(cache_ctxt->page);
   //   platform_default_log("%s:%d tid %2lu: ctxt %p is callback with page
   //   %p\n",
   //                __FILE__, __LINE__, platform_get_tid(), ctxt,
   //                cache_ctxt->page);
   ctxt->was_async = TRUE;
   // Move state machine ahead and requeue for dispatch
   if (ctxt->state == routing_async_state_get_index) {
      routing_async_set_state(ctxt, routing_async_state_got_index);
   } else {
      debug_assert(ctxt->state == routing_async_state_get_filter);
      routing_async_set_state(ctxt, routing_async_state_got_filter);
   }
   ctxt->cb(ctxt);
}


/*
 *-----------------------------------------------------------------------------
 * routing_filter_lookup_async --
 *
 *      Async filter lookup api. Returns if lookup found a key in *found_values.
 *      The ctxt should've been initialized using routing_filter_ctxt_init().
 *      The return value can be either of:
 *      async_locked: A page needed by lookup is locked. User should retry
 *                    request.
 *      async_no_reqs: A page needed by lookup is not in cache and the IO
 *                     subsystem is out of requests. User should throttle.
 *      async_io_started: Async IO was started to read a page needed by the
 *                        lookup into the cache. When the read is done, caller
 *                        will be notified using ctxt->cb, that won't run on
 *                        the thread context. It can be used to requeue the
 *                        async lookup request for dispatch in thread context.
 *                        When it's requeued, it must use the same function
 *                        params except found.
 *      success: Results are in *found_values
 *
 * Results:
 *      Async result.
 *
 * Side effects:
 *      None.
 *-----------------------------------------------------------------------------
 */
cache_async_result
routing_filter_lookup_async(cache              *cc,
                            routing_config     *cfg,
                            routing_filter     *filter,
                            key                 target,
                            uint64             *found_values,
                            routing_async_ctxt *ctxt)
{
   cache_async_result res  = 0;
   bool               done = FALSE;

   debug_assert(key_is_user_key(target));

   do {
      switch (ctxt->state) {
         case routing_async_state_start:
         {
            // Calculate filter parameters for the key
            hash_fn hash = cfg->hash;
            uint64  seed = cfg->seed;

            uint32 fp = hash(key_data(target), key_length(target), seed);
            fp >>= 32 - cfg->fingerprint_size;
            size_t value_size = filter->value_size;
            uint32 log_num_buckets =
               31 - __builtin_clz(filter->num_fingerprints);
            if (log_num_buckets < cfg->log_index_size) {
               log_num_buckets = cfg->log_index_size;
            }
            ctxt->remainder_size = cfg->fingerprint_size - log_num_buckets;
            size_t remainder_and_value_size = ctxt->remainder_size + value_size;
            ctxt->bucket =
               routing_get_bucket(fp << value_size, remainder_and_value_size);
            size_t index_remainder_and_value_size =
               ctxt->remainder_size + value_size + cfg->log_index_size;
            uint32 remainder_mask = (1UL << ctxt->remainder_size) - 1;
            ctxt->index           = routing_get_index(fp << value_size,
                                            index_remainder_and_value_size);
            ctxt->remainder       = fp & remainder_mask;

            uint64 addrs_per_page =
               cache_config_page_size(cfg->cache_cfg) / sizeof(uint64);
            ctxt->page_addr = filter->addr
                              + cache_config_page_size(cfg->cache_cfg)
                                   * (ctxt->index / addrs_per_page);
            routing_async_set_state(ctxt, routing_async_state_get_index);
            // fallthrough;
         }
         case routing_async_state_get_index:
         case routing_async_state_get_filter:
         {
            // Get the index or filter page.
            cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

            cache_ctxt_init(
               cc, routing_filter_async_callback, ctxt, cache_ctxt);
            res = cache_get_async(
               cc, ctxt->page_addr, PAGE_TYPE_FILTER, cache_ctxt);
            switch (res) {
               case async_locked:
               case async_no_reqs:
                  //            platform_default_log("%s:%d tid %2lu: ctxt %p is
                  //            retry\n",
                  //                         __FILE__, __LINE__,
                  //                         platform_get_tid(), ctxt);
                  /*
                   * Ctxt remains at same state. The invocation is done, but
                   * the request isn't; and caller will re-invoke me.
                   */
                  done = TRUE;
                  break;
               case async_io_started:
                  //            platform_default_log("%s:%d tid %2lu: ctxt %p is
                  //            io_started\n",
                  //                         __FILE__, __LINE__,
                  //                         platform_get_tid(), ctxt);
                  // Invocation is done; request isn't. Callback will move
                  // state.
                  done = TRUE;
                  break;
               case async_success:
                  ctxt->was_async = FALSE;
                  if (ctxt->state == routing_async_state_get_index) {
                     routing_async_set_state(ctxt,
                                             routing_async_state_got_index);
                  } else {
                     debug_assert(ctxt->state
                                  == routing_async_state_get_filter);
                     routing_async_set_state(ctxt,
                                             routing_async_state_got_filter);
                  }
                  break;
               default:
                  platform_assert(0);
            }
            break;
         }
         case routing_async_state_got_index:
         {
            // Got the index; find address of filter page
            cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

            if (ctxt->was_async) {
               cache_async_done(cc, PAGE_TYPE_FILTER, cache_ctxt);
            }
            uint64 *index_arr = ((uint64 *)cache_ctxt->page->data);
            uint64  addrs_per_page =
               cache_config_page_size(cfg->cache_cfg) / sizeof(uint64);
            ctxt->header_addr = index_arr[ctxt->index % addrs_per_page];
            ctxt->page_addr =
               ctxt->header_addr
               - (ctxt->header_addr % cache_config_page_size(cfg->cache_cfg));
            cache_unget(cc, cache_ctxt->page);
            routing_async_set_state(ctxt, routing_async_state_get_filter);
            break;
         }
         case routing_async_state_got_filter:
         {
            // Got the filter; find bucket and search for remainder
            cache_async_ctxt *cache_ctxt = ctxt->cache_ctxt;

            if (ctxt->was_async) {
               cache_async_done(cc, PAGE_TYPE_FILTER, cache_ctxt);
            }
            routing_hdr *hdr =
               (routing_hdr *)(cache_ctxt->page->data
                               + (ctxt->header_addr
                                  % cache_config_page_size(cfg->cache_cfg)));
            uint64 encoding_size =
               (hdr->num_remainders + cfg->index_size - 1) / 8 + 4;
            uint64 header_length = encoding_size + sizeof(routing_hdr);
            uint64 start, end;
            uint32 bucket_off = ctxt->bucket % cfg->index_size;
            routing_get_bucket_bounds(
               hdr->encoding, header_length, bucket_off, &start, &end);
            char *remainder_block_start = (char *)hdr + header_length;

            uint64 found_values_int = 0;
            for (uint32 i = 0; i < end - start; i++) {
               uint32 pos = end - i - 1;
               uint32 found_remainder_and_value;
               size_t value_size = filter->value_size;
               size_t remainder_and_value_size =
                  ctxt->remainder_size + value_size;
               routing_filter_get_remainder_and_value(
                  cfg,
                  (uint32 *)remainder_block_start,
                  pos,
                  &found_remainder_and_value,
                  remainder_and_value_size);
               uint32 found_remainder = found_remainder_and_value >> value_size;
               if (found_remainder == ctxt->remainder) {
                  uint32 value_mask  = (1UL << value_size) - 1;
                  uint16 found_value = found_remainder_and_value & value_mask;
                  platform_assert(found_value < 64);
                  found_values_int |= (1UL << found_value);
               }
            }
            *found_values = found_values_int;
            cache_unget(cc, cache_ctxt->page);
            res  = async_success;
            done = TRUE;
            break;
         }
         default:
            platform_assert(0);
      }
   } while (!done);

   return res;
}

/*
 *----------------------------------------------------------------------
 * routing_filter_zap
 *
 *      decs the ref count of the filter and destroys it if it reaches 0
 *----------------------------------------------------------------------
 */
void
routing_filter_zap(cache *cc, routing_filter *filter)
{
   if (filter->num_fingerprints == 0) {
      return;
   }

   uint64 meta_head = filter->meta_head;
   mini_unkeyed_dec_ref(cc, meta_head, PAGE_TYPE_FILTER, FALSE);
}

/*
 *----------------------------------------------------------------------
 * routing_filter_estimate_unique_keys
 *
 *      returns the expected number of unique input keys given the number of
 *      unique fingerprints in the filter.
 *----------------------------------------------------------------------
 */
uint32
routing_filter_estimate_unique_keys_from_count(routing_config *cfg,
                                               uint64          num_unique)
{
   double universe_size = 1UL << cfg->fingerprint_size;
   double unseen_fp     = universe_size - num_unique;
   /*
    * Compute the difference H_|U| - H_{|U| - #unique_fp}, where U is the fp
    * universe.
    */
   double harmonic_diff =
      log(universe_size) - log(unseen_fp)
      + 1 / 2.0 * (1 / universe_size - 1 / unseen_fp)
      - 1 / 12.0 * (1 / pow(universe_size, 2) - 1 / pow(unseen_fp, 2))
      + 1 / 120.0 * (1 / pow(universe_size, 4) - 1 / pow(unseen_fp, 4));
   uint32 estimated_input_keys = universe_size * harmonic_diff;
   return estimated_input_keys;
}

uint32
routing_filter_estimate_unique_keys(routing_filter *filter, routing_config *cfg)
{
   // platform_default_log("unique fp %u\n", filter->num_unique);
   return routing_filter_estimate_unique_keys_from_count(cfg,
                                                         filter->num_unique);
}

/*
 *----------------------------------------------------------------------
 *
 * Debug functions
 *
 *----------------------------------------------------------------------
 */

void
routing_filter_verify(cache          *cc,
                      routing_config *cfg,
                      routing_filter *filter,
                      uint16          value,
                      iterator       *itor)
{
   bool at_end;
   iterator_at_end(itor, &at_end);
   while (!at_end) {
      key     curr_key;
      message msg;
      iterator_get_curr(itor, &curr_key, &msg);
      debug_assert(key_is_user_key(curr_key));
      uint64          found_values;
      platform_status rc =
         routing_filter_lookup(cc, cfg, filter, curr_key, &found_values);
      platform_assert_status_ok(rc);
      platform_assert(routing_filter_is_value_found(found_values, value));
      iterator_advance(itor);
      iterator_at_end(itor, &at_end);
   }
}

void
routing_filter_print_encoding(routing_config *cfg, routing_hdr *hdr)
{
   uint32 i;
   platform_default_log("--- Encoding: %u\n", hdr->num_remainders);
   for (i = 0; i < hdr->num_remainders + cfg->index_size; i++) {
      if (i != 0 && i % 16 == 0)
         platform_default_log(" | ");
      if (hdr->encoding[i / 8] & (1 << i % 8))
         platform_default_log("1");
      else
         platform_default_log("0");
   }
   platform_default_log("\n");
}

void
routing_filter_print_index(cache          *cc,
                           routing_config *cfg,
                           uint64          filter_addr,
                           uint32          num_indices)
{
   uint64 i;

   platform_default_log("******************************************************"
                        "**************************\n");
   platform_default_log("***   filter INDEX\n");
   platform_default_log("***   filter_addr: %lu\n", filter_addr);
   platform_default_log("------------------------------------------------------"
                        "--------------------------\n");
   for (i = 0; i < num_indices; i++) {
      uint64 addrs_per_page =
         cache_config_page_size(cfg->cache_cfg) / sizeof(uint64);
      uint64 index_addr =
         filter_addr
         + cache_config_page_size(cfg->cache_cfg) * (i / addrs_per_page);
      page_handle *index_page =
         cache_get(cc, index_addr, TRUE, PAGE_TYPE_FILTER);
      platform_default_log("index 0x%lx: %lu\n",
                           i,
                           ((uint64 *)index_page->data)[i % addrs_per_page]);
      cache_unget(cc, index_page);
   }
}

void
routing_filter_print_remainders(routing_config *cfg,
                                routing_hdr    *hdr,
                                size_t          remainder_size,
                                size_t          value_size)
{
   uint64 i, j, start, end;
   uint64 encoding_size = (hdr->num_remainders + cfg->index_size - 1) / 8 + 1;
   uint64 header_length = encoding_size + sizeof(routing_hdr);
   platform_default_log("--- Remainders\n");
   size_t remainder_and_value_size = value_size + remainder_size;
   for (i = 0; i < cfg->index_size; i++) {
      routing_get_bucket_bounds(hdr->encoding, header_length, i, &start, &end);
      platform_default_log("0x%lx remainders:", i);
      for (j = start; j < end; j++) {
         uint32 remainder, value, remainder_and_value;
         routing_filter_get_remainder_and_value(
            cfg,
            (uint32 *)((char *)hdr + header_length),
            j,
            &remainder_and_value,
            remainder_and_value_size);
         remainder         = remainder_and_value >> value_size;
         uint32 value_mask = (1UL << value_size) - 1;
         value             = remainder_and_value & value_mask;
         platform_default_log(" 0x%x:%u", remainder, value);
      }
      platform_default_log("\n");
   }
}

void
routing_filter_print(cache *cc, routing_config *cfg, routing_filter *filter)
{
   uint64 filter_addr     = filter->addr;
   uint32 log_num_buckets = 31 - __builtin_clz(filter->num_fingerprints);
   if (log_num_buckets < cfg->log_index_size) {
      log_num_buckets = cfg->log_index_size;
   }
   uint32 log_num_indices = log_num_buckets - cfg->log_index_size;
   uint32 num_indices     = 1UL << log_num_indices;
   debug_assert(num_indices > 0);
   uint32 remainder_size = cfg->fingerprint_size - log_num_buckets;

   routing_filter_print_index(cc, cfg, filter_addr, num_indices);
   uint64 i;
   size_t value_size = filter->value_size;
   for (i = 0; i < num_indices; i++) {
      platform_default_log("----------------------------------------\n");
      platform_default_log("--- Index 0x%lx\n", i);
      page_handle *filter_page;
      routing_hdr *hdr =
         routing_get_header(cc, cfg, filter_addr, i, &filter_page);
      routing_filter_print_encoding(cfg, hdr);
      routing_filter_print_remainders(cfg, hdr, remainder_size, value_size);
      routing_unget_header(cc, filter_page);
   }
}
