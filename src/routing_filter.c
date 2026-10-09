// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *----------------------------------------------------------------------
 * routing_filter.c --
 *
 *     This file contains the implementation for a routing filter
 *----------------------------------------------------------------------
 */
#include "routing_filter.h"
#include "PackedArray.h"
#include "mini_allocator.h"
#include "iterator.h"
#include "platform_hash.h"
#include "platform_typed_alloc.h"
#include "platform_assert.h"
#include "platform_threads.h"

// This is used only for log(), which we should figure out how to avoid using,
// so I'm not going to bother to platformize it.
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
struct ONDISK routing_hdr {
   uint16 num_remainders;
   char   encoding[];
};

typedef struct routing_filter_params {
   uint32 log_num_buckets;
   uint32 log_num_indices;
   uint32 num_indices;
   uint32 remainder_size;
   uint32 value_size;
   uint32 remainder_and_value_size;
   uint32 index_remainder_and_value_size;
} routing_filter_params;

typedef struct routing_header_view {
   routing_hdr *hdr;
   char        *encoding;
   char        *remainders;
   uint64       encoding_size;
   uint64       remainder_bytes;
   uint64       header_length;
} routing_header_view;

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
          uint32  fp_size)
{
   uint32 *mIndex[MATRIX_ROWS]; // index matrix
   uint32 *pDst, *pSrc, *pTmp;
   uint32  i, j, m, n;
   uint32  u;
   if (fp_size == 0) {
      fp_size = 1;
   }
   uint32 rounds = (fp_size + 7) / 8;
   uint8  c;


   platform_assert(rounds <= MATRIX_ROWS);

   for (i = 0; i < MATRIX_ROWS; i++) {
      mIndex[i] = &mBuf[i * MATRIX_COLS];
      for (ptrdiff_t j = 0; j < MATRIX_COLS; j++) {
         platform_assert(mIndex[i][j] == 0);
      }
   }
   for (i = 0; i < count; i++) { // generate histograms
      u = pData[i];
      platform_assert(u < (1ULL << (8 * rounds)),
                      "pData[i]=0x%x u=0x%x, fp_size=%u rounds=%u\n",
                      pData[i],
                      u,
                      fp_size,
                      rounds);
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
         c = ((uint8 *)&u)[j];
         platform_assert((mIndex[j][c] < count),
                         "OS-pid=%d, thread-ID=%lu, i=%u, j=%u, c=%d"
                         ", mIndex[j][c]=%d, count=%u pData=%p pTemp=%p\n",
                         platform_get_os_pid(),
                         platform_get_tid(),
                         i,
                         j,
                         c,
                         mIndex[j][c],
                         count,
                         pData,
                         pTemp);
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

static inline void
routing_unset_bit(uint8 *data, uint64 bitnum)
{
   data[bitnum / 8] &= ~(1U << (bitnum % 8));
}

static inline uint32
routing_get_bucket(uint32 fp, size_t remainder_and_value_size)
{
   return remainder_and_value_size >= 32 ? 0 : fp >> remainder_and_value_size;
}

static inline uint32
routing_get_index(uint32 fp, size_t index_remainder_and_value_size)
{
   return index_remainder_and_value_size >= 32
             ? 0
             : fp >> index_remainder_and_value_size;
}

static inline void
routing_filter_get_remainder_and_value(const routing_config *cfg,
                                       uint32               *data,
                                       uint32                pos,
                                       uint32 *remainder_and_value,
                                       size_t  remainder_value_size)
{
   *remainder_and_value = PackedArray_get(data, pos, remainder_value_size);
}

static platform_status
routing_filter_get_params(const routing_config  *cfg,
                          const routing_filter  *filter,
                          routing_filter_params *params)
{
   if (cfg == NULL || cfg->cache_cfg == NULL || filter == NULL || params == NULL
       || cfg->fingerprint_size == 0 || cfg->fingerprint_size > 32
       || cfg->log_index_size >= 32
       || cfg->log_index_size > cfg->fingerprint_size
       || cfg->index_size != (1U << cfg->log_index_size))
   {
      return STATUS_INVALID_STATE;
   }

   ZERO_CONTENTS(params);
   if (!routing_filter_descriptor_is_valid(filter)) {
      return STATUS_INVALID_STATE;
   }
   if (filter->addr == 0) {
      return STATUS_OK;
   }

   params->log_num_buckets = 31 - __builtin_clz(filter->num_fingerprints);
   if (params->log_num_buckets < cfg->log_index_size) {
      params->log_num_buckets = cfg->log_index_size;
   }
   if (params->log_num_buckets > filter->fingerprint_size) {
      return STATUS_INVALID_STATE;
   }

   params->log_num_indices = params->log_num_buckets - cfg->log_index_size;
   params->num_indices     = 1U << params->log_num_indices;
   params->remainder_size  = filter->fingerprint_size - params->log_num_buckets;
   params->value_size      = filter->value_size;
   params->remainder_and_value_size =
      params->remainder_size + params->value_size;
   params->index_remainder_and_value_size =
      params->remainder_and_value_size + cfg->log_index_size;

   uint64 extent_size = cfg->extent_size;
   if (params->remainder_and_value_size == 0
       || params->remainder_and_value_size > 32
       || params->index_remainder_and_value_size > 32
       || params->num_indices > extent_size / sizeof(uint64))
   {
      return STATUS_INVALID_STATE;
   }
   return STATUS_OK;
}

static platform_status
routing_get_header(cache                *cc,
                   const routing_config *cfg,
                   uint64                filter_addr,
                   uint64                index,
                   page_handle         **filter_page,
                   uint64               *hdr_raw_addr)
{
   uint64 page_size      = cfg->page_size;
   uint64 addrs_per_page = page_size / sizeof(uint64);
   if (addrs_per_page == 0) {
      return STATUS_INVALID_STATE;
   }
   uint64 index_page_no = index / addrs_per_page;
   if (index_page_no > (UINT64_MAX - filter_addr) / page_size) {
      return STATUS_INVALID_STATE;
   }
   uint64       index_addr = filter_addr + page_size * index_page_no;
   page_handle *index_page = cache_get(cc, index_addr, TRUE, PAGE_TYPE_FILTER);
   if (index_page == NULL) {
      return STATUS_IO_ERROR;
   }
   *hdr_raw_addr = ((uint64 *)index_page->data)[index % addrs_per_page];
   cache_unget(cc, index_page);
   if (*hdr_raw_addr == 0 || *hdr_raw_addr % sizeof(uint32) != 0) {
      return STATUS_INVALID_STATE;
   }

   uint64 header_addr = *hdr_raw_addr - (*hdr_raw_addr % page_size);
   *filter_page       = cache_get(cc, header_addr, TRUE, PAGE_TYPE_FILTER);
   return *filter_page == NULL ? STATUS_IO_ERROR : STATUS_OK;
}

static inline void
routing_unget_header(cache *cc, page_handle *header_page)
{
   cache_unget(cc, header_page);
}

static inline uint64
routing_header_size(const routing_config *cfg, uint64 num_remainders)
{
   uint64 encoding_size = (num_remainders + cfg->index_size - 1) / 8 + 4;
   return ROUNDUP(sizeof(routing_hdr) + encoding_size, sizeof(uint32));
}

static inline uint64
routing_remainder_block_size(uint64 num_remainders, uint32 bits_per_item)
{
   uint64 bytes =
      num_remainders == 0 ? 3 : (num_remainders * bits_per_item - 1) / 8 + 4;
   return ROUNDUP(bytes, sizeof(uint32));
}

static platform_status
routing_header_view_init(const routing_config *cfg,
                         page_handle          *filter_page,
                         uint64                hdr_raw_addr,
                         uint32                bits_per_item,
                         routing_header_view  *view)
{
   uint64 page_size = cfg->page_size;
   if (filter_page == NULL || hdr_raw_addr < filter_page->disk_addr) {
      return STATUS_INVALID_STATE;
   }
   uint64 header_off = hdr_raw_addr - filter_page->disk_addr;
   if (header_off > page_size || page_size - header_off < sizeof(routing_hdr)
       || hdr_raw_addr % sizeof(uint32) != 0 || bits_per_item == 0
       || bits_per_item > 32)
   {
      return STATUS_INVALID_STATE;
   }

   routing_hdr *hdr = (routing_hdr *)(filter_page->data + header_off);
   uint64       num_remainders = hdr->num_remainders;
   if (num_remainders > ROUTING_FPS_PER_PAGE) {
      return STATUS_INVALID_STATE;
   }

   uint64 encoding_size = (num_remainders + cfg->index_size - 1) / 8 + 4;
   uint64 header_length = routing_header_size(cfg, num_remainders);
   uint64 remainder_bytes =
      routing_remainder_block_size(num_remainders, bits_per_item);
   uint64 bytes_available = page_size - header_off;
   if (header_length > bytes_available
       || remainder_bytes > bytes_available - header_length)
   {
      return STATUS_INVALID_STATE;
   }

   view->hdr             = hdr;
   view->encoding        = hdr->encoding;
   view->encoding_size   = encoding_size;
   view->header_length   = header_length;
   view->remainders      = (char *)hdr + view->header_length;
   view->remainder_bytes = remainder_bytes;
   return STATUS_OK;
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
static platform_status
routing_get_bucket_bounds(const routing_config      *cfg,
                          const routing_header_view *view,
                          uint64                     bucket_offset,
                          uint64                    *start,
                          uint64                    *end)
{
   if (bucket_offset >= cfg->index_size) {
      return STATUS_INVALID_STATE;
   }

   /*
    * The bucket bounds are determined by adjacent set bits in the encoding.
    * Locate both in one traversal.  Use memcpy for each word because the
    * packed routing_hdr leaves encoding only two-byte aligned.
    */
   uint64 target_separator = bucket_offset == 0 ? 0 : bucket_offset - 1;
   uint64 seen             = 0;
   for (uint64 byte_no = 0; byte_no < view->encoding_size; byte_no += 8) {
      uint64 word  = 0;
      uint64 bytes = view->encoding_size - byte_no;
      if (bytes > sizeof(word)) {
         bytes = sizeof(word);
      }
      memcpy(&word, view->encoding + byte_no, bytes);

      uint32 pop = __builtin_popcountll(word);
      if (target_separator >= seen + pop) {
         seen += pop;
         continue;
      }

      uint64 ordinal = target_separator - seen;
      while (ordinal-- != 0) {
         word &= word - 1;
      }
      if (word == 0) {
         return STATUS_INVALID_STATE;
      }

      uint64 separator = byte_no * 8 + __builtin_ctzll(word);
      if (bucket_offset == 0) {
         *start = 0;
         *end   = separator;
      } else {
         if (separator < bucket_offset - 1) {
            return STATUS_INVALID_STATE;
         }
         *start = separator - bucket_offset + 1;

         /* Discard the first separator and locate the adjacent one. */
         word &= word - 1;
         uint64 end_byte_no = byte_no;
         while (word == 0) {
            end_byte_no += sizeof(word);
            if (end_byte_no >= view->encoding_size) {
               return STATUS_INVALID_STATE;
            }
            bytes = view->encoding_size - end_byte_no;
            if (bytes > sizeof(word)) {
               bytes = sizeof(word);
            }
            memcpy(&word, view->encoding + end_byte_no, bytes);
         }
         separator = end_byte_no * 8 + __builtin_ctzll(word);
         if (separator < bucket_offset) {
            return STATUS_INVALID_STATE;
         }
         *end = separator - bucket_offset;
      }

      return *start <= *end && *end <= view->hdr->num_remainders
                ? STATUS_OK
                : STATUS_INVALID_STATE;
   }
   return STATUS_INVALID_STATE;
}

static platform_status
routing_get_bucket_counts(const routing_config      *cfg,
                          const routing_header_view *view,
                          uint32                    *count)
{
   memset(count, 0, cfg->index_size * sizeof(*count));
   uint64 bucket = 0;
   uint64 zeros  = 0;
   for (uint64 byte_no = 0;
        byte_no < view->encoding_size && bucket < cfg->index_size;
        byte_no++)
   {
      uint8 byte = (uint8)view->encoding[byte_no];
      for (uint32 bit = 0; bit < 8 && bucket < cfg->index_size; bit++) {
         if (byte & (1U << bit)) {
            if (zeros > UINT32_MAX) {
               return STATUS_INVALID_STATE;
            }
            count[bucket++] = zeros;
            zeros           = 0;
         } else {
            zeros++;
         }
      }
   }
   if (bucket != cfg->index_size) {
      return STATUS_INVALID_STATE;
   }
   uint64 total = 0;
   for (uint32 i = 0; i < cfg->index_size; i++) {
      total += count[i];
   }
   return total == view->hdr->num_remainders ? STATUS_OK : STATUS_INVALID_STATE;
}

static platform_status
routing_filter_lookup_view(const routing_config        *cfg,
                           const routing_filter_params *params,
                           uint32                       fp,
                           const routing_header_view   *view,
                           uint64                      *found_values)
{
   uint32 encoded_fp = fp << params->value_size;
   uint32 bucket =
      routing_get_bucket(encoded_fp, params->remainder_and_value_size);
   uint32          bucket_off = bucket % cfg->index_size;
   uint64          start, end;
   platform_status rc =
      routing_get_bucket_bounds(cfg, view, bucket_off, &start, &end);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint32 remainder_mask = params->remainder_size == 32 ? UINT32_MAX
                           : params->remainder_size == 0
                              ? 0
                              : (1U << params->remainder_size) - 1;
   uint32 remainder      = fp & remainder_mask;
   uint32 value_mask =
      params->value_size == 0 ? 0 : (1U << params->value_size) - 1;
   uint64 found = 0;
   for (uint64 pos = end; pos > start;) {
      pos--;
      uint32 found_remainder_and_value;
      routing_filter_get_remainder_and_value(cfg,
                                             (uint32 *)view->remainders,
                                             pos,
                                             &found_remainder_and_value,
                                             params->remainder_and_value_size);
      uint32 found_remainder = found_remainder_and_value >> params->value_size;
      if (found_remainder == remainder) {
         uint32 found_value = found_remainder_and_value & value_mask;
         if (found_value >= MAX_FILTERS) {
            return STATUS_INVALID_STATE;
         }
         found |= 1ULL << found_value;
      }
   }
   *found_values = found;
   return STATUS_OK;
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
 *      meta_head should be passed to routing_filter_dec_ref
 *----------------------------------------------------------------------
 */
platform_status
routing_filter_add(cache                *cc,
                   const routing_config *cfg,
                   routing_filter       *old_filter,
                   routing_filter       *filter,
                   uint32               *new_fp_arr,
                   uint64                num_new_fp,
                   uint16                value)
{
   ZERO_CONTENTS(filter);

   if (old_filter == NULL || new_fp_arr == NULL || value >= MAX_FILTERS
       || num_new_fp > UINT32_MAX
       || num_new_fp > UINT32_MAX - old_filter->num_fingerprints)
   {
      return STATUS_BAD_PARAM;
   }

   routing_filter_params old_params;
   platform_status rc = routing_filter_get_params(cfg, old_filter, &old_params);
   if (!SUCCESS(rc)) {
      return rc;
   }

   // old filter
   uint32 old_num_indices              = 1;
   size_t old_value_size               = 0;
   uint32 old_value_mask               = 0;
   size_t old_remainder_and_value_size = 0;
   if (old_filter->addr != 0) {
      mini_prefetch(cc, PAGE_TYPE_FILTER, old_filter->meta_head);
      old_num_indices = old_params.num_indices;
      old_value_size  = old_params.value_size;
      old_value_mask  = old_value_size == 0 ? 0 : (1U << old_value_size) - 1;
      old_remainder_and_value_size = old_params.remainder_and_value_size;
   }

   // compute parameters
   filter->num_fingerprints = (uint32)num_new_fp + old_filter->num_fingerprints;
   filter->num_unique       = 0;

   if (filter->num_fingerprints == 0) {
      return STATUS_BAD_PARAM;
   }

   uint32 fingerprint_size  = old_filter->addr == 0
                                 ? cfg->fingerprint_size
                                 : old_filter->fingerprint_size;
   filter->format_version   = ROUTING_FILTER_FORMAT_VERSION;
   filter->fingerprint_size = fingerprint_size;

   uint32 log_num_buckets = 31 - __builtin_clz(filter->num_fingerprints);
   if (log_num_buckets < cfg->log_index_size) {
      log_num_buckets = cfg->log_index_size;
   }
   size_t value_size = value == 0 ? 0 : 32 - __builtin_clz(value);
   if (log_num_buckets > fingerprint_size || fingerprint_size + value_size > 32)
   {
      ZERO_CONTENTS(filter);
      return STATUS_LIMIT_EXCEEDED;
   }

   uint32 log_num_indices = log_num_buckets - cfg->log_index_size;
   uint32 num_indices     = 1U << log_num_indices;
   debug_assert(num_indices > 0);
   uint32 remainder_size = fingerprint_size - log_num_buckets;
   if (old_value_size > value_size) {
      return STATUS_INVALID_STATE;
   }
   filter->value_size              = value_size;
   size_t remainder_and_value_size = value_size + remainder_size;
   size_t index_remainder_and_value_size =
      remainder_and_value_size + cfg->log_index_size;
   if (remainder_and_value_size == 0 || index_remainder_and_value_size > 32
       || num_indices > cfg->extent_size / sizeof(uint64))
   {
      ZERO_CONTENTS(filter);
      return STATUS_LIMIT_EXCEEDED;
   }
   uint32 remainder_and_value_mask  = remainder_and_value_size == 32
                                         ? UINT32_MAX
                                         : (1U << remainder_and_value_size) - 1;
   uint32 new_indices_per_old_index = num_indices / old_num_indices;

   // for convenience
   uint64 page_size        = cfg->page_size;
   uint64 extent_size      = cfg->extent_size;
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
   uint8  *encoding_buffer;
   uint64  encoding_buffer_size =
      (ROUTING_FPS_PER_PAGE + cfg->index_size - 1) / 8 + 4;
   uint64 encoding_buffer_words =
      (encoding_buffer_size + sizeof(uint32) - 1) / sizeof(uint32);
   size_t temp_buffer_count = num_new_fp +                // temp
                              num_indices +               // index_count
                              index_size +                // old_count
                              MATRIX_ROWS * MATRIX_COLS + // matrix
                              ROUTING_FPS_PER_PAGE +      // fp_buffer
                              ROUTING_FPS_PER_PAGE +      // old_fp_buffer
                              encoding_buffer_words;      // encoding_buffer
   debug_assert(temp_buffer_count < 100000000);
   uint32 *temp =
      TYPED_ARRAY_ZALLOC(PROCESS_PRIVATE_HEAP_ID, temp, temp_buffer_count);

   if (temp == NULL) {
      return STATUS_NO_MEMORY;
   }
   index_count     = temp + num_new_fp;
   old_count       = index_count + num_indices;
   matrix          = old_count + cfg->index_size;
   fp_buffer       = matrix + MATRIX_ROWS * MATRIX_COLS;
   old_fp_buffer   = fp_buffer + ROUTING_FPS_PER_PAGE;
   encoding_buffer = (uint8 *)(old_fp_buffer + ROUTING_FPS_PER_PAGE);
   memset(encoding_buffer, 0xff, encoding_buffer_size);

   // we use a mini_allocator to obtain pages
   allocator     *al               = cache_get_allocator(cc);
   uint64         meta_head        = 0;
   bool32         mini_initialized = FALSE;
   mini_allocator mini;
   page_handle   *index_page[MAX_PAGES_PER_EXTENT] = {0};
   uint64         num_index_pages                  = 0;
   page_handle   *filter_page                      = NULL;
   page_handle   *old_filter_node                  = NULL;
   rc = allocator_alloc(al, &meta_head, PAGE_TYPE_FILTER);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   filter->meta_head = meta_head;
   // filters use an unkeyed mini allocator
   rc = mini_init(&mini, cc, filter->meta_head, 0, 1, PAGE_TYPE_FILTER, NULL);
   if (!SUCCESS(rc)) {
      refcount ref = allocator_dec_ref(al, meta_head, PAGE_TYPE_FILTER);
      platform_assert(ref == AL_NO_REFS);
      cache_extent_discard(cc, meta_head, PAGE_TYPE_FILTER);
      ref = allocator_dec_ref(al, meta_head, PAGE_TYPE_FILTER);
      platform_assert(ref == AL_FREE);
      meta_head = 0;
      goto cleanup;
   }
   mini_initialized = TRUE;

   // set up the index pages
   uint64 addrs_per_page = page_size / sizeof(uint64);
   uint64 index_addr;
   rc = mini_alloc_page(&mini, 0, &index_addr, NULL);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   platform_assert(index_addr % extent_size == 0);
   index_page[0] = cache_alloc(cc, index_addr, PAGE_TYPE_FILTER);
   if (index_page[0] == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }
   num_index_pages++;
   for (uint64 i = 1; i < pages_per_extent; i++) {
      uint64 next_index_addr;
      rc = mini_alloc_page(&mini, 0, &next_index_addr, NULL);
      if (!SUCCESS(rc)) {
         goto cleanup;
      }
      platform_assert(next_index_addr == index_addr + i * page_size);
      index_page[i] = cache_alloc(cc, next_index_addr, PAGE_TYPE_FILTER);
      if (index_page[i] == NULL) {
         rc = STATUS_NO_MEMORY;
         goto cleanup;
      }
      num_index_pages++;
   }
   filter->addr = index_addr;

   // we write to the filter with the filter cursor
   uint64 addr;
   rc = mini_alloc_page(&mini, 0, &addr, NULL);
   if (!SUCCESS(rc)) {
      goto cleanup;
   }
   filter_page = cache_alloc(cc, addr, PAGE_TYPE_FILTER);
   if (filter_page == NULL) {
      rc = STATUS_NO_MEMORY;
      goto cleanup;
   }
   char  *filter_cursor           = filter_page->data;
   uint64 bytes_remaining_on_page = page_size;

   for (uint32 new_fp_no = 0; new_fp_no < num_new_fp; new_fp_no++) {
      new_fp_arr[new_fp_no] >>= 32 - fingerprint_size;
   }

   uint32 *fp_arr =
      RadixSort(new_fp_arr, matrix, temp, num_new_fp, fingerprint_size);

   for (uint32 new_fp_no = 0; new_fp_no < num_new_fp; new_fp_no++) {
      fp_arr[new_fp_no] <<= value_size;
      fp_arr[new_fp_no] |= value;
   }


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
      {
      }
      index_count[index_no] = fp_no - index_start;
   }

   fp_no                        = 0;
   uint64 old_fingerprints_seen = 0;
   for (uint32 old_index_no = 0; old_index_no < old_num_indices; old_index_no++)
   {
      // process metadata from old filter
      char  *old_block_start = NULL;
      uint16 old_index_count = 0;
      if (old_filter->addr != 0) {
         uint64 hdr_raw_addr;
         rc = routing_get_header(cc,
                                 cfg,
                                 old_filter->addr,
                                 old_index_no,
                                 &old_filter_node,
                                 &hdr_raw_addr);
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
         routing_header_view old_view;
         rc = routing_header_view_init(cfg,
                                       old_filter_node,
                                       hdr_raw_addr,
                                       old_remainder_and_value_size,
                                       &old_view);
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
         old_block_start = old_view.remainders;
         old_index_count = old_view.hdr->num_remainders;
         rc              = routing_get_bucket_counts(cfg, &old_view, old_count);
         if (!SUCCESS(rc)) {
            goto cleanup;
         }
         if (old_fingerprints_seen > old_filter->num_fingerprints
             || old_index_count
                   > old_filter->num_fingerprints - old_fingerprints_seen)
         {
            rc = STATUS_INVALID_STATE;
            goto cleanup;
         }
         old_fingerprints_seen += old_index_count;
         // routing_filter_print_encoding(cfg, old_hdr);
      }

      uint32 *old_src_fp         = old_fp_buffer;
      uint32 *dst_fp             = fp_buffer;
      uint32  index_bucket_start = old_index_no * index_size;
      if (old_index_count != 0) {
         if (old_index_count > ROUTING_FPS_PER_PAGE) {
            rc = STATUS_INVALID_STATE;
            goto cleanup;
         }
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
           index_off++)
      {
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
                || old_fps_added < old_index_count)
         {
            uint32 fp;
            bool32 is_old = ((new_fps_added == new_index_count)
                             || ((old_fps_added != old_index_count)
                                 && (old_src_fp[old_fps_added]
                                     <= new_src_fp[new_fps_added])));
            if (is_old) {
               fp = old_src_fp[old_fps_added++];
            } else {
               fp = new_src_fp[new_fps_added++];
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
            if (last_fp_added >> value_size != fp >> value_size) {
               filter->num_unique++;
            }
            if (bucket < last_bucket) {
               rc = STATUS_INVALID_STATE;
               goto cleanup;
            }
            if (fps_added == ROUTING_FPS_PER_PAGE) {
               rc = STATUS_LIMIT_EXCEEDED;
               goto cleanup;
            }
            header_bit += bucket - last_bucket;
            last_bucket = bucket;
            routing_unset_bit(encoding_buffer, header_bit++);
            last_fp_added       = fp;
            dst_fp[fps_added++] = fp & remainder_and_value_mask;
         }

         uint32 remainder_block_size =
            routing_remainder_block_size(fps_added, remainder_and_value_size);
         uint64 encoding_size = (fps_added + index_size - 1) / 8 + 4;
         uint64 header_size   = routing_header_size(cfg, fps_added);
         if (header_size > page_size
             || remainder_block_size > page_size - header_size)
         {
            rc = STATUS_LIMIT_EXCEEDED;
            goto cleanup;
         }
         if (header_size + remainder_block_size > bytes_remaining_on_page) {
            routing_unlock_and_unget_page(cc, filter_page);
            filter_page = NULL;
            rc          = mini_alloc_page(&mini, 0, &addr, NULL);
            if (!SUCCESS(rc)) {
               goto cleanup;
            }
            filter_page = cache_alloc(cc, addr, PAGE_TYPE_FILTER);
            if (filter_page == NULL) {
               rc = STATUS_NO_MEMORY;
               goto cleanup;
            }

            bytes_remaining_on_page = page_size;
            filter_cursor           = filter_page->data;
         }

         // Set the index_no
         // ALEX: for now the indices must fit in a single extent
         uint64 index_page_no = index_no / addrs_per_page;
         uint64 index_offset  = index_no % addrs_per_page;
         if (index_page_no >= num_index_pages
             || index_page[index_page_no] == NULL)
         {
            rc = STATUS_INVALID_STATE;
            goto cleanup;
         }
         uint64 *index_cursor = (uint64 *)(index_page[index_page_no]->data);
         index_cursor += index_offset;
         uint64 filter_page_offset = filter_cursor - filter_page->data;
         *index_cursor             = addr + filter_page_offset;

         routing_hdr *hdr    = (routing_hdr *)filter_cursor;
         hdr->num_remainders = fps_added;
         memmove(hdr->encoding, encoding_buffer, encoding_size);
         memset((char *)hdr + sizeof(*hdr) + encoding_size,
                0,
                header_size - sizeof(*hdr) - encoding_size);
         memset(encoding_buffer, 0xff, encoding_size);
         filter_cursor += header_size;
         memset(filter_cursor, 0, remainder_block_size);
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
         old_filter_node = NULL;
      }
   }
   debug_assert(fp_no == num_new_unique_fp);
   if (filter->num_unique > filter->num_fingerprints) {
      rc = STATUS_INVALID_STATE;
      goto cleanup;
   }
   routing_unlock_and_unget_page(cc, filter_page);

   for (uint64 i = 0; i < pages_per_extent; i++) {
      routing_unlock_and_unget_page(cc, index_page[i]);
   }

   mini_release(&mini);

   platform_free(PROCESS_PRIVATE_HEAP_ID, temp);

   return STATUS_OK;

cleanup:
   if (old_filter_node != NULL) {
      routing_unget_header(cc, old_filter_node);
   }
   if (filter_page != NULL) {
      routing_unlock_and_unget_page(cc, filter_page);
   }
   for (uint64 i = 0; i < num_index_pages; i++) {
      routing_unlock_and_unget_page(cc, index_page[i]);
   }
   if (mini_initialized) {
      mini_release(&mini);
      (void)mini_dec_ref(cc, meta_head, PAGE_TYPE_FILTER);
   }
   platform_free(PROCESS_PRIVATE_HEAP_ID, temp);
   ZERO_CONTENTS(filter);
   return rc;
}

static platform_status
routing_filter_prefetch(cache                *cc,
                        const routing_config *cfg,
                        routing_filter       *filter,
                        uint64                num_indices)
{
   if (cc == NULL || cfg == NULL || filter == NULL) {
      return STATUS_BAD_PARAM;
   }

   routing_filter_params params;
   platform_status       rc = routing_filter_get_params(cfg, filter, &params);
   if (!SUCCESS(rc)) {
      return rc;
   }
   if (num_indices > params.num_indices) {
      return STATUS_INVALID_STATE;
   }
   if (num_indices == 0) {
      return STATUS_OK;
   }
   uint64 last_extent_addr = 0;
   uint64 page_size        = cfg->page_size;
   uint64 addrs_per_page   = page_size / sizeof(uint64);
   if (addrs_per_page == 0) {
      return STATUS_INVALID_STATE;
   }
   uint64 num_index_pages = (num_indices - 1) / addrs_per_page + 1;
   uint64 index_no        = 0;

   for (uint64 index_page_no = 0; index_page_no < num_index_pages;
        index_page_no++)
   {
      if (index_page_no > (UINT64_MAX - filter->addr) / page_size) {
         return STATUS_INVALID_STATE;
      }
      uint64       index_addr = filter->addr + page_size * index_page_no;
      page_handle *index_page =
         cache_get(cc, index_addr, TRUE, PAGE_TYPE_FILTER);
      if (index_page == NULL) {
         return STATUS_IO_ERROR;
      }

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
         if (hdr_raw_addr == 0 || hdr_raw_addr % sizeof(uint32) != 0) {
            cache_unget(cc, index_page);
            return STATUS_INVALID_STATE;
         }
         uint64 extent_addr = hdr_raw_addr - (hdr_raw_addr % cfg->extent_size);
         if (extent_addr != last_extent_addr) {
            cache_prefetch(cc, extent_addr, PAGE_TYPE_FILTER);
            last_extent_addr = extent_addr;
         }
      }
      cache_unget(cc, index_page);
   }
   return STATUS_OK;
}

platform_status
routing_filter_estimate_unique_fp(cache                *cc,
                                  const routing_config *cfg,
                                  platform_heap_id      hid,
                                  routing_filter       *filter,
                                  uint64                num_filters,
                                  uint32               *num_unique_fp,
                                  uint32               *fingerprint_size)
{
   if (num_unique_fp == NULL || fingerprint_size == NULL) {
      platform_error_log("routing_filter_estimate_unique_fp: "
                         "output pointers must not be NULL\n");
      return STATUS_BAD_PARAM;
   }

   *num_unique_fp    = 0;
   *fingerprint_size = 0;
   if (filter == NULL || num_filters > MAX_FILTERS) {
      return STATUS_BAD_PARAM;
   }

   uint64 total_num_fp            = 0;
   uint32 common_fingerprint_size = 32;
   bool32 has_sampled_filter      = FALSE;
   for (uint64 i = 0; i != num_filters; i++) {
      routing_filter_params params;
      platform_status rc = routing_filter_get_params(cfg, &filter[i], &params);
      if (!SUCCESS(rc)) {
         return rc;
      }
      total_num_fp += filter[i].num_fingerprints;
      if (filter[i].addr != 0 && params.num_indices >= 16) {
         has_sampled_filter = TRUE;
         common_fingerprint_size =
            MIN(common_fingerprint_size, filter[i].fingerprint_size);
      }
   }
   if (!has_sampled_filter) {
      return STATUS_OK;
   }
   *fingerprint_size    = common_fingerprint_size;
   uint64 buffer_size64 = total_num_fp / 12;
   if (buffer_size64 > UINT32_MAX
       || cfg->index_size > UINT32_MAX - buffer_size64)
   {
      return STATUS_LIMIT_EXCEEDED;
   }
   uint32  buffer_size = buffer_size64;
   uint32  alloc_size  = buffer_size + cfg->index_size;
   uint32 *local       = TYPED_ARRAY_ZALLOC(hid, local, alloc_size);
   if (local == NULL) {
      platform_error_log("routing_filter_estimate_unique_fp: failed to "
                         "allocate fingerprint work buffer of %u uint32s\n",
                         alloc_size);
      return STATUS_NO_MEMORY;
   }
   uint32 *fp_arr = local;
   uint32 *count  = local + buffer_size;

   uint32          src_fp_no = 0;
   uint32          dst_fp_no = 0;
   platform_status rc        = STATUS_OK;
   /* fp_start has one sentinel after the entry for each filter. */
   uint32 fp_start[MAX_FILTERS + 1] = {0};
   for (uint64 i = 0; i != num_filters; i++) {
      if (filter[i].addr == 0) {
         fp_start[i + 1] = dst_fp_no;
         continue;
      }
      routing_filter_params params;
      rc = routing_filter_get_params(cfg, &filter[i], &params);
      if (!SUCCESS(rc)) {
         goto out;
      }
      uint32 num_indices             = params.num_indices;
      uint32 index_size              = cfg->index_size;
      uint32 filter_fingerprint_size = filter[i].fingerprint_size;

      if (num_indices >= 16) {
         // the filter is too small forget it
         platform_assert(num_indices % 16 == 0);
         num_indices /= 16;

         rc = routing_filter_prefetch(cc, cfg, &filter[i], num_indices);
         if (!SUCCESS(rc)) {
            goto out;
         }

         for (uint32 index_no = 0; index_no < num_indices; index_no++) {
            // process metadata
            char        *block_start;
            uint16       index_count = 0;
            page_handle *filter_node;
            uint64       hdr_raw_addr;
            rc = routing_get_header(
               cc, cfg, filter[i].addr, index_no, &filter_node, &hdr_raw_addr);
            if (!SUCCESS(rc)) {
               goto out;
            }
            routing_header_view view;
            rc = routing_header_view_init(cfg,
                                          filter_node,
                                          hdr_raw_addr,
                                          params.remainder_and_value_size,
                                          &view);
            if (!SUCCESS(rc)) {
               routing_unget_header(cc, filter_node);
               goto out;
            }
            block_start = view.remainders;
            index_count = view.hdr->num_remainders;
            rc          = routing_get_bucket_counts(cfg, &view, count);
            if (!SUCCESS(rc)) {
               routing_unget_header(cc, filter_node);
               goto out;
            }
            // routing_filter_print_encoding(cfg, hdr);

            uint32 index_bucket_start = index_no * index_size;
            if (src_fp_no > buffer_size
                || index_count > buffer_size - src_fp_no)
            {
               routing_unget_header(cc, filter_node);
               rc = STATUS_INVALID_STATE;
               goto out;
            }
            uint32 *src_fp = &fp_arr[src_fp_no];
            if (index_count != 0) {
               debug_only uint32 index_start = src_fp_no;
               PackedArray_unpack((uint32 *)block_start,
                                  0,
                                  src_fp,
                                  index_count,
                                  params.remainder_and_value_size);
               uint32 last_fp = UINT32_MAX;
               for (uint32 bucket_off = 0; bucket_off < index_size;
                    bucket_off++)
               {
                  uint32 bucket = index_bucket_start + bucket_off;
                  for (uint32 i = 0; i < count[bucket_off]; i++) {
                     fp_arr[src_fp_no] |= bucket
                                          << params.remainder_and_value_size;
                     fp_arr[src_fp_no] >>= params.value_size;
                     /* Wider fingerprints extend narrower fingerprints with
                      * low bits. Normalize to the narrowest participating
                      * width before merging the sorted streams. */
                     fp_arr[src_fp_no] >>=
                        filter_fingerprint_size - common_fingerprint_size;
                     if (fp_arr[src_fp_no] == last_fp) {
                        src_fp_no++;
                     } else {
                        if (dst_fp_no >= buffer_size) {
                           routing_unget_header(cc, filter_node);
                           rc = STATUS_INVALID_STATE;
                           goto out;
                        }
                        last_fp             = fp_arr[src_fp_no];
                        fp_arr[dst_fp_no++] = fp_arr[src_fp_no++];
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

   if (num_unique > UINT32_MAX / 16) {
      rc = STATUS_LIMIT_EXCEEDED;
      goto out;
   }
   *num_unique_fp = num_unique * 16;

out:
   platform_free(hid, local);
   return rc;
}

static inline async_status
routing_get_header_async(routing_filter_lookup_async_state *state, uint64 depth)
{
   async_begin(state, depth);

   state->page_size      = state->cfg->page_size;
   state->addrs_per_page = state->page_size / sizeof(uint64);
   if (state->addrs_per_page == 0) {
      async_return(state, STATUS_INVALID_STATE);
   }
   uint64 index_page_no = state->index / state->addrs_per_page;
   if (index_page_no > (UINT64_MAX - state->filter.addr) / state->page_size) {
      async_return(state, STATUS_INVALID_STATE);
   }
   state->index_addr = state->filter.addr + state->page_size * index_page_no;

   async_await_call(state,
                    cache_get_async,
                    &state->cache_get_state,
                    state->cc,
                    state->index_addr,
                    PAGE_TYPE_FILTER,
                    state->callback,
                    state->callback_arg);
   state->index_page = cache_get_async_state_result(&state->cache_get_state);
   if (state->index_page == NULL) {
      async_return(state, STATUS_IO_ERROR);
   }

   state->hdr_raw_addr =
      ((uint64 *)state->index_page->data)[state->index % state->addrs_per_page];
   cache_unget(state->cc, state->index_page);
   state->index_page = NULL;
   if (state->hdr_raw_addr == 0 || state->hdr_raw_addr % sizeof(uint32) != 0) {
      async_return(state, STATUS_INVALID_STATE);
   }
   state->header_addr =
      state->hdr_raw_addr - (state->hdr_raw_addr % state->page_size);

   async_await_call(state,
                    cache_get_async,
                    &state->cache_get_state,
                    state->cc,
                    state->header_addr,
                    PAGE_TYPE_FILTER,
                    state->callback,
                    state->callback_arg);
   state->filter_page = cache_get_async_state_result(&state->cache_get_state);
   if (state->filter_page == NULL) {
      async_return(state, STATUS_IO_ERROR);
   }
   async_return(state, STATUS_OK);
}


async_status
routing_filter_lookup_async(routing_filter_lookup_async_state *state)
{
   async_begin(state, 0);

   debug_assert(key_is_user_key(state->target));

   if (state->filter.addr == 0) {
      *state->found_values = 0;
      routing_filter_params params;
      async_return(
         state, routing_filter_get_params(state->cfg, &state->filter, &params));
   }

   routing_filter_params params;
   platform_status       rc =
      routing_filter_get_params(state->cfg, &state->filter, &params);
   if (!SUCCESS(rc)) {
      async_return(state, rc);
   }

   state->fp =
      data_key_hash(state->cfg->data_cfg, state->target, state->cfg->seed);
   state->fp >>= 32 - state->filter.fingerprint_size;
   state->remainder_size = params.remainder_size;
   state->index = routing_get_index(state->fp << state->filter.value_size,
                                    params.index_remainder_and_value_size);
   if (state->index >= params.num_indices) {
      async_return(state, STATUS_INVALID_STATE);
   }

   async_await_subroutine(state, routing_get_header_async);
   if (!SUCCESS(state->__async_result)) {
      async_return(state, state->__async_result);
   }

   /* Locals do not survive an async yield; reconstruct the checked parameters
    * before using the header page returned by the subroutine. */
   rc = routing_filter_get_params(state->cfg, &state->filter, &params);
   if (!SUCCESS(rc)) {
      routing_unget_header(state->cc, state->filter_page);
      async_return(state, rc);
   }
   routing_header_view view;
   rc = routing_header_view_init(state->cfg,
                                 state->filter_page,
                                 state->hdr_raw_addr,
                                 params.remainder_and_value_size,
                                 &view);
   if (SUCCESS(rc)) {
      rc = routing_filter_lookup_view(
         state->cfg, &params, state->fp, &view, state->found_values);
   }
   routing_unget_header(state->cc, state->filter_page);
   async_return(state, rc);
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
routing_filter_lookup(cache                *cc,
                      const routing_config *cfg,
                      routing_filter       *filter,
                      key                   target,
                      uint64               *found_values)
{
#if 0
   return async_call_sync_callback(cache_cleanup(cc),
                                   routing_filter_lookup_async,
                                   cc,
                                   cfg,
                                   *filter,
                                   target,
                                   found_values);
#else
   debug_assert(key_is_user_key(target));

   if (filter == NULL || found_values == NULL) {
      return STATUS_BAD_PARAM;
   }
   if (filter->addr == 0) {
      *found_values = 0;
      routing_filter_params params;
      return routing_filter_get_params(cfg, filter, &params);
   }

   routing_filter_params params;
   platform_status       rc = routing_filter_get_params(cfg, filter, &params);
   if (!SUCCESS(rc)) {
      return rc;
   }

   uint32 fp = data_key_hash(cfg->data_cfg, target, cfg->seed);
   fp >>= 32 - filter->fingerprint_size;
   uint32 index = routing_get_index(fp << params.value_size,
                                    params.index_remainder_and_value_size);
   if (index >= params.num_indices) {
      return STATUS_INVALID_STATE;
   }

   page_handle *filter_node = NULL;
   uint64       hdr_raw_addr;
   rc = routing_get_header(
      cc, cfg, filter->addr, index, &filter_node, &hdr_raw_addr);
   if (!SUCCESS(rc)) {
      return rc;
   }
   routing_header_view view;
   rc = routing_header_view_init(
      cfg, filter_node, hdr_raw_addr, params.remainder_and_value_size, &view);
   if (SUCCESS(rc)) {
      rc = routing_filter_lookup_view(cfg, &params, fp, &view, found_values);
   }
   routing_unget_header(cc, filter_node);
   return rc;
#endif
}

/*
 *----------------------------------------------------------------------
 * routing_filter_inc_ref
 *
 *      incs the ref count of the filter
 *----------------------------------------------------------------------
 */
void
routing_filter_inc_ref(cache *cc, routing_filter *filter)
{
   if (filter->num_fingerprints == 0) {
      return;
   }

   uint64 meta_head = filter->meta_head;
   mini_inc_ref(cc, meta_head);
}

/*
 * Record the reference a holder has on this filter during a crash-recovery
 * rebuild, enumerating its extents if this is the first one to reach it.  The
 * recovery counterpart of routing_filter_inc_ref(); see
 * mini_recover_references().  An empty filter owns nothing, exactly as in
 * routing_filter_inc_ref().
 */
platform_status
routing_filter_recover_allocations(cache *cc, routing_filter *filter)
{
   if (filter->num_fingerprints == 0) {
      return STATUS_OK;
   }
   return mini_recover_references(cc, filter->meta_head, PAGE_TYPE_FILTER);
}

/*
 *----------------------------------------------------------------------
 * routing_filter_dec_ref
 *
 *      decs the ref count of the filter and destroys it if it reaches 0
 *----------------------------------------------------------------------
 */
void
routing_filter_dec_ref(cache *cc, routing_filter *filter)
{
   if (filter->num_fingerprints == 0) {
      return;
   }

   uint64 meta_head = filter->meta_head;
   mini_dec_ref(cc, meta_head, PAGE_TYPE_FILTER);
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
routing_filter_estimate_unique_keys_from_count(uint32 fingerprint_size,
                                               uint64 num_unique)
{
   if (num_unique == 0) {
      return 0;
   }
   if (fingerprint_size == 0 || fingerprint_size > 32) {
      return UINT32_MAX;
   }
   uint64 universe_count = 1ULL << fingerprint_size;
   /* Seeing every possible fingerprint makes the inverse estimate diverge.
    * Corrupt metadata can also claim more unique fingerprints than exist.
    * In either case, saturate because this estimate is only a sizing hint. */
   if (num_unique >= universe_count) {
      return UINT32_MAX;
   }

   double universe_size   = universe_count;
   double unseen_fp       = universe_size - num_unique;
   double universe_size_2 = universe_size * universe_size;
   double universe_size_4 = universe_size_2 * universe_size_2;
   double unseen_fp_2     = unseen_fp * unseen_fp;
   double unseen_fp_4     = unseen_fp_2 * unseen_fp_2;
   /*
    * Compute the difference H_|U| - H_{|U| - #unique_fp}, where U is the fp
    * universe.
    */
   double harmonic_diff = log(universe_size) - log(unseen_fp)
                          + 1 / 2.0 * (1 / universe_size - 1 / unseen_fp)
                          - 1 / 12.0 * (1 / universe_size_2 - 1 / unseen_fp_2)
                          + 1 / 120.0 * (1 / universe_size_4 - 1 / unseen_fp_4);
   double estimated_input_keys = universe_size * harmonic_diff;
   if (estimated_input_keys >= UINT32_MAX) {
      return UINT32_MAX;
   }
   if (estimated_input_keys <= 0) {
      return 0;
   }
   return (uint32)estimated_input_keys;
}

uint32
routing_filter_estimate_unique_keys(const routing_filter *filter)
{
   // platform_default_log("unique fp %u\n", filter->num_unique);
   return routing_filter_estimate_unique_keys_from_count(
      filter->fingerprint_size, filter->num_unique);
}

uint64
routing_filter_space_use_bytes(cache *cc, const routing_filter *filter)
{
   return mini_space_use_bytes(cc, filter->meta_head, PAGE_TYPE_FILTER);
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
   while (iterator_can_next(itor)) {
      key     curr_key;
      message msg;
      iterator_curr(itor, &curr_key, &msg);
      debug_assert(key_is_user_key(curr_key));
      uint64          found_values;
      platform_status rc =
         routing_filter_lookup(cc, cfg, filter, curr_key, &found_values);
      platform_assert_status_ok(rc);
      platform_assert(routing_filter_is_value_found(found_values, value));
      rc = iterator_next(itor);
      platform_assert_status_ok(rc);
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

static platform_status
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
      page_handle    *filter_page = NULL;
      uint64          hdr_raw_addr;
      platform_status rc = routing_get_header(
         cc, cfg, filter_addr, i, &filter_page, &hdr_raw_addr);
      if (!SUCCESS(rc)) {
         return rc;
      }
      platform_default_log("index 0x%lx: %lu\n", i, hdr_raw_addr);
      routing_unget_header(cc, filter_page);
   }
   return STATUS_OK;
}

static void
routing_filter_print_remainders(routing_config            *cfg,
                                const routing_header_view *view,
                                size_t                     remainder_size,
                                size_t                     value_size)
{
   uint64 i, j, start, end;
   platform_default_log("--- Remainders\n");
   size_t remainder_and_value_size = value_size + remainder_size;
   for (i = 0; i < cfg->index_size; i++) {
      platform_status rc =
         routing_get_bucket_bounds(cfg, view, i, &start, &end);
      if (!SUCCESS(rc)) {
         platform_default_log("invalid routing encoding\n");
         return;
      }
      platform_default_log("0x%lx remainders:", i);
      for (j = start; j < end; j++) {
         uint32 remainder, value, remainder_and_value;
         routing_filter_get_remainder_and_value(cfg,
                                                (uint32 *)view->remainders,
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
   routing_filter_params params;
   platform_status       rc = routing_filter_get_params(cfg, filter, &params);
   if (!SUCCESS(rc)) {
      platform_default_log("invalid routing filter descriptor\n");
      return;
   }
   if (filter->addr == 0) {
      platform_default_log("empty routing filter\n");
      return;
   }

   rc = routing_filter_print_index(cc, cfg, filter->addr, params.num_indices);
   if (!SUCCESS(rc)) {
      platform_default_log("invalid routing filter index\n");
      return;
   }
   uint64 i;
   for (i = 0; i < params.num_indices; i++) {
      platform_default_log("----------------------------------------\n");
      platform_default_log("--- Index 0x%lx\n", i);
      page_handle *filter_page;
      uint64       hdr_raw_addr;
      rc = routing_get_header(
         cc, cfg, filter->addr, i, &filter_page, &hdr_raw_addr);
      if (!SUCCESS(rc)) {
         platform_default_log("invalid routing header address\n");
         return;
      }
      routing_header_view view;
      rc = routing_header_view_init(cfg,
                                    filter_page,
                                    hdr_raw_addr,
                                    params.remainder_and_value_size,
                                    &view);
      if (!SUCCESS(rc)) {
         routing_unget_header(cc, filter_page);
         platform_default_log("invalid routing header\n");
         return;
      }
      routing_filter_print_encoding(cfg, view.hdr);
      routing_filter_print_remainders(
         cfg, &view, params.remainder_size, params.value_size);
      routing_unget_header(cc, filter_page);
   }
}
