// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Shared, disk-resident log identity types. */

#pragma once

#include <stddef.h>

#include "splinterdb/public_platform.h"

#define SHARD_LOG_FORMAT_VERSION (1)

typedef struct log_nonce {
   uint64 high;
   uint64 low;
} log_nonce;

_Static_assert(sizeof(log_nonce) == 16, "on-disk log nonce layout changed");

/*
 * The on-disk head of one mini-allocator-backed log stream: the data head
 * (where replay begins), the metadata head (which owns the stream's extents),
 * and a per-stream nonce that validates its pages. The format version and
 * checksum seed select the reader independently of new-log configuration.
 * Fixed at creation and
 * shared by the log interface and higher-level durable records.
 */
typedef struct ONDISK log_head {
   uint64    addr;
   uint64    meta_addr;
   log_nonce nonce;
   uint64    checksum_seed;
   uint16    format_version;
} log_head;

_Static_assert(offsetof(log_head, addr) == 0,
               "log data address layout changed");
_Static_assert(offsetof(log_head, meta_addr) == 8,
               "log metadata address layout changed");
_Static_assert(offsetof(log_head, nonce) == 16, "log nonce offset changed");
_Static_assert(sizeof(log_head) == 42, "on-disk log head layout changed");

static inline bool32
log_nonce_is_equal(log_nonce left, log_nonce right)
{
   return left.high == right.high && left.low == right.low;
}

static inline bool32
log_head_is_equal(log_head left, log_head right)
{
   return left.addr == right.addr && left.meta_addr == right.meta_addr
          && log_nonce_is_equal(left.nonce, right.nonce)
          && left.checksum_seed == right.checksum_seed
          && left.format_version == right.format_version;
}
