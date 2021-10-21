// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _SPLINTER_RANDOM_H_
#define _SPLINTER_RANDOM_H_

#include "platform.h"

typedef struct random_state {
   uint64 seed;
   uint64 seq;
} random_state;

static inline void
random_init(random_state *rs,   // OUT
            uint64        seed, // IN
            uint64        seq)         // IN
{
   ZERO_CONTENTS(rs);
   rs->seed = seed;
   rs->seq  = seq;
}

static inline uint64
random_next_uint64(random_state *rs) // IN/OUT
{
   uint64 seq = __sync_fetch_and_add(&rs->seq, 1);
   return platform_checksum64(&seq, sizeof(uint64), rs->seed);
}

static inline int8
random_next_int8(random_state *rs) // IN/OUT
{
   return (int8)random_next_uint64(rs);
}

static inline int16
random_next_int16(random_state *rs) // IN/OUT
{
   return (int16)random_next_uint64(rs);
}

static inline int32
random_next_int32(random_state *rs) // IN/OUT
{
   return (int32)random_next_uint64(rs);
}

static inline int64
random_next_int64(random_state *rs) // IN/OUT
{
   return (int64)random_next_uint64(rs);
}

static inline uint8
random_next_uint8(random_state *rs) // IN/OUT
{
   return (uint8)random_next_uint64(rs);
}

static inline uint16
random_next_uint16(random_state *rs) // IN/OUT
{
   return (uint16)random_next_uint64(rs);
}

static inline uint32
random_next_uint32(random_state *rs) // IN/OUT
{
   return (uint32)random_next_uint64(rs);
}

static inline void
random_bytes(random_state *rs, char *v, size_t n)
{
   while (n >= sizeof(uint64)) {
      *(uint64 *)v = random_next_uint64(rs);
      v += sizeof(uint64);
      n -= sizeof(uint64);
   }

   if (n > 0) {
      uint64 remainder = random_next_uint64(rs);
      memmove(v, &remainder, n);
   }
}

#endif
