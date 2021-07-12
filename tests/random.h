// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

//////// Pseudo-random functions
typedef uint64 (*PRF)(const void *key, uint64 idx);

const void *platform_hash_prf_create (unsigned long seed);
void        platform_hash_prf_destroy(const void *key);
uint64      platform_hash_prf        (const void *key, uint64 idx);

/////// Stateful pseudo-random generators

typedef struct PRG {
  PRF prf;
  const void *key;
  uint64 idx;
} PRG;

uint64 PRG_peek(const PRG *prg, uint64 idx);
uint64 PRG_current(const PRG *prg);
uint64 PRG_step(PRG *prg);

void init_platform_hash_prg  (PRG *prg, unsigned long seed, uint64 seq);
void deinit_platform_hash_prg(PRG *prg);
