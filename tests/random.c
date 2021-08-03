// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "random.h"

//////// Pseudo-random functions
const void *platform_hash_prf_create(unsigned long seed) {
  return (const void *)seed;
}

void platform_hash_prf_destroy(const void *key) {}

uint64 platform_hash_prf(const void *key, uint64 idx) {
  return platform_hash64(&idx, sizeof(idx), (unsigned long)key);
}

/////// Stateful pseudo-random generators

uint64 PRG_peek(const PRG *prg, uint64 idx) {
  uint64 result = prg->prf(prg->key, idx);
  return result;
}

uint64 PRG_current(const PRG *prg) { return PRG_peek(prg, prg->idx); }

uint64 PRG_step(PRG *prg) {
  prg->idx++;
  return PRG_current(prg);
}

void init_platform_hash_prg(PRG *prg, unsigned long seed, uint64 seq) {
  prg->prf = platform_hash_prf;
  prg->key = platform_hash_prf_create(seed);
  prg->idx = seq;
}

void deinit_platform_hash_prg(PRG *prg) { platform_hash_prf_destroy(prg->key); }
