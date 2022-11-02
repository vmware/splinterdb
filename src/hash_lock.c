#include "hash_lock.h"

hash_lock_config default_hash_lock_config = {.size        = 1000000000,
                                             .seed        = 0xc0ffee,
                                             .hash        = platform_hash32,
                                             .sleep_in_ns = 1000};

void
hash_lock_init(hash_lock *hl, hash_lock_config *cfg)
{
   hl->cfg = cfg ? cfg : &default_hash_lock_config;
   hl->slots = TYPED_ARRAY_ZALLOC(0, hl->slots, cfg->size);
}

void
hash_lock_deinit(hash_lock *hl)
{
   platform_free(0, hl->slots);
   hl->cfg = NULL;
}

static inline uint32
get_index(slice key, const hash_lock_config *cfg)
{
   return cfg->hash(slice_data(key), slice_length(key), cfg->seed) % cfg->size;
}

void
hash_lock_acquire(hash_lock *hl, slice key)
{
   uint32 index = get_index(key, hl->cfg);
   while (!__sync_bool_compare_and_swap(&hl->slots[index], 0, 1)) {
      platform_sleep(hl->cfg->sleep_in_ns);
   }
}

void
hash_lock_release(hash_lock *hl, slice key)
{
   uint32 index = get_index(key, hl->cfg);
   __sync_bool_compare_and_swap(&hl->slots[index], 1, 0);
}
