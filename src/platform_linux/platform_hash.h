#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include <xxhash.h>

// hash functions
typedef uint32 (*hash_fn)(const void *input, size_t length, unsigned int seed);

#define HASH_SEED (42)

// checksums
typedef XXH32_hash_t  checksum32;
typedef XXH64_hash_t  checksum64;
typedef XXH128_hash_t checksum128;

#define platform_checksum32  XXH32
#define platform_checksum64  XXH64
#define platform_checksum128 XXH128

#define platform_hash32  XXH32
#define platform_hash64  XXH64
#define platform_hash128 XXH128

static inline bool32
platform_checksum_is_equal(checksum128 left, checksum128 right)
{
   return XXH128_isEqual(left, right);
}
