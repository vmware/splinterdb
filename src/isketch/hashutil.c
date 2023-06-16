
#include "hashutil.h"


//-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby

// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.


// 64-bit hash for 64-bit platforms

uint64_t
MurmurHash64A(const void *key, int len, unsigned int seed)
{
   const uint64_t m = 0xc6a4a7935bd1e995;
   const int      r = 47;

   uint64_t h = seed ^ (len * m);

   const uint64_t *data = (const uint64_t *)key;
   const uint64_t *end  = data + (len / 8);

   while (data != end) {
      uint64_t k = *data++;

      k *= m;
      k ^= k >> r;
      k *= m;

      h ^= k;
      h *= m;
   }

   const unsigned char *data2 = (const unsigned char *)data;

   switch (len & 7) {
      case 7:
         h ^= (uint64_t)data2[6] << 48;
      case 6:
         h ^= (uint64_t)data2[5] << 40;
      case 5:
         h ^= (uint64_t)data2[4] << 32;
      case 4:
         h ^= (uint64_t)data2[3] << 24;
      case 3:
         h ^= (uint64_t)data2[2] << 16;
      case 2:
         h ^= (uint64_t)data2[1] << 8;
      case 1:
         h ^= (uint64_t)data2[0];
         h *= m;
   };

   h ^= h >> r;
   h *= m;
   h ^= h >> r;

   return h;
}


// 64-bit hash for 32-bit platforms

uint64_t
MurmurHash64B(const void *key, int len, unsigned int seed)
{
   const unsigned int m = 0x5bd1e995;
   const int          r = 24;

   unsigned int h1 = seed ^ len;
   unsigned int h2 = 0;

   const unsigned int *data = (const unsigned int *)key;

   while (len >= 8) {
      unsigned int k1 = *data++;
      k1 *= m;
      k1 ^= k1 >> r;
      k1 *= m;
      h1 *= m;
      h1 ^= k1;
      len -= 4;

      unsigned int k2 = *data++;
      k2 *= m;
      k2 ^= k2 >> r;
      k2 *= m;
      h2 *= m;
      h2 ^= k2;
      len -= 4;
   }

   if (len >= 4) {
      unsigned int k1 = *data++;
      k1 *= m;
      k1 ^= k1 >> r;
      k1 *= m;
      h1 *= m;
      h1 ^= k1;
      len -= 4;
   }

   switch (len) {
      case 3:
         h2 ^= ((unsigned char *)data)[2] << 16;
      case 2:
         h2 ^= ((unsigned char *)data)[1] << 8;
      case 1:
         h2 ^= ((unsigned char *)data)[0];
         h2 *= m;
   };

   h1 ^= h2 >> 18;
   h1 *= m;
   h2 ^= h1 >> 22;
   h2 *= m;
   h1 ^= h2 >> 17;
   h1 *= m;
   h2 ^= h1 >> 19;
   h2 *= m;

   uint64_t h = h1;

   h = (h << 32) | h2;

   return h;
}

/*
 *   For any 1<k<=64, let mask=(1<<k)-1. hash_64() is a bijection on [0,1<<k),
 *   which means
 *     hash_64(x, mask)==hash_64(y, mask) if and only if x==y. hash_64i() is
 *     the inversion of
 *       hash_64(): hash_64i(hash_64(x, mask), mask) == hash_64(hash_64i(x,
 *       mask), mask) == x.
 */

// Thomas Wang's integer hash functions. See
// <https://gist.github.com/lh3/59882d6b96166dfc3d8d> for a snapshot.

uint64_t
hash_64(uint64_t key, uint64_t mask)
{
   key = (~key + (key << 21)) & mask; // key = (key << 21) - key - 1;
   key = key ^ key >> 24;
   key = ((key + (key << 3)) + (key << 8)) & mask; // key * 265
   key = key ^ key >> 14;
   key = ((key + (key << 2)) + (key << 4)) & mask; // key * 21
   key = key ^ key >> 28;
   key = (key + (key << 31)) & mask;
   return key;
}

// The inversion of hash_64(). Modified from
// <https://naml.us/blog/tag/invertible>
uint64_t
hash_64i(uint64_t key, uint64_t mask)
{
   uint64_t tmp;

   // Invert key = key + (key << 31)
   tmp = (key - (key << 31));
   key = (key - (tmp << 31)) & mask;

   // Invert key = key ^ (key >> 28)
   tmp = key ^ key >> 28;
   key = key ^ tmp >> 28;

   // Invert key *= 21
   key = (key * 14933078535860113213ull) & mask;

   // Invert key = key ^ (key >> 14)
   tmp = key ^ key >> 14;
   tmp = key ^ tmp >> 14;
   tmp = key ^ tmp >> 14;
   key = key ^ tmp >> 14;

   // Invert key *= 265
   key = (key * 15244667743933553977ull) & mask;

   // Invert key = key ^ (key >> 24)
   tmp = key ^ key >> 24;
   key = key ^ tmp >> 24;

   // Invert key = (~key) + (key << 21)
   tmp = ~key;
   tmp = ~(key - (tmp << 21));
   tmp = ~(key - (tmp << 21));
   key = ~(key - (tmp << 21)) & mask;

   return key;
}
