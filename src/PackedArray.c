// From https://github.com/gpakosz/PackedArray
// Lightly edited to match the routing filter API
//
// clang-format off
//
// see README.md for usage instructions.
// (‑●‑●)> released under the WTFPL v2 license, by Gregory Pakosz (@gpakosz)

#include "platform.h"

#ifndef PACKEDARRAY_SELF
#define PACKEDARRAY_SELF "PackedArray.c"
#endif

#ifdef PACKEDARRAY_IMPL

#ifndef PACKEDARRAY_JOIN
#define PACKEDARRAY_JOIN(lhs, rhs)    PACKEDARRAY_JOIN_(lhs, rhs)
#define PACKEDARRAY_JOIN_(lhs, rhs)   PACKEDARRAY_JOIN__(lhs, rhs)
#define PACKEDARRAY_JOIN__(lhs, rhs)  lhs##rhs
#endif // #ifndef PACKEDARRAY_JOIN

#ifndef PACKEDARRAY_IMPL_BITS_PER_ITEM
#error PACKEDARRAY_IMPL_BITS_PER_ITEM undefined
#endif // #ifndef PACKEDARRAY_IMPL_BITS_PER_ITEM

#if defined(PACKEDARRAY_IMPL_PACK_CASES) || defined(PACKEDARRAY_IMPL_UNPACK_CASES)

#ifndef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 0
#elif PACKEDARRAY_IMPL_CASE_I == 0
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 1
#elif PACKEDARRAY_IMPL_CASE_I == 1
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 2
#elif PACKEDARRAY_IMPL_CASE_I == 2
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 3
#elif PACKEDARRAY_IMPL_CASE_I == 3
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 4
#elif PACKEDARRAY_IMPL_CASE_I == 4
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 5
#elif PACKEDARRAY_IMPL_CASE_I == 5
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 6
#elif PACKEDARRAY_IMPL_CASE_I == 6
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 7
#elif PACKEDARRAY_IMPL_CASE_I == 7
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 8
#elif PACKEDARRAY_IMPL_CASE_I == 8
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 9
#elif PACKEDARRAY_IMPL_CASE_I == 9
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 10
#elif PACKEDARRAY_IMPL_CASE_I == 10
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 11
#elif PACKEDARRAY_IMPL_CASE_I == 11
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 12
#elif PACKEDARRAY_IMPL_CASE_I == 12
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 13
#elif PACKEDARRAY_IMPL_CASE_I == 13
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 14
#elif PACKEDARRAY_IMPL_CASE_I == 14
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 15
#elif PACKEDARRAY_IMPL_CASE_I == 15
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 16
#elif PACKEDARRAY_IMPL_CASE_I == 16
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 17
#elif PACKEDARRAY_IMPL_CASE_I == 17
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 18
#elif PACKEDARRAY_IMPL_CASE_I == 18
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 19
#elif PACKEDARRAY_IMPL_CASE_I == 19
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 20
#elif PACKEDARRAY_IMPL_CASE_I == 20
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 21
#elif PACKEDARRAY_IMPL_CASE_I == 21
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 22
#elif PACKEDARRAY_IMPL_CASE_I == 22
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 23
#elif PACKEDARRAY_IMPL_CASE_I == 23
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 24
#elif PACKEDARRAY_IMPL_CASE_I == 24
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 25
#elif PACKEDARRAY_IMPL_CASE_I == 25
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 26
#elif PACKEDARRAY_IMPL_CASE_I == 26
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 27
#elif PACKEDARRAY_IMPL_CASE_I == 27
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 28
#elif PACKEDARRAY_IMPL_CASE_I == 28
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 29
#elif PACKEDARRAY_IMPL_CASE_I == 29
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 30
#elif PACKEDARRAY_IMPL_CASE_I == 30
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 31
#elif PACKEDARRAY_IMPL_CASE_I == 31
#undef PACKEDARRAY_IMPL_CASE_I
#define PACKEDARRAY_IMPL_CASE_I 32
#endif // #ifndef PACKEDARRAY_IMPL_CASE_I

#ifndef PACKEDARRAY_IMPL_BITS_AVAILABLE
#define PACKEDARRAY_IMPL_BITS_AVAILABLE (32 - ((PACKEDARRAY_IMPL_CASE_I * PACKEDARRAY_IMPL_BITS_PER_ITEM) % 32))
#endif
#ifndef PACKEDARRAY_IMPL_START_BIT
#define PACKEDARRAY_IMPL_START_BIT ((PACKEDARRAY_IMPL_CASE_I * PACKEDARRAY_IMPL_BITS_PER_ITEM) % 32)
#endif
#ifndef PACKEDARRAY_IMPL_MASK
#define PACKEDARRAY_IMPL_MASK (uint32)((1ULL << PACKEDARRAY_IMPL_BITS_PER_ITEM) - 1)
#endif

#if defined(PACKEDARRAY_IMPL_PACK_CASES)

#ifndef PACKEDARRAY_IMPL_PACK_CASE_BREAK
#define PACKEDARRAY_IMPL_PACK_CASE_BREAK
#endif

      case PACKEDARRAY_IMPL_CASE_I:
#if (PACKEDARRAY_IMPL_BITS_PER_ITEM <= PACKEDARRAY_IMPL_BITS_AVAILABLE)
        packed |= *in++ << PACKEDARRAY_IMPL_START_BIT;
#if (PACKEDARRAY_IMPL_BITS_PER_ITEM == PACKEDARRAY_IMPL_BITS_AVAILABLE)
        *out++ = packed;
        packed = 0;
#endif
#else
        packed |= *in << PACKEDARRAY_IMPL_START_BIT;
        *out++ = packed;
        packed = *in++ >> PACKEDARRAY_IMPL_BITS_AVAILABLE;
#endif
        PACKEDARRAY_IMPL_PACK_CASE_BREAK

#if PACKEDARRAY_IMPL_CASE_I < 31
#include PACKEDARRAY_SELF
#else
#undef PACKEDARRAY_IMPL_CASE_I
#undef PACKEDARRAY_IMPL_PACK_CASE_BREAK
#undef PACKEDARRAY_IMPL_PACK_CASES
#endif

#elif defined(PACKEDARRAY_IMPL_UNPACK_CASES) // #if defined(PACKEDARRAY_IMPL_PACK_CASES)

#ifndef PACKEDARRAY_IMPL_UNPACK_CASE_BREAK
#define PACKEDARRAY_IMPL_UNPACK_CASE_BREAK
#endif

      case PACKEDARRAY_IMPL_CASE_I:
#if (PACKEDARRAY_IMPL_BITS_PER_ITEM <= PACKEDARRAY_IMPL_BITS_AVAILABLE)
        *out++ = (packed >> PACKEDARRAY_IMPL_START_BIT) & PACKEDARRAY_IMPL_MASK;
        PACKEDARRAY_IMPL_UNPACK_CASE_BREAK
#if (PACKEDARRAY_IMPL_CASE_I < 31) && (PACKEDARRAY_IMPL_BITS_PER_ITEM == PACKEDARRAY_IMPL_BITS_AVAILABLE)
        packed = *++in;
#endif
#else
        {
          uint32 low, high;
          low = packed >> PACKEDARRAY_IMPL_START_BIT;
          packed = *++in;
          high = packed << PACKEDARRAY_IMPL_BITS_AVAILABLE;

          *out++ = (low | high) & PACKEDARRAY_IMPL_MASK;
        }
        PACKEDARRAY_IMPL_UNPACK_CASE_BREAK
#endif

#if PACKEDARRAY_IMPL_CASE_I < 31
#include PACKEDARRAY_SELF
#else
#undef PACKEDARRAY_IMPL_CASE_I
#undef PACKEDARRAY_IMPL_UNPACK_CASE_BREAK
#undef PACKEDARRAY_IMPL_UNPACK_CASES
#endif

#endif // #elif defined(PACKEDARRAY_IMPL_UNPACK_CASES)

#else // #if defined(PACKEDARRAY_IMPL_PACK_CASES) || defined(PACKEDARRAY_IMPL_UNPACK_CASES)

void PACKEDARRAY_JOIN(__PackedArray_pack_, PACKEDARRAY_IMPL_BITS_PER_ITEM)(uint32* __restrict out, uint32 offset, const uint32* __restrict in, uint32 count)
{
  uint32 startBit;
  uint32 packed;
  const uint32* __restrict end;

  out += ((uint64_t)offset * (uint64_t)PACKEDARRAY_IMPL_BITS_PER_ITEM) / 32;
  startBit = ((uint64_t)offset * (uint64_t)PACKEDARRAY_IMPL_BITS_PER_ITEM) % 32;
  packed = *out & (uint32)((1ULL << startBit) - 1);

  offset = offset % 32;
  if (count >= 32 - offset)
  {
    int32_t n;

    n = (count + offset) / 32;
    count -= 32 * n - offset;
    switch (offset)
    {
      do
      {
#define PACKEDARRAY_IMPL_PACK_CASES
#include PACKEDARRAY_SELF
      } while (--n > 0);
    }

    if (count == 0) {
      return;
    }

    offset = 0;
    startBit = 0;
  }

  end = in + count;
  switch (offset)
  {
#define PACKEDARRAY_IMPL_PACK_CASES
#define PACKEDARRAY_IMPL_PACK_CASE_BREAK \
    if (in == end)\
      break;
#include PACKEDARRAY_SELF
  }
  PACKEDARRAY_ASSERT(in == end);
  if ((count * PACKEDARRAY_IMPL_BITS_PER_ITEM + startBit) % 32)
  {
    packed |= *out & ~((uint32)(1ULL << ((((uint64_t)count * (uint64_t)PACKEDARRAY_IMPL_BITS_PER_ITEM + startBit - 1) % 32) + 1)) - 1);
    *out = packed;
  }
}

void PACKEDARRAY_JOIN(__PackedArray_unpack_, PACKEDARRAY_IMPL_BITS_PER_ITEM)(const uint32* __restrict in, uint32 offset, uint32* __restrict out, uint32 count)
{
  uint32 packed;
  const uint32* __restrict end;

  in += ((uint64_t)offset * (uint64_t)PACKEDARRAY_IMPL_BITS_PER_ITEM) / 32;
  packed = *in;

  offset = offset % 32;
  if (count >= 32 - offset)
  {
    int32_t n;

    n = (count + offset) / 32;
    count -= 32 * n - offset;
    switch (offset)
    {
      do
      {
        packed = *++in;
#define PACKEDARRAY_IMPL_UNPACK_CASES
#include PACKEDARRAY_SELF
      } while (--n > 0);
    }

    if (count == 0) {
      return;
    }

    packed = *++in;
    offset = 0;
  }

  end = out + count;
  switch (offset)
  {
#define PACKEDARRAY_IMPL_UNPACK_CASES
#define PACKEDARRAY_IMPL_UNPACK_CASE_BREAK \
    if (out == end)\
      break;
#include PACKEDARRAY_SELF
  }
  PACKEDARRAY_ASSERT(out == end);
}

#undef PACKEDARRAY_IMPL_BITS_PER_ITEM
#undef PACKEDARRAY_IMPL_BITS_AVAILABLE
#undef PACKEDARRAY_IMPL_START_BIT
#undef PACKEDARRAY_IMPL_START_MASK

#endif // #if defined(PACKEDARRAY_IMPL_PACK_CASES) || defined(PACKEDARRAY_IMPL_UNPACK_CASES)

#else

#include "PackedArray.h"

#define PACKEDARRAY_ASSERT(expression) platform_assert(expression)

#define PACKEDARRAY_IMPL
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 1
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 2
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 3
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 4
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 5
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 6
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 7
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 8
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 9
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 10
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 11
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 12
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 13
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 14
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 15
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 16
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 17
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 18
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 19
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 20
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 21
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 22
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 23
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 24
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 25
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 26
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 27
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 28
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 29
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 30
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 31
#include PACKEDARRAY_SELF
#define PACKEDARRAY_IMPL_BITS_PER_ITEM 32
#include PACKEDARRAY_SELF
#undef PACKEDARRAY_IMPL

#include "poison.h"

#define PACKEDARRAY_MALLOC(size) platform_malloc(size)

void PackedArray_pack(uint32* a, const uint32 offset, const uint32* in, uint32 count, size_t bitsPerItem)
{
  PACKEDARRAY_ASSERT(a != NULL);
  PACKEDARRAY_ASSERT(in != NULL);

  switch (bitsPerItem)
  {
    case 1:   __PackedArray_pack_1(a, offset, in, count); break;
    case 2:   __PackedArray_pack_2(a, offset, in, count); break;
    case 3:   __PackedArray_pack_3(a, offset, in, count); break;
    case 4:   __PackedArray_pack_4(a, offset, in, count); break;
    case 5:   __PackedArray_pack_5(a, offset, in, count); break;
    case 6:   __PackedArray_pack_6(a, offset, in, count); break;
    case 7:   __PackedArray_pack_7(a, offset, in, count); break;
    case 8:   __PackedArray_pack_8(a, offset, in, count); break;
    case 9:   __PackedArray_pack_9(a, offset, in, count); break;
    case 10:  __PackedArray_pack_10(a, offset, in, count); break;
    case 11:  __PackedArray_pack_11(a, offset, in, count); break;
    case 12:  __PackedArray_pack_12(a, offset, in, count); break;
    case 13:  __PackedArray_pack_13(a, offset, in, count); break;
    case 14:  __PackedArray_pack_14(a, offset, in, count); break;
    case 15:  __PackedArray_pack_15(a, offset, in, count); break;
    case 16:  __PackedArray_pack_16(a, offset, in, count); break;
    case 17:  __PackedArray_pack_17(a, offset, in, count); break;
    case 18:  __PackedArray_pack_18(a, offset, in, count); break;
    case 19:  __PackedArray_pack_19(a, offset, in, count); break;
    case 20:  __PackedArray_pack_20(a, offset, in, count); break;
    case 21:  __PackedArray_pack_21(a, offset, in, count); break;
    case 22:  __PackedArray_pack_22(a, offset, in, count); break;
    case 23:  __PackedArray_pack_23(a, offset, in, count); break;
    case 24:  __PackedArray_pack_24(a, offset, in, count); break;
    case 25:  __PackedArray_pack_25(a, offset, in, count); break;
    case 26:  __PackedArray_pack_26(a, offset, in, count); break;
    case 27:  __PackedArray_pack_27(a, offset, in, count); break;
    case 28:  __PackedArray_pack_28(a, offset, in, count); break;
    case 29:  __PackedArray_pack_29(a, offset, in, count); break;
    case 30:  __PackedArray_pack_30(a, offset, in, count); break;
    case 31:  __PackedArray_pack_31(a, offset, in, count); break;
    case 32:  __PackedArray_pack_32(a, offset, in, count); break;
  }
}

void PackedArray_unpack(const uint32* a, const uint32 offset, uint32* out, uint32 count, size_t bitsPerItem)
{
  PACKEDARRAY_ASSERT(a != NULL);
  PACKEDARRAY_ASSERT(out != NULL);

  switch (bitsPerItem)
  {
    case 1:   __PackedArray_unpack_1(a, offset, out, count); break;
    case 2:   __PackedArray_unpack_2(a, offset, out, count); break;
    case 3:   __PackedArray_unpack_3(a, offset, out, count); break;
    case 4:   __PackedArray_unpack_4(a, offset, out, count); break;
    case 5:   __PackedArray_unpack_5(a, offset, out, count); break;
    case 6:   __PackedArray_unpack_6(a, offset, out, count); break;
    case 7:   __PackedArray_unpack_7(a, offset, out, count); break;
    case 8:   __PackedArray_unpack_8(a, offset, out, count); break;
    case 9:   __PackedArray_unpack_9(a, offset, out, count); break;
    case 10:  __PackedArray_unpack_10(a, offset, out, count); break;
    case 11:  __PackedArray_unpack_11(a, offset, out, count); break;
    case 12:  __PackedArray_unpack_12(a, offset, out, count); break;
    case 13:  __PackedArray_unpack_13(a, offset, out, count); break;
    case 14:  __PackedArray_unpack_14(a, offset, out, count); break;
    case 15:  __PackedArray_unpack_15(a, offset, out, count); break;
    case 16:  __PackedArray_unpack_16(a, offset, out, count); break;
    case 17:  __PackedArray_unpack_17(a, offset, out, count); break;
    case 18:  __PackedArray_unpack_18(a, offset, out, count); break;
    case 19:  __PackedArray_unpack_19(a, offset, out, count); break;
    case 20:  __PackedArray_unpack_20(a, offset, out, count); break;
    case 21:  __PackedArray_unpack_21(a, offset, out, count); break;
    case 22:  __PackedArray_unpack_22(a, offset, out, count); break;
    case 23:  __PackedArray_unpack_23(a, offset, out, count); break;
    case 24:  __PackedArray_unpack_24(a, offset, out, count); break;
    case 25:  __PackedArray_unpack_25(a, offset, out, count); break;
    case 26:  __PackedArray_unpack_26(a, offset, out, count); break;
    case 27:  __PackedArray_unpack_27(a, offset, out, count); break;
    case 28:  __PackedArray_unpack_28(a, offset, out, count); break;
    case 29:  __PackedArray_unpack_29(a, offset, out, count); break;
    case 30:  __PackedArray_unpack_30(a, offset, out, count); break;
    case 31:  __PackedArray_unpack_31(a, offset, out, count); break;
    case 32:  __PackedArray_unpack_32(a, offset, out, count); break;
  }
}

void PackedArray_set(uint32* a, const uint32 offset, const uint32 in, size_t bitsPerItem)
{
  uint32* __restrict out;
  uint32 startBit;
  uint32 bitsAvailable;
  uint32 mask;

  PACKEDARRAY_ASSERT(a != NULL);

  out = &a[((uint64_t)offset * (uint64_t)bitsPerItem) / 32];
  startBit = ((uint64_t)offset * (uint64_t)bitsPerItem) % 32;

  bitsAvailable = 32 - startBit;

  mask = (uint32)(1ULL << bitsPerItem) - 1;
  PACKEDARRAY_ASSERT(0 == (~mask & in));

  if (bitsPerItem <= bitsAvailable)
  {
    out[0] = (out[0] & ~(mask << startBit)) | (in << startBit);
  }
  else
  {
    // value spans 2 buffer cells
    uint32 low, high;

    low = in << startBit;
    high = in >> bitsAvailable;

    out[0] = (out[0] & ~(mask << startBit)) | low;

    out[1] = (out[1] & ~(mask >> (32 - startBit))) | high;
  }
}

uint32 PackedArray_get(const uint32* a, const uint32 offset, size_t bitsPerItem)
{
  const uint32* __restrict in;
  uint32 startBit;
  uint32 bitsAvailable;
  uint32 mask;
  uint32 out;

  PACKEDARRAY_ASSERT(a != NULL);

  in = &a[((uint64_t)offset * (uint64_t)bitsPerItem) / 32];
  startBit = ((uint64_t)offset * (uint64_t)bitsPerItem) % 32;

  bitsAvailable = 32 - startBit;

  mask = (uint32)(1ULL << bitsPerItem) - 1;

  if (bitsPerItem <= bitsAvailable)
  {
    out = (in[0] >> startBit) & mask;
  }
  else
  {
    // out spans 2 buffer cells
    uint32 low, high;

    low = in[0] >> startBit;
    high = in[1] << (32 - startBit);

    out = low ^ ((low ^ high) & (mask >> bitsAvailable << bitsAvailable));
  }

  return out;
}

#if !(defined(_MSC_VER) && _MSC_VER >= 1400) && !defined(__GNUC__)
// log base 2 of an integer, aka the position of the highest bit set
static uint32 __PackedArray_log2(uint32 v)
{
  // references
  // http://aggregate.org/MAGIC
  // http://graphics.stanford.edu/~seander/bithacks.html

  static const uint32 multiplyDeBruijnBitPosition[32] =
  {
    0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
    8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
  };

  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;

  return multiplyDeBruijnBitPosition[(uint32)(v * 0x7C4ACDDU) >> 27];
}
#endif
#endif
