#pragma once

#include <stddef.h>
#include <stdlib.h>

/*
 * The comparator follows the same conventions as that of qsort(3). Ie. if:
 * a>b: return 1
 * a<b: return -1
 * a==b: return 0
 * the array is sorted in increasing order.
 */

typedef int (*platform_sort_cmpfn)(const void *a, const void *b, void *arg);

static inline void
platform_sort_slow(void               *base,
                   size_t              nmemb,
                   size_t              size,
                   platform_sort_cmpfn cmpfn,
                   void               *cmparg,
                   void               *temp)
{
   return qsort_r(base, nmemb, size, cmpfn, cmparg);
}
