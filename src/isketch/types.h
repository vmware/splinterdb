#ifndef _TYPES_H_
#define _TYPES_H_

#ifdef __cplusplus
#   define __restrict__
extern "C" {
#endif

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>

#define KEY_SIZE 24

typedef char *KeyType;

// typedef struct {
//    uint64_t value : 58;
//    uint64_t refcount : 6;
// } ValueType;

// typedef uint64_t ValueType;
typedef unsigned __int128 ValueType;

#ifdef __cplusplus
}
#endif

#endif