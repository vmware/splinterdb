#ifndef _LOCK_H_
#define _LOCK_H_

#include <stdlib.h>
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>

#include "partitioned_counter.h"

#ifdef __cplusplus
#   define __restrict__
extern "C" {
#endif

#define NO_LOCK       (0x01)
#define TRY_ONCE_LOCK (0x02)
#define WAIT_FOR_LOCK (0x04)

#define GET_NO_LOCK(flag)       (flag & NO_LOCK)
#define GET_TRY_ONCE_LOCK(flag) (flag & TRY_ONCE_LOCK)
#define GET_WAIT_FOR_LOCK(flag) (flag & WAIT_FOR_LOCK)


typedef struct ReaderWriterLock {
   int64_t      readers;
   volatile int writer;
   pc_t         pc_counter;
} ReaderWriterLock;

bool
lock(volatile int *var, uint8_t flag);

void
unlock(volatile int *var);

void
rw_lock_init(ReaderWriterLock *rwlock);

bool
read_lock(ReaderWriterLock *rwlock, uint8_t flag, uint8_t thread_id);

void
read_unlock(ReaderWriterLock *rwlock, uint8_t thread_id);

bool
write_lock(ReaderWriterLock *rwlock, uint8_t flag);

void
write_unlock(ReaderWriterLock *rwlock);

#ifdef __cplusplus
}
#endif


#endif
