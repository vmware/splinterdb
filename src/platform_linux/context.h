#ifndef CONTEXT_H
#define CONTEXT_H

#include "platform.h"


#define UNLOCKALL            (1u<<0)
#define UNLOCKDELAY          (1u<<1)
#define NONTXUNLOCK	     (1u<<2)


typedef struct {
    uint64_t id;
    int32_t locksHeld;
    uint32_t bytesAllocated;
    uint64_t openTxs;
    uint64_t endTxs;
    uint64_t commitTxs;

    // debug statistics
    uint64_t bytesWritten;
    uint64_t mallocs;
    uint64_t frees;

    bool trackTxs;
    uint64_t trackTxnum;

    uint32 entry_array[MAX_LOCKS];
    bool write_array[MAX_LOCKS];
    bool claim_array[MAX_LOCKS];
    bool get_array[MAX_LOCKS];
    uint32 delayed_array[MAX_LOCKS];
    uint32 lock_curr;

    uint64_t reserved[2];
} ThreadContext;

void create_context(ThreadContext *contextMap);

ThreadContext *get_context(ThreadContext *contextMap, threadid idx);

int ctx_lock(ThreadContext *ctx);
int ctx_unlock(ThreadContext *ctx);

int unlockall_or_unlock_delay(ThreadContext *ctx);

void start_nontx(ThreadContext *ctx);
void end_nontx(ThreadContext *ctx);

void start_nontx_print(ThreadContext *ctx, threadid idx);


void start_nontx_withlocks(ThreadContext *ctx);
void end_nontx_withlocks(ThreadContext *ctx);

bool istracking(ThreadContext *ctx);

bool isinTX(ThreadContext *ctx);
#endif // CONTEXT_H
