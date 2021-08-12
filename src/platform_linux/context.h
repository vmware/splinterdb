#ifndef CONTEXT_H
#define CONTEXT_H

#include "platform.h"

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

    uint64_t reserved[2];
} ThreadContext;

void create_context(ThreadContext *contextMap);

ThreadContext *get_context(ThreadContext *contextMap, threadid idx);

int ctx_lock(ThreadContext *contextMap, threadid idx);
int ctx_unlock(ThreadContext *contextMap, threadid idx, uint32 entry_number);

void start_nontx(ThreadContext *ctx);
void end_nontx(ThreadContext *ctx);

void start_nontx_print(ThreadContext *ctx, threadid idx);

bool istracking(ThreadContext *ctx);
#endif // CONTEXT_H
