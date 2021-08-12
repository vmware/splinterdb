#include <stdint.h>
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "context.h"
/*
 * 1. Thread Context Management
 */

#ifdef NVM_STATS
static uint64_t bytesWritten = 0;
static uint64_t totalMallocs = 0;
static uint64_t totalFrees = 0;
#endif



void create_context(ThreadContext *contextMap) {
    printf("Creating context, max thread = %u \n", MAX_THREADS);
    for (uint64_t i = 0; i < MAX_THREADS; i++) {
        ThreadContext *ctx = &contextMap[i];
        if (__sync_bool_compare_and_swap(&ctx->id, 0, i)) {
            ctx->locksHeld = 0;
            ctx->bytesAllocated = 0;
            ctx->openTxs = 0;
	    ctx->endTxs = 0;
            ctx->bytesWritten = 0;
            ctx->mallocs = 0;
            ctx->frees = 0;
	    ctx->trackTxs = TRUE;
	    ctx->trackTxnum = 0;
        }
    }
}

ThreadContext *get_context(ThreadContext *contextMap, threadid idx) {
   ThreadContext *ctx = &contextMap[idx % MAX_THREADS];
   assert(ctx!=NULL);
   if (ctx->id == (uint64_t)idx) 
	   return ctx;
   else
	   return NULL;
}


int ctx_lock(ThreadContext *contextMap, threadid idx){
   ThreadContext *ctx = get_context(contextMap, idx);
   ctx->locksHeld++;

   if(!ctx->trackTxs){
      return 1;
   }

   if(ctx->endTxs != 0)
	   printf("ctx in context.c= %p, threadid = %lu \n", ctx, idx);


   assert(ctx->endTxs == 0);
   if (ctx->openTxs == 0) {
      //tx_open(ctx);
      ctx->openTxs++;
   }

   return 0;
}

void release_all_locks(ThreadContext *ctx){}

void add_unlock_delay(ThreadContext *contextMap, uint32 entry_number){}

int ctx_unlock(ThreadContext *contextMap, threadid idx, uint32 entry_number){
    ThreadContext *ctx = get_context(contextMap, idx);

    if(ctx->locksHeld > 0)
        ctx->locksHeld--;
    assert(ctx->locksHeld >= 0);

   if(!ctx->trackTxs)
      return 1;

    if (ctx->openTxs>0){
	ctx->openTxs--;
	ctx->endTxs++;
    }

    if (ctx->locksHeld == 0 && ctx->endTxs > 0) {
        //tx_commit(ctx);
        ctx->endTxs--;
	release_all_locks(ctx);
    }
    else
        add_unlock_delay(ctx, entry_number);

    return 0;
}



void start_nontx(ThreadContext *ctx){
   //assert(ctx->trackTxs);

   ctx->trackTxnum++;
    
   ctx->trackTxs = FALSE;
}

void start_nontx_print(ThreadContext *ctx, threadid idx){
	start_nontx(ctx);
}


void end_nontx(ThreadContext *ctx){
   //assert(!ctx->trackTxs);
  
   ctx->trackTxnum--;
   if(ctx->trackTxnum == 0)   
   ctx->trackTxs = TRUE;
}


bool istracking(ThreadContext *ctx){
	return ctx->trackTxs;
}

