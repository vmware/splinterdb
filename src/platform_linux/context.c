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
	    ctx->lock_curr = 0;
        }
	for(int i = 0; i < MAX_LOCKS; i++){
	    ctx->delayed_array[i] = -1;
	    ctx->write_array[i] = FALSE;
	    ctx->get_array[i] = FALSE;
	    ctx->claim_array[i] = FALSE;
	    ctx->old_page_persistent[i] = TRUE;
	    ctx->entry_array[i] = -1;
	    ctx->addr_array[i] = -1;
	}
    }
}

ThreadContext *get_context(ThreadContext *contextMap, threadid idx) {
   ThreadContext *ctx = &contextMap[idx];
   assert(ctx!=NULL);
   assert(ctx->id == (uint64_t)idx);
   return ctx;
}


int ctx_lock(ThreadContext *ctx){
   ctx->locksHeld++;
   if(!ctx->trackTxs){
      return 1;
   }
   //assert(ctx->endTxs == 0);
   if (ctx->openTxs == 0) {
      //tx_open(ctx);
      ctx->openTxs++;
   }
   return 0;
}


int unlockall_or_unlock_delay(ThreadContext *ctx){
//    if(!ctx->trackTxs){
	//assert(!isinTX(ctx));
	return NONTXUNLOCK;
//    }
    if (ctx->locksHeld == 0 && ctx->endTxs == 0) {
        return UNLOCKALL;
    }
    else
	return UNLOCKDELAY;
}


int ctx_unlock(ThreadContext *ctx){
    if(ctx->locksHeld > 0)
        ctx->locksHeld--;
    //assert(ctx->locksHeld >= 0);
    if(!ctx->trackTxs)
      return 1;
    if (ctx->openTxs>0){
	ctx->openTxs--;
	ctx->endTxs++;
    }
    if (ctx->locksHeld == 0 && ctx->endTxs > 0) {
        //tx_commit(ctx);
        ctx->endTxs--;
    }
    return 0;
}



void start_nontx(ThreadContext *ctx){
   //assert(ctx->trackTxs);

//   assert(ctx->lock_curr == 0);
//   assert(!isinTX(ctx));
   ctx->trackTxnum++;
    
   ctx->trackTxs = FALSE;
}

void start_nontx_print(ThreadContext *ctx, threadid idx){
	start_nontx(ctx);
}


void start_nontx_withlocks(ThreadContext *ctx){
   ctx->trackTxnum++;

   ctx->trackTxs = FALSE;
}


void end_nontx(ThreadContext *ctx){
   //assert(!ctx->trackTxs);
//   assert(!isinTX(ctx));
   ctx->trackTxnum--;
   if(ctx->trackTxnum == 0)   
   ctx->trackTxs = TRUE;
#ifdef PMEM_FLUSH
   pmem_drain();
#endif
}


void end_nontx_withlocks(ThreadContext *ctx){
   ctx->trackTxnum--;
   if(ctx->trackTxnum == 0)
   ctx->trackTxs = TRUE;
}


bool istracking(ThreadContext *ctx){
   return ctx->trackTxs;
}

bool isinTX(ThreadContext *ctx){
   if(ctx->endTxs!=0)
      return TRUE;
   else
      return FALSE;
}
