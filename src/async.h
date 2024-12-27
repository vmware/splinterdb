// Copyright 2024 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * async.h --
 *
 *     This file contains the tools for implementing and using async functions.
 *
 * The goal of this module is to make it easy to write async functions.  The
 * main procedure for writing an async function is:
 *
 * 1. Write the synchronous version first.
 *
 * 2. Move all the parameters and locals into a state structure.  See the
 * DEFINE_ASYNC_STATE macro below that will generate the structure and an
 * initializer function for you.
 *
 * 3 Rewrite the function to take a single state structure pointer and replace
 * all references to parameters and locals with references to the corresponding
 * fields in the state structure.
 *
 * 4. To call one asynchronous function from another, suspending the caller's
 * execution until the callee  completes, do
 *    async_await_call(your_state, function_to_call,
 *    functions_state_pointer, function_params...);
 * The function_state_pointer will typically be a pointer to a function state
 * structure that is a field of your state structure, e.g.
 *    async_await_call(my_state, function,
 *    &my_state->function_state, ...);
 * async_await_call() will initialize the function's state using the parameters
 * you pass.
 *
 * 5. To call a synchronous (i.e. normal) function from an asynchronous
 * function, just call it as you would normally.
 *
 * async functions can have a result, which will be stored in the __async_result
 * field of their state structure.  Callers can access this result via the
 * async_result macro.
 *
 * Managing execution
 * ------------------
 *
 * There are two general styles of asynchronous functions: polling-based and
 * callback-based.
 *
 * Polling-based functions
 * -----------------------
 *
 * For polling-based functions, you would generally call them from a
 * synchronous function by doing:
 *   function_state_init(&func_state, params...);
 *   while (!async_call(function, &func_state))
 *     do_something_else_or_sleep_or_whatever();
 *
 * Call-back-based functions
 * -------------------------
 *
 * Callback-based async functions are appropriate when you have some way of
 * receiving external notification that the awaited event has occured, and you
 * want to notify your callers that they can now resum execution of your code.
 * One example might be an asynchronous I/O library that calls a callback when
 * I/O completes.
 *
 * Callback-based functions introduce two complications: one at the callee side
 * and one at the caller side.  The callee needs to remember all the function
 * executions that are waiting for an event to occur.  This library includes a
 * simple wait queue mechanism that async function writers can use for this
 * purpose.  You can use async_wait_on_queue to atomically test whether a
 * condition is true and, if not, add your execution to a given wait queue and
 * suspend execution.  See laio.c and clockcache.c for examples.
 *
 * On the caller side, you generally maintain a pool of the states of running
 * function executions, and the callback you pass to your async function simply
 * flags its corresponding execution state as ready to resume execution, either
 * by setting some flag or moving it to a ready-to-run queue.  See the tests for
 * examples.
 *
 * Finally, if you want to call an asynchronous function and simply wait for its
 * completion synchronously, you can use async_call_sync_callback_function. Note
 * this macro assumes that the callback and callback_arg parameters are the last
 * parameters of the asynchronous function's state init method.  There is
 * currently no correpsonding macro for polling-based async functions, but only
 * because we currently have no need for one.
 *
 * Sub-routines
 * ------------
 *
 * Sometimes it is useful to break an asynchronous function into a top-level
 * function that calls several async subroutines.  The straightforward way to do
 * this is to create a state structure for each subroutine and follow the
 * methodology described above.  However, this can be tedious and wasteful.
 * Sometimes it is preferable to simply have all the subroutines use the same
 * state structure as the top-level function.
 *
 * This is fine, except that each subroutine needs its own async_state field to
 * record where it suspended execution.  Thus, the state structure for an
 * asychronous function (or function and collection of subroutines) must have an
 * array of async_states, which are used as a stack.  This is why the
 * DEFINE_ASYNC_STATE has a height parameter -- to specify the maximum height of
 * the stack of subroutines.
 *
 * Thus there are two slightly different types of asynchronous functions:
 * top-level async functions and their subroutines.  Top-level functions take a
 * single parameter -- a pointer to their state stucture.  They should call
 * async_begin with a depth of 0.  Subroutines take a pointer to the state and a
 * depth parameter. To call a subroutine, you can use the async_await_subroutine
 * macro, which will pass the correct depth parameter.
 *
 * The depth parameter cannot be stored in the state structure because doing so
 * would introduce race conditions, as described below.
 *
 * A note on races
 * ---------------
 *
 * One issue to keep in mind when extending this module is to avoid a race
 * condition with callback-based functions.  The issue is that, when an async
 * function suspends execution, it still has to unwind the run-time stack of all
 * its async ancestors.  If that async function saved its state on a wait queue,
 * then its top-level caller could get notified that the function is ready to
 * resume execution betore the original execution finishes unwinding its stack.
 * Then another thread could resume execution of the same async state before the
 * original execution has finished unwinding its stack.  Thus it is imperative
 * that, during the stack unwinding process, async functions must not read or
 * modify their state.  They must simply return to their caller.  See, for
 * example, async_yield_after for more details.
 */

#pragma once

/* Async functions return async_status.  ASYNC_STATUS_RUNNING means that the
 * function has not yet completed.  ASYNC_STATUS_DONE means that the function
 * has completed.  Note that completion does not mean that the function
 * succeeded, e.g. an asynchronous IO function may return DONE after an IO
 * error.  Success/failure is up to the individual function to define. */
typedef enum async_status {
   ASYNC_STATUS_RUNNING,
   ASYNC_STATUS_DONE
} async_status;

/* async_state is used internally to store where the function should resume
 * execution next time it is called. */
typedef void *async_state;
#define ASYNC_STATE_INIT NULL
#define ASYNC_STATE_DONE ((async_state)1)

/*
 * A few macros we need internally.
 */
#define _ASYNC_MERGE_TOKENS(a, b) a##b
#define _ASYNC_MAKE_LABEL(a)      _ASYNC_MERGE_TOKENS(_async_label_, a)
#define _ASYNC_LABEL              _ASYNC_MAKE_LABEL(__LINE__)

/*
 * Macros for implementing async functions.
 */

/* Each asynchronous function has an associated structure that holds all its
 * state -- its parameters, local variables, and async_state.  It is often
 * useful to break an asynchronous function into several simpler async
 * subroutines.  Rather than having to define a separate state structure for
 * each subroutine, we allow several subroutines to share a single state
 * structure.  However, each subroutine needs its own async_state, so we store
 * async_states in a stack within the state structure. */

#define ASYNC_STATE(statep) (statep)->__async_state_stack[__async_depth]

/* You MUST call this at the beginning of an async function. */
#define async_begin(statep, depth)                                             \
   const uint64 __async_depth = (depth);                                       \
   platform_assert(__async_depth < ARRAY_SIZE((statep)->__async_state_stack)); \
   do {                                                                        \
      if (ASYNC_STATE(statep) == ASYNC_STATE_DONE) {                           \
         return ASYNC_STATUS_DONE;                                             \
      } else if (ASYNC_STATE(statep) != ASYNC_STATE_INIT) {                    \
         goto *ASYNC_STATE(statep);                                            \
      }                                                                        \
   } while (0)

/* Call statement and then yield without further modifying our state. This is
 * useful for avoiding races when, e.g. stmt might cause another thread to begin
 * execution using our state. */
#define async_yield_after(statep, stmt)                                        \
   do {                                                                        \
      ASYNC_STATE(statep) = &&_ASYNC_LABEL;                                    \
      stmt;                                                                    \
      return ASYNC_STATUS_RUNNING;                                             \
   _ASYNC_LABEL:                                                               \
   {                                                                           \
   }                                                                           \
   } while (0)

#define async_yield(statep)                                                    \
   do {                                                                        \
      ASYNC_STATE(statep) = &&_ASYNC_LABEL;                                    \
      return ASYNC_STATUS_RUNNING;                                             \
   _ASYNC_LABEL:                                                               \
   {                                                                           \
   }                                                                           \
   } while (0)

/* Supports an optional return value. */
#define async_return(statep, ...)                                              \
   do {                                                                        \
      ASYNC_STATE(statep) = ASYNC_STATE_DONE;                                  \
      __VA_OPT__((statep->__async_result = (__VA_ARGS__)));                    \
      return ASYNC_STATUS_DONE;                                                \
   } while (0)

/* Suspend execution until expr is true. */
#define async_await(statep, expr)                                              \
   do {                                                                        \
      ASYNC_STATE(statep) = &&_ASYNC_LABEL;                                    \
   _ASYNC_LABEL:                                                               \
      if (!(expr)) {                                                           \
         return ASYNC_STATUS_RUNNING;                                          \
      }                                                                        \
   } while (0)

/* Call async function func and suspend execution until it completes. */
#define async_await_call(mystatep, func, funcstatep, ...)                      \
   do {                                                                        \
      func##_state_init(funcstatep __VA_OPT__(, __VA_ARGS__));                 \
      async_await(mystatep, async_call(func, funcstatep));                     \
   } while (0)

#define async_call_subroutine(func, statep, depth)                             \
   (func(statep, depth) == ASYNC_STATUS_DONE)

/* Like async_await_call, but for subroutines.  See comment on subroutines at
 * top of file. */
#define async_await_subroutine(mystatep, func)                                 \
   do {                                                                        \
      (mystatep)->__async_state_stack[__async_depth + 1] = ASYNC_STATE_INIT;   \
      async_await(mystatep,                                                    \
                  async_call_subroutine(func, mystatep, __async_depth + 1));   \
   } while (0)

/* Some async functions may support a callback that can be used to notify the
 * user when it would be useful to continue executing the async function. */
typedef void (*async_callback_fn)(void *);

/*
 * Wait queues for exections awaiting some condition.
 */
typedef struct async_waiter {
   struct async_waiter *next;
   async_callback_fn    callback;
   void                *callback_arg;
} async_waiter;

typedef struct async_wait_queue {
   uint64        lock;
   async_waiter *head;
   async_waiter *tail;
} async_wait_queue;

static inline void
async_wait_queue_init(async_wait_queue *queue)
{
   // memset(queue, 0, sizeof(*queue));
   queue->lock = 0;
   queue->head = NULL;
   queue->tail = NULL;
}

static inline void
async_wait_queue_deinit(async_wait_queue *queue)
{
   // platform_assert(queue->lock == 0);
   // platform_assert(queue->head == NULL);
   // platform_assert(queue->tail == NULL);
}

/* Internal function. */
static inline void
async_wait_queue_lock(async_wait_queue *q)
{
   while (__sync_lock_test_and_set(&q->lock, 1)) {
      // FIXME: Should be platform_pause() but cannot include platform_inline.h
      // here due to circular dependency induced by leakage of laio.h
      __builtin_ia32_pause();
   }
}

/* Internal function. */
static inline void
async_wait_queue_unlock(async_wait_queue *q)
{
   __sync_lock_release(&q->lock);
}

/* Internal function. */
static inline void
async_wait_queue_append(async_wait_queue *q,
                        async_waiter     *waiter,
                        async_callback_fn callback,
                        void             *callback_arg)
{
   waiter->callback     = callback;
   waiter->callback_arg = callback_arg;
   waiter->next         = NULL;

   if (q->head == NULL) {
      q->head = waiter;
   } else {
      q->tail->next = waiter;
   }
   q->tail = waiter;
}

/* Public: notify one waiter that the condition has become true. */
static inline void
async_wait_queue_release_one(async_wait_queue *q)
{
   async_waiter *waiter;

   async_wait_queue_lock(q);

   waiter = q->head;
   if (waiter) {
      q->head = waiter->next;
      if (q->head == NULL) {
         q->tail = NULL;
      }
   }
   async_wait_queue_unlock(q);

   if (waiter) {
      waiter->callback(waiter->callback_arg);
   }
}

/* Public: notify all waiters that the condition has become true. */
static inline void
async_wait_queue_release_all(async_wait_queue *q)
{
   async_waiter *waiter;

   async_wait_queue_lock(q);
   waiter  = q->head;
   q->head = NULL;
   q->tail = NULL;
   async_wait_queue_unlock(q);

   while (waiter != NULL) {
      async_waiter *next = waiter->next;
      waiter->callback(waiter->callback_arg);
      waiter = next;
   }
}

/* Public: Wait on the queue until the predicate <ready> evaluates to true.
 * There is a subtle race condition that this code avoids.  This code checks
 * <ready> without holding any locks.  If <ready> is not true, then it locks the
 * wait queue and checks again.  By checking again with lock held, this code
 * avoids the race where <ready> becomes true and all waiters get notified
 * between the time that we check the condition (w/o locks) and add ourselves to
 * the queue.
 */
#define async_wait_on_queue(ready, state, queue, node, callback, callback_arg) \
   do {                                                                        \
      if (!(ready)) {                                                          \
         do {                                                                  \
            async_wait_queue_lock(queue);                                      \
            if (!(ready)) {                                                    \
               async_wait_queue_append(queue, node, callback, callback_arg);   \
               async_yield_after(state, async_wait_queue_unlock(queue));       \
            } else {                                                           \
               async_wait_queue_unlock(queue);                                 \
            }                                                                  \
         } while (!(ready));                                                   \
      }                                                                        \
   } while (0)

/*
 * Macros for calling async functions.
 */

#define async_call(func, statep) (((func)(statep)) == ASYNC_STATUS_DONE)
#define async_result(statep)     ((statep)->__async_result)

static inline void
async_call_sync_callback_function(void *arg)
{
   int *ready = (int *)arg;
   *ready     = TRUE;
}

/* Call an async function and wait for it to finish. <wait> is code to be
 * executed in a loop until the async function finishes. */
#define async_call_sync_callback(wait, async_func, ...)                        \
   ({                                                                          \
      async_func##_state __async_state;                                        \
      int                __async_ready = FALSE;                                \
      async_func##_state_init(&__async_state,                                  \
                              __VA_OPT__(__VA_ARGS__, )                        \
                                 async_call_sync_callback_function,            \
                              &__async_ready);                                 \
      while (!async_call(async_func, &__async_state)) {                        \
         while (!__async_ready) {                                              \
            wait;                                                              \
         }                                                                     \
         __async_ready = FALSE;                                                \
      }                                                                        \
      async_result(&__async_state);                                            \
   })

/* Macros for defining the state structures and initialization functions of
 * asynchronous functions. */

#define DEFINE_STATE_STRUCT_FIELDS0(kind, type, name) type name;
#define DEFINE_STATE_STRUCT_FIELDS1(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS0(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS2(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS1(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS3(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS2(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS4(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS3(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS5(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS4(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS6(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS5(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS7(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS6(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS8(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS7(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS9(kind, type, name, ...)                     \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS8(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS10(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS9(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS11(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS10(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS12(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS11(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS13(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS12(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS14(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS13(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS15(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS14(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS16(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS15(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS17(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS16(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS18(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS17(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS19(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS18(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS20(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS19(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS21(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS20(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS22(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS21(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS23(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS22(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS24(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS23(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS25(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS24(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS26(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS25(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS27(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS26(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS28(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS27(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS29(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS28(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS30(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS29(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS31(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS30(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS32(kind, type, name, ...)                    \
   type name;                                                                  \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS31(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_FIELDS(...)                                        \
   __VA_OPT__(DEFINE_STATE_STRUCT_FIELDS32(__VA_ARGS__))

#define DEFINE_STATE_STRUCT_INIT_param(type, name) , type name
#define DEFINE_STATE_STRUCT_INIT_local(type, name)

#define DEFINE_STATE_STRUCT_INIT_PARAMS0(kind, type, name)                     \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)
#define DEFINE_STATE_STRUCT_INIT_PARAMS1(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS0(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS2(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS1(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS3(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS2(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS4(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS3(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS5(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS4(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS6(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS5(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS7(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS6(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS8(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS7(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS9(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS8(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS10(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS9(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS11(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS10(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS12(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS11(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS13(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS12(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS14(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS13(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS15(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS14(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS16(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS15(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS17(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS16(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS18(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS17(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS19(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS18(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS20(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS19(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS21(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS20(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS22(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS21(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS23(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS22(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS24(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS23(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS25(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS24(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS26(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS25(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS27(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS26(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS28(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS27(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS29(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS28(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS30(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS29(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS31(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS30(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS32(kind, type, name, ...)               \
   DEFINE_STATE_STRUCT_INIT_##kind(type, name)                                 \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS31(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_PARAMS(...)                                   \
   __VA_OPT__(DEFINE_STATE_STRUCT_INIT_PARAMS32(__VA_ARGS__))


#define DEFINE_STATE_STRUCT_INIT_STMT_param(type, name) __state->name = name;
#define DEFINE_STATE_STRUCT_INIT_STMT_local(type, name)

#define DEFINE_STATE_STRUCT_INIT_STMTS0(kind, type, name)                      \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)
#define DEFINE_STATE_STRUCT_INIT_STMTS1(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS0(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS2(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS1(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS3(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS2(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS4(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS3(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS5(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS4(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS6(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS5(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS7(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS6(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS8(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS7(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS9(kind, type, name, ...)                 \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS8(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS10(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS9(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS11(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS10(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS12(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS11(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS13(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS12(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS14(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS13(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS15(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS14(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS16(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS15(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS17(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS16(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS18(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS17(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS19(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS18(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS20(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS19(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS21(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS20(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS22(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS21(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS23(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS22(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS24(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS23(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS25(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS24(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS26(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS25(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS27(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS26(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS28(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS27(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS29(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS28(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS30(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS29(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS31(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS30(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS32(kind, type, name, ...)                \
   DEFINE_STATE_STRUCT_INIT_STMT_##kind(type, name)                            \
      __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS31(__VA_ARGS__))
#define DEFINE_STATE_STRUCT_INIT_STMTS(...)                                    \
   __VA_OPT__(DEFINE_STATE_STRUCT_INIT_STMTS32(__VA_ARGS__))


#define DEFINE_ASYNC_STATE(name, height, ...)                                  \
   _Static_assert(0 < height, "height must be greater than 0");                \
   typedef struct name {                                                       \
      async_state __async_state_stack[height];                                 \
      DEFINE_STATE_STRUCT_FIELDS(__VA_ARGS__)                                  \
   } name;                                                                     \
   static inline void name##_init(                                             \
      name *__state DEFINE_STATE_STRUCT_INIT_PARAMS(__VA_ARGS__))              \
   {                                                                           \
      __state->__async_state_stack[0] = ASYNC_STATE_INIT;                      \
      DEFINE_STATE_STRUCT_INIT_STMTS(__VA_ARGS__)                              \
   }
