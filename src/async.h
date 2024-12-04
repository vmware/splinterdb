// Copyright 2024 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * async.h --
 *
 *     This file contains the tools for implementing and using async functions.
 */

#pragma once

typedef void *async_state;
#define ASYNC_STATE_INIT NULL
#define ASYNC_STATE_DONE ((async_state)1)

/*
 * A few macros we need internally.
 */
#define _ASYNC_MERGE_TOKENS(a, b) a##b
#define _ASYNC_MAKE_LABEL(a)      _ASYNC_MERGE_TOKENS(_async_label_, a)
#define _ASYNC_LABEL              _ASYNC_MAKE_LABEL(__LINE__)

#ifdef __clang__
#   define WARNING_STATE_PUSH _Pragma("clang diagnostic push")
#   define WARNING_STATE_POP  _Pragma("clang diagnostic pop")
#   define WARNING_IGNORE_DANGLING_LABEL_POINTER
#elif defined(__GNUC__)
#   define WARNING_STATE_PUSH _Pragma("GCC diagnostic push")
#   define WARNING_STATE_POP  _Pragma("GCC diagnostic pop")
#   define WARNING_IGNORE_DANGLING_LABEL_POINTER                               \
      _Pragma("GCC diagnostic ignored \"-Wdangling-pointer\"")
#endif

/*
 * Macros for implementing async functions.
 */

// We declare a dummy local variable in async_begin.  We then reference this
// variable in all our other macros.  This ensures that the user cannot forget
// to call async_begin before calling any other async macros.  It also ensures
// that they cannot call async_begin twice.
#define ENSURE_ASYNC_BEGIN                                                     \
   do {                                                                        \
   } while (0 && __async_dummy)

#define async_begin(statep)                                                    \
   int __async_dummy;                                                          \
   do {                                                                        \
      async_state *_async_state_p = &(statep)->__async_state;                  \
      if (*_async_state_p == ASYNC_STATE_DONE) {                               \
         return ASYNC_STATE_DONE;                                              \
      } else if (*_async_state_p != ASYNC_STATE_INIT) {                        \
         goto **_async_state_p;                                                \
      }                                                                        \
   } while (0)

#define async_yield_after(statep, stmt)                                        \
   ENSURE_ASYNC_BEGIN;                                                         \
   do {                                                                        \
      WARNING_STATE_PUSH                                                       \
      WARNING_IGNORE_DANGLING_LABEL_POINTER(statep)->__async_state =           \
         &&_ASYNC_LABEL;                                                       \
      stmt;                                                                    \
      return (statep)->__async_state;                                          \
   _ASYNC_LABEL:                                                               \
   {}                                                                          \
      WARNING_STATE_POP                                                        \
   } while (0)


#define async_yield(statep)                                                    \
   ENSURE_ASYNC_BEGIN;                                                         \
   do {                                                                        \
      WARNING_STATE_PUSH                                                       \
      WARNING_IGNORE_DANGLING_LABEL_POINTER(statep)->__async_state =           \
         &&_ASYNC_LABEL;                                                       \
      return (statep)->__async_state;                                          \
   _ASYNC_LABEL:                                                               \
   {}                                                                          \
      WARNING_STATE_POP                                                        \
   } while (0)

#define async_return(statep, ...)                                              \
   ENSURE_ASYNC_BEGIN;                                                         \
   do {                                                                        \
      (statep)->__async_state = ASYNC_STATE_DONE;                              \
      __VA_OPT__((statep->__async_result = (__VA_ARGS__)));                    \
      return ASYNC_STATE_DONE;                                                 \
   } while (0)

#define async_await(statep, expr)                                              \
   ENSURE_ASYNC_BEGIN;                                                         \
   do {                                                                        \
      WARNING_STATE_PUSH                                                       \
      WARNING_IGNORE_DANGLING_LABEL_POINTER(statep)->__async_state =           \
         &&_ASYNC_LABEL;                                                       \
   _ASYNC_LABEL:                                                               \
      WARNING_STATE_POP                                                        \
      if (!(expr)) {                                                           \
         return statep->__async_state;                                         \
      }                                                                        \
   } while (0)

#define async_await_call(mystatep, func, funcstatep, ...)                      \
   do {                                                                        \
      func##_state_init(funcstatep __VA_OPT__(, __VA_ARGS__));                 \
      async_await(mystatep, async_call(func, funcstatep));                     \
   } while (0)


/* Some async functions may support a callback that can be used to notify the
 * user when it would be useful to continue executing the async function. */
typedef void (*async_callback_fn)(void *);

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
async_wait_queue_lock(async_wait_queue *q)
{
   while (__sync_lock_test_and_set(&q->lock, 1)) {
      // FIXME: Should be platform_pause() but cannot include platform_inline.h
      // here due to circular dependency induced by leakage of laio.h
      __builtin_ia32_pause();
   }
}

static inline void
async_wait_queue_unlock(async_wait_queue *q)
{
   __sync_lock_release(&q->lock);
}

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

static inline void
async_wait_queue_release_all(async_wait_queue *q)
{
   async_waiter *waiter;

   async_wait_queue_lock(q);

   while ((waiter = q->head)) {
      q->head = waiter->next;
      waiter->callback(waiter->callback_arg);
   }
   q->tail = NULL;

   async_wait_queue_unlock(q);
}

/*
 * Macros for calling async functions.
 */

#define async_call(func, statep) (((func)(statep)) == ASYNC_STATE_DONE)

#define async_done(statep) ((statep)->__async_state == ASYNC_STATE_DONE)

#define async_result(statep) ((statep)->__async_result)

void
async_call_sync_callback_function(void *arg);

#define async_call_sync_callback(io, hid, async_func, ...)                     \
   ({                                                                          \
      async_func##_state __async_state;                                        \
      bool32             __async_ready = FALSE;                                \
      async_func##_state_init(&__async_state,                                  \
                              __VA_OPT__(__VA_ARGS__, )                        \
                                 async_call_sync_callback_function,            \
                              &__async_ready);                                 \
      while (!async_call(async_func, &__async_state)) {                        \
         while (!__async_ready) {                                              \
            io_cleanup(io, 1);                                                 \
         }                                                                     \
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


#define DEFINE_ASYNC_STATE(name, ...)                                          \
   typedef struct name##_state {                                               \
      async_state __async_state;                                               \
      DEFINE_STATE_STRUCT_FIELDS(__VA_ARGS__)                                  \
   } name##_state;                                                             \
   void name##_state_init(                                                     \
      name##_state *__state DEFINE_STATE_STRUCT_INIT_PARAMS(__VA_ARGS__))      \
   {                                                                           \
      __state->__async_state = ASYNC_STATE_INIT;                               \
      DEFINE_STATE_STRUCT_INIT_STMTS(__VA_ARGS__)                              \
   }
