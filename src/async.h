typedef void * async_state;
#define ASYNC_STATE_INIT NULL
#define ASYNC_STATE_DONE ((async_state)1)

/*
 * A few macros we need internally.
 */
#define _ASYNC_MERGE_TOKENS(a, b) a##b
#define _ASYNC_MAKE_LABEL(a) _ASYNC_MERGE_TOKENS(_async_label_, a)
#define _ASYNC_LABEL _ASYNC_MAKE_LABEL(__LINE__)

#ifdef __clang__
#define WARNING_STATE_PUSH _Pragma("clang diagnostic push")
#define WARNING_STATE_POP _Pragma("clang diagnostic pop")
#define WARNING_IGNORE_DANGLING_LABEL_POINTER
#elif defined(__GNUC__)
#define WARNING_STATE_PUSH _Pragma("GCC diagnostic push")
#define WARNING_STATE_POP _Pragma("GCC diagnostic pop")
#define WARNING_IGNORE_DANGLING_LABEL_POINTER _Pragma("GCC diagnostic ignored \"-Wdangling-pointer\"")
#endif

/*
 * Macros for implementing async functions.
 */

#define async_begin(statep) \
    do { \
        async_state *_async_state_p = (async_state *)(statep); \
        if (*_async_state_p == ASYNC_STATE_DONE) { \
            return; \
        } else if (*_async_state_p != ASYNC_STATE_INIT) { \
            goto **_async_state_p; \
        } \
    } while (0)

#define async_end(statep) \
    do {\
        *((async_state *)(statep)) = ASYNC_STATE_DONE; \
        return; \
    } while (0)

#define async_yield(statep) \
    do {\
        WARNING_STATE_PUSH \
        WARNING_IGNORE_DANGLING_LABEL_POINTER \
        *((async_state *)(statep)) = &&_ASYNC_LABEL; return; _ASYNC_LABEL: {}\
        WARNING_STATE_POP \
    } while (0)

#define async_await(statep, expr) \
    do { \
      WARNING_STATE_PUSH \
      WARNING_IGNORE_DANGLING_LABEL_POINTER \
      *((async_state *)(statep)) = &&_ASYNC_LABEL; _ASYNC_LABEL:\
      WARNING_STATE_POP \
      if (!(expr)) { return; } \
    } while (0)

#define async_exit(statep) \
    do { *((async_state *)(statep)) = ASYNC_STATE_DONE; return; } while (0)

/*
 * Macros for calling async functions.
 */

#define async_init(statep) \
    do { *((async_state *)(statep)) = ASYNC_STATE_INIT; } while (0)

#define async_deinit(statep)

#define async_done(statep) \
    (*((async_state *)(statep)) == ASYNC_STATE_DONE)

#define async_call(func, statep) (((func)(statep)), async_done(statep))