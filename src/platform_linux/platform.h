// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef PLATFORM_H
#define PLATFORM_H

#include "splinterdb/public_platform.h"

/*
 * Platform directory is chosen via -I include options to compiler.
 * e.g. linux compile includes platform_linux directory
 * Sections:
 * 1- Shared types/typedefs (or #includes) that don't rely on anything
 *    platform-specific
 * 2- (#include)Platform-specific types/typedefs
 * 3- Shared types/typedefs that rely on platform types
 *    There should not be any generic #includes here, but potentially #includes
 *    of headers defined in splinterdb cannot be included until now.
 * 4- Shared function prototypes.
 *    Shared inline function prototypes (including prototypes for the
 *       platform-specific inline functions).
 * 5- (#include)Implementation of platform-specific inline functions
 * 6- Implementations of shared inline functions that aren't platform specific
 *       (These may be candidates to move outside of platform.h entirely)
 * 7- (Not actually in this header)
 *       Non-inline platform-specific function implementations belong in
 *       <platform_dir>/<whatever>.c
 */

#define PLATFORM_LINUX

/*
 * Section 1:
 * Shared types/typedefs that don't rely on anything platform-specific
 */
#if !defined(__cplusplus)
typedef int32 bool;
#endif

#if !defined(SPLINTER_DEBUG)
#   define SPLINTER_DEBUG 0
#else
#   if SPLINTER_DEBUG != 0 && SPLINTER_DEBUG != 1
#      error SPLINTER_DEBUG not 0 or 1
#   endif
#endif

// Helper function so that ARRAYSIZE can be used inside of _Static_assert
#define ASSERT_EXPR(condition, return_value)                                   \
   (sizeof(char[(condition) ? 1 : -1]) ? (return_value) : (return_value))

/*
 * Utility macro to test if an indexable expression is an array.
 * Gives an error at compile time if the expression is not indexable.
 * Only pointers and arrays are indexable.
 * Expression value is 0 for pointer and 1 for array
 */
#define IS_ARRAY(x)                                                            \
   __builtin_choose_expr(                                                      \
      __builtin_types_compatible_p(typeof((x)[0])[], typeof(x)), 1, 0)

/*
 * Errors at compile time if you use ARRAY_SIZE() on a pointer.
 * ARRAY_SIZE can be used inside of _Static_assert.
 */
#define ARRAY_SIZE(x) ASSERT_EXPR(IS_ARRAY(x), (sizeof(x) / sizeof((x)[0])))

/*
 * MAX_THREADS is used primarily for convenience, where allocations made on a
 * per-thread basis create an array with MAX_THREADS items, e.g. the
 * trunk_stats field in trunk_handle. The task subsystem also uses a 64-bit
 * bit-array to track thread IDs in use. This could be changed relatively
 * easily if needed.
 */
#define MAX_THREADS (64)
#define INVALID_TID (MAX_THREADS)

#define HASH_SEED (42)

/*
 * C11 and higher already supports native _Static_assert which has good
 * compiler output on errors
 * Since we are lower than C11, we need something that works.
 */
#if !defined(__STDC_VERSION__) || __STDC_VERSION__ < 201112l
#   define _Static_assert(expr, str)                                           \
      do {                                                                     \
         typedef char oc_assert_fail[((expr) ? 1 : -1)]                        \
            __attribute__((__unused__));                                       \
      } while (0)
#endif

// Data unit constants
#define KiB (1024UL)
#define MiB (KiB * 1024)
#define GiB (MiB * 1024)
#define TiB (GiB * 1024)

// Convert 'x' in unit-specifiers to bytes
#define KiB_TO_B(x) ((x)*KiB)
#define MiB_TO_B(x) ((x)*MiB)
#define GiB_TO_B(x) ((x)*GiB)
#define TiB_TO_B(x) ((x)*TiB)

// Convert 'x' in bytes to 'int'-value with unit-specifiers
#define B_TO_KiB(x) ((x) / KiB)
#define B_TO_MiB(x) ((x) / MiB)
#define B_TO_GiB(x) ((x) / GiB)
#define B_TO_TiB(x) ((x) / TiB)

// For x bytes, returns as int the fractional portion modulo a unit-specifier
#define B_TO_KiB_FRACT(x) ((100 * ((x) % KiB)) / KiB)
#define B_TO_MiB_FRACT(x) ((100 * ((x) % MiB)) / MiB)
#define B_TO_GiB_FRACT(x) ((100 * ((x) % GiB)) / GiB)
#define B_TO_TiB_FRACT(x) ((100 * ((x) % TiB)) / TiB)

// Time unit constants
#define THOUSAND (1000UL)
#define MILLION  (THOUSAND * THOUSAND)
#define BILLION  (THOUSAND * MILLION)

#define USEC_TO_SEC(x)  ((x) / MILLION)
#define USEC_TO_NSEC(x) ((x)*THOUSAND)
#define NSEC_TO_SEC(x)  ((x) / BILLION)
#define NSEC_TO_MSEC(x) ((x) / MILLION)
#define NSEC_TO_USEC(x) ((x) / THOUSAND)
#define SEC_TO_MSEC(x)  ((x)*THOUSAND)
#define SEC_TO_USEC(x)  ((x)*MILLION)
#define SEC_TO_NSEC(x)  ((x)*BILLION)

#define MAX_STRING_LENGTH 256

typedef void (*platform_thread_worker)(void *);

/*
 * The comparator follows the same conventions as that of qsort(3). Ie. if:
 * a>b: return 1
 * a<b: return -1
 * a==b: return 0
 * the array is sorted in increasing order.
 */

typedef int (*platform_sort_cmpfn)(const void *a, const void *b, void *arg);


/*
 * Helper macro that takes a pointer, type of the container, and the
 * name of the member the pointer refers to. The macro expands to a
 * new address pointing to the container which accomodates the
 * specified  member.
 */
#ifndef container_of
#   define container_of(ptr, type, memb)                                       \
      ((type *)((char *)(ptr)-offsetof(type, memb)))
#endif

/*
 * Section 2:
 * Platform-specific types/typedefs
 */
#include <platform_types.h>

// Platform status
typedef struct {
   internal_platform_status r;
} platform_status;

#define CONST_STATUS(status) ((const platform_status){.r = status})

typedef struct {
   uint64 v;
} PLATFORM_CACHELINE_ALIGNED cache_aligned_uint64;
_Static_assert(sizeof(cache_aligned_uint64) == PLATFORM_CACHELINE_SIZE,
               "Attribute set wrong");
typedef struct {
   uint32 v;
} PLATFORM_CACHELINE_ALIGNED cache_aligned_uint32;
_Static_assert(sizeof(cache_aligned_uint32) == PLATFORM_CACHELINE_SIZE,
               "Attribute set wrong");

typedef struct {
   char *token_str;
   char *last_token;
   int   last_token_len;
} platform_strtok_ctx;

extern bool platform_use_hugetlb;
extern bool platform_use_mlock;


/*
 * Section 3:
 * Shared types/typedefs that rely on platform-specific types/typedefs
 * There should not be any generic #includes here, but potentially #includes
 * of headers defined in splinterdb cannot be included until now.
 */
extern platform_log_handle *Platform_default_log_handle;
extern platform_log_handle *Platform_error_log_handle;


/*
 * Section 4:
 * Shared function declarations.
 * Shared inline function declarations (including declarations for the
 *   platform-specific inline functions).
 * Implementations of shared inline functions that aren't platform specific
 *   (These may be candidates to move outside of platform.h entirely)
 */
#if SPLINTER_DEBUG
#   define debug_assert(expr, ...) platform_assert(expr, __VA_ARGS__)
#   define debug_only              __attribute__((__unused__))
#   define debug_code(...)         __VA_ARGS__
#else
#   define debug_assert(expr, ...)
#   define debug_only __attribute__((__unused__))
#   define debug_code(...)
#endif // SPLINTER_DEBUG

#define platform_assert_status_ok(_s) platform_assert(SUCCESS(_s));

// hash functions
typedef uint32 (*hash_fn)(const void *input, size_t length, unsigned int seed);

extern platform_heap_handle Heap_handle;
extern platform_heap_id     Heap_id;

/*
 * Provide a tag for callers that do not want to use shared-memory allocation,
 * when configured but want to fallback to default scheme of allocating
 * process-private memory. Typically, this would default to malloc()/free().
 * (Clients that repeatedly allocate and free a large chunk of memory in some
 *  code path would want to use this tag.)
 */
#define PROCESS_PRIVATE_HEAP_ID (platform_heap_id) NULL

/*
 * -----------------------------------------------------------------------------
 * TYPED_MANUAL_MALLOC(), TYPED_MANUAL_ZALLOC() -
 * TYPED_ARRAY_MALLOC(),  TYPED_ARRAY_ZALLOC() -
 *
 * Utility macros to avoid common memory allocation / initialization mistakes.
 * NOTE: ZALLOC variants will also memset allocated memory chunk to 0.
 *
 * Call-flow is:
 *  TYPED_MALLOC()
 *   -> TYPED_ARRAY_MALLOC()
 *        -> TYPED_MANUAL_MALLOC() -> platform_aligned_malloc()
 *
 *  TYPED_ZALLOC()
 *   -> TYPED_ARRAY_ZALLOC()
 *        -> TYPED_MANUAL_ZALLOC() -> platform_aligned_zalloc()
 *
 * -----------------------------------------------------------------------------
 * Common mistake to make is:
 *    TYPE *foo = platform_malloc(sizeof(WRONG_TYPE));
 * or
 *    TYPE *foo;
 *    ...
 *    foo = platform_malloc(sizeof(WRONG_TYPE));
 * WRONG_TYPE may have been the correct type and the code changed, or you may
 * have the wrong number of '*'s, or you may have copied and pasted and forgot
 * to change the type/size.
 *
 * A useful pattern is:
 *    TYPE *foo = platform_malloc(sizeof(*foo))
 * but you can still cause a mistake by not typing the variable name correctly
 * twice.
 *
 * We _could_ make a macro MALLOC_AND_SET for the above pattern, e.g.
 *    define MALLOC_AND_SET(x) (x) = platform_malloc(sizeof(*x))
 * but that is hard to read.
 *
 * The boilerplate of extra typing (e.g. remember to type x twice) isn't a big
 * problem, it's just that you can get the types wrong and the compiler won't
 * notice.
 * We can keep the easy-to-read pattern of `x = function(x)` with typesafety by
 * including a cast inside the macro.
 *
 * These macros let you avoid the avoid the mistakes by typing malloc.
 *
 *    struct foo *foo_pointer = TYPED_MALLOC(foo_pointer);
 *    struct foo *foo_pointer2;
 *    foo_pointer2 = TYPED_MALLOC(foo_pointer2);
 *
 * To replace dynamic/vla arrays, use TYPED_ARRAY_MALLOC, e.g.
 *    struct foo array[X()];
 * becomes
 *    struct foo *array = TYPED_ARRAY_MALLOC(array, X());
 * ZALLOC versions will also memset to 0.
 *
 * All mallocs here are cache aligned.
 * Consider the following types:
 * struct unaligned_foo {
 *   cache_aligned_type bar;
 * };
 * If you (unaligned) malloc a 'unaligned_foo', the compile is still allowed to
 * assume that bar is properly cache aligned.  It may do unsafe optimizations.
 * One known unsafe optimization is turning memset(...,0,...) into avx
 * instructions that crash if something is not aligned.
 *
 * The simplest solution is to simply align ALL mallocs.
 * We do not do a sufficient number of mallocs for this to have any major
 * problems.  The slight additional memory usage (for tiny mallocs) should not
 * matter.  The potential minor perf hit should also not matter due to us
 * slowly coalescing all mallocs anyway into either initialization or amortized
 * situations.
 *
 * Alternative solutions are to be careful with mallocs, and/or make ALL structs
 * be aligned.
 *
 * Another common use case is if you have a struct with a flexible array member.
 * In that case you should use TYPED_FLEXIBLE_STRUCT_(M|Z)ALLOC
 *
 * If you are doing memory size calculation manually (e.g. if you're avoiding
 * multiple mallocs by doing one larger malloc and setting pointers manually,
 * or the data type has a something[] or something[0] at the end) you should
 * instead use the TYPED_*ALLOC_MANUAL macros that allow you to provide the
 * exact size.  These macros currently assume (and in debug mode assert) that
 * you will never malloc LESS than the struct/type size.
 *
 * DO NOT USE these macros to assign to a void*.  The debug asserts will cause
 * a compile error when debug is on.  Assigning to a void* should be done by
 * calling aligned_alloc manually (or create a separate macro)
 *
 * Parameters:
 *	hid - Platform heap-ID to allocate memory from.
 *	v   - Structure to allocate memory for.
 *	n   - Number of bytes of memory to allocate.
 * -----------------------------------------------------------------------------
 */
#define TYPED_MANUAL_MALLOC(hid, v, n, mf)                                     \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_malloc(hid,                                  \
                                         PLATFORM_CACHELINE_SIZE,              \
                                         (n),                                  \
                                         (mf),                                 \
                                         STRINGIFY(v),                         \
                                         __func__,                             \
                                         __FILE__,                             \
                                         __LINE__);                            \
   })

#define TYPED_MANUAL_ZALLOC(hid, v, n, mf)                                     \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_zalloc(hid,                                  \
                                         PLATFORM_CACHELINE_SIZE,              \
                                         (n),                                  \
                                         (mf),                                 \
                                         STRINGIFY(v),                         \
                                         __func__,                             \
                                         __FILE__,                             \
                                         __LINE__);                            \
   })

/*
 * TYPED_ALIGNED_MALLOC(), TYPED_ALIGNED_ZALLOC()
 *
 * Allocate memory for a typed structure at caller-specified alignment.
 * These are similar to TYPED_MANUAL_MALLOC() & TYPED_MANUAL_ZALLOC() but with
 * the difference that the alignment is caller-specified.
 *
 * Parameters:
 *	hid - Platform heap-ID to allocate memory from.
 *	a   - Alignment needed for allocated memory.
 *	v   - Structure to allocate memory for.
 *	n   - Number of bytes of memory to allocate.
 */
#define TYPED_ALIGNED_MALLOC(hid, a, v, n)                                     \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_malloc(                                      \
         hid, (a), (n), NULL, STRINGIFY(v), __func__, __FILE__, __LINE__);     \
   })
#define TYPED_ALIGNED_ZALLOC(hid, a, v, n)                                     \
   ({                                                                          \
      debug_assert((n) >= sizeof(*(v)));                                       \
      (typeof(v))platform_aligned_zalloc(                                      \
         hid, (a), (n), NULL, STRINGIFY(v), __func__, __FILE__, __LINE__);     \
   })

/*
 * FLEXIBLE_STRUCT_SIZE(): Compute the size of a structure 'v' with a nested
 * flexible array member, array_field_name, with 'n' members.
 *
 * Flexible array members don't necessarily start after sizeof(v)
 * They can start within the padding at the end, so the correct size
 * needed to allocate a struct with a flexible array member is the
 * larger of sizeof(struct v) or (offset of flexible array +
 * n*sizeof(arraymember))
 *
 * The only reasonable static assert we can do is check that the flexible array
 * member is actually an array.  We cannot check size==0 (compile error), and
 * since it doesn't necessarily start at the end we also cannot check
 * offset==sizeof.
 *
 * Parameters:
 *  v                   - Structure to allocate memory for.
 *  array_field_name    - Name of flexible array field nested in 'v'
 *  n                   - Number of members in array_field_name[].
 */
#define FLEXIBLE_STRUCT_SIZE(v, array_field_name, n)                           \
   ({                                                                          \
      _Static_assert(IS_ARRAY((v)->array_field_name),                          \
                     "flexible array members must be arrays");                 \
      max_size_t(sizeof(*(v)),                                                 \
                 (n) * sizeof((v)->array_field_name[0])                        \
                    + offsetof(typeof(*(v)), array_field_name));               \
   })

/*
 * -----------------------------------------------------------------------------
 * TYPED_FLEXIBLE_STRUCT_MALLOC(), TYPED_FLEXIBLE_STRUCT_ZALLOC() -
 *    Allocate memory for a structure with a nested flexible array member.
 *
 * Parameters:
 *  hid                 - Platform heap-ID to allocate memory from.
 *  v                   - Structure to allocate memory for.
 *  array_field_name    - Name of flexible array field nested in 'v'
 *  n                   - Number of members in array_field_name[].
 * -----------------------------------------------------------------------------
 */
#define TYPED_FLEXIBLE_STRUCT_MALLOC(hid, v, array_field_name, n)              \
   TYPED_MANUAL_MALLOC(hid,                                                    \
                       (v),                                                    \
                       FLEXIBLE_STRUCT_SIZE((v), array_field_name, (n)),       \
                       &memfrag_##v)

#define TYPED_FLEXIBLE_STRUCT_ZALLOC(hid, v, array_field_name, n)              \
   TYPED_MANUAL_ZALLOC(hid,                                                    \
                       (v),                                                    \
                       FLEXIBLE_STRUCT_SIZE((v), array_field_name, (n)),       \
                       &memfrag_##v)

/*
 * TYPED_ARRAY_MALLOC(), TYPED_ARRAY_ZALLOC()
 * Allocate memory for an array of 'n' elements of structure 'v'.
 *
 * Parameters:
 *  hid - Platform heap-ID to allocate memory from.
 *  v   - Structure to allocate memory for.
 *  n   - Number of members of type 'v' in array.
 *
 * Caller is expected to declare an on-stack platform_memfrag{} struct
 * named memfrag_<v>. This is used as output struct to return memory frag info.
 */
#define TYPED_ARRAY_MALLOC(hid, v, n)                                          \
   TYPED_MANUAL_MALLOC(hid, (v), (n) * sizeof(*(v)), &memfrag_##v)

#define TYPED_ARRAY_ZALLOC(hid, v, n)                                          \
   TYPED_MANUAL_ZALLOC(hid, (v), (n) * sizeof(*(v)), &memfrag_##v)

#define TYPED_ARRAY_MALLOC_MF(hid, v, n, mf)                                   \
   TYPED_MANUAL_MALLOC(hid, (v), (n) * sizeof(*(v)), mf)

#define TYPED_ARRAY_ZALLOC_MF(hid, v, n, mf)                                   \
   TYPED_MANUAL_ZALLOC(hid, (v), (n) * sizeof(*(v)), mf)

/*
 * TYPED_MALLOC(), TYPED_ZALLOC()
 * Allocate memory for one element of structure 'v'.
 *
 * Parameters:
 *  hid - Platform heap-ID to allocate memory from.
 *  v   - Structure to allocate memory for.
 */
#define TYPED_MALLOC(hid, v) TYPED_ARRAY_MALLOC_MF(hid, v, 1, NULL)
#define TYPED_ZALLOC(hid, v) TYPED_ARRAY_ZALLOC_MF(hid, v, 1, NULL)

/*
 * -----------------------------------------------------------------------------
 * Utility macros to clear memory
 * They have similar usage/prevent similar mistakes to the TYPED_MALLOC
 * kind of macros.
 * They are not appropriate when you have a malloced array or when you've
 * done non-obvious sized mallocs.
 *
 * Array/Pointer/Struct have different implementations cause the size
 * and pointer calculations are different.
 * Passing any one type to a clearing function of another type is likely
 * to be a bug, so if the calling isn't perfect it will give a compile-time
 * error.
 * -----------------------------------------------------------------------------
 */
/*
 * Zero an array.
 * Cause compile-time error if used on pointer or non-indexible variable
 */
#define ZERO_ARRAY(v)                                                          \
   do {                                                                        \
      _Static_assert(IS_ARRAY(v), "Use of ZERO_ARRAY on non-array object");    \
      memset((v), 0, sizeof(v));                                               \
   } while (0)

/*
 * Zero a manual array (e.g. we malloced an array).
 * Cause compile-time error if used on an array or non-indexible variable
 */
#define ZERO_CONTENTS_N(v, n)                                                  \
   do {                                                                        \
      _Static_assert(!IS_ARRAY(v), "Use of ZERO_CONTENTS on array");           \
      debug_assert((v) != NULL);                                               \
      memset((v), 0, (n) * sizeof(*(v)));                                      \
   } while (0)

/*
 * Zero a non-array pointer (clears what the pointer points to).
 * Cause compile-time error if used on an array or non-indexible variable
 *
 * Should not be used to zero out structs. Use ZERO_STRUCT instead.
 * It is difficult to add compile errors when you pass structs here, but
 * a debug compile is likely to compile error on the debug_assert.
 */
#define ZERO_CONTENTS(v) ZERO_CONTENTS_N((v), 1)

/*
 * Zero a struct.
 * We want to give a compile-time error if v is not a struct, so we cannot
 * use something like memset(&v, 0, sizeof(v)); Even C11 is not rich enough
 * to determine if a variable is a struct, so we use syntax errors to catch
 * it instead.
 *
 * Unfortunately C11/gnu11 syntax is only rich enough to cause errors on
 * pointers and primitives, but not arrays.
 *
 * Note: We could take advantage of the syntax and still use memset like this:
 *    __builtin_choose_expr(
 *       0,
 *       (typeof(v)) {},
 *       memset(&(v), 0, sizeof(v)))
 * however while the above still does prevent pointers and primitives, it will
 * incorrectly initialize arrays (read: corrupt memory) if they happen to get
 * passed in.
 *    Struct assignment properly initializes both arrays and structs:
 *       v = (typeof(v)) {}
 *    Memset for structs:
 *       memset(&v, 0, sizeof(v))
 *    Memset for arrays:
 *       memset(v, 0, sizeof(v))
 *
 * memset performance is affected by compiler and headers
 * struct assignment performance is just affected by compiler
 * It's not obvious that one has a performance advantage and since memset is
 * often a compiler intrinsic it's likely to have the same performance.
 *
 * Note; This version would only work during declaration:
 *    (v) = {}
 * Note; This version could work during declaration and a regular statement,
 * but we force it as a statement to match the usage of ZERO_ARRAY/ZERO_POINTER
 *    (v) = (typeof(v)) {}
 *
 * This macro intentionally CANNOT be used during declaration
 * (see ZERO_STRUCT_AT_DECL).
 */
#define ZERO_STRUCT(v)                                                         \
   do {                                                                        \
      (v) = (typeof(v)){};                                                     \
   } while (0)

/*
 * Zero a struct at declaration time.
 * Equivalent to doing:
 *    struct foo s;
 *    ZERO_STRUCT(s);
 * Usage example:
 *    struct foo ZERO_STRUCT_AT_DECL(s);
 * See documentation for ZERO_STRUCT.
 * Note:
 *    You can actually use ZERO_STRUCT_AT_DECL as a regular statement,
 *    but it is slightly less safe because the first v cannot be wrapped
 *    in parenthesis.
 */
#define ZERO_STRUCT_AT_DECL(v)                                                 \
   v = (typeof(v)) {}

void
platform_sort_slow(void               *base,
                   size_t              nmemb,
                   size_t              size,
                   platform_sort_cmpfn cmpfn,
                   void               *cmparg,
                   void               *temp);

#define IS_POWER_OF_2(n) ((n) > 0 && ((n) & ((n)-1)) == 0)

#ifndef MAX
#   define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

#ifndef MIN
#   define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

/*
 * Linux understands that you cannot continue after a failed assert already,
 * so we do not need a workaround for platform_assert in linux
 */
__attribute__((noreturn)) void
platform_assert_false(const char *filename,
                      int         linenumber,
                      const char *functionname,
                      const char *expr,
                      const char *message,
                      ...);

void
platform_assert_msg(platform_log_handle *log_handle,
                    const char          *filename,
                    int                  linenumber,
                    const char          *functionname,
                    const char          *expr,
                    const char          *message,
                    va_list              args);

/*
 * Caller-macro to invoke assertion checking. Avoids a function call for
 * most cases when the assertion will succeed.
 *
 * Note: The dangling fprintf() is really dead-code, as it executes after the
 * "noreturn" function implementing the assertion check executes, and fails.
 * -BUT- The fprintf() is solely there as a small compile-time check to ensure
 * that the arguments match the print-formats in any user-supplied message.
 */
#define platform_assert(expr, ...)                                             \
   ((expr) ? (void)0                                                           \
           : (platform_assert_false(                                           \
                 __FILE__, __LINE__, __func__, #expr, "" __VA_ARGS__),         \
              (void)fprintf(stderr, " " __VA_ARGS__)))

static inline timestamp
platform_get_timestamp(void);

static inline timestamp
platform_timestamp_elapsed(timestamp tv);

static inline timestamp
platform_get_real_time(void);

static inline void
platform_sleep_ns(uint64 ns);

static inline void
platform_semaphore_destroy(platform_semaphore *sema);

static inline void
platform_semaphore_init(platform_semaphore *sema,
                        int                 value,
                        platform_heap_id    heap_id);

static inline void
platform_semaphore_post(platform_semaphore *sema);

static inline void
platform_semaphore_wait(platform_semaphore *sema);

platform_status
platform_histo_create(platform_heap_id       heap_id,
                      uint32                 num_buckets,
                      const int64 *const     bucket_limits,
                      platform_histo_handle *histo);

void
platform_histo_destroy(platform_heap_id heap_id, platform_histo_handle *histo);

void
platform_histo_print(platform_histo_handle histo,
                     const char           *name,
                     platform_log_handle  *log_handle);

static inline threadid
platform_get_tid();

static inline void
platform_set_tid(threadid t);

static inline size_t
platform_strnlen(const char *s, size_t maxlen);

platform_log_handle *
platform_get_stdout_stream(void);

platform_status
platform_heap_create(platform_module_id    module_id,
                     size_t                max,
                     bool                  use_shmem,
                     platform_heap_handle *heap_handle,
                     platform_heap_id     *heap_id);

platform_status
platform_heap_destroy(platform_heap_handle *heap_handle);

platform_status
platform_buffer_init(buffer_handle *bh, size_t length);

void
platform_heap_set_splinterdb_handle(platform_heap_handle heap_handle,
                                    void                *addr);

void *
platform_heap_get_splinterdb_handle(platform_heap_handle heap_handle);

void *
platform_buffer_getaddr(const buffer_handle *bh);

platform_status
platform_buffer_deinit(buffer_handle *bh);

platform_status
platform_mutex_init(platform_mutex    *mu,
                    platform_module_id module_id,
                    platform_heap_id   heap_id);

platform_status
platform_mutex_destroy(platform_mutex *mu);

platform_status
platform_spinlock_init(platform_spinlock *lock,
                       platform_module_id module_id,
                       platform_heap_id   heap_id);

platform_status
platform_spinlock_destroy(platform_spinlock *lock);

platform_status
platform_thread_create(platform_thread       *thread,
                       bool                   detached,
                       platform_thread_worker worker,
                       void                  *arg,
                       platform_heap_id       heap_id);

platform_status
platform_thread_join(platform_thread thread);


platform_thread
platform_thread_id_self();

char *
platform_strtok_r(char *str, const char *delim, platform_strtok_ctx *ctx);

void
platform_enable_tracing_shm_ops();

void
platform_enable_tracing_shm_allocs();

void
platform_enable_tracing_shm_frees();


/*
 * Section 5:
 * Platform-specific inline implementations
 *
 * Non-inline implementations belong in a .c file in the platform_* directory.
 * Declarations for the non-inline functions can go in platform_inline.h
 */
/*
 * Structure to encapsulate a {memory-addr, memory-size} pair. Used to track
 * allocation and, more importantly, free of memory fragments for opaque
 * "objects". Used typically to manage memory for arrays of things.
 * The 'addr' field is intentionally -not- the 1st field, to reduce lazy
 * programming which might try to bypass provided interfaces.
 */
typedef struct platform_memfrag {
   size_t size;
   void  *addr;
} platform_memfrag;

#include <platform_inline.h>


/*
 * Section 6:
 * Non-platform-specific inline implementations
 */

/*
 * Utility macro to test if an argument to platform_free() is a
 * platform_memfrag{}.
 */
#define IS_MEM_FRAG(x)                                                         \
   __builtin_choose_expr(                                                      \
      __builtin_types_compatible_p(typeof((platform_memfrag *)0), typeof(x)),  \
      1,                                                                       \
      0)

/* Helper methods to do some common operations */
#define memfrag_start(mf) ((mf)->addr)
#define memfrag_size(mf)  ((mf)->size)

/*
 * void = memfrag_init_size(platform_memfrag *mf, <something> *ptr,
 *                          size_t nbytes)
 *
 * Macro to initialize a memory fragment that was allocated for nitems-items of
 * an object pointed at by 'ptr'. Sets it up to free the fragment.
 */
#define memfrag_init_size(mf, ptr, nbytes)                                     \
   do {                                                                        \
      (mf)->addr = (void *)ptr;                                                \
      (mf)->size = (nbytes);                                                   \
   } while (0)

static inline bool
memfrag_is_empty(const platform_memfrag *mf)
{
   return ((mf->addr == NULL) && (mf->size == 0));
}

static inline void
memfrag_set_empty(platform_memfrag *mf)
{
   debug_assert(!memfrag_is_empty(mf));
   mf->addr = NULL;
   mf->size = 0;
}

/* Move the memory fragment ownership from src to dst memory fragment */
static inline void
memfrag_move(platform_memfrag *dst, platform_memfrag *src)
{
   platform_assert(memfrag_is_empty(dst));
   platform_assert(!memfrag_is_empty(src));

   dst->addr = src->addr;
   dst->size = src->size;
   src->addr = NULL;
   src->size = 0;
}

/*
 * ----------------------------------------------------------------------------
 * void = platform_free(platform_heap_id hid, void *p);
 *
 * Similar to the TYPED_MALLOC functions, for all the free functions we need
 * to call platform_get_heap_id() from a macro instead of an inline function
 * (which may or may not end up inlined). Wrap free and free_volatile.
 *
 * This simple macro does a few interesting things:
 *
 * - This macro calls underlying platform_free_mem() to supply the 'size' of
 *    the memory fragment being freed. This is needed to support recycling of
 *    freed fragments when shared memory is used.
 *
 * - Most callers will be free'ing memory allocated pointing to a structure.
 *   This interface also provides a way to free opaque fragments described
 *   simply by a start-address and size of the fragment. Such usages occur,
 *   say, for memory fragments allocated for an array of n-structs.
 *
 * - To catch code errors where we may attempt to free the same memory fragment
 *   twice, it's a hard assertion if input ptr 'p' is NULL (likely already
 *   freed).
 *
 * - Defenses are built-in to protect callers which may be incorrectly using
 *   this interface to free memory allocated from shared-segment or the heap.
 * ----------------------------------------------------------------------------
 */
#define platform_free(hid, p)                                                  \
   do {                                                                        \
      platform_assert(((p) != NULL),                                           \
                      "Attempt to free a NULL ptr from '%s', line=%d",         \
                      __func__,                                                \
                      __LINE__);                                               \
      if (IS_MEM_FRAG(p)) {                                                    \
         platform_memfrag *_mf = (platform_memfrag *)(p);                      \
         platform_free_mem((hid), _mf->addr, _mf->size, STRINGIFY(p));         \
         _mf->addr = NULL;                                                     \
         _mf->size = 0;                                                        \
      } else {                                                                 \
         platform_assert((((hid) == PROCESS_PRIVATE_HEAP_ID)                   \
                          || (sizeof(*p) > sizeof(uint64))),                   \
                         "Attempt to free memory using '%s', which is a "      \
                         " pointer to an object of size %lu bytes"             \
                         ", from '%s', line=%d\n",                             \
                         STRINGIFY(p),                                         \
                         sizeof(*p),                                           \
                         __func__,                                             \
                         __LINE__);                                            \
         /*                                                                    \
          * Expect that 'p' is pointing to a struct. So get its size.          \
          * Except that while allocating memory for objects, we had previously \
          * aligned the required size to PLATFORM_CACHELINE_SIZE. Redo that    \
          * work here, so we correctly tell shared-memory the size to free.    \
          */                                                                   \
         const size_t _size = sizeof(*p);                                      \
         const size_t _reqd =                                                  \
            (_size + platform_alignment(PLATFORM_CACHELINE_SIZE, _size));      \
         platform_free_mem((hid), (p), _reqd, STRINGIFY(p));                   \
         (p) = NULL;                                                           \
      }                                                                        \
   } while (0)

/*
 * void = platform_free_mem(platform_heap_id hid, void *p, size_t size,
 *                          const char *objname);
 *
 * Free a memory chunk at address 'p' of size 'size' bytes. This exists to
 * facilitate re-cycling of free'd fragments in a shared-memory usage. That
 * machinery works off of the fragment's 'size', hence we need to provide that.
 */
#define platform_free_mem(hid, p, size, objname)                               \
   do {                                                                        \
      platform_free_from_heap(                                                 \
         hid, (p), (size), objname, __func__, __FILE__, __LINE__);             \
      (p) = NULL;                                                              \
   } while (0)

/*
 * ----------------------------------------------------------------------------
 * void = platform_free_volatile(platform_heap_id hid,
 *                               platform_memfrag *p)
 *
 * Similar to platform_free(), except it exists to free volatile ptr to
 * allocated memory. The interface expects that the (single-) caller has
 * packaged the memory fragment to-be-freed in a platform_memfrag *p arg.
 * There is just one consumer of this interface, so we don't go to the full
 * distance as its sibling interface.
 * ----------------------------------------------------------------------------
 */
#define platform_free_volatile(hid, p)                                         \
   do {                                                                        \
      debug_assert(((p) != NULL),                                              \
                   "Attempt to free a NULL ptr from '%s', line=%d",            \
                   __func__,                                                   \
                   __LINE__);                                                  \
      platform_assert(IS_MEM_FRAG(p),                                          \
                      "Attempt to free volatile memory ptr with an invalid"    \
                      " arg, from '%s', line=%d",                              \
                      __func__,                                                \
                      __LINE__);                                               \
      platform_memfrag *_mf = (platform_memfrag *)(p);                         \
      platform_free_volatile_from_heap(hid,                                    \
                                       _mf->addr,                              \
                                       _mf->size,                              \
                                       STRINGIFY(p),                           \
                                       __func__,                               \
                                       __FILE__,                               \
                                       __LINE__);                              \
      _mf->addr = NULL;                                                        \
      _mf->size = 0;                                                           \
   } while (0)

// Convenience function to free something volatile
static inline void
platform_free_volatile_from_heap(platform_heap_id heap_id,
                                 volatile void   *ptr,
                                 const size_t     size,
                                 const char      *objname,
                                 const char      *func,
                                 const char      *file,
                                 int              lineno)
{
   // Ok to discard volatile qualifier for free
   platform_free_from_heap(
      heap_id, (void *)ptr, size, objname, func, file, lineno);
}

static inline void *
platform_aligned_zalloc(platform_heap_id  heap_id,
                        size_t            alignment,
                        size_t            size,
                        platform_memfrag *memfrag, // OUT
                        const char       *objname,
                        const char       *func,
                        const char       *file,
                        int               lineno)
{
   void *x = platform_aligned_malloc(
      heap_id, alignment, size, memfrag, objname, func, file, lineno);
   if (LIKELY(x)) {
      memset(x, 0, size);
   }
   return x;
}

static inline size_t
max_size_t(size_t a, size_t b)
{
   return a > b ? a : b;
}

// Return absolute diff between two unsigned long values.
static inline size_t
diff_size_t(size_t a, size_t b)
{
   return ((a > b) ? (a - b) : (b - a));
}

static inline bool
SUCCESS(const platform_status s)
{
   return STATUS_IS_EQ(s, STATUS_OK);
}

platform_status
platform_condvar_init(platform_condvar *cv, platform_heap_id heap_id);

platform_status
platform_condvar_wait(platform_condvar *cv);

platform_status
platform_condvar_signal(platform_condvar *cv);

platform_status
platform_condvar_broadcast(platform_condvar *cv);

/* calculate difference between two pointers */
static inline ptrdiff_t
diff_ptr(const void *base, const void *limit)
{
   _Static_assert(sizeof(char) == 1, "Assumption violated");
   return (char *)limit - (char *)base;
}

#define DEFAULT_THROTTLE_INTERVAL_SEC (60)

static inline int
platform_backtrace(void **buffer, int size)
{
   return backtrace(buffer, size);
}

#endif // PLATFORM_H
