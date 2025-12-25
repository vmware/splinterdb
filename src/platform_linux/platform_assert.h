#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include <stdarg.h>
#include <stdio.h>

#if !defined(SPLINTER_DEBUG)
#   define SPLINTER_DEBUG 0
#else
#   if SPLINTER_DEBUG != 0 && SPLINTER_DEBUG != 1
#      error SPLINTER_DEBUG not 0 or 1
#   endif
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
