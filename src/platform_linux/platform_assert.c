#include "platform_assert.h"
#include "platform_log.h"
#include "platform_threads.h"
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>

/*
 * platform_assert_false() -
 *
 * Platform-specific assert implementation, with support to print an optional
 * message and arguments involved in the assertion failure. The caller-macro
 * ensures that this function will only be called when the 'exprval' is FALSE;
 * i.e. the assertion is failing.
 */
__attribute__((noreturn)) void
platform_assert_false(const char *filename,
                      int         linenumber,
                      const char *functionname,
                      const char *expr,
                      const char *message,
                      ...)
{
   va_list varargs;
   va_start(varargs, message);

   // Run-time assertion messages go to stderr.
   platform_assert_msg(Platform_error_log_handle,
                       filename,
                       linenumber,
                       functionname,
                       expr,
                       message,
                       varargs);
   va_end(varargs);
   platform_error_log("\n");

   abort();
}

/*
 * Lower-level function to generate the assertion message, alone.
 */
void
platform_assert_msg(platform_log_handle *log_handle,
                    const char          *filename,
                    int                  linenumber,
                    const char          *functionname,
                    const char          *expr,
                    const char          *message,
                    va_list              varargs)
{
   static char assert_msg_fmt[] = "OS-pid=%d, OS-tid=%lu, Thread-ID=%lu, "
                                  "Assertion failed at %s:%d:%s(): \"%s\". ";
   platform_log(log_handle,
                assert_msg_fmt,
                platform_getpid(),
                gettid(),
                platform_get_tid(),
                filename,
                linenumber,
                functionname,
                expr);
   vfprintf(log_handle, message, varargs);
}
