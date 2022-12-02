/* Copyright 2011-2021 Bas van den Berg
 * Copyright 2021 VMware, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
#include <stdio.h>

#define CTEST_MAIN

// uncomment lines below to enable/disable features. See README.md for details
#define CTEST_SEGFAULT
// #define CTEST_NO_COLORS
// #define CTEST_COLOR_OK

#include <setjmp.h>
#include <stdarg.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <wchar.h>

#include "util.h"
#include "ctest.h"

#define MSG_SIZE 4096

static size_t      ctest_errorsize;
static char       *ctest_errormsg;
static char        ctest_errorbuffer[MSG_SIZE];
static jmp_buf     ctest_err;
static int         color_output  = 1;
static const char *suite_name    = NULL;
static const char *testcase_name = NULL;

typedef int (*ctest_filter_func)(struct ctest *);

// If 0, then CTEST_LOG_INFO() and CTEST_LOG() will no-op.  Defaults to 0.
int Ctest_verbosity = 0;

/*
 * Global handles to command-line args are provided so that we can access
 * argc/argv indirectly thru these global variables inside setup methods.
 */
int          Ctest_argc = 0;
const char **Ctest_argv = NULL;

#define ANSI_BLACK    "\033[0;30m"
#define ANSI_RED      "\033[0;31m"
#define ANSI_GREEN    "\033[0;32m"
#define ANSI_YELLOW   "\033[0;33m"
#define ANSI_BLUE     "\033[0;34m"
#define ANSI_MAGENTA  "\033[0;35m"
#define ANSI_CYAN     "\033[0;36m"
#define ANSI_GREY     "\033[0;37m"
#define ANSI_DARKGREY "\033[01;30m"
#define ANSI_BRED     "\033[01;31m"
#define ANSI_BGREEN   "\033[01;32m"
#define ANSI_BYELLOW  "\033[01;33m"
#define ANSI_BBLUE    "\033[01;34m"
#define ANSI_BMAGENTA "\033[01;35m"
#define ANSI_BCYAN    "\033[01;36m"
#define ANSI_WHITE    "\033[01;37m"
#define ANSI_NORMAL   "\033[0m"

CTEST(suite, test) {}

/*
 * Function Prototypes
 */
static void
vprint_errormsg(const char *const fmt, va_list ap)
   CTEST_IMPL_FORMAT_PRINTF(1, 0);

static void
print_errormsg(const char *const fmt, ...) CTEST_IMPL_FORMAT_PRINTF(1, 2);

int
ctest_main(int argc, const char *argv[]);

void
ctest_usage(const char *progname, int program_is_unit_test);

static int
ctest_process_args(const int    argc,
                   const char  *argv[],
                   int          program_is_unit_test,
                   const char **suite_name,
                   const char **testcase_name);

int
ctest_is_unit_test(const char *argv0);

static int
ctest_get_verbosity();

static int
suite_all(struct ctest *t);

static int
suite_filter(struct ctest *t);

static int
testcase_filter(struct ctest *t);

static void
print_test_suite_names(const struct ctest *tbegin,
                       const struct ctest *tend,
                       const char         *suite_name,
                       int                 program_is_unit_test);

static uint64_t
getCurrentTime(void);

static void
color_print(const char *color, const char *text);

/*
 * ---------------------------------------------------------------------------
 * CTest Signal handler:
 * ---------------------------------------------------------------------------
 */
#ifdef CTEST_SEGFAULT
#   include <signal.h>
static void
sighandler(int signum)
{
   const char msg_color[] =
      ANSI_BRED "[SIGSEGV: Segmentation fault]" ANSI_NORMAL "\n";
   const char msg_nocolor[] = "[SIGSEGV: Segmentation fault]\n";

   const char *msg    = color_output ? msg_color : msg_nocolor;
   ssize_t     nbytes = write(STDOUT_FILENO, msg, strlen(msg));

   // Dead code to shutup compiler warning about unused variable etc.
   if (nbytes == 0) {
      printf("nbytes returned by write() call is %d\n", (int)nbytes);
   }

   /*
    * "Unregister" the signal handler and send the signal back to the process
    * so it can terminate as expected
    */
   signal(signum, SIG_DFL);
   kill(getpid(), signum);
}
#endif // CTEST_SEGFAULT

/*
 * ---------------------------------------------------------------------------
 * CTest main():
 * ---------------------------------------------------------------------------
 */
int
main(int argc, const char *argv[])
{
   int result = ctest_main(argc, argv);
   return result;
}

/*
 * ---------------------------------------------------------------------------
 * ctest_main():
 * ---------------------------------------------------------------------------
 */
__attribute__((no_sanitize_address)) int
ctest_main(int argc, const char *argv[])
{
   static int               num_ok   = 0;
   static int               num_fail = 0;
   static int               num_skip = 0;
   static int               idx      = 1;
   static ctest_filter_func filter   = suite_all;

#ifdef CTEST_SEGFAULT
   signal(SIGSEGV, sighandler);
#endif

   Ctest_verbosity = ctest_get_verbosity();

   int program_is_unit_test = ctest_is_unit_test(argv[0]);

   int print_list = 0; // Are we processing --list arg?

   // Process --help | --list arg up-front, before processing other variations.
   if ((argc >= 2)) {
      if (strcmp(argv[1], "--help") == 0) {
         ctest_usage(argv[0], program_is_unit_test);
         return num_fail;
      } else if (strcmp(argv[1], "--list") == 0) {
         print_list = 1;
      }
   }

   // Establish test-suite and test-case name filters
   int num_filter_args = ctest_process_args(
      argc, argv, program_is_unit_test, &suite_name, &testcase_name);

   if (num_filter_args < 0) {
      fprintf(stderr, "Incorrect usage. ");
      ctest_usage(argv[0], program_is_unit_test);
      return num_fail;
   }

   // Reset global argument related variables. These will be used in some
   // unit-tests to extract any --<config params> provided on the command-line,
   // using the config_parse() interface. Here, we want argv to point to just
   // the --<config params> args, if any.
   // NOTE: "-1", "+1" below is to move past argv[0], the binary name.
   Ctest_argc = (argc - num_filter_args - 1);
   Ctest_argv = (argv + 1);

   // Setup filter function depending on the way the program is being invoked
   if (program_is_unit_test) {
      if (suite_name != NULL) {
         filter = suite_filter;
      }
   } else if (testcase_name != NULL) {
      filter = testcase_filter;
   }

#ifdef CTEST_NO_COLORS
   color_output = 0;
#else
   color_output = isatty(1);
#endif
   uint64_t t1 = getCurrentTime();

   // Fabricate placeholder begin / end test suite marker
   struct ctest *ctest_begin = &CTEST_IMPL_TNAME(suite, test);
   struct ctest *ctest_end   = &CTEST_IMPL_TNAME(suite, test);

   // find begin and end of section by comparing magics
   while (1) {
      struct ctest *t = ctest_begin - 1;
      if (t->magic != CTEST_IMPL_MAGIC) {
         break;
      }
      ctest_begin--;
   }
   while (1) {
      struct ctest *t = ctest_end + 1;
      if (t->magic != CTEST_IMPL_MAGIC) {
         break;
      }
      ctest_end++;
   }
   ctest_end++; // end after last one

   if (print_list) {
      print_test_suite_names(
         ctest_begin, ctest_end, suite_name, program_is_unit_test);
      return 0;
   }
   static struct ctest *test;

   // Establish count of # of test-suites & test cases we will run (next).
   static int  total           = 0;
   static int  num_suites      = 0;
   const char *curr_suite_name = "";
   for (test = ctest_begin; test != ctest_end; test++) {
      if (test == &CTEST_IMPL_TNAME(suite, test)) {
         continue;
      }

      if (filter(test)) {
         if (strcmp(test->ssname, curr_suite_name)) {
            curr_suite_name = test->ssname;
            num_suites++;
         }
         total++; // Counts total # of test cases to run.
      }
   }

   // If we are running a standalone unit-test, register suite's name
   if (!suite_name && (num_suites == 1)) {
      suite_name = curr_suite_name;
   }
   printf("Running %d CTests, suite name '%s', test case '%s'.\n",
          num_suites,
          (suite_name ? suite_name : "all"),
          (testcase_name ? testcase_name : "all"));

   /*
    * ------------------------------------------------------------------
    * Main driver loop: Plough through the list of candidate test suite
    * names. And execute qualifying test cases in each suite.
    * ------------------------------------------------------------------
    */
   for (test = ctest_begin; test != ctest_end; test++) {
      if (test == &CTEST_IMPL_TNAME(suite, test)) {
         continue;
      }
      if (filter(test)) {
         ctest_errorbuffer[0] = 0;
         ctest_errorsize      = MSG_SIZE - 1;
         ctest_errormsg       = ctest_errorbuffer;

         printf("TEST %d/%d %s:%s ", idx, total, test->ssname, test->ttname);
         fflush(stdout);

         // Skip test cases that should be skipped.
         if (test->skip) {
            color_print(ANSI_BYELLOW, "[SKIPPED]");
            num_skip++;
         } else {
            int result = setjmp(ctest_err);
            if (result == 0) {
               if (test->setup && *test->setup) {
                  (*test->setup)(test->data);
               }
               if (test->data) {
                  test->run.unary(test->data);
               } else {
                  test->run.nullary();
               }
               if (test->teardown && *test->teardown)
                  (*test->teardown)(test->data);
                  // if we got here it's ok
#ifdef CTEST_COLOR_OK
               color_print(ANSI_BGREEN, "[OK]");
#else
               printf("[OK]\n");
#endif
               num_ok++;
            } else {
               color_print(ANSI_BRED, "[FAIL]");
               num_fail++;
            }
            if (ctest_errorsize != MSG_SIZE - 1) {
               printf("%s", ctest_errorbuffer);
            }
         }
         idx++;
      }
   }
   uint64_t t2 = getCurrentTime();

   const char *color = (num_fail) ? ANSI_BRED : ANSI_GREEN;
   char        results[80];
   snprintf(results,
            sizeof(results),
            "RESULTS: %d tests (%d ok, %d failed, %d skipped) ran in %" PRIu64
            " ms",
            total,
            num_ok,
            num_fail,
            num_skip,
            (t2 - t1) / 1000);
   color_print(color, results);

   if (num_fail > 0 && Ctest_verbosity == 0) {
      snprintf(results,
               sizeof(results),
               "(Rerun with env var VERBOSE=1 to see more log messages)");
      color_print(color, results);
   }
   return num_fail;
}

/*
 * ---------------------------------------------------------------------------
 * Helper Functions.
 * ---------------------------------------------------------------------------
 */
/*
 * ---------------------------------------------------------------------------
 * Identify the name of the program being executed (argv[0]). The usage will
 * vary depending on whether the top-level 'unit_test' or a stand-alone
 * unit-test binary is being invoked.
 *
 * Returns:
 *  1 - If it is 'unit_test'
 *  0 - For all other stand-alone unit-test binaries being invoked.
 * -1 - For any other error / unknown conditions.
 * ---------------------------------------------------------------------------
 */
int
ctest_is_unit_test(const char *argv0)
{
   const char *unit_test_str = "unit_test";
   const int   unit_test_len = strlen(unit_test_str);

   if (!argv0) {
      return -1;
   }

   // If we are running some other standalone program of shorter binary length.
   if (strlen(argv0) < unit_test_len) {
      return 0;
   }

   // Check for match of trailing portion of full binary name with 'unit_test'
   const char *startp = (argv0 + strlen(argv0) - unit_test_len);
   if (strncmp(unit_test_str, startp, unit_test_len) == 0) {
      return 1;
   }

   return 0;
}

// Determine the log message verbosity to use for this test run.
// If env var VERBOSE is unset, or is set to "0" or "false", the be quiet.
// Otherwise, be verbose.
static int
ctest_get_verbosity()
{
   char *val = getenv("VERBOSE");
   if (val == NULL) {
      return 0;
   }
   if ((strcmp(val, "0") == 0) || (strcmp(val, "false") == 0)) {
      return 0;
   }
   return 1;
}

/*
 * ---------------------------------------------------------------------------
 * Process the command-line arguments. The main job of this function is to
 * do some blind-parsing of the argc/argv[] and to figure out if:
 *
 * - the last arg is a "<suite-name>" or "<test-case-name>" arg.
 * - or the last-2-args is pair of "<suite-name>" "<test-case-name>" args.
 *
 * Handle the following valid usages:
 *
 * 1. We can invoke the top-level unit_test as follows:
 *
 *   unit_test [ --config-options ]* [ <suite-name> [ <test-case-name> ] ]
 *
 * 2. We can invoke an individual stand-alone unit_test as follows:
 *
 *      sub_unit_test [ --config-options ]* [ <test-case-name> ]
 *
 * This routines handles both cases to identify suite_name and test-case_name.
 *
 * Returns:
 *  # of trailing arguments processed; i.e. identified as either suite-name or
 *  test-name and extracted to output args.
 *  -1, in case of any usage / invocation
 * error.
 * ---------------------------------------------------------------------------
 */
static int
ctest_process_args(const int    argc,
                   const char  *argv[],
                   int          program_is_unit_test,
                   const char **suite_name,    // OUT
                   const char **testcase_name) // OUT
{
   if (argc <= 1) {
      return 0;
   }

   uint64 unused;

   /*
    * If the last arg is a --<arg>, then it's a config option with a value.
    * If last arg is parseable as a number, it's probably a "--config <value>"
    * No need for further processing to extract the "name"-args.
    */
   if ((strncmp(argv[argc - 1], "--", 2) == 0)
       || (try_string_to_uint64(argv[argc - 1], &unused)))
   {
      return 0;
   }

   /*
    * Here, argc >= 2; i.e. we are dealing with either one of these cases:
    *   - bin/unit_test <suite-name>
    *   - bin/unit_test <suite-name> <test-case-name>
    *   - bin/unit/standalone <test-case-name>
    * We expect up to 2 trailing "name"-args to be provided.
    */
   if (program_is_unit_test) {
      *suite_name = argv[argc - 1];
   } else {
      *testcase_name = argv[argc - 1];
   }

   if (argc == 2) {
      // We stripped off 1 "name"-argument from list.
      return 1;
   }

   // It's an error to issue: splinter_test --list <any-string>
   if (!program_is_unit_test && (strcmp(argv[argc - 2], "--list") == 0)) {
      return -1;
   }

   // Here, argc >= 3. If the last-but-one arg is a --<option> ...
   if (strncmp(argv[argc - 2], "--", 2) == 0) {
      /*
       * We stripped off the last one "name"-argument from list.
       * Rest are config options, which can be passed-through.
       */
      return 1;
   }

   // Here, still argc >= 3
   if (program_is_unit_test) {
      /*
       * Separately identify these two cases:
       *   - bin/unit_test <suite-name> <test-case-name>
       *   - bin/unit_test <suite-name>
       */
      if (try_string_to_uint64(argv[argc - 2], &unused)) {
         // We have already extracted the very last arg as suite-name.
         // Penultimate one probably belongs to a config option. We are done.
         return 1;
      }

      // Last pair is now parsed as: <suite-name> <test-case-name>
      *suite_name    = argv[argc - 2];
      *testcase_name = argv[argc - 1];
      return 2;
   } else if (!try_string_to_uint64(argv[argc - 2], &unused)) {
      /*
       * We could not parse the penultimate arg as a number. So, it's probably
       * a string. It's an error to issue:
       *   standalone [ config-options ]* <suite-name> <testcase-name>
       */
      return -1;
   }
   /*
    * We are down to the last case of:
    *   sub_unit-test [ config-options ]* <testcase-name>
    */
   return 1;
}

/*
 * Generate brief usage information to run CTests.
 */
void
ctest_usage(const char *progname, int program_is_unit_test)
{
   printf("Usage: %s ", progname);
   if (program_is_unit_test) {
      printf("[--list [ <suite-name> ] ]\n");
      printf("Usage: %s "
             "[ --<config options> ]* [ <suite-name> [ <test-case-name> ] ]\n",
             progname);
   } else {
      printf("[ --list | [ --<config options> ]* <test-case-name> ]\n");
   }
}

/*
 * This is a pass-through filter function, selecting all test suites
 * to run.
 */
static int
suite_all(struct ctest *t)
{
   (void)t; // fix unused parameter warning
   return 1;
}

/*
 * Function to filter which test case name to run. Currently, we only support
 * an exact match of the test casee name. (Wild-card matching may be
 * considered in the future). User can invoke as follows to just run one
 * test case from a specific suite:
 *
 * $ bin/unit_test splinterdb test_splinterdb_iterator_with_startkey
 */
static int
testcase_filter(struct ctest *t)
{
   if (!testcase_name) {
      return 1;
   }
   return strncmp(testcase_name, t->ttname, strlen(testcase_name)) == 0;
}

/*
 * ---------------------------------------------------------------------------
 * Function to filter suite name to run. Currently, we only support an
 * exact match of the suite-name. (Wild-card matching may be considered
 * in the future). Test case name filtering is implicitly subsumed in
 * this function, so that we need to do only one filter()'ing in outer
 * iteration of test execution loop.
 *
 * User can invoke as follows to just run one suite:
 *  $ bin/unit_test splinterdb
 *
 * User can invoke as follows to just run one test case from a suite:
 *  $ bin/unit_test splinterdb test_splinterdb_iterator_with_startkey
 * ---------------------------------------------------------------------------
 */
static int
suite_filter(struct ctest *t)
{
   int rv = (strncmp(suite_name, t->ssname, strlen(suite_name)) == 0);

   // If suite name itself didn't match, we are done.
   if (!rv)
      return rv;

   // If user didn't request filtering by test case name, we are done.
   if (!testcase_name)
      return rv;

   rv = testcase_filter(t);
   return rv;
}

/*
 * ---------------------------------------------------------------------------
 * print_test_suite_names(): Implements --list support
 *
 * This function lists the suite-names that can be run from unit_test,
 * or will list the test-case names that can be run for a give suite-name.
 *
 * Usage
 *
 *  - Will list all suite-names, or all test cases names in specified suite name
 *  $ unit_test --list [ <suite-name> ]
 *
 * When used as "unit_test --list <suite-name>", we first find the start of
 * the requested suite-name, then, will print list of test cases from it.
 * Processing returns early immediately upon start of the next test suite name.
 *
 *  - Will list all test cases names in specified standalone test
 *  $ standalone_test --list
 * ---------------------------------------------------------------------------
 */
static void
print_test_suite_names(const struct ctest *test_begin,
                       const struct ctest *test_end,
                       const char         *suite_name,
                       int                 program_is_unit_test)
{
   // unit_test --list                  : Print suite-name
   //    unit_test --list <suite-name>
   // or <standalone-test> --list       : Print test case name
   int print_test_case_name = (suite_name || !program_is_unit_test);

   printf("\nList of test %s that can be run:\n",
          (print_test_case_name ? "cases" : "suites"));

   const struct ctest *test;
   const char         *prev_suite_name  = NULL;
   int                 found_suite_name = 0;

   for (test = test_begin; test != test_end; test++) {
      // Skip placeholder entry of test definitions in input list
      if (test == &CTEST_IMPL_TNAME(suite, test)) {
         continue;
      }

      // If user has asked to list test-cases for specified suite ...
      if (suite_name) {
         // Find the first occurrence of that suite-name in the list.
         if (!found_suite_name) {
            // It's the wrong suite name; skip it.
            if (strcmp(test->ssname, suite_name)) {
               continue;
            }
            found_suite_name = 1;
         } else if (strcmp(test->ssname, suite_name)) {
            // All test cases come in a chunk for a test suite.
            // So, if user wanted to list test cases for a specific suite,
            // and we find the start of a new chunk of test cases, we are done.
            return;
         }
      }

      // Print each suite-name only once. It may appear multiple times
      // in the list of individual test-cases from that suite.
      else if (program_is_unit_test)
      {
         if (prev_suite_name && (strcmp(prev_suite_name, test->ssname) == 0)) {
            continue;
         }
      }

      printf("  %s\n", (print_test_case_name ? test->ttname : test->ssname));
      prev_suite_name = test->ssname;
   }
}

static uint64_t
getCurrentTime(void)
{
   struct timeval now;
   gettimeofday(&now, NULL);
   uint64_t now64 = (uint64_t)now.tv_sec;
   now64 *= 1000000;
   now64 += ((uint64_t)now.tv_usec);
   return now64;
}

static void
color_print(const char *color, const char *text)
{
   if (color_output)
      printf("%s%s" ANSI_NORMAL "\n", color, text);
   else
      printf("%s\n", text);
}

static void
vprint_errormsg(const char *const fmt, va_list ap)
{
   // (v)snprintf returns the number that would have been written
   const int ret = vsnprintf(ctest_errormsg, ctest_errorsize, fmt, ap);
   if (ret < 0) {
      ctest_errormsg[0] = 0x00;
   } else {
      const size_t size = (size_t)ret;
      const size_t s =
         (ctest_errorsize <= size ? size - ctest_errorsize : size);
      // ctest_errorsize may overflow at this point
      ctest_errorsize -= s;
      ctest_errormsg += s;
   }
}

static void
print_errormsg(const char *const fmt, ...)
{
   va_list argp;
   va_start(argp, fmt);
   vprint_errormsg(fmt, argp);
   va_end(argp);
}

static void
msg_start(const char *color, const char *title)
{
   if (color_output) {
      print_errormsg("%s", color);
   }
   print_errormsg("  %s: ", title);
}

static void
msg_end(void)
{
   if (color_output) {
      print_errormsg(ANSI_NORMAL);
   }
   print_errormsg("\n");
}

void
CTEST_LOG(const char *fmt, ...)
{
   if (Ctest_verbosity == 0) {
      return;
   }

   va_list argp;
   msg_start(ANSI_BLUE, "LOG");

   va_start(argp, fmt);
   vprint_errormsg(fmt, argp);
   va_end(argp);

   msg_end();
}

/* clang-format'ting messes this up, which breaks compilation */
// clang-format off
CTEST_IMPL_DIAG_PUSH_IGNORED(missing-noreturn)
// clang-format on

void
CTEST_ERR(const char *fmt, ...)
{
   va_list argp;
   msg_start(ANSI_YELLOW, "ERR");

   va_start(argp, fmt);
   vprint_errormsg(fmt, argp);
   va_end(argp);

   msg_end();
   longjmp(ctest_err, 1);
}

CTEST_IMPL_DIAG_POP()

/*
 * ---------------------------------------------------------------------------
 * Collection of functions implementing different assertion checks.
 * Invoked thru caller-macros defined in ctest.h, along with their prototypes.
 * ---------------------------------------------------------------------------
 */
void
assert_str(const char *exp, const char *real, const char *caller, int line)
{
   if ((exp == NULL && real != NULL) || (exp != NULL && real == NULL)
       || (exp && real && strcmp(exp, real) != 0))
   {
      CTEST_ERR("%s:%d  expected '%s', got '%s'", caller, line, exp, real);
   }
}

void
assert_wstr(const wchar_t *exp,
            const wchar_t *real,
            const char    *caller,
            int            line)
{
   if ((exp == NULL && real != NULL) || (exp != NULL && real == NULL)
       || (exp && real && wcscmp(exp, real) != 0))
   {
      CTEST_ERR("%s:%d  expected '%ls', got '%ls'", caller, line, exp, real);
   }
}

void
assert_data(const void *vexp,
            size_t      expsize,
            const void *vreal,
            size_t      realsize,
            const char *caller,
            int         line)
{
   const unsigned char *exp  = vexp;
   const unsigned char *real = vreal;
   size_t               i;
   if (expsize != realsize) {
      CTEST_ERR("%s:%d  expected %" PRIuMAX " bytes, got %" PRIuMAX,
                caller,
                line,
                (uintmax_t)expsize,
                (uintmax_t)realsize);
   }
   for (i = 0; i < expsize; i++) {
      if (exp[i] != real[i]) {
         CTEST_ERR("%s:%d expected 0x%02x at offset %" PRIuMAX " got 0x%02x",
                   caller,
                   line,
                   exp[i],
                   (uintmax_t)i,
                   real[i]);
      }
   }
}

/*
 * Implements equality assertion check.
 */
void
assert_equal(intmax_t    exp,
             intmax_t    real,
             const char *caller,
             int         line,
             const char *message,
             ...)
{
   if (exp != real) {
      VFPRINTF_USERMSG(stderr, message);
      CTEST_ERR("%s:%d  expected %" PRIdMAX ", got %" PRIdMAX,
                caller,
                line,
                exp,
                real);
   }
}

void
assert_equal_u(uintmax_t exp, uintmax_t real, const char *caller, int line)
{
   if (exp != real) {
      CTEST_ERR("%s:%d  expected %" PRIuMAX ", got %" PRIuMAX,
                caller,
                line,
                exp,
                real);
   }
}

void
assert_not_equal(intmax_t    exp,
                 intmax_t    real,
                 const char *caller,
                 int         line,
                 const char *message,
                 ...)
{
   if ((exp) == (real)) {
      VFPRINTF_USERMSG(stderr, message);
      CTEST_ERR("%s:%d  Value should not be %" PRIdMAX, caller, line, real);
   }
}

void
assert_not_equal_u(uintmax_t exp, uintmax_t real, const char *caller, int line)
{
   if ((exp) == (real)) {
      CTEST_ERR("%s:%d  should not be %" PRIuMAX, caller, line, real);
   }
}

void
assert_strnequal(const char *str1,
                 const char *str2,
                 int         n,
                 const char *caller,
                 int         line)
{
   if (strncmp(str1, str2, n)) {
      CTEST_ERR("%s:%d\n"
                "expected: '%.*s'\n"
                "got     : '%.*s'\n",
                caller,
                line,
                n,
                str1,
                n,
                str2);
   }
}

void
assert_interval(intmax_t    exp1,
                intmax_t    exp2,
                intmax_t    real,
                const char *caller,
                int         line)
{
   if (real < exp1 || real > exp2) {
      CTEST_ERR("%s:%d  expected %" PRIdMAX "-%" PRIdMAX ", got %" PRIdMAX,
                caller,
                line,
                exp1,
                exp2,
                real);
   }
}

void
assert_dbl_near(double      exp,
                double      real,
                double      tol,
                const char *caller,
                int         line)
{
   double diff    = exp - real;
   double absdiff = diff;
   /* avoid using fabs and linking with a math lib */
   if (diff < 0) {
      absdiff *= -1;
   }
   if (absdiff > tol) {
      CTEST_ERR("%s:%d  expected %0.3e, got %0.3e (diff %0.3e, tol %0.3e)",
                caller,
                line,
                exp,
                real,
                diff,
                tol);
   }
}

void
assert_dbl_far(double      exp,
               double      real,
               double      tol,
               const char *caller,
               int         line)
{
   double diff    = exp - real;
   double absdiff = diff;
   /* avoid using fabs and linking with a math lib */
   if (diff < 0) {
      absdiff *= -1;
   }
   if (absdiff <= tol) {
      CTEST_ERR("%s:%d  expected %0.3e, got %0.3e (diff %0.3e, tol %0.3e)",
                caller,
                line,
                exp,
                real,
                diff,
                tol);
   }
}

void
assert_null(void *real, const char *caller, int line)
{
   if ((real) != NULL) {
      CTEST_ERR("%s:%d  should be NULL", caller, line);
   }
}

void
assert_not_null(const void *real, const char *caller, int line)
{
   if (real == NULL) {
      CTEST_ERR("%s:%d  should not be NULL", caller, line);
   }
}

void
assert_true(int real, const char *caller, int line, const char *message, ...)
{
   if ((real) == 0) {
      VFPRINTF_USERMSG(stderr, message);
      CTEST_ERR("%s:%d  should be true", caller, line);
   }
}

void
assert_false(int real, const char *caller, int line, const char *message, ...)
{
   if ((real) != 0) {
      VFPRINTF_USERMSG(stderr, message);
      CTEST_ERR("%s:%d  should be false", caller, line);
   }
}

void
assert_fail(const char *caller, int line)
{
   CTEST_ERR("%s:%d  shouldn't come here", caller, line);
}
