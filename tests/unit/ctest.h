/* Copyright 2011-2021 Bas van den Berg
 * Copyright 2018-2021 VMware, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CTEST_H
#define CTEST_H

#ifdef __GNUC__
#define CTEST_IMPL_FORMAT_PRINTF(a, b) __attribute__ ((format(printf, a, b)))
#else
#define CTEST_IMPL_FORMAT_PRINTF(a, b)
#endif

#include <inttypes.h> /* intmax_t, uintmax_t, PRI* */
#include <stddef.h> /* size_t */

typedef void (*ctest_nullary_run_func)(void);
typedef void (*ctest_unary_run_func)(void*);
typedef void (*ctest_setup_func)(void*);
typedef void (*ctest_teardown_func)(void*);

union ctest_run_func_union {
    ctest_nullary_run_func nullary;
    ctest_unary_run_func unary;
};

#define CTEST_IMPL_PRAGMA(x) _Pragma (#x)

#if defined(__GNUC__)
#if defined(__clang__) || __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 6)
/* the GCC argument will work for both gcc and clang  */
#define CTEST_IMPL_DIAG_PUSH_IGNORED(w) \
    CTEST_IMPL_PRAGMA(GCC diagnostic push) \
    CTEST_IMPL_PRAGMA(GCC diagnostic ignored "-W" #w)
#define CTEST_IMPL_DIAG_POP() \
    CTEST_IMPL_PRAGMA(GCC diagnostic pop)
#else
/* the push/pop functionality wasn't in gcc until 4.6, fallback to "ignored"  */
#define CTEST_IMPL_DIAG_PUSH_IGNORED(w) \
    CTEST_IMPL_PRAGMA(GCC diagnostic ignored "-W" #w)
#define CTEST_IMPL_DIAG_POP()
#endif
#else
/* leave them out entirely for non-GNUC compilers  */
#define CTEST_IMPL_DIAG_PUSH_IGNORED(w)
#define CTEST_IMPL_DIAG_POP()
#endif

/*
 * ************************************************************************
 * struct ctest: Main structure defining test suites, test cases to run.
 * Through a whole bunch of preprocessing macros, an array of these test
 * suite / test case definitions will be constructed, of this struct type.
 * ************************************************************************
 */
struct ctest {
    const char* ssname;  // suite name
    const char* ttname;  // test name
    union ctest_run_func_union run;

    void* data;
    ctest_setup_func* setup;
    ctest_teardown_func* teardown;

    int skip;

    unsigned int magic;
};

/*
 * Global handles to command-line args are provided so that we can access
 * argc/argv indirectly thru these global variables inside setup methods.
 */
extern int Ctest_argc;
extern const char **Ctest_argv;

#define CTEST_IMPL_NAME(name) ctest_##name
#define CTEST_IMPL_FNAME(sname, tname) CTEST_IMPL_NAME(sname##_##tname##_run)
#define CTEST_IMPL_TNAME(sname, tname) CTEST_IMPL_NAME(sname##_##tname)
#define CTEST_IMPL_DATA_SNAME(sname) CTEST_IMPL_NAME(sname##_data)
#define CTEST_IMPL_DATA_TNAME(sname, tname) CTEST_IMPL_NAME(sname##_##tname##_data)
#define CTEST_IMPL_SETUP_FNAME(sname) CTEST_IMPL_NAME(sname##_setup)
#define CTEST_IMPL_SETUP_FPNAME(sname) CTEST_IMPL_NAME(sname##_setup_ptr)
#define CTEST_IMPL_SETUP_TPNAME(sname, tname) CTEST_IMPL_NAME(sname##_##tname##_setup_ptr)
#define CTEST_IMPL_TEARDOWN_FNAME(sname) CTEST_IMPL_NAME(sname##_teardown)
#define CTEST_IMPL_TEARDOWN_FPNAME(sname) CTEST_IMPL_NAME(sname##_teardown_ptr)
#define CTEST_IMPL_TEARDOWN_TPNAME(sname, tname) CTEST_IMPL_NAME(sname##_##tname##_teardown_ptr)

#define CTEST_IMPL_MAGIC (0xdeadbeef)
#ifdef __APPLE__
#define CTEST_IMPL_SECTION __attribute__ ((used, section ("__DATA, .ctest"), aligned(1)))
#else
#define CTEST_IMPL_SECTION __attribute__ ((used, section (".ctest"), aligned(1)))
#endif

#define CTEST_IMPL_STRUCT(sname, tname, tskip, tdata, tsetup, tteardown) \
    static struct ctest CTEST_IMPL_TNAME(sname, tname) CTEST_IMPL_SECTION = { \
        #sname, \
        #tname, \
        { (ctest_nullary_run_func) CTEST_IMPL_FNAME(sname, tname) }, \
        tdata, \
        (ctest_setup_func*) tsetup, \
        (ctest_teardown_func*) tteardown, \
        tskip, \
        CTEST_IMPL_MAGIC, \
    }

#define CTEST_SETUP(sname) \
    static void CTEST_IMPL_SETUP_FNAME(sname)(struct CTEST_IMPL_DATA_SNAME(sname)* data); \
    static void (*CTEST_IMPL_SETUP_FPNAME(sname))(struct CTEST_IMPL_DATA_SNAME(sname)*) = &CTEST_IMPL_SETUP_FNAME(sname); \
    static void CTEST_IMPL_SETUP_FNAME(sname)(struct CTEST_IMPL_DATA_SNAME(sname)* data)

#define CTEST_TEARDOWN(sname) \
    static void CTEST_IMPL_TEARDOWN_FNAME(sname)(struct CTEST_IMPL_DATA_SNAME(sname)* data); \
    static void (*CTEST_IMPL_TEARDOWN_FPNAME(sname))(struct CTEST_IMPL_DATA_SNAME(sname)*) = &CTEST_IMPL_TEARDOWN_FNAME(sname); \
    static void CTEST_IMPL_TEARDOWN_FNAME(sname)(struct CTEST_IMPL_DATA_SNAME(sname)* data)

#define CTEST_DATA(sname) \
    struct CTEST_IMPL_DATA_SNAME(sname); \
    static void (*CTEST_IMPL_SETUP_FPNAME(sname))(struct CTEST_IMPL_DATA_SNAME(sname)*); \
    static void (*CTEST_IMPL_TEARDOWN_FPNAME(sname))(struct CTEST_IMPL_DATA_SNAME(sname)*); \
    struct CTEST_IMPL_DATA_SNAME(sname)

#define CTEST_IMPL_CTEST(sname, tname, tskip) \
    static void CTEST_IMPL_FNAME(sname, tname)(void); \
    CTEST_IMPL_STRUCT(sname, tname, tskip, NULL, NULL, NULL); \
    static void CTEST_IMPL_FNAME(sname, tname)(void)

#define CTEST_IMPL_CTEST2(sname, tname, tskip) \
    static struct CTEST_IMPL_DATA_SNAME(sname) CTEST_IMPL_DATA_TNAME(sname, tname); \
    static void CTEST_IMPL_FNAME(sname, tname)(struct CTEST_IMPL_DATA_SNAME(sname)* data); \
    CTEST_IMPL_STRUCT(sname, tname, tskip, &CTEST_IMPL_DATA_TNAME(sname, tname), &CTEST_IMPL_SETUP_FPNAME(sname), &CTEST_IMPL_TEARDOWN_FPNAME(sname)); \
    static void CTEST_IMPL_FNAME(sname, tname)(struct CTEST_IMPL_DATA_SNAME(sname)* data)


void CTEST_LOG(const char* fmt, ...) CTEST_IMPL_FORMAT_PRINTF(1, 2);
void CTEST_ERR(const char* fmt, ...) CTEST_IMPL_FORMAT_PRINTF(1, 2);  // doesn't return

#define CTEST(sname, tname) CTEST_IMPL_CTEST(sname, tname, 0)
#define CTEST_SKIP(sname, tname) CTEST_IMPL_CTEST(sname, tname, 1)

#define CTEST2(sname, tname) CTEST_IMPL_CTEST2(sname, tname, 0)
#define CTEST2_SKIP(sname, tname) CTEST_IMPL_CTEST2(sname, tname, 1)


void assert_str(const char* exp, const char* real, const char* caller, int line);
#define ASSERT_STR(exp, real) assert_str(exp, real, __FILE__, __LINE__)

void assert_wstr(const wchar_t *exp, const wchar_t *real, const char* caller, int line);
#define ASSERT_WSTR(exp, real) assert_wstr(exp, real, __FILE__, __LINE__)

void assert_data(const unsigned char* exp, size_t expsize,
                 const unsigned char* real, size_t realsize,
                 const char* caller, int line);
#define ASSERT_DATA(exp, expsize, real, realsize) \
    assert_data(exp, expsize, real, realsize, __FILE__, __LINE__)

void assert_equal(intmax_t exp, intmax_t real, const char* caller, int line,
                  const char * message, ...);
#define ASSERT_EQUAL(exp, real, ...) assert_equal(exp, real, __FILE__, __LINE__, "" __VA_ARGS__)

// strcmp() of 2 null-terminated strings
#define ASSERT_STREQ(str1, str2, ...) assert_equal(strcmp(str1, str2), 0, __FILE__, __LINE__, "" __VA_ARGS__)

// strncmp() of 2 strings, which may not be null-terminated
void assert_strnequal(const char *str1, const char *str2, int n, const char* caller, int line);
#define ASSERT_STREQN(str1, str2, n, ...) assert_equal(strncmp(str1, str2, n), 0, __FILE__, __LINE__, "" __VA_ARGS__)

void assert_equal_u(uintmax_t exp, uintmax_t real, const char* caller, int line);
#define ASSERT_EQUAL_U(exp, real) assert_equal_u(exp, real, __FILE__, __LINE__)

void assert_not_equal(intmax_t exp, intmax_t real, const char* caller, int line, const char * message, ...);
#define ASSERT_NOT_EQUAL(exp, real, ...) assert_not_equal(exp, real, __FILE__, __LINE__, "" __VA_ARGS__)

void assert_not_equal_u(uintmax_t exp, uintmax_t real, const char* caller, int line);
#define ASSERT_NOT_EQUAL_U(exp, real) assert_not_equal_u(exp, real, __FILE__, __LINE__)

void assert_interval(intmax_t exp1, intmax_t exp2, intmax_t real, const char* caller, int line);
#define ASSERT_INTERVAL(exp1, exp2, real) assert_interval(exp1, exp2, real, __FILE__, __LINE__)

void assert_null(void* real, const char* caller, int line);
#define ASSERT_NULL(real) assert_null((void*)real, __FILE__, __LINE__)

void assert_not_null(const void* real, const char* caller, int line);
#define ASSERT_NOT_NULL(real) assert_not_null(real, __FILE__, __LINE__)

void assert_true(int real, const char* caller, int line, const char * message, ...);
#define ASSERT_TRUE(real, ...) assert_true(real, __FILE__, __LINE__, "" __VA_ARGS__)

void assert_false(int real, const char* caller, int line, const char * message, ...);
#define ASSERT_FALSE(real, ...) assert_false(real, __FILE__, __LINE__, "" __VA_ARGS__)

void assert_fail(const char* caller, int line);
#define ASSERT_FAIL() assert_fail(__FILE__, __LINE__)

void assert_dbl_near(double exp, double real, double tol, const char* caller, int line);
#define ASSERT_DBL_NEAR(exp, real) assert_dbl_near(exp, real, 1e-4, __FILE__, __LINE__)
#define ASSERT_DBL_NEAR_TOL(exp, real, tol) assert_dbl_near(exp, real, tol, __FILE__, __LINE__)

void assert_dbl_far(double exp, double real, double tol, const char* caller, int line);
#define ASSERT_DBL_FAR(exp, real) assert_dbl_far(exp, real, 1e-4, __FILE__, __LINE__)
#define ASSERT_DBL_FAR_TOL(exp, real, tol) assert_dbl_far(exp, real, tol, __FILE__, __LINE__)

/*
 * Extract out common code to print, when an assertion fails, a user-supplied
 * message with args.
 */
#define VFPRINTF_USERMSG(fh, message)               \
    do {                                            \
        va_list varargs;                            \
        va_start(varargs, (message));               \
        vfprintf((fh), (message), varargs);         \
        va_end(varargs);                            \
    } while (0)

#endif
