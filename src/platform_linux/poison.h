// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* **********************************************************
 * poison.h must be the LAST header you include.
 * poison.h should only be included by .c files and thus will not
 * help against static inline functions inside of headers but this
 * still drastically reduces the usage of these bad symbols
 *
 * Ideally we would put all this poisoning at the end of platform.h,
 * but many of the standard include files will cause thousands of
 * compile errors that way.  So we include it only in .c files and
 * only as the last include.
 *
 * **********************************************************/

/*
 * Intentionally has no include guard.
 * Poisoning is idempotent.
 */

// Insecure or difficult to use string functions
#pragma GCC poison strlen
#pragma GCC poison strncpy

/* String parsing functions we don't want to use */

#pragma GCC poison atoi
#pragma GCC poison strtol
#pragma GCC poison strtoll
#pragma GCC poison strtoul
#pragma GCC poison strtoull

/* =====================================
 * Everything below are things that have been platformized.
 * If there is a platform/???.c file that needs to do this move it
 * inside the following #ifdef and have the .c file define the macro
 * before including poison.h
 */
#pragma GCC poison aligned_alloc
#pragma GCC poison free
#pragma GCC poison malloc
#pragma GCC poison calloc
#pragma GCC poison realloc

#pragma GCC poison fclose
#pragma GCC poison fopen

#pragma GCC poison fflush
#pragma GCC poison fprintf
#pragma GCC poison fputs

#pragma GCC poison printf
// #pragma GCC poison FILE

#pragma GCC poison pthread_t

#pragma GCC poison strnlen

#pragma GCC poison timespec
#pragma GCC poison clock_gettime

#pragma GCC poison sem_destroy
#pragma GCC poison sem_post
#pragma GCC poison sem_t
#pragma GCC poison sem_wait

/*
 * We would poison these if we could but we can't.
 * prama GCC poison does not allow poisoning existing macros.
 */
#if 0 // Cannot poison existing macros
#   pragma GCC poison                             alloca
#   pragma GCC poison                             assert
#   pragma GCC poison EINVAL ENOMEM EINVAL ENOSPC ETIMEDOUT
#   pragma GCC poison                             pthread_cleanup_pop
#   pragma GCC poison                             pthread_cleanup_push

// macros on some systems.
#   pragma GCC poison                             strncat
#   pragma GCC poison                             strcmp
#endif // Cannot poison existing macros

#pragma GCC        poison __thread
#pragma GCC poison laio_handle
#pragma GCC poison mmap
#pragma GCC poison pthread_attr_destroy
#pragma GCC poison pthread_attr_init
#pragma GCC poison pthread_attr_setdetachstate
#pragma GCC poison pthread_attr_t
#pragma GCC poison pthread_create
#pragma GCC poison pthread_join
#pragma GCC poison pthread_mutex_destroy
#pragma GCC poison pthread_mutex_init
#pragma GCC poison pthread_mutex_lock
#pragma GCC poison pthread_mutex_t
#pragma GCC poison pthread_mutex_unlock
#pragma GCC poison pthread_rwlock_destroy
#pragma GCC poison pthread_rwlock_init
#pragma GCC poison pthread_rwlock_trywrlock
#pragma GCC poison pthread_rwlock_wrlock
#pragma GCC poison pthread_rwlock_tryrdlock
#pragma GCC poison pthread_rwlock_rdlock
#pragma GCC poison pthread_rwlock_t
#pragma GCC poison pthread_rwlock_unlock
#pragma GCC poison strerror
