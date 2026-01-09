// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/platform_linux/public_platform.h"
#include "platform_status.h"
#include "platform_util.h"

/* Default output file handles for different logging interfaces */
#define PLATFORM_CR "\r"

#define DEFAULT_THROTTLE_INTERVAL_SEC (60)


/*
 * A handle which buffers streamed content to be atomically written to a
 * platform_log_handle.
 */
typedef struct {
   char  *str;
   size_t size;
   FILE  *stream;
} platform_stream_handle;

extern platform_log_handle *Platform_default_log_handle;
extern platform_log_handle *Platform_error_log_handle;

platform_log_handle *
platform_get_stdout_stream(void);

static inline platform_status
platform_open_log_stream(platform_stream_handle *stream)
{
   ZERO_CONTENTS(stream);
   stream->stream = open_memstream(&stream->str, &stream->size);
   if (stream->stream == NULL) {
      return STATUS_NO_MEMORY;
   }
   return STATUS_OK;
}

static inline void
platform_flush_log_stream(platform_stream_handle *stream)
{
   fflush(stream->stream);
}

void
platform_close_log_stream(platform_stream_handle *stream,
                          platform_log_handle    *log_handle);

static inline platform_log_handle *
platform_log_stream_to_log_handle(platform_stream_handle *stream)
{
   return stream->stream;
}

static inline char *
platform_log_stream_to_string(platform_stream_handle *stream)
{
   platform_flush_log_stream(stream);
   return stream->str;
}

#define platform_log(log_handle, ...)                                          \
   do {                                                                        \
      fprintf((log_handle), __VA_ARGS__);                                      \
      fflush(log_handle);                                                      \
   } while (0)

#define platform_default_log(...)                                              \
   do {                                                                        \
      platform_log(Platform_default_log_handle, __VA_ARGS__);                  \
   } while (0)

#define platform_error_log(...)                                                \
   do {                                                                        \
      platform_log(Platform_error_log_handle, __VA_ARGS__);                    \
   } while (0)

#define platform_log_stream(stream, ...)                                       \
   do {                                                                        \
      platform_log_handle *log_handle =                                        \
         platform_log_stream_to_log_handle(stream);                            \
      platform_log(log_handle, __VA_ARGS__);                                   \
   } while (0)

#define platform_throttled_log(sec, log_handle, ...)                           \
   do {                                                                        \
      platform_log(log_handle, __VA_ARGS__);                                   \
   } while (0)

#define platform_throttled_default_log(sec, ...)                               \
   do {                                                                        \
      platform_default_log(__VA_ARGS__);                                       \
   } while (0)

#define platform_throttled_error_log(sec, ...)                                 \
   do {                                                                        \
      platform_error_log(__VA_ARGS__);                                         \
   } while (0)

#define platform_open_log_file(path, mode)                                     \
   ({                                                                          \
      platform_log_handle *lh = fopen(path, mode);                             \
      platform_assert(lh);                                                     \
      lh;                                                                      \
   })

#define platform_close_log_file(log_handle)                                    \
   do {                                                                        \
      fclose(log_handle);                                                      \
   } while (0)

#define FRACTION_FMT(w, s) "%" STRINGIFY_VALUE(w) "." STRINGIFY_VALUE(s) "f"
#define FRACTION_ARGS(f)   ((double)(f).numerator / (double)(f).denominator)
