// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform_log.h"
#include "platform_assert.h"

// By default, platform_default_log() messages are sent to /dev/null
// and platform_error_log() messages go to stderr (see below).
//
// Use platform_set_log_streams() to send the log messages elsewhere.
platform_log_handle *Platform_default_log_handle = NULL;
platform_log_handle *Platform_error_log_handle   = NULL;

// This function is run automatically at library-load time
void __attribute__((constructor))
platform_init_log_file_handles(void)
{
   FILE *dev_null_file = fopen("/dev/null", "w");
   platform_assert(dev_null_file != NULL);

   Platform_default_log_handle = dev_null_file;
   Platform_error_log_handle   = stderr;
}

// Set the streams where informational and error messages will be printed.
void
platform_set_log_streams(platform_log_handle *info_stream,
                         platform_log_handle *error_stream)
{
   platform_assert(info_stream != NULL);
   platform_assert(error_stream != NULL);
   Platform_default_log_handle = info_stream;
   Platform_error_log_handle   = error_stream;
}

// Return the stdout log-stream handle
platform_log_handle *
platform_get_stdout_stream(void)
{
   return Platform_default_log_handle;
}
