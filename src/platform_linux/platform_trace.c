// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform_trace.h"
#include "platform_threads.h"
#include <errno.h>
#include <stdio.h>

bool32       platform_trace_enabled;
static FILE *trace_file;

int
platform_trace_open(const char *filename)
{
   if (trace_file) {
      return EBUSY;
   }
   trace_file = fopen(filename, "wx");
   if (!trace_file) {
      return errno;
   }
   fprintf(trace_file, "start_ns,duration_ns,tid,event,arg0,arg1\n");
   platform_trace_enabled = TRUE;
   return 0;
}

int
platform_trace_close(void)
{
   platform_trace_enabled = FALSE;
   if (!trace_file) {
      return 0;
   }
   int rc = ferror(trace_file) ? EIO : 0;
   if (fclose(trace_file) != 0) {
      rc = errno;
   }
   trace_file = NULL;
   return rc;
}

void
platform_trace_end(const char *event, timestamp start, uint64 arg0, uint64 arg1)
{
   if (!start) {
      return;
   }
   uint64 duration = platform_timestamp_elapsed(start);
   /* One stdio call serializes complete records. These sites trace tasks and
    * exceptional waits, never ordinary individual inserts or lookups. */
   fprintf(trace_file,
           "%lu,%lu,%lu,%s,%lu,%lu\n",
           start,
           duration,
           platform_get_tid(),
           event,
           arg0,
           arg1);
}
