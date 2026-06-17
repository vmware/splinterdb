// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/splinterdb.h"
#include "platform_assert.h"
#include "platform_condvar.h"
#include "platform_heap.h"
#include "platform_status.h"

typedef enum notification_mode {
   NOTIFICATION_MODE_BLOCKING,
   NOTIFICATION_MODE_POLLING,
   NOTIFICATION_MODE_CALLBACK,
} notification_mode;

typedef struct notification {
   notification_mode                mode;
   bool32                           complete;
   platform_status                  status;
   void                            *user_data;
   splinterdb_notification_callback callback;
   platform_condvar                 cv;
} notification;

_Static_assert(sizeof(notification) <= sizeof(splinterdb_notification),
               "sizeof(splinterdb_notification) is too small");

_Static_assert(__alignof__(splinterdb_notification)
                  == __alignof__(notification),
               "mismatched alignment for splinterdb_notification");

static inline notification *
notification_from_splinterdb(splinterdb_notification *note)
{
   return (notification *)note;
}

static inline const notification *
notification_from_const_splinterdb(const splinterdb_notification *note)
{
   return (const notification *)note;
}

bool32
splinterdb_notification_is_blocking(splinterdb_notification *note);

void
splinterdb_notification_complete(splinterdb_notification *note,
                                 platform_status          status);
