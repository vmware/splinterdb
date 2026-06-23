// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 *------------------------------------------------------------------------------
 * notification.c --
 *
 *     Public SplinterDB notification API and internal completion helpers.
 *------------------------------------------------------------------------------
 */

#include "notification.h"
#include "platform_assert.h"
#include "platform_condvar.h"
#include "platform_heap.h"
#include "poison.h"

typedef enum notification_mode {
   NOTIFICATION_MODE_BLOCKING,
   NOTIFICATION_MODE_POLLING,
} notification_mode;

typedef struct notification {
   notification_mode mode;
   bool32            complete;
   platform_status   status;
   void             *user_data;
   platform_condvar  cv;
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

static inline int
platform_status_to_int(const platform_status status)
{
   return status.r;
}

static void
splinterdb_notification_init_common(splinterdb_notification *note,
                                    notification_mode        mode,
                                    void                    *user_data)
{
   notification *n = notification_from_splinterdb(note);

   n->mode      = mode;
   n->complete  = FALSE;
   n->status    = STATUS_OK;
   n->user_data = user_data;

   platform_status rc = platform_condvar_init(&n->cv, TRUE);
   platform_assert_status_ok(rc);
}

void
splinterdb_notification_init_blocking(splinterdb_notification *note)
{
   splinterdb_notification_init_common(note, NOTIFICATION_MODE_BLOCKING, NULL);
}

void
splinterdb_notification_init_polling(splinterdb_notification *note,
                                     void                    *user_data)
{
   splinterdb_notification_init_common(
      note, NOTIFICATION_MODE_POLLING, user_data);
}

void
splinterdb_notification_deinit(splinterdb_notification *note)
{
   notification *n = notification_from_splinterdb(note);

   platform_condvar_destroy(&n->cv);
}

_Bool
splinterdb_notification_poll(const splinterdb_notification *note, int *status)
{
   notification *n =
      notification_from_splinterdb((splinterdb_notification *)note);

   platform_status rc = platform_condvar_lock(&n->cv);
   platform_assert_status_ok(rc);

   bool32 complete = n->complete;
   if (status != NULL) {
      *status = platform_status_to_int(n->status);
   }

   rc = platform_condvar_unlock(&n->cv);
   platform_assert_status_ok(rc);

   return complete;
}

void *
splinterdb_notification_user_data(const splinterdb_notification *note)
{
   const notification *n = notification_from_const_splinterdb(note);

   return n->user_data;
}

bool32
splinterdb_notification_is_blocking(splinterdb_notification *note)
{
   if (note == NULL) {
      return FALSE;
   }

   notification *n = notification_from_splinterdb(note);

   return n->mode == NOTIFICATION_MODE_BLOCKING;
}

void
splinterdb_notification_complete(splinterdb_notification *note,
                                 platform_status          status)
{
   if (note == NULL) {
      return;
   }

   notification *n = notification_from_splinterdb(note);

   platform_status rc = platform_condvar_lock(&n->cv);
   platform_assert_status_ok(rc);

   platform_assert(!n->complete);
   n->status   = status;
   n->complete = TRUE;

   rc = platform_condvar_broadcast(&n->cv);
   platform_assert_status_ok(rc);

   rc = platform_condvar_unlock(&n->cv);
   platform_assert_status_ok(rc);
}
