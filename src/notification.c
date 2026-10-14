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
#include "platform_typed_alloc.h"
#include "poison.h"

typedef struct notification {
   bool32           internally_owned;
   uint32           references;
   platform_heap_id hid;
   bool32           complete;
   platform_status  status;
   void            *user_data;
   platform_condvar cv;
} notification;

_Static_assert(sizeof(notification) <= sizeof(splinterdb_notification),
               "sizeof(splinterdb_notification) is too small");

_Static_assert(__alignof__(splinterdb_notification)
                  == __alignof__(notification),
               "mismatched alignment for splinterdb_notification");

static inline notification *
notification_from_splinterdb(splinterdb_notification *note)
{
   platform_assert(note != NULL);
   platform_assert(note != SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET);
   return (notification *)note;
}

static inline const notification *
notification_from_const_splinterdb(const splinterdb_notification *note)
{
   platform_assert(note != NULL);
   platform_assert(note != SPLINTERDB_NOTIFICATION_FIRE_AND_FORGET);
   return (const notification *)note;
}

static inline int
platform_status_to_int(const platform_status status)
{
   return status.r;
}

static platform_status
notification_init(splinterdb_notification *note,
                  bool32                   internally_owned,
                  platform_heap_id         hid,
                  void                    *user_data)
{
   notification *n = notification_from_splinterdb(note);

   n->internally_owned = internally_owned;
   n->references       = internally_owned ? 2 : 0;
   n->hid              = hid;
   n->complete         = FALSE;
   n->status           = STATUS_OK;
   n->user_data        = user_data;

   return platform_condvar_init(&n->cv, TRUE);
}

platform_status
notification_create_blocking(platform_heap_id          hid,
                             splinterdb_notification **out)
{
   platform_assert(out != NULL);
   *out = NULL;

   splinterdb_notification *note = TYPED_MALLOC(hid, note);
   if (note == NULL) {
      return STATUS_NO_MEMORY;
   }

   platform_status rc = notification_init(note, TRUE, hid, NULL);
   if (!SUCCESS(rc)) {
      platform_free(hid, note);
      return rc;
   }

   *out = note;
   return STATUS_OK;
}

void
notification_release_blocking(splinterdb_notification *note)
{
   notification *n = notification_from_splinterdb(note);
   platform_assert(n->internally_owned);

   uint32 old_references =
      __atomic_fetch_sub(&n->references, 1, __ATOMIC_ACQ_REL);
   platform_assert(old_references > 0);
   if (old_references == 1) {
      platform_heap_id hid = n->hid;
      platform_condvar_destroy(&n->cv);
      platform_free(hid, note);
   }
}

void
splinterdb_notification_init_polling(splinterdb_notification *note,
                                     void                    *user_data)
{
   platform_status rc =
      notification_init(note, FALSE, PROCESS_PRIVATE_HEAP_ID, user_data);
   platform_assert_status_ok(rc);
}

void
splinterdb_notification_deinit(splinterdb_notification *note)
{
   notification *n = notification_from_splinterdb(note);
   platform_assert(!n->internally_owned);

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

   // A caller-owned notification may be destroyed as soon as we unlock. Its
   // ownership must be read before publishing completion to a polling caller.
   bool32 internally_owned = n->internally_owned;
   rc                      = platform_condvar_unlock(&n->cv);
   platform_assert_status_ok(rc);

   if (internally_owned) {
      // Retain the completion reference until after the final access to cv.
      notification_release_blocking(note);
   }
}
