// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/splinterdb.h"
#include "platform_heap.h"
#include "platform_status.h"

// Allocate an internal blocking notification from the database heap, so that
// another process may complete it when the database uses shared memory. On
// success, the waiter and the operation each own one reference. The waiter
// releases its reference after waiting, including when waiting fails before
// completion. Completion releases the operation's reference. If submission
// fails without arranging completion, the submitter must release both.
platform_status
notification_create_blocking(platform_heap_id          hid,
                             splinterdb_notification **out);

void
notification_release_blocking(splinterdb_notification *note);

// NULL is the internal no-recipient value; public sentinels are translated by
// the API before reaching this helper.
void
splinterdb_notification_complete(splinterdb_notification *note,
                                 platform_status          status);
