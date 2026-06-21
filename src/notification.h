// Copyright 2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "splinterdb/splinterdb.h"
#include "platform_status.h"

bool32
splinterdb_notification_is_blocking(splinterdb_notification *note);

void
splinterdb_notification_complete(splinterdb_notification *note,
                                 platform_status          status);
