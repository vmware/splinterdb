// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#if EXPERIMENTAL_MODE_TICTOC_DISK || EXPERIMENTAL_MODE_STO_DISK

#   include "splinterdb/data.h"

typedef struct transactional_data_config {
   data_config        super;
   const data_config *application_data_config;
} transactional_data_config;

void
transactional_data_config_init(data_config               *in_cfg, // IN
                               transactional_data_config *out_cfg // OUT
);

#endif
