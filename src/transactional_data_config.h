// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _TRANSACTIONAL_DATA_CONFIG_H_
#define _TRANSACTIONAL_DATA_CONFIG_H_

#include "splinterdb/data.h"

typedef struct transactional_data_config {
   data_config        super;
   const data_config *application_data_config;
} transactional_data_config;

void
transactional_data_config_init(data_config               *in_cfg, // IN
                               transactional_data_config *out_cfg // OUT
);

#endif // _TRANSACTIONAL_DATA_CONFIG_H_
