// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _TRANSACTION_DATA_CONFIG_H_
#define _TRANSACTION_DATA_CONFIG_H_

#include "splinterdb/data.h"

typedef struct transaction_data_config {
   data_config  super;
   data_config *application_data_config;
} transaction_data_config;

void
transaction_data_config_init(data_config             *in_cfg, // IN
                             transaction_data_config *out_cfg // OUT
);

#endif // _TRANSACTION_DATA_CONFIG_H_
