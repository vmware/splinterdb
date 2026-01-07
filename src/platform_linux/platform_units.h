// Copyright 2018-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/* Can probably be moved into a platform_common directory */

#pragma once

// Data unit constants
#define KiB (1024UL)
#define MiB (KiB * 1024)
#define GiB (MiB * 1024)
#define TiB (GiB * 1024)

// Convert 'x' in unit-specifiers to bytes
#define KiB_TO_B(x) ((x) * KiB)
#define MiB_TO_B(x) ((x) * MiB)
#define GiB_TO_B(x) ((x) * GiB)
#define TiB_TO_B(x) ((x) * TiB)

// Convert 'x' in bytes to 'int'-value with unit-specifiers
#define B_TO_KiB(x) ((x) / KiB)
#define B_TO_MiB(x) ((x) / MiB)
#define B_TO_GiB(x) ((x) / GiB)
#define B_TO_TiB(x) ((x) / TiB)

// For x bytes, returns as int the fractional portion modulo a unit-specifier
#define B_TO_KiB_FRACT(x) ((100 * ((x) % KiB)) / KiB)
#define B_TO_MiB_FRACT(x) ((100 * ((x) % MiB)) / MiB)
#define B_TO_GiB_FRACT(x) ((100 * ((x) % GiB)) / GiB)
#define B_TO_TiB_FRACT(x) ((100 * ((x) % TiB)) / TiB)

// Time unit constants
#define THOUSAND (1000UL)
#define MILLION  (THOUSAND * THOUSAND)
#define BILLION  (THOUSAND * MILLION)

#define USEC_TO_SEC(x)  ((x) / MILLION)
#define USEC_TO_NSEC(x) ((x) * THOUSAND)
#define NSEC_TO_SEC(x)  ((x) / BILLION)
#define NSEC_TO_MSEC(x) ((x) / MILLION)
#define NSEC_TO_USEC(x) ((x) / THOUSAND)
#define SEC_TO_MSEC(x)  ((x) * THOUSAND)
#define SEC_TO_USEC(x)  ((x) * MILLION)
#define SEC_TO_NSEC(x)  ((x) * BILLION)
