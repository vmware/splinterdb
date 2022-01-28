// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __UTIL_H
#define __UTIL_H

typedef struct writable_buffer writable_buffer;

uint64
writable_buffer_length(writable_buffer *wb);

/* Allocates memory as needed. Returns TRUE on success. */
bool
writable_buffer_set_length(writable_buffer *wb, uint64 newlength);

/* Returns a ptr to the data region held by this writable_buffer */
void *
writable_buffer_data(writable_buffer *wb);

#endif /* __UTIL_H */
