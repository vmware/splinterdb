// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef __PUBLIC_UTIL_H__
#define __PUBLIC_UTIL_H__

typedef struct writable_buffer writable_buffer;

uint64
writable_buffer_length(writable_buffer *wb);

/* Allocates memory as needed. Returns TRUE on success. */
bool
writable_buffer_resize(writable_buffer *wb, uint64 newlength);

/* Returns a ptr to the data region held by this writable_buffer */
void *
writable_buffer_data(writable_buffer *wb);

#endif /* __PUBLIC_UTIL_H__ */
