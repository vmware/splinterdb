#pragma once

#include <stdint.h>
#include <stddef.h>

typedef struct shmallocator shmallocator;

int
shmallocator_init(shmallocator *shmallocator,
                  uint64_t      chunk_table_size,
                  size_t        size);

size_t
shmallocator_size(shmallocator *shmallocator);

void
shmallocator_deinit(shmallocator *shmallocator);

void *
shmalloc(shmallocator *shmallocator, size_t alignment, size_t size);

void
shfree(shmallocator *shmallocator, void *ptr);

void *
shrealloc(shmallocator *shmallocator, void *ptr, size_t size);