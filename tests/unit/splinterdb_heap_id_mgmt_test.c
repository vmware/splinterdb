// Copyright 2023-2026 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinterdb_heap_id_mgmt_test.c
 *
 * This is a very specific white-box test to exercise splinterdb_create() and
 * splinterdb_close() interfaces, to exercise the handling of platform heap
 * in the presence of error paths. Usually, the platform_heap is created when
 * splinterdb_create() is done, and the heap is destroyed upon close. There is
 * another way for the calling code to create the platform-heap externally.
 * The cases in this test verify that we can create(), close() and reopen
 * SplinterDB even while managing the externally created platform heap.
 *
 * Tests for the other code-path, where splinterdb_create() itself creates
 * the platform heap, and verifying that the heap is correctly managed in the
 * face of errors exist in other unit-tests, e.g, splinterdb_quick_test.c,
 * task_system_test.c etc.
 * -----------------------------------------------------------------------------
 */
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <assert.h>
#include <string.h>
#include "shmalloc.h"
#include "ctest.h"

#define IS_ALIGNED(ptr, alignment)                                             \
   ((((uintptr_t)(ptr)) & ((uintptr_t)(alignment) - 1)) == 0)

uint64_t log_stress_iterations   = 100000;
uint64_t log_max_allocations     = 1000000;
uint64_t log_max_allocation_size = 22; /* 4 MB */
uint64_t fast_mode               = 0;

typedef struct allocation {
   pthread_spinlock_t lock;
   uint8_t           *p;
   size_t             size;
   size_t             alignment;
   int                c;
} allocation;

allocation *allocs = NULL;

uint64_t
random_size(unsigned int *seed)
{
   uint64_t oom = rand_r(seed) % (log_max_allocation_size + 2);
   if (oom == log_max_allocation_size) {
      return 0;
   }
   if (oom == log_max_allocation_size + 1) {
      return 1ULL << log_max_allocation_size;
   }
   return (1ULL << oom) + (rand_r(seed) % (1ULL << oom));
}

static void
validate_data(uint8_t *ptr, size_t old_size, size_t new_size, int c)
{
   if (fast_mode) {
      if (new_size < old_size) {
         ASSERT_EQUAL(ptr[0], c);
      } else if (old_size > 0) {
         ASSERT_EQUAL(ptr[0], c);
         ASSERT_EQUAL(ptr[old_size - 1], c);
      }
   } else {
      uint64_t size = old_size < new_size ? old_size : new_size;
      for (size_t j = 0; j < size; j++) {
         ASSERT_EQUAL(ptr[j], c);
      }
   }
}

/*
 * Stress test: Random allocations and deallocations
 */
static void *
thread_worker(void *arg)
{
   shmallocator *shm = (shmallocator *)arg;

   unsigned int seed = (unsigned int)(time(NULL) ^ pthread_self());

   uint64_t failed_allocations = 0;

   for (int i = 0; i < (1ULL << log_stress_iterations); i++) {
      /* Randomly decide: allocate or deallocate */
      int idx = rand_r(&seed) % (1ULL << log_max_allocations);
      while (pthread_spin_trylock(&allocs[idx].lock) != 0) {
         idx = rand_r(&seed) % (1ULL << log_max_allocations);
      }

      if (allocs[idx].p == NULL || rand_r(&seed) % 2) {
         ASSERT_TRUE(allocs[idx].p != NULL || allocs[idx].size == 0);

         /* Allocate */
         size_t size = random_size(&seed);
         size_t alignment;

         uint8_t *ptr;
         if (allocs[idx].p == NULL && rand_r(&seed) % 2) {
            alignment = 1ULL << (rand_r(&seed) % 13);
            ptr       = shmalloc(shm, alignment, size);
         } else {
            alignment = 1;
            ptr       = shrealloc(shm, allocs[idx].p, size);
         }

         if (allocs[idx].p != NULL && ptr != NULL) {
            validate_data(ptr, allocs[idx].size, size, allocs[idx].c);
         }

         if (ptr != NULL || size == 0) {
            if (allocs[idx].p == NULL) {
               allocs[idx].alignment = alignment;
            }
            allocs[idx].p    = ptr;
            allocs[idx].size = size;


            ASSERT_TRUE(IS_ALIGNED(ptr, allocs[idx].alignment));

            /* Fill with pattern based on thread number */
            allocs[idx].c = (unsigned char)(seed & 0xFF);
            if (fast_mode && size > 0) {
               ptr[0]        = allocs[idx].c;
               ptr[size - 1] = allocs[idx].c;
            } else {
               memset(ptr, allocs[idx].c, size);
            }
         } else {
            failed_allocations++;
         }
      } else {
         /* Deallocate a random allocation */
         uint8_t *ptr  = allocs[idx].p;
         size_t   size = allocs[idx].size;

         validate_data(ptr, size, size, allocs[idx].c);

         shfree(shm, ptr);

         allocs[idx].p    = NULL;
         allocs[idx].size = 0;
      }

      pthread_spin_unlock(&allocs[idx].lock);
   }

   /* Free all remaining allocations */
   for (int i = 0; i < (1ULL << log_max_allocations); i++) {
      if (pthread_spin_trylock(&allocs[i].lock) == 0) {
         if (allocs[i].p != NULL) {
            shfree(shm, allocs[i].p);
            allocs[i].p    = NULL;
            allocs[i].size = 0;
         }
         pthread_spin_unlock(&allocs[i].lock);
      }
   }

   printf("Failed allocations: %lu\n", failed_allocations);
   return NULL;
}


static void
stress_test(uint64_t lg_stress_iterations,
            uint64_t lg_max_allocations,
            uint64_t lg_max_allocation_size,
            uint64_t fst_mode,
            uint64_t log_shared_region_size,
            uint64_t num_processes,
            uint64_t num_threads_per_process)
{
   log_stress_iterations   = lg_stress_iterations;
   log_max_allocations     = lg_max_allocations;
   log_max_allocation_size = lg_max_allocation_size;
   fast_mode               = fst_mode;

   allocs = mmap(NULL,
                 sizeof(allocation) * (1ULL << log_max_allocations),
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED | MAP_ANONYMOUS,
                 -1,
                 0);
   if (allocs == MAP_FAILED) {
      perror("mmap failed");
      exit(1);
   }

   for (int i = 0; i < (1ULL << log_max_allocations); i++) {
      pthread_spin_init(&allocs[i].lock, PTHREAD_PROCESS_SHARED);
   }

   shmallocator *shm = mmap(NULL,
                            (1ULL << log_shared_region_size),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS,
                            -1,
                            0);
   if (shm == MAP_FAILED) {
      perror("mmap failed");
      exit(1);
   }


   if (shmallocator_init(
          shm, (1ULL << log_max_allocations), (1ULL << log_shared_region_size))
       != 0)
   {
      perror("shmallocator_init failed");
      exit(1);
   }

   ASSERT_TRUE(num_processes <= 64);

   pid_t pids[64];
   memset(pids, 0, sizeof(pids));

   for (int i = 0; i < num_processes - 1; i++) {
      pid_t pid = fork();
      ASSERT_TRUE(pid >= 0);
      if (pid == 0) {
         printf("Child %d created\n", getpid());
         break;
      } else {
         pids[i] = pid;
      }
   }

   ASSERT_TRUE(num_threads_per_process <= 64);
   pthread_t threads[64];
   for (int i = 0; i < num_threads_per_process - 1; i++) {
      ASSERT_TRUE(pthread_create(&threads[i], NULL, thread_worker, shm) == 0);
   }

   thread_worker(shm);

   for (int i = 0; i < num_threads_per_process - 1; i++) {
      ASSERT_TRUE(pthread_join(threads[i], NULL) == 0);
   }

   // If we are the parent process, wait for all children to complete
   if (num_processes > 1) {
      if (pids[num_processes - 2] != 0) {
         for (int i = 0; i < num_processes - 1; i++) {
            int child_pid;
            int status;
            child_pid = wait(&status);
            ASSERT_TRUE(child_pid != -1);
            ASSERT_EQUAL(WIFEXITED(status), 1);
            ASSERT_EQUAL(WEXITSTATUS(status), 0);
         }
         shmallocator_deinit(shm);
      }
   }


   munmap(allocs, sizeof(allocation) * (1ULL << log_max_allocations));
   munmap(shm, (1ULL << log_shared_region_size));

   if (num_processes > 1 && pids[num_processes - 2] == 0) {
      printf("Child %d completed\n", getpid());
      exit(0);
   }
}


CTEST_DATA(shmalloc){};

CTEST_SETUP(shmalloc) {}

CTEST_TEARDOWN(shmalloc) {}

CTEST2(shmalloc, stress_test)
{
   stress_test(18, 16, 22, 0, 32, 4, 4);
}
