// Copyright 2018-2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * ----------------------------------------------------------------------------
 * splinter_io_apis_test.c --
 *
 * This test exercises core IO interfaces that are used by SplinterDB
 * to verify that these interfaces work as expected when executed:
 *
 * - Using threads, with Splinter using default malloc()-memory allocation
 *
 * The test design is quite simple:
 *
 * - Starting with a default IO configuration, create a device of some size
 *   and using Sync IO interfaces driven by the main program, fill the
 *   device out with some known data. Read back each page worth of data from
 *   the file using sync-read and verify correctness. This case covers the
 *   correctness check for basic sync read/write APIs driven by the main thread.
 *
 * - Using the data generated above, fire-up n-threads. Assign contiguous
 *   sections of the file to each thread. Each thread verifies previously
 *   written data using sync-read. Then, each thread writes new data to its
 *   section using sync-writes, and verifies using sync-reads.
 *
 * NOTE: To support multi-process execution using shared-memory, the lower
 *  level AIO interfaces were changed such that each thread / process sets
 *  up its own IO-context. Thread registration / de-registration interacts
 *  with this IO-context setup & cleanup. The different cases in this test,
 *  the multi-threaded execution and especially the execution of IO tests
 *  in a fork()'ed sub-process -- all exercise and validate these interfaces.
 *  Hence, there is no separate lower-level API exerciser unit-test.
 * ----------------------------------------------------------------------------
 */
#include <sys/wait.h>

#include "platform.h"
#include "config.h"
#include "io.h"
#include "core.h" // Needed for trunk_get_scratch_size()
#include "task.h"

/*
 * Structure to package arguments needed by test-case functions. This packaging
 * allows us to pass a set of args around. These are then supplied by worker
 * functions invoked by pthreads to invoke the work-horse function(s).
 * Most fields are used by all functions receiving this arg. Some are for diags.
 */
typedef struct io_test_fn_args {
   platform_heap_id    hid;
   io_config          *io_cfgp;
   platform_io_handle *io_hdlp;
   task_system        *tasks;
   uint64              start_addr;
   uint64              end_addr;
   char                stamp_char;
   platform_thread     thread;
   const char         *whoami; // 'Parent' or 'Child'
} io_test_fn_args;

/* Whether to display verbose-progress from each thread's activity */
bool32 Verbose_progress = FALSE;

/*
 * Global to track address of allocated IO-handle allocated in parent process.
 * Used for cross-checking in child's context after fork().
 */
platform_io_handle *Parent_io_handle = NULL;

/*
 * Different test cases in this test drive multiple threads each doing one
 * type of activity. Declare the interface of such thread handler functions.
 */
typedef void (*test_io_thread_hdlr)(void *arg);

/* Device size; small one is good enough for IO APIs testing */
#define DEVICE_SIZE_MB 128

#define HEAP_SIZE_MB 256

/* SplinterDB device size; small one is good enough for IO APIs testing */
#define SPLINTER_DEVICE_SIZE_MB 128

/*
 * Use small hard-coded # of threads to avoid allocating memory for
 * thread-specific arrays of parameters. It's sufficient to shake out the
 * IO sub-system APIs with just small # of threads.
 */
#define NUM_THREADS 3

/*
 * Each thread will perform async I/O (Read followed by Write) for these
 * many pages. Test will exercise NUM_THREADS threads performing async
 * RW IO, on these many pages / thread.
 */
#define NUM_PAGES_RW_ASYNC_PER_THREAD 16

// Function prototypes
static platform_status
test_sync_writes(platform_heap_id    hid,
                 io_config          *io_cfgp,
                 platform_io_handle *io_hdlp,
                 uint64              start_addr,
                 uint64              end_addr,
                 char                stamp_char);

static platform_status
test_sync_reads(platform_heap_id    hid,
                io_config          *io_cfgp,
                platform_io_handle *io_hdlp,
                uint64              start_addr,
                uint64              end_addr,
                char                stamp_char);

static platform_status
test_sync_write_reads_by_threads(io_test_fn_args *io_test_param,
                                 int              nthreads,
                                 const char      *whoami);

static platform_status
test_async_reads(platform_heap_id    hid,
                 io_config          *io_cfgp,
                 platform_io_handle *io_hdlp,
                 uint64              start_addr,
                 char                stamp_char,
                 const char         *whoami);

static platform_status
test_async_reads_by_threads(io_test_fn_args *io_test_param,
                            int              nthreads,
                            const char      *whoami);

static void
load_thread_params(io_test_fn_args *io_test_param,
                   io_test_fn_args *thread_params,
                   int              nthreads);

static platform_status
do_n_thread_creates(const char         *thread_type,
                    uint64              num_threads,
                    io_test_fn_args    *params,
                    test_io_thread_hdlr thread_hdlr);

void
test_sync_writes_worker(void *arg);

void
test_sync_reads_worker(void *arg);

void
test_async_reads_worker(void *arg);

/*
 * Helper inline functions.
 */
/*
 * Convert the range {start, end}-address of the device on which we
 * want to do IO to # of pages. Distribute this across # of threads to
 * return the # of pages / thread to perform IO.
 */
static inline uint64
npages_per_thread(io_test_fn_args *io_test_param, int nthreads)
{
   return (((io_test_param->end_addr - io_test_param->start_addr)
            / io_test_param->io_cfgp->page_size)
           / nthreads);
}

/*
 * ----------------------------------------------------------------------------
 * splinter_io_apis_test() - Entry point 'main' for SplinterDB IO APIs testing.
 * ----------------------------------------------------------------------------
 */
int
splinter_io_apis_test(int argc, char *argv[])
{
   uint64 heap_capacity = (HEAP_SIZE_MB * MiB); // small heap is sufficient.

   // Move past the 1st arg which will be the driving tag, 'io_apis_test'.
   argc--;
   argv++;

   // Do minimal IO config setup, using default IO values.
   master_config master_cfg;

   // Initialize the IO sub-system configuration.
   config_set_defaults(&master_cfg);

   // Parse config-related command-line arguments. Only expecting to support:
   // --verbose-progress
   platform_status rc = config_parse(&master_cfg, 1, argc, argv);
   if (!SUCCESS(rc)) {
      return -1;
   }

   // Create a heap for io system's memory allocation.
   platform_heap_id hid = NULL;

   rc = platform_heap_create(
      platform_get_module_id(), heap_capacity, master_cfg.use_shmem, &hid);
   platform_assert_status_ok(rc);

   Verbose_progress = master_cfg.verbose_progress;

   // Ensure that the default max async q-depth configured for
   // master-cfg is sufficiently big enough for this test case.
   platform_assert(NUM_PAGES_RW_ASYNC_PER_THREAD
                      <= master_cfg.io_async_queue_depth,
                   "NUM_PAGES_RW_ASYNC_PER_THREAD=%d, "
                   "Master io_async_queue_depth=%lu\n",
                   NUM_PAGES_RW_ASYNC_PER_THREAD,
                   master_cfg.io_async_queue_depth);

   io_config io_cfg;
   ZERO_STRUCT(io_cfg);

   io_config_init(&io_cfg,
                  master_cfg.page_size,
                  master_cfg.extent_size,
                  master_cfg.io_flags,
                  master_cfg.io_perms,
                  master_cfg.io_async_queue_depth,
                  "splinterdb_io_apis_test_db");

   int pid = platform_getpid();
   platform_default_log("Parent OS-pid=%d, Exercise IO sub-system test on"
                        " device '%s'"
                        ", page_size=%lu, extent_size=%lu"
                        ", kernel_queue_size=%lu ...\n",
                        pid,
                        io_cfg.filename,
                        io_cfg.page_size,
                        io_cfg.extent_size,
                        io_cfg.kernel_queue_size);

   // For this test, we allocate this structure. In a running Splinter
   // instance, this struct is nested inside the splinterdb{} handle.
   platform_io_handle *io_hdl = TYPED_ZALLOC(hid, io_hdl);
   if (!io_hdl) {
      goto heap_destroy;
   }
   Parent_io_handle = io_hdl;

   // Initialize the handle to the IO sub-system. A device with a small initial
   // size gets created here.
   rc = io_handle_init(io_hdl, &io_cfg, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to initialize IO handle: %s\n",
                         platform_status_to_string(rc));
      goto io_free;
   }

   uint64 disk_size_MB = DEVICE_SIZE_MB;
   uint64 disk_size    = (disk_size_MB * MiB); // bytes

   uint64 start_addr = 0;
   uint64 end_addr   = disk_size;

   /*
    * Basic exercise of sync write / read APIs, from main thread.
    * NOTE: The same functions will also be executed by thread's worker fns.
    */
   test_sync_writes(hid, &io_cfg, io_hdl, start_addr, end_addr, 'a');
   test_sync_reads(hid, &io_cfg, io_hdl, start_addr, end_addr, 'a');

   /*
    * Setup the task system which is needed for testing with threads.
    */
   uint64             num_bg_threads[NUM_TASK_TYPES] = {0};
   task_system_config task_cfg;
   rc = task_system_config_init(
      &task_cfg, TRUE /* use stats */, num_bg_threads, core_get_scratch_size());
   platform_assert(SUCCESS(rc));

   task_system *tasks = NULL;
   rc                 = task_system_create(hid, io_hdl, &tasks, &task_cfg);
   platform_assert(SUCCESS(rc));

   threadid    main_thread_idx = platform_get_tid();
   const char *whoami          = "Parent";

   /*
    * Grab, in the main thread, the opaque IO-context handle generated by
    * io_setup(). This will be used to compare the address as seen by
    * child threads in its IO context.
    */
   if (Verbose_progress) {
      platform_default_log("Before fork()'ing: OS-pid=%d, ThreadID=%lu (%s)"
                           ", Parent io_hdl=%p\n",
                           pid,
                           main_thread_idx,
                           whoami,
                           io_hdl);
   }

   /*
    * Setup a top-level test parameters structure encompassing the whole
    * device. This will flow through minions where disk chunks are carved
    * up, assigned to individual threads for IO processing.
    * Change the char to write so we can tell apart from previous contents.
    */
   io_test_fn_args io_test_fn_arg = {.hid        = hid,
                                     .io_cfgp    = &io_cfg,
                                     .io_hdlp    = io_hdl,
                                     .tasks      = tasks,
                                     .start_addr = start_addr,
                                     .end_addr   = end_addr,
                                     .stamp_char = 'A',
                                     .whoami     = whoami};

   /* Fork a child process, if so requested */
   if (master_cfg.fork_child) {
      pid = fork();

      if (pid < 0) {
         platform_error_log("fork() of child process failed: pid=%d\n", pid);
         goto io_free;
      } else if (pid) {
         int wstatus;
         int wr = wait(&wstatus);
         platform_assert(wr != -1, "wait failure: %s", strerror(errno));
         platform_assert(WIFEXITED(wstatus),
                         "Child terminated abnormally: SIGNAL=%d",
                         WIFSIGNALED(wstatus) ? WTERMSIG(wstatus) : 0);
         platform_assert(WEXITSTATUS(wstatus) == 0);

         if (Verbose_progress) {
            platform_default_log("Thread-ID=%lu, OS-pid=%d: "
                                 "Child execution wait() completed."
                                 " Resuming parent ...\n",
                                 platform_get_tid(),
                                 platform_getpid());
         }
      }
   }

   // Exercise R/W tests: Run this block if we are in the main thread
   // (i.e. didn't fork) or if we are a child process created after fork()'ing.
   if (!master_cfg.fork_child || (pid == 0)) {

      threadid this_thread_idx = platform_get_tid();

      whoami = ((master_cfg.fork_child && (pid == 0)) ? "Child" : "Parent");

      if (pid == 0) { // In child process ...

         if (Verbose_progress) {
            platform_default_log("After  fork()'ing: OS-pid=%d"
                                 ", ThreadID=%lu (%s)"
                                 ", before thread registration"
                                 ", Parent io_handle=%p, io_hdl=%p\n",
                                 platform_getpid(),
                                 this_thread_idx,
                                 whoami,
                                 Parent_io_handle,
                                 io_hdl);
         }

         task_register_this_thread(tasks, core_get_scratch_size());
         this_thread_idx = platform_get_tid();

         // Reset the handles / variables that have changed in the child
         // process now that we have re-done IO-setup in the child's context
         io_test_fn_arg.io_hdlp = io_hdl;
         io_test_fn_arg.whoami  = whoami;

         // Trace handles in child process, after thread registration
         platform_default_log("After  fork()'ing: %s OS-pid=%d"
                              ", ThreadID=%lu",
                              whoami,
                              platform_getpid(),
                              this_thread_idx);

         if (Verbose_progress) {
            platform_default_log("%s after  thread registration"
                                 ", Parent io_handle=%p, child io_hdl=%p\n",
                                 whoami,
                                 Parent_io_handle,
                                 io_hdl);
         }
      }

      test_sync_write_reads_by_threads(&io_test_fn_arg, NUM_THREADS, whoami);

      /*
       * Exercise Async reads followed by async writes for main / child thread.
       * NOTE: The same functions will also be executed by thread's worker fns.
       */
      rc = test_async_reads(hid, &io_cfg, io_hdl, start_addr, 'A', whoami);
      platform_assert_status_ok(rc);

      test_async_reads_by_threads(&io_test_fn_arg, NUM_THREADS, whoami);

      // The forked child process which uses Splinter masquerading as a
      // "thread" needs to relinquish its resources before exiting.
      task_deregister_this_thread(tasks);
   }

   // Only the parent process should dismantle stuff
   if (pid > 0) {
      task_system_destroy(hid, &tasks);
      io_handle_deinit(io_hdl);
   }

io_free:
   if (pid > 0) {
      platform_free(hid, io_hdl);
   }
heap_destroy:
   if (pid > 0) {
      platform_heap_destroy(&hid);
   } else if (pid == 0) {
      platform_default_log("%s: OS-pid=%d, Thread-ID=%lu"
                           " execution completed.\n",
                           whoami,
                           platform_getpid(),
                           platform_get_tid());
   }
   return (SUCCESS(rc) ? 0 : -1);
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_writes() - Write out a swath of disk using page-sized sync-write
 * IO.
 *
 * This routine is used to verify that basic sync-write API works as expected.
 * We just test that the IO succeeded; not the resulting contents. That
 * verification will be done by its sibling routine test_sync_reads().
 *
 * Parameters:
 *	 io_cfg		- Ptr to IO config struct to use
 *  io_hdlp		- Platform-specific IO handle
 *	 start_addr	- Start address to write from.
 *	 end_addr	- End address to write to (addr-to-write < end_addr)
 *  stamp_char	- Character to write out, on each page
 * -----------------------------------------------------------------------------
 */
static platform_status
test_sync_writes(platform_heap_id    hid,
                 io_config          *io_cfgp,
                 platform_io_handle *io_hdlp,
                 uint64              start_addr,
                 uint64              end_addr,
                 char                stamp_char)
{
   platform_thread this_thread = platform_get_tid();
   platform_status rc          = STATUS_NO_MEMORY;

   int page_size = (int)io_cfgp->page_size;

   // Allocate a buffer to do page I/O
   char *buf = TYPED_ARRAY_ZALLOC(hid, buf, page_size);
   if (!buf) {
      goto out;
   }

   memset(buf, stamp_char, page_size);

   io_handle *io_hdl = (io_handle *)io_hdlp;

   uint64 num_IOs = 0;
   // Iterate thru all pages and do the writes
   for (uint64 curr = start_addr; curr < end_addr; curr += page_size, num_IOs++)
   {
      rc = io_write(io_hdl, buf, page_size, curr);
      if (!SUCCESS(rc)) {
         platform_error_log("\n**** Error! Write IO at addr %lu failed;"
                            " Expected to write out %d bytes.\n",
                            curr,
                            page_size);
         platform_assert(SUCCESS(rc));
         goto free_buf;
      }
   }

   if (Verbose_progress || (this_thread == 0)) {
      platform_default_log(
         "  %s(): OS-pid=%d, Thread %lu performed %lu %dK page write IOs "
         "from start addr=%lu through end addr=%lu\n",
         __func__,
         platform_getpid(),
         this_thread,
         num_IOs,
         (int)(page_size / KiB),
         start_addr,
         end_addr);
   }

free_buf:
   platform_free(hid, buf);
out:
   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_writes_worker() - Shell worker function invoked by threads which
 * calls test_sync_writes().
 * -----------------------------------------------------------------------------
 */
void
test_sync_writes_worker(void *arg)
{
   io_test_fn_args *argp = (io_test_fn_args *)arg;

   test_sync_writes(argp->hid,
                    argp->io_cfgp,
                    argp->io_hdlp,
                    argp->start_addr,
                    argp->end_addr,
                    argp->stamp_char);
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_reads() - Read a swath of disk using page-sized sync-read IO.
 *
 * This routine is used to verify that basic sync-read API works as expected.
 * test_sync_writes() has minted out a known character. Verify that every
 * read-IO reads back the same contents in each page.
 *
 * Parameters:
 *	io_cfg		- Ptr to IO config struct to use
 *  io_hdlp		- Platform-specific IO handle
 *	 start_addr	- Start address to write from.
 *	 end_addr	- End address to read from (addr-to-read < end_addr)
 *  stamp_char	- Character that was written out, on each page
 * -----------------------------------------------------------------------------
 */
static platform_status
test_sync_reads(platform_heap_id    hid,
                io_config          *io_cfgp,
                platform_io_handle *io_hdlp,
                uint64              start_addr,
                uint64              end_addr,
                char                stamp_char)
{
   platform_thread this_thread = platform_get_tid();

   int page_size = (int)io_cfgp->page_size;

   // Allocate a buffer to do page I/O, and an expected results buffer
   char *buf = TYPED_ARRAY_ZALLOC(hid, buf, page_size);
   char *exp = TYPED_ARRAY_ZALLOC(hid, exp, page_size);
   memset(exp, stamp_char, page_size);

   platform_status rc = STATUS_OK;

   io_handle *io_hdl = (io_handle *)io_hdlp;

   uint64 num_IOs = 0;
   // Iterate thru all pages and do the writes
   for (uint64 curr = start_addr; curr < end_addr; curr += page_size, num_IOs++)
   {
      rc = io_read(io_hdl, buf, page_size, curr);
      if (!SUCCESS(rc)) {
         platform_error_log("\n**** Error! Read IO at addr %lu failed;"
                            " Expected to read %d bytes.\n",
                            curr,
                            page_size);
         platform_assert(SUCCESS(rc));
         goto free_buf;
      }

      int rv = memcmp(exp, buf, page_size);
      if (rv != 0) {
         rc = STATUS_IO_ERROR;
         platform_error_log("Page IO at address=%lu is incorrect.\n", curr);
         goto free_buf;
      }
      // Clear out buffer for next page read.
      memset(buf, 'X', page_size);
   }

   if (Verbose_progress || (this_thread == 0)) {
      platform_default_log(
         "  %s(): OS-pid=%d, Thread %lu performed %lu %dK page read  IOs "
         "from start addr=%lu through end addr=%lu\n",
         __func__,
         platform_getpid(),
         this_thread,
         num_IOs,
         (int)(page_size / KiB),
         start_addr,
         end_addr);
   }

free_buf:
   platform_free(hid, buf);
   platform_free(hid, exp);
   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_reads_worker() - Shell worker function invoked by threads which
 * calls test_sync_reads().
 * -----------------------------------------------------------------------------
 */
void
test_sync_reads_worker(void *arg)
{
   io_test_fn_args *argp = (io_test_fn_args *)arg;

   test_sync_reads(argp->hid,
                   argp->io_cfgp,
                   argp->io_hdlp,
                   argp->start_addr,
                   argp->end_addr,
                   argp->stamp_char);
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_write_reads_by_threads() --
 *
 * Driver function to exercise IO write / read APIs when executed by n-threads.
 * The input specifies the start / end address of the disk which is available
 * for IO. This is segmented into n-contiguous chunks and assigned to n-threads.
 * Each thread will first do the writes, and each thread will read back the
 * data it wrote, to verify that the contents are correct.
 * -----------------------------------------------------------------------------
 */
static platform_status
test_sync_write_reads_by_threads(io_test_fn_args *io_test_param,
                                 int              nthreads,
                                 const char      *whoami)
{
   platform_assert((nthreads <= NUM_THREADS),
                   "nthreads=%d should be <= %d\n",
                   nthreads,
                   NUM_THREADS);
   io_test_fn_args thread_params[NUM_THREADS];
   ZERO_ARRAY(thread_params);

   // # of pages allocated to each thread.
   uint64 npages = npages_per_thread(io_test_param, nthreads);
   platform_default_log(
      "\n%s(): %s process, OS-pid=%d, creates %d threads, %lu pages/thread \n",
      __func__,
      whoami,
      platform_getpid(),
      nthreads,
      npages);

   load_thread_params(io_test_param, thread_params, nthreads);

   /*
    * Execute the n-threads doing sync-writes on their disk pieces.
    */
   platform_status rc = do_n_thread_creates(
      "write_threads", nthreads, thread_params, test_sync_writes_worker);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (int i = 0; i < nthreads; i++) {
      platform_thread_join(thread_params[i].thread);
   }

   platform_default_log(
      "%s: Completed execution of test_sync_writes_worker() by %d-threads.\n",
      whoami,
      nthreads);

   /*
    * Execute the n-threads doing sync-reads from their disk pieces.
    */
   rc = do_n_thread_creates(
      "read_threads", nthreads, thread_params, test_sync_reads_worker);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (int i = 0; i < nthreads; i++) {
      platform_thread_join(thread_params[i].thread);
   }

   platform_default_log(
      "%s: Completed execution of test_sync_reads_worker() by %d-threads.\n",
      whoami,
      nthreads);
   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * load_thread_params() - Apportion the device across multiple threads for IO.
 *
 * The main test driver specifies the overall disk / device's start/end
 * boundaries. This function distributes the test parameters across multiple
 * threads, basically, setting up the start/end address of the regions that
 * each thread will perform IO on.
 *
 * NOTE: Caller is expected to have allocated a thread_params[] array big enough
 *       for nthreads.
 * Parameters:
 *   io_test_param   - Overall test execution parameters
 *   thread_params   - Array of thread-specific execution parameters
 *   nthreads			- # of threads.
 * -----------------------------------------------------------------------------
 */
static void
load_thread_params(io_test_fn_args *io_test_param,
                   io_test_fn_args *thread_params,
                   int              nthreads)
{
   // The input gives start / end addresses of the disk to use for IO testing.
   // Carve this into somewhat equal chunks, across page boundaries, with the
   // very last thread possibly getting fewer pages to do IO on. That's ok.
   uint64 start_addr = io_test_param->start_addr;
   uint64 end_addr   = io_test_param->end_addr;
   int    page_size  = (int)io_test_param->io_cfgp->page_size;

   // # of pages allocated to each thread.
   uint64 npages = npages_per_thread(io_test_param, nthreads);

   io_test_fn_args *param = thread_params;
   for (int i = 0; i < nthreads; i++, param++) {
      param->hid     = io_test_param->hid;
      param->io_cfgp = io_test_param->io_cfgp;
      param->io_hdlp = io_test_param->io_hdlp;
      param->tasks   = io_test_param->tasks;

      // Set up the start/end address of the chunk of device for this thread
      // The last thread's end will be the devices end, which may happen if
      // device size cannot be uniformly divided into equal # of pages.
      end_addr          = (start_addr + (npages * page_size));
      param->start_addr = start_addr;
      param->end_addr =
         (i == (nthreads - 1)) ? io_test_param->end_addr : end_addr;

      // Reset start of the next chunk for next thread to work on
      start_addr = end_addr;

      param->stamp_char = io_test_param->stamp_char;
      param->whoami     = io_test_param->whoami;
   }
}

/*
 * -----------------------------------------------------------------------------
 * test_async_reads() - Performs async reads of n-pages starting from a
 * start address.
 *
 * This routine is used to exercise & verify basic async IO APIs.
 * Previous test cases have left the device in a known state. We perform
 * async reads on a hard-coded # of pages and cross-check that, upon
 * completion of the IO, the data is read as expected.
 * -----------------------------------------------------------------------------
 */

typedef struct async_read_state {
   platform_heap_id      hid;
   uint64                lock;
   char                 *expected;
   io_async_state_buffer iostate;
} async_read_state;

static void
async_read_state_lock(async_read_state *state)
{
   while (__sync_lock_test_and_set(&state->lock, 1)) {
      platform_yield();
   }
}

static void
async_read_state_unlock(async_read_state *state)
{
   __sync_lock_release(&state->lock);
}

/*
 *----------------------------------------------------------------------
 * read_async_callback --
 *
 *    Async callback called after async read IO completes.
 *----------------------------------------------------------------------
 */
static void
read_async_callback(void *arg)
{
   async_read_state *state = (async_read_state *)arg;
   async_read_state_lock(state);
   if (io_async_run(state->iostate) != ASYNC_STATUS_DONE) {
      async_read_state_unlock(state);
      return;
   }

   uint64              count;
   const struct iovec *iov = io_async_state_get_iovec(state->iostate, &count);

   debug_assert((count == 1), "count=%lu\n", count);
   if (Verbose_progress) {
      platform_default_log("Aysnc-callback for read of page=%p completed.\n",
                           iov->iov_base);
   }

   // Buffer that IO-read would have completed reading into
   char *buf_addr = iov->iov_base;

   // Expected contents passed-in via metadata when async-read was issued.
   int page_size = (4 * KiB);

   int rv = memcmp(state->expected, buf_addr, page_size);
   if (rv != 0) {
      platform_error_log("Page IO read at address=%p is incorrect.\n",
                         buf_addr);
   }

   platform_free(state->hid, state);
}

static platform_status
test_async_reads(platform_heap_id    hid,
                 io_config          *io_cfgp,
                 platform_io_handle *io_hdlp,
                 uint64              start_addr,
                 char                stamp_char,
                 const char         *whoami)
{
   platform_thread this_thread = platform_get_tid();
   platform_status rc          = STATUS_OK;

   int page_size = (int)io_cfgp->page_size;

   // Allocate a buffer to do page I/O, and an expected results buffer
   uint64 nbytes = (page_size * NUM_PAGES_RW_ASYNC_PER_THREAD);
   char  *buf    = TYPED_ARRAY_ZALLOC(hid, buf, nbytes);
   if (!buf) {
      rc = STATUS_NO_MEMORY;
      goto out;
   }

   char *exp = TYPED_ARRAY_ZALLOC(hid, exp, page_size);
   if (!exp) {
      rc = STATUS_NO_MEMORY;
      goto free_buf;
   }
   memset(exp, stamp_char, page_size);

   platform_default_log("\n%s: Thread=%lu: %s() Test Async reads for %d"
                        " pages ...\n",
                        whoami,
                        this_thread,
                        __func__,
                        NUM_PAGES_RW_ASYNC_PER_THREAD);

   io_handle *ioh = (io_handle *)io_hdlp;

   // Perform async reads of n-pages, into an allocated buffer
   char  *buf_addr  = buf;
   uint64 this_addr = start_addr;
   for (int i = 0; i < NUM_PAGES_RW_ASYNC_PER_THREAD;
        i++, this_addr += page_size, buf_addr += page_size)
   {
      async_read_state *state = TYPED_MALLOC(hid, state);
      platform_assert(state != NULL);
      state->lock     = 0;
      state->expected = exp;
      state->hid      = hid;
      io_async_state_init(state->iostate,
                          ioh,
                          io_async_preadv,
                          this_addr,
                          read_async_callback,
                          state);
      io_async_state_append_page(state->iostate, buf_addr);

      async_read_state_lock(state);
      io_async_run(state->iostate);
      async_read_state_unlock(state);

      if (Verbose_progress) {
         platform_default_log(
            "  [%2d] Thread=%lu: Async read issued for page=%ld\n",
            i,
            this_thread,
            this_addr);
      }
   }

   io_wait_all(ioh);

   platform_free(hid, exp);
free_buf:
   platform_free(hid, buf);
out:
   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * test_async_reads_by_threads() --
 *
 * Driver function to exercise Async IO read APIs when executed by n-threads.
 * The input specifies the start / end address of the disk which is available
 * for IO. This is segmented into n-contiguous chunks and assigned to n-threads.
 * Each thread will perform async reads on its assigned chunk of disk space.
 * -----------------------------------------------------------------------------
 */
static platform_status
test_async_reads_by_threads(io_test_fn_args *io_test_param,
                            int              nthreads,
                            const char      *whoami)
{
   platform_assert((nthreads <= NUM_THREADS),
                   "nthreads=%d should be <= %d\n",
                   nthreads,
                   NUM_THREADS);
   io_test_fn_args thread_params[NUM_THREADS];
   ZERO_ARRAY(thread_params);

   // # of pages allocated to each thread.
   uint64 npages = npages_per_thread(io_test_param, nthreads);
   platform_default_log("%s: %s() for %d threads, %lu pages/thread ...\n",
                        io_test_param->whoami,
                        __func__,
                        nthreads,
                        npages);

   load_thread_params(io_test_param, thread_params, nthreads);

   /*
    * Execute the n-threads doing async-reads on their disk pieces.
    */
   platform_status rc = do_n_thread_creates(
      "async_read_threads", nthreads, thread_params, test_async_reads_worker);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (int i = 0; i < nthreads; i++) {
      platform_thread_join(thread_params[i].thread);
   }

   platform_default_log(
      "%s: Completed execution of test_async_reads_worker() by %d-threads.\n",
      whoami,
      nthreads);
   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * test_async_reads_worker() - Shell worker function invoked by threads which
 * calls test_async_reads().
 * -----------------------------------------------------------------------------
 */
void
test_async_reads_worker(void *arg)
{
   io_test_fn_args *argp = (io_test_fn_args *)arg;

   test_async_reads(argp->hid,
                    argp->io_cfgp,
                    argp->io_hdlp,
                    argp->start_addr,
                    argp->stamp_char,
                    argp->whoami);
}

/*
 * do_n_thread_creates() --
 *
 * Helper function to create n-threads, each thread executing the specified
 * thread_hdlr handler function. [ Copied from splinter_test.c!! ]
 */
static platform_status
do_n_thread_creates(const char         *thread_type,
                    uint64              num_threads,
                    io_test_fn_args    *params,
                    test_io_thread_hdlr thread_hdlr)
{
   platform_status ret;
   for (uint64 i = 0; i < num_threads; i++) {
      ret = task_thread_create(thread_type,
                               thread_hdlr,
                               &params[i],
                               core_get_scratch_size(),
                               params[i].tasks,
                               params[i].hid,
                               &params[i].thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }
   return ret;
}
