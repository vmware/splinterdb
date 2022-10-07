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
 * - Using threads, with Splinter setup to use shared-memory
 * - Using forked processes, with Splinter setup to use shared-memory
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
 * ----------------------------------------------------------------------------
 */

#include "platform.h"
#include "config.h"
#include "io.h"
#include "trunk.h" // Needed for trunk_get_scratch_size()
#include "task.h"

/*
 * Structure to package arguments needed by test-case functions, supplied by
 * worker functions invoked by pthreads.
 */
typedef struct io_test_fn_args {
   platform_heap_id    arg_hid;
   io_config          *arg_io_cfgp;
   platform_io_handle *arg_io_hdlp;
   task_system        *arg_tasks;
   uint64              arg_start_addr;
   uint64              arg_end_addr;
   char                arg_stamp_char;
   platform_thread     arg_thread;
} io_test_fn_args;

/*
 * Different test cases in this test drive multiple threads each doing one
 * type of activity. Declare the interface of such thread handler functions.
 */
typedef void (*test_io_thread_hdlr)(void *arg);

/* Use hard-coded # of threads to avoid allocating memory for thread-specific
 * arrays of parameters.
 */
#define NUM_THREADS 5

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
test_sync_write_reads_by_threads(io_test_fn_args *io_test_param, int nthreads);

static platform_status
do_n_thread_creates(const char         *thread_type,
                    uint64              num_threads,
                    io_test_fn_args    *params,
                    test_io_thread_hdlr thread_hdlr);

void
test_sync_writes_worker(void *arg);


/*
 * ----------------------------------------------------------------------------
 * splinter_io_apis_test() - Entry point 'main' for SplinterDB IO APIs testing.
 * ----------------------------------------------------------------------------
 */
int
splinter_io_apis_test(int argc, char *argv[])
{

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.

   bool use_shmem = FALSE;

   // Create a heap for io system's memory allocation.
   platform_heap_handle hh  = NULL;
   platform_heap_id     hid = NULL;
   platform_status      rc  = platform_heap_create(
      platform_get_module_id(), heap_capacity, use_shmem, &hh, &hid);
   platform_assert_status_ok(rc);

   // Do minimal IO config setup, using default IO values.
   master_config master_cfg;
   io_config     io_cfg;

   // Initialize the IO sub-system configuration.
   config_set_defaults(&master_cfg);
   io_config_init(&io_cfg,
                  master_cfg.page_size,
                  master_cfg.extent_size,
                  master_cfg.io_flags,
                  master_cfg.io_perms,
                  master_cfg.io_async_queue_depth,
                  "splinter_io_apis_test.db");

   platform_default_log("Exercise IO sub-system test on device '%s'"
                        ", page_size=%lu, extent_size=%lu, async_queue_size=%lu"
                        ", kernel_queue_size=%lu, async_max_pages=%lu ...\n",
                        io_cfg.filename,
                        io_cfg.page_size,
                        io_cfg.extent_size,
                        io_cfg.async_queue_size,
                        io_cfg.kernel_queue_size,
                        io_cfg.async_max_pages);

   platform_io_handle *io_handle = TYPED_MALLOC(hid, io_handle);

   // Initialize the handle to the IO sub-system. A device with a small initial
   // size gets created here.
   rc = io_handle_init(io_handle, &io_cfg, hh, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to initialize IO handle: %s\n",
                         platform_status_to_string(rc));
      goto io_free;
   }

   uint64 disk_size_MB = 256;
   uint64 disk_size    = (disk_size_MB * MiB); // bytes

   uint64 start_addr = 0;
   uint64 end_addr   = disk_size;

   // Basic exercise of sync write / read APIs, from main thread.
   test_sync_writes(hid, &io_cfg, io_handle, start_addr, end_addr, 'a');
   test_sync_reads(hid, &io_cfg, io_handle, start_addr, end_addr, 'a');

   /*
    * Setup the task system which is needed for testing with threads.
    */
   task_system *tasks                          = NULL;
   uint8        num_bg_threads[NUM_TASK_TYPES] = {0};

   rc = task_system_create(hid,
                           io_handle,
                           &tasks,
                           TRUE,  // Use statistics,
                           FALSE, // Background threads are off
                           num_bg_threads,
                           trunk_get_scratch_size());

   // Change the char to write so we can tell apart from previous contents.
   io_test_fn_args io_test_fn_arg = {.arg_hid        = hid,
                                     .arg_io_cfgp    = &io_cfg,
                                     .arg_io_hdlp    = io_handle,
                                     .arg_tasks      = tasks,
                                     .arg_start_addr = start_addr,
                                     .arg_end_addr   = end_addr,
                                     .arg_stamp_char = 'A'};

   test_sync_write_reads_by_threads(&io_test_fn_arg, NUM_THREADS);

io_free:
   platform_free(hid, io_handle);
   platform_heap_destroy(&hh);

   return (SUCCESS(rc) ? 0 : -1);
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_writes() - Write out a swath of disk using page-sized sync-write
 * IO.
 *
 * This routine is used to verify that basic sync-write API works as expected.
 * We just test that the IO succeeded; not the resulting contents. That
 *verification will be done by its sibling routine test_sync_reads().
 *
 * Parameters:
 *	io_cfg		- Ptr to IO config struct to use
 *  io_hdlp		- Platform-specific IO handle
 *	start_addr	- Start address to write from.
 *	end_addr	- End address to write to (< end_addr)
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

   int page_size = (int)io_cfgp->page_size;

   // Allocate a buffer to do page I/O
   char *buf = TYPED_ARRAY_ZALLOC(hid, buf, page_size);

   memset(buf, stamp_char, page_size);

   platform_status rc = STATUS_OK;

   io_handle *io_hdl = (io_handle *)io_hdlp;

   uint64 num_IOs = 0;
   // Iterate thru all pages and do the writes
   for (uint64 curr = start_addr; curr < end_addr; curr += page_size, num_IOs++)
   {
      rc = io_write(io_hdl, buf, page_size, curr);
      if (!SUCCESS(rc)) {
         platform_error_log("Write IO at addr %lu wrote %d bytes"
                            ", expected to write out %d bytes.\n",
                            curr,
                            io_hdl->nbytes_rw,
                            page_size);
         goto free_buf;
      }
   }

   platform_default_log("  %s(): Thread %lu performed %lu %dK page write IOs "
                        "from start addr=%lu through end addr=%lu\n",
                        __FUNCTION__,
                        this_thread,
                        num_IOs,
                        (int)(page_size / KiB),
                        start_addr,
                        end_addr);

free_buf:
   platform_free(hid, buf);
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
   test_sync_writes(argp->arg_hid,
                    argp->arg_io_cfgp,
                    argp->arg_io_hdlp,
                    argp->arg_start_addr,
                    argp->arg_end_addr,
                    argp->arg_stamp_char);
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_reads() - Read a swath of disk using page-sized sync-read IO.
 *
 * This routine is used to verify that basic sync-read API works as expected.
 * test_sync_writes() has minted out a known character. Verify that every
 * read reads back the same contents in each page.
 *
 * Parameters:
 *	io_cfg		- Ptr to IO config struct to use
 *  io_hdlp		- Platform-specific IO handle
 *	start_addr	- Start address to write from.
 *	end_addr	- End address to write to (< end_addr)
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
         platform_error_log("Read IO at addr %lu read %d bytes"
                            ", expected to read %d bytes.\n",
                            curr,
                            io_hdl->nbytes_rw,
                            page_size);
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

   platform_default_log("  %s():  Thread %lu performed %lu %dK page read  IOs "
                        "from start addr=%lu through end addr=%lu\n",
                        __FUNCTION__,
                        this_thread,
                        num_IOs,
                        (int)(page_size / KiB),
                        start_addr,
                        end_addr);

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
   test_sync_reads(argp->arg_hid,
                   argp->arg_io_cfgp,
                   argp->arg_io_hdlp,
                   argp->arg_start_addr,
                   argp->arg_end_addr,
                   argp->arg_stamp_char);
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
test_sync_write_reads_by_threads(io_test_fn_args *io_test_param, int nthreads)
{
   // The input gives start / end addresses of the disk to use for IO testing.
   // Carve this into somewhat equal chunks, across page boundaries, with the
   // very last thread possibly getting few pages to do IO on. That's ok.
   uint64 start_addr = io_test_param->arg_start_addr;
   uint64 end_addr   = io_test_param->arg_end_addr;
   int    page_size  = (int)io_test_param->arg_io_cfgp->page_size;

   // # of pages allocated to each thread.
   uint64 npages = ((end_addr - start_addr) / page_size) / nthreads;

   io_test_fn_args thread_params[NUM_THREADS];
   ZERO_ARRAY(thread_params);

   platform_default_log("Executing %s, for %d threads, %lu pages/thread ...\n",
                        __FUNCTION__,
                        nthreads,
                        npages);

   io_test_fn_args *param = thread_params;
   for (int i = 0; i < nthreads; i++, param++) {
      param->arg_hid     = io_test_param->arg_hid;
      param->arg_io_cfgp = io_test_param->arg_io_cfgp;
      param->arg_io_hdlp = io_test_param->arg_io_hdlp;
      param->arg_tasks   = io_test_param->arg_tasks;

      // Set up the start/end address of the chunk of device for this thread
      // The last thread's end will be the devices end, which may happen if
      // device size cannot be uniformly divided into equal # of pages.
      end_addr              = (start_addr + (npages * page_size));
      param->arg_start_addr = start_addr;
      param->arg_end_addr =
         (i == (nthreads - 1)) ? io_test_param->arg_end_addr : end_addr;

      // Reset start of the next chunk for next thread to work on
      start_addr = end_addr;

      param->arg_stamp_char = io_test_param->arg_stamp_char;
   }

   /*
    * Execute the n-threads doing sync-writes on their disk pieces.
    */
   platform_status rc = do_n_thread_creates(
      "write_threads", nthreads, thread_params, test_sync_writes_worker);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (int i = 0; i < nthreads; i++) {
      platform_thread_join(thread_params[i].arg_thread);
   }

   /*
    * Execute the n-threads doing sync-reads from their disk pieces.
    */
   rc = do_n_thread_creates(
      "read_threads", nthreads, thread_params, test_sync_reads_worker);
   if (!SUCCESS(rc)) {
      return rc;
   }

   for (int i = 0; i < nthreads; i++) {
      platform_thread_join(thread_params[i].arg_thread);
   }

   return rc;
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
                               trunk_get_scratch_size(),
                               params[i].arg_tasks,
                               params[i].arg_hid,
                               &params[i].arg_thread);
      if (!SUCCESS(ret)) {
         return ret;
      }
   }
   return ret;
}
