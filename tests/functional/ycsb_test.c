// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "platform.h"

#include "trunk.h"
#include "task.h"
#include "rc_allocator.h"
#include "clockcache.h"
#include "test.h"
#include "random.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/mman.h>

#define YCSB_KEY_SIZE  24
#define YCSB_DATA_SIZE 102

#define LATENCY_MANTISSA_BITS  (10ULL)
#define LATENCY_EXPONENT_LIMIT (64ULL - LATENCY_MANTISSA_BITS)
#define LATENCY_MANTISSA_LIMIT (1ULL << LATENCY_MANTISSA_BITS)
typedef uint64_t latency_table[LATENCY_EXPONENT_LIMIT][LATENCY_MANTISSA_LIMIT];

/* This is the fastest way I can come up
 * with to do a log-scale-esque transform. */
void
record_latency(latency_table table, uint64_t latency)
{
   uint64_t rough_log2 = 64 - __builtin_clzll(latency);
   uint64_t exponent;
   uint64_t mantissa;
   if (LATENCY_MANTISSA_BITS + 1 < rough_log2) {
      exponent = rough_log2 - LATENCY_MANTISSA_BITS;
      mantissa = latency >> (exponent);
   } else if (LATENCY_MANTISSA_BITS + 1 == rough_log2) {
      exponent = 1;
      mantissa = latency - LATENCY_MANTISSA_LIMIT;
   } else {
      exponent = 0;
      mantissa = latency;
   }
   platform_assert(exponent < LATENCY_EXPONENT_LIMIT);
   platform_assert(mantissa < LATENCY_MANTISSA_LIMIT);
   table[exponent][mantissa]++;
}

/*
 * dest = table1 + table2
 * dest may be physically equal to table1 or table2 or both.
 */
void
sum_latency_tables(latency_table dest,
                   latency_table table1,
                   latency_table table2)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++) {
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++) {
         dest[exponent][mantissa] =
            table1[exponent][mantissa] + table2[exponent][mantissa];
      }
   }
}

/* Thread-safe code for folding a table into dest. */
void
add_latency_table(latency_table dest, latency_table table)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++) {
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++) {
         __sync_fetch_and_add(&dest[exponent][mantissa],
                              table[exponent][mantissa]);
      }
   }
}

uint64_t
num_latencies(latency_table table)
{
   uint64_t count = 0;
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         count += table[exponent][mantissa];
   return count;
}

uint64_t
compute_latency(uint64_t exponent, uint64_t mantissa)
{
   if (exponent == 0)
      return mantissa;
   else if (exponent == 1)
      return mantissa + LATENCY_MANTISSA_LIMIT;
   else
      return (mantissa + LATENCY_MANTISSA_LIMIT) << (exponent - 1);
}

uint64_t
total_latency(latency_table table)
{
   uint64_t total = 0;
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         total +=
            table[exponent][mantissa] * compute_latency(exponent, mantissa);
   return total;
}

double
mean_latency(latency_table table)
{
   uint64_t nl = num_latencies(table);
   if (nl)
      return 1.0 * total_latency(table) / nl;
   else
      return 0.0;
}

uint64_t
min_latency(latency_table table)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         if (table[exponent][mantissa])
            return compute_latency(exponent, mantissa);
   return 0;
}

uint64_t
max_latency(latency_table table)
{
   int64_t exponent;
   int64_t mantissa;

   for (exponent = LATENCY_EXPONENT_LIMIT - 1; exponent >= 0; exponent--)
      for (mantissa = LATENCY_MANTISSA_LIMIT - 1; mantissa >= 0; mantissa--)
         if (table[exponent][mantissa])
            return compute_latency(exponent, mantissa);
   return 0;
}

uint64_t
ith_latency(latency_table table, uint64_t rank)
{
   uint64_t exponent;
   uint64_t mantissa;

   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         if (rank < table[exponent][mantissa]) {
            return compute_latency(exponent, mantissa);
         } else {
            rank -= table[exponent][mantissa];
         }
   return max_latency(table);
}

uint64_t
latency_percentile(latency_table table, float percent)
{
   return ith_latency(table, (percent / 100) * num_latencies(table));
}

void
print_latency_table(latency_table table, platform_log_handle *log_handle)
{
   uint64_t exponent;
   uint64_t mantissa;
   bool     started = FALSE;
   uint64_t max     = max_latency(table);

   platform_log(log_handle, "latency count\n");
   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++)
         if (started || table[exponent][mantissa]) {
            platform_log(log_handle,
                         "%20lu %20lu\n",
                         compute_latency(exponent, mantissa),
                         table[exponent][mantissa]);
            started = TRUE;
            if (max == compute_latency(exponent, mantissa))
               return;
         }
}

void
write_latency_table(char *filename, latency_table table)
{
   FILE *stream = fopen(filename, "w");
   platform_assert(stream != NULL);
   print_latency_table(table, stream);
   fclose(stream);
}

void
print_latency_cdf(platform_log_handle *log_handle, latency_table table)
{
   uint64_t count_so_far = 0;
   uint64_t exponent;
   uint64_t mantissa;
   uint64_t total = num_latencies(table);

   if (total == 0)
      total = 1;

   platform_log(log_handle, "latency count\n");
   for (exponent = 0; exponent < LATENCY_EXPONENT_LIMIT; exponent++)
      for (mantissa = 0; mantissa < LATENCY_MANTISSA_LIMIT; mantissa++) {
         count_so_far += table[exponent][mantissa];
         if (count_so_far)
            platform_log(log_handle,
                         "%20lu %f\n",
                         compute_latency(exponent, mantissa),
                         1.0 * count_so_far / total);
         if (count_so_far == total)
            return;
      }
}

void
write_latency_cdf(char *filename, latency_table table)
{
   FILE *stream = fopen(filename, "w");
   platform_assert(stream != NULL);
   print_latency_cdf(stream, table);
   fclose(stream);
}

typedef struct ycsb_op {
   char   cmd;
   char   key[YCSB_KEY_SIZE];
   char   value[YCSB_DATA_SIZE];
   uint64 range_len;
   uint64 start_time;
   uint64 end_time;
   bool   found;
} ycsb_op;

typedef struct running_times {
   uint64_t earliest_thread_start_time;
   uint64_t last_thread_finish_time;
   uint64_t sum_of_wall_clock_times;
   uint64_t sum_of_cpu_times;
   uint64_t cleanup_time;
} running_times;

typedef struct latency_tables {
   latency_table pos_queries;
   latency_table neg_queries;
   latency_table all_queries;
   latency_table deletes;
   latency_table inserts;
   latency_table updates;
   latency_table scans;
} latency_tables;

typedef struct ycsb_log_params {
   // Inputs
   char    *filename;
   uint64   nthreads;
   uint64   batch_size;
   uint64   total_ops;
   ycsb_op *ycsb_ops; // array of ops to be performed

   // Init
   platform_thread thread;

   // State
   uint64        next_op;
   trunk_handle *spl;

   // Coordination
   uint64 *threads_complete;
   uint64 *threads_work_complete;
   uint64  total_threads;

   running_times  times;
   latency_tables tables;

   task_system *ts;
} ycsb_log_params;

typedef struct ycsb_phase {
   char            *name;
   uint64           nlogs;
   ycsb_log_params *params;
   uint64_t         total_ops;
   running_times    times;
   latency_tables   tables;
   char            *measurement_command;
} ycsb_phase;

static void
nop_tuple_func(key tuple_key, message value, void *arg)
{}

static void
ycsb_thread(void *arg)
{
   platform_status   rc;
   uint64            i;
   ycsb_log_params  *params     = (ycsb_log_params *)arg;
   trunk_handle     *spl        = params->spl;
   uint64            num_ops    = params->total_ops;
   uint64            batch_size = params->batch_size;
   uint64            my_batch;
   merge_accumulator value;
   merge_accumulator_init(&value, spl->heap_id);

   uint64_t start_time = platform_get_timestamp();
   __sync_val_compare_and_swap(
      &params->times.earliest_thread_start_time, 0, start_time);

   struct timespec start_thread_cputime;
   clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_thread_cputime);

   my_batch = __sync_fetch_and_add(&params->next_op, batch_size);
   while (my_batch < num_ops) {
      if (num_ops < my_batch + batch_size)
         batch_size = num_ops - my_batch;

      ycsb_op *ops = &params->ycsb_ops[my_batch];
      for (i = 0; i < batch_size; i++) {
         ops->start_time = platform_get_timestamp();
         switch (ops->cmd) {
            case 'r':
            {
               rc = trunk_lookup(
                  spl, key_create(YCSB_KEY_SIZE, ops->key), &value);
               platform_assert_status_ok(rc);
               // if (!ops->found) {
               //   char key_str[128];
               //   trunk_key_to_string(spl, ops->key, key_str);
               //   platform_default_log("Key %s not found\n", key_str);
               //   trunk_print_lookup(spl, ops->key);
               //   platform_assert(0);
               //}
               break;
            }
            case 'd':
            case 'i':
            case 'u':
            {
               message val =
                  message_create(MESSAGE_TYPE_INSERT,
                                 slice_create(YCSB_DATA_SIZE, ops->value));
               rc = trunk_insert(spl, key_create(YCSB_KEY_SIZE, ops->key), val);
               platform_assert_status_ok(rc);
               break;
            }
            case 's':
            {
               rc = trunk_range(spl,
                                key_create(YCSB_KEY_SIZE, ops->key),
                                ops->range_len,
                                nop_tuple_func,
                                NULL);
               platform_assert_status_ok(rc);
               break;
            }
            default:
            {
               platform_error_log("Unknown YCSB command %c, skipping command\n",
                                  ops->cmd);
               break;
            }
         }
         ops->end_time = platform_get_timestamp();
         ops++;
      }
      my_batch = __sync_fetch_and_add(&params->next_op, batch_size);
   }

   __sync_fetch_and_add(params->threads_complete, 1);

   while (*params->threads_complete != params->total_threads) {
      trunk_perform_tasks(spl);
      platform_sleep_ns(2000);
   }

   if (__sync_fetch_and_add(params->threads_work_complete, 1)
       == params->total_threads - 1)
   {
      cache_flush(spl->cc);
   }

   uint64_t end_time                     = platform_get_timestamp();
   params->times.last_thread_finish_time = end_time;
   __sync_fetch_and_add(&params->times.sum_of_wall_clock_times,
                        end_time - start_time);

   struct timespec end_thread_cputime;
   clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_thread_cputime);
   uint64_t thread_cputime =
      SEC_TO_NSEC(end_thread_cputime.tv_sec) + end_thread_cputime.tv_nsec
      - SEC_TO_NSEC(start_thread_cputime.tv_sec) - start_thread_cputime.tv_nsec;
   __sync_fetch_and_add(&params->times.sum_of_cpu_times, thread_cputime);
   merge_accumulator_deinit(&value);
}

static int
run_ycsb_phase(trunk_handle    *spl,
               ycsb_phase      *phase,
               task_system     *ts,
               platform_heap_id hid)
{
   int              success = 0;
   int              i;
   int              nthreads;
   platform_thread *threads;
   platform_status  ret;

   nthreads = 0;
   for (i = 0; i < phase->nlogs; i++)
      nthreads += phase->params[i].nthreads;

   platform_memfrag *mf = NULL;
   platform_memfrag  memfrag_threads;
   threads = TYPED_ARRAY_MALLOC(hid, threads, nthreads);
   if (threads == NULL)
      return -1;

   uint64 threads_complete      = 0;
   uint64 threads_work_complete = 0;

   uint64_t cur_thread = 0;
   for (i = 0; i < phase->nlogs; i++) {
      phase->params[i].spl                   = spl;
      phase->params[i].threads_complete      = &threads_complete;
      phase->params[i].threads_work_complete = &threads_work_complete;
      phase->params[i].total_threads         = nthreads;
      phase->params[i].ts                    = ts;
      int j;
      for (j = 0; j < phase->params[i].nthreads; j++) {
         platform_assert(cur_thread < nthreads);
         ret = task_thread_create("ycsb_thread",
                                  ycsb_thread,
                                  &phase->params[i],
                                  trunk_get_scratch_size(),
                                  ts,
                                  hid,
                                  &threads[cur_thread]);
         if (!SUCCESS(ret)) {
            success = -1;
            goto shutdown;
         }
         cur_thread++;
      }
   }

shutdown:
   while (0 < nthreads) {
      platform_status result = platform_thread_join(threads[nthreads - 1]);
      if (!SUCCESS(result)) {
         success = -1;
         break;
      }
      nthreads--;
   }
   mf = &memfrag_threads;
   platform_free(hid, mf);

   if (phase->measurement_command) {
      const size_t     bufsize = 1024;
      platform_memfrag memfrag_filename;
      char            *filename = TYPED_ARRAY_MALLOC(hid, filename, bufsize);
      platform_assert(filename);
      snprintf(filename, bufsize, "%s.measurement", phase->name);
      FILE *measurement_output = fopen(filename, "wb");
      platform_assert(measurement_output != NULL);
      FILE *measurement_cmd = popen(phase->measurement_command, "r");
      platform_assert(measurement_cmd != NULL);

      platform_memfrag memfrag_buffer;
      char            *buffer = TYPED_ARRAY_MALLOC(hid, buffer, bufsize);
      size_t           num_read;
      size_t           num_written;
      do {
         num_read    = fread(buffer, 1, bufsize, measurement_cmd);
         num_written = fwrite(buffer, 1, num_read, measurement_output);
         if (num_written != num_read) {
            platform_error_log(
               "Could not write to measurement output file %s\n", filename);
            platform_free(hid, filename);
            platform_free(hid, buffer);
            exit(1);
         }
      } while (!feof(measurement_cmd));
      fclose(measurement_output);
      pclose(measurement_cmd);

      mf = &memfrag_filename;
      platform_free(hid, mf);
      mf = &memfrag_buffer;
      platform_free(hid, mf);
   }

   return success;
}

static int
run_all_ycsb_phases(trunk_handle    *spl,
                    ycsb_phase      *phase,
                    uint64           nphases,
                    task_system     *ts,
                    platform_heap_id hid)
{
   uint64 i;
   for (i = 0; i < nphases; i++) {
      platform_default_log("Beginning phase %lu\n", i);
      if (run_ycsb_phase(spl, &phase[i], ts, hid) < 0)
         return -1;
      trunk_print_insertion_stats(Platform_default_log_handle, spl);
      trunk_print_lookup_stats(Platform_default_log_handle, spl);
      cache_print_stats(Platform_default_log_handle, spl->cc);
      // trunk_reset_stats(spl);
      cache_reset_stats(spl->cc);
      platform_default_log("Phase %s complete\n", phase[i].name);
   }
   return 0;
}

typedef struct parse_ycsb_log_req {
   char     *filename;
   bool      lock;
   uint64    start_line;
   uint64    end_line;
   uint64   *num_ops;
   ycsb_op **ycsb_ops;
   uint64   *max_range_len;
} parse_ycsb_log_req;

static void
parse_ycsb_log_file(void *arg)
{
   parse_ycsb_log_req *req     = (parse_ycsb_log_req *)arg;
   platform_heap_id    hid     = platform_get_heap_id();
   bool                lock    = req->lock;
   uint64             *num_ops = req->num_ops;

   random_state rs;
   random_init(&rs, req->start_line, 0);

   char *filename = req->filename;
   FILE *fp       = fopen(filename, "r");
   if (fp == NULL) {
      platform_default_log("failed to open file\n");
      *req->ycsb_ops = NULL;
      return;
   }

   char  *buffer  = NULL;
   size_t bufsize = 0;
   for (uint64 i = 0; i < req->start_line; i++) {
      int ret = getline(&buffer, &bufsize, fp);
      platform_assert(ret > 0);
   }
   uint64 num_lines = req->end_line - req->start_line;

   platform_memfrag  memfrag_result;
   platform_memfrag *mf = &memfrag_result;
   ;
   ycsb_op *result = TYPED_ARRAY_MALLOC(hid, result, num_lines);
   if (result == NULL) {
      platform_error_log("Failed to allocate memory for log\n");
      goto close_file;
   }
   if (lock && mlock(result, num_lines * sizeof(ycsb_op))) {
      platform_error_log("Failed to lock log into RAM.\n");
      platform_free(hid, mf);
      goto close_file;
   }

   uint64 i = 0;
   for (i = 0; i < num_lines; i++) {
      int ret = getline(&buffer, &bufsize, fp);
      platform_assert(ret > 0);
      data_handle *dh = (data_handle *)&result[i].value;
      dh->ref_count   = 1;
      ret = sscanf(buffer, "%c %64s", &result[i].cmd, result[i].key);

      platform_assert(ret == 2);
      if (result[i].cmd == 'r') {
         platform_assert(ret == 2);
      } else if (result[i].cmd == 'd') {
         platform_assert(ret == 2);
      } else if (result[i].cmd == 'u') {
         platform_assert(ret == 2);
         random_bytes(&rs, (char *)dh->data, YCSB_DATA_SIZE - 2);
      } else if (result[i].cmd == 'i') {
         platform_assert(ret == 2);
         random_bytes(&rs, (char *)dh->data, YCSB_DATA_SIZE - 2);
      } else if (result[i].cmd == 's') {
         ret = sscanf(buffer,
                      "%c %64s %lu\n",
                      &result[i].cmd,
                      result[i].key,
                      &result[i].range_len);
         if (result[i].range_len > *req->max_range_len) {
            *req->max_range_len = result[i].range_len;
         }
         platform_assert(ret == 3);
      } else {
         platform_assert(0);
      }
   }
   *num_ops = num_lines;

close_file:
   // RESOLVE - -Fix this ...
   if (buffer) {
      platform_free(hid, buffer);
   }
   fclose(fp);
   mf = &memfrag_result;
   platform_assert(result != NULL);
   *req->ycsb_ops = result;
   platform_free(hid, req);
}

void
unload_ycsb_log(ycsb_op *log, uint64 num_ops)
{
   munlock(log, num_ops * sizeof(*log));
   platform_free(platform_get_heap_id(), log);
}

static void
usage(const char *argv0)
{
   platform_error_log(
      "Usage:\n"
      "\t%s $name $trace_prefix $threads (-c $measurement_command) (-e)\n",
      argv0);
   config_usage();
}

static platform_status
load_ycsb_logs(int          argc,
               char        *argv[],
               uint64      *nphases,
               bool        *use_existing,
               ycsb_phase **output,
               int         *args_consumed,
               uint64      *log_size_bytes_out,
               uint64      *memory_bytes_out)
{
   uint64 _nphases            = 1;
   uint64 num_threads         = 0;
   bool   mlock_log           = TRUE;
   char  *measurement_command = NULL;
   uint64 log_size_bytes      = 0;
   *use_existing              = FALSE;
   char            *name;
   platform_status  ret;
   platform_heap_id hid = platform_get_heap_id();

   if (argc < 6) {
      usage(argv[0]);
      return STATUS_BAD_PARAM;
   }

   if (argc > 6) {
      if (strncmp(argv[5], "-c", sizeof("-c")) == 0) {
         if (argc < 8) {
            usage(argv[0]);
            return STATUS_BAD_PARAM;
         }
         measurement_command = argv[7];
         if (argc > 7 && strncmp(argv[8], "-e", sizeof("-e")) == 0) {
            *use_existing  = TRUE;
            *args_consumed = 9;
         } else {
            *args_consumed = 8;
         }
      } else if (strncmp(argv[6], "-e", sizeof("-e")) == 0) {
         *use_existing  = TRUE;
         *args_consumed = 7;
      } else {
         *args_consumed = 6;
      }
   } else {
      *args_consumed = 6;
   }

   name                      = argv[1];
   char *trace_filename      = argv[2];
   num_threads               = strtoull(argv[3], NULL, 0);
   const int max_num_threads = 256;
   if (num_threads == 0 || num_threads > max_num_threads) {
      usage(argv[0]);
      return STATUS_BAD_PARAM;
   }
   uint64 num_lines = strtoull(argv[4], NULL, 0);
   if (num_lines < num_threads) {
      return STATUS_BAD_PARAM;
   }
   *memory_bytes_out = MiB_TO_B(strtoull(argv[5], NULL, 0));

   // char *resize_cgroup_command =
   //   TYPED_ARRAY_MALLOC(hid, resize_cgroup_command, 1024);
   // platform_assert(resize_cgroup_command);

   // uint64 load_bytes = *use_existing ? GiB_TO_B(128UL) : GiB_TO_B(128UL);
   // snprintf(resize_cgroup_command, 1024,
   //      "echo %lu > /sys/fs/cgroup/memory/benchmark/memory.limit_in_bytes",
   //      load_bytes);
   // int rc = system(resize_cgroup_command);
   // platform_assert(rc == 0);
   // platform_free(hid, resize_cgroup_command);

   platform_memfrag *mf = NULL;
   platform_memfrag  memfrag_phases;
   ycsb_phase       *phases = TYPED_ARRAY_MALLOC(hid, phases, _nphases);
   log_size_bytes += _nphases * sizeof(ycsb_phase);

   platform_memfrag memfrag_params;
   ycsb_log_params *params = TYPED_ARRAY_MALLOC(hid, params, num_threads);
   log_size_bytes += num_threads * sizeof(ycsb_log_params);
   platform_assert(phases && params);

   memset(phases, 0, _nphases * sizeof(ycsb_phase));
   memset(params, 0, num_threads * sizeof(ycsb_log_params));

   phases[0].params = params;

   uint64 lognum     = 0;
   uint64 batch_size = 100;

   phases[0].name                = name;
   phases[0].params              = &params[0];
   phases[0].measurement_command = measurement_command;

   uint64 start_line    = 0;
   uint64 max_range_len = 0;
   for (lognum = 0; lognum < num_threads; lognum++) {
      params[lognum].nthreads   = 1;
      params[lognum].batch_size = batch_size;
      params[lognum].filename   = trace_filename;
      parse_ycsb_log_req *req   = TYPED_MALLOC(hid, req);
      req->filename             = trace_filename;
      req->lock                 = mlock_log;
      req->num_ops              = &params[lognum].total_ops;
      req->ycsb_ops             = &params[lognum].ycsb_ops;
      req->start_line           = start_line;
      req->end_line             = start_line + num_lines / num_threads;
      req->max_range_len        = &max_range_len;
      if (lognum < num_lines % num_threads) {
         req->end_line++;
      }
      uint64 num_lines = req->end_line - req->start_line;
      log_size_bytes += num_lines * sizeof(ycsb_op);
      start_line = req->end_line;
      ret        = platform_thread_create(
         &params[lognum].thread, FALSE, parse_ycsb_log_file, req, hid);
      platform_assert_status_ok(ret);
      phases[0].nlogs++;
   }
   platform_assert(start_line == num_lines);

   for (uint64 i = 0; i < num_threads; i++) {
      platform_thread_join(params[i].thread);
      if (params[i].ycsb_ops == NULL) {
         platform_error_log("Bad log file: %s\n", params[i].filename);
         goto bad_params;
      }
   }
   log_size_bytes +=
      num_threads * max_range_len * (YCSB_KEY_SIZE + YCSB_DATA_SIZE);

   *log_size_bytes_out = log_size_bytes;
   *nphases            = _nphases;
   *output             = phases;
   return STATUS_OK;

bad_params:
   mf = &memfrag_phases;
   platform_free(hid, mf);
   mf = &memfrag_params;
   platform_free(hid, mf);
   return STATUS_BAD_PARAM;
}

void
unload_ycsb_logs(ycsb_phase *phases, uint64 nphases)
{
   int i, j;

   for (i = 0; i < nphases; i++)
      for (j = 0; j < phases[i].nlogs; j++)
         unload_ycsb_log(phases[i].params[j].ycsb_ops,
                         phases[i].params[j].total_ops);
   platform_free(platform_get_heap_id(), phases[0].params);
   platform_free(platform_get_heap_id(), phases);
}

void
compute_log_latency_tables(ycsb_log_params *params)
{
   ycsb_op *ops = params->ycsb_ops;
   memset(&params->tables, 0, sizeof(params->tables));

   uint64 i = 0;
   for (i = 0; i < params->total_ops; i++) {
      switch (ops->cmd) {
         case 'r':
            if (ops->found) {
               record_latency(params->tables.pos_queries,
                              ops->end_time - ops->start_time);
            } else {
               record_latency(params->tables.neg_queries,
                              ops->end_time - ops->start_time);
            }
            break;
         case 'd':
            record_latency(params->tables.deletes,
                           ops->end_time - ops->start_time);
            break;
         case 'i':
            record_latency(params->tables.inserts,
                           ops->end_time - ops->start_time);
            break;
         case 'u':
            record_latency(params->tables.updates,
                           ops->end_time - ops->start_time);
            break;
         case 's':
            record_latency(params->tables.scans,
                           ops->end_time - ops->start_time);
            break;
      }
      ops++;
   }
   sum_latency_tables(params->tables.all_queries,
                      params->tables.pos_queries,
                      params->tables.neg_queries);
}

void
compute_phase_latency_tables(ycsb_phase *phase)
{
   uint64_t i;
   for (i = 0; i < phase->nlogs; i++)
      compute_log_latency_tables(&phase->params[i]);

   for (i = 0; i < phase->nlogs; i++) {
      add_latency_table(phase->tables.pos_queries,
                        phase->params[i].tables.pos_queries);
      add_latency_table(phase->tables.neg_queries,
                        phase->params[i].tables.neg_queries);
      add_latency_table(phase->tables.all_queries,
                        phase->params[i].tables.all_queries);
      add_latency_table(phase->tables.deletes, phase->params[i].tables.deletes);
      add_latency_table(phase->tables.inserts, phase->params[i].tables.inserts);
      add_latency_table(phase->tables.updates, phase->params[i].tables.updates);
      add_latency_table(phase->tables.scans, phase->params[i].tables.scans);
   }
}

void
compute_all_phase_latency_tables(ycsb_phase *phases, int num_phases)
{
   uint64_t i;
   for (i = 0; i < num_phases; i++)
      compute_phase_latency_tables(&phases[i]);
}

void
compute_phase_statistics(ycsb_phase *phase)
{
   uint64_t i;

   for (i = 0; i < phase->nlogs; i++) {
      if (i == 0
          || phase->params[i].times.earliest_thread_start_time
                < phase->times.earliest_thread_start_time)
      {
         phase->times.earliest_thread_start_time =
            phase->params[i].times.earliest_thread_start_time;
      }
      if (i == 0
          || phase->params[i].times.last_thread_finish_time
                > phase->times.last_thread_finish_time)
      {
         phase->times.last_thread_finish_time =
            phase->params[i].times.last_thread_finish_time;
      }
      phase->times.sum_of_wall_clock_times +=
         phase->params[i].times.sum_of_wall_clock_times;
      phase->times.sum_of_cpu_times += phase->params[i].times.sum_of_cpu_times;
      phase->total_ops += phase->params[i].total_ops;
   }
}

void
compute_all_phase_statistics(ycsb_phase *phases, int num_phases)
{
   uint64_t i;
   for (i = 0; i < num_phases; i++)
      compute_phase_statistics(&phases[i]);
}

void
compute_all_report_data(ycsb_phase *phases, int num_phases)
{
   compute_all_phase_latency_tables(phases, num_phases);
   compute_all_phase_statistics(phases, num_phases);
}

void
write_log_latency_table(char         *phase_name,
                        uint64_t      lognum,
                        char         *operation_name,
                        latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%02lu.%s.latency.df",
            phase_name,
            lognum,
            operation_name);
   write_latency_table(filename, table);
}

void
write_log_latency_cdf(char         *phase_name,
                      uint64_t      lognum,
                      char         *operation_name,
                      latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%02lu.%s.latency.cdf",
            phase_name,
            lognum,
            operation_name);
   write_latency_cdf(filename, table);
}

void
write_phase_latency_table(char         *phase_name,
                          char         *operation_name,
                          latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%s.latency.df",
            phase_name,
            operation_name);
   write_latency_table(filename, table);
}

void
write_phase_latency_cdf(char         *phase_name,
                        char         *operation_name,
                        latency_table table)
{
   char filename[1024];
   snprintf(filename,
            sizeof(filename),
            "%s.%s.latency.cdf",
            phase_name,
            operation_name);
   write_latency_cdf(filename, table);
}

void
write_phase_latency_tables(ycsb_phase *phase)
{
   uint64_t i;
   for (i = 0; i < phase->nlogs; i++) {
      write_log_latency_table(
         phase->name, i, "pos_query", phase->params[i].tables.pos_queries);
      write_log_latency_table(
         phase->name, i, "neg_query", phase->params[i].tables.neg_queries);
      write_log_latency_table(
         phase->name, i, "all_query", phase->params[i].tables.all_queries);
      write_log_latency_table(
         phase->name, i, "delete", phase->params[i].tables.deletes);
      write_log_latency_table(
         phase->name, i, "insert", phase->params[i].tables.inserts);
      write_log_latency_table(
         phase->name, i, "update", phase->params[i].tables.updates);
      write_log_latency_table(
         phase->name, i, "scan", phase->params[i].tables.scans);
   }

   for (i = 0; i < phase->nlogs; i++) {
      write_log_latency_cdf(
         phase->name, i, "pos_query", phase->params[i].tables.pos_queries);
      write_log_latency_cdf(
         phase->name, i, "neg_query", phase->params[i].tables.neg_queries);
      write_log_latency_cdf(
         phase->name, i, "all_query", phase->params[i].tables.all_queries);
      write_log_latency_cdf(
         phase->name, i, "delete", phase->params[i].tables.deletes);
      write_log_latency_cdf(
         phase->name, i, "insert", phase->params[i].tables.inserts);
      write_log_latency_cdf(
         phase->name, i, "update", phase->params[i].tables.updates);
      write_log_latency_cdf(
         phase->name, i, "scan", phase->params[i].tables.scans);
   }

   write_phase_latency_table(
      phase->name, "pos_query", phase->tables.pos_queries);
   write_phase_latency_table(
      phase->name, "neg_query", phase->tables.neg_queries);
   write_phase_latency_table(
      phase->name, "all_query", phase->tables.all_queries);
   write_phase_latency_table(phase->name, "delete", phase->tables.deletes);
   write_phase_latency_table(phase->name, "insert", phase->tables.inserts);
   write_phase_latency_table(phase->name, "update", phase->tables.updates);
   write_phase_latency_table(phase->name, "scan", phase->tables.scans);

   write_phase_latency_cdf(phase->name, "pos_query", phase->tables.pos_queries);
   write_phase_latency_cdf(phase->name, "neg_query", phase->tables.neg_queries);
   write_phase_latency_cdf(phase->name, "all_query", phase->tables.all_queries);
   write_phase_latency_cdf(phase->name, "delete", phase->tables.deletes);
   write_phase_latency_cdf(phase->name, "insert", phase->tables.inserts);
   write_phase_latency_cdf(phase->name, "update", phase->tables.updates);
   write_phase_latency_cdf(phase->name, "scan", phase->tables.scans);
}

void
print_operation_statistics(platform_log_handle *output,
                           char                *operation_name,
                           latency_table        table)
{
   platform_log(
      output, "%s_count: %lu\n", operation_name, num_latencies(table));
   platform_log(
      output, "%s_total_latency: %lu\n", operation_name, total_latency(table));
   platform_log(
      output, "%s_min_latency: %lu\n", operation_name, min_latency(table));
   platform_log(
      output, "%s_mean_latency: %lf\n", operation_name, mean_latency(table));
   platform_log(output,
                "%s_median_latency: %lu\n",
                operation_name,
                latency_percentile(table, 50.0));
   platform_log(output,
                "%s_99.0_latency: %lu\n",
                operation_name,
                latency_percentile(table, 99));
   platform_log(output,
                "%s_99.5_latency: %lu\n",
                operation_name,
                latency_percentile(table, 99.5));
   platform_log(output,
                "%s_99.9_latency: %lu\n",
                operation_name,
                latency_percentile(table, 99.9));
   platform_log(
      output, "%s_max_latency: %lu\n", operation_name, max_latency(table));

   platform_log(
      output, "%s_000_latency: %lu\n", operation_name, min_latency(table));
   int i;
   for (i = 5; i < 100; i += 5) {
      platform_log(output,
                   "%s_%03d_latency: %lu\n",
                   operation_name,
                   i,
                   latency_percentile(table, i));
   }
   platform_log(
      output, "%s_100_latency: %lu\n", operation_name, max_latency(table));
}

void
print_statistics_file(platform_log_handle *output,
                      uint64_t             total_ops,
                      running_times       *times,
                      latency_tables      *tables)
{
   uint64_t wall_clock_time =
      times->last_thread_finish_time - times->earliest_thread_start_time;
   platform_log(output, "total_operations: %lu\n", total_ops);
   platform_log(output, "wall_clock_time: %lu\n", wall_clock_time);
   platform_log(
      output, "sum_of_wall_clock_times: %lu\n", times->sum_of_wall_clock_times);
   platform_log(output, "sum_of_cpu_times: %ld\n", times->sum_of_cpu_times);
   platform_log(output,
                "mean_overall_latency: %f\n",
                total_ops ? 1.0 * times->sum_of_wall_clock_times / total_ops
                          : 0);
   platform_log(output,
                "mean_overall_throughput: %f\n",
                wall_clock_time ? 1000000000.0 * total_ops / wall_clock_time
                                : 0);

   print_operation_statistics(output, "pos_query", tables->pos_queries);
   print_operation_statistics(output, "neg_query", tables->neg_queries);
   print_operation_statistics(output, "all_query", tables->all_queries);
   print_operation_statistics(output, "delete", tables->deletes);
   print_operation_statistics(output, "insert", tables->inserts);
   print_operation_statistics(output, "update", tables->updates);
   print_operation_statistics(output, "scan", tables->scans);
}

void
write_log_statistics_file(char            *phase_name,
                          uint64_t         lognum,
                          ycsb_log_params *params)
{
   char filename[1024];
   snprintf(
      filename, sizeof(filename), "%s.%02lu.statistics", phase_name, lognum);
   FILE *output = fopen(filename, "w");
   platform_assert(output != NULL);

   print_statistics_file(
      output, params->total_ops, &params->times, &params->tables);

   fclose(output);
}

void
write_phase_statistics_files(ycsb_phase *phase)
{
   uint64_t i;
   for (i = 0; i < phase->nlogs; i++)
      write_log_statistics_file(phase->name, i, &phase->params[i]);

   char filename[1024];
   snprintf(filename, sizeof(filename), "%s.statistics", phase->name);
   FILE *output = fopen(filename, "w");
   platform_assert(output != NULL);

   print_statistics_file(
      output, phase->total_ops, &phase->times, &phase->tables);

   fclose(output);
}

void
write_all_reports(ycsb_phase *phases, int num_phases)
{
   uint64_t i;
   for (i = 0; i < num_phases; i++) {
      write_phase_latency_tables(&phases[i]);
      write_phase_statistics_files(&phases[i]);
   }
}

int
ycsb_test(int argc, char *argv[])
{
   io_config          io_cfg;
   allocator_config   allocator_cfg;
   clockcache_config  cache_cfg;
   shard_log_config   log_cfg;
   int                config_argc;
   char             **config_argv;
   platform_status    rc;
   uint64             seed;
   task_system_config task_cfg;
   task_system       *ts = NULL;

   uint64                 nphases;
   bool                   use_existing = 0;
   ycsb_phase            *phases;
   int                    args_consumed;
   test_message_generator gen;

   uint64 log_size_bytes, memory_bytes;
   rc = load_ycsb_logs(argc,
                       argv,
                       &nphases,
                       &use_existing,
                       &phases,
                       &args_consumed,
                       &log_size_bytes,
                       &memory_bytes);
   if (!SUCCESS(rc) || phases == NULL) {
      platform_default_log("Failed to load ycsb logs\n");
      return -1;
   }
   platform_default_log("Log size: %luMiB\n", B_TO_MiB(log_size_bytes));

   config_argc = argc - args_consumed;
   config_argv = argv + args_consumed;

   // Create a heap for io, allocator, cache and splinter
   platform_heap_handle hh;
   platform_heap_id     hid;
   rc =
      platform_heap_create(platform_get_module_id(), 1 * GiB, FALSE, &hh, &hid);
   platform_assert_status_ok(rc);

   data_config  *data_cfg;
   trunk_config *splinter_cfg = TYPED_MALLOC(hid, splinter_cfg);
   uint64        num_bg_threads[NUM_TASK_TYPES] = {0}; // no bg threads

   rc = test_parse_args(splinter_cfg,
                        &data_cfg,
                        &io_cfg,
                        &allocator_cfg,
                        &cache_cfg,
                        &log_cfg,
                        &task_cfg,
                        &seed,
                        &gen,
                        &num_bg_threads[TASK_TYPE_MEMTABLE],
                        &num_bg_threads[TASK_TYPE_NORMAL],
                        config_argc,
                        config_argv);
   if (!SUCCESS(rc)) {
      platform_error_log("ycsb: failed to parse config options: %s\n",
                         platform_status_to_string(rc));
      goto cleanup;
   }

   if (data_cfg->max_key_size != YCSB_KEY_SIZE) {
      rc = STATUS_BAD_PARAM;
      platform_error_log("ycsb: key size configuration does not match\n");
      goto cleanup;
   }

   uint64 overhead_bytes = memory_bytes
                              / cache_config_page_size(splinter_cfg->cache_cfg)
                              * (sizeof(clockcache_entry) + 64)
                           + allocator_cfg.extent_capacity * sizeof(uint8)
                           + allocator_cfg.page_capacity * sizeof(uint32);
   uint64 buffer_bytes = MiB_TO_B(1024);
   // if (memory_bytes > GiB_TO_B(40)) {
   //   buffer_bytes = use_existing ? MiB_TO_B(2048) : MiB_TO_B(1280);
   //} else {
   //   buffer_bytes = use_existing ? MiB_TO_B(512) : MiB_TO_B(1280);
   //}
   // int64 buffer_bytes = use_existing ? MiB_TO_B(768) : MiB_TO_B(1280);
   buffer_bytes += overhead_bytes;
   buffer_bytes = ROUNDUP(buffer_bytes, 2 * MiB);
   platform_default_log("overhead %lu MiB buffer %lu MiB\n",
                        B_TO_MiB(overhead_bytes),
                        B_TO_MiB(buffer_bytes));
   cache_cfg.capacity      = memory_bytes - buffer_bytes;
   cache_cfg.page_capacity = cache_cfg.capacity / cache_cfg.io_cfg->page_size;

   uint64 al_size = allocator_cfg.extent_capacity * sizeof(uint8);
   al_size        = ROUNDUP(al_size, 2 * MiB);
   platform_assert(cache_cfg.capacity % (2 * MiB) == 0);
   uint64 huge_tlb_memory_bytes = cache_cfg.capacity + al_size;
   platform_assert(huge_tlb_memory_bytes % (2 * MiB) == 0);
   // uint64 huge_tlb_pages = huge_tlb_memory_bytes / (2 * MiB);
   // uint64 remaining_memory_bytes =
   //   memory_bytes + log_size_bytes - huge_tlb_memory_bytes;
   platform_default_log("memory: %lu MiB hugeTLB: %lu MiB cache: %lu MiB\n",
                        B_TO_MiB(memory_bytes),
                        B_TO_MiB(huge_tlb_memory_bytes),
                        B_TO_MiB(cache_cfg.capacity));

   // char *resize_cgroup_command =
   //   TYPED_ARRAY_MALLOC(hid, resize_cgroup_command, 1024);
   // platform_assert(resize_cgroup_command);
   // snprintf(resize_cgroup_command, 1024,
   //      "echo %lu > /sys/fs/cgroup/memory/benchmark/memory.limit_in_bytes",
   //      remaining_memory_bytes);
   // int sys_rc = system(resize_cgroup_command);
   // platform_assert(sys_rc == 0);
   // platform_free(hid, resize_cgroup_command);

   // char *resize_hugetlb_command =
   //   TYPED_ARRAY_MALLOC(hid, resize_hugetlb_command, 1024);
   // platform_assert(resize_hugetlb_command);
   // snprintf(resize_hugetlb_command, 1024,
   //      "echo %lu > /proc/sys/vm/nr_hugepages",
   //      huge_tlb_pages);
   // int sys_rc = system(resize_hugetlb_command);
   // platform_assert(sys_rc == 0);
   // platform_free(hid, resize_hugetlb_command);

   platform_io_handle *io = TYPED_MALLOC(hid, io);
   platform_assert(io != NULL);
   if (!SUCCESS(rc)) {
      goto free_iohandle;
   }
   rc = io_handle_init(io, &io_cfg, hh, hid);
   if (!SUCCESS(rc)) {
      goto free_iohandle;
   }

   rc = test_init_task_system(hid, io, &ts, &task_cfg);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to init splinter state: %s\n",
                         platform_status_to_string(rc));
      goto deinit_iohandle;
   }

   rc_allocator  al;
   clockcache   *cc = TYPED_MALLOC(hid, cc);
   trunk_handle *spl;

   if (use_existing) {
      rc_allocator_mount(
         &al, &allocator_cfg, (io_handle *)io, hid, platform_get_module_id());
      rc = clockcache_init(cc,
                           &cache_cfg,
                           (io_handle *)io,
                           (allocator *)&al,
                           "test",
                           hid,
                           platform_get_module_id());
      platform_assert_status_ok(rc);
      spl = trunk_mount(splinter_cfg,
                        (allocator *)&al,
                        (cache *)cc,
                        ts,
                        test_generate_allocator_root_id(),
                        hid);
      platform_assert(spl);
   } else {
      rc_allocator_init(
         &al, &allocator_cfg, (io_handle *)io, hid, platform_get_module_id());
      rc = clockcache_init(cc,
                           &cache_cfg,
                           (io_handle *)io,
                           (allocator *)&al,
                           "test",
                           hid,
                           platform_get_module_id());
      platform_assert_status_ok(rc);
      spl = trunk_create(splinter_cfg,
                         (allocator *)&al,
                         (cache *)cc,
                         ts,
                         test_generate_allocator_root_id(),
                         hid);
      platform_assert(spl);
   }

   run_all_ycsb_phases(spl, phases, nphases, ts, hid);

   trunk_unmount(&spl);
   clockcache_deinit(cc);
   platform_free(hid, cc);
   rc_allocator_unmount(&al);
   test_deinit_task_system(hid, &ts);
   rc = STATUS_OK;

   // struct rusage usage;
   // sys_rc = getrusage(RUSAGE_SELF, &usage);
   // platform_assert(sys_rc == 0);
   // platform_default_log("max memory usage:             %8luMiB\n",
   //      B_TO_MiB(usage.ru_maxrss * KiB));
   // platform_default_log("over provision for op buffer: %8luMiB\n",
   //      B_TO_MiB(usage.ru_maxrss * KiB - log_size_bytes));

   compute_all_report_data(phases, nphases);
   write_all_reports(phases, nphases);
   for (uint64 i = 0; i < nphases; i++) {
      for (uint64 j = 0; j < phases[i].nlogs; j++) {
         platform_free(hid, phases[i].params[j].ycsb_ops);
      }
      platform_free(hid, phases[i].params);
   }
   platform_free(hid, phases);

deinit_iohandle:
   io_handle_deinit(io);
free_iohandle:
   platform_free(hid, io);
cleanup:
   platform_free(hid, splinter_cfg);
   platform_heap_destroy(&hh);

   return SUCCESS(rc) ? 0 : -1;
}
