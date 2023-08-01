#pragma once

#include "platform.h"

#define USE_TRANSACTION_STATS 0

#define MAX_RECORD_SIZE 1000000

typedef struct transaction_stats {
   uint64 begin_time[MAX_THREADS];
   uint64 commit_start_time[MAX_THREADS];
   uint64 write_start_time[MAX_THREADS];

   uint64 *transaction_times[MAX_THREADS];
   uint64 *execution_times[MAX_THREADS];
   uint64 *validation_times[MAX_THREADS];
   uint64 *write_times[MAX_THREADS];

   uint64 *abort_transaction_times[MAX_THREADS];
   uint64 *abort_execution_times[MAX_THREADS];
   uint64 *abort_validation_times[MAX_THREADS];

   uint64 num_commit_txns[MAX_THREADS];
   uint64 num_abort_txns[MAX_THREADS];
} transaction_stats;

void
transaction_stats_init(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   stats->transaction_times[thr_i] =
      TYPED_ARRAY_ZALLOC(0, stats->transaction_times[thr_i], MAX_RECORD_SIZE);
   stats->execution_times[thr_i] =
      TYPED_ARRAY_ZALLOC(0, stats->execution_times[thr_i], MAX_RECORD_SIZE);
   stats->validation_times[thr_i] =
      TYPED_ARRAY_ZALLOC(0, stats->validation_times[thr_i], MAX_RECORD_SIZE);
   stats->write_times[thr_i] =
      TYPED_ARRAY_ZALLOC(0, stats->write_times[thr_i], MAX_RECORD_SIZE);

   stats->abort_transaction_times[thr_i] = TYPED_ARRAY_ZALLOC(
      0, stats->abort_transaction_times[thr_i], MAX_RECORD_SIZE);
   stats->abort_execution_times[thr_i] = TYPED_ARRAY_ZALLOC(
      0, stats->abort_execution_times[thr_i], MAX_RECORD_SIZE);
   stats->abort_validation_times[thr_i] = TYPED_ARRAY_ZALLOC(
      0, stats->abort_validation_times[thr_i], MAX_RECORD_SIZE);

   stats->num_commit_txns[thr_i] = 0;
   stats->num_abort_txns[thr_i]  = 0;
}

void
transaction_stats_deinit(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   platform_free(0, stats->transaction_times[thr_i]);
   platform_free(0, stats->execution_times[thr_i]);
   platform_free(0, stats->validation_times[thr_i]);
   platform_free(0, stats->write_times[thr_i]);

   platform_free(0, stats->abort_transaction_times[thr_i]);
   platform_free(0, stats->abort_execution_times[thr_i]);
   platform_free(0, stats->abort_validation_times[thr_i]);
}

void
transaction_stats_begin(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   stats->begin_time[thr_i] = platform_get_timestamp();
}

void
transaction_stats_commit_start(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   stats->commit_start_time[thr_i] = platform_get_timestamp();
}

void
transaction_stats_write_start(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   stats->write_start_time[thr_i] = platform_get_timestamp();
}

void
transaction_stats_commit_end(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   uint64 end_time = platform_get_timestamp();
   uint64 write_time =
      platform_timestamp_diff(stats->write_start_time[thr_i], end_time);
   stats->write_times[thr_i][stats->num_commit_txns[thr_i]] = write_time;
   uint64 validation_time = platform_timestamp_diff(
      stats->commit_start_time[thr_i], stats->write_start_time[thr_i]);
   stats->validation_times[thr_i][stats->num_commit_txns[thr_i]] =
      validation_time;
   uint64 execution_time = platform_timestamp_diff(
      stats->begin_time[thr_i], stats->commit_start_time[thr_i]);
   stats->execution_times[thr_i][stats->num_commit_txns[thr_i]] =
      execution_time;
   uint64 transaction_time =
      platform_timestamp_diff(stats->begin_time[thr_i], end_time);
   stats->transaction_times[thr_i][stats->num_commit_txns[thr_i]] =
      transaction_time;
   stats->num_commit_txns[thr_i]++;
}

void
transaction_stats_abort_end(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   uint64 end_time = platform_get_timestamp();
   uint64 validation_time =
      platform_timestamp_diff(stats->commit_start_time[thr_i], end_time);
   stats->abort_validation_times[thr_i][stats->num_abort_txns[thr_i]] =
      validation_time;
   uint64 execution_time = platform_timestamp_diff(
      stats->begin_time[thr_i], stats->commit_start_time[thr_i]);
   stats->abort_execution_times[thr_i][stats->num_abort_txns[thr_i]] =
      execution_time;
   uint64 transaction_time =
      platform_timestamp_diff(stats->begin_time[thr_i], end_time);
   stats->abort_transaction_times[thr_i][stats->num_abort_txns[thr_i]] =
      transaction_time;
   stats->num_abort_txns[thr_i]++;
}

void
transaction_stats_dump(transaction_stats *stats, int thr_i)
{
   platform_assert(thr_i < MAX_THREADS);
   char logfile[128] = {0};
   sprintf(logfile, "transaction_stats_%d.txt", thr_i);
   platform_log_handle *lh = platform_open_log_file(logfile, "w");
   int                  i;
   platform_log(
      lh, "commit_transaction_times: %lu\n", stats->num_commit_txns[thr_i]);
   for (i = 0; i < stats->num_commit_txns[thr_i]; i++) {
      platform_log(lh, "T %lu\n", stats->transaction_times[thr_i][i]);
      platform_log(lh, "E %lu\n", stats->execution_times[thr_i][i]);
      platform_log(lh, "V %lu\n", stats->validation_times[thr_i][i]);
      platform_log(lh, "W %lu\n", stats->write_times[thr_i][i]);
   }
   platform_log(
      lh, "abort_transaction_times: %lu\n", stats->num_abort_txns[thr_i]);
   for (i = 0; i < stats->num_abort_txns[thr_i]; i++) {
      platform_log(lh, "T %lu\n", stats->abort_transaction_times[thr_i][i]);
      platform_log(lh, "E %lu\n", stats->abort_execution_times[thr_i][i]);
      platform_log(lh, "V %lu\n", stats->abort_validation_times[thr_i][i]);
   }
   platform_close_log_file(lh);
}