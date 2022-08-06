// Copyright 2018-2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * trunk_log.h --
 *
 *     This file contains functions and macros supporting trunk trace logging.
 */

#ifndef __TRUNK_LOG_H
#define __TRUNK_LOG_H

/*
 * If verbose_logging_enabled is enabled in trunk_config, these functions print
 * to cfg->log_handle.
 */

static inline bool
trunk_verbose_logging_enabled(trunk_handle *spl)
{
   return spl->cfg.verbose_logging_enabled;
}

static inline platform_log_handle *
trunk_log_handle(trunk_handle *spl)
{
   platform_assert(trunk_verbose_logging_enabled(spl));
   platform_assert(spl->cfg.log_handle != NULL);
   return spl->cfg.log_handle;
}

static inline platform_status
trunk_open_log_stream_if_enabled(trunk_handle           *spl,
                                 platform_stream_handle *stream)
{
   if (trunk_verbose_logging_enabled(spl)) {
      return platform_open_log_stream(stream);
   }
   return STATUS_OK;
}

static inline void
trunk_close_log_stream_if_enabled(trunk_handle           *spl,
                                  platform_stream_handle *stream)
{
   if (trunk_verbose_logging_enabled(spl)) {
      platform_assert(stream != NULL);
      platform_close_log_stream(stream, trunk_log_handle(spl));
   }
}

#define trunk_log_stream_if_enabled(spl, _stream, message, ...)                \
   do {                                                                        \
      if (trunk_verbose_logging_enabled(spl)) {                                \
         platform_log_stream(                                                  \
            (_stream), "[%3lu] " message, platform_get_tid(), ##__VA_ARGS__);  \
      }                                                                        \
   } while (0)

#define trunk_default_log_if_enabled(spl, message, ...)                        \
   do {                                                                        \
      if (trunk_verbose_logging_enabled(spl)) {                                \
         platform_default_log(message, __VA_ARGS__);                           \
      }                                                                        \
   } while (0)

void
trunk_print_locked_node(platform_log_handle *log_handle,
                        trunk_handle        *spl,
                        trunk_node          *node);

static inline void
trunk_log_node_if_enabled(platform_stream_handle *stream,
                          trunk_handle           *spl,
                          trunk_node             *node)
{
   if (trunk_verbose_logging_enabled(spl)) {
      platform_log_handle *log_handle =
         platform_log_stream_to_log_handle(stream);
      trunk_print_locked_node(log_handle, spl, node);
   }
}

#endif // __TRUNK_LOG_H
