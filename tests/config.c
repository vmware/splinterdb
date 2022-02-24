// Copyright 2018-2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "config.h"

void
config_set_defaults(master_config *cfg)
{
   *cfg = (master_config){
      .io_filename   = "db",
      .cache_logfile = "cache_log",

      .page_size   = 4096,
      .extent_size = 128 * 1024,

      .io_flags             = O_RDWR | O_CREAT,
      .io_perms             = 0755,
      .io_async_queue_depth = 256,

      .allocator_capacity = GiB_TO_B(30),

      .cache_capacity = GiB_TO_B(1),

      .btree_rough_count_height = 1,

      .filter_remainder_size = 6,
      .filter_index_size     = 256,

      .use_log = FALSE,

      .memtable_capacity     = MiB_TO_B(24),
      .fanout                = 8,
      .max_branches_per_node = 24,
      .use_stats             = FALSE,
      .reclaim_threshold     = UINT64_MAX,

      .key_size     = 24,
      .message_size = 100,

      .seed = 0,
   };
}

static scaling_entry standard_scaling_entries[] = {
   {"k", 1024},
   {"kB", 1024},
   {"kb", 1024},
   {"kiB", 1024},
   {"kib", 1024},
   {"K", 1024},
   {"KB", 1024},
   {"Kb", 1024},
   {"KiB", 1024},
   {"Kib", 1024},
   {"m", 1024 * 1024},
   {"mB", 1024 * 1024},
   {"mb", 1024 * 1024},
   {"miB", 1024 * 1024},
   {"mib", 1024 * 1024},
   {"M", 1024 * 1024},
   {"MB", 1024 * 1024},
   {"Mb", 1024 * 1024},
   {"MiB", 1024 * 1024},
   {"Mib", 1024 * 1024},
   {"g", 1024 * 1024 * 1024},
   {"gB", 1024 * 1024 * 1024},
   {"gb", 1024 * 1024 * 1024},
   {"giB", 1024 * 1024 * 1024},
   {"gib", 1024 * 1024 * 1024},
   {"G", 1024 * 1024 * 1024},
   {"GB", 1024 * 1024 * 1024},
   {"Gb", 1024 * 1024 * 1024},
   {"GiB", 1024 * 1024 * 1024},
   {"Gib", 1024 * 1024 * 1024},
};

scaling_table standard_scaling_table = {.nentries =
                                           ARRAY_SIZE(standard_scaling_entries),
                                        .entries = standard_scaling_entries};

static scaling_entry *
lookup_scaling_entry(scaling_table *st, const char *suffix)
{
   for (int i = 0; i < st->nentries; i++) {
      if (strcmp(st->entries[i].suffix, suffix) == 0) {
         return &st->entries[i];
      }
   }
   return NULL;
}

static char *config_type_names[5] = {"bool", "int", "int", "string", "string"};

static bool
config_read_integer_field(config_field *f, char *default_vals, uint64 *v)
{
   char *p = default_vals + f->offset;
   if (f->size == 1) {
      *v = *(uint8 *)p;
   } else if (f->size == 2) {
      *v = *(uint16 *)p;
   } else if (f->size == 4) {
      *v = *(uint32 *)p;
   } else if (f->size == 8) {
      *v = *(uint64 *)p;
   } else {
      platform_error_log("Internal error: invalid field-size in config spec\n");
      return FALSE;
   }

   return TRUE;
}

static void
config_print_value(config_field *f, char *default_vals)
{
   char  *p = default_vals + f->offset;
   uint64 v;
   if (!config_read_integer_field(f, default_vals, &v)) {
      return;
   }

   switch (f->type) {
      case CONFIG_FIELD_BOOL:
         platform_error_log("%s", v ? "true" : "false");
         break;
      case CONFIG_FIELD_UINT:
         platform_error_log("%lu", v);
         break;
      case CONFIG_FIELD_INT:
         platform_error_log("%ld", (int64)v);
         break;
      case CONFIG_FIELD_CHAR_POINTER:
         platform_error_log("%s", (char *)v);
         break;
      case CONFIG_FIELD_INLINE_STRING:
         platform_error_log("%.*s", (int)f->size, p);
         break;
      default:
         platform_error_log(
            "Internal error: invalid field-type in config spec\n");
   }
}

void
config_struct_usage(config_struct *st, char *default_vals)
{
   for (int i = 0; i < st->nfields; i++) {
      config_field *f = &st->fields[i];
      if (f->type != CONFIG_FIELD_BOOL) {
         platform_error_log("  --%s <%s>\n"
                            "  -%s  <%s>\n"
                            "       %s\n",
                            f->long_name,
                            config_type_names[f->type],
                            f->short_name,
                            config_type_names[f->type],
                            f->description);
      } else {
         platform_error_log("  --%s\n"
                            "  --no-%s\n"
                            "  --set-%s\n"
                            "  --unset-%s\n"
                            "  -%s\n"
                            "       %s\n",
                            f->long_name,
                            f->long_name,
                            f->long_name,
                            f->long_name,
                            f->short_name,
                            f->description);
      }
      platform_error_log("       (default: ");
      config_print_value(f, default_vals);
      platform_error_log(")\n");
   }
}

void
config_structs_usage(uint64 nstructs, config_struct *structs, char **defaults)
{
   for (int i = 0; i < nstructs; i++) {
      config_struct_usage(&structs[i], defaults[i]);
   }
}

static int
required_args(config_field *f)
{
   if (f->type == CONFIG_FIELD_BOOL)
      return 1;
   else
      return 2;
}

static void
config_set_integer_field(config_field *f, char *s, uint64 v)
{
   if (f->type == CONFIG_FIELD_BOOL) {
      uint64 oldv = config_read_integer_field(f, s, &oldv);
      if (v) {
         v = oldv | f->value;
      } else {
         v = oldv & ~f->value;
      }
   }
   char *p = s + f->offset;
   if (f->size == 1) {
      *(uint8 *)p = v;
   } else if (f->size == 2) {
      *(uint16 *)p = v;
   } else if (f->size == 4) {
      *(uint32 *)p = v;
   } else if (f->size == 8) {
      *(uint64 *)p = v;
   } else {
      platform_error_log("Internal error: invalid field-size in config spec\n");
      return;
   }
}

static bool
config_parse_integer_value(config_field *f, const char *arg, uint64 *v)
{
   uint64 negative_limit;
   uint64 positive_limit;

   if (f->type == CONFIG_FIELD_UINT) {
      negative_limit = 0;
      positive_limit = (1ULL << (f->size * 8)) - 1;
   } else {
      negative_limit = 1ULL << (f->size * 8 - 1);
      positive_limit = (1ULL << (f->size * 8 - 1)) - 1;
   }

   const char *suffix =
      try_string_to_uint64_limit(arg, negative_limit, positive_limit, v);
   if (!suffix) {
      platform_error_log("Invalid integer parameter \"%s\"", arg);
      return FALSE;
   }

   if (*suffix) {
      scaling_entry *entry = lookup_scaling_entry(f->scales, suffix);
      if (entry == NULL) {
         platform_error_log("Invalid scaling suffix \"%s\"", suffix);
         return FALSE;
      }
      uint64 newv = *v * entry->scaling_factor;
      if (newv / entry->scaling_factor != *v || positive_limit < newv
          || negative_limit < -newv)
      {
         platform_error_log("Integer parameter too large \"%s\"", arg);
         return FALSE;
      }
      *v = newv;
   }
   return TRUE;
}

int
config_struct_parse_arg(int            argc,
                        const char   **argv,
                        config_struct *st,
                        char          *value)
{
   const char *opt     = argv[0];
   int   longopt = FALSE;

   if (*opt != '-') {
      return 0;
   }
   opt++;
   if (*opt == '-') {
      longopt = TRUE;
      opt++;
   }

   const char *negopt = NULL;
   const char *posopt = opt;
   if (longopt && strncmp("no-", opt, 3) == 0) {
      negopt = opt + 3;
   } else if (longopt && strncmp("unset-", opt, 6) == 0) {
      negopt = opt + 6;
   } else if (longopt && strncmp("set-", opt, 4) == 0) {
      posopt = opt + 4;
   }

   for (int i = 0; i < st->nfields; i++) {
      config_field *f      = &st->fields[i];
      char         *target = longopt ? f->long_name : f->short_name;
      if (!target) {
         continue;
      }
      if (strcmp(opt, target) == 0) {
         if (argc < required_args(f)) {
            continue;
         }
         switch (f->type) {
            case CONFIG_FIELD_BOOL:
               config_set_integer_field(f, value, 1);
               return 1;
            case CONFIG_FIELD_UINT:
            case CONFIG_FIELD_INT:
            {
               uint64 v;
               bool   success = config_parse_integer_value(f, argv[1], &v);
               if (!success)
                  continue;
               config_set_integer_field(f, value, v);
               return 2;
            }
            case CONFIG_FIELD_CHAR_POINTER:
               config_set_integer_field(f, value, (uint64)argv[1]);
               return 2;
            case CONFIG_FIELD_INLINE_STRING:
               snprintf(value + f->offset, f->size, "%s", argv[1]);
               return 2;
         }
      } else if (f->type == CONFIG_FIELD_BOOL && strcmp(posopt, target) == 0) {
         config_set_integer_field(f, value, 1);
         return 1;
      } else if (f->type == CONFIG_FIELD_BOOL && strcmp(negopt, target) == 0) {
         config_set_integer_field(f, value, 0);
         return 1;
      }
   }

   return 0;
}

bool
config_structs_parse_args(int            argc,
                          const char   **argv,
                          uint64         nstructs,
                          config_struct *structs,
                          char         **values)
{
   int i = 0;
   while (i < argc) {
      int consumed;
      for (int s = 0; s < nstructs; s++) {
         consumed =
            config_struct_parse_arg(argc - i, argv + i, &structs[s], values[s]);
         if (consumed) {
            break;
         }
      }
      if (!consumed) {
         platform_error_log("Unknown command-line argument \"%s\"", argv[i]);
         return FALSE;
      }
      i += consumed;
   }

   return TRUE;
}

config_field io_config_fields[] = {
   CONFIG_STRUCT_FIELD(io_config,
                       async_queue_size,
                       CONFIG_FIELD_UINT,
                       "libaio-queue-depth",
                       "Q",
                       "queue depth for libaio",
                       0,
                       NULL),
   CONFIG_STRUCT_FIELD(io_config,
                       page_size,
                       CONFIG_FIELD_UINT,
                       "page_size",
                       "P",
                       "page size (in bytes) for libaio, caching, etc",
                       0,
                       &standard_scaling_table),
   CONFIG_STRUCT_FIELD(io_config,
                       extent_size,
                       CONFIG_FIELD_UINT,
                       "extent_size",
                       "E",
                       "extent size (in bytes) for libaio, caching, etc",
                       0,
                       &standard_scaling_table),
   CONFIG_STRUCT_FIELD(io_config,
                       flags,
                       CONFIG_FIELD_BOOL,
                       "O_DIRECT",
                       "D",
                       "use O_DIRECT",
                       O_DIRECT,
                       NULL),
   CONFIG_STRUCT_FIELD(io_config,
                       flags,
                       CONFIG_FIELD_BOOL,
                       "O_CREAT",
                       "C",
                       "create database file if it does not already exist",
                       O_CREAT,
                       NULL),
   CONFIG_STRUCT_FIELD(io_config,
                       filename,
                       CONFIG_FIELD_INLINE_STRING,
                       "db-location",
                       "F",
                       "name of the database file",
                       0,
                       NULL),
   CONFIG_STRUCT_FIELD(io_config,
                       perms,
                       CONFIG_FIELD_UINT,
                       "db-perms",
                       "p",
                       "permissions to set when creating the database file",
                       0,
                       NULL),
};

config_struct io_config_struct = CONFIG_STRUCT_OF_FIELDS(io_config_fields);

config_field rc_allocator_config_fields[] = {
   CONFIG_STRUCT_FIELD(rc_allocator_config,
                       capacity,
                       CONFIG_FIELD_UINT,
                       "db-capacity",
                       "S",
                       "size of the database file",
                       0,
                       &standard_scaling_table),
};

config_struct rc_allocator_config_struct =
   CONFIG_STRUCT_OF_FIELDS(rc_allocator_config_fields);

config_field clockcache_config_fields[] = {
   CONFIG_STRUCT_FIELD(clockcache_config,
                       capacity,
                       CONFIG_FIELD_UINT,
                       "cache-capacity",
                       "C",
                       "size of the cache",
                       0,
                       &standard_scaling_table),
   CONFIG_STRUCT_FIELD(clockcache_config,
                       use_stats,
                       CONFIG_FIELD_BOOL,
                       "cache-stats",
                       "s",
                       "collect cache statistics",
                       1,
                       NULL),
   CONFIG_STRUCT_FIELD(clockcache_config,
                       logfile,
                       CONFIG_FIELD_INLINE_STRING,
                       "cache-debug-log",
                       "l",
                       "file for cache event logging",
                       0,
                       NULL),
};

config_struct clockcache_config_struct =
   CONFIG_STRUCT_OF_FIELDS(clockcache_config_fields);

config_field btree_config_fields[] = {
   CONFIG_STRUCT_FIELD(
      btree_config,
      rough_count_height,
      CONFIG_FIELD_UINT,
      "rough-count-height",
      "h",
      "height to descend to for rough b-tree statistics computations",
      0,
      NULL),
};

config_struct btree_config_struct =
   CONFIG_STRUCT_OF_FIELDS(btree_config_fields);

config_field routing_config_fields[] = {
   CONFIG_STRUCT_FIELD(routing_config,
                       fingerprint_size,
                       CONFIG_FIELD_UINT,
                       "filter-remainder-size",
                       "r",
                       "remainder size in the routing filter",
                       0,
                       NULL),
   CONFIG_STRUCT_FIELD(routing_config,
                       index_size,
                       CONFIG_FIELD_UINT,
                       "filter-index-size",
                       "I",
                       "size of the routing filter index",
                       0,
                       NULL),
};

config_struct routing_config_struct =
   CONFIG_STRUCT_OF_FIELDS(routing_config_fields);

void
config_usage()
{
   platform_error_log("Configuration:\n");
   platform_error_log("\t--page_size\n");
   platform_error_log("\t--extent_size\n");
   platform_error_log("\t--set-hugetlb\n");
   platform_error_log("\t--unset-hugetlb\n");
   platform_error_log("\t--set-mlock\n");
   platform_error_log("\t--unset-mlock\n");
   platform_error_log("\t--db-location\n");
   platform_error_log("\t--set-O_DIRECT\n");
   platform_error_log("\t--unset-O_DIRECT\n");
   platform_error_log("\t--set-O_CREAT\n");
   platform_error_log("\t--unset-O_CREAT\n");
   platform_error_log("\t--db-perms\n");
   platform_error_log("\t--db-capacity-gib\n");
   platform_error_log("\t--db-capacity-mib\n");
   platform_error_log("\t--libaio-queue-depth\n");
   platform_error_log("\t--cache-capacity-gib\n");
   platform_error_log("\t--cache-capacity-mib\n");
   platform_error_log("\t--cache-debug-log\n");
   platform_error_log("\t--memtable-capacity-gib\n");
   platform_error_log("\t--memtable-capacity-mib\n");
   platform_error_log("\t--rough-count-height\n");
   platform_error_log("\t--filter-remainder-size\n");
   platform_error_log("\t--fanout\n");
   platform_error_log("\t--max-branches-per-node\n");
   platform_error_log("\t--stats\n");
   platform_error_log("\t--no-stats\n");
   platform_error_log("\t--log\n");
   platform_error_log("\t--no-log\n");
   platform_error_log("\t--key-size\n");
   platform_error_log("\t--data-size\n");
   platform_error_log("\t--seed\n");
}

platform_status
config_parse(master_config *cfg, const uint8 num_config, int argc, char *argv[])
{
   uint64 i;
   uint8  cfg_idx;
   for (i = 0; i < argc; i++) {
      if (0) {
         config_set_uint64("page-size", cfg, page_size)
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               if (cfg[cfg_idx].page_size != 4096) {
                  platform_error_log("page_size must be 4096 for now\n");
                  platform_error_log("config: failed to parse page-size\n");
                  return STATUS_BAD_PARAM;
               }
               if (!IS_POWER_OF_2(cfg[cfg_idx].page_size)) {
                  platform_error_log("page_size must be a power of 2\n");
                  platform_error_log("config: failed to parse page-size\n");
                  return STATUS_BAD_PARAM;
               }
            }
         }
         config_set_uint64("extent-size", cfg, extent_size) {}
         config_has_option("set-hugetlb")
         {
            platform_use_hugetlb = TRUE;
         }
         config_has_option("unset-hugetlb")
         {
            platform_use_hugetlb = FALSE;
         }
         config_has_option("set-mlock")
         {
            platform_use_mlock = TRUE;
         }
         config_has_option("unset-mlock")
         {
            platform_use_mlock = FALSE;
         }
         config_set_string("db-location", cfg, io_filename) {}
         config_has_option("set-O_DIRECT")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags |= O_DIRECT;
            }
         }
         config_has_option("unset-O_DIRECT")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags &= ~O_DIRECT;
            }
         }
         config_has_option("set-O_CREAT")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags |= O_CREAT;
            }
         }
         config_has_option("unset-O_CREAT")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].io_flags &= ~O_CREAT;
            }
         }
         config_set_uint32("db-perms", cfg, io_perms) {}
         config_set_mib("db-capacity", cfg, allocator_capacity) {}
         config_set_gib("db-capacity", cfg, allocator_capacity) {}
         config_set_uint64("libaio-queue-depth", cfg, io_async_queue_depth) {}
         config_set_mib("cache-capacity", cfg, cache_capacity) {}
         config_set_gib("cache-capacity", cfg, cache_capacity) {}
         config_set_string("cache-debug-log", cfg, cache_logfile) {}
         config_set_mib("memtable-capacity", cfg, memtable_capacity) {}
         config_set_gib("memtable-capacity", cfg, memtable_capacity) {}
         config_set_uint64("rough-count-height", cfg, btree_rough_count_height)
         {}
         config_set_uint64("filter-remainder-size", cfg, filter_remainder_size)
         {}
         config_set_uint64("fanout", cfg, fanout) {}
         config_set_uint64("max-branches-per-node", cfg, max_branches_per_node)
         {}
         config_set_mib("reclaim-threshold", cfg, reclaim_threshold) {}
         config_set_gib("reclaim-threshold", cfg, reclaim_threshold) {}
         config_has_option("stats")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_stats = TRUE;
            }
         }
         config_has_option("no-stats")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_stats = FALSE;
            }
         }
         config_has_option("log")
         {
            for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
               cfg[cfg_idx].use_log = TRUE;
            }
         }
         config_set_uint64("key-size", cfg, key_size) {}
         config_set_uint64("data-size", cfg, message_size) {}
         config_set_uint64("seed", cfg, seed) {}
         config_set_else
         {
            platform_error_log("config: invalid option: %s\n", argv[i]);
            return STATUS_BAD_PARAM;
         }
      }
      for (cfg_idx = 0; cfg_idx < num_config; cfg_idx++) {
         if (cfg[cfg_idx].extent_size % cfg[cfg_idx].page_size != 0) {
            platform_error_log(
               "config: extent_size is not a multiple of page_size\n");
            return STATUS_BAD_PARAM;
         }
         if (cfg[cfg_idx].extent_size / cfg[cfg_idx].page_size
             > MAX_PAGES_PER_EXTENT) {
            platform_error_log("config: pages per extent too high: %lu > %lu\n",
                               cfg[cfg_idx].extent_size
                                  / cfg[cfg_idx].page_size,
                               MAX_PAGES_PER_EXTENT);
            return STATUS_BAD_PARAM;
         }
      }
      return STATUS_OK;
   }
