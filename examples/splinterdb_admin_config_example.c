// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * SplinterDB Basic Configuration Example Program.
 */

#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "example_common.h"

/* Tag to identify messages from application program */
#define APP_ME "App-Config"

/*
 * App-specific 'defaults' that can be parameterized, eventually.
 */
#define APP_DB_NAME "splinterdb_admin_example_db"

#define APP_DEVICE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define APP_CACHE_SIZE_MB  64   // Size of cache; can be changed across boots

/* Application declares the limit of key-sizes it intends to use */
#define APP_MAX_KEY_SIZE ((int)100)

// Function Prototypes
void
configure_splinter_instance(splinterdb_config *splinterdb_cfg,
                            data_config       *splinter_data_cfg,
                            const char        *filename,
                            uint64             dev_size,
                            uint64             cache_size);

int
reboot_splinter_instance(const splinterdb_config *splinterdb_cfg,
                         splinterdb             **spl_handle_out);

/*
 * -------------------------------------------------------------------------------
 * main() Driver for basic SplinterDB example program.
 * -------------------------------------------------------------------------------
 */
int
main()
{
   printf("     **** SplinterDB Basic Example program ****\n\n");

   // Initialize data configuration, describing your key-value properties
   data_config splinter_data_cfg;
   default_data_config_init(APP_MAX_KEY_SIZE, &splinter_data_cfg);

   // Basic configuration of a SplinterDB instance
   splinterdb_config splinterdb_cfg;
   configure_splinter_instance(&splinterdb_cfg,
                               &splinter_data_cfg,
                               APP_DB_NAME,
                               (APP_DEVICE_SIZE_MB * K_MiB),
                               (APP_CACHE_SIZE_MB * K_MiB));

   splinterdb *spl_handle = NULL; // To a running SplinterDB instance

   // -- ACTION IS HERE --
   int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   if (rc) {
      ex_err("SplinterDB creation failed. (rc=%d)\n", rc);
      return rc;
   }
   ex_msg("Created SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   rc = reboot_splinter_instance(&splinterdb_cfg, &spl_handle);
   if (rc) {
      ex_err("SplinterDB reboot failed. (rc=%d)\n", rc);
      return rc;
   }

   splinterdb_close(&spl_handle);
   ex_msg("Shutdown SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   return rc;
}

/*
 * -------------------------------------------------------------------------------
 * configure_splinter_instance()
 *
 * Basic configuration of a SplinterDB instance, specifying min parameters such
 * as the device's name, device and cache sizes.
 * -------------------------------------------------------------------------------
 */
void
configure_splinter_instance(splinterdb_config *splinterdb_cfg,
                            data_config       *splinter_data_cfg,
                            const char        *filename,
                            uint64             dev_size, // in bytes
                            uint64             cache_size)           // in bytes
{
   memset(splinterdb_cfg, 0, sizeof(*splinterdb_cfg));
   splinterdb_cfg->filename   = filename;
   splinterdb_cfg->disk_size  = dev_size;
   splinterdb_cfg->cache_size = cache_size;
   splinterdb_cfg->data_cfg   = splinter_data_cfg;
   return;
}

/*
 * -------------------------------------------------------------------------------
 * reboot_splinter_instance()
 *
 * Commands to shutdown a running SplinterDB instance and re-start it with
 * a previously used configuration.
 * -------------------------------------------------------------------------------
 */
int
reboot_splinter_instance(const splinterdb_config *splinterdb_cfg,
                         splinterdb             **spl_handle_out)
{
   splinterdb *spl_handle = *spl_handle_out;

   // -- ACTION IS HERE --
   splinterdb_close(&spl_handle);
   ex_msg("Shutdown SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);

   int rc = splinterdb_open(splinterdb_cfg, spl_handle_out);
   if (rc) {
      ex_err("Error re-opening SplinterDB instance, dbname '%s'.\n",
             APP_DB_NAME);
   }
   ex_msg("Re-opened SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);
   return rc;
}
