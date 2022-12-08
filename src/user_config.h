#include <stdio.h>
#include <string.h>

#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots
#define DB_FILE_NAME "rm24"
/* Application declares the limit of key-sizes it intends to use */
#define USER_MAX_KEY_SIZE ((int)70)
//typedef struct splinterdb splinterdb;



// trunk_handle *getcfg(){
//     data_config splinter_data_cfg;
//     default_data_config_init(USER_MAX_KEY_SIZE, &splinter_data_cfg);
//     splinterdb_config splinterdb_cfg;
//     memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
//     splinterdb_cfg.filename   = DB_FILE_NAME;
//     splinterdb_cfg.disk_size  = (DB_FILE_SIZE_MB * 1024 * 1024);
//     splinterdb_cfg.cache_size = (CACHE_SIZE_MB * 1024 * 1024);
//     splinterdb_cfg.data_cfg   = &splinter_data_cfg;
//     splinterdb *spl_handle = NULL; // To a running SplinterDB instance
//     int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
//     trunk_handle *spl=NULL;
//     spl=spl_handle->cfg;
//     return spl;
// }