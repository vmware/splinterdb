# SplinterDB Overview

SplinterDB is an embedded, general purpose Key-Value Store designed from
the ground-up to deliver high performance on modern storage devices like NVMe
SSDs. The driving design tenet behind SplinterDB is to maximize the I/O bandwidth
on the device, thereby delivering high-performance to your application.
Internal data structures and sub-systems are all designed to be highly efficient
in their memory and CPU-resource usage.

SplinterDB, sometimes also referred to simply as Splinter, is offered as a library
with a simple Key-Value Store API that can be used to build your application.

Keys and values are stored as byte-streams where, by default, the keys are
lexicographically sorted.  User-specified key-comparison methods are also
supported, and have to be specified as part of the configuration.

Programmatic interfaces are provided to insert a new key/value pair, retrieve
a value for a given key, iterate through a set of keys, delete one or more
values given a key or range of keys.

----

## Quick-start examples of the interfaces

Here are a few examples of how to use the interfaces to create a new SplinterDB
instance, and to insert and retrieve key-value pairs.

### Configure and create a new SplinterDB instance:


```
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"

int
main()
{
   /*
    * Initialize data configuration, using default key-comparison handling.
    */
   data_config splinter_data_cfg;
   default_data_config_init(APP_MAX_KEY_SIZE, &splinter_data_cfg);

   /* Basic configuration of a SplinterDB instance */
   splinterdb_config splinterdb_cfg;
   memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
   splinterdb_cfg.filename   = APP_DB_NAME;
   splinterdb_cfg.disk_size  = (APP_DEVICE_SIZE_MB * 1024);
   splinterdb_cfg.cache_size = (APP_CACHE_SIZE_MB * 1024);
   splinterdb_cfg.data_cfg   = &splinter_data_cfg;

   splinterdb *spl_handle = NULL; // To a running SplinterDB instance

   int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
   printf("Created SplinterDB instance, dbname '%s'.\n\n", APP_DB_NAME);
```

### Insert a new key-value pair:

```
   const char *fruit = "apple";
   const char *descr = "An apple a day keeps the doctor away!";
   slice       key   = slice_create((size_t)strlen(fruit), fruit);
   slice       value = slice_create((size_t)strlen(descr), descr);
   rc                = splinterdb_insert(spl_handle, key, value);
   printf("Inserted key '%s'\n", fruit);
```

### Retrieve a value given the key:

```
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);

   fruit = "Orange";
   key   = slice_create((size_t)strlen(fruit), fruit);
   rc    = splinterdb_lookup(spl_handle, key, &result);
   rc    = splinterdb_lookup_result_value(spl_handle, &result, &value);
   printf("Found key: '%s', value: '%.*s'\n",
          fruit, (int)slice_length(value), (char *)slice_data(value));
```

----

## Technical Specifications

* Can we some things here like ... ?? Should be one layer down ...
* Supported on Linux (Mac support is in the works.)
* OS-Level features used ... not a make or break, can be shoved down.
    * Async I/O (libaio?)
    * Pthread support

* Out-of-the box configuration: (See [Configuration](./configuration.md) for more details.)
    * Max key-size = 256 bytes, Max value-size = 1024 bytes
    * 4 KB page size

