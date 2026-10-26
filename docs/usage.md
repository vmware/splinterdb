# Using SplinterDB

SplinterDB is an embedded key-value store.  It is currently Linux-only.

To use SplinterDB, you must link your program to the library and call
the C functions declared in the [`include/splinterdb`](../include/splinterdb)
headers.  More details on our build process are [here](build.md).

This document is a high-level overview, but the code comments in the
[header files](../include/splinterdb) are the definitive API reference.
We're actively evolving our public API surface right now, and will do our
best to keep this document up to date as details change.  If something
looks out-of-date, please open an issue or pull request.

## Program flow

- Choose values for [configuration options](../include/splinterdb/splinterdb.h#:~:text=Configuration%20options%20for%20SplinterDB)
  defined in the `splinterdb_config{}` structure.
  For the `data_cfg` field, simple applications may opt to use
  the `default_data_config_init()`
  [initializer method](../src/default_data_config.c#:~:text=default%5Fdata%5Fconfig%5Finit)
  which provides a basic key / value interface with lexicographical
  sorting of keys.

- Call `splinterdb_create()` to create a new database in a file or block device,
  `splinterdb_open()` to open an existing database, and
  `splinterdb_close()` to flush any pending writes and close the database.

   All access to a SplinterDB database file or device must go through the
   single `splinterdb*` object returned.  It is not safe for more than one
   process to open the same SplinterDB database file or device.

- Basic key/value operations like `insert`, `delete`, point `lookup` and
  range scan using an `iterator` are available.  See the code comments
  in [`splinterdb.h`](../include/splinterdb/splinterdb.h).

- If an application frequently makes small modifications to existing values,
  better performance may be possible by using a custom `data_config` object
  and the `splinterdb_insert_raw_message()` function.
  A raw message may encode a "blind update" to a value, for example an
  "append" or "increment" operation that may be persisted without doing a read.
  Some applications may be able to avoid a read-modify-write sequence this way.
  To use the message-oriented API, the application implements the `data_config`
  interface defined in
  [`data.h`](../include/splinterdb/data.h#:~:text=struct%20data%5Fconfig%20{))
  and sets it on `splinterdb_config` when creating/opening a database.

- SplinterDB is designed to deliver high-performance for multi-threaded
  applications, but follow these guidelines:

  - The thread which called `splinterdb_create()` or `splinterdb_open()`
    is called the "initial thread".

  - The initial thread should be the one to call `splinterdb_close()` when
    the `splinterdb` instance is no longer needed.

  - Threads are registered automatically the first time they call a SplinterDB
    public API, and are deregistered automatically when they exit. Internally,
    SplinterDB allocates per-thread scratch space at registration time.

  - The low-level thread registration APIs remain available for tests and
    callers that want to reserve or release a thread ID explicitly.

  - Note these rules apply to system threads, not [runtime-managed threading](https://en.wikipedia.org/wiki/Green_threads)
    available in higher-level languages.

- Internally SplinterDB supports asynchronous IO, but this capability is not
  yet documented.  Some example code for this may be found in the unit and
  functional tests.


## Reusing a value blob

Use `splinterdb_blob_create()` to copy a value into blob storage once, then
`splinterdb_insert_blob()` to insert it under multiple keys. Full extents are
shared; tails may be copied on insertion or compaction. Small values may be
copied entirely, and each handle has allocation overhead, so this API is most
useful for large repeated values.

```c
int
insert_shared_values(splinterdb *db, slice value, slice first_key, slice second_key)
{
   splinterdb_blob *blob = NULL;
   int rc = splinterdb_blob_create(db, value, &blob);
   if (rc != 0) {
      return rc;
   }

   rc = splinterdb_insert_blob(db, first_key, blob, NULL);
   if (rc == 0) {
      rc = splinterdb_insert_blob(db, second_key, blob, NULL);
   }
   splinterdb_blob_destroy(&blob);
   if (rc == 0) {
      rc = splinterdb_durable_barrier(db, NULL);
   }
   return rc;
}
```

The input value may be released after creation succeeds. Destroying a handle
releases the caller's ownership and clears its pointer; successfully inserted
values retain their own ownership. The two inserts above are independent
operations: if the second fails, the first is still applied.

A handle is immutable and belongs to the same open database instance that
created it. Threads may insert it concurrently, but destruction must not race
any use. Handles cannot be serialized or reused after close/reopen. Destroy
all handles before `splinterdb_close(&db, FALSE)`, which returns `EBUSY` while
handles remain live. Forced close releases the handles and invalidates every
outstanding pointer; do not use or destroy those pointers afterward.

Creating a standalone blob does not establish crash durability. Inserted
references follow ordinary insert and durability-barrier semantics; a
successful blocking barrier covers the preceding successful inserts and their
blob data. Recovery reclaims storage that no recovered data references.
Keys still use the existing `slice` API and key-size limit; large-key support
is separate from this value-blob API.


## Example programs
- [`tests/unit/splinterdb_quick_test.c`](../tests/unit/splinterdb_quick_test.c) covers
   various basic operations on a single thread. The
   [setup step](../tests/unit/splinterdb_quick_test.c#:~:text=CTEST%5FSETUP\(splinterdb%5Fquick)
   shows how to specify a default configuration and then create a new instance.
- [`tests/unit/splinterdb_stress_test.c`](../tests/unit/splinterdb_stress_test.c) uses multiple threads
- [`tests/unit/splinter_test.c`](../tests/unit/splinter_test.c#:~:text=CTEST%5FSETUP\(splinter)
  covers slightly advanced steps to create a SplinterDB instance. It covers
  configuration of a SplinterDB instance, creating heap memory, allocating
  memory for various sub-system configurations, and initialization of
  individual sub-systems.
