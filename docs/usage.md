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


## Passing values and reusable blobs

Both `splinterdb_insert()` and `splinterdb_update()` accept a public `message`.
Use `message_create(length, pointer)` for direct bytes, or
`message_from_slice(slice)` for an existing slice. These constructors borrow
the input without allocation or copying. Keys and lookup results still use
slices. Existing callers can replace value-side `slice_create()` calls with
`message_create()`.

The called operation determines the message type: insert always overwrites,
and update always applies a delta through the configured merge callbacks.
Neither modifies the caller's message. Its `type` field is ignored on input,
so the same message can be passed to either operation.

Use `splinterdb_blob_create()` to copy bytes into blob storage once, then pass
the returned message to either operation. Full extents are shared; tails may
be copied on insertion or compaction. Small values may be copied entirely,
and each created blob has allocation overhead, so this API is most useful
for large repeated payloads.

```c
int
insert_shared_values(splinterdb *db, slice bytes, slice first_key, slice second_key)
{
   message value;
   int rc = splinterdb_blob_create(db, bytes, &value);
   if (rc != 0) {
      return rc;
   }

   rc = splinterdb_insert(db, first_key, value, NULL);
   if (rc == 0) {
      rc = splinterdb_insert(db, second_key, value, NULL);
   }
   splinterdb_blob_release(&value);
   if (rc == 0) {
      rc = splinterdb_durable_barrier(db, NULL);
   }
   return rc;
}
```

A reusable delta is passed as `splinterdb_update(db, key, value, NULL)`.
It is processed by the configured merge callbacks just like a direct delta;
the resulting value need not retain the delta's blob storage.

The input bytes may be released after blob creation succeeds. Releasing the
message drops the caller's ownership and resets that message to an empty
direct message; successfully inserted values or deltas retain their own
ownership. Operations are independent: if a second operation fails, a first
successful operation remains applied.

Copying a message only borrows the same payload. Release each created blob
exactly once, after all uses of all copies finish; release invalidates the
other copies. Callback messages are borrowed and must not be released.
Do not change a blob message's `cc`, `data`, or descriptor bytes.
For a blob, `message_data()`, `message_length()`, and `message_slice()`
describe its encoded descriptor, not its logical bytes.

A blob belongs to the open database instance that created it. Threads may
reuse it concurrently, but release must not race any use. Blob messages cannot
be serialized or reused after close/reopen. Release all created blobs before
`splinterdb_close(&db, FALSE)`, which returns `EBUSY` while they remain live.
Forced close releases them and invalidates every outstanding copy.

Creating a standalone blob does not establish crash durability. Inserted
references follow ordinary update and durability-barrier semantics. Recovery
reclaims storage that no recovered data references. Keys retain the existing
size limit; large-key support is separate.

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
