# Using SplinterDB

SplinterDB is an embedded key-value store.  To use it, you must link your
program to it and call the C functions declared in the
[`include/splinterdb`](../include/splinterdb) headers.

The headers and their code comments are the definitive API reference.
This document is a high-level overview.  Some details here may be out of date.


## Program flow

- Choose values for `splinterdb_config`.  For the `data_cfg` field, simple
  applications may opt to use the `default_data_config`, provides a basic
  key / value interface with lexicographical sorting of keys.

- Call `splinterdb_create()` to create a new database file or block device,
  `splinterdb_open()` to open an already-created one, and
  `splinterdb_close()` to close the file or device.

   All access to the database must go through the single `splinterdb*` object
   returned.  It is not safe for more than one process to open the same
   SplinterDB disk file or device.

- Basic key/value operations like `insert`, `delete`, point `lookup` and
  range scan using an `iterator` are available.  See the code comments
  in `splinterdb.h`.

- If an application frequently makes small modifications to existing values,
  better performance may be possible by using a custom `data_config` object
  and the `splinterdb_insert_raw_message` function.
  A raw message may encode a "blind update" to a value, for example an
  "append" or "increment" operation that may be persisted without doing a read.
  Some applications may be able to avoid a read-modify-write sequence this way.
  To use the message-oriented API, the application implements the `data_config`
  interface defined in [`data.h`](../include/splinterdb/data.h) and sets it on
  `splinterdb_config` when creating/opening a database.

- SplinterDB is designed to deliver high-performance for multi-threaded
  applications, but follow these guidelines:

  - The thread which called `splinterdb_create()` or `splinterdb_open()`
    is called the "initial thread".

  - The initial thread should be the one to call `splinterdb_close()` when
    the `splinterdb` is no longer needed.

  - Threads (other than the initial thread) that will use the `splinterdb`
    must be registered before use and unregistered before exiting:

    - From a non-initial thread, call `splinterdb_register_thread()`.
      Internally, SplinterDB will allocate scratch space for use by that thread.

    - To avoid leaking memory, a non-initial thread should call
      `splinterdb_deregister_thread()` before it exits.

  - Known issue: In a pinch, non-initial, registered threads may call
    `splinterdb_close()`, but their scratch memory would be leaked.

  - Note these rules apply to system threads, not [runtime-managed threading](https://en.wikipedia.org/wiki/Green_threads)
    available in higher-level languages.

- Internally SplinterDB supports asynchronous IO, but this capability is not
  yet documented.  Some example code for this may be found in the unit and functional tests.


## Example programs
- [`tests/unit/splinterdb_quick_test.c`](../tests/unit/splinterdb_quick_test.c) covers various basic operations on a single thread
- [`tests/unit/splinterdb_stress_test.c`](../tests/unit/splinterdb_stress_test.c) uses multiple threads
- [`splinterdb-cli`](../rust/splinterdb-cli) is a small Rust program that demonstrates inserts, lookups, deletes and
range scans, and includes a multithreaded test of insert performance.


## Compiling and linking
SplinterDB is a C library, not a standalone program.  To use it, you'll need to link your program against it at build time or at run time.  Our [build process](build.md) produces `libsplinterdb.a` and `libsplinterdb.so` libraries, and headers for the public API are in [`include/splinterdb/`](../include/splinterdb).

SplinterDB is currently Linux-only.

## Optional: Docker-based development
Some application developers may prefer to use a [Docker-based workflow](docker.md), which can leverage containers built by our continuous integration system.
