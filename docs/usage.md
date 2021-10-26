# Using SplinterDB

_A quick-start guide for how to integrate SplinterDB into a program._

(Or just skip to the [Example programs](#example-programs) below.)

## 1. Get the SplinterDB library and headers

## Table of contents
1. [Get the SplinterDB library and headers  <a name="SDB_libs_and_headers"></a>](#get-the-splinterdb-library-and-headers--)
    - [Option 1: from source](#option-1-from-source)
    - [Option 2: using Docker](#option-2-using-docker)
 2. [Include SplinterDB in your program](#include-splinterdb-in-your-program)
 3. [Call SplinterDB APIs from your program](#call-splinterdb-apis-from-your-program)
 4. [SplinterDB CLI](#splinterdb-cli)

------

## Get the SplinterDB library and headers

### Option 1: from source
Follow instructions to [build SplinterDB from source](build.md).

### Option 2: using Docker
Our Continuous Integration system publishes [Docker images](../Dockerfile)
that are sufficient for linking SplinterDB into another program.

Example usage:
```shell
$ docker run -it --rm projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```

The container includes:
- Runtime dependencies for SplinterDB, including `libaio` and `libxxhash`
- The SplinterDB static and shared libraries: `/usr/local/lib/libsplinterdb.{a,so}`
- Header files for SplinterDB's public API: `/usr/local/include/splinterdb/`
- The pre-built test binary and test script: `/splinterdb/bin/driver_test` and `/splinterdb/test.sh`

> Note: this image does not include the tools to build SplinterDB itself
from source.  See [our build docs](build.md) for those instructions.

Docker-based development is beyond the scope of this doc, but consider
using bind mounts to access source code on your host OS:
```shell
$ docker run -it --rm \
    --mount type=bind,source="$PWD/my-app-src",target=/my-app-src \
    projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```


## Include SplinterDB in your program

For example, a C linker would need the flag `-lsplinterdb`.  You may also need to configure include and library directories.


## Call SplinterDB APIs from your program

For basic key-value store use cases, [`splinterdb_kv.h`](../include/splinterdb/splinterdb_kv.h) should suffice.

- Set the fields in `splinterdb_kv_cfg`

- `splinterdb_kv_create()` will create a new database and `splinterdb_kv_close()` closes it.
   All access to that database must go through the single `splinterdb_kv*` object returned.
   In particular, it is not safe for multiple processes to open the same disk file or device.

    > For example, a RDBMS using SplinterDB as a storage layer might consolidate all "table open" and "table close"
      operations across different client connections into a shared, per-table, reference-counted resource which
      internally calls `splinterdb_kv_open()` in its constructor and `splinterdb_kv_close()` in its destructor.

- Basic key/value operations like insert, delete, lookup and iteration
  (range scan) are supported.

- A range query should use the iterator workflow described in `splinterdb_kv.h`.

- SplinterDB is optimized for multi-threaded use, but follow these guidelines:

  - The thread which called `splinterdb_kv_create()` or `splinterdb_kv_open()`
    is called the "initial thread".

  - The initial thread should be the one to call `splinterdb_kv_close()` when
    the `splinterdb_kv` is no longer needed.

  - Threads (other than the initial thread) that will use the `splinterdb_kv`
    must be registered before use and unregistered before exiting:

    - From a non-initial thread, call `splinterdb_kv_register_thread()`.
      Internally, SplinterDB will allocate scratch space for use by that thread.

    - To avoid leaking memory, a non-initial thread should call
      `splinterdb_kv_deregister_thread()` before it exits.

  - Known issue: In a pinch, non-initial, registered threads may call
    `splinterdb_kv_close()`, but their scratch memory would be leaked.

  - Note these rules apply to system threads, not [runtime-managed threading](https://en.wikipedia.org/wiki/Green_threads)
    available in higher-level languages.

## Example programs
Our C unit tests serve as examples for how to call SplinterDB APIs.  For example
- [`tests/unit/splinterdb_kv_test.c`](../tests/unit/splinterdb_kv_test.c) covers various basic operations on a single thread
- [`tests/unit/splinterdb_kv_stress_test.c`](../tests/unit/splinterdb_kv_stress_test.c) uses multiple threads

In addition, [`splinterdb-cli`](../rust/splinterdb-cli) is a small Rust program that may serve as an example for basic usage, including threading.

