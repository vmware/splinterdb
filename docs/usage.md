# Using SplinterDB

_A rough guide for how to integrate SplinterDB into a program._

## 1. Get the SplinterDB library and headers

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
- runtime dependencies for SplinterDB, including `libaio` and `libxxhash`
- the SplinterDB static and shared libraries: `/usr/local/lib/libsplinterdb.{a,so}`
- header files for SplinterDB's public API: `/usr/local/include/splinterdb/`
- the pre-built test binary and test script: `/splinterdb/bin/driver_test` and `/splinterdb/test.sh`

> Note: this image does not include tools to build SplinterDB itself
from source.  See [our build docs](build.md) for that.

Docker-based development is beyond the scope of this doc, but consider
using bind mounts to access source code on your host OS:
```shell
$ docker run -it --rm \
    --mount type=bind,source="$PWD/my-app-src",target=/my-app-src \
    projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```


## 2. Include SplinterDB in your program

For example, a C linker would need the flag `-lsplinterdb`.  You may also need to configure include and library directories.


## 3. Call SplinterDB APIs from your program

For basic key-value store use cases, [`kvstore_basic.h`](../src/kvstore_basic.h) should suffice.

- Set the fields in `kvstore_basic_cfg`

- `kvstore_basic_create()` will create a new database and `kvstore_basic_close()` closes it.
   All access to that database must go through the single `kvstore_basic*` object returned.
   In particular, it is not safe for multiple processes to open the same disk file or device.

    > For example, a RDBMS using SplinterDB as a storage layer might consolidate all "table open" and "table close"
      operations across different client connections into a shared, per-table, reference-counted resource which
      internally calls `kvstore_basic_open()` in its constructor and `kvstore_basic_close()` in its destructor.

- Basic key/value operations like insert, delete, lookup and iteration
  (range scan) are supported.

- A range query should use the iterator workflow described in `kvstore_basic.h`.

- SplinterDB is optimized for multi-threaded use, but follow these guidelines:

  - The thread which called `kvstore_basic_create()` or `kvstore_basic_open()`
    is called the "initial thread".

  - The initial thread should be the one to call `kvstore_basic_close()` when
    the `kvstore_basic` is no longer needed.

  - Threads (other than the initial thread) that will use the `kvstore_basic`
    must be registered before use and unregistered before exiting:

    - From a non-initial thread, call `kvstore_basic_register_thread()`.
      Internally, SplinterDB will allocate scratch space for use by that thread.

    - To avoid leaking memory, a non-initial thread should call
      `kvstore_basic_deregister_thread()` before it exits.

  - Known issue: In a pinch, non-initial, registered threads may call
    `kvstore_basic_close()`, but their scratch memory would be leaked.

  - Note these rules apply to system threads, not [runtime-managed threading](https://en.wikipedia.org/wiki/Green_threads)
    available in higher-level languages.

Look at [`tests/kvstore_basic_test.c`](../tests/kvstore_basic_test.c) for example code.
