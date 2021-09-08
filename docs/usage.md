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

The container includes the SplinterDB runtime dependencies, shared library,
test binary, and the header files for SplinterDB's public API.

```shell
docker$ find /splinterdb
/splinterdb
/splinterdb/bin
/splinterdb/bin/driver_test
/splinterdb/bin/splinterdb.so
/splinterdb/include
/splinterdb/include/platform_public.h
/splinterdb/include/data.h
/splinterdb/include/kvstore.h
/splinterdb/test.sh

docker$ ldd /splinterdb/bin/splinterdb.so
   linux-vdso.so.1 (0x00007ffc57fa4000)
   libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fefbd0dd000)
   libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007fefbd0ba000)
   libaio.so.1 => /lib/x86_64-linux-gnu/libaio.so.1 (0x00007fefbd0b5000)
   libxxhash.so.0 => /lib/x86_64-linux-gnu/libxxhash.so.0 (0x00007fefbd0ab000)
   libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fefbceb9000)
   /lib64/ld-linux-x86-64.so.2 (0x00007fefbd2a9000)
```

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

- `kvstore_basic_init()` will open a database and `kvstore_basic_deinit()` closes it.
   Concurrent access to that database must go through the `kvstore_basic*` object
   returned by `kvstore_basic_init()`.

    > For example, a RDBMS using SplinterDB as a storage layer might consolidate all "table open" and "table close" operations across different client connections into a shared, per-table, reference-counted resource which internally calls `kvstore_basic_init()` in its constructor and `kvstore_basic_deinit()` in its destructor.

- `kvstore_basic_register_thread()` must be called once for each thread that needs to use the database.  Note this may be non-trivial for languages with [runtime-managed threading](https://en.wikipedia.org/wiki/Green_threads).

- Once registered, a thread may insert, delete, lookup and iterate.

- A range query should use the iterator workflow described in `kvstore_basic.h`.

A complete (single-threaded) example is in [`tests/kvstore_basic_test.c`](../tests/kvstore_basic_test.c).