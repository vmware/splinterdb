# Building from source
This document describes how to build the sources and test SplinterDB itself.
The bulk of the SplinterDB library code is written in C, with some supporting code implemented
in Rust.

To integrate SplinterDB into another application, see [Usage](usage.md).

## On Linux
Builds are known to work on Ubuntu using recent versions of GCC and Clang.

In CI, we test against the versions that Ubuntu "jammy" 22.04 provides by
default, currently GCC 11 and Clang 13.

```shell
$ export COMPILER=gcc-11
$ sudo apt update -y
$ sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER
$ export CC=$COMPILER
$ export LD=$COMPILER
$ make [debug]
$ make run-tests
$ sudo make install
```

### Build Artifacts

Resulting from a build are the following artifacts
- In the ./lib dir, shared libraries: libsplinterdb.so and libsplinterdb.a
- In the ./bin dir, driver programs used to test SplinterDB:
     - driver_test - Binary to drive various tests
     - unit_test - Binary to drive a collection of unit-tests
     - In the ./bin/unit/ dir, a collection of stand-alone unit-test binaries for different modules, all of which are linked in the unit_test binary.
 - A stand-alone splinterdb-cli tool, developed in Rust

### Sanitizer Builds

In CI, we also run memory-sanitizer and address-sanitizer builds and run
existing tests against these builds. You can specify build-time flags to
run these sanitizer builds, as mentioned in the [Makefile](../Makefile)


```shell
# To run address-sanitizer builds
$ make clean
$ DEFAULT_CFLAGS="-fsanitize=address" DEFAULT_LDFLAGS="-fsanitize=address" make

# To run memory-sanitizer builds
$ make clean
$ DEFAULT_CFLAGS="-fsanitize=memory" DEFAULT_LDFLAGS="-fsanitize=memory" make
```

Note(s):
- Currently, unit-tests can only be executed with address-sanitizer builds done with the clang compiler.
- Other tests can be executed with address-sanitizer builds done using gcc.
- Memory-sanitizer builds are only supported with the clang compiler.

### Using Docker
Docker can be used to build from source on many platforms, including Mac and Windows.

Change into the directory containing this repository and then do
```shell
$ docker run -it --rm --mount type=bind,source="$PWD",target=/splinterdb \
     projects.registry.vmware.com/splinterdb/build-env /bin/bash
```

> Note: the `build-env` image contains a working build environment, but does not
contain the SplinterDB source code.  That must be mounted into the running
container, e.g. the `--mount` command shown above.

Inside the container is a Linux environment with
[all dependencies for building and testing](../Dockerfile.build-env)
with either GCC or Clang.

For example, from inside the running container:
```shell
docker$ cd /splinterdb
docker$ export CC=clang  # or gcc
docker$ export LD=clang
docker$ make
docker$ make test
docker$ make install
```
