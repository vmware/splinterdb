# Building from source

This document is about how to build and test SplinterDB itself.

To integrate SplinterDB into another application, see [Usage](usage.md).

## On Linux
Builds are known to work on Ubuntu using recent versions of GCC and Clang.

### Tooling
In CI, we test against GCC 9 and Clang 13.

We use `clang-format-13` for code formatting.

To install `clang-13` tools on Ubuntu Linux, do this:

```shell
$ wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -

$ sudo add-apt-repository 'deb http://apt.llvm.org/focal/ llvm-toolchain-focal-13 main'

$ sudo apt-get install -y clang-13 clang-format-13
```

### Full build
Here are the steps to do a full-build of the library, run smoke tests, and to install the shared libraries:

```shell
$ export COMPILER=gcc    # or clang-13
$ sudo apt update -y
$ sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER
$ export CC=$COMPILER
$ export LD=$COMPILER
$ make [debug]
$ make run-tests
$ sudo make install
```
Debug builds are done using the `$ make debug` command.

### Build Artifacts

The following artifacts are produced by a successful build:
- In the `./lib` dir, shared libraries: `libsplinterdb.so` and `libsplinterdb.a`
- In the `./bin` dir, driver programs used to test SplinterDB:
     - `driver_test` - Binary to drive various functional and performance tests
     - `unit_test` - Binary to drive a collection of unit-tests
     - In the `./bin/unit` dir, a collection of stand-alone unit-test binaries for different modules, all of which are linked in the unit_test binary.
 - A stand-alone `splinterdb-cli` tool, developed in Rust

### Sanitizer Builds

In CI, we also run memory-sanitizer and address-sanitizer builds and run
existing tests against these builds. You can specify build-time flags to
run these sanitizer builds, as mentioned in the [Makefile](../Makefile)


```shell
## To run address-sanitizer builds
$ make clean
$ DEFAULT_CFLAGS="-fsanitize=address" DEFAULT_LDFLAGS="-fsanitize=address" make

## To run memory-sanitizer builds
$ make clean
$ DEFAULT_CFLAGS="-fsanitize=memory" DEFAULT_LDFLAGS="-fsanitize=memory" make
```

> Note
- Currently, unit-tests can only be executed with address-sanitizer builds done with the clang compiler.
- Other tests can be executed with address-sanitizer builds done using gcc.
- Memory-sanitizer builds are only supported with the clang compiler.

Additional options controlling builds can be found by running
```shell
$ make help
```

### Code formatting
Our CI system enforces code formatting rules.  If you're making code changes
that you wish to [contribute back to the project](../CONTRIBUTING.md), please
format the code using the [format-check.sh](../format-check.sh) script:

```shell
$ format-check.sh fixall
```
Files will be edited in place.

## On other platforms
Currently SplinterDB only works on Linux.  Some SplinterDB developers work in a Linux virtual machine.  You may also have success with a [Docker-based workflow](docker.md).
