# Building from source
This document is about how to build and test SplinterDB itself.

To integrate SplinterDB into another application, see [Usage](usage.md).

## On Linux
Builds are known to work on Ubuntu using recent versions of GCC and Clang.

### Tooling
In CI, we test against GCC 13 and Clang 16.

We use `clang-format-16` for code formatting.

### Full build
Here are the steps to do a full-build of the library, run smoke tests, and to install the shared libraries:

```shell
$ export COMPILER=gcc    # or clang
$ sudo apt update -y
$ sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER
$ export CC=$COMPILER
$ export LD=$COMPILER
$ make
$ make run-tests
$ sudo make install
```

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
