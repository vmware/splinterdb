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
