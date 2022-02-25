# Building from source
This document is about how to build and test SplinterDB itself.
To integrate SplinterDB into another application, see [Usage](usage.md).

## On Linux
Builds are known to work on Ubuntu using recent versions of GCC and Clang.

In CI, we test against the default version of gcc that Ubuntu "focal" 20.04 supports,
currently GCC 9.3.
We use Clang 13 as the code base is formatted using clang-format-13 rules.

Here are the steps we used to install `clang-13` tools on Linux.

```shell
$ wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -

$ sudo add-apt-repository 'deb http://apt.llvm.org/focal/ llvm-toolchain-focal-13 main'

$ sudo apt-get install -y clang-13 clang-format-13
```

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

> Note: In CI, we also run a job to check source code formatting.

Before committing your changes, run the [format-check.sh](../format-check.sh#:~:text=clang\-format\-13)
script to verify that your code changes conform to required formatting rules.

```shell
$ format-check.sh fixall
```

Check for any files that need re-formatting, which will be edited in-place.

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
docker$ export CC=clang-13  # or gcc
docker$ export CC=$COMPILER
docker$ export LD=$COMPILER
docker$ make
docker$ make test
docker$ make install
```
