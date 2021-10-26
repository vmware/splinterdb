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
$ make
$ make run-tests
$ sudo make install
```

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
