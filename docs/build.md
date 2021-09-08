# Building from source
This document is about how to build and test SplinterDB itself.
To integrate SplinterDB into another application, see [Usage](usage.md).

## On Linux
Builds are known to work on Ubuntu using recent versions of GCC and Clang.

In CI, we test against the versions that Ubuntu "focal" 20.04 provides by
default, currently GCC 9 and Clang 10.

```shell
$ export COMPILER=gcc-9
$ sudo apt update -y
$ sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER
$ export CC=$COMPILER
$ export LD=$COMPILER
$ make
$ make test
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
```