# SplinterDB
SplinterDB is a key-value store designed for high performance on fast storage devices.

See
> Alexander Conway, Abhishek Gupta, Vijay Chidambaram, Martin Farach-Colton, Richard P. Spillane, Amy Tai, Rob Johnson:
[SplinterDB: Closing the Bandwidth Gap for NVMe Key-Value Stores](https://www.usenix.org/conference/atc20/presentation/conway). USENIX Annual Technical Conference 2020: 49-63

## Build and test workflows

### Test a pre-built library with Docker
Our Continuous Integration (CI) system builds the latest commit on `main`
into a container image suitable for running tests and integrating SplinterDB
into other applications.  To run tests:
```shell
$ docker run --rm projects.registry.vmware.com/splinterdb/splinterdb
```
Or add `/bin/bash` to get a shell in the container:
```shell
$ docker run -it --rm projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```

The `projects.registry.vmware.com/splinterdb/splinterdb` image contains runtime
dependencies, the built shared library, test binary, and header files for the
public API.  It does not include tools to build SplinterDB itself from source.
See [`Dockerfile`](Dockerfile) for details.


### Build from source with Docker
To build and test from source, change into the
directory containing this repository and then use our
`projects.registry.vmware.com/splinterdb/build-env` image:
```shell
$ docker run -it --rm --mount type=bind,source="$PWD",target=/splinterdb \
     projects.registry.vmware.com/splinterdb/build-env /bin/bash
```

Inside that shell, you can build and test with either GCC or Clang:
```shell
docker$ cd /splinterdb
docker$ export CC=clang  # or gcc
docker$ export LD=clang
docker$ make
docker$ make test
```

Unlike the `splinterdb/splinterdb` image, the `splinterdb/build-env` image
contains everything needed to build from source with either GCC or Clang.
See [`Dockerfile.build-env`](Dockerfile.build-env) for details.

### Build from source on Linux
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

## Test configuration
By default the configuration file `default.cfg` is used. This creates a `db` file in the working directory to use as back end. To modify this configuration, copy to `splinter_test.cfg` and make changes.
