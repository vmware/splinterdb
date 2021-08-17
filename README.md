# SplinterDB
SplinterDB is a key-value store designed for high performance on fast storage devices.

See
> Alexander Conway, Abhishek Gupta, Vijay Chidambaram, Martin Farach-Colton, Richard P. Spillane, Amy Tai, Rob Johnson:
[SplinterDB: Closing the Bandwidth Gap for NVMe Key-Value Stores](https://www.usenix.org/conference/atc20/presentation/conway). USENIX Annual Technical Conference 2020: 49-63

## Build and test workflows

### Pre-built binary with Docker
The most recent build from `main` can be tested from a local Docker container:
```shell
docker run --rm projects.registry.vmware.com/splinterdb/splinterdb
```
Or add `/bin/bash` to get a shell in the container:
```shell
docker run -it --rm projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```

### Build from source with Docker
If you want to test using local changes to the source, you can build your own image:
```shell
docker build -t splinterdb .
```
and then get a shell in that image:
```shell
docker run -it --rm splinterdb /bin/bash
```

### Build from source on Linux
Builds are known to work on Ubuntu using recent versions of GCC and Clang.

In CI, we test against the versions that Ubuntu "focal" 20.04 provides by
default, currently GCC 9 and Clang 10.

```shell
export COMPILER=gcc-9
sudo apt update -y
sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER

export CC=$COMPILER
export LD=$COMPILER
make
make test
```

## Test configuration
By default the configuration file `default.cfg` is used. This creates a `db` file in the working directory to use as back end. To modify this configuration, copy to `splinter_test.cfg` and make changes.
