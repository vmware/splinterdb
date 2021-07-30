# SplinterDB
SplinterDB is a key-value store designed for high performance on fast storage devices.

## Build and test workflows

### Pre-built binary with Docker
The most recent build from `main` can be tested from a local Docker container:
```shell
docker run --rm --cap-add=IPC_LOCK projects.registry.vmware.com/splinterdb/splinterdb
```
Or add `/bin/bash` to get a shell in the container:
```shell
docker run -it --rm --cap-add=IPC_LOCK projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```

### Build from source with Docker
If you want to test using local changes to the source, you can build your own image:
```shell
docker build -t splinterdb .
```
and then get a shell in that image:
```shell
docker run -it --rm --cap-add=IPC_LOCK splinterdb /bin/bash
```

### Build from source on Linux
Builds are known to work on Ubuntu using recent versions of GCC and Clang.
We test against `gcc-9` and `clang-12` in CI.

```shell
export COMPILER=gcc-9
sudo apt update -y
sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER

export CC=$COMPILER
export LD=$COMPILER
make
```

The test binary needs [CAP_IPC_LOCK](https://man7.org/linux/man-pages/man7/capabilities.7.html), but you can set it once
```shell
sudo make setcap
```

so that you can run the tests without `sudo`
```shell
make test
```


## Test configuration
By default the configuration file `default.cfg` is used. This creates a `db` file in the working directory to use as back end. To modify this configuration, copy to `splinter_test.cfg` and make changes.
