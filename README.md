# SplinterDB
SplinterDB is a key-value store designed for high performance on fast storage devices.

## Build and test

### Option 1: On Linux
Install dependencies
```
sudo apt update
sudo apt install -y libaio-dev libconfig-dev libxxhash-dev
```

Build
```
export CC=clang-8
export LD=clang-8
make
```

The test binary needs [CAP_IPC_LOCK](https://man7.org/linux/man-pages/man7/capabilities.7.html), but you can set it once
```
sudo make setcap
```

so that you can run the tests without `sudo`
```
make test
```

### Option 2: With Docker
Build
```
docker build -t splinterdb .
```

Run tests
```
docker run --rm --cap-add=IPC_LOCK splinterdb
```

Or to get a shell in the container where you can interactively run tests
```
docker run --rm -it --cap-add=IPC_LOCK splinterdb /bin/bash
```

## Test configuration
By default the configuration file `default.cfg` is used. This creates a `db` file in the working directory to use as back end. To modify this configuration, copy to `splinter_test.cfg` and make changes.
