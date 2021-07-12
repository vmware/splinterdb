SplinterDB
==========

SplinterDB is a key-value store designed for high performance on fast storage devices.

Installation
============
To compile this repository, you need `gawk`, `libaio`, and 'libconfig' dev headers installed.
You can do this with
```
  sudo apt update
  sudo apt install gawk libaio-dev libconfig-dev
```

Access the submodules (xxhash)
```
  git submodule init
  git submodule update
```

Then, to compile:
```
  make
```

Configuration
=============
By default the configuration file default.cfg is used. This creates a db file in the working directory to use as back end. To modify this configuration, copy to "splinter_test.cfg" and make changes.
