# HOW TO BUILD

This branch is an experimental one for transactional systems. So this is a
little bit different from the main.

### Init/Update submodule

This implementation utilizes the iceberg hashtable as a submodule. To initialize
the submodule, 

```sh
git submodule update --init --recursive
```

### Compile the iceberg hashtable

To compile the iceberg hashtable, move to `third-party/iceberghashtable` and run
`make`.

### Set up environmental variables

To inform the iceberg hash table to SplinterDB, we must set environmental
veriables such as `LD_LIBRARY_PATH`. Run `source set-env.sh`, which is in the
project root.

### Build SplinterDB

You can finally build SplinterDB by running `make`.