# `splinterdb-cli`

A simple command line utility for SplinterDB.

It also serves as an example of how to build an application using the Rust wrapper for SplinterDB.

For build instructions, see the [README in the parent directory](../README.md).

For usage, run `target/debug/splinterdb-cli --help`:

## Walkthrough
Initialize a new database file on disk
```
$ target/debug/splinterdb-cli --file /tmp/my-db init-db --disk-mb 2000 --key-size 20
```
Note this creates both the named `/tmp/my-db` file and an extra metadata
file `/tmp/my-db.meta`.  Both files must be present for the other
commands to work.

List contents (currently empty)
```
$ target/debug/splinterdb-cli --file /tmp/my-db list
```

Add some data
```
$ target/debug/splinterdb-cli --file /tmp/my-db insert -k "key1" -v "value1"
$ target/debug/splinterdb-cli --file /tmp/my-db insert -k "key2" -v "value2"
$ target/debug/splinterdb-cli --file /tmp/my-db insert -k "key3" -v "value3"
```

List contents again
```
$ target/debug/splinterdb-cli --file /tmp/my-db list
```

Delete a key/value pair
```
$ target/debug/splinterdb-cli --file /tmp/my-db delete --key "key2"
```

Lookup a single value
```
$ target/debug/splinterdb-cli --file /tmp/my-db get -k "key1"
```

## Performance testing
The same tool may be used for testing the performance of SplinterDB.

This will overwrite the chosen file or block device with random data, and print results at the end
```
$ target/debug/splinterdb-cli --file /tmp/test perf --threads 8 --writes-per-thread 50000
```
