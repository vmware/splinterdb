# Testing SplinterDB

In this section we discuss how to:

* Exercise existing unit, functional, and performance benchmarking tests
* Add new tests for new source code changes

_Audience: All users and contributors._


## Testing Overview
In CI, we execute these tests against the following build modes
- All tests using clang-based optimized and debug builds
- All tests using gcc-based optimized and debug builds
- Rust-based SplinterDB CLI tool, and a performance test driven by this CLI (optimized and debug builds)
- All tests using address-sanitizer debug builds using the clang compiler
- All tests using memory-sanitizer debug builds using the gcc compiler
- clang-format checks
- [shellcheck](https://www.shellcheck.net) and shfmt checks run against shell scripts

To run a small collection of unit-tests to give you a very quick initial baseline stabiity of the product, do

```shell
$ make run-tests
```
The `make run-tests` target invokes the underlying [`test.sh`](../test.sh#:~:text=run-tests) script.

To execute the full set of tests, including functional and performance tests, you can do one of the following:

```shell
$ INCLUDE_SLOW_TESTS=true make run-tests
$ INCLUDE_SLOW_TESTS=true ./test.sh
```

In CI, all product test execution is driven by the top-level [test.sh](../test.sh) script, which invokes the build artifacts produced for testing as described below.

### Testing Artifacts

As part of the make build output the following artifacts are produced under
the `./bin` directory:
- A `unit_test` binary and a collection of stand-alone unit-test binaries are produced under the `./bin` directory. 
- A `driver_test` binary to drive functional and performance tests

-----
The following sections describe how to execute individual testing artifacts, for more granular test stabilization.

## Running Unit tests

All unit-test sources from [tests/unit](../tests/unit) directory are linked to produce the `unit_test` binary, and the collection of individual module-specific unit-tests found in the `bin/unit` directory.

### Usage

```shell
$ ./bin/unit_test --help

./bin/unit_test [ --<config options> ]* [ <suite-name> [ <test-case-name> ] ]
```

To run all the unit-tests, do:  `$ ./bin/unit_test`

To run unit-tests for a specific module, provide the suite-name.

` $ ./bin/unit_test btree`

To run a specific test-case in a test-suite, do:

` $ ./bin/unit_test btree test_leaf_hdr_search`

Some unit-tests can be executed by specifying different command-line arguments, to exercise different configurations. This functionality is of limited use, and currently meant for internal usage.

### Usage to run standlone unit tests

Each unit-test that is bundled in the ./bin/unit_test binary can also be run standalone to get faster tunraround on individual tests

```shell
<unit-test-program> [--help] | [ <test-case-name> ]
```

As an example, you can run the unit-tests for basic SplinterDB and BTree functionality as follows:

`$ ./bin/unit/splinter_test`

`$ ./bin/unit/btree_test`

Some unit-tests are designed to run with multiple threads, and larger volumes of data. These are generally named as stress tests. You can run them standalone as follows

``` $ ./bin/unit/btree_stress_test```


## Running Functional Tests

All functional tests are executed by the `driver_test` binary

```shell
$ ./bin/driver_test --help
Dispatch test --help
invalid test
List of tests:
	btree_test
	filter_test
	splinter_test
	log_test
	cache_test
	ycsb_test
```

Each functional test can be run with different parameters. Use the following syntax to get test-specific configuration supported.

` $ ./bin/driver_test <test-name> --help`

```shell
$ ./bin/driver_test btree_test --help
$ ./bin/driver_test splinter_test --help
```

You can find examples of the configuration that are commonly used for testing product stability in the [test.sh](../test.sh#:~:splinter_test) driver script. 

## Running Performance Tests

SplinterDB performance tests are executed using the `--perf` argument to the `driver_test` program using the `splinter_test` option. 

Several different options are supported to execute different performance tests. These can be found using:

 ```$ ./bin/driver_test splinter_test --help```

 An example usage of performance tests that are executed in our CI runs can be found [here in test.sh](../test.sh#:~:--max-async-inflight)

## Using the splinterdb-cli to run Performance Tests

The Rust-based `splinterdb-cli` tool can be used to drive one kind of performance benchmarking tests to get basic insert throughput metrics for your hardware configuration, playing around with a few parameters such as the number of threads performing concurrent inserts, number of writes/thread

### Usage

Basich help / usage information:

`$ ./rust/target/release/splinterdb-cli --help`

To get sub-command specific help / usage:

`$ ./rust/target/release/splinterdb-cli <sub-command> --help`

Example:

```shell
$ ./rust/target/release/splinterdb-cli perf --help

## Run a performance benchmark with default parameters
$ ./rust/target/release/splinterdb-cli perf

## Test your performance with varying parameters
$ ./rust/target/release/splinterdb-cli --file /tmp/sdb.perf.db perf --disk-mb 1000 --cache-mb 400 --threads 20 -w 200000
```