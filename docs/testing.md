# Testing SplinterDB

In this section we discuss how to exercise unit, functional
and performance benchmarking tests.

_Audience: All users and contributors._


## Testing Overview
To run the full test-suite, do: `$ make run-tests` or `$make test-results`.  The former runs the tests and outputs the results to the terminal.  The `test-results` target generates a file with the test results.

The [`make run-tests`](../Makefile#:~:text=run%2Dtests) target invokes the
underlying [`test.sh`](../test.sh) script to run quick tests.

To execute a smaller set of tests, you can do the following:
```shell
$ TESTS_FUNCTION=<func> make <test-results | run-tests>
```
See `test.sh` for valid values for `TESTS_FUNCTION`.

In CI, all test execution is driven by the top-level [test.sh](../test.sh)
script, which exercises individual build artifacts produced for testing, as
described below.

### Testing Artifacts

As part of the make build output the following artifacts are produced under
the `./bin` directory:
- A `unit_test` binary, which runs a collection of quick-running unit tests
- A collection of stand-alone unit-test binaries in the `./bin/unit` directory.
- A `driver_test` binary to drive functional and performance tests

-----
The following sections describe how to execute individual testing artifacts,
for more granular test stabilization.

## Running Unit tests

To run all the unit-tests, do:  `$ ./bin/unit_test`

Some unit-tests are designed to run with multiple threads, and larger
volumes of data. These are generally named as stress tests.

You can run them standalone as follows: `$ ./bin/unit/btree_stress_test`

See [Unit testing](./unit-testing.md)
for more details on unit test development and unit testing.


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

Each functional test can be run with different parameters.
Use the following syntax to get the test-specific configuration supported.

` $ ./bin/driver_test <test-name> --help`

```shell
$ ./bin/driver_test btree_test --help
$ ./bin/driver_test splinter_test --help
```

In the `test.sh` script you can find
[examples of the command-line parameters](../test.sh#:~:text=driver%5Ftest%20splinter%5Ftest%20%2D%2Dfunctionality%201000000)
that are commonly used for testing stability.

## Running Performance Tests

SplinterDB performance tests are executed by `driver_test` using the `splinter_test`
option with the `--perf` argument.

Several different options are supported to execute different performance tests.
These can be found using:

 ```$ ./bin/driver_test splinter_test --help```

 An example usage of performance tests that are executed in our CI runs can be found
 [here in test.sh](../test.sh#:~:text=%2D%2Dperf%20%2D%2Dmax%2Dasync%2Dinflight)

