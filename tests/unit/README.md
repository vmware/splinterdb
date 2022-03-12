# SplinterDB Unit Testing

We have adapted the [CTest framework](https://github.com/bvdberg/ctest),
which is a unit testing framework for software written in C/C++,

See this [README.md](https://github.com/bvdberg/ctest/blob/master/README.md)
file for a fuller description of the features of this CTest framework.

---
## Developing using CTest

- All unit-test code lives under the `tests/unit/` sub-directory
- Naming Conventions:

  - For a source file named `<file_name>.c`, the unit-test file is named `<file_name>_test.c`
  - The test suite name used for test cases in each test file is usually the `<file_name>` prefix
    - Each test case is named `test_<something>`
- To write a new test suite, copy the
  [unit_test_template_dot_c](unit_test_template_dot_c) file to your new test suite.
- Follow the instruction given in the [splinterdb_quick_test.c](./splinterdb_quick_test.c)
  to build individual test cases using the testing framework.
- If your new unit test is small and runs quickly (under a few seconds), consider
  adding it here to the list of "smoke tests" in
  [test.sh](../../test.sh#:~:text=Only%20running%20fast%20unit%20tests)

----
## How To build unit tests

Build directives are defined in the top-level `Makefile`. After the initial [build setup](../../docs/build.md), build unit tests as follows.

Build the SplinterDB library and all test code: `$ make clean && make`

The unit test driver program `bin/unit_test` and all standalone unit test binaries will be generated, including other test drivers.

Rebuild all the test binaries:  `$ make  tests`

Rebuild just the unit test binaries: `$ make unit_test`

To build debug versions of tests: `$ make debug tests` or  `$ make debug unit_test`

----
## Basic help / usage

To get **basic help / usage** information: `$ bin/unit_test --help`

```shell
$ ./bin/unit_test --help
Usage: ./bin/unit_test [--list [ <suite-name> ] ]
Usage: ./bin/unit_test [ --<config options> ]* [ <suite-name> [ <test-case-name> ] ]
```

> Note: The `[ --<config options> ]*` option allows specifying different configuration
  parameters for test execution. This is supported by very few unit tests
  and is **only** meant for internal developer-use.


To **list all the unit-test suites** that can be run: `$ bin/unit_test --list`

To list all the test cases from a given test suite: `$ bin/unit_test --list <suite-name>`

**Example**: List all test cases from the [`splinterdb_quick_test`](splinterdb_quick_test.c) test suite.

`$ bin/unit_test --list splinterdb_quick`

You can also list the test cases that can be run from an individual standalone
unit-test binary with the `--list` argument.

**Example**: List all the test cases from the [`btree`](btree_test.c) test suite using the standalone binary

`$ bin/unit/btree_test --list`


----
## How To Run unit-tests

Quick **smoke-tests** run of unit tests: `$ make run-tests`

This runs a small subset of unit tests, which execute very quickly, so that you
get a quick turnaround on the overall stability of your changes.

You can achieve the same result by executing the following test driver script
[test.sh](../../test.sh#:~:text=if%20\[%20"$INCLUDE%5FSLOW%5FTESTS"%20!=%20"true"%20\]): `$ ./test.sh`

To **run** all the unit-test suites: `$ bin/unit_test`

To run a specific suite, optionally filtering on a specific test case:

`$ bin/unit_test <suite-name> [ <test-case-name> ]`

**Example**: Run all test cases named `test_leaf_hdr*` from the `btree_test` suite

`$ bin/unit_test btree test_leaf_hdr`

To run all test cases from a specific unit-test binary:[ `$ bin/unit/splinter_test`](splinter_test.c)

To run all test cases named with a prefix from a specific unit-test binary:

`$ bin/unit/<binary-name> <test-case-prefix>`

**Example**: Run all SplinterDB iterator-related test cases from the [`splinterdb_quick_test`](./splinterdb_quick_test.c#:~:text=%20test%5Fsplinterdb%5Fiterator):

`$ bin/unit/splinterdb_quick_test test_splinterdb_iterator`
