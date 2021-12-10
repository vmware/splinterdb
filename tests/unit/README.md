# SplinterDB Unit-Testing

We have adapted the [CTest framework](https://github.com/bvdberg/ctest),
which is a unit testing framework for software written in C/C++,

See this [README.md](https://github.com/bvdberg/ctest/blob/master/README.md) file for a
fuller description of the features of this CTest framework.

---
## Developing using CTest

* All unit-test code lives under the test/unit/ sub-directory
* Naming Conventions:

    * For a source file named `<fileName>.c`, the unit-test file is named `<fileName>_test.c`
    * The test suite name used for test cases in each test file is usually the `<fileName>` prefix
    * Each test case is named `test_<something>`



* How To Build unit-tests:

    * Build directives are defined in the top-level Makefile. Build as follows:

```shell
$ cd /splinterdb

# Build the SplinterDB library first. 'make' will rebuild bin/unit_test also.
$ make clean; make [debug]

# Build the unit-test binary
$ make bin/unit_test
```

* How To Run unit-tests:

```shell

# Get help/usage information
$ bin/unit_test --help

# Run all the unit test suites
$ bin/unit_test

# Run a specific suite, optionally filtering on a specific test case
$ bin/unit_test suite-name [ <test-case-name> ]
```
