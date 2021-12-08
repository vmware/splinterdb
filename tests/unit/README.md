# SplinterDB Unit-Testing

We have adopted CTest, which is a unit testing framework for software written in C/C++,
to develop unit tests for SplinterDB. 

The [CTest framework](https://github.com/bvdberg/ctest) has been slightly modified and
adapted to work with the SplinterDB code base.

See this [README.md](https://github.com/bvdberg/ctest/blob/master/README.md) file for a
fuller description of the features of this CTest framework.

---
## Developing using CTest

* All unit-test code lives under the test/unit/ sub-directory
* Naming Conventions:

    * For a source file named \<fileName\>.c, the unit-test file is named \<fileName\>_test.c
    * The test suite name used for test cases in each test file is usually the \<fileName\> prefix
    * Each test case is named test_\<something\>



* How To Build unit-tests:

    * Build directives are defined in the top-level Makefile. Build as follows:

```shell
$ cd /splinterdb

# Build the SplinterDB library first
$ make clean; make [debug]

# Build the unit-test binary
$ make bin/ctests
```

* How To Run unit-tests:

```shell

# Get help/usage information
$ bin/ctest --help

# Run all the test suites
$ bin/ctests

# Run a specific suite, optionally filtering on a specific test case
$ bin/ctests suite-name [ <test-case-name> ]
```
