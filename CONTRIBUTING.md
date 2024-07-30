# Contributing

The SplinterDB project welcomes contributions from the community.

This page presents guidelines for contributing to SplinterDB. Following the guidelines helps to make the contribution process easy, collaborative, and productive.

## Submitting Bug Reports and Feature Requests
Please search through our [GitHub Issues](https://github.com/vmware/splinterdb/issues) for existing issues related to your need.  If something new is appropriate:
- For bug reports, be as specific as possible about the error and the conditions under which it occurred.
- For feature requests, please provide as much context as possible about your use case.


## Contributing code or documentation
For small obvious fixes, follow the steps below.  For larger changes, please first [open an Issue](https://github.com/vmware/splinterdb/issues/new) that describes your need ([*talk, then code*](https://dave.cheney.net/2019/02/18/talk-then-code)).

1. Follow [build](docs/build.md) for instructions on building from source
and follow [testing](docs/testing.md) for instructions on testing your changes.

2. Before submitting code, please ensure that:
    - Code builds: `$ make`
    - [All Tests pass](./Makefile#:~:text=test%2Dresults): `$ make test-results`
    - Code is formatted properly: `$ ./format-check.sh fixall`

        To get an automatic format check on every git commit, add a pre-commit hook:
        ```
        ln -s -f ../../format-check.sh .git/hooks/pre-commit
        ```

3. Then open a [Pull Request](https://github.com/vmware/splinterdb/pulls).  You'll need to wait for a project maintainer to add the `ok-to-test` label to your PR.  Then our continuous integration (CI) system will run checks, which can take a couple hours.  Also, if you have not signed our Contributor License Agreement (CLA), a "CLA-bot" will take you through the process and update the issue.  If you have questions about the CLA process, see our CLA [FAQ](https://cla.vmware.com/faq) or just ask for help on your pull request.
