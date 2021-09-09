# Contributing

The SplinterDB project team welcomes contributions from the community.

If you wish to contribute code and you have not signed our Contributor License Agreement (CLA), our CLA-bot will take you through the process and update the issue when you open a [Pull Request](https://help.github.com/articles/creating-a-pull-request). If you have questions about the CLA process, see our CLA [FAQ](https://cla.vmware.com/faq) or contact us through the GitHub issue tracker.

This page presents guidelines for contributing to SplinterDB. Following the guidelines helps to make the contribution process easy, collaborative, and productive. 

## Submitting Bug Reports and Feature Requests

Please submit bug reports and feature requests by using our GitHub Issues page.

Before you submit a bug report about the code in the repository, please check the Issues page to see whether someone has already reported the problem. In the bug report, be as specific as possible about the error and the conditions under which it occurred. On what version and build did it occur? What are the steps to reproduce the bug? 

Feature requests should fall within the scope of the project.

## Pull Requests
See [docs/build.md](docs/build.md) for instructions on building from source.

Before submitting a pull request, please ensure that:
- code builds: `make`
- tests pass: `make test`
- new code is formatted properly: `./format-check.sh`

To get an automatic format check on every git commit, add a pre-commit hook:
```
ln -s -f ../../format-check.sh .git/hooks/pre-commit
```
