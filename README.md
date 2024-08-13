![SplinterDB Project Logo](docs/images/splinterDB-logo.png)

![Splinter Tests](https://github.com/vmware/splinterdb/actions/workflows/run-tests.yml/badge.svg)


## What is SplinterDB?
SplinterDB is a key-value store designed for high performance on fast storage devices.

See [this talk at the CMU Database Seminar](https://www.youtube.com/watch?v=1gOlXfbiT_Y) and this paper:
> Alexander Conway, Abhishek Gupta, Vijay Chidambaram, Martin Farach-Colton, Richard P. Spillane, Amy Tai, Rob Johnson:
[SplinterDB: Closing the Bandwidth Gap for NVMe Key-Value Stores](https://www.usenix.org/conference/atc20/presentation/conway). USENIX Annual Technical Conference 2020: 49-63

## Usage
SplinterDB is a library intended to be embedded within another program.  See [Usage](docs/usage.md) for a guide on how to integrate SplinterDB into an application.

SplinterDB is *provided as-is* and is not recommended for use in production until version 1.0. See [Limitations](docs/limitations.md) for details.

## Build
See [Build](docs/build.md) for instructions on building SplinterDB from source.

See [Documentation](docs/README.md) for preliminary documentation.
