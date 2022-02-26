![SplinterDB Project Logo](docs/images/splinterDB-logo.png)

## What is SplinterDB?
SplinterDB is a key-value store designed for high performance on fast storage devices.

See
> Alexander Conway, Abhishek Gupta, Vijay Chidambaram, Martin Farach-Colton, Richard P. Spillane, Amy Tai, Rob Johnson:
[SplinterDB: Closing the Bandwidth Gap for NVMe Key-Value Stores](https://www.usenix.org/conference/atc20/presentation/conway). USENIX Annual Technical Conference 2020: 49-63

## Usage
SplinterDB is a library intended to be embedded within another program.  See [Usage](docs/usage.md) for a guide on how to integrate SplinterDB into an application.

SplinterDB library is provided as is and until version 1.0 it is not recommended to run in production environments (APIs and functionality are expected to change). SplinterDB is developed and run in user space of linux systems like Ubuntu. This is a [list](docs/limitations.md) of the known limitations and missing features.

## Build
See [Build](docs/build.md) for instructions on building SplinterDB from source.
