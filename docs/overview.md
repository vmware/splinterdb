# SplinterDB Overview
This document provides an overview of the SplinterDB Key-Value Store, its
key capabilities, and target applications / workloads which can benefit from this
library.

SplinterDB is an embedded, general purpose Key-Value Store (KVS) designed from
the ground-up to deliver high performance on modern storage devices like NVMe SSDs.
The driving design tenet behind SplinterDB is to maximize the I/O bandwidth
on the device, thereby delivering high-performance to your application.

Keys and values are stored as byte-streams, where, by default, the keys are
lexicographically sorted.  User-specified comparison methods are supported.

**Salient Design Points**:

* Saturate I/O bandwidth on storage device
* Maximize Insert performance
* High-performance point query lookups
* Reasonably performant range scans
* Simple and easy-to-use Administration and Configuration interfaces

SplinterDB, sometimes also referred to simply as Splinter, is offered as a library
with a simple Key-Value Store API that you can link / embed in your application.
Command-line and programmatic interfaces are provided to insert a new key/value pair
(KV-pair), retrieve a value for a given key, scan for a set of values for a range
of keys specified by (min-key, max-key) bounds, delete one or more values given a key
or range of keys. See [KVS API Reference Manual](./kvs_api_refman.md) for more details.

**Technical Specifications**:

* Supported on Linux (Mac support is in the works.)
* OS-Level features used
    * Async I/O (libaio?)
    * Pthread support

* Out-of-the box configuration: (See [Configuration](./configuration.md) for more details.)
    * Max key-size = 256 bytes, Max value-size = 1024 bytes
    * 4 KB page size

**History**

SplinterDB was designed for deep integration with VMware's vSAN technology. Rooted from
this history, the designs evolved to enable Splinter to run in resource-constrained environments.
Overall, SplinterDB has minimal resource usage, and we strive hard to maintain that characteristic.

The version of Splinter ... should we talk about forked ... feature-gap?

Timeline will be nice to have here .. to set some chronology.
