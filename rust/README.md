# Rust Wrapper for SplinterDB

These docs assume some basic familiarity with the Rust language and tools, particularly the [Rust build tool `cargo`](https://doc.rust-lang.org/book/ch01-03-hello-cargo.html)

## Overview
Rust may be suitable for developing applications that use SplinterDB, and for writing certain types of tests of SplinterDB.

This directory contains Rust bindings for SplinterDB
- `splinterdb-sys`: Lowest level, unsafe Rust declarations for a subset of the SplinterDB public API.
- `splinterdb-rs`: A safe and ergonomic Rust wrapper, intended for use by other Rust libraries and Rust applications.
- `splinterdb-cli`: A simple command line utility that provides a limited key/value interface.
   It serves as an example of how to build a Rust application that uses SplinterDB as a library, and can be used for basic performance testing.

## Usage
Ensure you have Rust and Cargo available, e.g. use [rustup](https://rustup.rs/).

Next, [build and install the SplinterDB C library](../../docs/build.md) **using `clang-13`**,
e.g.:
```sh
CC=clang-13 LD=clang-13 make -C .. && make install -C ..
```

Then from this directory, run
```sh
cargo build
cargo test
```

Cargo builds into the `target/debug` subdirectory.  For release builds, add `--release` to the above commands and look in `target/release`.


## Why does this only build with `clang` and not `gcc`?
Short answer: because of link time optimization (LTO).

Longer answer:
To use LTO across languages, e.g. C with Rust, all compilation units must be built using the same toolchain.
The Rust compiler is based on LLVM, not GCC.  Therefore, SplinterDB must be built with `clang` (or
without LTO), in order to be usable from Rust.
