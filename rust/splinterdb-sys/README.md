# `splinterdb-sys`

Lowest level, unsafe Rust declarations for (some of) the C functions exported from SplinterDB.

Currently this is limited to the `splinterdb` and `default_data_config` APIs, but could be enlarged in the future.

This package statically links against `libsplinterdb.a`.

## Re-generating the code
The [bindgen](https://github.com/rust-lang/rust-bindgen) tool is used to generate Rust
declarations for functions and structures.

To update that generated Rust code, based on the latest C headers in SplinterDB:

1. If you've created a new C header file with functions that should be usable
   from Rust, then add it to [src/include.h](src/include.h).
   When processed by `bindgen`, the include (`-I`) path will have the
   [SplinterDB include directory](../../include).

2. Ensure you have [bindgen](https://github.com/rust-lang/rust-bindgen) available on your `$PATH`.
   It usually suffices to
   ```sh
   cargo install bindgen
   ```

3. Regenerate the Rust code
   ```sh
   ./src/regenerate.sh
   ```

4. Verify
   ```sh
   cargo build
   cargo test
   ```
   There may be build errors, which require manual fixing.
   Look at the [existing flags in `src/regenerate.sh`](src/regenerate.sh) and
   the [hand-written bits in `src/lib.rs`](src/lib.rs).
