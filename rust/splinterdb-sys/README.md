# `splinterdb-sys`

Lowest level, unsafe Rust declarations for (some of) the C functions exported from SplinterDB.

The exported headers are listed in `wrapper.h`. In order to build, the splinterdb shared libraries must be built and present at `\usr\local\lib`. The location where the shared libraries are found can be changed by modifying `build.rs`.

If the shared libraries change, `cargo build` should automatically detect the change and rebuild this package.

## Generating the Wrapper
The wrapper code is generated automatically upon a call to `cargo build` based upon the files listed in `wrapper.h`.

If it is necessary to expand the functionality of `splinterdb-sys` simply add more files to `wrapper.h` and build again.

## Verifying the Wrapper
From this directory run
```sh
cargo build
cargo test
```

This runs the smoke test for this library, ensuring that the C bindings are created successfully. If errors occur ensure that `\usr\local\lib` is in `LD_LIBRARY_PATH` and the shared libraries have been successfully installed at that location.
