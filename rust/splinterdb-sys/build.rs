extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    // Tell cargo to look for shared libraries in the specified directory
    println!("cargo:rustc-link-search=/usr/local/lib");

    // Tell cargo to tell rustc to link to splinterdb shared lib
    println!("cargo:rustc-link-lib=splinterdb");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        .no_copy("splinterdb.*")
        .no_copy("writable_buffer")
        .no_copy("data_config")
        .allowlist_type("splinterdb.*")
        .allowlist_function("splinterdb.*")
        .allowlist_function("default_data_config.*")
        .allowlist_function("merge.*")
        .allowlist_var("SPLINTERDB.*")
        .allowlist_var(".*_SIZE")
        .clang_arg("-DSPLINTERDB_PLATFORM_DIR=platform_linux")
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
