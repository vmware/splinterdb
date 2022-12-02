extern crate bindgen;
extern crate cbindgen;

use std::{env, path::PathBuf};

use cbindgen::Language;

const PLATFORM_DIR: &str = "linux";

fn gen_import() {
    println!("cargo:rerun-if-changed=wrapper.h");
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .ctypes_prefix("cty")
        .clang_args(["-I", "../include"])
        .clang_args(["-I", "../src"])
        .clang_arg(format!(
            "-DSPLINTERDB_PLATFORM_DIR=platform_{}",
            PLATFORM_DIR
        ))
        .clang_args(["-I", &format!("../src/platform_{}", PLATFORM_DIR)])
        .generate()
        .expect("Unable to generate import bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("couldn't write bindings");
}

fn gen_export() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    println!("cargo:rerun-if-changed=../src/rblob.h");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(Language::C)
        .with_no_includes()
        .generate()
        .expect("Unable to generate export bindings")
        .write_to_file("../src/rblob.h");
}

fn main() {
    gen_import();
    gen_export();
}
