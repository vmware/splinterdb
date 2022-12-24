extern crate bindgen;
extern crate cbindgen;

use std::{env, path::PathBuf};

use cbindgen::Language;

fn gen_import() {
    println!("cargo:rerun-if-changed=wrapper.h");

    let platform_dir = env::var("PLATFORM_DIR").unwrap_or("linux".to_string());

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .ctypes_prefix("cty")
        .clang_args(["-I", "../include"])
        .clang_args(["-I", "../src"])
        .clang_arg(format!("-DSPLINTERDB_PLATFORM_DIR={platform_dir}",))
        .clang_args(["-I", &format!("../src/{platform_dir}")])
        //.generate_inline_functions(true)
        .generate()
        .expect("Unable to generate import bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("couldn't write bindings");
}

fn gen_export() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let out = PathBuf::from("..")
        .join(env::var("BUILD_ROOT").unwrap_or("build".to_string()))
        .join("include")
        .join("rblob.h");

    println!("cargo:rerun-if-changed={}", out.to_string_lossy());

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(Language::C)
        .with_no_includes()
        .generate()
        .expect("Unable to generate export bindings")
        .write_to_file(out);
}

fn main() {
    gen_import();
    //gen_export();
}
