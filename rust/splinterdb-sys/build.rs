// link to libsplinterdb.a, libaio and libxxhash
#[allow(clippy::print_literal)]
fn main() {
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let abi = std::env::var("CARGO_CFG_TARGET_ENV").unwrap();
    let profile = std::env::var("PROFILE").unwrap();
    let system_lib_dir = format!("/usr/lib/{}-{}-{}", arch, os, abi);

    let package_dir = std::env::current_dir().unwrap();
    let splinter_dir = package_dir.parent().unwrap().parent().unwrap();
    let splinter_lib_dir = splinter_dir
        .join("build")
        .join(profile)
        .join("lib")
        .to_str()
        .unwrap()
        .to_owned();

    println!("cargo:rustc-link-search=native={}", splinter_lib_dir);
    println!("cargo:rustc-link-search=native={}", system_lib_dir);
    println!("cargo:rustc-link-lib=static={}", "aio");
    println!("cargo:rustc-link-lib=static={}", "xxhash");
    println!("cargo:rustc-link-lib=static={}", "splinterdb");
}
