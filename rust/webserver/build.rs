use std::env;

fn main() {
    println!("cargo:rustc-link-search={}", env::var("CMAKE_BINARY_DIR").unwrap());
    println!("cargo:rustc-link-search=$ORIGIN");
}