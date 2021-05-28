use std::env;

fn main() {
    if Ok("release".to_owned()) == env::var("PROFILE") {
        println!("Using release path");
        println!("cargo:rustc-link-search=/home/henry/search/cmake-build-release/");
    } else {
        println!("cargo:rustc-link-search=/home/henry/search/cmake-build-debug/");
    }
}