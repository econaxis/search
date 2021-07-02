use std::env;

fn main() {
    // println!("cargo:rustc-link-search={}", env::var("CMAKE_BINARY_DIR").unwrap());
    // if Ok("release".to_owned()) == env::var("PROFILE") {
    //     println!("cargo:rustc-link-search=/home/henry/search/cmake-build-release/");
    //     println!(r#"cargo:rustc-cdylib-link-arg=-Wl,-rpath,/home/henry/search/cmake-build-release/"#);
    // } else {
    //     println!("cargo:rustc-link-search=/home/henry/search/cmake-build-debug/");
    //     println!(r#"cargo:rustc-cdylib-link-arg=-Wl,-rpath,/home/henry/search/cmake-build-debug/"#);
    // }
    println!("cargo:rustc-link-search={}", env::var("CMAKE_BINARY_DIR").unwrap());
    println!("cargo:rustc-link-search={}", env::var("CMAKE_BINARY_DIR").unwrap());
    println!("cargo:rustc-link-search=$ORIGIN");
    println!("cargo:rustc-link-search=$ORIGIN/../../");
    // println!(r#"cargo:rustc-link-lib=c-search-abi"#);
}