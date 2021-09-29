fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("{}/src/", std::env::var("CARGO_MANIFEST_DIR").unwrap());
    tonic_build::configure()
        .out_dir(path).compile(&["src/grpc.proto"], &["src/"]).unwrap();
    Ok(())
}