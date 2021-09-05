fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/replicated_slave").compile(&["src/replicated_slave/grpc.proto"], &["src/replicated_slave"]).unwrap();
    Ok(())
}