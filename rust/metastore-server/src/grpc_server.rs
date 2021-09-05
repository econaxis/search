use tonic::transport::Server;

mod replicator_entrypoint;
mod grpc_defs;
use grpc_defs::replicator_server::ReplicatorServer;
use replicator_entrypoint::FollowerGRPCServer;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    replicator_entrypoint::setup_logging();
    let addr = "0.0.0.0:50051".parse()?;
    let greeter = FollowerGRPCServer::default();

    Server::builder()
        .add_service(ReplicatorServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}