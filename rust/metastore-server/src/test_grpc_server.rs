use tonic::transport::{Server, Error};

mod replicator_entrypoint;
mod grpc_defs;

use grpc_defs::replicator_server::ReplicatorServer;
use replicator_entrypoint::{FollowerGRPCServer, setup_logging};
use tokio::runtime::Runtime;
use crate::replicator_entrypoint::{generate_follower, Client};
use metastore::{DatabaseInterface, LockDataRef};
use std::convert::Infallible;


fn main() {
    setup_logging();
    let handle = generate_follower("0.0.0.0:50051".parse().unwrap());

    let mut rt = Runtime::new().unwrap();

    rt.block_on(async {
        println!("connecting");
        let client = Client::connect("http://0.0.0.0:50051").await.unwrap();
        let a: &dyn DatabaseInterface = &client;
        let txn = LockDataRef::debug_new(5);
        client.new_transaction(&txn)?;
        a.serve_write(txn, &"k".into(), "value".into())?;

        Result::Ok::<(), String>(())
    }).unwrap();

    handle.join();
}

#[cfg(test)]
#[test]
fn test_grpc() {
    let handle = generate_follower("0.0.0.0:50051".parse().unwrap());

    let mut rt = Runtime::new().unwrap();

    rt.block_on(async {
        println!("connecting");
        let client = Client::connect("http://0.0.0.0:50051").await.unwrap();
        let a = &client as &dyn DatabaseInterface;
        let txn = LockDataRef::debug_new(5);
        client.new_transaction(&txn)?;
        a.serve_write(txn, &"k".into(), "value".into())?;

        Result::Ok::<(), String>(())
    }).unwrap();

}