use std::convert::Infallible;

use tokio::runtime::Runtime;
use tonic::transport::{Error, Server};

use follower_grpc_server::FollowerGRPCServer;
use grpc_defs::main_replicator_server::MainReplicatorServer as grpcMainReplicatorServer;
use grpc_defs::replicator_server::ReplicatorServer;
use metastore::{DatabaseInterface, LockDataRef, SelfContainedDb};
use replicator_entrypoint::setup_logging;

use crate::main_db_impl::MainReplicatorServer;
use crate::replicator_entrypoint::{generate_threaded_follower, Client};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

mod follower_grpc_server;
mod grpc_defs;
mod json_request_writers;
mod main_db_impl;
mod replicator_entrypoint;

fn main() {
    setup_logging();
    let mut rt = Runtime::new().unwrap();
    // let client = generate_threaded_follower("0.0.0.0:50051");
    // let client = rt.block_on(client);

    let server = MainReplicatorServer::default();
    let handle = Server::builder()
        .add_service(grpcMainReplicatorServer::new(server))
        .serve(SocketAddr::from_str("0.0.0.0:50051").unwrap());

    rt.block_on(handle);
    // rt.block_on(async {
    //     println!("connecting");
    //     let a: &dyn DatabaseInterface = client.as_ref();
    //     let txn = LockDataRef::debug_new(5);
    //     client.new_transaction(&txn)?;
    //     a.serve_write(txn, &"k".into(), "value".into())?;
    //
    //     a.commit(txn)?;
    //     Result::Ok::<(), String>(())
    // })
    // .unwrap();

    std::thread::sleep(Duration::from_secs_f64(1e10));
    // println!("finished");
}

#[cfg(test)]
#[test]
fn test_grpc() {
    let client = generate_threaded_follower("0.0.0.0:50051".parse().unwrap());
    let mut rt = Runtime::new().unwrap();

    rt.block_on(async move {
        let client = client.await;
        println!("connecting");
        let a: &dyn DatabaseInterface = client.as_ref();
        let txn = LockDataRef::debug_new(5);
        client.new_transaction(&txn)?;
        a.serve_write(txn, &"k".into(), "value".into())?;

        Result::Ok::<(), String>(())
    })
    .unwrap();
}
