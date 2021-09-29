use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use futures::executor::block_on;
use log::LevelFilter;
use tokio::runtime::Runtime;
use tonic::{transport::Server, Code, Request, Response, Status};

use metastore::{
    DatabaseInterface, LockDataRef, NetworkResult, ObjectPath, SelfContainedDb, TypedValue,
    ValueWithMVCC,
};

use crate::follower_grpc_server::FollowerGRPCServer;
use crate::grpc_defs;
use crate::grpc_defs::replicator_server::ReplicatorServer;
use crate::grpc_defs::{
    Empty, Kv, LockDataRefId, ReadRequest, Value, ValueRanged, WriteError, WriteRequest,
};
use std::convert::TryFrom;
use std::str::FromStr;
use tonic::transport::Endpoint;

pub fn setup_logging() {
    env_logger::Builder::new()
        .format_timestamp_millis()
        .parse_default_env()
        .filter_level(LevelFilter::Debug)
        .filter_module("h2", LevelFilter::Warn)
        .filter_module("hyper", LevelFilter::Warn)
        .filter_module("tower", LevelFilter::Warn)
        .init();
    println!("Using env logger");
}

pub type Client = grpc_defs::replicator_client::ReplicatorClient<tonic::transport::Channel>;

// todo: implement async_database_interface specifically for this client and make a wrapper around
// `n` (replication factor) number of clients to reduce latency.
impl DatabaseInterface for Client {
    fn new_transaction(&self, txn: &LockDataRef) -> NetworkResult<(), String> {
        log::debug!("(Localside) Creating new transaction {}", txn.id);
        block_on(self.clone().new_with_time(LockDataRefId { id: txn.id }));
        NetworkResult::default()
    }

    fn serve_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<ValueWithMVCC, String> {
        unimplemented!()
    }

    fn serve_range_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        unimplemented!()
    }

    fn serve_write(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
        value: TypedValue,
    ) -> NetworkResult<(), String> {
        log::debug!("(Localside) Doing serve_write {}", txn.id);
        let kv = Kv {
            key: key.to_string(),
            value: value.to_string(),
        };
        let write = WriteRequest {
            txn: Option::from(LockDataRefId { id: txn.id }),
            kv: Some(kv),
        };
        block_on(Client::serve_write(&mut self.clone(), write));
        NetworkResult::default()
    }

    fn commit(&self, txn: LockDataRef) -> NetworkResult<(), String> {
        log::debug!("(Localside) Doing commit {}", txn.id);
        block_on(Client::commit(
            &mut self.clone(),
            LockDataRefId { id: txn.id },
        ));
        NetworkResult::default()
    }

    fn abort(&self, p0: LockDataRef) -> NetworkResult<(), String> {
        block_on(Client::abort(
            &mut self.clone(),
            LockDataRefId { id: p0.id },
        ));
        NetworkResult::default()
    }
}

pub async fn generate_threaded_follower(addr: &str) -> Box<dyn DatabaseInterface> {
    async fn wait_for_socket_open(addr: &str) {
        while tokio::net::TcpStream::connect(addr).await.is_err() {}
    }

    let greeter = FollowerGRPCServer::default();

    let addr1 = addr.to_string();
    std::thread::spawn(move || {
        let handle = Server::builder()
            .add_service(ReplicatorServer::new(greeter))
            .serve(SocketAddr::from_str(&addr1).unwrap());

        Runtime::new().unwrap().block_on(handle);
    });

    wait_for_socket_open(addr).await;

    let addr = format!("http://{}", addr);
    let client = Client::connect(Endpoint::try_from(addr).unwrap())
        .await
        .unwrap();
    Box::new(client)
}
