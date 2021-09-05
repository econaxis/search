use tonic::{transport::Server, Response, Request, Status, Code};
use crate::grpc_defs;
use crate::grpc_defs::{LockDataRefId, Empty, ReadRequest, Value, ValueRanged, WriteRequest, WriteError, Kv};
use metastore::{SelfContainedDb, LockDataRef, DatabaseInterface, ObjectPath, TypedValue, NetworkResult, ValueWithMVCC};

pub struct FollowerGRPCServer(SelfContainedDb);

use log::LevelFilter;

pub fn setup_logging() {
    env_logger::Builder::new().format_timestamp_millis()
        .parse_default_env()
        .filter_level(LevelFilter::Debug)
        .filter_module("h2", LevelFilter::Warn)
        .filter_module("hyper", LevelFilter::Warn)
        .init();
    println!("Using env logger");
}

impl Default for FollowerGRPCServer {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl From<LockDataRefId> for LockDataRef {
    fn from(request: LockDataRefId) -> Self {
        LockDataRef { id: request.id, timestamp: request.id.into() }
    }
}

impl From<WriteRequest> for (LockDataRef, ObjectPath, TypedValue) {
    fn from(a: WriteRequest) -> Self {
        let txn: LockDataRef = a.txn.unwrap().into();
        let kv = a.kv.unwrap();
        let key: ObjectPath = kv.key.into();
        let value = TypedValue::from(kv.value);

        (txn, key, value)
    }
}

impl From<ReadRequest> for (LockDataRef, ObjectPath) {
    fn from(a: ReadRequest) -> Self {
        let ReadRequest { txn, key } = a;
        let txn = LockDataRef::from(txn.unwrap());
        let key = ObjectPath::from(key);
        (txn, key)
    }
}

#[tonic::async_trait]
impl grpc_defs::replicator_server::Replicator for FollowerGRPCServer {
    async fn new_with_time(&self, request: Request<LockDataRefId>) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        let request = LockDataRef::from(request);

        log::debug!("Creating transaction {:?}", request);
        self.0.new_transaction(&request);

        Ok(Response::new(Empty {}))
    }

    async fn serve_read(&self, request: Request<ReadRequest>) -> Result<Response<Value>, Status> {
        let (lockdataref, key) = request.into_inner().into();
        let res = self.0.serve_read(lockdataref, &key).
            0.map_err(|err| Status::new(Code::Unavailable, "Unavailable"))?.unwrap();

        let res = grpc_defs::value::Res::Val(res.into_inner().1.to_string());
        let val = Value { res: Some(res) };
        Ok(Response::new(val))
    }

    async fn serve_range_read(&self, request: Request<ReadRequest>) -> Result<Response<ValueRanged>, Status> {
        todo!()
    }

    async fn serve_write(&self, request: Request<WriteRequest>) -> Result<Response<WriteError>, Status> {
        let (txn, key, value) = request.into_inner().into();
        self.0.serve_write(txn, &key, value);

        log::debug!("Written {}", key);
        Ok(Response::new(WriteError { res: None }))
    }

    async fn commit(&self, request: Request<LockDataRefId>) -> Result<Response<Empty>, Status> {
        let request: LockDataRef = request.into_inner().into();

        self.0.commit(request);
        log::debug!("Committed {}", request.id);

        Ok(Response::new(Empty {}))
    }

    async fn abort(&self, request: Request<LockDataRefId>) -> Result<Response<Empty>, Status> {
        let request: LockDataRef = request.into_inner().into();

        self.0.abort(request);
        Ok(Response::new(Empty {}))
    }
}

use futures::executor::block_on;
use std::env;


type Client = grpc_defs::replicator_client::ReplicatorClient<tonic::transport::Channel>;

impl DatabaseInterface for Client {
    fn new_transaction(&self, txn: &LockDataRef) -> NetworkResult<(), String> {
        block_on(self.clone().new_with_time(LockDataRefId { id: txn.id }));
        NetworkResult::default()
    }

    fn serve_read(&self, txn: LockDataRef, key: &ObjectPath) -> NetworkResult<ValueWithMVCC, String> {
        todo!()
    }

    fn serve_range_read(&self, txn: LockDataRef, key: &ObjectPath) -> NetworkResult<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        todo!()
    }

    fn serve_write(&self, txn: LockDataRef, key: &ObjectPath, value: TypedValue) -> NetworkResult<(), String> {
        let kv = Kv { key: key.to_string(), value: value.to_string() };
        let write = WriteRequest { txn: Option::from(LockDataRefId { id: txn.id }), kv: Some(kv) };
        block_on(Client::serve_write(&mut self.clone(), write));
        NetworkResult::default()
    }

    fn commit(&self, txn: LockDataRef) -> NetworkResult<(), String> {
        block_on(Client::commit(&mut self.clone(), LockDataRefId { id: txn.id }));
        NetworkResult::default()
    }

    fn abort(&self, p0: LockDataRef) -> NetworkResult<(), String> {
        block_on(Client::abort(&mut self.clone(), LockDataRefId { id: p0.id }));
        NetworkResult::default()
    }
}