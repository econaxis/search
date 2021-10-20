use crate::grpc_defs::main_replicator_server::MainReplicator;
use crate::grpc_defs::{Empty, Json, JsonWriteRequest, LockDataRefId, ReadRequest};
use crate::json_request_writers::{read_json_request_txn, write_json_txnid};
use metastore::{DatabaseInterface, LockDataRef, SelfContainedDb};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::{IntoRequest, Request, Response, Status};

pub struct MainReplicatorServer(SelfContainedDb, AtomicU64);

impl Default for MainReplicatorServer {
    fn default() -> Self {
        Self(SelfContainedDb::new_with_replication(), AtomicU64::new(2))
    }
}

#[tonic::async_trait]
impl MainReplicator for MainReplicatorServer {
    async fn create_transaction(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LockDataRefId>, Status> {
        let new_txn_counter = self.1.fetch_add(1, Ordering::SeqCst);
        let txn = LockDataRef::debug_new(new_txn_counter);
        self.0.new_transaction(&txn);

        Ok(Response::new(LockDataRefId { id: txn.id }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<Json>, Status> {
        let request = request.into_inner();
        let txn: LockDataRef = request.txn.unwrap().into();
        let res = read_json_request_txn(&request.key, &self.0, txn);
        let res = serde_json::to_string(&res).unwrap();

        Ok(Response::new(Json { inner: res }))
    }

    async fn write(&self, request: Request<JsonWriteRequest>) -> Result<Response<Json>, Status> {
        let JsonWriteRequest { path, value, txn } = request.into_inner();
        let value = value.unwrap();
        let txn: LockDataRef = txn.unwrap().into();

        let value: serde_json::Value = serde_json::from_str(&value.inner).unwrap();
        write_json_txnid(value, txn, &self.0, &path).unwrap();

        Ok(Response::new(Json {
            inner: "success".into(),
        }))
    }

    async fn abort(&self, request: Request<LockDataRefId>) -> Result<Response<Empty>, Status> {
        let txn: LockDataRef = request.into_inner().into();
        self.0.abort(txn);
        Ok(Response::new(Empty {}))
    }

    async fn commit(&self, request: Request<LockDataRefId>) -> Result<Response<Empty>, Status> {
        let txn: LockDataRef = request.into_inner().into();
        self.0.commit(txn);
        Ok(Response::new(Empty {}))
    }
}
