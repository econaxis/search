use crate::grpc_defs::main_replicator_server::MainReplicator;
use crate::grpc_defs::{Empty, Json, JsonWriteRequest, LockDataRefId, ReadRequest};
use crate::json_request_writers::{read_json_request, write_json_txnid};
use metastore::{DatabaseInterface, LockDataRef, SelfContainedDb};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct MainReplicatorServer(SelfContainedDb, AtomicU64);

#[tonic::async_trait]
impl MainReplicator for MainReplicatorServer {
    async fn create_transaction(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<LockDataRefId>, Status> {
        let new_txn_counter = self.1.fetch_add(1, Ordering::SeqCst);
        let txn = LockDataRef::debug_new(new_txn_counter);
        self.0.new_transaction(&txn);

        Ok(Response::new(LockDataRefId { id: txn.id }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<Json>, Status> {
        let request = request.into_inner();
        let res = read_json_request(&request.key, &self.0.db);
        let res = serde_json::to_string(&res).unwrap();

        Ok(Response::new(Json { inner: res }))
    }

    async fn write(&self, request: Request<JsonWriteRequest>) -> Result<Response<Json>, Status> {
        let JsonWriteRequest { path, value } = request.into_inner();
        let value = value.unwrap();

        let value: serde_json::Value = serde_json::from_str(&value.inner).unwrap();
        write_json_txnid(value)
    }

    async fn abort(&self, request: Request<LockDataRefId>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn commit(&self, request: Request<LockDataRefId>) -> Result<Response<Empty>, Status> {
        todo!()
    }
}
