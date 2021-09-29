use tonic::{Code, Request, Response, Status};

use metastore::{DatabaseInterface, LockDataRef, ObjectPath, SelfContainedDb, TypedValue};

use crate::grpc_defs;
use crate::grpc_defs::{
    Empty, LockDataRefId, ReadRequest, Value, ValueRanged, WriteError, WriteRequest,
};

pub struct FollowerGRPCServer(SelfContainedDb);

impl Default for FollowerGRPCServer {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl From<LockDataRefId> for LockDataRef {
    fn from(request: LockDataRefId) -> Self {
        LockDataRef {
            id: request.id,
            timestamp: request.id.into(),
        }
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
    async fn new_with_time(
        &self,
        request: Request<LockDataRefId>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        let request = LockDataRef::from(request);

        log::debug!("(Follower) Creating transaction {:?}", request);
        self.0.new_transaction(&request);

        Ok(Response::new(Empty {}))
    }

    async fn serve_read(&self, request: Request<ReadRequest>) -> Result<Response<Value>, Status> {
        let (lockdataref, key) = request.into_inner().into();
        let res = self
            .0
            .serve_read(lockdataref, &key)
            .0
            .map_err(|err| Status::new(Code::Unavailable, "Unavailable"))?
            .unwrap();

        let res = grpc_defs::value::Res::Val(res.into_inner().1.to_string());
        let val = Value { res: Some(res) };
        Ok(Response::new(val))
    }

    async fn serve_range_read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<ValueRanged>, Status> {
        todo!()
    }

    async fn serve_write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteError>, Status> {
        let (txn, key, value) = request.into_inner().into();
        log::debug!("(Follower) Written {} {}", key, &value);
        self.0.serve_write(txn, &key, value);

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
