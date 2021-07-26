#![feature(min_type_alias_impl_trait)]

use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use hyper::service::{make_service_fn, service_fn};

use btreemap_kv_backend::MutBTreeMap;
use kv_backend::{IntentMapType, ValueWithMVCC};
use mvcc_manager::WriteIntentStatus;

use timestamp::Timestamp;

mod btreemap_kv_backend;
mod debugging_utils;
mod hyper_error_converter;
mod hyperserver;
mod json_processing;
pub mod kv_backend;
mod mvcc_manager;
mod mvcc_metadata;
mod object_path;
mod parsing;
mod rwtransaction_wrapper;
mod timestamp;

// Contains only write intent status for now, but may contain more in the future.
pub struct TransactionLockData(pub WriteIntentStatus);

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub struct LockDataRef {
    pub id: u64,
    pub timestamp: Timestamp,
}

impl TransactionLockData {
    pub fn get_write_intent(&self) -> WriteIntentStatus {
        self.0
    }
}

impl LockDataRef {
    pub fn to_txn<'a>(
        &self,
        map: &'a HashMap<LockDataRef, TransactionLockData>,
    ) -> &'a TransactionLockData {
        map.get(self).unwrap()
    }
}

pub struct MutSlab(pub UnsafeCell<slab::Slab<ValueWithMVCC>>);

impl MutSlab {
    pub fn get(&self) -> &mut slab::Slab<ValueWithMVCC> {
        unsafe { &mut *self.0.get() }
    }
}

pub struct DbContext {
    pub db: MutBTreeMap,
    pub transaction_map: RefCell<IntentMapType>,
    pub old_values_store: MutSlab,
}

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let ctx = DbContext {
        db: MutBTreeMap::new(),
        transaction_map: RefCell::new(HashMap::new()),
        old_values_store: MutSlab(UnsafeCell::new(slab::Slab::new())),
    };

    let ctx = Arc::new(Mutex::new(ctx));

    let make_svc = make_service_fn(move |_conn| {
        // Since this outer closure is called everytime a new TCP connection comes in,
        // we have to clone the state into us.
        // service_fn converts our function into a `Service`
        let ctx = ctx.clone();
        async move {
            Ok::<_, String>(service_fn(move |req| {
                // Since this inner closure is called everytime a request is made (from the same TCP connection),
                // have to clone the state again.
                hyperserver::route_request(req, ctx.clone())
            }))
        }
    });

    let server = hyper::Server::bind(&addr).serve(make_svc);

    server.await;
}
