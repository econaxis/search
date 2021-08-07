#![feature(assert_matches)]
#![feature(trace_macros)]
#![feature(backtrace)]
use std::cell::{RefCell, UnsafeCell};

use std::sync::Mutex;

use rwtransaction_wrapper::ValueWithMVCC;
use rwtransaction_wrapper::{IntentMap, MutBTreeMap};

use crate::wal_watcher::ByteBufferWAL;
use thread_tests::tests::monotonic;
use crate::replicated_slave::ReplicatedDatabase;
use std::rc::Rc;

// mod hyper_error_converter;
extern crate quickcheck;
extern crate quickcheck_macros;

// mod hyperserver;
mod c_interface;
mod object_path;
mod parsing;
mod rwtransaction_wrapper;

#[macro_use]
mod test_transaction_generate;
mod thread_tests;
mod timestamp;
mod wal_watcher;

mod secondary_indexing;

#[macro_use]
mod error_macro;
mod hermitage_tests;
mod replicated_slave;
mod extensions_interface;

pub struct MutSlab(Mutex<UnsafeCell<slab::Slab<ValueWithMVCC>>>);

impl MutSlab {
    pub fn get_mut(&self, key: usize) -> &mut ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }
            .get_mut(key)
            .unwrap()
    }

    pub fn remove(&self, key: usize) -> ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }.remove(key)
    }
    pub fn new() -> Self {
        Self(Mutex::new(UnsafeCell::new(slab::Slab::with_capacity(10000))))
    }
    pub fn insert(&self, v: ValueWithMVCC) -> usize {
        unsafe { &mut *self.0.lock().unwrap().get() }.insert(v)
    }
}

pub fn create_empty_context() -> DbContext {
    DbContext {
        db: MutBTreeMap::new(),
        transaction_map: IntentMap::new(),
        old_values_store: MutSlab::new(),
        wallog: ByteBufferWAL::new(),
        replicators: None
    }
}

pub fn create_replicated_context() -> DbContext {
    DbContext {
        db: MutBTreeMap::new(),
        transaction_map: IntentMap::new(),
        old_values_store: MutSlab::new(),
        wallog: ByteBufferWAL::new(),
        replicators: Some(Box::new(ReplicatedDatabase::new()))
    }
}

pub struct DbContext {
    pub db: MutBTreeMap,
    pub transaction_map: IntentMap,
    pub old_values_store: MutSlab,
    pub wallog: ByteBufferWAL,
    pub replicators: Option<Box<ReplicatedDatabase>>
}

impl DbContext {
    pub fn replicator(&self) -> &ReplicatedDatabase {
        self.replicators.as_ref().unwrap()
    }
}

unsafe impl Send for DbContext {}

unsafe impl Sync for DbContext {}

fn main() {
    for _ in 0..500 {
        wal_watcher::tests::test1();
    }
    // monotonic();
    // thread_tests::tests::unique_set_insertion_test();
}

// #[tokio::main]
// async fn main() {
//     let ctx = create_empty_context();
//
//     let ctx = Arc::new(Mutex::new(ctx));
//
//     let server = create_web_server(ctx);
//     server.await;
// }

// fn create_web_server(ctx: Arc<Mutex<DbContext>>) -> impl Future {
//     let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
//
//     let make_svc = make_service_fn(move |_conn| {
//         // Since this outer closure is called everytime a new TCP connection comes in,
//         // we have to clone the state into us.
//         // service_fn converts our function into a `Service`
//         let ctx = ctx.clone();
//         async move {
//             Ok::<_, String>(service_fn(move |req| {
//                 // Since this inner closure is called everytime a request is made (from the same TCP connection),
//                 // have to clone the state again.
//                 hyperserver::route_request(req, ctx.clone())
//             }))
//         }
//     });
//
//     let server = hyper::Server::bind(&addr).serve(make_svc);
//     server
// }
