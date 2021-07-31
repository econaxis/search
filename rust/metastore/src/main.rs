#![feature(min_type_alias_impl_trait)]
#![feature(assert_matches)]

use std::cell::{RefCell, UnsafeCell};

use std::sync::{Mutex};


use rwtransaction_wrapper::{IntentMap, MutBTreeMap};
use rwtransaction_wrapper::ValueWithMVCC;

use crate::wal_watcher::ByteBufferWAL;

// mod hyper_error_converter;
extern crate quickcheck;
extern crate quickcheck_macros;

// mod hyperserver;
mod object_path;
mod parsing;
mod rwtransaction_wrapper;
mod timestamp;
mod c_interface;
mod wal_watcher;
mod test_transaction_generate;
mod thread_tests;


pub struct MutSlab(Mutex<UnsafeCell<slab::Slab<ValueWithMVCC>>>);

impl MutSlab {
    pub fn get_mut(&self, key: usize) -> &mut ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }.get_mut(key).unwrap()
    }

    pub fn remove(&self, key: usize) -> ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }.remove(key)
    }
    pub fn new() -> Self {
        Self(Mutex::new(UnsafeCell::new(slab::Slab::new())))
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
        wallog: RefCell::new(ByteBufferWAL::new())
    }
}

pub struct DbContext {
    pub db: MutBTreeMap,
    pub transaction_map: IntentMap,
    pub old_values_store: MutSlab,
    pub wallog: RefCell<ByteBufferWAL>
}

unsafe impl Send for DbContext {}
unsafe impl Sync for DbContext {}

fn main() {
    thread_tests::tests::unique_set_insertion_test();
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

