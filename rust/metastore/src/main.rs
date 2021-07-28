#![feature(min_type_alias_impl_trait)]
#![feature(assert_matches)]

use std::cell::{UnsafeCell};

use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};




use hyper::service::{make_service_fn, service_fn};

mod hyper_error_converter;
mod hyperserver;
mod object_path;
mod parsing;
mod rwtransaction_wrapper;
mod timestamp;
mod c_interface;
mod wal_watcher;

use rwtransaction_wrapper::{MVCCMetadata, ValueWithMVCC, MutBTreeMap, IntentMap};

pub struct MutSlab(pub UnsafeCell<slab::Slab<ValueWithMVCC>>);

impl MutSlab {
    pub fn get(&self) -> &mut slab::Slab<ValueWithMVCC> {
        unsafe { &mut *self.0.get() }
    }
}

pub fn create_empty_context() -> DbContext {
    DbContext {
        db: MutBTreeMap::new(),
        transaction_map: IntentMap::new(),
        old_values_store: MutSlab(UnsafeCell::new(slab::Slab::new())),
    }
}

pub struct DbContext {
    pub db: MutBTreeMap,
    pub transaction_map: IntentMap,
    pub old_values_store: MutSlab,
}

#[tokio::main]
async fn main() {
    let ctx = create_empty_context();

    let ctx = Arc::new(Mutex::new(ctx));

    let server = create_web_server(ctx);
    server.await;
}

fn create_web_server(ctx: Arc<Mutex<DbContext>>) -> impl Future {
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

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
    server
}

