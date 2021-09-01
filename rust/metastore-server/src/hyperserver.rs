use std::sync::{Arc, Mutex};

use hyper::{Body, Method, Request, Response};
use serde_json::{Value as JSONValue};

mod hyper_error_converter;
mod json_request_writers;
use metastore::{DbContext};
use std::future::Future;
use hyper::service::{make_service_fn, service_fn};




pub async fn route_request<'a>(
    req: Request<Body>,
    ctx: Arc<Mutex<DbContext>>,
) -> Result<Response<Body>, String> {
    println!("uri: {}", req.uri());
    println!("{:?}", req.method());

    let ret = match req.method() {
        &Method::GET => {
            println!("Serving get request");
            Ok(Response::new(
                json_request_writers::read_json_request(
                    req.uri().path_and_query().unwrap().as_str(),
                    &*ctx.lock().unwrap(),
                ).to_string()
                    .into(),
            ))
        }
        &Method::POST => write_request(req, ctx).await,
        _ => Err("Method not supported".to_string()),
    };

    hyper_error_converter::map_str_error(ret)
}

use metastore::rwtransaction_wrapper::ReplicatedTxn;
use metastore::db_context::create_replicated_context;

async fn write_request(
    mut req: Request<Body>,
    ctx: Arc<Mutex<DbContext>>,
) -> Result<Response<Body>, String> {
    let body = req.body_mut();
    let bytes = hyper::body::to_bytes(body).await.unwrap();
    let bytes: &[u8] = &bytes;

    let value: JSONValue = serde_json::from_reader(bytes).unwrap();
    let mut lock = ctx.lock().unwrap();
    let mut txn = ReplicatedTxn::new(&mut lock);
    json_request_writers::write_json(value, &mut txn)?;
    txn.commit();

    Ok(Response::builder().body(Body::from("Written successful")).unwrap())
}

#[tokio::main]
async fn main() {
    let ctx = create_replicated_context();

    let ctx = Arc::new(Mutex::new(ctx));

    let server = create_web_server(ctx);

    println!("starting server");
    server.await;
}

use std::net::SocketAddr;

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
                println!("{:?} request came in", req);
                route_request(req, ctx.clone())
            }))
        }
    });

    let server = hyper::Server::bind(&addr).serve(make_svc);
    server
}

