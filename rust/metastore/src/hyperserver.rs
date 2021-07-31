use std::sync::{Arc, Mutex};

use hyper::{Body, Method, Request, Response};
use serde_json::{Value as JSONValue};

use crate::{DbContext, hyper_error_converter};
use crate::rwtransaction_wrapper::{RWTransactionWrapper};
use crate::rwtransaction_wrapper::json_request_writers;

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


async fn write_request(
    mut req: Request<Body>,
    ctx: Arc<Mutex<DbContext>>,
) -> Result<Response<Body>, String> {
    let body = req.body_mut();
    let bytes = hyper::body::to_bytes(body).await.unwrap();
    let bytes: &[u8] = &bytes;

    let value: JSONValue = serde_json::from_reader(bytes).unwrap();
    let mut lock = ctx.lock().unwrap();
    let mut txn = RWTransactionWrapper::new(&mut lock);
    json_request_writers::write_json(value, &mut txn)?;
    txn.commit();

    let dbdump = lock.db.printdb();
    Ok(Response::builder().body(dbdump.into()).unwrap())
}

