use std::borrow::Borrow;
use std::sync::{Arc, Mutex};

use hyper::{Body, Method, Request, Response};
use serde_json::Value as JSONValue;

use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::RWTransactionWrapper;
use crate::{debugging_utils, hyper_error_converter, json_processing, DbContext};

fn read_request(uri: &str, ctx: &DbContext) -> String {
    let objpath = ObjectPath::new(uri);
    let mut txn = RWTransactionWrapper::new(ctx);
    let ret = txn.read_range(&objpath);
    txn.commit();
    ret
}

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
                read_request(
                    req.uri().path_and_query().unwrap().as_str(),
                    &*ctx.lock().unwrap(),
                )
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

    let map = json_processing::json_to_map(value);

    let lock = ctx.lock().unwrap();
    let mut txn = RWTransactionWrapper::new(lock.borrow());
    for (key, value) in map {
        txn.write(&key, value.to_string().into())
    }
    txn.commit()?;

    let dbdump = debugging_utils::printdb(&lock.db);
    Ok(Response::builder().body(dbdump.into()).unwrap())
}
