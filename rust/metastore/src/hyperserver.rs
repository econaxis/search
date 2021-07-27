use std::sync::{Arc, Mutex};

use hyper::{Body, Method, Request, Response};
use serde_json::{Value as JSONValue, Value};


use crate::rwtransaction_wrapper::RWTransactionWrapper;
use crate::{debugging_utils, hyper_error_converter, json_processing, DbContext};




pub fn read_json_request(uri: &str, ctx: &DbContext) -> JSONValue {

    let objpath = format!("/user{}", uri).into();
    let mut txn = RWTransactionWrapper::new(&ctx);
    let ret = txn.read_range(&objpath);

    let mut json = JSONValue::Null;
    for row in ret {
        let path: Vec<&str> = row.0.split_parts().collect();
        json_processing::create_materialized_path(&mut json, &path, row.1.1.clone());
    }

    txn.commit();

    let json = json.pointer_mut(objpath.as_str()).unwrap().take();
    json
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
                read_json_request(
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
    write_json(value, &mut txn)?;
    txn.commit();

    let dbdump = debugging_utils::printdb(&lock.db);
    Ok(Response::builder().body(dbdump.into()).unwrap())
}

pub fn write_json(value: Value, txn: &mut RWTransactionWrapper) -> Result<(), String> {
    let map = json_processing::json_to_map(value);
    for (key, value) in map {
        txn.write(&key, value.to_string().into())?
    }
    Ok(())
}
