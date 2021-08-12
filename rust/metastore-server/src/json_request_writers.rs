mod json_data_model_tests;
mod json_processing;

use serde_json::{Value as JSONValue, Value};

use metastore::object_path::ObjectPath;
use metastore::rwtransaction_wrapper::ReplicatedTxn;
use metastore::DbContext;

pub fn read_json_request(uri: &str, ctx: &DbContext) -> JSONValue {
    let objpath = prettify_json_path(uri);

    let mut txn = ReplicatedTxn::new(&ctx);
    let ret = txn.read_range_owned(&objpath).unwrap();

    let mut json = JSONValue::Null;
    for row in ret {
        let path: Vec<&str> = row.0.split_parts().collect();
        json_processing::create_materialized_path(&mut json, &path, row.1.into_inner().1);
    }

    txn.commit();

    let stripped = match objpath.as_str().strip_suffix('/') {
        Some(x) => x,
        None => objpath.as_str(),
    };
    let json = match  json.pointer_mut(stripped) {
        Some(a) => a.take(),
        None => JSONValue::Null
    };
    json
}

fn prettify_json_path(uri: &str) -> ObjectPath {
    let mut uri = uri.to_string();
    if !uri.starts_with("/user/") {
        if uri.starts_with('/') {
            uri = format!("/user{}", uri).into()
        } else {
            uri = format!("/user/{}", uri).into()
        }
    };

    if !uri.ends_with('/') {
        uri.push('/');
    }
    ObjectPath::from(uri)
}

pub fn write_json(value: Value, txn: &mut ReplicatedTxn) -> Result<(), String> {
    let map = json_processing::json_to_map(value);
    for (key, value) in map {
        txn.write(&key, value.to_string().into())?;
    }
    Ok(())
}
