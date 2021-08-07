mod json_data_model_tests;
mod json_processing;

use serde_json::{Value as JSONValue, Value};

use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::DBTransaction;
use crate::DbContext;

pub fn read_json_request(uri: &str, ctx: &DbContext) -> JSONValue {
    let objpath = if uri.starts_with("/user/") {
        ObjectPath::new(uri)
    } else {
        if uri.starts_with('/') {
            format!("/user{}", uri).into()
        } else {
            format!("/user/{}", uri).into()
        }
    };
    let mut txn = DBTransaction::new(&ctx);
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
    let json = json.pointer_mut(stripped).unwrap().take();
    json
}

pub fn write_json(value: Value, txn: &mut DBTransaction) -> Result<(), String> {
    let map = json_processing::json_to_map(value);
    for (key, value) in map {
        txn.write(&key, value.to_string().into())?;
    }
    Ok(())
}
