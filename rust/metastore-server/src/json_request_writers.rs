mod json_data_model_tests;
mod json_processing;

use serde_json::{Value as JSONValue, Value};

use metastore::object_path::ObjectPath;
use metastore::rwtransaction_wrapper::ReplicatedTxn;
use metastore::{DatabaseInterface, DbContext, LockDataRef, SelfContainedDb, TypedValue};

pub fn read_json_request_txn(uri: &str, ctx: &SelfContainedDb, txn: LockDataRef) -> JSONValue {
    let objpath = prettify_json_path(uri);

    let ret = ctx.serve_range_read(txn, &objpath).0.unwrap().unwrap();
    log::debug!("Reading {} using txn {}", objpath.as_str(), txn.id,);

    let mut json = JSONValue::Null;
    for row in ret {
        let path: Vec<&str> = row.0.split_parts().collect();
        json_processing::create_materialized_path(&mut json, &path, row.1.into_inner().1);
    }

    let stripped = match objpath.as_str().strip_suffix('/') {
        Some(x) => x,
        None => objpath.as_str(),
    };
    match json.pointer_mut(stripped) {
        Some(a) => a.take(),
        None => JSONValue::Null,
    }
}
// todo: modify function to handle transactions
pub fn read_json_request(uri: &str, ctx: &DbContext) -> JSONValue {
    unimplemented!()
    // let mut txn = ReplicatedTxn::new(&ctx);
    //
    // txn.commit();
    // json
}

fn prettify_json_path(uri: &str) -> ObjectPath {
    let mut uri = uri.to_string();
    if !uri.starts_with('/') {
        uri = format!("/{}", uri);
    }

    if !uri.ends_with('/') {
        uri.push('/');
    }
    ObjectPath::from(uri)
}

pub fn write_json(value: Value, txn: &mut ReplicatedTxn) -> Result<(), String> {
    // todo: have to delete all existing values before inserting the new one.
    // or else, we might get data corruption
    /*
    /test/ = 5             -
    /test/a = 3            +
    /test/b = true         +

    ^^^ this is corrupted data. What JSON object does /test/ represent?
    We can either have all (+) rows XOR all (-) rows.
     */
    let map = json_processing::json_to_map(value);
    for (key, value) in map {
        txn.write(&key, value.to_string().into())?;
    }
    Ok(())
}

pub fn write_json_txnid(
    value: Value,
    txn: LockDataRef,
    db: &SelfContainedDb,
    path: &str,
) -> Result<(), String> {
    // todo: have to delete all existing values before inserting the new one.
    // or else, we might get data corruption
    /*
    /test/ = 5             -
    /test/a = 3            +
    /test/b = true         +

    ^^^ this is corrupted data. What JSON object does /test/ represent?
    We can either have all (+) rows XOR all (-) rows.
     */
    let map = json_processing::json_to_map(value);
    for (key, value) in map {
        let key_absolute = ObjectPath::from(path.to_owned() + key.as_str());
        let value: TypedValue = value.to_string().into();
        log::debug!("Wrote {} {}", key_absolute.as_str(), value.as_str());
        db.serve_write(txn, &key_absolute, value).unwrap_all();
    }
    Ok(())
}
