use std::fmt::Write;

use crate::btreemap_kv_backend::MutBTreeMap;
use crate::mvcc_manager::WriteIntent;

// Prints the database to stdout
pub fn printdb(db: &MutBTreeMap) -> String {
    let mut str: String = String::new();

    let wimapper = |a: &Option<WriteIntent>| -> String {
        match a {
            None => "None".to_string(),
            Some(wi) => wi.associated_transaction.timestamp.to_string(),
        }
    };

    for (key, value) in db.iter() {
        println!("Key: {}", key.as_str());
        str.write_fmt(format_args!(
            "{}: {{beg: {}, end: {}, lr: {}, wi: {}; {}}}\n",
            key.to_string(),
            value.0.begin_ts.to_string(),
            value.0.end_ts.to_string(),
            value.0.last_read.to_string(),
            wimapper(&value.0.cur_write_intent),
            value.1
        ))
        .unwrap();
    }
    str
}
