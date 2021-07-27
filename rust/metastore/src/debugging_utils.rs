use std::fmt::Write;

use crate::btreemap_kv_backend::MutBTreeMap;


// Prints the database to stdout
pub fn printdb(db: &MutBTreeMap) -> String {
    let mut str: String = String::new();


    for (key, value) in db.iter() {
        println!("Key: {}", key.as_str());
        str.write_fmt(format_args!(
            "{}: ({}) {}\n",
            key.to_string(),
            value.0,
            value.1
        ))
        .unwrap();
    }
    str
}
