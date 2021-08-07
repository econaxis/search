use crate::rwtransaction_wrapper::{LockDataRef, ValueWithMVCC};
use crate::object_path::ObjectPath;
use std::borrow::Cow;

trait Extension {
    fn serve_read(&self, txn: LockDataRef, key: &ObjectPath) -> Result<ValueWithMVCC, String>;
    fn serve_range_read(&self, txn: LockDataRef, key: &ObjectPath) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String>;
    fn serve_write(&self, txn: LockDataRef, key: &ObjectPath, value: Cow<str>) -> Result<(), String>;
    fn commit(&self, txn: LockDataRef) -> Result<(), String>;
}