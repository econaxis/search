use std::borrow::{Cow};

mod mvcc_manager;
pub mod json_request_writers;

use crate::{DbContext};
use mvcc_manager::LockDataRef;
use mvcc_manager::WriteIntentStatus;
use crate::object_path::ObjectPath;
pub use mvcc_manager::ValueWithMVCC;
pub use mvcc_manager::MVCCMetadata;
pub use mvcc_manager::IntentMap;
pub use mvcc_manager::MutBTreeMap;



pub struct RWTransactionWrapper<'a> {
    ctx: &'a DbContext,
    txn: LockDataRef,
    committed: bool,
}

#[cfg(test)]
use crate::timestamp::Timestamp;
#[cfg(test)]
impl<'a> RWTransactionWrapper<'a> {

    pub fn new_with_time(ctx: &'a DbContext, time: Timestamp) -> Self {
        let txn = ctx.transaction_map.make_write_txn_with_time(time);

        Self {
            ctx,
            txn,
            committed: false,
        }
    }
}

impl<'a> RWTransactionWrapper<'a> {
    pub fn read_range(&mut self, key: &ObjectPath) -> impl Iterator<Item=(&ObjectPath, &ValueWithMVCC)> {
        let range = key.get_prefix_ranges();
        self.ctx.db.range(range)
    }
    #[must_use]
    pub fn read(&mut self, key: Cow<str>) -> Result<&'a str, String> {
        let key = match key {
            Cow::Borrowed(a) => ObjectPath::from(a),
            Cow::Owned(a) => ObjectPath::from(a)
        };

        mvcc_manager::read(self.ctx, &key, self.txn)
    }
    #[must_use]
    pub fn write(&mut self, key: &ObjectPath, value: Cow<str>) -> Result<(), String> {
        mvcc_manager::update(self.ctx, &key, value.into_owned(), self.txn)
    }


    pub fn commit(mut self) {
        self.ctx
            .transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Committed);
        self.committed = true;
    }

    pub fn abort(&self) -> Result<(), String> {
        // todo: lookup the previous mvcc value and upgrade it back to the main database
        todo!()
    }

    pub fn new(ctx: &'a DbContext) -> Self {
        let txn = ctx.transaction_map.make_write_txn();

        Self {
            ctx,
            txn,
            committed: false,
        }
    }
}

impl Drop for RWTransactionWrapper<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // self.abort().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::Value;

    use crate::create_empty_context;

    use super::*;
    use std::borrow::Cow::Borrowed;

    #[test]
    fn test1() {
        let ctx = create_empty_context();
        let mut txn = RWTransactionWrapper::new(&ctx);
        let key = "test".into();
        txn.write(&key, "fdsvc".into());
        txn.read(key.as_str().into());
        txn.commit();
    }

    #[test]
    fn test2() {
        let ctx = create_empty_context();
        let ctx = &ctx;

        let (a0, a1, a2, a3): (ObjectPath, ObjectPath, ObjectPath, ObjectPath) = ("key0".into(), "key1".into(), "key2".into(), "key3".into());


        let mut txn0 = RWTransactionWrapper::new(ctx);
        let mut txn1 = RWTransactionWrapper::new(ctx);
        let mut txn2 = RWTransactionWrapper::new(ctx);
        let mut txn3 = RWTransactionWrapper::new(ctx);

        txn0.write(&a0, "key0value".into());
        txn0.commit();

        txn1.write(&a1, Cow::from("key1value"));
        txn2.write(&a2, Cow::from("key2value"));
        txn3.write(&a3, Cow::from("key3value"));

        assert_eq!(txn1.read(a0.as_str().into()), Ok("key0value"));
        txn1.commit();
        assert_eq!(txn2.read(a0.as_str().into()), Ok("key0value"));
        assert_eq!(txn2.read(a1.as_str().into()), Ok("key1value"));

        txn2.commit();
        assert_eq!(txn3.read(a0.as_str().into()), Ok("key0value"));
        assert_eq!(txn3.read(a1.as_str().into()), Ok("key1value"));
        assert_eq!(txn3.read(a2.as_str().into()), Ok("key2value"));

        txn3.commit();
    }

    #[test]
    fn test3() {
        let ctx = create_empty_context();
        let ctx = &ctx;

        let (a, b): (ObjectPath, ObjectPath) = ("key0".into(), "key1".into());


        let mut txn0 = RWTransactionWrapper::new(ctx);
        let mut txn1 = RWTransactionWrapper::new(ctx);

        txn0.write(&a, "key0value".into()).unwrap();

        txn1.write(&b, Cow::from("key1value")).unwrap();
        assert_matches!(txn1.read(Borrowed(a.as_str())), Err(_err));

        txn0.commit();
        txn1.commit();
    }
}
