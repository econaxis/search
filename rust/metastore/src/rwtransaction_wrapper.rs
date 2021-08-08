use std::borrow::Cow;

pub mod json_request_writers;
mod mvcc_manager;

use crate::object_path::ObjectPath;
use crate::DbContext;
pub use mvcc_manager::IntentMap;
pub use mvcc_manager::MVCCMetadata;
pub use mvcc_manager::btreemap_kv_backend::MutBTreeMap;
use mvcc_manager::WriteIntentStatus;
pub use mvcc_manager::{LockDataRef, UnlockedWritableMVCC, ValueWithMVCC};

pub struct DBTransaction<'a> {
    ctx: &'a DbContext,
    main: Transaction,
}


// equivalent to RWTransactionWrapper but without the borrowing reference.
pub struct Transaction {
    txn: LockDataRef,
    log: WalTxn,
    committed: bool,
}

impl Transaction {
    pub fn new_with_time(ctx: &DbContext, time: Timestamp) -> Self {
        let txn = ctx.transaction_map.make_write_txn_with_time(time);

        Self {
            txn,
            log: WalTxn::new(txn.timestamp),
            committed: false,
        }
    }
    pub fn abort(&mut self, ctx: &DbContext) {
        if !self.committed {
            let prev = ctx
                .transaction_map
                .set_txn_status(self.txn, WriteIntentStatus::Aborted);
            assert_eq!(prev, Some(WriteIntentStatus::Pending));
        }
    }
    pub fn read_range_owned(
        &mut self, ctx: &DbContext,
        key: &ObjectPath,
    ) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let (lock, mut range) = ctx.db.range_with_lock(key.get_prefix_ranges(), self.txn.timestamp);
        let mut keys: Vec<_> = range.map(|a| (a.0.clone(), a.1.clone())).collect();
        std::mem::drop(lock);

        let mut keys1 = Vec::new();

        for key in keys {
            match self.read_mvcc(ctx, &key.0) {
                Ok(kv2) => {
                    keys1.push((key.0, kv2));
                }
                // These are the acceptable errors
                Err(a) if a.starts_with("ValueNotFound") => {}
                Err(a) if &a == "Other(\"Read value doesn't exist\")" => { println!("range err (ignore) {} txn {}", a, self.txn.id) }
                Err(err) => {
                    return Err(err);
                }
            }
        };

        Ok(keys1)
    }
    pub fn read_mvcc(&mut self, ctx: &DbContext, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let ret = mvcc_manager::read(ctx, &key, self.txn).map_err(|a| <ReadError as Into<String>>::into(a))?;

        self.log.log_read(ObjectPath::from(key.to_owned()), ret.clone());
        Ok(ret)
    }

    pub fn write(
        &mut self, ctx: &DbContext,
        key: &ObjectPath,
        value: Cow<str>,
    ) -> Result<(), String> {
        let ret = mvcc_manager::update(ctx, key, value.clone().into_owned(), self.txn)?;

        self.log.log_write(key.clone(), ret);
        Ok(())
    }

    pub fn commit(&mut self, ctx: &DbContext) {
        let mut placeholder = WalTxn::new(self.txn.timestamp);
        std::mem::swap(&mut placeholder, &mut self.log);
        ctx.wallog.store(placeholder).unwrap();
        self.committed = true;
        let prev = ctx
            .transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Committed);
        // assert_eq!(prev, Some(WriteIntentStatus::Pending));
    }
}

use crate::timestamp::Timestamp;
use crate::wal_watcher::{WalTxn, WalStorer};
use serde_json::to_string;
use crate::rwtransaction_wrapper::mvcc_manager::ReadError;

impl<'a> DBTransaction<'a> {
    pub fn get_txn(&self) -> &LockDataRef {
        &self.main.txn
    }
    pub fn new_with_time(ctx: &'a DbContext, time: Timestamp) -> Self {
        let ret = Self {
            main: Transaction::new_with_time(ctx, time),
            ctx,
        };
        ctx.replicator().new_with_time(&ret.get_txn());
        ret
    }
}

use lazy_static::lazy_static;
use crate::replicated_slave::ReplicatedDatabase;
use std::time::Duration;

impl<'a> DBTransaction<'a> {
    pub fn read_range_owned(
        &mut self,
        key: &ObjectPath,
    ) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let res = self.ctx.replicator().serve_range_read(*self.get_txn(), key)?;
        let res1 = self.main.read_range_owned(self.ctx, key)?;

        assert_eq!(res.len(), res1.len());
        res.iter().zip(res1.iter()).for_each(|(a, b)| {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1.as_inner().1, b.1.as_inner().1);
        });

        Ok(res1)
    }
    pub fn read_mvcc(&mut self, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let myres = self.main.read_mvcc(self.ctx, key)?;

        let res = self.ctx.replicator().serve_read(*self.get_txn(), key)?;
        assert_eq!(res.as_inner().1, myres.as_inner().1);

        Ok(myres)
    }
    pub fn read(&mut self, key: &ObjectPath) -> Result<String, String> {
        self.read_mvcc(key).map(|a| a.into_inner().1)
    }
    pub fn write(
        &mut self,
        key: &ObjectPath,
        value: Cow<str>,
    ) -> Result<(), String> {
        let res = self.ctx.replicator().serve_write(*self.get_txn(), key, value.clone()).map_err(|a| format!("replicator error {}", a));
        let res1 = self.main.write(self.ctx, key, value);

        if res.is_ok() != res1.is_ok() {
            println!("nonmatching write results: {:?} {:?}", res, res1);
        }
        return res.and(res1);
    }
    pub fn commit(mut self) {
        let _l = self.ctx.transaction_map.begin_atomic();
        let _l1 = self.ctx.replicator().begin_atomic_commit();
        let t = *self.get_txn();
        let two = self.main.commit(self.ctx);
        let one = self.ctx.replicator().commit(t);
    }
    pub fn abort(mut self) {
        let _l = self.ctx.transaction_map.begin_atomic();
        self.ctx.replicator().abort(*self.get_txn());
        self.main.abort(self.ctx)
    }

    pub fn new(ctx: &'a DbContext) -> Self {
        Self::new_with_time(ctx, Timestamp::now())
    }
}


// Static functions for convenience (auto transaction)
pub mod auto_commit {
    use crate::rwtransaction_wrapper::{ValueWithMVCC, DBTransaction};
    use std::borrow::Cow;
    use crate::DbContext;
    use crate::object_path::ObjectPath;

    pub fn read(db: &DbContext, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let mut txn = DBTransaction::new(db);
        let ret = txn.read_mvcc(key);
        txn.commit();
        ret
    }

    pub fn read_range(db: &DbContext, key: &ObjectPath) -> Vec<(ObjectPath, ValueWithMVCC)> {
        let mut txn = DBTransaction::new(db);
        txn.read_range_owned(key).unwrap()
    }

    pub fn write(db: &DbContext, key: &ObjectPath, value: Cow<str>) {
        let mut txn = DBTransaction::new(db);
        txn.write(key, value).unwrap();
        txn.commit();
    }
}


#[cfg(test)]
mod tests {
    use crate::{create_replicated_context};

    use super::*;
    use crate::wal_watcher::WalLoader;
    use std::borrow::Cow::Borrowed;

    #[test]
    fn test1() {
        let ctx = create_replicated_context();
        let mut txn = DBTransaction::new(&ctx);
        let key = "test".into();
        txn.write(&key, "fdsvc".into());
        txn.read(&key);
        txn.commit();
    }

    #[test]
    fn test2() {
        let ctx = create_replicated_context();
        let ctx = &ctx;

        let (a0, a1, a2, a3): (ObjectPath, ObjectPath, ObjectPath, ObjectPath) =
            ("key0".into(), "key1".into(), "key2".into(), "key3".into());

        let mut txn0 = DBTransaction::new(ctx);
        let mut txn1 = DBTransaction::new(ctx);
        let mut txn2 = DBTransaction::new(ctx);
        let mut txn3 = DBTransaction::new(ctx);

        txn0.write(&a0, "key0value".into());
        txn0.commit();

        txn1.write(&a1, Cow::from("key1value"));
        txn2.write(&a2, Cow::from("key2value"));
        txn3.write(&a3, Cow::from("key3value"));

        assert_eq!(txn1.read(&a0).unwrap().as_str(), "key0value");
        txn1.commit();
        assert_eq!(txn2.read(&a0).unwrap().as_str(), "key0value");
        assert_eq!(txn2.read(&a1).unwrap().as_str(), "key1value");
        assert_matches!(txn2.read(&a3), Err(..));
        assert_matches!(txn3.read(&a1).unwrap().as_str(), "key1value");
        assert_matches!(txn3.read(&a2), Err(..));

        txn2.commit();
        assert_eq!(txn3.read(&a0), Ok("key0value".to_string()));
        assert_eq!(txn3.read(&a1), Ok("key1value".to_string()));
        assert_eq!(txn3.read(&a2), Ok("key2value".to_string()));

        txn3.commit();

        let blank = create_replicated_context();
        ctx.wallog.apply(&blank);

        assert_eq!(blank.db.printdb(), ctx.db.printdb());
    }

    #[test]
    fn test3() {
        let ctx = create_replicated_context();
        let ctx = &ctx;

        let (a, b): (ObjectPath, ObjectPath) = ("key0".into(), "key1".into());

        let mut txn0 = DBTransaction::new(ctx);
        let mut txn1 = DBTransaction::new(ctx);

        txn0.write(&a, "key0value".into()).unwrap();

        txn1.write(&b, Cow::from("key1value")).unwrap();
        assert_matches!(txn1.read(&a), Err(_err));

        txn0.commit();
        txn1.commit();
    }
}
