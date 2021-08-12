use std::borrow::Cow;

mod mvcc_manager;

use crate::object_path::ObjectPath;
use crate::DbContext;
pub use mvcc_manager::IntentMap;
pub use mvcc_manager::MVCCMetadata;
pub use mvcc_manager::btreemap_kv_backend::MutBTreeMap;
use mvcc_manager::WriteIntentStatus;
pub use mvcc_manager::{LockDataRef, UnlockedWritableMVCC, ValueWithMVCC};

use log::debug;

pub struct ReplicatedTxn<'a> {
    ctx: &'a DbContext,
    main: Transaction,
    committed: bool,
}


// equivalent to RWTransactionWrapper but without the borrowing reference.
pub struct Transaction {
    txn: LockDataRef,
    log: WalTxn,
}

impl Transaction {
    pub fn new_with_time(ctx: &DbContext, time: Timestamp) -> Self {
        let txn = ctx.transaction_map.make_write_txn_with_time(time);

        Self {
            txn,
            log: WalTxn::new(txn.timestamp),
        }
    }
    pub fn abort(&mut self, ctx: &DbContext) {
        let prev = ctx
            .transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Aborted).unwrap();
    }
    pub fn read_range_owned(
        &mut self, ctx: &DbContext,
        key: &ObjectPath,
    ) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let (lock, range) = ctx.db.range_with_lock(key.get_prefix_ranges(), self.txn.timestamp);
        let keys: Vec<_> = range.map(|a| (a.0.clone(), a.1.clone())).collect();
        std::mem::drop(lock);

        let mut keys1 = Vec::new();

        for key in keys {
            match self.read_mvcc(ctx, &key.0) {
                Ok(kv2) => {
                    keys1.push((key.0, kv2));
                }
                // These are the acceptable errors
                Err(a) if a.starts_with("ValueNotFound") => {}
                Err(a) if &a == "Other(\"Read value doesn't exist\")" => { }
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

    pub fn commit(&mut self, ctx: &DbContext) -> Result<(), String> {
        let mut placeholder = WalTxn::new(self.txn.timestamp);
        std::mem::swap(&mut placeholder, &mut self.log);
        ctx.wallog.store(placeholder).unwrap();
        ctx
            .transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Committed)
    }
}

use crate::timestamp::Timestamp;
use crate::wal_watcher::{WalTxn, WalStorer};

use crate::rwtransaction_wrapper::mvcc_manager::ReadError;
use crate::file_debugger::print_to_file;

impl<'a> ReplicatedTxn<'a> {
    pub fn get_txn(&self) -> &LockDataRef {
        &self.main.txn
    }
    pub fn new_with_time(ctx: &'a DbContext, time: Timestamp) -> Self {
        let ret = Self {
            main: Transaction::new_with_time(ctx, time),
            ctx,
            committed: false
        };
        ctx.replicator().new_with_time(&ret.get_txn());
        ret
    }
}


impl<'a> ReplicatedTxn<'a> {
    pub fn read_range_owned(
        &mut self,
        key: &ObjectPath,
    ) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let res = self.ctx.replicator().serve_range_read(*self.get_txn(), key)?;
        let res1 = self.main.read_range_owned(self.ctx, key)?;

        if res.len() != res1.len() {
            eprintln!("Replicator read doesn't match");
            return Err("Replicator read doesn't match".to_string());
            // print_to_file(format_args!("{:?}\n{:?}\n{:?}\n{:?}", res, res1, self.ctx.transaction_map, self.ctx.replicator().debug_txnmap()));
            // panic!()
        } else {
            res.iter().zip(res1.iter()).for_each(|(a, b)| {
                assert_eq!(a.0, b.0);
                assert_eq!(a.1.as_inner().1, b.1.as_inner().1);
            });
        }

        Ok(res1)
    }
    pub fn read_mvcc(&mut self, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let res = self.ctx.replicator().serve_read(*self.get_txn(), key)?;
        let myres = self.main.read_mvcc(self.ctx, key)?;
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
        let res1 = self.main.write(self.ctx, key, value.clone());
        let res = self.ctx.replicator().serve_write(*self.get_txn(), key, value.clone()).map_err(|a| format!("replicator error {}", a));

        if res.is_ok() != res1.is_ok() {
            debug!("nonmatching write results: {:?} {:?} {}", res, res1, self.get_txn().id);
            // Must abort writes, todo!
        }
        return res.and(res1);
    }
    pub fn commit(mut self) -> Result<(), String> {
        // todo fix atomic commit section with actual two-phase commit.
        let t = *self.get_txn();
        let _two = self.main.commit(self.ctx)?;
        let _one = self.ctx.replicator().commit(t);
        self.committed = true;
        Ok(())
    }
    pub fn abort(&mut self) {
        self.ctx.replicator().abort(*self.get_txn());
        self.main.abort(self.ctx)
    }

    pub fn new(ctx: &'a DbContext) -> Self {
        Self::new_with_time(ctx, Timestamp::now())
    }
}


// Static functions for convenience (auto-generates a transaction)
pub mod auto_commit {
    use crate::rwtransaction_wrapper::{ValueWithMVCC, ReplicatedTxn};
    use std::borrow::Cow;
    use crate::DbContext;
    use crate::object_path::ObjectPath;

    pub fn read(db: &DbContext, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let mut txn = ReplicatedTxn::new(db);
        let ret = txn.read_mvcc(key);
        txn.commit();
        ret
    }

    pub fn read_range(db: &DbContext, key: &ObjectPath) -> Vec<(ObjectPath, ValueWithMVCC)> {
        let mut txn = ReplicatedTxn::new(db);
        txn.read_range_owned(key).unwrap()
    }

    pub fn write(db: &DbContext, key: &ObjectPath, value: Cow<str>) {
        let mut txn = ReplicatedTxn::new(db);
        txn.write(key, value).unwrap();
        txn.commit();
    }
}


impl Drop for ReplicatedTxn<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.abort()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_watcher::WalLoader;
    use std::borrow::Cow::Borrowed;
    use crate::db_context::{create_replicated_context};
    use crate::db;


    #[test]
    fn check_phantom() {
        let db = db!("/test/1" = "1", "/test/2" = "2", "/test/5" = "5", "/user/0" = "0");
        let mut wtxn = ReplicatedTxn::new(&db);
        let mut txn = ReplicatedTxn::new(&db);

        let range = txn.read_range_owned(&"/test/".into()).unwrap();
        assert_matches!(wtxn.write(&"/test/6".into(), "6".into()), Err(..));
        assert_matches!(wtxn.write(&"/test/0".into(), "4".into()), Err(..));
        assert_matches!(wtxn.write(&"/test/4".into(), "4".into()), Err(..));
        assert_matches!(wtxn.write(&"/user/4".into(), "4".into()), Ok(..));
    }
    #[test]
    fn check_phantom2() {
        let db = db!("/test/1" = "1", "/test/2" = "2", "/test/5" = "5", "/user/0" = "0");
        let mut txn1 = ReplicatedTxn::new(&db);
        let mut txn2 = ReplicatedTxn::new(&db);

        let range = txn1.read_range_owned(&"/test/".into()).unwrap();
        let range = txn2.read_range_owned(&"/test/".into()).unwrap();

        txn1.write(&"/test/3".into(), "3".into()).unwrap();
        txn2.write(&"/test/4".into(), "3".into()).unwrap();
    }


    #[test]
    fn check_phantom3() {
        // regression test
        // phantom checks work OK normally, but when the database is empty, there's nothing to lock.
        // this's because we're locking tuples to either side of the newly inserted tuple.
        // therefore, phantoms slip through.
        let db = db!();
        let mut txn1 = ReplicatedTxn::new(&db);
        let mut txn2 = ReplicatedTxn::new(&db);

        let range = txn1.read_range_owned(&"/test/".into()).unwrap();
        let range = txn2.read_range_owned(&"/test/".into()).unwrap();

        assert_matches!(txn1.write(&"/test/3".into(), "3".into()), Err(..));
        assert_matches!(txn2.write(&"/test/4".into(), "3".into()), Ok(..));

        println!("{}", db.db.printdb());
    }


    #[test]
    fn test1() {
        let ctx = create_replicated_context();
        let mut txn = ReplicatedTxn::new(&ctx);
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

        let mut txn0 = ReplicatedTxn::new(ctx);
        let mut txn1 = ReplicatedTxn::new(ctx);
        let mut txn2 = ReplicatedTxn::new(ctx);
        let mut txn3 = ReplicatedTxn::new(ctx);

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

        let mut txn0 = ReplicatedTxn::new(ctx);
        let mut txn1 = ReplicatedTxn::new(ctx);

        txn0.write(&a, "key0value".into()).unwrap();

        txn1.write(&b, Cow::from("key1value")).unwrap();
        assert_matches!(txn1.read(&a), Err(_err));

        txn0.commit();
        txn1.commit();
    }

    #[test]
    pub fn independent_writes_dont_block() {
        use crate::wal_watcher::wal_check_consistency::check_func;
        use crate::db;
        // tests that locks only acquired for individual key/value operations, not for the whole transaction
        let db = db!("k" = "v");
        let mut t1 = ReplicatedTxn::new(&db);
        let mut t2 = ReplicatedTxn::new(&db);
        t1.write(&"k".into(), "v2".into());
        match t2.write(&"k".into(), "v3".into()) {
            Err(x) => println!("(good) expected error: {}", x),
            _ => panic!("should've errored")
        }
        match t2.read(&"k".into()) {
            Err(x) => println!("(good) expected error: {}", x),
            _ => panic!("should've errored")
        }
        match t2.write(&"k1".into(), "v3".into()) {
            Ok(x) => println!("(good) expected value: {:?}", x),
            _ => panic!("should've been ok")
        }
        t1.commit();
        t2.commit();
        check_func(&db);
    }
}
