mod mvcc_manager;

use crate::object_path::ObjectPath;
use crate::DbContext;
pub use mvcc_manager::btreemap_kv_backend::MutBTreeMap;
pub use mvcc_manager::IntentMap;
pub use mvcc_manager::MVCCMetadata;
use mvcc_manager::WriteIntentStatus;
pub use mvcc_manager::{LockDataRef, UnlockedWritableMVCC, ValueWithMVCC};

use log::debug;

pub struct ReplicatedTxn<'a> {
    ctx: &'a DbContext,
    main: Transaction,
    done: bool,
}

// equivalent to RWTransactionWrapper but without the borrowing reference.
pub struct Transaction {
    pub(crate) txn: LockDataRef,
    written_kv: Vec<(ObjectPath, ValueWithMVCC)>,
    log: WalTxn,
}

impl Transaction {
    fn new(txn: LockDataRef) -> Self {
        Self {
            txn,
            log: WalTxn::new(txn.timestamp),
            written_kv: Vec::new(),
        }
    }
    pub fn new_with_time_id(ctx: &DbContext, time: Timestamp, id: u64) -> Self {
        let txn = ctx.transaction_map.make_write_txn_with_time(time, id);
        Self::new(txn)
    }
    pub fn new_with_time(ctx: &DbContext, time: Timestamp) -> Self {
        let txn = ctx
            .transaction_map
            .make_write_txn_with_time(time, Timestamp::now().0);
        Self::new(txn)
    }
    pub fn abort(&mut self, ctx: &DbContext) {
        ctx.transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Aborted)
            .unwrap();
    }
    pub fn read_range_owned(
        &mut self,
        ctx: &DbContext,
        key: &ObjectPath,
    ) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let (_lock, range) = ctx
            .db
            .range_with_lock(key.get_prefix_ranges(), self.txn.timestamp);

        let mut keys1 = Vec::new();
        for (key, value_ptr) in range {
            coz::progress!();
            match mvcc_manager::read_reference(ctx, value_ptr, self.txn) {
                Ok(kv2) => {
                    keys1.push((key.clone(), kv2));
                }
                // These are the acceptable errors
                Err(ReadError::ValueNotFound) => {}
                Err(ReadError::Other(a)) if &a == "Other(\"Read value doesn't exist\")" => {}
                Err(err) => {
                    return Err(format!("{:?}", err));
                }
            }
        }

        Ok(keys1)
    }
    pub fn read_mvcc(
        &mut self,
        ctx: &DbContext,
        key: &ObjectPath,
    ) -> Result<ValueWithMVCC, String> {
        let ret =
            mvcc_manager::read(ctx, key, self.txn).map_err(<ReadError as Into<String>>::into)?;
        Ok(ret)
    }

    pub fn write(
        &mut self,
        ctx: &DbContext,
        key: &ObjectPath,
        value: TypedValue,
    ) -> Result<(), String> {
        use crate::rpc_handler::DatabaseInterface;
        mvcc_manager::update(ctx, key, value.clone(), self.txn)?;

        (&mut self.log).serve_write(self.txn, key, value);
        Ok(())
    }

    pub fn commit(&mut self, ctx: &DbContext) -> Result<(), String> {
        let kv = &mut self.written_kv;
        let txn = self.txn;
        kv.drain(..).for_each(|a| {
            // As optimization, clear the committed intents
            ctx.db.get_mut(&a.0).map(|a| a.clear_committed_intent(txn));
        });
        let mut placeholder = WalTxn::new(self.txn.timestamp);
        std::mem::swap(&mut placeholder, &mut self.log);
        ctx.wallog.store(placeholder).unwrap();

        ctx.transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Committed)
    }
}

use crate::timestamp::Timestamp;
use crate::wal_watcher::{WalStorer, WalTxn};

use crate::rpc_handler::DatabaseInterface;
pub use crate::rwtransaction_wrapper::mvcc_manager::{ReadError, TypedValue};
use rand::Rng;

impl<'a> ReplicatedTxn<'a> {
    pub fn get_txn(&self) -> &LockDataRef {
        &self.main.txn
    }
    pub fn new_with_time(ctx: &'a DbContext, time: Timestamp) -> Self {
        let ret = Self {
            main: Transaction::new_with_time(ctx, time),
            ctx,
            done: false,
        };
        ctx.replicator().new_transaction(ret.get_txn());
        ret
    }
}

impl<'a> ReplicatedTxn<'a> {
    pub fn read_range_owned(
        &mut self,
        key: &ObjectPath,
    ) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let res1 = self.main.read_range_owned(self.ctx, key)?;

        // We don't need to send reads to the replicators for performance reasons.
        // let res = self.ctx.replicator().serve_range_read(*self.get_txn(), key)??;
        Ok(res1)
    }
    pub fn read_mvcc(&mut self, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let myres = self.main.read_mvcc(self.ctx, key)?;

        // Every X times, do a replicator read, just to make sure everything matches.
        if probabilistic_should_quorum_read() {
            let res = self.ctx.replicator().serve_read(*self.get_txn(), key)??;
            if res.as_inner().1 != myres.as_inner().1 {
                return Err("Replicator reads don't match".to_string());
            }
        }

        Ok(myres)
    }
    pub fn read(&mut self, key: &ObjectPath) -> Result<TypedValue, String> {
        self.read_mvcc(key).map(|a| a.into_inner().1)
    }
    pub fn write(&mut self, key: &ObjectPath, value: TypedValue) -> Result<(), String> {
        let res1 = self
            .main
            .write(self.ctx, key, value.clone())
            .map_err(|a| format!("main error {}", a));
        let res = self
            .ctx
            .replicator()
            .serve_write(*self.get_txn(), key, value)?
            .map_err(|a| format!("replicator error {}", a));

        if res.is_ok() != res1.is_ok() {
            debug!(
                "nonmatching write results: {:?} {:?} {}",
                res,
                res1,
                self.get_txn().id
            );
            // Must abort writes, todo!
        }
        res.and(res1)
    }
    pub fn commit(mut self) -> Result<(), String> {
        // todo fix atomic commit section with actual two-phase commit.
        let t = *self.get_txn();
        self.ctx.replicator().commit(t)?;
        self.main.commit(self.ctx)?;
        self.done = true;
        Ok(())
    }
    pub fn abort(&mut self) {
        self.ctx.replicator().abort(*self.get_txn());
        self.main.abort(self.ctx);
        self.done = true;
    }

    pub fn new(ctx: &'a DbContext) -> Self {
        Self::new_with_time(ctx, Timestamp::now())
    }
}

fn probabilistic_should_quorum_read() -> bool {
    use rand::thread_rng;
    thread_rng().gen_bool(0.005)
}

// Static functions for convenience (auto-generates a transaction)
#[allow(unused)]
pub mod auto_commit {
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::mvcc_manager::TypedValue;
    use crate::rwtransaction_wrapper::ValueWithMVCC;
    use crate::{DbContext, ReplicatedTxn};

    pub fn read(db: &DbContext, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let mut txn = ReplicatedTxn::new(db);
        let ret = txn.read_mvcc(key);
        txn.commit().unwrap();
        ret
    }

    pub fn write(db: &DbContext, key: &ObjectPath, value: TypedValue) {
        let mut txn = ReplicatedTxn::new(db);
        txn.write(key, value).unwrap();
        txn.commit().unwrap();
    }
}

impl Drop for ReplicatedTxn<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.abort()
        } else {
            let status = self.ctx.transaction_map.get_by_ref(&self.main.txn);
            debug_assert_matches!(
                status.unwrap().0,
                WriteIntentStatus::Committed | WriteIntentStatus::Aborted
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_watcher::WalLoader;

    use crate::db;
    use crate::db_context::{create_empty_context, create_replicated_context};

    #[test]
    #[should_panic]
    fn cant_commit_abort_twice() {
        let db = db!();
        let mut t = ReplicatedTxn::new(&db);
        t.abort();
        t.commit();
    }

    #[test]
    #[ignore]
    fn regression_phantom() {
        /*
        Check for phantom bug. Previously when we insert a key, we check that the keys to the left/right of that new key has a lower read_timestamp.
        This is called next-key locking. This prevents phantoms from occuring. For example, doing a range read on /test/a at Timestamp `a` should lock
        that whole range "/test/a". Thus, a transaction with an older timestamp cannot go in and write to that range anymore.

        However, our current implementation of next key locking is primitive. It does not take into account that next/previous keys could be pending instead of
        committed value. Therefore, I can sneak a phantom in by doing the following (which should be illegal): */
        let db = db!("/test/a" = "a", "/other/c" = "c");
        let mut t1 = ReplicatedTxn::new(&db);
        let mut t2 = ReplicatedTxn::new(&db);
        let mut t3 = ReplicatedTxn::new(&db);

        dbg!(t3.read_range_owned(&"/test/a/".into()).unwrap());
        dbg!(t1.write(&"/test/a/1".into(), "a".into()));

        // Not fixed yet, there should be error here.
        // When it locks the next key, it'd lock the uncommitted value /test/a/1 instead of the actual /test/a value.
        // This is a bug.
        assert_matches!(t2.write(&"/test/a/2".into(), "a".into()), Err(..));
        t1.abort();
        t2.commit();

        let mut t4 = ReplicatedTxn::new(&db);
        println!("{:?}", t4.read_range_owned(&"/".into()));
        println!("{}", db.db.printdb());
    }

    #[test]
    fn check_phantom() {
        let db = db!(
            "/test/1" = "1",
            "/test/2" = "2",
            "/test/5" = "5",
            "/user/0" = "0"
        );
        let mut wtxn = ReplicatedTxn::new(&db);
        let mut txn = ReplicatedTxn::new(&db);

        let _range = txn.read_range_owned(&"/test/".into()).unwrap();
        assert_matches!(wtxn.write(&"/test/6".into(), "6".into()), Err(..));
        assert_matches!(wtxn.write(&"/test/0".into(), "4".into()), Err(..));
        assert_matches!(wtxn.write(&"/test/4".into(), "4".into()), Err(..));
        assert_matches!(wtxn.write(&"/user/4".into(), "4".into()), Ok(..));
        wtxn.commit().unwrap();
    }

    #[test]
    fn test_delete() {
        let db = db!("k" = "v");
        let mut t = ReplicatedTxn::new(&db);
        t.write(&"k".into(), TypedValue::Deleted).unwrap();
        t.commit().unwrap();

        assert_matches!(auto_commit::read(&db, &"k".into()), Err(..));
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

        let _range = txn1.read_range_owned(&"/test/".into()).unwrap();
        let _range = txn2.read_range_owned(&"/test/".into()).unwrap();

        assert_matches!(txn1.write(&"/test/3".into(), "3".into()), Err(..));
        assert_matches!(txn2.write(&"/test/4".into(), "3".into()), Ok(..));

        txn1.commit();
        txn2.commit();
        println!("{}", db.db.printdb());
    }

    #[test]
    fn test1() {
        let ctx = create_replicated_context();
        let mut txn = ReplicatedTxn::new(&ctx);
        let key = "test".into();
        txn.write(&key, "fdsvc".into());
        txn.read(&key);
        txn.commit().unwrap();
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
        txn0.commit().unwrap();

        txn1.write(&a1, TypedValue::from("key1value")).unwrap();
        txn2.write(&a2, TypedValue::from("key2value")).unwrap();
        txn3.write(&a3, TypedValue::from("key3value")).unwrap();

        assert_eq!(txn1.read(&a0).unwrap().as_str(), "key0value");
        txn1.commit().unwrap();
        assert_eq!(txn2.read(&a0).unwrap().as_str(), "key0value");
        assert_eq!(txn2.read(&a1).unwrap().as_str(), "key1value");
        assert_matches!(txn2.read(&a3), Err(..));
        assert_matches!(txn3.read(&a1).unwrap().as_str(), "key1value");
        assert_matches!(txn3.read(&a2), Err(..));

        txn2.commit().unwrap();
        assert_eq!(txn3.read(&a0), Ok("key0value".into()));
        assert_eq!(txn3.read(&a1), Ok("key1value".into()));
        assert_eq!(txn3.read(&a2), Ok("key2value".into()));

        txn3.commit().unwrap();
    }

    #[test]
    fn test3() {
        let ctx = create_replicated_context();
        let ctx = &ctx;

        let (a, b): (ObjectPath, ObjectPath) = ("key0".into(), "key1".into());

        let mut txn0 = ReplicatedTxn::new(ctx);
        let mut txn1 = ReplicatedTxn::new(ctx);

        txn0.write(&a, "key0value".into()).unwrap();

        txn1.write(&b, TypedValue::from("key1value")).unwrap();
        assert_matches!(txn1.read(&a), Err(_err));

        txn0.commit();
        txn1.commit();
    }

    #[test]
    pub fn independent_writes_dont_block() {
        use crate::db;
        // tests that locks only acquired for individual key/value operations, not for the whole transaction
        let db = db!("k" = "v");
        let mut t1 = ReplicatedTxn::new(&db);
        let mut t2 = ReplicatedTxn::new(&db);
        t1.write(&"k".into(), "v2".into());
        match t2.write(&"k".into(), "v3".into()) {
            Err(x) => println!("(good) expected error: {}", x),
            _ => panic!("should've errored"),
        }
        match t2.read(&"k".into()) {
            Err(x) => println!("(good) expected error: {}", x),
            _ => panic!("should've errored"),
        }
        match t2.write(&"k1".into(), "v3".into()) {
            Ok(x) => println!("(good) expected value: {:?}", x),
            _ => panic!("should've been ok"),
        }
        t1.commit();
        t2.commit();
    }
}
