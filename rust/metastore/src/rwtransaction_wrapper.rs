use std::borrow::{Cow};

mod mvcc_manager;
pub mod json_request_writers;

use crate::{DbContext};
use mvcc_manager::WriteIntentStatus;
use crate::object_path::ObjectPath;
pub use mvcc_manager::{ValueWithMVCC, UnlockedMVCC, LockDataRef};
pub use mvcc_manager::MVCCMetadata;
pub use mvcc_manager::IntentMap;
pub use mvcc_manager::MutBTreeMap;


pub struct RWTransactionWrapper<'a> {
    ctx: &'a DbContext,
    txn: LockDataRef,
    log: WalTxn,
    committed: bool,
}

use crate::timestamp::Timestamp;
use crate::wal_watcher::{WalTxn};

impl<'a> RWTransactionWrapper<'a> {
    pub fn get_txn(&self) -> &LockDataRef {
        &self.txn
    }
    pub fn new_with_time(ctx: &'a DbContext, time: Timestamp) -> Self {
        let txn = ctx.transaction_map.make_write_txn_with_time(time);

        Self {
            ctx,
            txn,
            log: WalTxn::new(txn.timestamp),
            committed: false,
        }
    }
}

impl<'a> RWTransactionWrapper<'a> {
    pub fn read_range_owned(&mut self, key: &ObjectPath) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let (lock, mut range) = self.ctx.db.range_with_lock(key.get_prefix_ranges());
        let mut keys: Vec<_> = range.map(|a| (a.0.clone(), a.1.clone())).collect();
        std::mem::drop(lock);

        if keys.iter().all(
            |kv|
                match self.read_mvcc(kv.0.as_cow_str()) {
                    Ok(kv2) => {
                        if kv.1.as_inner().1 == kv2.as_inner().1 {
                            true
                        } else {
                            println!("second read failed {:?} {:?}", kv.1, kv2);
                            false
                        }
                    }
                    Err(err) => {
                        println!("Error: {}", err);
                        false
                    }
                }) {
            Ok(Vec::new())
        } else {
            Err("Reread failed, different results".to_string())
        }
    }
    pub fn read_mvcc(&mut self, key: Cow<str>) -> Result<ValueWithMVCC, String> {
        let key = match key {
            Cow::Borrowed(a) => ObjectPath::from(a),
            Cow::Owned(a) => ObjectPath::from(a)
        };

        let ret = mvcc_manager::read(self.ctx, &key, self.txn)?;

        let val = ret.as_inner().1.parse::<u64>().unwrap();
        if val >= 100000 {
            let _a = true;
            unreachable!()
        };
        // println!("Val: {}", val);

        self.log.log_read(ObjectPath::from(key.to_owned()));
        Ok(ret)
    }

    pub fn read(&mut self, key: Cow<str>) -> Result<String, String> {
        self.read_mvcc(key).map(|a| a.into_inner().1)
    }

    pub fn write(&mut self, key: &ObjectPath, value: Cow<str>) -> Result<&'a ValueWithMVCC, String> {
        // TODO: if error then abort transaction
        // not urgent or necessary, because you cannot actually write the key (lower layer prevents this), but good for WAL so theres no errors.
        let ret = mvcc_manager::update(self.ctx, key, value.into_owned(), self.txn)?;

        self.log.log_write(key.clone(), ret.clone());
        Ok(ret)
    }


    pub fn commit(mut self) {
        self.ctx
            .transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Committed);

        let mut placeholder = WalTxn::new(self.txn.timestamp);
        std::mem::swap(&mut placeholder, &mut self.log);

        // TODO: fix concurrency errors
        // self.ctx.wallog.borrow_mut().store(placeholder);
        self.committed = true;
        println!("Txn {} committed by thread {:?}", self.txn.id, std::thread::current().id());
    }

    pub fn abort(&self) {
        // todo: lookup the previous mvcc values written and upgrade it back to the main database
        // store the write set
        self.ctx
            .transaction_map
            .set_txn_status(self.txn, WriteIntentStatus::Aborted);
        // println!("{:?} aborted", self.txn);
    }

    pub fn new(ctx: &'a DbContext) -> Self {
        let txn = ctx.transaction_map.make_write_txn();

        Self {
            ctx,
            txn,
            log: WalTxn::new(txn.timestamp),
            committed: false,
        }
    }
}

impl Drop for RWTransactionWrapper<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::create_empty_context;

    use super::*;
    use std::borrow::Cow::Borrowed;
    use crate::wal_watcher::WalLoader;

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

        assert_eq!(txn1.read(a0.as_str().into()).unwrap().as_str(), "key0value");
        txn1.commit();
        assert_eq!(txn2.read(a0.as_str().into()).unwrap().as_str(), "key0value");
        assert_eq!(txn2.read(a1.as_str().into()).unwrap().as_str(), "key1value");
        assert_matches!(txn2.read(a3.as_cow_str()), Err(..));
        assert_matches!(txn3.read(a1.as_cow_str()).unwrap().as_str(), "key1value");
        assert_matches!(txn3.read(a2.as_cow_str()), Err(..));

        txn2.commit();
        assert_eq!(txn3.read(a0.as_str().into()), Ok("key0value".to_string()));
        assert_eq!(txn3.read(a1.as_str().into()), Ok("key1value".to_string()));
        assert_eq!(txn3.read(a2.as_str().into()), Ok("key2value".to_string()));

        txn3.commit();

        let blank = create_empty_context();
        ctx.wallog.borrow().apply(&blank);

        assert_eq!(blank.db.printdb(), ctx.db.printdb());
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
