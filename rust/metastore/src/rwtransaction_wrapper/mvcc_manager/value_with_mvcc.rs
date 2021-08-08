use crate::rwtransaction_wrapper::mvcc_manager::mvcc_metadata::WriteIntentError;
use crate::rwtransaction_wrapper::mvcc_manager::{LockDataRef, WriteIntent, WriteIntentStatus, ReadError};
use crate::rwtransaction_wrapper::{MVCCMetadata, MutBTreeMap};
use crate::DbContext;

use std::sync::atomic::{AtomicBool, Ordering};
use crate::timestamp::Timestamp;
use std::sync::{RwLockWriteGuard, Mutex, MutexGuard, TryLockResult, LockResult};
use std::time::Duration;
use std::thread::ThreadId;
use std::cell::{RefCell, UnsafeCell};
use std::backtrace::Backtrace;
use std::io::Read;
use parking_lot::{ReentrantMutex, ReentrantMutexGuard};
use std::borrow::BorrowMut;
use std::ops::Deref;

// #[derive(Debug)]
// struct MyMutex<T>(Mutex<T>, UnsafeCell<Option<(ThreadId, Backtrace)>>);

type MyMutex<T> = parking_lot::ReentrantMutex<T>;
type Guard<'a, T> = parking_lot::ReentrantMutexGuard<'a, T>;

#[derive(Debug)]
pub struct ValueWithMVCC {
    meta: MVCCMetadata,
    val: String,
    lock: MyMutex<()>,
}

impl ValueWithMVCC {
    // Get the underlying value without going through the lock first
    // If the caller already has a database-wide lock, then this function is safe
    pub fn get_val(&self) -> &str {
        let l = self.lock.lock();
        &self.val
    }
}

impl ValueWithMVCC {
    pub(crate) fn into_inner(self) -> (MVCCMetadata, String) {
        (self.meta, self.val)
    }
}


impl Clone for ValueWithMVCC {
    fn clone(&self) -> Self {
        let _l = self.lock.lock();
        Self {
            meta: self.meta.clone(),
            val: self.val.clone(),
            lock: MyMutex::new(()),
        }
    }
}

pub struct UnlockedWritableMVCC<'a>(pub UnlockedReadableMVCC<'a>);


pub struct UnlockedReadableMVCC<'a> {
    pub meta: &'a mut MVCCMetadata,
    pub val: &'a mut String,
    lock: Guard<'a, ()>,
}

impl<'a> UnlockedReadableMVCC<'a> {
    // From an unlocked MVCC (so we have exclusive write access to this thing),
    // pull out the old value from self.prev_mvcc because this current value was written by an aborted txn,
    // and therefore is invalid.
    // At the end, self should be the old metadata and the old value.
    // The old value should have a write intent that is also aborted (as when the previous txn wrote, it would've
    // layed out WI on both the committed (old value) and the aborted invalid value. Must clear that WI and replace it with ours.
    fn rescue_previous_value(&mut self, ctx: &DbContext) {
        let cur_end_ts = self.meta.get_end_time();

        if let Ok(_) = self.meta.get_prev_mvcc(ctx) {
            let (mut oldmeta, oldstr) = self.meta.remove_prev_mvcc(ctx).into_inner();

            assert_eq!(oldmeta.get_write_intents(), None);

            oldmeta.set_end_time(cur_end_ts);
            std::mem::replace(&mut *self.meta, oldmeta);
            std::mem::replace(self.val, oldstr);
        } else {
            // todo!("aborted insertion transaction -- cannot be rollbacked yet/deleted")
            *self.val = super::btreemap_kv_backend::TOMBSTONE.to_owned();
            let txn = self.meta.get_write_intents().unwrap().associated_transaction;
            self.meta.aborted_reset_write_intent(txn, None);
        }
    }
    pub fn clean_intents(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
    ) -> Result<(), WriteIntentError> {
        let res = self.check_write_intents(ctx, txn);

        return if let Err(err) = res {
            match err {
                WriteIntentError::Aborted(x) => {
                    self.meta.aborted_reset_write_intent(x, Some(WriteIntent {
                        associated_transaction: x,
                        was_commited: true,
                    }));

                    self.rescue_previous_value(ctx);

                    self.clean_intents(ctx, txn)
                }
                _ => { Err(err) }
            }
        } else {
            Ok(())
        };
        // self.check_read(&ctx, txn).unwrap();
    }

    pub(crate) fn confirm_read(&mut self, p0: LockDataRef) {
        self.meta.confirm_read(p0);
    }

    // Locks the current (latest) mvcc value for write
    pub fn lock_for_write(
        mut self,
        ctx: &'a DbContext,
        txn: LockDataRef,
    ) -> Result<UnlockedWritableMVCC<'a>, String> {
        assert_eq!(self.meta.get_end_time(), Timestamp::maxtime());
        self.clean_intents(ctx, txn).map_err(|a| a.tostring())?;
        let previntent = self.meta.get_write_intents();
        self.meta.check_write(txn)?;
        match self.meta.get_write_intents() {
            Some(wi) if wi.associated_transaction == txn => {}
            None => {
                // We haven't inserted a write intent, therefore this value must have been committed before us.
                self.meta.atomic_insert_write_intents(WriteIntent {
                    associated_transaction: txn,
                    was_commited: true,
                })?;
            }
            _ => {
                panic!("Write intent still exists, unreachable code")
            }
        };
        let mut writable = UnlockedWritableMVCC(self);

        let int = writable.0.meta.get_write_intents();
        assert_eq!(int.clone().unwrap().associated_transaction, txn);

        if previntent != int.clone() && previntent.is_some() {
            println!("{:?} trying to access {:?}", int, previntent);
        }

        Ok(writable)
    }

    pub fn check_read(
        &mut self,
        txnmap: &DbContext,
        txn: LockDataRef,
    ) -> Result<(), ReadError> {
        self.check_write_intents(txnmap, txn).unwrap();
        self.meta
            .check_read(&txnmap.transaction_map, txn)
            .map_err(|a| ReadError::Other(a))?;

        if MutBTreeMap::is_deleated(self.val) {
            // On the second restart, they will be blocked by the MutBtreemap from reading this value.
            // Special case here because after we "fixed the abort/commit intents," this ValueWithMVCC doesn't
            // go through the MutBtreemap code path again to be checked.
            println!("deleated");
            Err(ReadError::ValueNotFound)
        } else {
            Ok(())
        }
    }
    pub fn check_write_intents(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
    ) -> Result<(), WriteIntentError> {
        self.meta.check_write_intents(&ctx.transaction_map, txn)
    }
}

impl<'a> UnlockedWritableMVCC<'a> {
    pub fn to_inner(self) -> UnlockedReadableMVCC<'a> {
        self.0
    }

    pub fn release_lock(mut self, txn: LockDataRef) {
        assert_eq!(self.0.meta.get_write_intents().unwrap().associated_transaction, txn);
        self.0.meta.aborted_reset_write_intent(txn, None);
    }

    pub(super) fn become_newer_version(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
        newvalue: String,
    ) -> Result<(), String> {
        // If we're rewriting our former value, then that value never got committed, so it hsouldn't be archived
        assert_eq!(
            self.0.meta
                .get_write_intents()
                .unwrap()
                .associated_transaction,
            txn
        );
        let was_committed = self.0.meta.get_write_intents().as_ref().unwrap().was_commited;

        assert!(self.0.meta.get_beg_time() <= txn.timestamp);
        let oldmvcc = self.0.meta.into_newer(txn.timestamp);
        assert_eq!(
            self.0.meta
                .get_write_intents()
                .unwrap()
                .associated_transaction,
            txn
        );

        let oldvalue = std::mem::replace(self.0.val, newvalue);
        assert_eq!(
            self.0.meta
                .get_write_intents()
                .unwrap()
                .associated_transaction,
            txn
        );


        assert!(oldmvcc.get_write_intents().is_none());
        assert_eq!(
            self.0.meta
                .get_write_intents()
                .unwrap()
                .associated_transaction,
            txn
        );


        if was_committed {
            let archived = ValueWithMVCC {
                meta: oldmvcc,
                val: oldvalue,
                lock: MyMutex::new(()),
            };
            let oldmetadata_key = ctx.old_values_store.insert(archived);

            self.0.meta.insert_prev_mvcc(oldmetadata_key);
        }
        Ok(())
    }
}


impl ValueWithMVCC {
    pub fn new(txn: LockDataRef, value: String) -> Self {
        let meta = MVCCMetadata::new(txn);

        Self { meta: meta, val: value, lock: MyMutex::new(()) }
    }

    pub fn from_tuple(a: MVCCMetadata, v: String) -> Self {
        Self { meta: a, val: v, lock: MyMutex::new(()) }
    }
}

impl ValueWithMVCC {
    pub fn get_readable(
        &mut self,
    ) -> UnlockedReadableMVCC<'_> {
        let lock = self.lock.lock();
        UnlockedReadableMVCC {
            meta: &mut self.meta,
            val: &mut self.val,
            lock,
        }
    }

    pub fn as_inner(&self) -> (MVCCMetadata, &String) {
        let _l = self.lock.lock();
        (self.meta.clone(), &self.val)
    }
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::rwtransaction_wrapper::DBTransaction;
    use crate::timestamp::Timestamp;

    #[test]
    fn test_fix() {
        let db = db!("adfs" = "value");
        let mut write = DBTransaction::new(&db);
        write.write(&"adfs".into(), "fdsvcx".into());
        write.abort();

        let mut read = DBTransaction::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value".to_string());
    }

    #[test]
    fn test_abort_2() {
        let db = db!("adfs" = "value");
        let mut write = DBTransaction::new(&db);
        write.write(&"adfs".into(), "fdsvcx".into());
        write.abort();

        let mut write = DBTransaction::new(&db);
        write.write(&"adfs".into(), "value2".into());
        write.commit();

        let mut read = DBTransaction::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value2".to_string());
    }

    #[test]
    fn test_abort_multi() {
        let db = db!("adfs" = "value");

        for _ in 0..20 {
            let mut write = DBTransaction::new(&db);
            write.write(&"adfs".into(), "fdsvcx".into());
            write.abort();
        }


        let mut read = DBTransaction::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value".to_string());

        let mut write = DBTransaction::new(&db);
        write.write(&"adfs".into(), "value2".into());
        write.commit();

        let mut read = DBTransaction::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value2".to_string());
    }

    #[test]
    fn test_read_in_the_past() {
        let db = db!("k" = "v");

        let begin_time = 300000;
        for i in begin_time..begin_time + 10 {
            let mut write = DBTransaction::new_with_time(&db, Timestamp::from(i));
            write.write(&"k".into(), i.to_string().into()).unwrap();
            write.commit();
        }

        for i in begin_time..begin_time + 10 {
            let mut read = DBTransaction::new_with_time(&db, Timestamp::from(i));
            assert_eq!(read.read(&"k".into()).unwrap(), i.to_string());
            read.commit();
        }

        // db.wallog.borrow().print();
    }
}