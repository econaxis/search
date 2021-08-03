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

#[derive(Debug)]
struct MyMutex<T>(Mutex<T>, UnsafeCell<Option<(ThreadId, Backtrace)>>);

impl<T> MyMutex<T> {
    fn get(&self) -> &mut Option<(ThreadId, Backtrace)> {
        unsafe { &mut *self.1.get() }
    }

    fn lock(&self) -> MutexGuard<'_, T> {
        // println!("{:?} try ", std::thread::current().id());
        //
        // match &*self.get() {
        //     Some((id, bt)) if id == &std::thread::current().id() => {
        //         if self.0.try_lock().is_err() {
        //             // println!("{:?}", bt);
        //             // panic!("Circular mutex dependency")
        //         }
        //     },
        //     _ => {}
        // }

        let lock = self.0.lock().unwrap();
        // let prevowner = self.get().replace((std::thread::current().id(), Backtrace::force_capture()));

        // println!("{:?} success ", std::thread::current().id());
        lock
    }

    fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
        self.0.try_lock()
    }

    fn new(t: T) -> Self {
        MyMutex(Mutex::new(t), UnsafeCell::new(None))
    }

    fn into_inner(self) -> LockResult<T> {
        self.0.into_inner()
    }
}

#[derive(Debug)]
pub struct ValueWithMVCC {
    meta: MyMutex<MVCCMetadata>,
    val: String,
}

impl ValueWithMVCC {
    pub(crate) fn into_inner(self) -> (MVCCMetadata, String) {
        (self.meta.into_inner().unwrap(), self.val)
    }
}


impl Clone for ValueWithMVCC {
    fn clone(&self) -> Self {
        let mut iters = 0;
        let l = loop {
            iters += 1;
            let l = self.meta.try_lock();
            if l.is_ok() { break l.unwrap(); };
            if iters > 50 {panic!("Too many lock attempts")}
            println!("Retrying failed lock {}", iters);
            std::thread::sleep(Duration::from_millis(20))
        };
        let meta = l.clone();
        std::mem::drop(l);
        Self {
            meta: MyMutex::new(meta),
            val: self.val.clone(),
        }
    }
}

pub struct UnlockedWritableMVCC<'a>(pub UnlockedReadableMVCC<'a>);


pub struct UnlockedReadableMVCC<'a> {
    pub meta: MutexGuard<'a, MVCCMetadata>,
    pub val: &'a mut String,
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

        if let Ok(prev) = self.meta.get_prev_mvcc(ctx) {
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
        }
        // self.check_read(&ctx, txn).unwrap();
    }

    pub fn clone_value(&self) -> ValueWithMVCC {
        ValueWithMVCC {
            meta: MyMutex::new(self.meta.clone()),
            val: self.val.clone(),
        }
    }
    pub(crate) fn confirm_read(&mut self, p0: LockDataRef) {
        self.meta.confirm_read(p0);
    }

    // Locks the current (latest) mvcc value for write
    pub fn lock_for_write(
        self,
        ctx: &'a DbContext,
        txn: LockDataRef,
    ) -> Result<UnlockedWritableMVCC<'a>, String> {
        let mut writable = UnlockedWritableMVCC(UnlockedReadableMVCC {
            meta: self.meta,
            val: self.val,
        });
        assert_eq!(writable.0.meta.get_end_time(), Timestamp::maxtime());

        writable.0.clean_intents(ctx, txn).map_err(|a| a.tostring())?;
        writable.0.meta.check_write(txn)?;
        match writable.0.meta.get_write_intents() {
            Some(wi) if wi.associated_transaction == txn => {}
            None => {
                // We haven't inserted a write intent, therefore this value must have been committed before us.
                writable.0.meta.atomic_insert_write_intents(WriteIntent {
                    associated_transaction: txn,
                    was_commited: true,
                })?;
            }
            _ => {
                panic!("Write intent still exists, unreachable code")
            }
        };
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


    pub fn clone_value(&self) -> ValueWithMVCC {
        ValueWithMVCC {
            meta: MyMutex::new(self.0.meta.clone()),
            val: self.0.val.clone(),
        }
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
        let reference = self.0.meta.clone();
        let oldmvcc = self.0.meta.into_newer(txn.timestamp);
        let oldvalue = std::mem::replace(self.0.val, newvalue);

        assert!(oldmvcc.get_write_intents().is_none());
        assert_eq!(
            self.0.meta
                .get_write_intents()
                .unwrap()
                .associated_transaction,
            txn
        );


        let archived = ValueWithMVCC {
            meta: MyMutex::new(oldmvcc),
            val: oldvalue,
        };

        if was_committed {
            let oldmetadata_key = ctx.old_values_store.insert(archived);

            self.0.meta.insert_prev_mvcc(oldmetadata_key);
        }
        Ok(())
    }
}


impl ValueWithMVCC {
    pub fn new(txn: LockDataRef, value: String) -> Self {
        let meta = MVCCMetadata::new(txn);

        Self { meta: MyMutex::new(meta), val: value }
    }

    pub fn from_tuple(a: MVCCMetadata, v: String) -> Self {
        Self { meta: MyMutex::new(a), val: v }
    }
}

impl ValueWithMVCC {
    pub fn get_readable(
        &mut self,
    ) -> Result<UnlockedReadableMVCC<'_>, String> {
        let lock = self.meta.try_lock().map_err(|a| "Couldn't get locks".to_string())?;
        Ok(UnlockedReadableMVCC {
            meta: lock,
            val: &mut self.val,
        })
    }

    pub fn as_inner(&self) -> (MVCCMetadata, &String) {
        (self.meta.lock().clone(), &self.val)
    }
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::rwtransaction_wrapper::RWTransactionWrapper;
    use crate::timestamp::Timestamp;

    #[test]
    fn test_fix() {
        let db = db!("adfs" = "value");
        let mut write = RWTransactionWrapper::new(&db);
        write.write(&"adfs".into(), "fdsvcx".into());
        write.abort();

        let mut read = RWTransactionWrapper::new(&db);
        assert_eq!(read.read("adfs".into()).unwrap(), "value".to_string());
    }

    #[test]
    fn test_abort_2() {
        let db = db!("adfs" = "value");
        let mut write = RWTransactionWrapper::new(&db);
        write.write(&"adfs".into(), "fdsvcx".into());
        write.abort();

        let mut write = RWTransactionWrapper::new(&db);
        write.write(&"adfs".into(), "value2".into());
        write.commit();

        let mut read = RWTransactionWrapper::new(&db);
        assert_eq!(read.read("adfs".into()).unwrap(), "value2".to_string());
    }

    #[test]
    fn test_abort_multi() {
        let db = db!("adfs" = "value");

        for _ in 0..20 {
            let mut write = RWTransactionWrapper::new(&db);
            write.write(&"adfs".into(), "fdsvcx".into());
            write.abort();
        }


        let mut read = RWTransactionWrapper::new(&db);
        assert_eq!(read.read("adfs".into()).unwrap(), "value".to_string());

        let mut write = RWTransactionWrapper::new(&db);
        write.write(&"adfs".into(), "value2".into());
        write.commit();

        let mut read = RWTransactionWrapper::new(&db);
        assert_eq!(read.read("adfs".into()).unwrap(), "value2".to_string());
    }

    #[test]
    fn test_read_in_the_past() {
        let db = db!("k" = "v");

        let begin_time = 300000;
        for i in begin_time..begin_time + 10 {
            let mut write = RWTransactionWrapper::new_with_time(&db, Timestamp::from(i));
            write.write(&"k".into(), i.to_string().into()).unwrap();
            write.commit();
        }

        for i in begin_time..begin_time + 10 {
            let mut read = RWTransactionWrapper::new_with_time(&db, Timestamp::from(i));
            assert_eq!(read.read("k".into()).unwrap(), i.to_string());
            read.commit();
        }

        // db.wallog.borrow().print();
    }
}
