use crate::rwtransaction_wrapper::mvcc_manager::mvcc_metadata::WriteIntentError;
use crate::rwtransaction_wrapper::mvcc_manager::{LockDataRef, WriteIntent, WriteIntentStatus};
use crate::rwtransaction_wrapper::MVCCMetadata;
use crate::DbContext;

use std::sync::atomic::{AtomicBool, Ordering};
use crate::timestamp::Timestamp;
use std::sync::{RwLockWriteGuard, Mutex, MutexGuard};

#[derive(Debug)]
pub struct ValueWithMVCC {
    meta: Mutex<MVCCMetadata>,
    val: String,
}

impl ValueWithMVCC {
    pub(crate) fn into_inner(self) -> (MVCCMetadata, String) {
        (self.meta.into_inner().unwrap(), self.val)
    }
}

impl Clone for ValueWithMVCC {
    fn clone(&self) -> Self {
        let meta = self.meta.lock().unwrap().clone();
        Self {
            meta: Mutex::new(meta),
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
            // todo!("aborted insertion transaction -- cannot be rollbacked yet")
            *self.val = "0".to_string();
            let txn = self.meta.get_write_intents().unwrap().associated_transaction;
            self.meta.aborted_reset_write_intent(txn, None);
        }
    }
    pub fn fix_errors(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
    ) -> Result<(), String> {
        let res = self.check_read(ctx, txn);

        let is_aborted = if let Err(err) = res {
            match err {
                WriteIntentError::Aborted(x) => {
                    // rationale: aborts are caused by write transactions only.
                    // write transactions move the old, committed value to old_values_store,
                    // and write the new value to the main DB. `self` must be the new, uncommitted value,
                    // instead of the old, committed value.
                    self.meta.aborted_reset_write_intent(x, Some(txn))
                }
                WriteIntentError::WritingInThePast => Err(format!("Past intent error {:?}", self.meta)),
                WriteIntentError::PendingIntent(_) => Err("Pending intent error".to_string()),
                WriteIntentError::Other(x) => Err(format!("Other intent error {}", x))
            }?;
            true
        } else {
            false
        };
        if is_aborted {
            let mut prev_wi = self.meta.get_write_intents().unwrap();
            prev_wi.was_commited = true;
            assert_eq!(prev_wi.associated_transaction, txn);
            self.rescue_previous_value(ctx);
        }


        self.check_write_intents(ctx, txn)
            .map_err(WriteIntentError::tostring)?;

        self.meta
            .check_read(&ctx.transaction_map, txn)?;
        Ok(())
    }

    pub fn clone_value(&self) -> ValueWithMVCC {
        ValueWithMVCC {
            meta: Mutex::new(self.meta.clone()),
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

        writable.0.fix_errors(ctx, txn)?;
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
    ) -> Result<(), WriteIntentError> {
        self.check_write_intents(txnmap, txn)?;
        self.meta
            .check_read(&txnmap.transaction_map, txn)
            .map_err(|a| WriteIntentError::Other(a))?;
        Ok(())
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
            meta: Mutex::new(self.0.meta.clone()),
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
            meta: Mutex::new(oldmvcc),
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

        Self { meta: Mutex::new(meta), val: value }
    }

    pub fn from_tuple(a: MVCCMetadata, v: String) -> Self {
        Self { meta: Mutex::new(a), val: v }
    }
}

impl ValueWithMVCC {
    pub fn get_readable(
        &mut self,
    ) -> Result<UnlockedReadableMVCC<'_>, String> {
        Ok(UnlockedReadableMVCC {
            meta: self.meta.lock().map_err(|a| "Lock failed".to_string())?,
            val: &mut self.val,
        })
    }

    pub fn as_inner(&self) -> (MutexGuard<'_, MVCCMetadata>, &String) {
        (self.meta.lock().unwrap(), &self.val)
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
