use std::cell::UnsafeCell;
use std::marker::PhantomData;

use super::mvcc_metadata::WriteIntentError;
use super::typed_value::TypedValue;
use super::{LockDataRef, ReadError, WriteIntent};
use crate::rwtransaction_wrapper::{MVCCMetadata, MutBTreeMap};
use crate::timestamp::Timestamp;
use crate::DbContext;

type MyMutex<T> = parking_lot::Mutex<T>;
type Guard<'a, T> = parking_lot::MutexGuard<'a, T>;

#[derive(Debug)]
pub struct ValueWithMVCC {
    meta: MVCCMetadata,
    val: TypedValue,
    lock: MyMutex<()>,
    _marker: PhantomData<UnsafeCell<()>>,
}

impl Clone for ValueWithMVCC {
    fn clone(&self) -> Self {
        let _l = self.lock.lock();
        Self {
            meta: self.meta.clone(),
            val: self.val.clone(),
            lock: MyMutex::new(()),
            _marker: PhantomData::default(),
        }
    }
}

pub struct UnlockedWritableMVCC<'a> {
    // Manipulating pointers is OK because we have the lock
    // We know we have exclusive access to this data item.
    meta_ptr: *mut MVCCMetadata,
    pub val: *mut TypedValue,
    pub meta: &'a MVCCMetadata,
    #[allow(unused)]
    lock: Guard<'a, ()>,
}

#[derive(Debug)]
pub struct UnlockedReadableMVCC<'a> {
    pub meta: &'a MVCCMetadata,
    pub val: &'a TypedValue,
    lock: Guard<'a, ()>,
}

impl<'a> UnlockedReadableMVCC<'a> {
    pub(crate) fn confirm_read(&self, timestamp: Timestamp) {
        self.meta.confirm_read(timestamp);
    }
}

impl<'a> UnlockedWritableMVCC<'a> {
    pub(super) fn inplace_update(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
        newvalue: TypedValue,
    ) -> Result<(), String> {
        // If we're rewriting our former value, then that value never got committed, so it hsouldn't be archived
        let was_committed = self.meta.get_write_intents().as_ref().unwrap().was_commited;

        assert!(self.meta.get_beg_time() <= txn.timestamp);
        let oldmvcc = unsafe { &mut *self.meta_ptr }.inplace_into_newer(txn.timestamp);

        let oldvalue = std::mem::replace(unsafe { &mut *self.val }, newvalue);

        if was_committed {
            let archived = ValueWithMVCC {
                meta: oldmvcc,
                val: oldvalue,
                lock: MyMutex::new(()),
                _marker: PhantomData::default(),
            };
            let oldmetadata_key = ctx.old_values_store.insert(archived);

            unsafe { &mut *self.meta_ptr }.insert_prev_mvcc(oldmetadata_key);
        }
        Ok(())
    }
}

impl ValueWithMVCC {
    pub fn get_val(&self) -> &TypedValue {
        let _l = self.lock.lock();
        &self.val
    }
    pub fn into_inner(self) -> (MVCCMetadata, TypedValue) {
        (self.meta, self.val)
    }
    pub fn new(txn: LockDataRef, val: TypedValue) -> Self {
        let meta = MVCCMetadata::new(txn);

        Self {
            meta,
            val,
            _marker: PhantomData::default(),
            lock: MyMutex::new(()),
        }
    }

    pub fn from_tuple(meta: MVCCMetadata, val: TypedValue) -> Self {
        Self {
            meta,
            val,
            _marker: PhantomData::default(),
            lock: MyMutex::new(()),
        }
    }
}

// From an unlocked MVCC (so we have exclusive write access to this thing),
// pull out the old value from self.prev_mvcc because this current value was written by an aborted txn,
// and therefore is invalid.
// At the end, self should be the old metadata and the old value.
// The old value should have a write intent that is also aborted (as when the previous txn wrote, it would've
// layed out WI on both the committed (old value) and the aborted invalid value. Must clear that WI and replace it with ours.
fn rescue_previous_value(meta: &mut MVCCMetadata, val: &mut TypedValue, ctx: &DbContext) {
    let cur_end_ts = meta.get_end_time();

    if meta.get_prev_mvcc(ctx).is_ok() {
        let (mut oldmeta, oldstr) = meta.remove_prev_mvcc(ctx).into_inner();

        assert!(oldmeta.get_write_intents().is_none());

        oldmeta.set_end_time(cur_end_ts);
        *meta = oldmeta;
        *val = oldstr;
    } else {
        *val = TypedValue::Deleted;
        let txn = meta.get_write_intents().unwrap().associated_transaction;
        meta.aborted_reset_write_intent(txn, None).unwrap();
    }
}

// This is a fre function rather than a member method because of Rust's borrowing rules
// Lock needs to be held immutably, but this function needs to borrow &self mutably.
// Free function allows splitting borrows of lock, meta, and val.
fn fixup_write_intents(
    meta: &mut MVCCMetadata,
    val: &mut TypedValue,
    ctx: &DbContext,
    txn: LockDataRef,
) -> Result<(), WriteIntentError> {
    let res = meta.check_write_intents(&ctx.transaction_map, txn);
    // Clean intents
    if let Err(err) = res {
        match err {
            WriteIntentError::Aborted(x) => {
                meta.aborted_reset_write_intent(
                    x,
                    Some(WriteIntent {
                        associated_transaction: txn,
                        was_commited: true,
                    }),
                )?;

                rescue_previous_value(meta, val, ctx);

                // The value we rescued may also have been aborted, so we continue checking.
                // In the common case, this should return Ok.
                assert!(meta.get_write_intents().is_none());
                Ok(())
            }
            WriteIntentError::Committed(curwriteintent) => {
                meta.cur_write_intent
                    .compare_swap_none(Some(curwriteintent), None)
                    .map_err(WriteIntentError::Other)
                    .unwrap();
                assert!(meta.get_write_intents().is_none());
                Ok(())
            }
            _ => Err(err),
        }
    } else {
        Ok(())
    }
}

impl ValueWithMVCC {
    pub fn clear_committed_intent(&mut self, txn: LockDataRef) {
        self.meta.cur_write_intent.compare_swap_none(
            Some(WriteIntent {
                associated_transaction: txn,
                was_commited: false,
            }),
            None,
        );
    }
    pub fn get_mvcc_copy(&self) -> MVCCMetadata {
        let _l = self.lock.lock();
        self.meta.clone()
    }

    pub fn check_read(&self, ctx: &DbContext, txn: LockDataRef) -> Result<(), ReadError> {
        self.meta
            .check_read(&ctx.transaction_map, txn)
            .map_err(ReadError::Other)?;

        if MutBTreeMap::is_deleated(&self.val) {
            // On the second restart, they will be blocked by the MutBtreemap from reading this value.
            // Special case here because after we "fixed the abort/commit intents," this ValueWithMVCC doesn't
            // go through the MutBtreemap code path again to be checked.
            Err(ReadError::ValueNotFound)
        } else {
            Ok(())
        }
    }

    #[allow(clippy::cast_ref_to_mut)]
    pub fn get_readable_fix_errors(
        &self,
        ctx: &DbContext,
        txn: LockDataRef,
    ) -> Result<UnlockedReadableMVCC<'_>, ReadError> {
        let lock = self.lock.lock();

        // We guarantee there's no other threads accessing this value because we hold the lock.
        let mut_meta = unsafe { &mut *(&self.meta as *const MVCCMetadata as *mut MVCCMetadata) };
        let mut_val = unsafe { &mut *(&self.val as *const TypedValue as *mut TypedValue) };
        fixup_write_intents(mut_meta, mut_val, ctx, txn)
            .map_err(|a| ReadError::Other(a.tostring()))?;
        self.check_read(ctx, txn)?;
        Ok(UnlockedReadableMVCC {
            meta: mut_meta,
            val: mut_val,
            lock,
        })
    }

    // Take in `readable` by value to ensure that this value was previously checked for readability/has no existing write intents on it.
    pub(super) fn get_writable<'a>(
        &'a self,
        txn: LockDataRef,
        readable: UnlockedReadableMVCC<'a>,
    ) -> Result<UnlockedWritableMVCC<'a>, String> {
        let lock = readable.lock;
        self.meta.check_write(txn)?;

        let writable = UnlockedWritableMVCC {
            meta_ptr: &self.meta as *const MVCCMetadata as *mut MVCCMetadata,
            val: &self.val as *const TypedValue as *mut TypedValue,
            meta: &self.meta,
            lock,
        };

        match self.meta.get_write_intents() {
            // Our write intent has already been inserted, presumably from another preceding operation.
            // It's locked by us, so we can safely pass.
            Some(wi) if wi.associated_transaction == txn => {}
            None => {
                // Insert our write intent into this value to "lock" it from future transactions.
                unsafe { &mut *writable.meta_ptr }
                    .atomic_insert_write_intents(WriteIntent {
                        associated_transaction: txn,
                        was_commited: true,
                    })
                    .unwrap();
            }
            _ => {
                panic!("Write intent still exists, unreachable code. This function should've been called *after* calling get_readable(), which guarantees that no other transactions are currently writing");
            }
        };

        Ok(writable)
    }
    pub fn as_inner(&self) -> (MVCCMetadata, &TypedValue) {
        let _l = self.lock.lock();
        (self.meta.clone(), &self.val)
    }
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::rwtransaction_wrapper::ReplicatedTxn;
    use crate::timestamp::Timestamp;

    #[test]
    fn test_fix() {
        let db = db!("adfs" = "value");
        let mut write = ReplicatedTxn::new(&db);
        write.write(&"adfs".into(), "fdsvcx".into());
        write.abort();

        let mut read = ReplicatedTxn::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value".into());
    }

    #[test]
    fn test_abort_2() {
        let db = db!("adfs" = "value");
        let mut write = ReplicatedTxn::new(&db);
        write.write(&"adfs".into(), "fdsvcx".into());
        write.abort();

        let mut write = ReplicatedTxn::new(&db);
        write.write(&"adfs".into(), "value2".into());
        write.commit();

        let mut read = ReplicatedTxn::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value2".into());
    }

    #[test]
    fn test_abort_multi() {
        let db = db!("adfs" = "value");

        for _ in 0..20 {
            let mut write = ReplicatedTxn::new(&db);
            write.write(&"adfs".into(), "fdsvcx".into());
            write.abort();
        }

        let mut read = ReplicatedTxn::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value".into());

        let mut write = ReplicatedTxn::new(&db);
        write.write(&"adfs".into(), "value2".into());
        write.commit();

        let mut read = ReplicatedTxn::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value2".into());
    }

    #[test]
    fn test_read_in_the_past() {
        let db = db!("k" = "v");

        let begin_time = 300000;
        for i in begin_time..begin_time + 10 {
            let mut write = ReplicatedTxn::new_with_time(&db, Timestamp::from(i));
            write.write(&"k".into(), i.to_string().into()).unwrap();
            write.commit();
        }

        for i in begin_time..begin_time + 10 {
            let mut read = ReplicatedTxn::new_with_time(&db, Timestamp::from(i));
            assert_eq!(read.read(&"k".into()).unwrap(), i.to_string().into());
            read.commit();
        }

        // db.wallog.borrow().print();
    }
}
