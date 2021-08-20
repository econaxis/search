use crate::rwtransaction_wrapper::mvcc_manager::mvcc_metadata::WriteIntentError;
use crate::rwtransaction_wrapper::mvcc_manager::{LockDataRef, WriteIntent, ReadError};
use crate::rwtransaction_wrapper::{MVCCMetadata, MutBTreeMap};
use crate::DbContext;


use crate::timestamp::Timestamp;

type MyMutex<T> = parking_lot::ReentrantMutex<T>;
type Guard<'a, T> = parking_lot::ReentrantMutexGuard<'a, T>;


#[derive(Debug)]
pub struct ValueWithMVCC {
    meta: MVCCMetadata,
    val: String,
    lock: MyMutex<()>,
}

impl ValueWithMVCC {
    pub fn get_val(&self) -> &str {
        let _l = self.lock.lock();
        &self.val
    }
    pub fn into_inner(self) -> (MVCCMetadata, String) {
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

pub struct UnlockedWritableMVCC<'a> {
    pub meta: &'a mut MVCCMetadata,
    pub val: &'a mut String,
    #[allow(unused)]
    lock: Guard<'a, ()>,
}


#[derive(Debug)]
pub struct UnlockedReadableMVCC<'a> {
    pub meta: &'a MVCCMetadata,
    pub val: &'a String,
    lock: Guard<'a, ()>,
}

impl<'a> UnlockedReadableMVCC<'a> {
    pub(crate) fn confirm_read(&self, timestamp: Timestamp) {
        self.meta.confirm_read(timestamp);
    }
}

impl<'a> UnlockedWritableMVCC<'a> {
    pub fn into_inner(self) -> UnlockedReadableMVCC<'a> {
        UnlockedReadableMVCC {
            meta: self.meta,
            val: self.val,
            lock: self.lock,
        }
    }


    pub(super) fn inplace_update(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
        newvalue: String,
    ) -> Result<(), String> {
        // If we're rewriting our former value, then that value never got committed, so it hsouldn't be archived
        let was_committed = self.meta.get_write_intents().as_ref().unwrap().was_commited;

        assert!(self.meta.get_beg_time() <= txn.timestamp);
        let oldmvcc = self.meta.inplace_into_newer(txn.timestamp);

        let oldvalue = std::mem::replace(self.val, newvalue);

        if was_committed {
            let archived = ValueWithMVCC {
                meta: oldmvcc,
                val: oldvalue,
                lock: MyMutex::new(()),
            };
            let oldmetadata_key = ctx.old_values_store.insert(archived);

            self.meta.insert_prev_mvcc(oldmetadata_key);
        }
        Ok(())
    }
}


impl ValueWithMVCC {
    pub fn new(txn: LockDataRef, val: String) -> Self {
        let meta = MVCCMetadata::new(txn);

        Self { meta, val, lock: MyMutex::new(()) }
    }


    pub fn from_tuple(meta: MVCCMetadata, val: String) -> Self {
        Self { meta, val, lock: MyMutex::new(()) }
    }
}

impl ValueWithMVCC {
    pub fn get_readable_unchecked(&self) -> UnlockedReadableMVCC<'_> {
        UnlockedReadableMVCC {
            meta: &self.meta,
            val: &self.val,
            lock: self.lock.lock(),
        }
    }
    // From an unlocked MVCC (so we have exclusive write access to this thing),
    // pull out the old value from self.prev_mvcc because this current value was written by an aborted txn,
    // and therefore is invalid.
    // At the end, self should be the old metadata and the old value.
    // The old value should have a write intent that is also aborted (as when the previous txn wrote, it would've
    // layed out WI on both the committed (old value) and the aborted invalid value. Must clear that WI and replace it with ours.
    fn rescue_previous_value(&mut self, ctx: &DbContext) {
        let cur_end_ts = self.meta.get_end_time();

        if self.meta.get_prev_mvcc(ctx).is_ok() {
            let (mut oldmeta, oldstr) = self.meta.remove_prev_mvcc(ctx).into_inner();

            assert!(oldmeta.get_write_intents().is_none());

            oldmeta.set_end_time(cur_end_ts);
            self.meta = oldmeta;
            self.val = oldstr;
        } else {
            // todo!("aborted insertion transaction -- cannot be rollbacked yet/deleted")
            self.val = crate::rwtransaction_wrapper::mvcc_manager::btreemap_kv_backend::TOMBSTONE.to_owned();
            let txn = self.meta.get_write_intents().unwrap().associated_transaction;
            self.meta.aborted_reset_write_intent(txn, None).unwrap();
        }
    }

    pub fn fixup_write_intents(&mut self, ctx: &DbContext, txn: LockDataRef) -> Result<(), WriteIntentError> {
        let res = self.meta.check_write_intents(&ctx.transaction_map, txn);
        // Clean intents
        if let Err(err) = res {
            match err {
                WriteIntentError::Aborted(x) => {
                    self.meta.aborted_reset_write_intent(x, Some(WriteIntent {
                        associated_transaction: x,
                        was_commited: true,
                    })).unwrap();

                    self.rescue_previous_value(ctx);

                    // The value we rescued may also have been aborted, so we continue checking.
                    // In the common case, this should return Ok.

                    assert!(self.meta.get_write_intents().is_none());
                    Ok(())
                }
                WriteIntentError::Committed(curwriteintent) => {
                    self.meta.cur_write_intent.compare_swap_none(Some(curwriteintent), None).map_err(WriteIntentError::Other).unwrap();
                    Ok(())
                }
                _ => { Err(err) }
            }
        } else {
            Ok(())
        }
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

    pub fn get_readable_fix_errors(
        &mut self,
        ctx: &DbContext,
        txn: LockDataRef,
    ) -> Result<UnlockedReadableMVCC<'_>, ReadError> {
        self.fixup_write_intents(ctx, txn).map_err(|a| ReadError::Other(a.tostring()))?;
        self.check_read(ctx, txn)?;
        Ok(UnlockedReadableMVCC {
            meta: &mut self.meta,
            val: &mut self.val,
            lock: self.lock.lock(),
        })
    }


    pub(in crate::rwtransaction_wrapper::mvcc_manager) fn get_writable(&mut self, txn: LockDataRef) -> Result<UnlockedWritableMVCC<'_>, String> {
        // NOTE: must have called fix_errors before this.
        assert_eq!(self.meta.get_end_time(), Timestamp::maxtime());
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
                panic!("Write intent still exists, unreachable code. This function should've been called *after* calling get_readable(), which guarantees that no other transactions are currently writing");
            }
        };

        let writable = UnlockedWritableMVCC {
            meta: &mut self.meta,
            val: &mut self.val,
            lock: self.lock.lock(),
        };

        Ok(writable)
    }
    pub fn as_inner(&self) -> (MVCCMetadata, &String) {
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
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value".to_string());
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
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value2".to_string());
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
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value".to_string());

        let mut write = ReplicatedTxn::new(&db);
        write.write(&"adfs".into(), "value2".into());
        write.commit();

        let mut read = ReplicatedTxn::new(&db);
        assert_eq!(read.read(&"adfs".into()).unwrap(), "value2".to_string());
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
            assert_eq!(read.read(&"k".into()).unwrap(), i.to_string());
            read.commit();
        }

        // db.wallog.borrow().print();
    }
}
