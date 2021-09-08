use std::fmt::{Debug, Display, Formatter};

use super::lock_data_manager::{IntentMap, LockDataRef};
use crate::rwtransaction_wrapper::ValueWithMVCC;
use crate::timestamp::Timestamp;
use crate::DbContext;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::sync::Mutex;

#[derive(Debug, PartialEq, Clone)]
pub enum WriteIntentError {
    Aborted(LockDataRef),
    PendingIntent(LockDataRef),
    Committed(WriteIntent),
    Other(String),
}

crate::custom_error_impl!(WriteIntentError);

impl WriteIntentError {
    pub fn tostring(self) -> String {
        self.into()
    }
}

impl MVCCMetadata {
    pub fn aborted_reset_write_intent(
        &mut self,
        compare: LockDataRef,
        new: Option<WriteIntent>,
    ) -> Result<(), String> {
        let curwi = self.get_write_intents();
        if curwi.as_ref().map(|a| a.associated_transaction) != Some(compare) {
            return Err("Write intent not equals to compare, can't swap".to_string());
        }
        let curwi = curwi.unwrap();

        self.cur_write_intent.compare_swap_none(Some(curwi), new)
    }

    pub fn atomic_insert_write_intents(&mut self, wi: WriteIntent) -> Result<(), String> {
        self.cur_write_intent.compare_swap_none(None, Some(wi))
    }

    pub fn new(txn: LockDataRef) -> Self {
        Self {
            begin_ts: txn.timestamp,
            end_ts: Timestamp::maxtime(),
            last_read: Cell::new(txn.timestamp),
            cur_write_intent: WriteIntentMutex::new(Some(WriteIntent {
                associated_transaction: txn,
                was_commited: false,
            })),
            previous_mvcc_value: None,
        }
    }

    pub(super) fn get_write_intents(&self) -> Option<WriteIntent> {
        self.cur_write_intent.get()
    }

    pub(super) fn check_write_intents(
        &self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), WriteIntentError> {
        let curwriteintent = self.cur_write_intent.get().clone();
        match curwriteintent {
            None => Ok(()),
            Some(wi) if wi.associated_transaction == cur_txn => Ok(()),
            Some(WriteIntent {
                     associated_transaction,
                     was_commited,
                 }) => {
                match txnmap
                    .get_by_ref(&associated_transaction)
                    .unwrap()
                    .get_write_intent()
                {
                    // If the transaction is committed already, then all good.
                    WriteIntentStatus::Committed => {
                        // All good, we write after the txn has committed
                        // todo: remove committed intent
                        Err(WriteIntentError::Committed(WriteIntent {
                            associated_transaction,
                            was_commited,
                        }))
                    }
                    WriteIntentStatus::Aborted => {
                        // Transaction has been aborted, so we are reading an aborted value. Therefore, we should also abort this current transaction.
                        Err(WriteIntentError::Aborted(associated_transaction))
                    }
                    WriteIntentStatus::Pending => {
                        Err(WriteIntentError::PendingIntent(associated_transaction))
                    }
                }
            }
        }
    }

    pub(super) fn check_write(&self, cur_txn: LockDataRef) -> Result<(), String> {
        if self.end_ts != Timestamp::maxtime() {
            // This record is not the latest, so we can't write to it.
            return Err("Trying to write on a historical MVCC record".to_string());
        }

        if cur_txn.timestamp < self.last_read.get() {
            Err("Timestamp smaller than last read".to_string())
        } else if cur_txn.timestamp < self.begin_ts {
            Err("Timestamp smaller than begin time".to_string())
        } else {
            Ok(())
        }
    }

    pub(super) fn inplace_into_newer(&mut self, timestamp: Timestamp) -> Self {
        // We should have a lock on this to access the inner.
        assert!(self.cur_write_intent.get().is_some());

        // Clone acquires a write lock itself, so to prevent deadlock, we acquire write lock after clone
        let mut old = self.clone();

        // Just to prevent others from reading (e.g. clone), write lock
        self.begin_ts = timestamp;
        old.end_ts = timestamp;

        // Can't write to a value that has already been read before us.
        // Should've been checked before in `check_read`. we check again for correctness, just in case
        assert!(self.last_read.get() <= timestamp);
        self.last_read.set(timestamp);

        old.cur_write_intent
            .compare_swap_none(self.cur_write_intent.get().clone(), None)
            .unwrap();

        // Because we're creating a newer value, newer value must be not committed
        // todo! design a better api so we remove get_mut functoin
        self.cur_write_intent.get_mut().unwrap().was_commited = false;

        if old.begin_ts > old.end_ts {
            println!("{:?} {:?}", old, self);
            panic!()
        }
        assert!(self.begin_ts <= self.end_ts);

        old
    }

    pub(super) fn check_read(
        &self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        if cur_txn.timestamp < self.begin_ts || cur_txn.timestamp > self.end_ts {
            return Err("Timestamp not valid".to_string());
        }
        self.check_write_intents(txnmap, cur_txn)
            .map_err(WriteIntentError::tostring)?;
        Ok(())
    }

    pub(super) fn confirm_read(&self, timestamp: Timestamp) {
        if self.last_read.get() < timestamp {
            self.last_read.set(timestamp);
        }
    }
}

pub struct WriteIntentMutex(Option<WriteIntent>, Mutex<()>);

impl Clone for WriteIntentMutex {
    fn clone(&self) -> Self {
        Self(self.0.clone(), Mutex::new(()))
    }
}

impl PartialEq for WriteIntentMutex {
    fn eq(&self, other: &Self) -> bool {
        return self.get() == other.get();
    }
}

impl WriteIntentMutex {
    pub(crate) fn get_mut(&mut self) -> Option<&mut WriteIntent> {
        self.0.as_mut()
    }
    pub(crate) fn compare_swap_none(
        &self,
        compare: Option<WriteIntent>,
        swap: Option<WriteIntent>,
    ) -> Result<(), String> {
        let _l = self.1.lock();
        if self.0 == compare {
            unsafe {
                let oldvalue = std::ptr::read(&self.0);
                std::ptr::write(&self.0 as *const _ as *mut _, swap.clone());
                assert_eq!(oldvalue, compare);
            }
            assert_eq!(self.0, swap);
            Ok(())
        } else if self.0 == swap {
            Ok(())
        } else {
            Err("Another thread has replaced this value".to_string())
        }
    }
    pub fn get(&self) -> Option<WriteIntent> {
        let _l = self.1.lock();
        self.0.clone()
    }
    pub fn new(wi: Option<WriteIntent>) -> Self {
        Self(wi, Mutex::new(()))
    }
}

impl Default for WriteIntentMutex {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Debug for WriteIntentMutex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.get()
            .as_ref()
            .map(|a| f.write_fmt(format_args!("{:?}", a)));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MVCCMetadata {
    begin_ts: Timestamp,
    end_ts: Timestamp,
    last_read: Cell<Timestamp>,
    #[serde(skip)]
    pub(crate) cur_write_intent: WriteIntentMutex,
    previous_mvcc_value: Option<usize>,
}

impl PartialEq for MVCCMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.begin_ts == other.begin_ts
            && self.end_ts == other.end_ts
            && self.last_read == other.last_read
    }
}

// Getters and setters for the struct
// We don't want public API access on this thing, because there's no locking whatsoever.
// Therefore, should open read-only access.
impl MVCCMetadata {
    pub(crate) fn get_beg_time(&self) -> Timestamp {
        self.begin_ts
    }

    pub(crate) fn get_prev_mvcc<'a>(
        &self,
        ctx: &'a DbContext,
    ) -> Result<&'a mut ValueWithMVCC, String> {
        // Don't want to return erroneous values when another thread is doing a swap/mutating this value.
        // Because we're just reading previous_mvcc_value and not trusting that the actual String value is correct, we can bypass putting down a write intent.
        // This usually happens for reads.

        // Check that there are no write intents
        self.previous_mvcc_value
            .map(|a| ctx.old_values_store.get_mut(a))
            .ok_or_else(|| "MVCC Value doesn't exist".to_string())
    }

    pub(crate) fn remove_prev_mvcc(&self, ctx: &DbContext) -> ValueWithMVCC {
        ctx.old_values_store
            .get_mut(self.previous_mvcc_value.unwrap())
            .clone()
    }
    pub(crate) fn insert_prev_mvcc(&mut self, p0: usize) {
        self.previous_mvcc_value.replace(p0);
    }

    pub fn sorta_equal(&self, other: &Self) -> bool {
        self.begin_ts == other.begin_ts && self.end_ts == other.end_ts
    }
    pub fn get_end_time(&self) -> Timestamp {
        self.end_ts
    }
    pub fn set_end_time(&mut self, time: Timestamp) {
        self.end_ts = time;
    }
    pub fn get_last_read_time(&self) -> Timestamp {
        self.last_read.get()
    }
}

#[cfg(test)]
mod tests {
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::ReplicatedTxn;

    use super::*;
    use crate::db_context::create_replicated_context;

    #[test]
    fn check_read() {
        // Only for 5 -> Infinity
        let txn = LockDataRef {
            id: 0,
            timestamp: Timestamp::from(5),
        };
        let metadata = MVCCMetadata::new(txn);
        let emptymap = IntentMap::new();

        let mut txn1 = LockDataRef {
            id: 0,
            timestamp: Timestamp(2),
        };
        assert_matches!(metadata.check_read(&emptymap, txn1), Err(..));

        txn1.timestamp = Timestamp(5);
        assert_matches!(metadata.check_read(&emptymap, txn1), Ok(..));
    }

    #[test]
    fn check_read_with_active_txn() {
        let ctx = create_replicated_context();
        let mut txn1 = ReplicatedTxn::new_with_time(&ctx, Timestamp::from(5));
        let mut txnread = ReplicatedTxn::new_with_time(&ctx, Timestamp::from(10));
        let key1 = ObjectPath::new("key1");
        txn1.write(&key1, "value1".into()).unwrap();
        assert_matches!(txnread.read(&ObjectPath::from("key1")), Err(..));

        txn1.commit();
        assert_eq!(txnread.read(&"key1".into()), Ok("value1".into()));
    }

    #[test]
    fn check_later_read_means_failed_write() {
        let ctx = create_replicated_context();
        let key = ObjectPath::from("key1");

        {
            let mut txninit = ReplicatedTxn::new_with_time(&ctx, Timestamp::from(1));
            txninit.write(&key, "whatever".into()).unwrap();
            txninit.commit();
        }

        let mut txn1 = ReplicatedTxn::new_with_time(&ctx, Timestamp::from(5));
        let mut txnread = ReplicatedTxn::new_with_time(&ctx, Timestamp::from(10));

        txnread.read(&key).unwrap();

        assert_matches!(txn1.write(&key, "should fail".into()), Err(..));
    }
}

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum WriteIntentStatus {
    Aborted,
    Pending,
    Committed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteIntent {
    pub associated_transaction: LockDataRef,
    pub was_commited: bool,
}

impl Display for MVCCMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "beg: {}, end: {}, lr: {}",
            self.begin_ts.to_string(),
            self.end_ts.to_string(),
            self.get_last_read_time().to_string(),
        ))
    }
}
