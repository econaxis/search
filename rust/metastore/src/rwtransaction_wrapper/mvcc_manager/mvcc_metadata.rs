use std::fmt::{Debug, Display, Formatter, Write};

use super::lock_data_manager::{IntentMap, LockDataRef};
use crate::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{LockResult, Mutex, MutexGuard, TryLockResult, RwLock, RwLockReadGuard, RwLockWriteGuard};
use crate::DbContext;
use crate::rwtransaction_wrapper::ValueWithMVCC;

#[derive(Debug, PartialEq, Clone)]
pub enum WriteIntentError {
    Aborted(LockDataRef),
    PendingIntent(LockDataRef),
    Other(String),
}


crate::custom_error_impl!(WriteIntentError);


impl WriteIntentError {
    pub fn tostring(self) -> String {
        // if matches!(self, WriteIntentError::Aborted(_)) {
        //     panic!("aborted error should've been handled");
        // }
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
        if curwi.as_ref().map_or(None, |a| Some(a.associated_transaction)) != Some(compare) {
            return Err("Write intent not equals to compar, can't swap".to_string());
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
            last_read: Timestamp::mintime(),
            cur_write_intent: WriteIntentMutex::new(Some(WriteIntent {
                associated_transaction: txn,
                was_commited: false,
            })),
            previous_mvcc_value: None,
            read_cnt: ReadCounter::new(),
        }
    }

    pub(super) fn get_write_intents(
        &self,
    ) -> Option<WriteIntent> {
        self.cur_write_intent.read()
    }

    pub(super) fn check_write_intents(
        &mut self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), WriteIntentError> {
        let curwriteintent = self.cur_write_intent.read();
        match curwriteintent {
            None => Ok(()),
            Some(wi) if wi.associated_transaction == cur_txn => Ok(()),
            Some(WriteIntent {
                     associated_transaction,
                     ..
                 }) => {
                match txnmap
                    .get_by_ref(&associated_transaction)
                    .unwrap()
                    .get_write_intent()
                {
                    // If the transaction is committed already, then all good.
                    WriteIntentStatus::Committed => {
                        // All good, we write after the txn has committed
                        self.cur_write_intent.compare_swap_none(curwriteintent, None).map_err(|a| WriteIntentError::Other(a))?;
                        Ok(())
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

    pub(super) fn check_write(
        &mut self,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        if self.end_ts != Timestamp::maxtime() {
            // This record is not the latest, so we can't write to it.
            return Err("Trying to write on a historical MVCC record".to_string());
        }

        if cur_txn.timestamp < self.last_read {
            Err("Timestamp smaller than last read".to_string())
        } else if cur_txn.timestamp < self.begin_ts {
            Err("Timestamp smaller than begin time".to_string())
        } else {
            Ok(())
        }
    }

    pub(super) fn into_newer(&mut self, timestamp: Timestamp) -> Self {
        // We should have a lock on this to access the inner.
        assert!(self.cur_write_intent.read().is_some());

        // Clone acquires a write lock itself, so to prevent deadlock, we acquire write lock after clone
        let mut old = self.clone();

        // Just to prevent others from reading (e.g. clone), write lock
        let mut intent = self.cur_write_intent.write_block();
        self.begin_ts = timestamp;
        old.end_ts = timestamp;

        // Can't write to a value that has already been read before us.
        // Should've been checked before in `check_read`. we check again for correctness, just in case
        assert!(self.last_read <= timestamp);
        self.last_read = timestamp;


        old.cur_write_intent.compare_swap_none(intent.clone(), None).unwrap();


        // Because we're creating a newer value, newer value must be not committed
        intent.as_mut().unwrap().was_commited = false;

        if !(old.begin_ts <= old.end_ts) {
            println!("{:?} {:?}", old, self);
            panic!()
        }
        assert!(self.begin_ts <= self.end_ts);

        old
    }

    pub(super) fn check_read(
        &mut self,
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

    pub(super) fn confirm_read(&mut self, cur_txn: LockDataRef) {
        self.last_read = self.last_read.max(cur_txn.timestamp);
    }

    #[cfg(test)]
    pub fn check_matching_timestamps(&self, other: &Self) -> bool {
        self.end_ts == other.end_ts
            && self.begin_ts == other.begin_ts
            && self.last_read == other.last_read
    }
}


struct WriteIntentMutex(Mutex<Option<WriteIntent>>);

impl PartialEq for WriteIntentMutex {
    fn eq(&self, other: &Self) -> bool {
        if self.read() == other.read() {
            true
        } else {
            false
        }
    }
}

pub fn is_repeatable(a: &MVCCMetadata, b: &MVCCMetadata) -> bool {
    a.begin_ts == b.begin_ts &&
        a.end_ts == b.end_ts &&
        a.cur_write_intent == b.cur_write_intent &&
        a.previous_mvcc_value == b.previous_mvcc_value
}

impl Clone for WriteIntentMutex {
    fn clone(&self) -> Self {
        Self(Mutex::new(self.read()))
    }
}

impl WriteIntentMutex {
    pub(crate) fn compare_swap_none(&self, compare: Option<WriteIntent>, swap: Option<WriteIntent>) -> Result<(), String> {
        let mut writer = self.write_block();

        if &*writer == &compare {
            let oldvalue = std::mem::replace(&mut *writer, swap);
            assert_eq!(oldvalue, compare);
            Ok(())
        } else if &*writer == &swap {
            Ok(())
        } else {
            Err("Another thread has replaced this value".to_string())
        }
    }
}

impl WriteIntentMutex {
    pub fn read(&self) -> Option<WriteIntent> {
        self.0.lock().unwrap().clone()
    }
    pub(crate) fn try_read(&self) -> TryLockResult<MutexGuard<'_, Option<WriteIntent>>> {
        self.0.try_lock()
    }
    pub fn write_block(&self) -> MutexGuard<'_, Option<WriteIntent>> {
        self.0.lock().unwrap()
    }
    pub fn new(wi: Option<WriteIntent>) -> Self {
        Self(Mutex::new(wi))
    }
}

impl Default for WriteIntentMutex {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Debug for WriteIntentMutex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.try_read()
            .map(|a| f.write_fmt(format_args!("{:?}", &*a)));
        Ok(())
    }
}

static ORD: Ordering = Ordering::SeqCst;

#[derive(Serialize, Deserialize, Debug)]
struct ReadCounter {
    data: RwLock<()>,
}

impl ReadCounter {
    fn new() -> ReadCounter {
        ReadCounter {
            data: RwLock::new(()),
        }
    }
}

impl ReadCounter {
    pub fn read(&self) -> RwLockReadGuard<'_, ()> {
        self.data.read().unwrap()
    }
    pub fn update(&self) -> RwLockWriteGuard<'_, ()> {
        self.data.write().unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MVCCMetadata {
    begin_ts: Timestamp,
    end_ts: Timestamp,
    last_read: Timestamp,
    #[serde(skip)]
    cur_write_intent: WriteIntentMutex,
    previous_mvcc_value: Option<usize>,
    read_cnt: ReadCounter,
}

impl MVCCMetadata {
    pub(crate) fn acquire_lock(&self) -> MutexGuard<'_, Option<WriteIntent>> {
        self.cur_write_intent.write_block()
    }
}

impl PartialEq for MVCCMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.begin_ts == other.begin_ts &&
            self.end_ts == other.end_ts &&
            self.last_read == other.last_read
    }
}

impl Clone for MVCCMetadata {
    fn clone(&self) -> Self {
        let guard = self.cur_write_intent.write_block();
        Self {
            begin_ts: self.begin_ts,
            end_ts: self.end_ts,
            last_read: self.last_read,
            cur_write_intent: WriteIntentMutex::new(guard.clone()),
            previous_mvcc_value: self.previous_mvcc_value,
            read_cnt: ReadCounter::new(),
        }
    }
}

impl MVCCMetadata {
    pub(crate) fn get_beg_time(&self) -> Timestamp {
        self.begin_ts
    }
    pub(crate) fn get_prev_mvcc<'a>(&self, ctx: &'a DbContext) -> Result<&'a mut ValueWithMVCC, String> {
        // Don't want to return erroneous values when another thread is doing a swap/mutating this value.
        // Because we're just reading previous_mvcc_value and not trusting that the actual String value is correct, we can bypass putting down a write intent.
        // This usually happens for reads.

        // Check that there are no write intents
        self.previous_mvcc_value.map(|a| {
            ctx.old_values_store.get_mut(a)
        }).ok_or("MVCC Value doesn't exist".to_string())
    }

    pub(crate) fn remove_prev_mvcc(&self, ctx: &DbContext) -> ValueWithMVCC {
        let guard = self.cur_write_intent.write_block();
        ctx.old_values_store.remove(self.previous_mvcc_value.unwrap())
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

    pub fn read(&self) -> RwLockReadGuard<'_, ()> { self.read_cnt.read() }
    pub fn write(&self) -> RwLockWriteGuard<'_, ()> { self.read_cnt.update() }
}

#[cfg(test)]
mod tests {
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::RWTransactionWrapper;
    use crate::*;

    use super::*;

    #[test]
    fn check_read() {
        // Only for 5 -> Infinity
        let txn = LockDataRef {
            id: 0,
            timestamp: Timestamp::from(5),
        };
        let mut metadata = MVCCMetadata::new(txn);
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
        let ctx = create_empty_context();
        let mut txn1 = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(5));
        let mut txnread = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(10));
        let key1 = ObjectPath::new("key1");
        txn1.write(&key1, "value1".into()).unwrap();
        assert_matches!(txnread.read(ObjectPath::from("key1").as_cow_str()), Err(..));

        txn1.commit();
        assert_eq!(txnread.read("key1".into()), Ok("value1".to_string()));
    }

    #[test]
    fn check_later_read_means_failed_write() {
        let ctx = create_empty_context();
        let key = ObjectPath::from("key1");

        {
            let mut txninit = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(1));
            txninit.write(&key, "whatever".into()).unwrap();
            txninit.commit();
        }

        let mut txn1 = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(5));
        let mut txnread = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(10));

        txnread.read(key.as_cow_str()).unwrap();

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
            self.last_read.to_string(),
        ))
    }
}
