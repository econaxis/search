use std::fmt::{Display, Formatter, Write, Debug};


use super::lock_data_manager::{IntentMap, LockDataRef};
use crate::timestamp::Timestamp;
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicBool};
use std::sync::{LockResult, TryLockResult, Mutex, MutexGuard};


#[derive(Debug, PartialEq, Clone)]
pub enum WriteIntentError {
    Aborted(LockDataRef),
    WritingInThePast,
    PendingIntent(LockDataRef),
    Other(String),
}

impl Into<String> for WriteIntentError {
    fn into(self) -> String {
        let mut buf = String::new();
        buf.write_fmt(format_args!("{:?}", self));
        buf
    }
}

impl WriteIntentError {
    pub fn tostring(self) -> String {
        // if matches!(self, WriteIntentError::Aborted(_)) {
        //     panic!("aborted error should've been handled");
        // }
        self.into()
    }
}


impl MVCCMetadata {
    pub fn aborted_reset_write_intent(&mut self, compare: LockDataRef, new: Option<LockDataRef>) -> Result<(), String> {
        let mut writer = self.cur_write_intent.write_block();
        if writer.as_ref().unwrap().associated_transaction == compare {
            println!("Clearing {:?}", compare);
            let wi = new.map(|a| WriteIntent {associated_transaction: a});
            std::mem::replace(&mut *writer, wi);
            // writer.replace(WriteIntent {associated_transaction: new});
            Ok(())
        } else {
            Err("Compare value not same".to_string())
        }
    }

    pub fn atomic_insert_write_intents(&mut self, wi: WriteIntent) -> Result<(), String> {
        //println!("{} try lock", wi.associated_transaction.id);

        self.cur_write_intent.compare_swap_none(wi)

        // ATOMIC_LOCK_TEMP.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).unwrap();

        // } else {
        //     Err("Write intent is not none")
        // }
    }

    pub fn new(txn: LockDataRef) -> Self {
        Self {
            begin_ts: txn.timestamp,
            end_ts: Timestamp::maxtime(),
            last_read: Timestamp::mintime(),
            cur_write_intent: WriteIntentMutex::new(Some(WriteIntent { associated_transaction: txn })),
            previous_mvcc_value: None,
        }
    }

    pub(in super) fn get_write_intents(
        &self,
        txnmap: &IntentMap,
    ) -> Option<(WriteIntentStatus, WriteIntent)> {
        self.cur_write_intent.read().map(|wi| (txnmap.get_by_ref(&wi.associated_transaction).unwrap().get_write_intent(), wi.clone()))
    }

    pub(in super) fn check_write_intents(
        &mut self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), WriteIntentError> {
        // todo! make this to not write (read only) and do the commit taking later on.
        let curwriteintent = self.cur_write_intent.read();
        match curwriteintent {
            None => Ok(()),
            Some(wi) if wi.associated_transaction == cur_txn => Ok(()),
            Some(WriteIntent {
                     associated_transaction,
                 }) => {
                match txnmap.get_by_ref(&associated_transaction).unwrap().get_write_intent()
                {
                    // If the transaction is committed already, then all good.
                    WriteIntentStatus::Committed => {
                        if associated_transaction.timestamp < cur_txn.timestamp {
                            // All good, we write after the txn has committed
                            self.cur_write_intent.write().map(|mut a| {
                                match a.as_ref() {
                                    Some(w) if w.associated_transaction == associated_transaction => {
                                        a.take();
                                    }
                                    _ => {}
                                }
                            }).unwrap();
                            Ok(())
                        } else {
                            Err(WriteIntentError::WritingInThePast)
                        }
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

    pub(in super) fn check_write(
        &mut self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        self.check_read(txnmap, cur_txn)?;

        if self.end_ts != Timestamp::maxtime() {
            // This record is not the latest, so we can't write to it.
            return Err("Trying to write on a historical MVCC record".to_string());
        }

        if cur_txn.timestamp < self.last_read {
            Err("Timestamp bigger than last read".to_string())
        } else {
            Ok(())
        }
    }

    // todo!
    pub(super) fn into_newer(&mut self, timestamp: Timestamp) -> Self {
        // We should have a lock on this to access the inner.
        assert!(self.cur_write_intent.read().is_some());

        // Clone acquires a write lock itself, so to prevent deadlock, we acquire write lock after clone
        let mut old = self.clone();

        // Just to prevent others from reading (e.g. clone), write lock
        let _guard = self.cur_write_intent.write_block();
        self.begin_ts = timestamp;
        old.end_ts = timestamp;

        // Can't write to a value that has already been read before us.
        // Should've been checked before in `check_read`. we check again for correctness, just in case
        assert!(self.last_read <= timestamp);
        self.last_read = timestamp;


        assert!(old.begin_ts <= old.end_ts);
        assert!(self.begin_ts <= self.end_ts);

        old
    }


    pub(in super) fn check_read(
        &mut self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        if cur_txn.timestamp < self.begin_ts || cur_txn.timestamp > self.end_ts {
            return Err("Timestamp not valid".to_string());
        }
        self.check_write_intents(txnmap, cur_txn).map_err(WriteIntentError::tostring)?;
        Ok(())
    }

    pub(in super) fn confirm_read(&mut self, cur_txn: LockDataRef) {
        // todo: only set this when the reads commit
        // is this necessary for ACID?
        self.last_read = self.last_read.max(cur_txn.timestamp);
    }

    #[cfg(test)]
    pub fn check_matching_timestamps(&self, other: &Self) -> bool {
        self.end_ts == other.end_ts &&
            self.begin_ts == other.begin_ts &&
            self.last_read == other.last_read
    }
}

struct WriteIntentMutex(Mutex<Option<WriteIntent>>);

impl PartialEq for WriteIntentMutex {
    fn eq(&self, other: &Self) -> bool {
        if self.read() == other.read() {
            true
        } else {
            let _a = self.read();
            let _b = other.read();
            let _c = true;
            false
        }
    }
}

impl Clone for WriteIntentMutex {
    fn clone(&self) -> Self {
        Self(Mutex::new(self.read()))
    }
}


impl WriteIntentMutex {
    pub(crate) fn compare_swap_none(&self, wi: WriteIntent) -> Result<(), String> {
        let mut writer = self.write_block();
        match &*writer {
            None => {
                let prev = writer.replace(wi);
                assert!(prev.is_none());
                Ok(())
            }
            Some(oldwi) if oldwi.associated_transaction == wi.associated_transaction => {
                Ok(())
            }
            _ => {
                println!("warning: Write intent atomic error, another thread has replaced value, todo!");
                writer.replace(wi);
                Ok(())
            }
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
    pub fn write(&self) -> LockResult<MutexGuard<'_, Option<WriteIntent>>> {
        self.0.lock()
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
        self.try_read().map(|a|
            f.write_fmt(format_args!("{:?}", &*a))
        );
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct MVCCMetadata {
    begin_ts: Timestamp,
    end_ts: Timestamp,
    last_read: Timestamp,
    #[serde(skip)]
    cur_write_intent: WriteIntentMutex,
    #[serde(skip)]
    previous_mvcc_value: Option<usize>,
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
        }
    }
}

impl MVCCMetadata {
    pub(crate) fn get_prev_mvcc(&self) -> Option<usize> {
        // Don't want to return erroneous values when another thread is doing a swap/mutating this value.
        // Because we're just reading previous_mvcc_value and not trusting that the actual String value is correct, we can bypass putting down a write intent.
        // This usually happens for reads.
        let guard = self.cur_write_intent.write_block();
        self.previous_mvcc_value
    }
    pub(crate) fn insert_prev_mvcc(&mut self, p0: usize) {
        self.previous_mvcc_value.replace(p0);
    }

    // pub fn try_clone(&self, txn: LockDataRef) -> Result<Self, String> {
    //     unimplemented!();
    //     let wi = self.cur_write_intent.try_read().map_err(|_| "lock failed".to_string())?;
    //
    //     match &*wi {
    //         Some(wi) if wi.associated_transaction == txn => {}
    //         None => {}
    //         _ => return Err("Write intent still exists".to_string())
    //     };
    //     Ok(Self {
    //         begin_ts: self.begin_ts,
    //         end_ts: self.end_ts,
    //         last_read: self.last_read,
    //         cur_write_intent: WriteIntentMutex::new(wi.clone()),
    //         previous_mvcc_value: self.previous_mvcc_value,
    //     })
    // }
}


impl MVCCMetadata {
    pub fn sorta_equal(&self, other: &Self) -> bool {
        self.begin_ts == other.begin_ts &&
            self.end_ts == other.end_ts
    }
    pub fn get_end_time(&self) -> Timestamp {
        self.end_ts
    }
    pub fn set_end_time(&mut self, time: Timestamp) {
        self.end_ts = time;
    }
}


#[cfg(test)]
mod tests {
    use crate::*;
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::RWTransactionWrapper;

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


#[derive(PartialEq, Copy, Clone)]
pub enum WriteIntentStatus {
    Aborted,
    Pending,
    Committed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriteIntent {
    pub associated_transaction: LockDataRef,
}

#[cfg(test)]
impl Default for MVCCMetadata {
    fn default() -> Self {
        Self {
            begin_ts: Timestamp::mintime(),
            end_ts: Timestamp::maxtime(),
            last_read: Timestamp::mintime(),
            cur_write_intent: WriteIntentMutex::new(None),
            previous_mvcc_value: None,
        }
    }
}

impl Display for MVCCMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "beg: {}, end: {}, lr: {}",
            self.begin_ts.to_string(),
            self.end_ts.to_string(),
            self.last_read.to_string(),
            // self.cur_write_intent.read()
        ))
    }
}