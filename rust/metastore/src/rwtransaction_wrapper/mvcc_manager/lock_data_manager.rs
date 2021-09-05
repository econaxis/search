use std::collections::HashMap;

use super::mvcc_metadata::WriteIntentStatus;
use crate::timestamp::Timestamp;
use std::sync::RwLock;

use parking_lot::Mutex;
use std::fmt::{Debug, Formatter};

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub struct LockDataRef {
    pub id: u64,
    pub timestamp: Timestamp,
}

impl LockDataRef {
    pub fn debug_new(id: u64) -> Self {
        Self {
            id,
            timestamp: Timestamp::from(id),
        }
    }
}

impl TransactionLockData {
    pub fn get_write_intent(&self) -> WriteIntentStatus {
        self.0
    }
}

#[derive(Default)]
pub struct IntentMap(RwLock<HashMap<LockDataRef, TransactionLockData>>, Mutex<()>);

impl Debug for IntentMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.read().unwrap().iter().for_each(|(a, b)| {
            f.write_fmt(format_args!("{}: {:?}, ", a.id, b)).unwrap();
        });
        Ok(())
    }
}

unsafe impl Send for IntentMap {}

impl IntentMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_txn_status(
        &self,
        txn: LockDataRef,
        status: WriteIntentStatus,
    ) -> Result<(), String> {
        let prev = self
            .0
            .write()
            .unwrap()
            .insert(txn, TransactionLockData(status))
            .map(|a| a.0);
        if prev == Some(WriteIntentStatus::Pending) || status == WriteIntentStatus::Aborted {
            Ok(())
        } else {
            Err("Previous wi is not pending".to_string())
        }
    }

    pub fn make_write_txn(&self) -> LockDataRef {
        let txn = TransactionLockData(WriteIntentStatus::Pending);
        let timestamp = Timestamp::now();
        let txnref = LockDataRef {
            id: timestamp.0,
            timestamp,
        };
        self.0.write().unwrap().insert(txnref, txn);
        txnref
    }

    pub fn generate_read_txn_with_time(time: Timestamp) -> LockDataRef {
        LockDataRef {
            id: time.0,
            timestamp: time,
        }
    }

    pub fn get_by_ref(&self, l: &LockDataRef) -> Option<TransactionLockData> {
        self.0.read().unwrap().get(l).cloned()
    }
    pub fn make_write_txn_with_time(&self, timestamp: Timestamp, id: u64) -> LockDataRef {
        let txn = TransactionLockData(WriteIntentStatus::Pending);

        let txnref = LockDataRef { id, timestamp };
        self.0.write().unwrap().insert(txnref, txn);
        txnref
    }
}

// Contains only write intent status for now, but may contain more in the future.
#[derive(Clone)]
pub struct TransactionLockData(pub WriteIntentStatus);

impl Debug for TransactionLockData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.0))
    }
}
