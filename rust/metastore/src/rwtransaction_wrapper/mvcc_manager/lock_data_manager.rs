use std::collections::HashMap;

use super::mvcc_metadata::WriteIntentStatus;
use crate::timestamp::Timestamp;
use std::sync::{RwLock, RwLockWriteGuard};
use std::collections::hash_map::RandomState;
use parking_lot::{Mutex, RawMutex};
use parking_lot::lock_api::MutexGuard;

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub struct LockDataRef {
    pub id: u64,
    pub timestamp: Timestamp,
}

impl TransactionLockData {
    pub fn get_write_intent(&self) -> WriteIntentStatus {
        self.0
    }
}

pub struct IntentMap(RwLock<HashMap<LockDataRef, TransactionLockData>>, Mutex<()>);

unsafe impl Send for IntentMap {}

impl IntentMap {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()), Mutex::new(()))
    }

    pub fn begin_atomic(&self) -> MutexGuard<'_, RawMutex, ()> {
        self.1.lock()
    }
    pub fn set_txn_status(&self, txn: LockDataRef, status: WriteIntentStatus) -> Option<WriteIntentStatus> {
        self.0.write().unwrap().insert(txn, TransactionLockData(status)).map(|a| a.0)
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
        let _l = self.begin_atomic();
        self.0.read().unwrap().get(l).map(|a| a.clone())
    }
}

impl IntentMap {
    pub fn make_write_txn_with_time(&self, timestamp: Timestamp) -> LockDataRef {
        let txn = TransactionLockData(WriteIntentStatus::Pending);
        let txnref = LockDataRef {
            id: timestamp.0,
            timestamp,
        };
        self.0.write().unwrap().insert(txnref, txn);
        txnref
    }
}

// Contains only write intent status for now, but may contain more in the future.
#[derive(Clone)]
pub struct TransactionLockData(pub WriteIntentStatus);
