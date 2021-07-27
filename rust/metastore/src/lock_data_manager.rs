use std::collections::HashMap;

use crate::mvcc_manager::WriteIntentStatus;
use crate::timestamp::Timestamp;
use std::cell::RefCell;

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

impl LockDataRef {
    pub fn to_txn<'a>(
        &self,
        map: &'a HashMap<LockDataRef, TransactionLockData>,
    ) -> &'a TransactionLockData {
        map.get(self).unwrap()
    }
}

pub struct IntentMap(RefCell<HashMap<LockDataRef, TransactionLockData>>);

impl IntentMap {
    pub fn new() -> Self{
        Self(RefCell::new(HashMap::new()))
    }

    pub fn set_txn_status(&self, txn: LockDataRef, status: WriteIntentStatus) {
        self.0.borrow_mut().get_mut(&txn).unwrap().0 = status;
    }

    pub fn make_write_txn(&self) -> LockDataRef {
        let txn = TransactionLockData(WriteIntentStatus::Pending);
        let timestamp = Timestamp::now();
        let txnref = LockDataRef {
            id: timestamp.0,
            timestamp,
        };
        self.0.borrow_mut().insert(txnref, txn);
        txnref
    }
    pub fn make_read_txn() -> LockDataRef {
        let id = Timestamp::now();
        LockDataRef {
            id: id.0,
            timestamp: id,
        }
    }

    pub fn generate_read_txn_with_time(time: Timestamp) -> LockDataRef {
        LockDataRef {
            id: time.0,
            timestamp: time,
        }
    }

    pub fn get_by_ref(&self, l: &LockDataRef) -> Option<TransactionLockData> {
        self.0.borrow().get(l).map(|a| a.clone())
    }
}

#[cfg(test)]
impl IntentMap {
    pub fn make_write_txn_with_time(&self, timestamp: Timestamp) -> LockDataRef {
        let txn = TransactionLockData(WriteIntentStatus::Pending);
        let txnref = LockDataRef {
            id: timestamp.0,
            timestamp,
        };
        self.0.borrow_mut().insert(txnref, txn);
        txnref
    }
}
// Contains only write intent status for now, but may contain more in the future.
#[derive(Clone)]
pub struct TransactionLockData(pub WriteIntentStatus);
