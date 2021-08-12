use crate::{DbContext};
use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::{ValueWithMVCC, LockDataRef, Transaction, IntentMap};
use std::borrow::Cow;


use std::collections::HashMap;
use std::cell::{UnsafeCell};

use std::sync::{Mutex, MutexGuard, LockResult};
use parking_lot::RawMutex;
use std::ops::{DerefMut};
use crate::db_context::create_empty_context;

mod rpc_handler;


pub struct ReplicatedDatabase {
    db: DbContext,
    transactions: Mutex<UnsafeCell<HashMap<LockDataRef, Mutex<Transaction>>>>,
}

unsafe impl Sync for ReplicatedDatabase {}

unsafe impl Send for ReplicatedDatabase {}

impl ReplicatedDatabase {
    pub fn debug_txnmap(&self) -> &IntentMap {
        &self.db.transaction_map
    }
}

// Right now the slave database does nothing special except repeat requests from the master.
impl ReplicatedDatabase {
    pub fn new() -> Self {
        Self {
            db: create_empty_context(),
            // todo!: hack around multithreaded issues
            // inserting a new transaction causes reallocation which invalidates previous references
            // obviously we don't want to lock the whole thing up while running transactions, but also don't want to reallocate
            // possible fixes: use Slab allocator, use non reallocating data structures
            // forgot about rehash!!!!
            transactions: Mutex::new(UnsafeCell::new(HashMap::with_capacity(1000000))),
        }
    }

    pub fn dump(&self) -> String {
        self.db.db.printdb()
    }



    fn get_txn(&self, txn: &LockDataRef) -> MutexGuard<'_, Transaction> {
        let lock = self.transactions.lock().unwrap();
        let txnmap = unsafe { &mut *lock.get().as_mut().unwrap() };

        if txnmap.len() as f32 > txnmap.capacity() as f32 * 0.8 {
            todo!("hashmap would reallocate and cause segfault, todo! until we implement replication for networked")
        }
        let rwtxn = txnmap.entry(*txn).or_insert_with(|| {
            Mutex::new(Transaction::new_with_time(&self.db, txn.timestamp))
        });
        rwtxn.lock().unwrap()
    }

    fn remove_txn(&self, mut a: MutexGuard<Transaction>, b: &LockDataRef) -> Transaction {
        // todo: temporary solution
        // todo: must find a way to remove transactions, or else everything erorrs out.
        let replacement = Transaction::new_with_time(&self.db, b.timestamp);
        let old = std::mem::replace(a.deref_mut(), replacement);
        old
    }

    pub fn new_with_time(&self, txn: &LockDataRef) {
        let _txn = self.get_txn(txn);
    }

    pub fn serve_read(&self, txn: LockDataRef, key: &ObjectPath) -> Result<ValueWithMVCC, String> {
        let mut rwtxn = self.get_txn(&txn);
        rwtxn.read_mvcc(&self.db, key)
    }
    pub fn serve_range_read(&self, txn: LockDataRef, key: &ObjectPath) -> Result<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let mut rwtxn = self.get_txn(&txn);
        rwtxn.read_range_owned(&self.db, key)
    }

    pub fn serve_write(&self, txn: LockDataRef, key: &ObjectPath, value: Cow<str>) -> Result<(), String> {
        let mut rwtxn = self.get_txn(&txn);
        rwtxn.write(&self.db, key, value)
    }
    pub fn commit(&self, txn: LockDataRef) {
        let mut rwtxn = self.get_txn(&txn);
        rwtxn.commit(&self.db);
        self.remove_txn(rwtxn, &txn);
    }
    pub fn abort(&self, p0: LockDataRef) {
        let mut rwtxn = self.get_txn(&p0);
        let mut rwtxn = self.remove_txn(rwtxn, &p0);
        rwtxn.abort(&self.db);
    }
}