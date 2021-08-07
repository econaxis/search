use crate::{DbContext, create_empty_context};
use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::{ValueWithMVCC, DBTransaction, LockDataRef, Transaction};
use std::borrow::Cow;
use std::pin::Pin;
use crate::wal_watcher::WalTxn;
use std::collections::HashMap;
use std::cell::{RefCell, UnsafeCell};
use crate::timestamp::Timestamp;
use std::sync::{Mutex, MutexGuard};

mod rpc_handler;


pub struct ReplicatedDatabase {
    db: DbContext,
    transactions: Mutex<UnsafeCell<HashMap<LockDataRef, Mutex<Transaction>>>>,
}

unsafe impl Sync for ReplicatedDatabase{}
unsafe impl Send for ReplicatedDatabase{}

// Right now the slave database does nothing special except repeat requests from the master.
impl ReplicatedDatabase {
    pub fn new() -> Self {
        Self {
            db: create_empty_context(),
            // todo!: hack around multithreaded issues
            // inserting a new transaction causes reallocation which invalidates previous references
            // obviously we don't want to lock the whole thing up while running transactions, but also don't want to reallocate
            // possible fixes: use Slab allocator, use non reallocating data structures
            transactions: Mutex::new(UnsafeCell::new(HashMap::with_capacity(10000))),
        }
    }

    pub fn dump(&self) -> String {
        self.db.db.printdb()
    }

    pub fn new_with_time(&self, txn: &LockDataRef) {
        self.get_txn(txn);
    }

    fn get_txn(&self, txn: &LockDataRef) -> MutexGuard<'_, Transaction> {
        let lock = self.transactions.lock().unwrap();
        let mut txnmap = unsafe { &mut *lock.get().as_mut().unwrap() };

        let rwtxn = txnmap.entry(*txn).or_insert_with(|| {
            Mutex::new(Transaction::new_with_time(&self.db, txn.timestamp))
        });
        rwtxn.lock().unwrap()
    }
    fn take_txn(&self, txn: LockDataRef) -> Option<Transaction> {

        let _l = self.get_txn(&txn);
        let lock = self.transactions.lock().unwrap();
        let mut txnmap = unsafe { &mut *lock.get().as_mut().unwrap() };
        let res = txnmap.remove(&txn);
        std::mem::drop(_l);
        res.map(|a| a.into_inner().unwrap())
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
    pub fn commit(&self, txn: LockDataRef) -> Result<(), String> {
        let mut rwtxn = self.get_txn(&txn);
        rwtxn.commit(&self.db)
    }
    pub fn abort(&self, p0: LockDataRef) {
        let mut rwtxn = self.get_txn(&p0);
        rwtxn.abort(&self.db);
    }
}