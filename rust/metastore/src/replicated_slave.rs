use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::{LockDataRef, Transaction, ValueWithMVCC};
use crate::DbContext;

use std::cell::UnsafeCell;
use std::collections::HashMap;

use crate::db_context::{create_empty_context, create_replicated_context};
use parking_lot::{FairMutex, FairMutexGuard, Mutex, RwLock};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

struct ConcurrentHashmap {
    lock: FairMutex<()>,
    counter: RwLock<()>,
    remove_queue: Mutex<Vec<LockDataRef>>,
    map: UnsafeCell<HashMap<LockDataRef, FairMutex<Transaction>>>,
}

struct CounterGuard<'a>(&'a AtomicU64);

impl<'a> CounterGuard<'a> {
    pub fn new(a: &'a AtomicU64) -> Self {
        a.fetch_add(1, SeqCst);
        Self(a)
    }
}

use parking_lot::RwLockReadGuard;

#[derive(Debug)]
struct HashmapGuard<'a> {
    inner: FairMutexGuard<'a, Transaction>,
    // drop order is important! counter must be dropped last
    counter: RwLockReadGuard<'a, ()>,
}

impl<'a> HashmapGuard<'a> {
    pub fn new(inner: FairMutexGuard<'a, Transaction>, counter: RwLockReadGuard<'a, ()>) -> Self {
        Self { inner, counter }
    }
}

impl<'a> Deref for HashmapGuard<'a> {
    type Target = Transaction;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<'a> DerefMut for HashmapGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl<'a> Drop for CounterGuard<'a> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, SeqCst);
    }
}

impl ConcurrentHashmap {
    pub fn new() -> Self {
        Self {
            lock: Default::default(),
            counter: Default::default(),
            remove_queue: Default::default(),
            map: UnsafeCell::new(HashMap::default()),
        }
    }
    pub fn insert(&self, k: LockDataRef, v: Transaction) {
        let _lock = self.counter.write();
        let map = unsafe { &mut *self.map.get().as_mut().unwrap() };

        map.insert(k, FairMutex::new(v));
    }

    pub fn get(&self, k: &LockDataRef) -> Option<HashmapGuard<'_>> {
        let map = unsafe { &mut *self.map.get().as_mut().unwrap() };
        let counter = self.counter.read();
        let _l = self.lock.lock();
        map.get(k).map(|a| HashmapGuard::new(a.lock(), counter))
    }

    pub fn remove(&self, v: HashmapGuard<'_>) {
        // There might be error here as someone might try to lock the removed version while we don't have the hashmapguard lock.
        let mut queue = self.remove_queue.lock();
        let txn = v.inner.txn;
        queue.push(txn);
        std::mem::drop(v);

        if queue.len() > 50 {
            let mut queue_cloned = queue.clone();
            std::mem::drop(queue);
            let _lock = self.counter.write();
            let _l = self.lock.lock();
            let map = unsafe { &mut *self.map.get().as_mut().unwrap() };
            queue_cloned.drain(..).for_each(|txn| {
                map.remove(&txn);
            });
        }
    }
}

pub struct SelfContainedDb {
    pub db: DbContext,
    transactions: ConcurrentHashmap,
}

unsafe impl Sync for SelfContainedDb {}

unsafe impl Send for SelfContainedDb {}

// Utility implementation
impl Default for SelfContainedDb {
    fn default() -> Self {
        Self {
            db: create_empty_context(),
            transactions: ConcurrentHashmap::new(),
        }
    }
}

impl SelfContainedDb {
    pub fn new_with_replication() -> Self {
        Self::new(create_replicated_context())
    }
    pub fn new(db: DbContext) -> Self {
        Self {
            db,
            transactions: ConcurrentHashmap::new(),
        }
    }
    pub fn get_inner(&self) -> &DbContext {
        &self.db
    }

    fn get_txn(&self, txn: &LockDataRef) -> HashmapGuard {
        match self.transactions.get(txn) {
            None => {
                // Used incorrectly.
                panic!("Transaction doesn't exist")
            }
            Some(a) => a,
        }
    }
    fn create_txn(&self, txn: &LockDataRef) -> HashmapGuard {
        match self.transactions.get(txn) {
            None => {
                self.transactions.insert(
                    *txn,
                    Transaction::new_with_time_id(&self.db, txn.timestamp, txn.id),
                );
                self.transactions.get(txn).unwrap()
            }
            Some(a) => {
                log::error!("Transaction {:?}", a);
                panic!("Transaction already exists")
            },
        }
    }

    fn remove_txn(&self, a: HashmapGuard<'_>) {
        self.transactions.remove(a)
    }
}

use super::rwtransaction_wrapper::TypedValue;
use crate::rpc_handler::{DatabaseInterface, NetworkResult};

/// Main transaction-related implementations
impl DatabaseInterface for SelfContainedDb {
    fn new_transaction(&self, txn: &LockDataRef) -> NetworkResult<(), String> {
        let _txn = self.create_txn(txn);
        NetworkResult::default()
    }

    // todo: make new type instead of valuewithmvcc to represent a "safe", thread-local value
    fn serve_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<ValueWithMVCC, String> {
        let mut rwtxn = self.get_txn(&txn);
        NetworkResult::from(rwtxn.read_mvcc(&self.db, key))
    }
    fn serve_range_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        let mut rwtxn = self.get_txn(&txn);
        NetworkResult::from(rwtxn.read_range_owned(&self.db, key))
    }

    fn serve_write(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
        value: TypedValue,
    ) -> NetworkResult<(), String> {
        let mut rwtxn = self.get_txn(&txn);
        NetworkResult::from(rwtxn.write(&self.db, key, value))
    }
    fn commit(&self, txn: LockDataRef) -> NetworkResult<(), String> {
        let mut rwtxn = self.get_txn(&txn);
        rwtxn.commit(&self.db).unwrap();
        self.remove_txn(rwtxn);
        NetworkResult::default()
    }
    fn abort(&self, p0: LockDataRef) -> NetworkResult<(), String> {
        let mut rwtxn = self.get_txn(&p0);
        rwtxn.abort(&self.db);
        self.remove_txn(rwtxn);
        NetworkResult::default()
    }
}
