use crate::{DbContext};
use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::{ValueWithMVCC, LockDataRef, Transaction};
use std::borrow::Cow;


use std::collections::HashMap;
use std::cell::{UnsafeCell};

use parking_lot::{Mutex, MutexGuard};
use std::sync::atomic::Ordering::SeqCst;
use std::ops::{DerefMut, Deref};
use crate::db_context::create_empty_context;
use std::sync::atomic::AtomicU64;
use std::hint::spin_loop;

mod rpc_handler;

struct ConcurrentHashmap {
    lock: Mutex<()>,
    counter: AtomicU64,
    map: UnsafeCell<HashMap<LockDataRef, Mutex<Transaction>>>,
}
struct CounterGuard<'a>(&'a AtomicU64);
impl<'a> CounterGuard <'a> {
    pub fn new(a: &'a AtomicU64) -> Self {
        a.fetch_add(1, SeqCst);
        Self(a)
    }
}
struct HashmapGuard<'a> {
    inner: MutexGuard<'a, Transaction>,
    // drop order is important! counter must be dropped last
    counter: CounterGuard<'a>,
}

impl<'a> HashmapGuard<'a> {
    pub fn new(inner: MutexGuard<'a, Transaction>, counter: &'a AtomicU64) -> Self {
        let counter = CounterGuard::new(counter);
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
            map: UnsafeCell::new(HashMap::default()),
        }
    }
    pub fn insert(&self, k: LockDataRef, v: Transaction) {
        let _l = self.lock.lock();
        let map = unsafe { &mut *self.map.get().as_mut().unwrap() };

        while self.counter.load(SeqCst) != 0 {
            spin_loop();
        }

        map.insert(k, Mutex::new(v));
    }

    pub fn get(&self, k: &LockDataRef) -> Option<HashmapGuard<'_>> {
        let _l = self.lock.lock();
        let map = unsafe { &mut *self.map.get().as_mut().unwrap() };
        map.get(k).map(|a| HashmapGuard::new(a.lock(), &self.counter))
    }

    pub fn remove(&self, v: HashmapGuard<'_>) -> Option<Transaction> {
        let txn = v.inner.txn;
        std::mem::drop(v);
        let _l = self.lock.lock();
        let map = unsafe { &mut *self.map.get().as_mut().unwrap() };
        map.remove(&txn).map(|a| a.into_inner())
    }
}

pub struct SelfContainedDb {
    db: DbContext,
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
    pub fn get_inner(&self) -> &DbContext {
        &self.db
    }

    fn get_txn(&self, txn: &LockDataRef) -> HashmapGuard {
        match self.transactions.get(txn) {
            None => {
                self.transactions.insert(*txn, Transaction::new_with_time(&self.db, txn.timestamp));
                self.transactions.get(txn).unwrap()
            }
            Some(a) => a
        }
    }

    fn remove_txn(&self, a: HashmapGuard<'_>) -> Option<Transaction> {
        // todo: temporary solution
        // todo: must find a way to remove transactions, or else everything erorrs out.
        self.transactions.remove(a)
    }
}


/// Main transaction-related implementations
impl SelfContainedDb {
    pub fn new_transaction(&self, txn: &LockDataRef) {
        let _txn = self.get_txn(txn);
    }

    // todo: make new type instead of valuewithmvcc to represent a "safe", thread-local value
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
        self.remove_txn(rwtxn);
    }
    pub fn abort(&self, p0: LockDataRef) {
        let mut rwtxn = self.get_txn(&p0);
        rwtxn.abort(&self.db);
        self.remove_txn(rwtxn);
    }
}