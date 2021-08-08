use std::borrow::Borrow;
use std::cell::{UnsafeCell};
use std::collections::btree_map::{BTreeMap, Range};
use std::ops::RangeBounds;

use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::mvcc_manager::value_with_mvcc::ValueWithMVCC;
use std::collections::Bound;
use std::fmt::Write;
use std::sync::{RwLockReadGuard, RwLock};
use crate::timestamp::Timestamp;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

pub struct MutBTreeMap {
    btree: RwLock<UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>,
    time: AtomicU64,
}

unsafe impl Sync for MutBTreeMap {}

unsafe impl Send for MutBTreeMap {}

pub const TOMBSTONE: &str = "__null";

impl MutBTreeMap {
    pub fn remove_key(&self, _key: &ObjectPath) {}

    pub fn new() -> Self {
        Self { btree: RwLock::new(UnsafeCell::new(BTreeMap::new())), time: AtomicU64::new(Timestamp::mintime().0) }
    }

    pub fn range<R>(&self, range: R) -> Range<'_, ObjectPath, ValueWithMVCC>
        where
            R: RangeBounds<ObjectPath>,
    {
        unsafe { &*self.btree.read().unwrap().get() }.range(range)
    }

    pub fn range_with_lock<R>(
        &self,
        range: R,
        time: Timestamp
    ) -> (
        RwLockReadGuard<'_, UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>,
        Range<'_, ObjectPath, ValueWithMVCC>,
    )
        where
            R: RangeBounds<ObjectPath>,
    {
        let lock = self.btree.read().unwrap();
        let range = unsafe { &*lock.get() }.range(range);

        let prevtime = self.time.load(SeqCst);

        if time.0 > prevtime {
            self.time.store(time.0, SeqCst);
        }

        (lock, range)
    }

    pub fn is_deleated(a: &str) -> bool {
        return a == TOMBSTONE;
    }
    pub fn null_value_mapper(a: &mut ValueWithMVCC) -> Option<&mut ValueWithMVCC> {
        if Self::is_deleated(unsafe { a.get_val() }) {
            None
        } else {
            Some(a)
        }
    }
    pub fn get_mut<T>(&self, key: &T) -> Option<&mut ValueWithMVCC>
        where
            ObjectPath: Borrow<T> + Ord,
            T: Ord + ?Sized,
    {
        unsafe { &mut *self.btree.read().unwrap().get() }.get_mut(key).and_then(Self::null_value_mapper)
    }
    pub fn get_mut_with_lock<T>(
        &self,
        key: &T,
    ) -> (
        RwLockReadGuard<'_, UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>,
        Option<&mut ValueWithMVCC>,
    )
        where
            ObjectPath: Borrow<T> + Ord,
            T: Ord + ?Sized,
    {
        let lock = self.btree.read().unwrap();
        let s = unsafe { &mut *lock.get() };

        (lock, s.get_mut(key).and_then(Self::null_value_mapper))
    }

    pub fn insert(&self, key: ObjectPath, value: ValueWithMVCC, time: Timestamp) -> Result<Option<ValueWithMVCC>, String> {
        let lock = self.btree.write().unwrap();
        if self.time.load(SeqCst) > time.0 {
            return Err("Timestamp cache invalidated this txn, would lead to write anomalies".to_string())
        }

        Ok(unsafe { &mut *lock.get() }.insert(key, value))
    }

    pub fn iter(&self) -> Range<ObjectPath, ValueWithMVCC> {
        let min = Bound::Included(ObjectPath::new("\x01"));
        let max = Bound::Included(ObjectPath::new("\x7f"));

        self.range((min, max))
    }

    // Prints the database to stdout
    pub fn printdb(&self) -> String {
        let _lock = self.btree.read().unwrap();
        let mut str: String = String::new();

        for (key, value) in self.iter() {
            let (x, y) = value.as_inner();
            str.write_fmt(format_args!(
                "{} {:?} {}\n",
                key.to_string(),
                x.get_write_intents(),
                y
            ))
                .unwrap();
        }
        str
    }
}
