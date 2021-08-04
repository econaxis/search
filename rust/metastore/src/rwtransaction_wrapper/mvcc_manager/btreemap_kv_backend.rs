use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::btree_map::{BTreeMap, Range};
use std::ops::RangeBounds;

use crate::object_path::ObjectPath;
use crate::rwtransaction_wrapper::mvcc_manager::value_with_mvcc::ValueWithMVCC;
use std::collections::Bound;
use std::fmt::Write;
use std::sync::{Mutex, MutexGuard, RwLockReadGuard, RwLock, RwLockWriteGuard};

pub struct MutBTreeMap(RwLock<UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>);

unsafe impl Sync for MutBTreeMap {}

unsafe impl Send for MutBTreeMap {}

pub const TOMBSTONE: &str = "__null";

impl MutBTreeMap {
    pub fn remove_key(&self, _key: &ObjectPath) {}

    pub fn new() -> Self {
        Self(RwLock::new(UnsafeCell::new(BTreeMap::new())))
    }

    pub fn range<R>(&self, range: R) -> Range<'_, ObjectPath, ValueWithMVCC>
        where
            R: RangeBounds<ObjectPath>,
    {
        unsafe { &*self.0.read().unwrap().get() }.range(range)
    }

    pub fn range_with_lock<R>(
        &self,
        range: R,
    ) -> (
        RwLockReadGuard<'_, UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>,
        Range<'_, ObjectPath, ValueWithMVCC>,
    )
        where
            R: RangeBounds<ObjectPath>,
    {
        let lock = self.0.read().unwrap();
        let range = unsafe { &*lock.get() }.range(range);

        (lock, range)
    }

    pub fn is_deleated(a: &str) -> bool {
        return a == TOMBSTONE;
    }
    pub fn null_value_mapper(a: &mut ValueWithMVCC) -> Option<&mut ValueWithMVCC> {
        if Self::is_deleated(unsafe {a.get_val()}) {
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
        unsafe { &mut *self.0.read().unwrap().get() }.get_mut(key).and_then(Self::null_value_mapper)
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
        let lock = self.0.read().unwrap();
        let s = unsafe { &mut *lock.get() };

        (lock, s.get_mut(key).and_then(Self::null_value_mapper))
    }

    pub fn insert(&self, key: ObjectPath, value: ValueWithMVCC) -> Option<ValueWithMVCC> {
        let lock = self.0.write().unwrap();

        unsafe { &mut *lock.get() }.insert(key, value)
    }

    pub fn iter(&self) -> Range<ObjectPath, ValueWithMVCC> {
        let min = Bound::Included(ObjectPath::new("\x01"));
        let max = Bound::Included(ObjectPath::new("\x7f"));

        self.range((min, max))
    }

    // Prints the database to stdout
    pub fn printdb(&self) -> String {
        let lock = self.0.read().unwrap();
        let mut str: String = String::new();

        for (key, value) in self.iter() {
            let (x, y) = value.as_inner();
            str.write_fmt(format_args!(
                "{}: ({}) {}\n",
                key.to_string(),
                x,
                y
            ))
                .unwrap();
        }
        str
    }
}
