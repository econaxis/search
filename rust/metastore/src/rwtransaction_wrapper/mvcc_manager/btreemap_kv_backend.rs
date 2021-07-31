use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::btree_map::{BTreeMap, Range};
use std::ops::RangeBounds;

use crate::rwtransaction_wrapper::mvcc_manager::value_with_mvcc::ValueWithMVCC;
use crate::object_path::ObjectPath;
use std::collections::Bound;
use std::fmt::Write;
use std::sync::{Mutex, MutexGuard};

pub struct MutBTreeMap(Mutex<UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>);

unsafe impl Sync for MutBTreeMap {}

unsafe impl Send for MutBTreeMap {}

impl MutBTreeMap {
    pub fn lock_for_write(&self) -> MutexGuard<UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>> {
        self.0.lock().unwrap()
    }

    pub fn remove_key(&self, _key: &ObjectPath) {}

    pub fn new() -> Self {
        Self(Mutex::new(UnsafeCell::new(BTreeMap::new())))
    }

    pub fn range<R>(&self, range: R) -> Range<'_, ObjectPath, ValueWithMVCC>
        where
            R: RangeBounds<ObjectPath>,
    {
        unsafe { &*self.0.lock().unwrap().get() }.range(range)
    }

    pub fn range_with_lock<R>(&self, range: R) -> (MutexGuard<'_, UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>, Range<'_, ObjectPath, ValueWithMVCC>)
        where
            R: RangeBounds<ObjectPath>,
    {
        let lock = self.0.lock().unwrap();
        let range = unsafe { &*lock.get() }.range(range);

        (lock, range)
    }

    pub fn get_mut<T>(&self, key: &T) -> Option<&mut ValueWithMVCC>
        where
            ObjectPath: Borrow<T> + Ord,
            T: Ord + ?Sized,
    {
        unsafe { &mut *self.0.lock().unwrap().get() }.get_mut(key)
    }
    pub fn get_mut_with_lock<T>(&self, key: &T) -> (MutexGuard<'_, UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>>, Option<&mut ValueWithMVCC>)
        where
            ObjectPath: Borrow<T> + Ord,
            T: Ord + ?Sized,
    {
        let lock = self.0.lock().unwrap();
        let s = unsafe { &mut *lock.get() };

        (lock, s.get_mut(key))
    }

    pub fn insert(&self, key: ObjectPath, value: ValueWithMVCC) -> Option<ValueWithMVCC> {
        let lock = self.0.lock().unwrap();

        unsafe { &mut *lock.get() }.insert(key, value)
    }

    pub fn iter(&self) -> Range<ObjectPath, ValueWithMVCC> {
        let min = Bound::Included(ObjectPath::new("\x01"));
        let max = Bound::Included(ObjectPath::new("\x7f"));

        self.range((min, max))
    }


    // Prints the database to stdout
    pub fn printdb(&self) -> String {
        let mut str: String = String::new();


        for (key, value) in self.iter() {
            str.write_fmt(format_args!(
                "{}: ({}) {}\n",
                key.to_string(),
                value.as_inner().0,
                value.as_inner().1
            ))
                .unwrap();
        }
        str
    }
}

