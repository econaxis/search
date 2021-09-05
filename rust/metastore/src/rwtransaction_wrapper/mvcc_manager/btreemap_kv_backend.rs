use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::btree_map::{BTreeMap, Range};
use std::ops::RangeBounds;

use super::value_with_mvcc::ValueWithMVCC;
use super::TypedValue;
use crate::object_path::ObjectPath;
use crate::timestamp::Timestamp;
use std::collections::Bound;
use std::fmt::Write;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{RwLock, RwLockReadGuard};

type UnsafeMapType = UnsafeCell<MapType>;
type MapType = BTreeMap<ObjectPath, ValueWithMVCC>;

pub struct MutBTreeMap {
    btree: RwLock<UnsafeMapType>,
    time: AtomicU64,
}

unsafe impl Sync for MutBTreeMap {}

unsafe impl Send for MutBTreeMap {}

impl Default for MutBTreeMap {
    fn default() -> Self {
        Self {
            btree: RwLock::new(UnsafeCell::new(BTreeMap::new())),
            time: AtomicU64::new(Timestamp::mintime().0),
        }
    }
}

impl MutBTreeMap {
    pub fn remove_key(&self, _key: &ObjectPath) {}

    pub fn new() -> Self {
        Self::default()
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
        time: Timestamp,
    ) -> (
        RwLockReadGuard<'_, UnsafeMapType>,
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

    pub fn is_deleated(a: &TypedValue) -> bool {
        matches!(a, TypedValue::Deleted)
    }
    pub fn null_value_mapper(a: &mut ValueWithMVCC) -> Option<&mut ValueWithMVCC> {
        if Self::is_deleated(a.get_val()) {
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
        unsafe { &mut *self.btree.read().unwrap().get() }
            .get_mut(key)
            .and_then(Self::null_value_mapper)
    }
    pub fn get_mut_with_lock<T>(
        &self,
        key: &T,
    ) -> (
        RwLockReadGuard<'_, UnsafeMapType>,
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

    // Checks for phantoms by making sure our write time is larger than their read times.
    fn check_adjacent_keys(
        btree: &MapType,
        key: &ObjectPath,
        writetime: Timestamp,
    ) -> (Option<bool>, Option<bool>) {
        let prev = btree.range(..key).next_back();

        // maybe have to move this to mvcc_manager? btreemap shouldn't deal with concurrency-related issues at all.
        let prevbool = prev.map(|x| x.1.get_mvcc_copy().get_last_read_time() <= writetime);
        let next = btree.range(key..).next();
        let nextbool = next.map(|x| x.1.get_mvcc_copy().get_last_read_time() <= writetime);

        (prevbool, nextbool)
    }

    pub fn insert(
        &self,
        key: ObjectPath,
        value: ValueWithMVCC,
        time: Timestamp,
    ) -> Result<Option<ValueWithMVCC>, String> {
        // todo: inserts should be handled by mvcc manager
        let lock = self.btree.write().unwrap();
        let btree = unsafe { &mut *lock.get() };

        let (prev, next) = Self::check_adjacent_keys(btree, &key, time);

        let should_insert =
            // If there are no keys to the left and right (this is an empty database), then we must lock on the global timestamp instance.
            if prev.or(next).is_none() {
                assert!(btree.is_empty());
                // If there's nowhere that we can lock, this would lead to an error (checked by test `check_phantom3`)
                // Have to lock the global time instance instead.
                let ret = self.time.load(SeqCst) <= time.0;
                self.time.fetch_max(time.0, SeqCst);
                ret
            } else {
                prev.unwrap_or(true) && next.unwrap_or(true)
            };

        if should_insert {
            Ok(btree.insert(key, value))
        } else {
            Err("phantom detected".to_string())
        }
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

            if !matches!(y, TypedValue::Deleted) && x.get_write_intents().is_none() {
                str.write_fmt(format_args!(
                    "{} {:?} {:?}\n",
                    key.to_string(),
                    x.get_write_intents().as_ref().map(|a| a),
                    y
                ))
                .unwrap();
            }
        }
        str
    }
}
