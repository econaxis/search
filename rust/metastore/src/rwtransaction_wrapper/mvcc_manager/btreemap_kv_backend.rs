use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::btree_map::{BTreeMap, Range, RangeMut};
use std::ops::RangeBounds;

use super::kv_backend::ValueWithMVCC;
use crate::object_path::ObjectPath;
use std::collections::Bound;
use std::fmt::Write;

pub struct MutBTreeMap(UnsafeCell<BTreeMap<ObjectPath, ValueWithMVCC>>);

impl MutBTreeMap {
    pub fn new() -> Self {
        Self(UnsafeCell::new(BTreeMap::new()))
    }

    pub fn range<R>(&self, range: R) -> Range<'_, ObjectPath, ValueWithMVCC>
    where
        R: RangeBounds<ObjectPath>,
    {
        unsafe { &*self.0.get() }.range(range)
    }

    pub fn get_mut<T>(&self, key: &T) -> Option<&mut ValueWithMVCC>
    where
        ObjectPath: Borrow<T> + Ord,
        T: Ord + ?Sized,
    {
        unsafe { &mut *self.0.get() }.get_mut(key)
    }

    pub fn range_mut<R>(&self, range: R) -> RangeMut<'_, ObjectPath, ValueWithMVCC>
    where
        R: RangeBounds<ObjectPath>,
    {
        unsafe { &mut *self.0.get() }.range_mut(range)
    }

    pub fn insert(&self, key: ObjectPath, value: ValueWithMVCC) -> Option<ValueWithMVCC> {
        unsafe { &mut *self.0.get() }.insert(key, value)
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
            println!("Key: {}", key.as_str());
            str.write_fmt(format_args!(
                "{}: ({}) {}\n",
                key.to_string(),
                value.0,
                value.1
            ))
                .unwrap();
        }
        str
    }

}

