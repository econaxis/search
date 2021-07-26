use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::btree_map::{BTreeMap, Range, RangeMut};
use std::ops::RangeBounds;

use crate::kv_backend::ValueWithMVCC;
use crate::object_path::ObjectPath;
use std::collections::Bound;

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
        let min = Bound::Included(ObjectPath([0u8].to_vec()));
        let max = Bound::Included(ObjectPath([127u8].to_vec()));

        self.range((min, max))
    }
}
