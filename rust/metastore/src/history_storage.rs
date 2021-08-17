use std::cell::UnsafeCell;
use std::sync::Mutex;
use crate::rwtransaction_wrapper::ValueWithMVCC;

pub struct MutSlab(Mutex<UnsafeCell<slab::Slab<ValueWithMVCC>>>);

impl MutSlab {
    pub fn get_mut(&self, key: usize) -> &mut ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }
            .get_mut(key)
            .unwrap()
    }

    pub fn remove(&self, key: usize) -> ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }.remove(key)
    }
    pub fn new() -> Self {
        Self(Mutex::new(UnsafeCell::new(slab::Slab::with_capacity(10000))))
    }
    pub fn insert(&self, v: ValueWithMVCC) -> usize {
        unsafe { &mut *self.0.lock().unwrap().get() }.insert(v)
    }
}