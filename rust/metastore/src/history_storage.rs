use std::cell::UnsafeCell;
use std::sync::Mutex;
use crate::rwtransaction_wrapper::ValueWithMVCC;

pub struct MutSlab(Mutex<UnsafeCell<slab::Slab<ValueWithMVCC>>>);

impl Default for MutSlab {
    fn default() -> Self {
        Self(Mutex::new(UnsafeCell::new(slab::Slab::with_capacity(1))))
    }
}
impl MutSlab {
    #[allow(clippy::mut_from_ref)]
    pub fn get_mut(&self, key: usize) -> &mut ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }
            .get_mut(key)
            .unwrap()
    }

    pub fn remove(&self, key: usize) -> ValueWithMVCC {
        unsafe { &mut *self.0.lock().unwrap().get() }.remove(key)
    }
    pub fn new() -> Self {
        Default::default()
    }
    pub fn insert(&self, v: ValueWithMVCC) -> usize {
        unsafe { &mut *self.0.lock().unwrap().get() }.insert(v)
    }
}
