use std::ffi::{c_void, CStr};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice;

use crate::cffi::{self, clone_one_index, ctypes};

#[derive(Copy, Clone, Default, Debug)]
#[repr(C)]
pub struct DocumentPositionPointer_v3(pub u32, pub u32, pub u8);

#[derive(Debug, Clone)]
pub struct VecDPP(Vec<DocumentPositionPointer_v3>);


pub struct C_SSK(*const ctypes::SortedKeysIndexStub);




unsafe impl Send for C_SSK {}
unsafe impl Sync for C_SSK {}

impl Clone for C_SSK {
    fn clone(&self) -> Self {
        let cloned = unsafe {clone_one_index(self.0)};
        Self (cloned)
    }
}

impl VecDPP {
    pub fn new() -> Self {
        VecDPP(Vec::new())
    }
}

impl Deref for VecDPP {
    type Target = Vec<DocumentPositionPointer_v3>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VecDPP {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<*const ctypes::SortedKeysIndexStub> for C_SSK {
    fn as_ref(&self) -> &*const ctypes::SortedKeysIndexStub {
        &self.0
    }
}

impl C_SSK {
    pub fn from_file_suffix(suffix: &str) -> Self {
        let cstr = CStr::from_bytes_with_nul(suffix.as_bytes()).unwrap();
        let ssk = unsafe { cffi::load_one_index(cstr.as_ptr()) };
        Self(ssk)
    }
}

impl Drop for C_SSK {
    fn drop(&mut self) {
        unsafe { cffi::delete_one_index(self.0) };
    }
}

#[no_mangle]
pub extern fn fill_rust_vec(vec: *mut VecDPP, data: *const c_void, size: usize) {
    let vec = unsafe { &mut *vec };

    assert_eq!(size % mem::size_of::<DocumentPositionPointer_v3>(), 0);
    let dppsize = size / mem::size_of::<DocumentPositionPointer_v3>();
    vec.resize_with(dppsize, Default::default);

    let data_slice = unsafe { slice::from_raw_parts(data as *const DocumentPositionPointer_v3, dppsize) };
    vec.copy_from_slice(data_slice);
}