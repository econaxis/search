use std::ffi::{c_void, CStr};
use std::slice;

use crate::cffi::{self, clone_one_index, ctypes};

pub struct C_SSK(*const ctypes::SortedKeysIndexStub);




unsafe impl Send for C_SSK {}
unsafe impl Sync for C_SSK {}

impl Clone for C_SSK {
    fn clone(&self) -> Self {
        let cloned = unsafe {clone_one_index(self.0)};
        Self (cloned)
    }
}


impl AsRef<*const ctypes::SortedKeysIndexStub> for C_SSK {
    fn as_ref(&self) -> &*const ctypes::SortedKeysIndexStub {
        &self.0
    }
}

impl C_SSK {
    #[allow(unused)]
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
pub extern fn fill_rust_vec(vec: *mut Vec<u8>, data: *const c_void, size: usize) {
    let vec = unsafe { &mut *vec };

    vec.resize(size, 0);
    let data_slice = unsafe { slice::from_raw_parts(data as *const u8, size) };
    vec.copy_from_slice(data_slice);
}