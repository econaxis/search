use std::ffi::{c_void, CStr, CString};
use std::slice;
use std::mem;
use std::ops::{Deref, DerefMut};

use crate::cffi::{ctypes, self, c_char, search_index_top_n};

#[derive(Copy, Clone, Default, Debug)]
#[repr(C)]
pub struct DocumentPositionPointer_v3(pub u32, pub u32, pub u8);

#[derive(Debug, Clone)]
pub struct VecDPP(Vec<DocumentPositionPointer_v3>);


pub struct C_SSK(*const ctypes::SortedKeysIndexStub);

unsafe impl Send for C_SSK {}

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

    pub fn search_terms(&self, terms: &[String]) -> VecDPP {
        // Convert to acceptable *const *const c_char type.
        let strs: Vec<CString> = terms.into_iter().map(|x| {
            CString::new(x.as_bytes()).unwrap()
        }).collect();

        let chars: Vec<*const c_char> = strs.iter().map(|x| x.as_ptr()).collect();
        let chars: *const *const c_char = chars.as_ptr();

        let output_buf = VecDPP::new();

        unsafe { search_index_top_n(*self.as_ref(), &output_buf, terms.len() as i32, chars) }

        println!("{:?}", output_buf);

        output_buf
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