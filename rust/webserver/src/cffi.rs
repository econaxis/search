use crate::RustVecInterface::{VecDPP};

use std::ffi;
use std::io::Read;
use std::os::raw::c_char;

use std::ffi::{CStr, OsStr};
use std::path::{Path, PathBuf};
use std::borrow::Borrow;
use serde::{Serialize, Deserialize};
use std::str::FromStr;
use std::hash::{Hash, Hasher};


pub mod ctypes {
    extern {
        pub type vector; // std::vector<DocIDFilePair> type.
    pub type ifstream; // ifstream type
    pub type SortedKeysIndexStub;
    }
}


// Contains FFI declarations for connecting to C++ shared library.
#[link(name = "c-search-abi")]
extern "C" {
    fn create_ifstream_from_path(path: *const c_char) -> *const ctypes::ifstream;
    fn deallocate_ifstream(stream: *const ctypes::ifstream);
    fn read_from_ifstream(stream: *const ctypes::ifstream, buffer: *mut c_char, max_len: u32);
    fn read_filepairs(stream: *const ctypes::ifstream, vecpointer: *const *const ctypes::vector, length: *const u32);
    fn deallocate_vec(ptr: *const ctypes::vector);
    fn copy_filepairs_to_buf(vec: *const ctypes::vector, buf: *const C_DocIDFilePair, max_length: u32);

    pub fn load_one_index(suffix_name: *const c_char) -> *mut ctypes::SortedKeysIndexStub;

    pub fn delete_one_index(ssk: *const ctypes::SortedKeysIndexStub);

    #[allow(improper_ctypes)]
    pub fn search_multi_indices(num_indices: i32, indices: *const *const ctypes::SortedKeysIndexStub, num_terms: i32, query_terms: *const *const c_char,
                                output_buffer: *const VecDPP);

    pub fn initialize_dir_vars();

    pub fn query_for_filename(index: *const ctypes::SortedKeysIndexStub, docid: u32, buffer: *const c_char, bufferlen: u32) -> u32;

    pub fn clone_one_index(other: *const ctypes::SortedKeysIndexStub) -> *const ctypes::SortedKeysIndexStub;
}


struct ifstream(pub *const ctypes::ifstream);


#[repr(C)]
#[derive(Clone)]
struct C_DocIDFilePair {
    docid: u32,
    cstr: *const c_char,
}

pub mod filepairs {
    use std::path::Path;
    use super::CVector;
    use crate::cffi::{DocIDFilePair, C_DocIDFilePair};

    #[allow(unused)]
    pub fn get_filepairs<P: AsRef<Path>>(path: P) -> Vec<DocIDFilePair> {
        let mut cvec = unsafe { CVector::new_from_path(path) };
        cvec.buffer.drain(0..).map(|i: C_DocIDFilePair| {
            i.into()
        }).collect()
    }
}

#[derive(Default, Serialize, Deserialize, Debug, Eq)]
pub struct DocIDFilePair {
    pub docid: u32,
    pub path: PathBuf,
    pub filemap_path: Option<PathBuf>,
    pub bytes: Option<u32>,
    pub num_words: Option<u32>,
}

impl Borrow<OsStr> for DocIDFilePair {
    fn borrow(&self) -> &OsStr {
        &self.path.as_os_str()
    }
}

impl Hash for DocIDFilePair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.as_os_str().hash(state);
        state.finish();
    }
}

impl PartialEq for DocIDFilePair {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}


impl<'a> Into<(u32, &'a str)> for C_DocIDFilePair {
    fn into(self) -> (u32, &'a str) {
        let cstr = unsafe {
            CStr::from_ptr(self.cstr)
        };
        (self.docid, cstr.to_str().unwrap())
    }
}

impl Into<DocIDFilePair> for C_DocIDFilePair {
    fn into(self) -> DocIDFilePair {
        let tup: (u32, _) = self.into();
        let path = PathBuf::from_str(tup.1).unwrap();
        DocIDFilePair {
            docid: tup.0,
            path,
            ..Default::default()
        }
    }
}

impl Default for C_DocIDFilePair {
    fn default() -> Self {
        Self {
            docid: 0,
            cstr: std::ptr::null(),
        }
    }
}

trait CObj_Drop<T> {
    fn deallocate(&self);
}

struct CVector<T> {
    pub buffer: Vec<T>,
    vectorlocation: *const ctypes::vector,
}

impl<T> CObj_Drop<T> for CVector<T> {
    default fn deallocate(&self) {
        unreachable!()
    }
}

impl CObj_Drop<C_DocIDFilePair> for CVector<C_DocIDFilePair> {
    fn deallocate(&self) {
        unsafe { deallocate_vec(self.vectorlocation) };
    }
}

impl CVector<C_DocIDFilePair> {
    pub unsafe fn new(stream: &ifstream) -> Self {
        let vecpointer: *const *const ctypes::vector = std::ptr::null();
        let length: u32 = 0;
        read_filepairs(stream.as_ctypes(), vecpointer, &length as *const u32);

        let mut buf = Vec::new();
        buf.resize(length as usize, C_DocIDFilePair::default());
        let ptrloc = buf.as_slice().as_ptr() as *const C_DocIDFilePair;
        copy_filepairs_to_buf(*vecpointer, ptrloc, length);

        CVector {
            buffer: buf,
            vectorlocation: *vecpointer,
        }
    }

    pub unsafe fn new_from_path<P: AsRef<Path>>(path: P) -> Self {
        let stream = ifstream::from_path(path);
        Self::new(&stream)
    }
}


impl<T> Drop for CVector<T> {
    fn drop(&mut self) {
        CObj_Drop::deallocate(self);
    }
}


impl ifstream {
    pub fn from_path<P: AsRef<Path>>(p: P) -> Self {
        let pc_char = p.as_ref().to_str().unwrap();
        let pc_char = ffi::CString::new(pc_char).unwrap();
        let stream = unsafe {
            create_ifstream_from_path(pc_char.as_ptr())
        };
        ifstream {
            0: stream
        }
    }
    fn as_ctypes(&self) -> *const ctypes::ifstream {
        self.0 as *const ffi::c_void as *const ctypes::ifstream
    }
}


impl Read for ifstream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let buflen = buf.len() as u32;
        unsafe {
            read_from_ifstream(self.0, &mut buf[0] as *mut u8 as *mut c_char, buflen);
        };
        Ok(buflen as usize)
    }
}


impl Drop for ifstream {
    fn drop(&mut self) {
        println!("Dropping ifstream");
        unsafe { deallocate_ifstream(self.0); };
    }
}

