use std::ffi;
use std::io::Read;
pub use std::os::raw::c_char;

use std::fmt::Error;

use std::ffi::{c_void, CStr};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::borrow::Borrow;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::ser::SerializeTuple;
use std::str::FromStr;

mod ctypes {
    extern {
        pub type vector; // std::vector<DocIDFilePair> type
        pub type ifstream; // ifstream type
    }
}


#[link(name = "c-search-abi")]
extern "C" {
    fn create_ifstream_from_path(path: *const c_char) -> *const ctypes::ifstream;
    fn deallocate_ifstream(stream: *const ctypes::ifstream);
    fn read_from_ifstream(stream: *const ctypes::ifstream, buffer: *mut c_char, max_len: u32);
    fn read_str(stream: *const ctypes::ifstream, buffer: *const c_char) -> u32;
    fn read_filepairs(stream: *const ctypes::ifstream, vecpointer: *const *const ctypes::vector, length: *const u32);
    fn deallocate_vec(ptr: *const ctypes::vector);
    fn copy_filepairs_to_buf(vec: *const ctypes::vector, buf: *const C_DocIDFilePair, max_length: u32);
}


struct ifstream(pub *const ctypes::ifstream);



#[repr(C)]
#[derive(Clone)]
struct C_DocIDFilePair {
    docid: u32,
    cstr: *const c_char,
}

pub fn get_filepairs<P: AsRef<Path>>(path: P) -> Vec<DocIDFilePair> {
    let mut cvec = unsafe {CVector::new_from_path(path)};
    let mut vec: Vec<DocIDFilePair> = Vec::new();
    cvec.buffer.drain(0..).map(|i: C_DocIDFilePair| {
        i.into()
    }).collect()
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct DocIDFilePair {
    pub docid: u32,
    pub path: Option<PathBuf>,
    pub filemap_path: Option<PathBuf>,
    pub bytes: Option<u32>,
    pub num_words: Option<u32>
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
            path: Some(path),
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

struct CVector {
    pub buffer: Vec<C_DocIDFilePair>,
    vectorlocation: *const ctypes::vector,
}

impl CVector {
    pub unsafe fn new(stream: &ifstream) -> Self {
        let mut vecpointer: *const ctypes::vector = 0 as *const _;
        let mut length: u32 = 0;
        read_filepairs(stream.as_ctypes(), &vecpointer as *const *const _, &length as *const u32);

        let mut buf = Vec::new();
        buf.resize(length as usize, C_DocIDFilePair::default());
        let ptrloc = buf.as_slice().as_ptr() as *const C_DocIDFilePair;
        copy_filepairs_to_buf(vecpointer, ptrloc, length);

        CVector {
            buffer: buf,
            vectorlocation: vecpointer,
        }
    }

    pub unsafe fn new_from_path<P: AsRef<Path>>(path: P) -> Self {
        let stream = ifstream::from_path(path);
        Self::new(&stream)
    }
}

impl Drop for CVector {
    fn drop(&mut self) {
        unsafe { deallocate_vec(self.vectorlocation) };
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
        return self.0 as *const ffi::c_void as *const ctypes::ifstream;
    }
}


impl Read for ifstream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let buflen = buf.len() as u32;
        unsafe {
            read_from_ifstream(self.0, &mut buf[0] as *mut u8 as *mut c_char, buflen);
        };
        return Ok(buflen as usize);
    }
}


impl Drop for ifstream {
    fn drop(&mut self) {
        println!("Dropping ifstream");
        unsafe { deallocate_ifstream(self.0); };
    }
}

