use crate::{data_file_dir};
use super::RustVecInterface::{VecDPP, DocumentPositionPointer_v3};
use regex;

use std::ffi;
use std::io::Read;
pub use std::os::raw::c_char;

use std::fmt::Error;
use std::fs;

use std::ffi::{c_void, CStr, OsStr};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::borrow::Borrow;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::ser::SerializeTuple;
use std::str::FromStr;


pub mod ctypes {
    extern {
        pub type vector; // std::vector<DocIDFilePair> type.
    pub type ifstream; // ifstream type
    pub type SortedKeysIndexStub;
    }
}

mod public {
    use std::path::{Path, PathBuf};
    use std::fs;
    use std::io::{BufReader, Read, BufRead};
    use crate::cffi::{DocIDFilePair, convert_name_to_abs};
    use crate::{data_file_dir, indice_file_dir};
    use regex::Regex;
    use std::collections::HashSet;
    use std::ffi::OsStr;

    const filemap_regex: &str = r"filemap-(.*)";

    fn filemaps<P: AsRef<Path>>(p: P) -> Vec<PathBuf> {
        let joined_regex: Regex = Regex::new(r"# joined .*").unwrap();

        // Check the index_files file too, make sure there aren't any mistakes.
        let p = p.as_ref().to_path_buf().join("index_files");

        let bfr = fs::File::open(&p).map(|x| BufReader::new(x));
        let mut result_index_files = Vec::new();
        let lines = 0;

        if let Ok(bfr) = bfr {
            for s in bfr.lines() {
                let s = s.unwrap();

                // Check that this line has not been joined already.
                if joined_regex.is_match(&s) {
                    continue;
                }

                let s = PathBuf::from(format!("{}/filemap-{}", indice_file_dir, s));
                if s.as_path().exists() {
                    result_index_files.push(s);
                } else {
                    eprintln!("Path doesn't exist: {:#?}", &s);
                }
            };
        };

        result_index_files
    }

    fn word_count(p: &Path) -> Option<u32> {
        let f = fs::File::open(p).ok()?;
        let mut reader = BufReader::new(f);
        let mut spaces = 0;

        Some(reader.bytes().fold(0, |value, item| {
            if item.ok() == Some(b' ') {
                value + 1
            } else {
                value
            }
        }))
    }

    pub fn os_str_to_str<P: AsRef<Path>>(p: &P) -> &str {
        p.as_ref().as_os_str().to_str().unwrap()
    }

    /// Generate metadata for all filemap-* files in a directory as a vector of DocIDFilePairs.
    pub fn generate_metadata_for_dir<Path_t: AsRef<Path>>(path: Path_t, processed_already: &HashSet<DocIDFilePair>)
                                                          -> Vec<DocIDFilePair> {
        let a: Vec<PathBuf> = filemaps(path).drain_filter(|path| {
            // Only filter paths not in the procesed already path.
            let path: &OsStr = path.as_ref();
            processed_already.get(path).is_none()
        }).collect();
        let mut fp_total = Vec::new();
        for ref path in a {
            let fp = super::get_filepairs(path).into_iter().map(|mut elem: DocIDFilePair| {
                // Fill in the remaining data of the elem.
                elem.filemap_path = Some(path.clone());

                let abspath = convert_name_to_abs(&elem.path);
                // elem.bytes = fs::metadata(&abspath).map(|metadata| metadata.len() as u32).ok();
                // elem.num_words = word_count(&abspath);
                elem
            }).collect::<Vec<DocIDFilePair>>();

            println!("{} files processed", fp.len());
            fp_total.extend(fp);
        }
        fp_total
    }
}

fn convert_name_to_abs(p: &Path) -> PathBuf {
    let out = PathBuf::from(data_file_dir.to_owned()).join("data").join(p);
    return out;
}

pub use public::*;
use std::hash::{Hash, Hasher};


#[link(name = "c-search-abi")]
extern "C" {
    fn create_ifstream_from_path(path: *const c_char) -> *const ctypes::ifstream;
    fn deallocate_ifstream(stream: *const ctypes::ifstream);
    fn read_from_ifstream(stream: *const ctypes::ifstream, buffer: *mut c_char, max_len: u32);
    fn read_str(stream: *const ctypes::ifstream, buffer: *const c_char) -> u32;
    fn read_filepairs(stream: *const ctypes::ifstream, vecpointer: *const *const ctypes::vector, length: *const u32);
    fn deallocate_vec(ptr: *const ctypes::vector);
    fn copy_filepairs_to_buf(vec: *const ctypes::vector, buf: *const C_DocIDFilePair, max_length: u32);

    pub fn load_one_index(suffix_name: *const c_char) -> *mut ctypes::SortedKeysIndexStub;

    #[allow(improper_ctypes)]
    pub fn search_index_top_n(index: *const ctypes::SortedKeysIndexStub, output_buffer: *const VecDPP,
                              term_num: i32, query_terms: *const *const c_char);
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

pub fn get_filepairs<P: AsRef<Path>>(path: P) -> Vec<DocIDFilePair> {
    let mut cvec = unsafe { CVector::new_from_path(path) };
    let mut vec: Vec<DocIDFilePair> = Vec::new();
    cvec.buffer.drain(0..).map(|i: C_DocIDFilePair| {
        i.into()
    }).collect()
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
        return self.path == other.path;
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
            path: path,
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

