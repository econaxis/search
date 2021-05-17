#![allow(unused)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![feature(extern_types)]
#![feature(min_specialization)]
#![feature(drain_filter)]
mod cffi;
mod index_file_checker;
mod NameDatabase;

use std::path::{Path, PathBuf};
use crate::cffi::DocIDFilePair;
use std::fs;
use std::io::{BufReader, Read};
use crate::NameDatabase::NamesDatabase;
use std::ffi::{OsStr, OsString};


const data_file_dir: &str = "/mnt/nfs/extra/data-files/";
const indice_file_dir: &str = "/mnt/nfs/extra/data-files/indices/";



fn main() {
    // let n = NamesDatabase::new("/mnt/nfs/extra/data-files/indices/".as_ref());
    // let a =n.get_from_str("Uncanny%20valley");
    // println!("{:?}", a);
    let n = NamesDatabase::new("/mnt/nfs/extra/data-files/indices/".as_ref());

    println!("{:?}", n);
}


