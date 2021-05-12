#![allow(unused)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![feature(extern_types)]

mod cffi;
mod index_file_checker;

use serde_json::{to_string, ser, to_string_pretty};
use std::path::{Path, PathBuf};
use std::fs::{ReadDir, read_dir, DirEntry, File};
use fancy_regex;
use crate::cffi::DocIDFilePair;
use std::str::FromStr;
use std::ops::Deref;
use std::fs;
use std::io::{BufReader, Read};

const filemap_regex: &str = r"filemap-(.*)";

fn filemaps<'a, P: AsRef<Path>>(p: P) -> Vec<PathBuf> {
    let regex: fancy_regex::Regex = fancy_regex::Regex::new(filemap_regex).unwrap();

    read_dir(p).unwrap().filter(|d| {
        let str = d.as_ref().unwrap().file_name();
        let str = str.to_str().unwrap();
        regex.is_match(str).unwrap()
    }).map(|d| {
        d.as_ref().unwrap().path()
    }).collect()
}

fn word_count(p: &Path) -> u32 {
    let f = File::open(p).unwrap();
    let mut reader = BufReader::new(f);
    let mut spaces = 0;

    reader.bytes().fold(0, |value, item| {
        if item.ok() == Some(b' ') {
            value + 1
        } else {
            value
        }
    })
}

fn main() {
    let a = filemaps("/mnt/nfs/extra/data-files/indices");
    let mut fp_total = Vec::new();
    for ref path in a {
        let fp = cffi::get_filepairs(path).into_iter().map(|mut elem: DocIDFilePair| {
            // Fill in the remaining data of the elem.
            elem.filemap_path = Some(path.clone());

            if let Some(ref path) = elem.path {
                elem.bytes = fs::metadata(path).map(|metadata| metadata.len() as u32).ok();
                elem.num_words = Some(word_count(path));
            }
            elem
        }).collect::<Vec<DocIDFilePair>>();
        fp_total.extend(fp);
    }

    let out_file = File::create("/temp/output").unwrap();
    ser::to_writer_pretty(out_file, &fp_total);

}


