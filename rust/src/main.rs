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
mod RustVecInterface;
mod IndexWorker;

use std::path::{Path, PathBuf};
use crate::cffi::DocIDFilePair;
use std::fs;
use std::io::{BufReader, Read, Write};
use crate::NameDatabase::NamesDatabase;
use std::ffi::{OsStr, OsString};
use RustVecInterface::C_SSK;
use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use futures::executor;

const data_file_dir: &str = "/mnt/nfs/extra/data-files/";
const indice_file_dir: &str = "/mnt/nfs/extra/data-files/indices/";

const equals_separator: &str = "===============================";

const html_prefix: &str = r#"<!doctype html><html>
<head> <meta charset="utf-8"/></head>
<body style = "margin: 20%">"#;
const html_suffix: &str = "</body></html>";


fn highlight_matches(str: &str, terms: &[String]) -> Vec<(usize, usize)> {
    let aut = AhoCorasickBuilder::new().ascii_case_insensitive(true).build(terms);

    aut.find_iter(str).map(|match_| {
        (match_.start(), match_.end())
    }).collect()
}

fn utf8_to_str(a: &[u8]) -> &str {
    let res = std::str::from_utf8(a);
    if let Ok(v) = res { v } else {
        "UTF8 - error"
    }
}

fn main() {
    unsafe { cffi::initialize_dir_vars() };

    let mut iw = IndexWorker::IndexWorker::new(&["PA18E\0".to_owned(), "xmB4m-PA18E-cqfS7-H0Yr1-4kUYr-C-cOO\0".to_owned()]);
    let queries = vec!["CAN".to_owned(), "DISNEY".to_owned()];
    let fut = iw.send_query_async(&queries);
    let res = executor::block_on(fut);
    println!("{:?}", res);

    // let res = iw.poll_for_results();

    let mut outfile = fs::File::create("/tmp/output").unwrap();

    outfile.write(html_prefix.as_bytes());

    for s in &res {
        write!(&mut outfile, "<h1>File: {}</h1></br>", s);
        let str = match IndexWorker::load_file_to_string(s.as_ref()) {
            None => "".to_owned(),
            Some(x) => x
        };
        let mut str = str.as_str();
        let mut strindices: Vec<usize> = str.char_indices().map(|(pos, _)| pos).collect();



        // Limit highlighting to first 5kb only
        if str.len() > 20000 {
            str = &str[0..strindices[20000]];
        }

        let mut matches = highlight_matches(str, queries.as_slice());

        for (start, end) in matches {
            if (start >= end) {
                let a = 5;
            }
            // Start a new chunk.
            let lastend = if str.len() > end + 50 { end + 50 } else { str.len() - 1 };
            let firstbegin = if start > 50 { start - 50 } else { 0 };

            let lastend = strindices[strindices.partition_point(|&x| x <= lastend) - 1];
            let firstbegin = strindices[strindices.partition_point(|&x| x <= firstbegin) - 1];
            // let end = strindices[strindices.partition_point(|&x| x<= end)];
            // let start = strindices[strindices.partition_point(|&x| x<= start)];

            write!(&mut outfile, "</br> ...{}<mark>{}</mark>{}...", &str[firstbegin..start], &str[start..end], &str[end..lastend]);
        }
    }

    outfile.write(html_suffix.as_bytes());
}


