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
mod highlighter;
mod RustVecInterface;
mod microsecond_timer;
mod IndexWorker;

use std::path::{Path, PathBuf};
use crate::cffi::DocIDFilePair;
use std::{fs, time};
use std::io::{BufReader, Read, Write};
use std::cmp::Ord;
use crate::NameDatabase::NamesDatabase;
use std::ffi::{OsStr, OsString};
use RustVecInterface::C_SSK;
use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use futures::{executor, io};
use tokio::net::TcpListener;
use tokio::io::{Result, AsyncReadExt, split, AsyncWriteExt, AsyncWrite};
use tokio::runtime::Runtime;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::highlighter::highlight_files;
use tracing_subscriber::FmtSubscriber;
use tracing::{Level, Instrument, span};
use tracing_subscriber::fmt::format::FmtSpan;


const data_file_dir: &str = "/mnt/nfs/extra/data-files/";
const indice_file_dir: &str = "/mnt/nfs/extra/data-files/indices/";

const equals_separator: &str = "===============================";

const html_prefix: &str = r#"<!doctype html><html>
<head> <meta charset="utf-8"/></head>
<body style = "margin: 20%">"#;
const html_suffix: &str = "</body></html>";


fn utf8_to_str(a: &[u8]) -> &str {
    let res = std::str::from_utf8(a);
    if let Ok(v) = res { v } else {
        "UTF8 - error"
    }
}

async fn start_socket_server(mut iw: IndexWorker::IndexWorker) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3000").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        let start = time::Instant::now();
        let mut buf = [0u8; 1000];
        let res = socket.read(&mut buf).await.unwrap();

        if res == 0 {
            println!("Closing connection");
            break;
        };

        let mut str = std::str::from_utf8(&buf[0..res]).unwrap();
        str = str.trim();
        let query: Vec<String> = str.split_whitespace().map(|x| x.to_owned()).collect();
        println!("Query start: {}", start.elapsed().as_micros());

        let mut res = iw.send_query_async(&query).await;


        // Moves the socket into a new task and runs highlighting.
        // Since highlighting might be resource intensive, we don't want to block incoming connections.
        let task = tokio::spawn(async move {
            let fullsize = res.len();
            res.truncate(20);
            highlight_files(res.as_slice(), query.as_slice(), &mut socket).await;
        }.instrument(span!(Level::INFO, "Highlighting match")));
        task.await;
    }
    Ok(())
}

fn main() {
    unsafe { cffi::initialize_dir_vars() };

    let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG)
        .with_span_events(FmtSpan::ACTIVE).with_timer(microsecond_timer::MicrosecondTimer{}).finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let suffices = ["PA18E\0","xmB4m-PA18E-cqfS7-H0Yr1-4kUYr-C-cOO\0", "PA18E\0","xmB4m-PA18E-cqfS7-H0Yr1-4kUYr-C-cOO\0","xmB4m-PA18E-cqfS7-H0Yr1-4kUYr-C-cOO\0" ];

    let mut iw = IndexWorker::IndexWorker::new(&suffices);

    let rt = Runtime::new().unwrap();

    rt.block_on(start_socket_server(iw));

    // let queries = vec!["CANADI".to_owned(), "DISNEY".to_owned()];
    // let fut = iw.send_query_async(&queries);
    // let res = executor::block_on(fut);
    // println!("{:?}", res);
    //
    // // let res = iw.poll_for_results();
    //
    // let mut outfile = fs::File::create("/tmp/output").unwrap();
    //
    // outfile.write(html_prefix.as_bytes());
    //
    // for s in &res {
    //     write!(&mut outfile, "<h1>File: {}</h1></br>", s);
    //     let str = match IndexWorker::load_file_to_string(s.as_ref()) {
    //         None => "".to_owned(),
    //         Some(x) => x
    //     };
    //     let mut str = str.as_str();
    //     let mut strindices: Vec<usize> = str.char_indices().map(|(pos, _)| pos).collect();
    //     // Limit highlighting to first 5kb only
    //     if str.len() > 20000 {
    //         str = &str[0..strindices[20000]];
    //         strindices.truncate(20000);
    //     }
    //
    //
    //
    //
    //     let mut matches = highlight_matches(str, queries.as_slice());
    //
    //     for (start, end) in matches {
    //         // Start a new chunk.
    //         let lastend = (end + 30).clamp(0, strindices.len() - 1);
    //         let firstbegin = (start - 30).clamp(0, strindices.len() - 1);
    //
    //         let lastend = strindices[strindices.partition_point(|&x| x<= lastend)];
    //         let firstbegin = strindices[strindices.partition_point(|&x| x<= firstbegin)];
    //
    //         if (firstbegin >= lastend) {
    //             let a = 5;
    //         }
    //         write!(&mut outfile, "</br> ...{}<mark>{}</mark>{}...", &str[firstbegin..start], &str[start..end], &str[end..lastend]);
    //     }
    // }
    //
    // outfile.write(html_suffix.as_bytes());
}


