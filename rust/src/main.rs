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
use futures::{executor, io, Stream};
use tokio::net::TcpListener;
use tokio::io::{Result, AsyncReadExt, split, AsyncWriteExt, AsyncWrite};
use tokio::runtime::Runtime;
use std::sync::Arc;
use tokio::sync::Mutex;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use crate::highlighter::{highlight_files, serialize_response_to_json};
use tracing_subscriber::FmtSubscriber;
use tracing::{Level, Instrument, span, event, debug};
use tracing_subscriber::fmt::format::FmtSpan;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder, HttpResponseBuilder};
use serde::Deserialize;
use std::time::Duration;
use std::task::{Context, Poll};
use std::pin::Pin;
use actix_web::http::StatusCode;
use actix_web::web::Bytes;
use actix_web::body::BodyStream;
use std::collections::HashMap;

use std::sync::atomic::{AtomicU32, Ordering};
use std::cell::Cell;
use std::borrow::Borrow;

static jobs_counter: AtomicU32 = AtomicU32::new(1);

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

struct HighlightRequest {
    query: Vec<String>,
    files: Vec<String>,
}

struct ApplicationState {
    iw: IndexWorker::IndexWorker,
    highlighting_jobs: Arc<Mutex<HashMap<u32, HighlightRequest>>>,
}

struct FT(u32);

impl Stream for FT {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0 += 1;
        let mut b = vec![0u8; 1000];
        for i in 1..1000 {
            b[i] = (i * 2 % 250) as u8;
        }
        b[500] = b'\r';
        b[501] = b'\n';

        let b = Bytes::from(b);
        std::thread::sleep(Duration::from_millis(300));
        match self.0 {
            0 => return Poll::Ready(Some(Ok(b))),
            1 => return Poll::Ready(Some(Ok(b))),
            2..=10 => return Poll::Ready(Some(Ok(b))),
            _ => return Poll::Ready(None)
        }
    }
}

#[derive(Deserialize, Debug)]
struct Query {
    q: String,
}

#[derive(Deserialize)]
struct HighlightId {
    id: u32,
}

#[get("/search/")]
async fn handle_request(data: web::Data<ApplicationState>, query: web::Query<Query>) -> HttpResponse {
    let iw = &data.iw;
    let (id, jsonstr) = work_on_query(&data, iw, &query.q).await;

    let jsonout = serde_json::json!({
        "id": id,
        "data": jsonstr
    });

    HttpResponse::Ok()
        .content_type("application/json")
        .append_header(("highlight-request-id", id))
        .body(jsonout.to_string())
}

#[get("/test")]
async fn test_get() -> HttpResponse {
    let bs = BodyStream::new(FT(0));
    HttpResponseBuilder::new(StatusCode::OK).body(bs)
}

#[get("/highlight/")]
async fn highlight_handler(data: web::Data<ApplicationState>, highlightid: web::Query<HighlightId>) -> HttpResponse {
    let mut jobs = data.highlighting_jobs.lock().await;
    let jobrequest = jobs.get(&highlightid.id);
    if jobrequest.is_none() {
        return HttpResponse::BadRequest().finish();
    }
    let HighlightRequest { query, files } = jobrequest.unwrap().clone();
    let query = query.clone();
    let files = files.clone();
    std::mem::drop(jobs);

    debug!(flen = files.len(), "Received highlighting request");

    let res = tokio::task::spawn_blocking(move || {
        highlight_files(files.as_slice(), query.as_slice())
    });

    let res = res.await.unwrap();

    HttpResponse::Ok()
        .content_type("application/json")
        .body(serialize_response_to_json(&res))
}

#[get("/")]
async fn main_page() -> HttpResponse {
    let mut buf = String::new();
    fs::File::open("/home/henry/search/website/index.html").unwrap().read_to_string(&mut buf).unwrap();
    HttpResponse::Ok().body(buf)
}

async fn work_on_query(data: &web::Data<ApplicationState>, iw: &IndexWorker::IndexWorker, query: &str) -> (u32, Vec<String>) {
    let start = time::Instant::now();

    let query: Vec<String> = query.split_whitespace().map(|x| x.to_owned()).collect();

    let mut res = iw.send_query_async(&query).await;

    let id = jobs_counter.fetch_add(1, Ordering::Relaxed);

    // Moves the socket into a new task and runs highlighting.
    // Since highlighting might be resource intensive, we don't want to block incoming connections.
    let fullsize = res.len();

    // data.highlighting_jobs.lock().await.insert(id, HighlightRequest { query: query.clone(), files: res.clone() });
    (id, res)
}

fn setup_logging() {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG)
        .with_span_events(FmtSpan::ACTIVE).with_timer(microsecond_timer::MicrosecondTimer {}).finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
}

fn main() -> io::Result<()> {
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file("/home/henry/127.0.0.1+1-key.pem", SslFiletype::PEM)
        .unwrap();
    builder.set_certificate_chain_file("/home/henry/127.0.0.1+1.pem").unwrap();
    unsafe { cffi::initialize_dir_vars() };

    setup_logging();
    let suffices = ["WpHIH\0", "hBvUn\0", "6uVWX\0"];
    // let suffices = &suffices[0..1];


    let mut highlighting_jobs = Arc::new(Mutex::new(HashMap::new()));


    // Tokio or actix does this weird thing where they clone the IndexWorker many times
    // This wastes memory, spawns useless threads, and there's no way I know to fix it.
    // Solution: wrap `iw` in an Arc. Then actix can clone it however many times it wants.
    // This won't spawn new threads or allocate memory. However, when we actually use it,
    // we clone `iw` once, preventing excess threads.
    let mut iw = Arc::new(IndexWorker::IndexWorker::new(&suffices));

    let sys = actix_rt::System::with_tokio_rt(|| tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
    ).block_on(async move {
        HttpServer::new(move || {
            let iw = iw.clone().as_ref().clone();
            App::new()
                .data(ApplicationState { iw: iw, highlighting_jobs: highlighting_jobs.clone() })
                .service(handle_request)
                .service(main_page)
                .service(test_get)
                .service(highlight_handler)
        }).workers(3)
            .bind("0.0.0.0:8080").unwrap()
            .run().await;
    }
    );


    // sys.run();
    // sys.block_on(fut);

    Ok(())

    // rt.block_on(start_socket_server(iw));

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


