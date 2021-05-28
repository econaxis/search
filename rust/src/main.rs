#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![feature(extern_types)]
#![feature(min_specialization)]
#![feature(drain_filter)]

mod cffi;
mod NameDatabase;
mod highlighter;
mod RustVecInterface;
mod IndexWorker;


use std::{fs, env};
use std::io::{Read};

use futures::{io, Stream};

use tokio::io::{Result};

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::highlighter::{highlight_files, serialize_response_to_json};
use tracing_subscriber::FmtSubscriber;
use tracing::{Level, Instrument, span, event, debug, info, debug_span};
use tracing_subscriber::fmt::format::FmtSpan;
use actix_web::{get, web, App, HttpResponse, HttpServer, HttpResponseBuilder};
use serde::Deserialize;
use std::time::Duration;
use std::task::{Context, Poll};
use std::pin::Pin;
use actix_web::http::StatusCode;
use actix_web::web::Bytes;
use actix_web::body::BodyStream;
use std::collections::HashMap;

use std::sync::atomic::{AtomicU32, Ordering};


use crate::IndexWorker::ResultsList;
use std::ops::Deref;


static jobs_counter: AtomicU32 = AtomicU32::new(1);


struct HighlightRequest {
    query: Vec<String>,
    files: ResultsList,
}

struct ApplicationState {
    iw: Vec<IndexWorker::IndexWorker>,
    highlighting_jobs: Arc<Mutex<HashMap<u32, HighlightRequest>>>,
}

struct FT(u32);

impl Stream for FT {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

fn clear_highlight_tasks(cur_docid: u32, highlight_queue: &mut HashMap<u32, HighlightRequest>) {
    let _sp = span!(Level::DEBUG, "clearing highlight queue", length = highlight_queue.len()).entered();
    if highlight_queue.len() > 50 {
        let limit = cur_docid.saturating_sub(30);
        highlight_queue.retain(|k, _v| *k > limit);
    }
}

#[get("/highlight/")]
async fn highlight_handler(data: web::Data<ApplicationState>, highlightid: web::Query<HighlightId>) -> HttpResponse {
    let jobs = data.highlighting_jobs.lock().await;
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
        highlight_files(&files, query.as_slice())
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


async fn broadcast_query(indices: &[IndexWorker::IndexWorker], query: &[String]) -> ResultsList {
    let futures_list: Vec<_> = indices.iter().map(|iw| {
        iw.send_query_async(query)
    }).collect();

    let res = futures::future::join_all(futures_list);
    let res = res.instrument(debug_span!("Fanning out requests")).await;

    let _sp = debug_span!("Reducing requests").entered();
    let mut res = res.into_iter().reduce(|x1, x2| x1.join(x2)).unwrap();
    res.sort();
    res
}

async fn work_on_query(data: &web::Data<ApplicationState>, iw: &[IndexWorker::IndexWorker], query: &str) -> (u32, ResultsList) {
    let query: Vec<String> = query.split_whitespace().map(|x| x.to_owned()).collect();

    let res = broadcast_query(iw, &query).await;

    let id = jobs_counter.fetch_add(1, Ordering::Relaxed);

    // Since highlighting might be resource intensive, we don't want to block incoming connections.
    let mut highlight_jobs = data.highlighting_jobs.lock().await;

    // Truncate the results list to max 10 elements. We don't need to highlight anymore.
    // let res_trunc = &res[0..std::cmp::min(res.len(), 10)];
    // let res_trunc = res_trunc.to_vec();
    highlight_jobs.insert(id, HighlightRequest { query: query.clone(), files: ResultsList::from(res.to_vec()) });

    if highlight_jobs.len() > 100 {
        clear_highlight_tasks(id, &mut highlight_jobs);
    }
    (id, res)
}

fn setup_logging() {
    if env::var("RUST_LOG").is_ok() {
        env_logger::Builder::new().format_timestamp_millis()
            // .filter_level(log::LevelFilter::Debug)
            // .filter_module("tracing::span", LevelFilter::Trace)
            .parse_default_env().init();
        println!("Using env logger");
    } else {
        let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG)
            .with_span_events(FmtSpan::ACTIVE).finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        println!("Using tracing_subscriber");
    }
}

fn main() -> io::Result<()> {
    // let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    // builder
    //     .set_private_key_file("/home/henry/127.0.0.1+1-key.pem", SslFiletype::PEM)
    //     .unwrap();
    // builder.set_certificate_chain_file("/home/henry/127.0.0.1+1.pem").unwrap();
    unsafe { cffi::initialize_dir_vars() };

    setup_logging();
    let suffices = ["oPV3-\0", "JX9vH\0", "D59V9\0", "WDHnk\0", "j493v\0", "k7FSu\0", "tg_2O\0", "vz1R3\0", "dLABs\0", "nPoty\0", "pEgBu\0", "-be7U\0", "pxVOI\0", "EcEAk\0", "yyQfQ\0", "Xo25c\0", "Sx0s2\0", "sUj-F\0", "fyuQf\0", "WpHIH-hBvUn\0", "6uVWX-c5H8m\0", "f0FRh-3Gw1R\0", "c4WUJ-od7Ew\0", "UgD0W-G_78v\0", ];

    // Tokio or actix does this weird thing where they clone the IndexWorker many times
    // This wastes memory, spawns useless threads, and there's no way I know to fix it.
    // Solution: wrap `iw` in an Arc. Then actix can clone it however many times it wants.
    // This won't spawn new threads or allocate memory. However, when we actually use it,
    // we clone `iw` once, preventing excess threads.
    let iw: Vec<_> = suffices.chunks(4).map(|chunk| {
        IndexWorker::IndexWorker::new(chunk)
    }).collect();


    let highlighting_jobs = Arc::new(Mutex::new(HashMap::new()));


    let _sys = actix_rt::System::with_tokio_rt(|| tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
    ).block_on(async move {
        let iw = Arc::new(iw);

        HttpServer::new(move || {
            App::new()
                .data(ApplicationState { iw: iw.deref().clone(), highlighting_jobs: highlighting_jobs.clone() })
                .service(handle_request)
                .service(main_page)
                .service(test_get)
                .service(highlight_handler)
        }).workers(1)
            .bind("0.0.0.0:8080").unwrap()
            .run().await.unwrap();
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


