#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![feature(extern_types)]
#![feature(min_specialization)]
#![feature(drain_filter)]

mod cffi;
mod highlighter;
mod RustVecInterface;
mod IndexWorker;
mod webserver;
mod elapsed_span;

use std::{env};

use futures::{io};


use tracing_subscriber::FmtSubscriber;
use tracing::{Level};
use tracing_subscriber::fmt::format::FmtSpan;






use std::sync::atomic::{AtomicU32};
use std::sync::Arc;


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

const suffices: [&str; 28] = ["-6hPk--fBi2-1AXWf-1I-v2\0", "-m1eu--x27H\0", "007Q2-07d0W\0", "09Bh4-0CPUM\0", "0C_XD-0FKHk\0", "0GjnV-0vC-V\0", "1KPHc-1NIR2-1ODd5-1SH0Q\0", "1X53f-1_eOr-1x6Rr-23tuq\0", "2Cd6o-2Ck2K\0", "2DRxd-2EF1V\0", "2ERdD-2Pc6Y\0", "2ThGI-2TlGQ\0", "2jcnf-2kZe1-2uXiy-345Pi\0", "39Wk1-3SGe9-3TCj6-3Tww6\0", "3YPZL-3hT5C-3kC2J-4-dYP\0", "40hMg-42uFe-48Se7-4F4kK\0", "4MJRG-4SB8O\0", "4VwhL-4X7Cn\0", "4Y0WM-4gRJF-4kWig-4x5Z1\0", "5-zvj-55dHo\0", "5M-CI-5Q1bU\0", "5UoCA-5Y-lE-5pXVT-60HHO\0", "61JCz-69OTH-6HwDy-6Wszn\0", "6ZdyN-6cI5f\0", "6ZdyN-6cI5f-6hvij-77dNI\0", "6hvij-77dNI\0", "7D4Oq-7DmYH\0", "7D4Oq-7DmYH-7Pnxv-7RHZw\0"];
fn main() -> io::Result<()> {
    setup_logging();

    // let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    // builder
    //     .set_private_key_file("/home/henry/127.0.0.1+1-key.pem", SslFiletype::PEM)
    //     .unwrap();
    // builder.set_certificate_chain_file("/home/henry/127.0.0.1+1.pem").unwrap();
    unsafe { cffi::initialize_dir_vars() };
    let iw: Vec<_> = suffices.chunks(4).map(|chunk| {
        IndexWorker::IndexWorker::new(chunk)
    }).collect();

    let appstate = webserver::ApplicationState {
        iw,
        highlighting_jobs: Arc::new(Default::default()),
        jobs_counter: Default::default()
    };

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let server = webserver::get_server(appstate);
        server.await.unwrap();
    });



    // let highlighting_jobs = Arc::new(Mutex::new(HashMap::new()));


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


