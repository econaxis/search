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

use std::{env};


use futures::{io};







use tracing_subscriber::FmtSubscriber;
use tracing::{Level};
use tracing_subscriber::fmt::format::FmtSpan;
use serde::Deserialize;





use std::sync::atomic::{AtomicU32};
use std::sync::Arc;


static jobs_counter: AtomicU32 = AtomicU32::new(1);


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
// async fn main() {
//     let server = webserver::get_server().await;
//     server.await;
// }

const suffices: [&str; 24] = ["oPV3-\0", "JX9vH\0", "D59V9\0", "WDHnk\0", "j493v\0", "k7FSu\0", "tg_2O\0", "vz1R3\0", "dLABs\0", "nPoty\0", "pEgBu\0", "-be7U\0", "pxVOI\0", "EcEAk\0", "yyQfQ\0", "Xo25c\0", "Sx0s2\0", "sUj-F\0", "fyuQf\0", "WpHIH-hBvUn\0", "6uVWX-c5H8m\0", "f0FRh-3Gw1R\0", "c4WUJ-od7Ew\0", "UgD0W-G_78v\0", ];

fn main() -> io::Result<()> {
    setup_logging();

    // let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    // builder
    //     .set_private_key_file("/home/henry/127.0.0.1+1-key.pem", SslFiletype::PEM)
    //     .unwrap();
    // builder.set_certificate_chain_file("/home/henry/127.0.0.1+1.pem").unwrap();
    unsafe { cffi::initialize_dir_vars() };
    // let iw: Vec<_> = suffices[0..4].chunks(4).map(|chunk| {
    //     IndexWorker::IndexWorker::new(chunk)
    // }).collect();

    // let appstate = webserver::ApplicationState {
    //     iw,
    //     highlighting_jobs: Arc::new(Default::default()),
    //     jobs_counter: Default::default()
    // };

    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(8).enable_all().build().unwrap();
    runtime.block_on(async move {
        // let server = webserver::get_server(Arc::from(appstate));
        let server = webserver::get_server();
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


