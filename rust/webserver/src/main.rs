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
use tracing::{Level, debug};
use tracing_subscriber::fmt::format::FmtSpan;
use std::sync::Arc;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::{PathBuf};
use std::iter::FromIterator;
use env_logger;
use tracing::log::LevelFilter;

fn setup_logging() {
    if env::var("RUST_LOG").is_ok() {
        env_logger::Builder::new().format_timestamp_millis()
            // .filter_level(log::LevelFilter::Debug)
            .parse_default_env()
            // .filter_level(LevelFilter::Debug)
            // .filter_module("tracing::span", LevelFilter::Trace)
            .filter_module("hyper", LevelFilter::Warn)
            .init();
        println!("Using env logger");
    } else {
        let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG)
            .with_span_events(FmtSpan::ACTIVE).finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        println!("Using tracing_subscriber");
    }
}

fn open_index_files() -> Vec<String> {
    let i_fpath = PathBuf::from_iter([env::var("DATA_FILES_DIR").unwrap(), "indices/index_files".to_string()]);

    let i_f = File::open(i_fpath).unwrap();
    let bufreader = BufReader::new(i_f);

    bufreader.lines().filter_map(|l| {
        let mut l = l.unwrap();
        if l.chars().next().map(|c| c != '#').unwrap_or(false) {
            l.push('\0');
            Some(l)
        } else {
            None
        }
    }).collect()
}

fn main() -> io::Result<()> {
    setup_logging();

    unsafe { cffi::initialize_dir_vars() };

    let indices = open_index_files();
    let indices = indices.leak();

    let chunk_size = (indices.len() as f32 / 4 as f32).ceil() as usize;


    debug!("Loading {} indices", indices.len());
    let iw: Vec<_> = indices.chunks(chunk_size).map(|chunk| {
        IndexWorker::IndexWorker::new(Vec::from(chunk))
    }).collect();


    let appstate = webserver::ApplicationState {
        iw,
        highlighting_jobs: Arc::new(Default::default()),
        jobs_counter: Default::default(),
    };

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let server = webserver::get_server(appstate);

        debug!("Starting web server");
        server.await.unwrap();
    });

    Ok(())
}


