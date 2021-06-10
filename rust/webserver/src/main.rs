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
use std::sync::Arc;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::{PathBuf};
use std::iter::FromIterator;


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

fn open_index_files() -> Vec<String> {
    let i_fpath = PathBuf::from_iter([env::var("DATA_FILES_DIR").unwrap(), "indices/index_files".to_string()]);

    let i_f = File::open(i_fpath).unwrap();
    let bufreader = BufReader::new(i_f);

    bufreader.lines().map(|l| {
        let mut l = l.unwrap();
        l.push('\0');
        l
    }).collect()
}

// const suffices: [&str; 30] = ["bDqAA-YGF-E\0", "IPkxL-0UIm3\0", "1RC0P-BWoSj\0", "nTNYn-fjulY\0", "OLfQJ-2GRMG\0", "1T7w3-2FyYw\0", "SgfRj-7jVDy\0", "8B_D4-FlgH7\0", "RfCBA-GXPHZ\0", "ZqqTC-QAErs\0", "JK6Fd-vI2JZ\0", "b6EiO-Woo4F\0", "z9GQY-J_jsV\0", "KM6KY-Ub7Q9\0", "umhkl-Rs2iJ\0", "KjvRz-vq5_a\0", "J8412-1CgFX\0", "A7wJU-oDt4o\0", "tQdWr-bJ92Q\0", "JUuTA-X2PtI\0", "qv-re-pG8NH\0", "Erc4o--rGoP\0", "XenTF-EG9dm\0", "QfhPp-EO2sQ\0", "pXJ7k-U4mhP\0", "BFd7n-UIslu\0", "u6g5i-bgGAN\0", "xhmyc-X6yXp\0", "jm4BP-nwCBP\0", "0Z56k-fyvHt\0" ];
fn main() -> io::Result<()> {
    setup_logging();

    unsafe { cffi::initialize_dir_vars() };

    let indices = open_index_files();

    let chunk_size = indices.len() / 4;

    let iw: Vec<_> = indices.chunks(chunk_size).map(|chunk| {
        IndexWorker::IndexWorker::new(chunk)
    }).collect();

    let appstate = webserver::ApplicationState {
        iw,
        highlighting_jobs: Arc::new(Default::default()),
        jobs_counter: Default::default()
    };

    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let server = webserver::get_server(appstate);
        server.await.unwrap();
    });

    Ok(())
}


