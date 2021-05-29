#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![feature(extern_types)]
#![feature(min_specialization)]
#![feature(drain_filter)]

mod highlighter;
mod RustVecInterface;
mod IndexWorker;
mod webserver;

pub mod cffi;