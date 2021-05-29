#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![feature(extern_types)]
#![feature(min_specialization)]
#![feature(drain_filter)]


pub mod cffi;
pub mod highlighter;
pub mod RustVecInterface;
pub mod IndexWorker;
pub mod elapsed_span;
pub mod webserver;