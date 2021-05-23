#![feature(extern_types)]
#![feature(min_specialization)]
#![feature(drain_filter)]
#![allow(unused)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

pub mod NameDatabase;
mod cffi;

pub mod dir_finder;
pub mod RustVecInterface;

pub use dir_finder::*;
pub use RustVecInterface::*;
pub use crate::NameDatabase::*;
