use std::io::Write;
use std::{fmt, fs};

use std::cell::RefCell;
use std::fs::OpenOptions;

thread_local! {
    static FILE: RefCell<fs::File> = RefCell::new(OpenOptions::new().append(true).create(true).open("debug.txt").unwrap());
}

#[allow(unused)]
pub fn print_to_file(args: fmt::Arguments) {
    FILE.with(|f| f.borrow_mut().write_fmt(args)).unwrap();
}
