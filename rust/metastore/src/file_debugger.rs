use std::{fmt, fs};
use std::io::Write;


use std::fs::OpenOptions;
use std::cell::RefCell;

thread_local! {
    static FILE: RefCell<fs::File> = RefCell::new(OpenOptions::new().append(true).create(true).open("debug.txt").unwrap());
}

#[allow(unused)]
pub fn print_to_file(args: fmt::Arguments) {
    FILE.with(|f| f.borrow_mut().write_fmt(args)).unwrap();
}