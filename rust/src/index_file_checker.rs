use std::fs;
use std::path::Path;
use std::io::{Read, Write};
use std::fs::File;
use std::time::SystemTime;
use regex::RegexBuilder;

const space: char = ' ';
const PATH: &str = "/mnt/nfs/.cache/data-files/";

enum FileTypes {
    positions,
    frequencies,
    terms,
}
fn get_filename(t: FileTypes, suffix: &str) -> String {
    match t {
        FileTypes::positions => format!("{}-{}", "positions", suffix),
        FileTypes::terms => format!("{}-{}", "terms", suffix),
        FileTypes::frequencies => format!("{}-{}", "frequencies", suffix)
    }
}
