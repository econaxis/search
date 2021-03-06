use std::{borrow::Borrow, hash::Hash, fs::{File, self}, iter::FromIterator, ffi::OsStr, collections::{HashSet}, path::{Path}};
use std::path::{PathBuf};
use std::{env};
use std::io::{BufReader, BufRead};
use webserver::cffi::{DocIDFilePair, filepairs::get_filepairs};
use regex::Regex;
pub use public_ffi::*;
use serde::Serialize;
use rmp_serde;
use std::str::FromStr;
use std::collections::HashMap;


#[derive(Default, Debug)]
pub struct NamesDatabase {
    set: HashSet<DocIDFilePair>,
    json_path: PathBuf,
}

mod public_ffi {
    use std::os::raw::c_char;
    use std::ffi::CStr;
    use crate::NameDatabase::NamesDatabase;

    fn c_char_to_str<'a>(s: *const c_char) -> &'a str {
        unsafe { CStr::from_ptr(s) }.to_str().unwrap()
    }

    #[no_mangle]
    pub extern fn register_temporary_file(ndb: *mut NamesDatabase, path: *const c_char, docid: u32) {
        unsafe { &mut *ndb }.insert(c_char_to_str(path), docid);
    }

    #[no_mangle]
    pub extern fn new_name_database(name: *const c_char) -> *const NamesDatabase {
        let name = c_char_to_str(name);
        let namesdb = Box::new(NamesDatabase::new(name.as_ref()));
        Box::into_raw(namesdb)
    }

    #[no_mangle]
    pub extern fn search_name_database(ndb: *const NamesDatabase, key: *const c_char) -> bool {
        let key = c_char_to_str(key);
        let ndb = unsafe { &*ndb };
        ndb.get_from_str(key).is_some()
    }

    #[no_mangle]
    pub extern fn drop_name_database(ndb: *mut NamesDatabase) {
        unsafe {Box::from_raw(ndb)};
    }


    #[no_mangle]
    pub extern fn serialize_namesdb(ndb: *mut NamesDatabase) {
        // unsafe { &mut *ndb }.drop_serialize();
        unsafe { &mut *ndb }.serialize();
    }
}

#[cfg(debug_assertions)]
#[allow(unused)]
fn pretty_serialize(d: &HashSet<DocIDFilePair>) {
    let outfile = fs::File::create("/tmp/file_metadata.pretty.json").unwrap();
    serde_json::to_writer_pretty(outfile, &d).unwrap();
}

#[allow(unused)]
#[cfg(not(debug_assertions))]
fn pretty_serialize(_: &HashSet<DocIDFilePair>) {}

impl NamesDatabase {
    pub fn new(metadata_path: &Path) -> Self {
        let json_path = metadata_path.join("file_metadata.msgpack");

        if fs::metadata(&json_path).map_or(0, |f| f.len()) > 10 {
            let this = Self::from_json_file(&json_path);
            return this;
        }
        let processed_data: HashSet<DocIDFilePair> = {
            fs::File::create(&json_path).expect(&*format!("Couldn't open file {:?}", json_path));
            let empty_hash = HashSet::new();
            HashSet::from_iter(generate_metadata_for_dir(metadata_path, &empty_hash))
        };

        let this = Self {
            set: processed_data,
            json_path,
        };
        println!("Creating new NDB file");
        this.serialize();
        this
    }

    pub fn insert(&mut self, path: &str, id: u32) {
        self.set.insert(DocIDFilePair { docid: id, path: PathBuf::from_str(path).unwrap(), ..Default::default() });
    }

    pub fn from_json_file(json_path: &Path) -> Self {
        use serde::Deserialize;
        let f = File::open(json_path).unwrap();

        let mut deserializer = rmp_serde::Deserializer::new(f);
        let data: Vec<DocIDFilePair> = Deserialize::deserialize(&mut deserializer).unwrap();

        let set = HashSet::from_iter(data.into_iter());
        NamesDatabase { set, json_path: json_path.to_owned() }
    }
    pub fn get<Q>(&self, key: &Q) -> Option<&DocIDFilePair>
        where DocIDFilePair: Borrow<Q>, Q: Hash + Eq + ?Sized {
        self.set.get(&key)
    }

    pub fn get_from_str(&self, key: &str) -> Option<&DocIDFilePair> {
        self.get(OsStr::new(key))
    }

    fn serialize(&self) {
        // Serialize new metadata file.
        let binary_outfile = fs::File::create(&self.json_path).unwrap();

        let mut serializer = rmp_serde::Serializer::new(&binary_outfile);
        self.set.serialize(&mut serializer).unwrap();
    }
}


fn filemaps<P: AsRef<Path>>(p: P) -> Vec<PathBuf> {
    let joined_regex: Regex = Regex::new(r"# joined .*").unwrap();

    // Check the index_files file too, make sure there aren't any mistakes.
    let p = p.as_ref().to_path_buf().join("index_files");

    let bfr = fs::File::open(&p).map(BufReader::new);
    let mut result_index_files = Vec::new();

    if let Ok(bfr) = bfr {
        for s in bfr.lines() {
            let s = s.unwrap();

            // Check that this line has not been joined already.
            if joined_regex.is_match(&s) {
                continue;
            }

            let s = PathBuf::from(format!("{}/indices/filemap-{}", env::var("DATA_FILES_DIR").unwrap(), s));
            if s.as_path().exists() {
                result_index_files.push(s);
            } else {
                eprintln!("Path doesn't exist: {:#?}", &s);
            }
        };
    };

    result_index_files
}


/// Generate metadata for all filemap-* files in a directory as a vector of DocIDFilePairs.
pub fn generate_metadata_for_dir<P: AsRef<Path>>(path: P, processed_already: &HashSet<DocIDFilePair>)
                                                 -> HashSet<DocIDFilePair> {
    let a: Vec<PathBuf> = filemaps(path).drain_filter(|path| {
        // Only filter paths not in the procesed already path.
        let path: &OsStr = path.as_ref();
        processed_already.get(path).is_none()
    }).collect();
    // let mut fp_total = HashSet::new();
    let mut dup_finder: HashMap<DocIDFilePair, &PathBuf> = HashMap::new();
    let mut errored = false;
    for path in &a {
        println!("{}", path.display());
        for mut elem in get_filepairs(path).into_iter() {
            // Fill in the remaining data of the elem.
            elem.filemap_path = Some(path.clone());


            if let Some(oldpath) = dup_finder.get(&elem) {
                if errored { continue; } else { errored = true; }
                if oldpath == &path { continue; };
                eprintln!("Duplicate filename found {} {}", path.display(), oldpath.display());
            } else {
                dup_finder.insert(elem, path);
            }
        }

        // println!("Total files currently {}", fp_total.len());
    }
    println!("NDB size: {} files", dup_finder.len());
    dup_finder.into_keys().collect()
}
