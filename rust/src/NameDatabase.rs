use std::{
    borrow::Borrow,
    hash::Hash,
    fs::{File, self},
    iter::FromIterator,
    ffi::OsStr,
    collections::{HashSet},
    path::{Path},
};
use crate::cffi::{DocIDFilePair, generate_metadata_for_dir};
pub use public_ffi::*;
use serde::Serialize;


#[derive(Default, Debug)]
pub struct NamesDatabase {
    set: HashSet<DocIDFilePair>,
}

mod public_ffi {
    use std::os::raw::c_char;
    use std::ffi::CStr;
    use crate::NameDatabase::NamesDatabase;

    fn c_char_to_str<'a>(s: *const c_char) -> &'a str {
        unsafe { CStr::from_ptr(s) }.to_str().unwrap()
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
        unsafe { Box::from_raw(ndb) };
    }
}


fn pretty_serialize(d: &HashSet<DocIDFilePair>) {
    let outfile = fs::File::create("file_metadata.pretty.json").unwrap();
    serde_json::to_writer_pretty(outfile, &d).unwrap();
}

impl NamesDatabase {
    pub fn new(metadata_path: &Path) -> Self {
        let json_path = metadata_path.join("file_metadata.msgpack");
        let json_path = json_path.as_path();

        let json_metadata = fs::metadata(&json_path);

        let mut processed_data: HashSet<DocIDFilePair> = if json_metadata.is_ok() && json_metadata.unwrap().len() > 1 {
            println!("Reusing same JSON metadata file");
            let cur_data = Self::from_json_file(json_path);
            cur_data.set
        } else {
            fs::File::create(&json_path).unwrap();
            HashSet::new()
        };

        processed_data.extend(generate_metadata_for_dir(metadata_path, &processed_data));

        // Serialize new metadata file.
        let binary_outfile = fs::File::create(&json_path).unwrap();

        let mut serializer = rmp_serde::Serializer::new(&binary_outfile);
        processed_data.serialize(&mut serializer).unwrap();

        pretty_serialize(&processed_data);
        Self {
            set: HashSet::from_iter(processed_data.into_iter())
        }
    }

    pub fn from_json_file(json_path: &Path) -> Self {
        use serde::Deserialize;
        let f = File::open(json_path).unwrap();

        let mut deserializer = rmp_serde::Deserializer::new(f);
        let data: Vec<DocIDFilePair> = Deserialize::deserialize(&mut deserializer).unwrap();

        let set = HashSet::from_iter(data.into_iter());
        NamesDatabase { set }
    }
    pub fn get<Q>(&self, key: &Q) -> Option<&DocIDFilePair>
        where DocIDFilePair: Borrow<Q>, Q: Hash + Eq + ?Sized {
        self.set.get(&key)
    }

    pub fn get_from_str(&self, key: &str) -> Option<&DocIDFilePair> {
        self.get(OsStr::new(key))
    }
}