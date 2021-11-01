#ifndef GAME_RUST_INTERFACE_H
#define GAME_RUST_INTERFACE_H

using NamesDatabase = void;
using RustVec = void;

using TableManager = void;

// Old interface
extern "C" NamesDatabase* new_name_database(const char* name);
extern "C" bool search_name_database(const NamesDatabase* ndb, const char *key);
extern "C" void serialize_namesdb(NamesDatabase* ndb);
extern "C" void fill_rust_vec (RustVec*, void* data, std::size_t size);
extern "C" void register_temporary_file(const NamesDatabase *, const char* path, uint32_t docid);
extern "C" void drop_name_database(NamesDatabase *);

// New interface
extern "C" void db1_store(TableManager* db, unsigned int id, char* name, char* document);
extern "C" char* db1_get(TableManager* db, unsigned int id);
extern "C" TableManager* db1_new();


/*
 * #[no_mangle]
pub unsafe extern "C" fn db1_store(db: *mut TableManager<Document>, name: *mut c_char, document: *mut c_char) {
    let name = CString::from_raw(name).to_string_lossy().into_owned();
    let document = CString::from_raw(document).to_string_lossy().into_owned();
    println!("Saving {}", name);

    (&mut *db).store(Document { name, document });
}

#[no_mangle]
pub unsafe extern "C" fn db1_get(db: *mut TableManager<Document>, name: *const c_char) -> *mut c_char {
    let name = CStr::from_ptr(name).to_str().unwrap();

    let hash = Document::get_hash(name);
    let mut result = (&mut *db).get_in_all(hash..=hash);
    let document = std::mem::take(&mut result.first_mut().unwrap().document);
    let document = CString::new(document).unwrap();
    document.into_raw()
}

 */

#endif //GAME_RUST_INTERFACE_H
