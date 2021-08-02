use crate::DbContext;
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn read_value(_db: *const DbContext, _key: *const c_char) -> *const c_char {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn write_value(_db: *const DbContext, _key: *const c_char, _value: *const c_char) {
    unimplemented!()
}
