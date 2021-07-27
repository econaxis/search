
use std::os::raw::c_char;
use crate::DbContext;


#[no_mangle]
pub extern fn read_value(_db: *const DbContext, _key: *const c_char) -> *const c_char {
    unimplemented!()
}

#[no_mangle]
pub extern fn write_value(_db: *const DbContext, _key: *const c_char, _value: *const c_char) {
    unimplemented!()
}