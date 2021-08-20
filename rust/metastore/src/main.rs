/*
Tasks: grpc for replication
query processing
more threading tests
secondary indexes



 */

#![feature(assert_matches)]
#![feature(try_blocks)]
#![feature(trace_macros)]
#![feature(backtrace)]

use std::cell::UnsafeCell;
use std::sync::Mutex;

pub use rwtransaction_wrapper::{IntentMap, MutBTreeMap};
pub use rwtransaction_wrapper::ValueWithMVCC;
pub use db_context::DbContext;

use crate::replicated_slave::SelfContainedDb;
use crate::thread_tests::tests::monotonic;
use crate::wal_watcher::ByteBufferWAL;
pub use crate::object_path::ObjectPath;


// mod hyper_error_converter;
extern crate quickcheck;
extern crate quickcheck_macros;

// mod hyperserver;
mod c_interface;
mod object_path;
mod parsing;
mod rwtransaction_wrapper;

#[macro_use]
mod retry;


#[macro_use]
mod test_transaction_generate;
mod thread_tests;
mod timestamp;
mod wal_watcher;

mod secondary_indexing;

#[macro_use]
mod error_macro;
mod hermitage_tests;
mod replicated_slave;
mod file_debugger;
mod db_context;
mod history_storage;
mod btree_index;

pub struct MutSlab(Mutex<UnsafeCell<slab::Slab<ValueWithMVCC>>>);

fn main() {
    for _ in 0..500 {
        monotonic();
        thread_tests::tests_walwatcher::test2();
        thread_tests::tests_walwatcher::test1();
        print!("one iter\n");
    }
    thread_tests::tests::unique_set_insertion_test();
}

