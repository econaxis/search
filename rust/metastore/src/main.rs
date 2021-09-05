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
#![feature(drain_filter)]
#![feature(try_trait_v2)]

extern crate crossbeam;
extern crate lazy_static;
extern crate log;
extern crate parking_lot;
extern crate quickcheck;
extern crate quickcheck_macros;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate slab;

mod local_replication_handler;
mod rpc_handler;

pub use db_context::DbContext;
pub use rwtransaction_wrapper::ValueWithMVCC;
pub use rwtransaction_wrapper::{IntentMap, MutBTreeMap};

pub use crate::object_path::ObjectPath;

// mod hyper_error_converter;

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
mod tuple_maker;

pub use crate::rwtransaction_wrapper::ReplicatedTxn;
pub use rwtransaction_wrapper::TypedValue;
#[macro_use]
mod error_macro;
mod btree_index;
mod db_context;
mod file_debugger;
mod hermitage_tests;
mod history_storage;
mod replicated_slave;

fn main() {
    for _ in 0..20 {
        // thread_tests::tests::monotonic();
        // thread_tests::tests_walwatcher::test2();
        println!("done test2");
        thread_tests::tests_walwatcher::test1();
        println!("done test1");
        thread_tests::tests::unique_set_insertion_test();
        println!("done unique_set_insertion_test");
    }
    // thread_tests::tests::unique_set_insertion_test();
}
