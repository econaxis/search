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

// mod hyper_error_converter;
extern crate quickcheck;
extern crate quickcheck_macros;

pub use crate::db_context::DbContext;
pub use crate::object_path::ObjectPath;
pub use crate::rwtransaction_wrapper::ReplicatedTxn;

// mod hyperserver;
pub mod btree_index;
pub mod c_interface;
pub mod object_path;
pub mod parsing;
pub mod rwtransaction_wrapper;

pub use replicated_slave::SelfContainedDb;
pub use rwtransaction_wrapper::LockDataRef;
pub use rpc_handler::DatabaseInterface;
pub use rpc_handler::NetworkResult;
pub use rwtransaction_wrapper::{ValueWithMVCC, TypedValue};

#[macro_use]
pub mod test_transaction_generate;
pub mod thread_tests;
pub mod timestamp;
pub mod wal_watcher;

pub mod secondary_indexing;

#[macro_use]
pub mod error_macro;
pub mod db_context;
pub mod file_debugger;
pub mod hermitage_tests;
pub mod history_storage;
mod local_replication_handler;
pub mod replicated_slave;
mod retry;
mod rpc_handler;
mod tuple_maker;
