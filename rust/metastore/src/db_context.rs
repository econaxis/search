use crate::replicated_slave::SelfContainedDb;
use crate::wal_watcher::{ByteBufferWAL, WalLoader};

use crate::history_storage::MutSlab;
use crate::rwtransaction_wrapper::{MutBTreeMap, IntentMap};


use crate::rpc_handler::{DatabaseInterface};
use crate::local_replication_handler::LocalReplicationHandler;
use crate::wal_watcher::wal_check_consistency::check_func1;
use std::ops::Deref;
use crate::timestamp::Timestamp;
use crate::file_debugger::print_to_file;


pub mod self_contained_wrapper;

pub fn create_empty_context() -> DbContext {
    DbContext {
        db: MutBTreeMap::new(),
        transaction_map: IntentMap::new(),
        old_values_store: MutSlab::new(),
        wallog: ByteBufferWAL::new(),
        replicators: None,
    }
}

pub fn create_replicated_context() -> DbContext {
    DbContext {
        db: MutBTreeMap::new(),
        transaction_map: IntentMap::new(),
        old_values_store: MutSlab::new(),
        wallog: ByteBufferWAL::new(),
        replicators: Some(Box::new(LocalReplicationHandler::new(3, SelfContainedDb::default))),
    }
}

pub struct DbContext {
    pub db: MutBTreeMap,
    pub transaction_map: IntentMap,
    pub old_values_store: MutSlab,
    pub wallog: ByteBufferWAL,
    pub replicators: Option<Box<dyn DatabaseInterface>>,
}

impl Drop for DbContext {
    fn drop(&mut self) {
        // Checks if our database and the replicated database are the exact same by comparing debug strings.
        if let Some(repl) = &self.replicators {
            if !check_func1(self, repl.deref(), Timestamp::now()).unwrap() {
                panic!("error: nonmatching");
            } else {
                println!("replication matches");
            }
        }
    }
}

impl DbContext {
    pub fn replicator(&self) -> &Box<dyn DatabaseInterface >{
        self.replicators.as_ref().unwrap()
    }
}

unsafe impl Send for DbContext {}

unsafe impl Sync for DbContext {}
