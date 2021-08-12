use crate::replicated_slave::ReplicatedDatabase;
use crate::wal_watcher::ByteBufferWAL;

use crate::history_storage::MutSlab;
use crate::rwtransaction_wrapper::{MutBTreeMap, IntentMap};

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
        replicators: Some(Box::new(ReplicatedDatabase::new())),
    }
}

pub struct DbContext {
    pub db: MutBTreeMap,
    pub transaction_map: IntentMap,
    pub old_values_store: MutSlab,
    pub wallog: ByteBufferWAL,
    pub replicators: Option<Box<ReplicatedDatabase>>,
}

impl Drop for DbContext {
    fn drop(&mut self) {
        // Checks if our database and the replicated database are the exact same by comparing debug strings.
        if let Some(repl) = &self.replicators {
            let my = self.db.printdb();
            let theirs = repl.dump();

            if my != theirs {
                // println!("drop db checking");
                // println!("{}\n{}", my, theirs);
                eprintln!("error: nonmatching");

                println!("{:?}", self.transaction_map);
                // panic!()
            } else {
                println!("replication matches");
            }
        }
    }
}

impl DbContext {
    pub fn replicator(&self) -> &ReplicatedDatabase {
        self.replicators.as_ref().unwrap()
    }
}

unsafe impl Send for DbContext {}

unsafe impl Sync for DbContext {}
