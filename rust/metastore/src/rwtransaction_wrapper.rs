use std::borrow::Cow;

use serde_json::Value as JSONValue;

use crate::mvcc_manager::{write_intent_manager, WriteIntentStatus};
use crate::object_path::ObjectPath;
use crate::{json_processing, mvcc_manager, DbContext, LockDataRef};

pub struct RWTransactionWrapper<'a> {
    ctx: &'a DbContext,
    txn: LockDataRef,
    committed: bool,
}

impl<'a> RWTransactionWrapper<'a> {
    pub fn read_range(&mut self, key: &ObjectPath) -> String {
        let range = key.get_prefix_ranges();
        let mut json = JSONValue::Null;
        for row in self.ctx.db.range_mut(range) {
            let path: Vec<&str> = row.0.split_parts().collect();
            json_processing::create_materialized_path(&mut json, &path, row.1 .1.clone());
        }

        json.to_string()
    }

    pub fn write(&mut self, key: &ObjectPath, value: Cow<str>) {
        mvcc_manager::update(self.ctx, key, value.into_owned(), self.txn).unwrap();
    }

    pub fn commit(mut self) -> Result<(), String> {
        self.ctx
            .transaction_map
            .borrow_mut()
            .get_mut(&self.txn)
            .unwrap()
            .0 = WriteIntentStatus::Committed;
        self.committed = true;
        Ok(())
    }

    pub fn abort(&self) -> Result<(), String> {
        // todo: lookup the previous mvcc value and upgrade it back to the main database
        todo!()
    }

    pub fn new(ctx: &'a DbContext) -> Self {
        let txn = write_intent_manager::generate_write_txn(ctx);

        Self {
            ctx,
            txn,
            committed: false,
        }
    }
}

impl Drop for RWTransactionWrapper<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.abort().unwrap();
        }
    }
}
