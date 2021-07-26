use crate::kv_backend::{IntentMapType, ValueWithMVCC};
use crate::mvcc_manager::{WriteIntent, WriteIntentStatus};
use crate::timestamp::Timestamp;
use crate::{DbContext, LockDataRef};

impl MVCCMetadata {
    pub fn new_default(timestamp: Timestamp) -> Self {
        Self {
            begin_ts: timestamp,
            end_ts: Timestamp::maxtime(),
            last_read: Timestamp::mintime(),
            cur_write_intent: None,
            previous_mvcc_value: None,
        }
    }

    pub fn add_write_intent(&mut self, wi: WriteIntent) {
        if self.cur_write_intent.is_some() {
            println!("warning: inserting write intent when it already exists");
        }

        self.cur_write_intent.insert(wi);
    }

    fn check_write_intents(
        &mut self,
        txnmap: &IntentMapType,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        match &self.cur_write_intent {
            None => Ok(()),
            Some(WriteIntent {
                associated_transaction,
            }) if *associated_transaction == cur_txn => Ok(()),
            Some(WriteIntent {
                associated_transaction,
            }) => {
                match txnmap.get(associated_transaction) {
                    None => {
                        println!("No transaction found for transaction ref, ignoring");
                        Ok(())
                    }
                    // If the transaction is committed already, then all good.
                    Some(txn) if txn.get_write_intent() == WriteIntentStatus::Committed => {
                        if associated_transaction.timestamp < cur_txn.timestamp {
                            // All good, we write after the txn has committed
                            self.cur_write_intent.take();

                            Ok(())
                        } else {
                            Err("Tried to read or write to a value committed after".to_string())
                        }
                    }
                    Some(txn) if txn.get_write_intent() == WriteIntentStatus::Aborted => {
                        // Transaction has been aborted, so we are reading an aborted value. Therefore, we should also abort this current transaction.
                        Err("Reading or writing to an aborted value".to_string())
                    }
                    Some(..) => {
                        Err("Write Intent still exists and has not been committed yet".to_owned())
                    }
                }
            }
        }
    }

    pub fn check_write(
        &mut self,
        txnmap: &IntentMapType,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        self.check_read(txnmap, cur_txn)?;

        if cur_txn.timestamp < self.last_read {
            return Err("Timestamp bigger than last read".to_string());
        }

        Ok(())
    }

    pub fn become_newer_version(
        ctx: &DbContext,
        ValueWithMVCC(oldmvcc, oldvalue): &mut ValueWithMVCC,
    ) {
        assert_ne!(oldmvcc.end_ts, Timestamp::maxtime());

        let newmvcc = Self {
            begin_ts: oldmvcc.end_ts,
            end_ts: Timestamp::maxtime(),
            last_read: oldmvcc.end_ts,
            cur_write_intent: None,
            previous_mvcc_value: None,
        };
        let newvalue = String::new();

        let oldmetadata_ = std::mem::replace(oldmvcc, newmvcc);
        let oldvalue_ = std::mem::replace(oldvalue, newvalue);

        // Alias the variable for clarity. Self is now the newer metadata
        let newmetadata_: &mut MVCCMetadata = oldmvcc;
        let _newvalue_ = oldvalue;

        let oldmetadata_key = ctx
            .old_values_store
            .get()
            .insert(ValueWithMVCC(oldmetadata_, oldvalue_));
        newmetadata_.previous_mvcc_value.insert(oldmetadata_key);
    }

    pub fn deactivate(&mut self, timestamp: Timestamp) -> Result<(), String> {
        self.end_ts = timestamp;
        self.last_read = self.last_read.max(timestamp);
        assert!(self.begin_ts <= self.end_ts);
        Ok(())
    }

    pub fn check_read(
        &mut self,
        txnmap: &IntentMapType,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        if cur_txn.timestamp < self.begin_ts || cur_txn.timestamp > self.end_ts {
            return Err("Timestamp not valid".to_string());
        }

        self.check_write_intents(txnmap, cur_txn)?;

        self.last_read = self.last_read.max(cur_txn.timestamp);
        Ok(())
    }
}

#[derive(Debug)]
pub struct MVCCMetadata {
    pub begin_ts: Timestamp,
    pub end_ts: Timestamp,
    pub last_read: Timestamp,
    pub cur_write_intent: Option<WriteIntent>,
    pub previous_mvcc_value: Option<usize>,
}
