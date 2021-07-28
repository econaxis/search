use std::fmt::{Display, Formatter};

use crate::DbContext;
use super::kv_backend::ValueWithMVCC;
use super::lock_data_manager::{IntentMap, LockDataRef};
use crate::timestamp::Timestamp;
use serde::{Serialize, Deserialize};


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

    pub(in super) fn add_write_intent(&mut self, wi: WriteIntent) {
        if self.cur_write_intent.is_some() {
            println!("warning: inserting write intent when it already exists. make sure already ran check");
        }

        self.cur_write_intent.insert(wi);
    }

    fn check_write_intents(
        &mut self,
        txnmap: &IntentMap,
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
                match txnmap.get_by_ref(associated_transaction) {
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

    pub(in super) fn check_write(
        &mut self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        self.check_read(txnmap, cur_txn)?;

        if self.end_ts != Timestamp::maxtime() {
            // This record is not the latest, so we can't write to it.
            return Err("Trying to write on a historical MVCC record".to_string());
        }

        if cur_txn.timestamp < self.last_read {
            Err("Timestamp bigger than last read".to_string())
        } else {
            Ok(())
        }
    }

    pub(in super) fn become_newer_version(
        ctx: &DbContext,
        ValueWithMVCC(oldmvcc, oldvalue): &mut ValueWithMVCC,
        newvalue: String
    ) {
        assert_ne!(oldmvcc.end_ts, Timestamp::maxtime());

        let newmvcc = Self {
            begin_ts: oldmvcc.end_ts,
            end_ts: Timestamp::maxtime(),
            last_read: oldmvcc.end_ts,
            cur_write_intent: None,
            previous_mvcc_value: None,
        };

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

    pub(in super) fn deactivate(&mut self, timestamp: Timestamp) -> Result<(), String> {
        self.end_ts = timestamp;
        self.last_read = self.last_read.max(timestamp);
        assert!(self.begin_ts <= self.end_ts);
        Ok(())
    }

    pub(in super) fn check_read(
        &mut self,
        txnmap: &IntentMap,
        cur_txn: LockDataRef,
    ) -> Result<(), String> {
        if cur_txn.timestamp < self.begin_ts || cur_txn.timestamp > self.end_ts {
            return Err("Timestamp not valid".to_string());
        }
        self.check_write_intents(txnmap, cur_txn)?;
        Ok(())
    }

    pub(in super) fn confirm_read(&mut self, cur_txn: LockDataRef) {
        // todo: only set this when the reads commit
        self.last_read = self.last_read.max(cur_txn.timestamp);
    }

    #[cfg(test)]
    pub fn check_matching_timestamps(&self, other: &Self) -> bool {
        self.end_ts == other.end_ts &&
            self.begin_ts == other.begin_ts &&
            self.last_read == other.last_read
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVCCMetadata {
    begin_ts: Timestamp,
    end_ts: Timestamp,
    last_read: Timestamp,
    #[serde(skip)]
    cur_write_intent: Option<WriteIntent>,

    #[serde(skip)]
    pub(in super) previous_mvcc_value: Option<usize>,
}

#[cfg(test)]
impl Default for MVCCMetadata {
    fn default() -> Self {
        Self {
            begin_ts: Timestamp::mintime(),
            end_ts: Timestamp::maxtime(),
            last_read: Timestamp::mintime(),
            cur_write_intent: None,
            previous_mvcc_value: None
        }
    }
}

impl Display for MVCCMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "beg: {}, end: {}, lr: {}, wi: {}",
            self.begin_ts.to_string(),
            self.end_ts.to_string(),
            self.last_read.to_string(),
            &self.cur_write_intent.is_some()
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use crate::object_path::ObjectPath;
    use crate::rwtransaction_wrapper::RWTransactionWrapper;

    use super::*;

    #[test]
    fn check_read() {
        // Only for 5 -> Infinity
        let mut metadata = MVCCMetadata::new_default(Timestamp::from(5));
        let emptymap = IntentMap::new();

        let mut txn1 = LockDataRef {
            id: 0,
            timestamp: Timestamp(2),
        };
        assert_matches!(metadata.check_read(&emptymap, txn1), Err(..));

        txn1.timestamp = Timestamp(5);
        assert_matches!(metadata.check_read(&emptymap, txn1), Ok(..));
    }

    #[test]
    fn check_read_with_active_txn() {
        let ctx = create_empty_context();
        let mut txn1 = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(5));
        let mut txnread = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(10));
        let key1 = ObjectPath::new("key1");
        txn1.write(&key1, "value1".into()).unwrap();
        assert_matches!(txnread.read(ObjectPath::from("key1").as_cow_str()), Err(..));

        txn1.commit();
        assert_matches!(txnread.read("key1".into()), Ok("value1"));
    }

    #[test]
    fn check_later_read_means_failed_write() {
        let ctx = create_empty_context();
        let key = ObjectPath::from("key1");

        {
            let mut txninit = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(1));
            txninit.write(&key, "whatever".into()).unwrap();
            txninit.commit();
        }

        let mut txn1 = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(5));
        let mut txnread = RWTransactionWrapper::new_with_time(&ctx, Timestamp::from(10));


        txnread.read(key.as_cow_str()).unwrap();

        assert_matches!(txn1.write(&key, "should fail".into()), Err(..));
    }

    #[test]
    fn test_become_newer_version() {
        let ctx = create_empty_context();
        let mvcc = MVCCMetadata {
            begin_ts: Timestamp(1),
            end_ts: Timestamp(5),
            ..Default::default()
        };

        let value = String::new();
        let mut combined = ValueWithMVCC(mvcc.clone(), value);
        MVCCMetadata::become_newer_version(&ctx, &mut combined, "fdsvc".to_string());


        assert_matches!(combined.0, MVCCMetadata {
            begin_ts: Timestamp(5),
            end_ts: Timestamp(0),
            last_read: Timestamp(5),
            ..
        });

        let txn = LockDataRef {
            id: 0,
            timestamp: Timestamp(2),
        };

        assert_matches!(combined.0.check_read(&ctx.transaction_map, txn), Err(..));

        let prevvalue = ctx.old_values_store.get().get_mut(combined.0.previous_mvcc_value.unwrap()).unwrap();
        assert_matches!(combined.0.check_read(&ctx.transaction_map, txn), Err(..));
        assert_matches!(prevvalue.0.check_read(&ctx.transaction_map, txn), Ok(()));
    }

    #[test]
    fn test_become_newer_version2() {
        let ctx = create_empty_context();
        let mvcc = MVCCMetadata {
            begin_ts: Timestamp(1),
            end_ts: Timestamp::maxtime(),
            ..Default::default()
        };
        let value = "zeroth".to_string();
        let key = ObjectPath::new("key");
        ctx.db.insert(key.clone(), ValueWithMVCC(mvcc.clone(), value));
        let mut combined = ctx.db.get_mut(&key).unwrap();

        combined.0.deactivate(Timestamp(5));
        MVCCMetadata::become_newer_version(&ctx, &mut combined, "first".to_string());

        combined.0.deactivate(Timestamp(10));
        MVCCMetadata::become_newer_version(&ctx, &mut combined, "second".to_string());

        combined.0.deactivate(Timestamp(15));
        MVCCMetadata::become_newer_version(&ctx, &mut combined, "third".to_string());

        let mut txn = RWTransactionWrapper::new_with_time(&ctx, Timestamp(16));
        assert_matches!(txn.read(key.as_cow_str()).unwrap(), "third");

        let mut txn = RWTransactionWrapper::new_with_time(&ctx, Timestamp(11));
        assert_matches!(txn.read(key.as_cow_str()).unwrap(), "second");

        let mut txn = RWTransactionWrapper::new_with_time(&ctx, Timestamp(6));
        assert_matches!(txn.read(key.as_cow_str()).unwrap(), "first");

        let mut txn = RWTransactionWrapper::new_with_time(&ctx, Timestamp(1));
        assert_matches!(txn.read(key.as_cow_str()).unwrap(), "zeroth");
    }
}


#[derive(PartialEq, Copy, Clone)]
pub enum WriteIntentStatus {
    Aborted,
    Pending,
    Committed,
}

#[derive(Debug, Clone)]
pub struct WriteIntent {
    pub associated_transaction: LockDataRef,
}
