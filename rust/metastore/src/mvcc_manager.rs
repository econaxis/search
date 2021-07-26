use crate::LockDataRef;

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

pub mod write_intent_manager {
    pub use std::sync::atomic::{AtomicU64, Ordering};

    pub use crate::kv_backend::IntentMapType;
    pub use crate::mvcc_manager::WriteIntent;
    pub use crate::mvcc_manager::WriteIntentStatus;
    use crate::timestamp::Timestamp;
    use crate::DbContext;
    pub use crate::{LockDataRef, TransactionLockData};

    pub fn generate_write_txn(ctx: &DbContext) -> LockDataRef {
        let txn = TransactionLockData(WriteIntentStatus::Pending);
        let timestamp = Timestamp::now();
        let txnref = LockDataRef {
            id: timestamp.0,
            timestamp,
        };

        ctx.transaction_map.borrow_mut().insert(txnref, txn);

        txnref
    }

    pub fn generate_read_txn() -> LockDataRef {
        let id = Timestamp::now();
        LockDataRef {
            id: id.0,
            timestamp: id,
        }
    }

    pub fn generate_read_txn_with_time(time: Timestamp) -> LockDataRef {
        LockDataRef {
            id: time.0,
            timestamp: time,
        }
    }
}

use std::ops::Deref;

use crate::btreemap_kv_backend::MutBTreeMap;
use crate::kv_backend::ValueWithMVCC;
use crate::mvcc_metadata::MVCCMetadata;
use crate::object_path::ObjectPath;
use crate::DbContext;

pub fn update(
    ctx: &DbContext,
    key: &ObjectPath,
    new_value: String,
    txn: LockDataRef,
) -> Result<(), String> {
    let wi = WriteIntent {
        associated_transaction: txn,
    };
    if check_has_value(&ctx.db, key) {
        let res = get_latest_mvcc_value(&ctx.db, key);
        res.0.check_write(&ctx.transaction_map.borrow(), txn)?;
        res.0.deactivate(txn.timestamp)?;
        MVCCMetadata::become_newer_version(ctx, res);

        std::mem::replace(&mut res.1, new_value);
        res.0.add_write_intent(wi);
    } else {
        // We're inserting a new key here.
        let newerversion = MVCCMetadata::new_default(txn.timestamp);
        ctx.db
            .insert(key.clone(), ValueWithMVCC(newerversion, new_value));
    };

    Ok(())
}

fn check_has_value(db: &MutBTreeMap, key: &ObjectPath) -> bool {
    // todo: fix error with unbounded min bound
    db.get_mut(key).is_some()
}

fn get_latest_mvcc_value<'a>(db: &'a MutBTreeMap, key: &ObjectPath) -> &'a mut ValueWithMVCC {
    let res = db.get_mut(key).unwrap();
    res
}

fn get_correct_mvcc_value<'a>(
    ctx: &'a DbContext,
    key: &ObjectPath,
    txn: LockDataRef,
) -> &'a mut ValueWithMVCC {
    let res = get_latest_mvcc_value(&ctx.db, key);

    let mut prevoption = &res.0.previous_mvcc_value;

    while let Some(prev) = prevoption {
        let prevval: &'a mut ValueWithMVCC = ctx.old_values_store.get().get_mut(*prev).unwrap();
        if prevval.0.end_ts >= txn.timestamp && prevval.0.begin_ts <= txn.timestamp {
            return prevval;
        } else {
            prevoption = &prevval.0.previous_mvcc_value;
        }
    }
    panic!("no value found");
}

pub fn read<'a>(
    ctx: &'a DbContext,
    key: &ObjectPath,
    cur_txn: LockDataRef,
) -> Result<&'a str, String> {
    let ValueWithMVCC(metadata, str) = get_correct_mvcc_value(ctx, key, cur_txn);
    metadata.check_read(&ctx.transaction_map.borrow().deref(), cur_txn)?;
    Ok(str)
}
