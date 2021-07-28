mod mvcc_metadata;
mod lock_data_manager;
mod kv_backend;
mod btreemap_kv_backend;
pub use mvcc_metadata::MVCCMetadata;

use crate::DbContext;
pub use kv_backend::ValueWithMVCC;
use crate::object_path::ObjectPath;

pub use btreemap_kv_backend::MutBTreeMap;

pub use lock_data_manager::{LockDataRef, IntentMap};
pub use mvcc_metadata::{WriteIntent, WriteIntentStatus};

pub(super) fn update(
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
        res.0.check_write(&ctx.transaction_map, txn)?;
        res.0.deactivate(txn.timestamp)?;
        MVCCMetadata::become_newer_version(ctx, res, new_value);
        res.0.add_write_intent(wi);
    } else {
        // We're inserting a new key here.
        let mut newerversion = MVCCMetadata::new_default(txn.timestamp);
        newerversion.add_write_intent(wi);
        ctx.db
            .insert(key.clone(), ValueWithMVCC(newerversion, new_value));
    };


    Ok(())
}

fn check_has_value(db: &MutBTreeMap, key: &ObjectPath) -> bool {
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
) -> Result<&'a mut ValueWithMVCC, &'static str> {
    let res = get_latest_mvcc_value(&ctx.db, key);
    if res.0.check_read(&ctx.transaction_map, txn).is_ok() {
        return Ok(res);
    }

    let mut prevoption = &res.0.previous_mvcc_value;
    while let Some(prev) = prevoption {
        let prevval: &'a mut ValueWithMVCC = ctx.old_values_store.get().get_mut(*prev).unwrap();

        if prevval.0.check_read(&ctx.transaction_map, txn).is_ok() {
            return Ok(prevval);
        } else {
            prevoption = &prevval.0.previous_mvcc_value;
        }
    }
    Err("no value found")
}

pub(super) fn read<'a>(
    ctx: &'a DbContext,
    key: &ObjectPath,
    cur_txn: LockDataRef,
) -> Result<&'a str, String> {
    let ValueWithMVCC(metadata, str) = get_correct_mvcc_value(ctx, key, cur_txn)?;
    metadata.check_read(&ctx.transaction_map, cur_txn)?;
    metadata.confirm_read(cur_txn);
    Ok(str)
}

